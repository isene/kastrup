mod config;
mod database;
mod log;
mod message;
mod organizer;
mod poller;
mod source;
mod sources;

use crust::{Crust, Pane, Input};
use crust::style;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::mpsc as std_mpsc;

/// Background DB write operations (fire-and-forget from main thread)
enum DbWriteOp {
    MarkRead(i64),
    MarkUnread(i64),
    ToggleStar(i64),
    DeleteMessages(Vec<i64>),
    UpdateFolder(i64, String, serde_json::Value),
    UpdateLabels(i64, String),
    UpdateMetadata(i64, String),
    SyncMaildirFlag(serde_json::Value, i64),
    SetSetting(String, String),
    Execute(String, Vec<String>), // raw SQL with string params
}

use config::{Config, Identity};
use database::{Database, Filters};
use message::Message;

// --- Compose target picker ---

/// One reachable destination for `m` (compose new). Harvested from the
/// currently filtered messages; scoped to whichever sources have a `send`
/// template configured in `config.senders`.
struct ComposeTarget {
    plugin_type: String,
    conversation_id: String,
    folder: String,
    source_id: i64,
    recent_ts: i64,
}

// --- Folder browser types ---

struct FolderEntry {
    name: String,
    full_name: String,
    depth: usize,
    has_children: bool,
    collapsed: bool,
}

/// Wrap URLs in a line with OSC 8 hyperlink escapes so kitty keeps them
/// clickable even when the visible text wraps across multiple pane lines.
fn hyperlink_urls(line: &str) -> String {
    use std::sync::OnceLock;
    static URL_RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = URL_RE.get_or_init(|| {
        regex::Regex::new(r#"https?://[^\s<>()\[\]{}\x00-\x1f\x7f]+[^\s<>()\[\]{}\x00-\x1f\x7f.,;:!?'"]"#)
            .unwrap()
    });
    let mut out = String::with_capacity(line.len());
    let mut last = 0;
    for m in re.find_iter(line) {
        out.push_str(&line[last..m.start()]);
        let url = m.as_str();
        // OSC 8: \e]8;;URL\e\\text\e]8;;\e\\
        out.push_str("\x1b]8;;");
        out.push_str(url);
        out.push_str("\x1b\\");
        out.push_str(url);
        out.push_str("\x1b]8;;\x1b\\");
        last = m.end();
    }
    out.push_str(&line[last..]);
    out
}

fn discover_maildir_folders(maildir_path: &std::path::Path) -> Vec<String> {
    let mut folder_names = Vec::new();
    if let Ok(entries) = std::fs::read_dir(maildir_path) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.starts_with('.') || name == "." || name == ".." { continue; }
            let path = entry.path();
            if !path.is_dir() { continue; }
            if !path.join("cur").is_dir() && !path.join("new").is_dir() { continue; }
            folder_names.push(name[1..].to_string());
        }
    }
    folder_names.sort();
    folder_names
}

fn build_folder_tree(folder_names: &[String]) -> serde_json::Map<String, serde_json::Value> {
    let mut tree = serde_json::Map::new();
    for name in folder_names {
        let parts: Vec<&str> = name.split('.').collect();
        let mut node = &mut tree;
        for part in parts {
            if !node.contains_key(part) {
                node.insert(part.to_string(), serde_json::json!({}));
            }
            node = node.get_mut(part).unwrap().as_object_mut().unwrap();
        }
    }
    tree
}

fn flatten_folder_tree(
    tree: &serde_json::Map<String, serde_json::Value>,
    prefix: &str,
    depth: usize,
    collapsed: &HashMap<String, bool>,
) -> Vec<FolderEntry> {
    let mut result = Vec::new();
    let mut keys: Vec<&String> = tree.keys().collect();
    keys.sort();
    for key in keys {
        let full_name = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };
        let children = tree[key].as_object();
        let has_children = children.map(|c| !c.is_empty()).unwrap_or(false);
        let is_collapsed = collapsed.get(&full_name).copied().unwrap_or(false);

        result.push(FolderEntry {
            name: key.clone(),
            full_name: full_name.clone(),
            depth,
            has_children,
            collapsed: is_collapsed,
        });

        if has_children && !is_collapsed {
            if let Some(children) = children {
                result.extend(flatten_folder_tree(children, &full_name, depth + 1, collapsed));
            }
        }
    }
    result
}

// --- Preferences types ---

enum PrefType {
    Bool(bool),
    Choice(Vec<&'static str>, String),
    Text(String),
    Num(u8, u8, u8), // value, min, max
}

fn next_pref(p: &mut PrefType) {
    match p {
        PrefType::Bool(v) => *v = !*v,
        PrefType::Choice(opts, v) => {
            let idx = opts.iter().position(|&o| o == v.as_str()).unwrap_or(0);
            *v = opts[(idx + 1) % opts.len()].to_string();
        }
        PrefType::Num(v, _, max) => *v = if *v >= *max { *max } else { *v + 1 },
        _ => {}
    }
}

fn prev_pref(p: &mut PrefType) {
    match p {
        PrefType::Bool(v) => *v = !*v,
        PrefType::Choice(opts, v) => {
            let idx = opts.iter().position(|&o| o == v.as_str()).unwrap_or(0);
            *v = opts[(idx + opts.len() - 1) % opts.len()].to_string();
        }
        PrefType::Num(v, min, _) => *v = if *v <= *min { *min } else { *v - 1 },
        _ => {}
    }
}

struct App {
    top: Pane,
    left: Pane,
    right: Pane,
    bottom: Pane,
    cols: u16,
    rows: u16,

    db: Arc<Database>,
    config: Config,
    source_type_map: HashMap<i64, String>,

    // Stats cache with TTL
    stats_cache: Option<(std::time::Instant, (i64, i64, i64))>,
    last_db_refresh: std::time::Instant,

    // State
    running: bool,
    current_view: String,
    active_folder: Option<String>,
    in_source_view: bool,
    index: usize,

    filtered_messages: Vec<Message>,
    views: Vec<database::View>,
    sources_list: Vec<source::Source>,

    sort_order: String,
    sort_inverted: bool,
    date_format: String,
    width: u16,
    border: u8,

    tagged: HashSet<i64>,
    delete_marked: HashSet<i64>,
    browsed_ids: HashSet<i64>,
    unseen_ids: HashSet<i64>,

    folder_collapsed: HashMap<String, bool>,
    folder_count_cache: HashMap<String, (i64, i64)>,

    feedback_message: Option<(String, u8)>,
    feedback_expires: Option<std::time::Instant>,

    showing_image: bool,
    right_pane_msg_id: Option<i64>,
    pending_forward_ids: Vec<i64>,
    pending_forward_attachments: Vec<String>,
    pending_reply_id: Option<i64>,
    compose_source_type: Option<String>,
    image_display: Option<glow::Display>,

    // Threading state
    show_threaded: bool,
    group_by_folder: bool,
    display_messages: Vec<Message>,
    section_collapsed: HashMap<String, bool>,

    // Background poller
    poller: Option<poller::Poller>,
    poller_rx: Option<std::sync::mpsc::Receiver<poller::PollerEvent>>,
    write_tx: std_mpsc::Sender<DbWriteOp>,

    // Help state
    showing_help: bool,
    help_extended: bool,
    right_pane_locked: bool,
}

fn main() {
    log::info(&format!("Kastrup v{} starting", env!("CARGO_PKG_VERSION")));
    // Parse CLI args: --compose-to EMAIL --subject SUBJECT, or mailto:URL
    let args: Vec<String> = std::env::args().collect();
    let mut compose_to: Option<String> = None;
    let mut compose_subject: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--compose-to" if i + 1 < args.len() => { compose_to = Some(args[i + 1].clone()); i += 2; }
            "--subject" if i + 1 < args.len() => { compose_subject = Some(args[i + 1].clone()); i += 2; }
            a if a.starts_with("mailto:") => {
                // Parse mailto:user@host?subject=X&cc=Y&body=Z
                let rest = &a[7..];
                let (addr, query) = rest.split_once('?').unwrap_or((rest, ""));
                compose_to = Some(addr.to_string());
                for param in query.split('&') {
                    if let Some((k, v)) = param.split_once('=') {
                        let decoded = v.replace("%20", " ").replace("+", " ");
                        match k.to_lowercase().as_str() {
                            "subject" => compose_subject = Some(decoded),
                            _ => {}
                        }
                    }
                }
                i += 1;
            }
            _ => { i += 1; }
        }
    }

    Crust::init();
    let (cols, rows) = Crust::terminal_size();

    let config = Config::load();
    let db = Arc::new(Database::new().expect("Failed to open heathrow database"));
    let source_type_map = db.get_source_type_map();
    let views = db.get_views();

    let width = config.pane_width;
    let border = config.border_style;
    let (top, left, right, bottom) = create_panes(cols, rows, width, border, &config);

    // Spawn background DB writer thread
    let (write_tx, write_rx) = std_mpsc::channel::<DbWriteOp>();
    let writer_db = db.clone();
    std::thread::spawn(move || {
        while let Ok(op) = write_rx.recv() {
            match op {
                DbWriteOp::MarkRead(id) => { writer_db.mark_as_read(id); }
                DbWriteOp::MarkUnread(id) => { writer_db.mark_as_unread(id); }
                DbWriteOp::ToggleStar(id) => { writer_db.toggle_star(id); }
                DbWriteOp::DeleteMessages(ids) => { writer_db.delete_messages(&ids); }
                DbWriteOp::UpdateFolder(id, folder, meta) => { writer_db.update_message_folder(id, &folder, &meta); }
                DbWriteOp::UpdateLabels(id, json) => {
                    let conn = writer_db.conn.lock().unwrap();
                    let _ = conn.execute("UPDATE messages SET labels = ? WHERE id = ?", rusqlite::params![json, id]);
                }
                DbWriteOp::UpdateMetadata(id, json) => {
                    let conn = writer_db.conn.lock().unwrap();
                    let _ = conn.execute("UPDATE messages SET metadata = ? WHERE id = ?", rusqlite::params![json, id]);
                }
                DbWriteOp::SyncMaildirFlag(metadata, id) => {
                    sync_maildir_seen_flag_bg(&metadata, &writer_db, id);
                }
                DbWriteOp::SetSetting(key, val) => { writer_db.set_setting(&key, &val); }
                DbWriteOp::Execute(sql, params) => {
                    let conn = writer_db.conn.lock().unwrap();
                    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
                    let _ = conn.execute(&sql, param_refs.as_slice());
                }
            }
        }
    });

    let mut app = App {
        top, left, right, bottom,
        cols, rows,
        db,
        config,
        source_type_map,
        stats_cache: None,
        last_db_refresh: std::time::Instant::now(),
        running: true,
        current_view: "A".to_string(),
        active_folder: None,
        in_source_view: false,
        index: 0,
        filtered_messages: Vec::new(),
        views,
        sources_list: Vec::new(),
        sort_order: "latest".to_string(),
        sort_inverted: false,
        date_format: "%b %e".to_string(),
        width,
        border,
        tagged: HashSet::new(),
        delete_marked: HashSet::new(),
        browsed_ids: HashSet::new(),
        unseen_ids: HashSet::new(),
        folder_collapsed: HashMap::new(),
        folder_count_cache: HashMap::new(),
        feedback_message: None,
        feedback_expires: None,
        showing_image: false,
        right_pane_msg_id: None,
        pending_forward_ids: Vec::new(),
        pending_forward_attachments: Vec::new(),
        pending_reply_id: None,
        compose_source_type: None,
        image_display: None,
        show_threaded: false,
        group_by_folder: false,
        display_messages: Vec::new(),
        section_collapsed: HashMap::new(),
        poller: None,
        poller_rx: None,
        write_tx,
        showing_help: false,
        help_extended: false,
        right_pane_locked: false,
    };

    // Apply config defaults
    app.sort_order = app.config.sort_order.clone();
    app.sort_inverted = app.config.sort_inverted;
    app.date_format = app.config.date_format.clone();

    // First-run wizard if database is empty
    if app.db.is_empty() && app.db.get_sources(false).is_empty() {
        app.first_run_wizard();
    }

    // Start background poller
    let (poller_tx, poller_rx) = std::sync::mpsc::channel();
    let poller = poller::Poller::start(app.db.clone(), poller_tx);
    app.poller = Some(poller);
    app.poller_rx = Some(poller_rx);

    // Load initial view
    let default_view = app.config.default_view.clone();
    app.switch_to_view(&default_view);
    app.render_all();

    // Handle --compose-to from CLI (e.g. from Tock)
    if let Some(to) = compose_to {
        let subj = compose_subject.unwrap_or_default();
        app.compose_to(&to, &subj);
    }

    while app.running {
        // Check feedback expiry
        if let Some(expires) = app.feedback_expires {
            if std::time::Instant::now() >= expires {
                app.feedback_message = None;
                app.feedback_expires = None;
                app.render_bottom_bar();
            }
        }

        let key = Input::getchr(Some(2));
        match key {
            Some(k) => app.handle_key(&k),
            None => {
                // Check for new messages from poller
                let mut new_count = 0usize;
                if let Some(ref rx) = app.poller_rx {
                    while let Ok(event) = rx.try_recv() {
                        match event {
                            poller::PollerEvent::NewMessages(count) => {
                                new_count += count;
                            }
                        }
                    }
                }
                if new_count > 0 {
                    app.set_feedback(
                        &format!("{} new message(s)", new_count),
                        app.config.theme_colors.feedback_ok,
                    );
                    app.refresh_current_view();
                }
                // Periodic DB refresh (skip when showing inline images)
                if !app.showing_image && app.delete_marked.is_empty() && app.last_db_refresh.elapsed().as_secs() >= 5 {
                    app.last_db_refresh = std::time::Instant::now();
                    app.refresh_current_view();
                }
            }
        }
    }

    // Stop poller immediately (don't wait for drop)
    log::info("Stopping poller...");
    if let Some(mut p) = app.poller.take() {
        p.stop();
    }
    log::info("Dropping app...");
    drop(app);
    log::info("Cleanup...");
    Crust::cleanup();
    log::info("Exit.");
}

fn create_panes(cols: u16, rows: u16, width: u16, border: u8, config: &Config) -> (Pane, Pane, Pane, Pane) {
    let top_bg = config.theme_colors.top_bg;
    let bottom_bg = config.theme_colors.bottom_bg;

    let top = Pane::new(1, 1, cols, 1, 255, top_bg);
    let bottom = Pane::new(1, rows, cols, 1, 252, bottom_bg);

    let left_w = (cols.saturating_sub(4)) * width / 10;
    let content_h = rows.saturating_sub(4);
    let mut left = Pane::new(2, 3, left_w, content_h, config.theme_colors.list_fg as u16, config.theme_colors.list_bg as u16);
    let mut right = Pane::new(left_w + 4, 3, cols.saturating_sub(left_w + 4), content_h, config.theme_colors.content_fg as u16, config.theme_colors.content_bg as u16);

    // Border styles: 0=none, 1=right only, 2=both, 3=left only
    left.border = matches!(border, 2 | 3);
    left.border_fg = Some(config.theme_colors.border_fg as u16);
    right.border = matches!(border, 1 | 2);
    right.border_fg = Some(config.theme_colors.border_fg as u16);
    if left.border { left.border_refresh(); }
    if right.border { right.border_refresh(); }

    left.scroll = true;
    right.scroll = true;

    (top, left, right, bottom)
}

// --- Key dispatch ---

impl App {
    fn handle_key(&mut self, key: &str) {
        // While an inline image is visible, only D acts (download). Any
        // other key dismisses the image, like ESC would. Otherwise their
        // redraw paints email text underneath the still-visible image.
        if self.showing_image {
            if key == "D" {
                self.download_images();
                return;
            }
            self.clear_inline_image();
            self.render_message_content();
            return;
        }

        if self.in_source_view {
            self.handle_source_key(key);
            return;
        }

        match key {
            // Navigation
            "j" | "DOWN" => { self.move_down(); }
            "k" | "UP" => { self.move_up(); }
            "h" | "LEFT" => {
                if self.show_threaded { self.collapse_current(); }
            }
            "HOME" => { self.go_first(); }
            "END" => { self.go_last(); }
            "PgDOWN" => { self.page_down(); }
            "PgUP" => { self.page_up(); }
            "ENTER" => {
                if self.show_threaded {
                    if let Some(msg) = self.display_messages.get(self.index) {
                        if msg.is_header { self.toggle_collapse(); return; }
                    }
                }
                self.open_message();
            }
            " " | "SPACE" => {
                if self.show_threaded { self.toggle_collapse(); }
            }
            "n" => { self.next_unread(); }
            "p" => { self.prev_unread(); }
            "J" => { self.jump_to_date(); }
            "G" => { self.cycle_view_mode(); }
            "{" | "C-UP" => { /* move section - needs threading */ }
            "}" | "C-DOWN" => { /* move section - needs threading */ }

            // View switching
            "A" => { self.switch_to_view("A"); }
            "N" => { self.switch_to_view("N"); }
            "S" => { self.show_sources(); }
            "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" => {
                self.switch_to_view(key);
            }
            "F1" | "F2" | "F3" | "F4" | "F5" | "F6" | "F7" | "F8" | "F9"
            | "F10" | "F11" | "F12" => {
                self.switch_to_view(key);
            }
            "F" => { self.show_favorites_browser(); }
            "L" => { self.load_more(); }
            "C-R" => { self.refresh_view(); }
            "C-F" => { self.edit_filter(); }
            "K" => { self.kill_view(); }

            // Message operations
            "R" => { self.toggle_read(); }
            "M" => { self.mark_all_read(); }
            "*" | "-" => { self.toggle_star(); }
            "t" => { self.toggle_tag(); }
            "T" => { self.tag_all_toggle(); }
            "C-T" => { self.tag_by_regex(); }
            "d" => { self.toggle_delete_mark(); }
            "<" => { self.purge_deleted(); }
            "u" | "U" => { self.unsee_message(); }
            "S-SPACE" => { self.mark_browsed_as_read(); }

            // Compose / reply
            "r" => { self.reply(false); }
            "e" => { self.reply(true); }
            "g" => { self.reply_all(); }
            "f" => {
                self.bottom.say(&style::fg(" Forward: i=Inline  a=Attach as .eml", 226));
                if let Some(mode) = Input::getchr(Some(5)) {
                    match mode.as_str() {
                        "i" => {
                            if self.tagged.is_empty() { self.forward_inline(); }
                            else { self.forward_tagged_inline(); }
                        }
                        "a" => {
                            if self.tagged.is_empty() { self.forward_attach(); }
                            else { self.forward_tagged_attach(); }
                        }
                        _ => { self.render_bottom_bar(); }
                    }
                } else { self.render_bottom_bar(); }
            }
            "m" => { self.compose_new(); }
            "E" => { self.edit_message(); }

            // Attachments / external
            "v" => { self.view_attachments(); }
            "V" => { self.toggle_inline_image(); }
            "D" => { self.download_images(); }
            "x" => { self.open_external(); }
            "X" => { self.open_in_browser(); }

            // Search / filter
            "/" => { self.search_prompt(); }
            "@" => { self.address_book_menu(); }

            // Sort
            "o" => { self.cycle_sort(); }
            "i" => { self.toggle_sort_invert(); }

            // Labels / save / misc
            "l" => { self.label_message(); }
            "s" => { self.file_message(); }
            "+" => { self.external_react(false); }
            "-" => { self.external_react(true); }
            "I" => { self.ai_assistant(); }
            "Z" => { self.open_in_timely(); }

            // UI
            "w" => { self.cycle_width(); }
            "W" => { self.cycle_width_reverse(); }
            "D" => { self.cycle_date_format(); }
            "c" => { self.set_view_color(); }
            "C" => { self.show_preferences(); }
            "?" => {
                if self.showing_help && !self.help_extended {
                    self.show_extended_help();
                    self.help_extended = true;
                } else if self.showing_help && self.help_extended {
                    self.showing_help = false;
                    self.help_extended = false;
                    self.right_pane_locked = false;
                    self.render_message_content();
                } else {
                    self.show_help();
                    self.showing_help = true;
                    self.help_extended = false;
                    self.right_pane_locked = true;
                }
            }
            "y" => { self.copy_message_id(); }
            "Y" => { self.copy_right_pane(); }
            "B" => { self.show_folder_browser(); }

            "C-B" => { self.cycle_border(); }

            // Right pane scroll
            "S-DOWN" => { self.right.linedown(); }
            "S-UP" => { self.right.lineup(); }
            "TAB" | "S-RIGHT" => { self.right.pagedown(); }
            "S-TAB" | "S-LEFT" => { self.right.pageup(); }

            // Resize
            "RESIZE" => { self.handle_resize(); }
            "C-L" => { self.force_redraw(); }

            // Quit
            "q" | "Q" => { self.running = false; }

            _ => {}
        }
    }

    fn handle_source_key(&mut self, key: &str) {
        match key {
            "ESC" | "q" => {
                self.in_source_view = false;
                let v = self.config.default_view.clone();
                self.switch_to_view(&v);
            }
            "j" | "DOWN" => {
                if self.index < self.sources_list.len().saturating_sub(1) {
                    self.index += 1;
                }
                self.render_source_list();
                self.render_source_info();
            }
            "k" | "UP" => {
                if self.index > 0 { self.index -= 1; }
                self.render_source_list();
                self.render_source_info();
            }
            "ENTER" => {
                // Show messages from selected source
                if let Some(src) = self.sources_list.get(self.index) {
                    let sid = src.id;
                    self.in_source_view = false;
                    let mut filters = Filters::default();
                    filters.source_id = Some(sid);
                    self.filtered_messages = self.db.get_messages(&filters, 500, 0);
                    for msg in &mut self.filtered_messages {
                        if let Some(st) = self.source_type_map.get(&msg.source_id) {
                            msg.source_type = st.clone();
                        }
                    }
                    self.current_view = "S".to_string();
                    self.index = 0;
                    self.sort_messages();
                    self.rebuild_display();
                    self.render_all();
                }
            }
            // Source-specific operations
            "a" => { self.add_source(); }
            "e" => { self.edit_source(); }
            "d" => { self.delete_source(); }
            "t" => { self.test_source(); }
            " " | "SPACE" => { self.toggle_source(); }
            "c" => { self.set_source_color(); }
            "p" => { self.set_source_poll_interval(); }
            "C-R" => { self.refresh_view(); }
            // Allow view switching from source view
            "A" | "N" | "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" => {
                self.in_source_view = false;
                self.switch_to_view(key);
            }
            // UI controls pass through
            "w" => { self.cycle_width(); }
            "W" => { self.cycle_width_reverse(); }
            "C-B" => { self.cycle_border(); }
            "D" => { self.cycle_date_format(); }
            "C-L" => { self.force_redraw(); }
            "RESIZE" => { self.handle_resize(); }
            "Y" => { self.copy_right_pane(); }
            _ => {}
        }
    }
}

// --- Rendering ---

impl App {
    fn cached_stats(&mut self) -> (i64, i64, i64) {
        if let Some((time, stats)) = &self.stats_cache {
            if time.elapsed().as_secs() < 5 {
                return *stats;
            }
        }
        let stats = self.db.get_stats();
        self.stats_cache = Some((std::time::Instant::now(), stats));
        stats
    }

    fn render_all(&mut self) {
        self.render_top_bar();
        if self.in_source_view {
            self.render_source_list();
            self.render_source_info();
        } else {
            self.render_message_list();
            if !self.right_pane_locked {
                self.render_message_content();
            }
        }
        self.render_bottom_bar();
    }

    fn render_top_bar(&mut self) {
        let (_, unread, _) = self.cached_stats();
        let total = self.filtered_messages.len() as i64;

        let tc = &self.config.theme_colors;
        let view_label = if let Some(ref folder) = self.active_folder {
            style::fg(folder, tc.view_custom)
        } else { match self.current_view.as_str() {
            "A" => style::fg("All", tc.view_all),
            "N" => style::fg("New", tc.view_new),
            "S" => style::fg("Sources", tc.view_sources),
            "*" => style::fg("Starred", tc.view_starred),
            v => {
                // Look for named custom view with key number prefix
                if let Some(view) = self.views.iter().find(|vw| vw.key_binding.as_deref() == Some(v)) {
                    format!("{} {}", style::fg(&format!("[{}]", v), tc.hint_fg), style::fg(&view.name, tc.view_custom))
                } else {
                    format!("{} {}", style::fg(&format!("[{}]", v), tc.hint_fg), style::fg(&format!("View {}", v), tc.view_custom))
                }
            }
        } };

        // Set terminal window title
        let title_name = if let Some(ref folder) = self.active_folder {
            folder.clone()
        } else { match self.current_view.as_str() {
            "A" => "All".to_string(),
            "N" => "New".to_string(),
            "S" => "Sources".to_string(),
            "*" => "Starred".to_string(),
            v => self.views.iter().find(|vw| vw.key_binding.as_deref() == Some(v))
                .map(|vw| format!("{} {}", v, vw.name))
                .unwrap_or_else(|| format!("View {}", v)),
        } };
        Crust::set_title(&format!("Kastrup - {}", title_name));

        // Capitalize sort label
        let sort_cap = {
            let mut c = self.sort_order.chars();
            match c.next() {
                None => String::new(),
                Some(first) => format!("{}{}", first.to_uppercase(), c.as_str()),
            }
        };
        let sort_arrow = if self.sort_inverted { "\u{2191}" } else { "\u{2193}" };
        let sort_label = style::fg(&format!(" [{}{}]", sort_cap, sort_arrow), tc.info_fg);

        // Mode indicator
        let mode = if self.group_by_folder { "Folders" } else if self.show_threaded { "Threaded" } else { "Flat" };
        let mode_label = style::fg(&format!(" [{}]", mode), tc.hint_fg);

        // Position indicator (use display_messages count in threaded mode)
        let display_total = if self.show_threaded { self.display_messages.len() as i64 } else { total };
        let pos_label = if display_total > 0 {
            style::fg(&format!(" [{}/{}]", self.index + 1, display_total), tc.info_fg)
        } else {
            style::fg(" [0/0]", tc.info_fg)
        };

        let right_info = style::fg(
            &format!("{} unread / {} msgs", unread, total), tc.info_fg
        );

        // Build top bar: " Kastrup - [key] ViewName [Sort] [Mode] [pos] ... N unread / T msgs"
        let prefix = style::fg(" Kastrup - ", tc.prefix_fg);
        let left_part = format!("{}{}{}{}{}", prefix, view_label, sort_label, mode_label, pos_label);
        let left_width = crust::display_width(&left_part);
        let right_width = crust::display_width(&right_info);
        let padding = if self.cols as usize > left_width + right_width + 1 {
            " ".repeat(self.cols as usize - left_width - right_width)
        } else {
            " ".to_string()
        };

        self.top.say(&format!("{}{}{}", left_part, padding, right_info));
    }

    fn render_message_list(&mut self) {
        let h = self.left.h as usize;
        let messages = if self.show_threaded {
            &self.display_messages
        } else {
            &self.filtered_messages
        };
        if messages.is_empty() {
            self.left.set_text(&style::fg("  No messages", self.config.theme_colors.no_msg));
            self.left.ix = 0;
            self.left.full_refresh();
            return;
        }

        // Scrolloff=3: keep 3 lines visible above/below cursor
        let total = messages.len();
        let scrolloff: usize = 3;
        let mut start = self.left.ix;
        if total <= h {
            start = 0;
        } else if self.index < start + scrolloff {
            start = self.index.saturating_sub(scrolloff);
        } else if self.index + scrolloff >= start + h {
            let max_start = total.saturating_sub(h);
            start = (self.index + scrolloff + 1).saturating_sub(h).min(max_start);
        }

        let pane_w = self.left.w as usize;
        let end = (start + h + 5).min(total); // Small buffer for scrolloff
        let mut lines = Vec::with_capacity(end - start);
        for i in start..end {
            let msg = &messages[i];
            let selected = i == self.index;
            if msg.is_header {
                lines.push(self.format_section_header(msg, selected, pane_w));
            } else {
                lines.push(self.format_message_line(msg, selected, pane_w));
            }
        }

        self.left.set_text(&lines.join("\n"));
        self.left.ix = 0;
        self.left.refresh();
        if self.left.border { self.left.border_refresh(); }
    }

    fn format_section_header(&self, msg: &Message, selected: bool, pane_w: usize) -> String {
        let tc = &self.config.theme_colors;
        let subject = msg.subject.as_deref().unwrap_or("Section");
        let is_collapsed = msg.thread_id.as_ref()
            .and_then(|name| self.section_collapsed.get(name))
            .copied()
            .unwrap_or(false);
        let arrow = if is_collapsed { "\u{25B8}" } else { "\u{25BE}" };
        let unread_mark = if !msg.read {
            style::fg(" *", tc.unread)
        } else {
            String::new()
        };
        let (icon, scolor) = source_info(&msg.source_type, tc);
        let content = format!("{} {} {} [{}]{}",
            arrow, icon, subject, msg.content, unread_mark);
        let content_w = crust::display_width(&content);
        let padding = if pane_w > content_w { " ".repeat(pane_w - content_w) } else { String::new() };
        let full = format!("{}{}", content, padding);

        if selected {
            style::underline(&style::bold(&style::fg(&full, scolor)))
        } else {
            style::bold(&style::fg(&full, tc.thread))
        }
    }

    fn format_message_line(&self, msg: &Message, selected: bool, pane_w: usize) -> String {
        // N flag
        let nflag = if !msg.read {
            style::fg("N", self.config.theme_colors.unread)
        } else {
            " ".to_string()
        };

        // Replied/forwarded flag
        let forwarded = msg.metadata.get("forwarded").and_then(|v| v.as_bool()).unwrap_or(false);
        let rflag = if msg.replied && forwarded {
            style::fg("\u{2194}", self.config.theme_colors.replied) // ↔ both
        } else if msg.replied {
            style::fg("\u{2190}", self.config.theme_colors.replied) // ← replied
        } else if forwarded {
            style::fg("\u{2192}", self.config.theme_colors.replied) // → forwarded
        } else {
            " ".to_string()
        };

        // Indicator: D > tag > star > attachment > space
        let ind = if self.delete_marked.contains(&msg.id) {
            style::fg("D", self.config.theme_colors.delete_mark)
        } else if self.tagged.contains(&msg.id) {
            style::fg("\u{2022}", self.config.theme_colors.tag)
        } else if msg.starred {
            style::fg("\u{2605}", self.config.theme_colors.star)
        } else if !msg.attachments.is_empty() {
            style::fg("\u{208A}", self.config.theme_colors.attach_ind)
        } else {
            " ".to_string()
        };

        // Date
        let date_str = format_timestamp(msg.timestamp, &self.date_format);
        let date_padded = format!("{:>6}", &date_str[..date_str.len().min(6)]);

        // Source icon and color
        let stype = &msg.source_type;
        let (icon, scolor) = source_info(stype, &self.config.theme_colors);

        // Sender (15 chars max)
        let sender_display = msg.sender_name.as_deref().unwrap_or(&msg.sender);
        let sender_truncated = truncate_str(sender_display, 14);
        let sender_padded = format!("{:<14} ", sender_truncated); // 14 chars + always 1 space gap

        // Subject fills remaining width (decode RFC 2047 encoded-words)
        let raw_subject = msg.subject.as_deref().unwrap_or("");
        let subject = sources::maildir::decode_rfc2047(raw_subject);
        // Calculate available width for subject
        // "N r I DDDDDD i sender          subject"
        // 1+1+1+1+6+1+1+1+15+1 = 29 fixed chars
        let fixed = 29;
        let subj_w = pane_w.saturating_sub(fixed);
        let subject_truncated = truncate_str(&subject, subj_w);

        let flags = format!("{}{}{}", nflag, rflag, ind);

        // Build content as PLAIN text (no ANSI), color applied in styling step below
        let content = format!("{} {} {}{}", date_padded, icon, sender_padded, subject_truncated);

        // Pad to full width
        let flags_w = crust::display_width(&flags);
        let content_w = crust::display_width(&content);
        let padding = if pane_w > flags_w + content_w + 1 {
            " ".repeat(pane_w - flags_w - content_w - 1)
        } else {
            String::new()
        };
        let full_content = format!("{}{}", content, padding);

        // Apply styling: single color on the full content (no nested ANSI)
        let color = if self.delete_marked.contains(&msg.id) {
            self.config.theme_colors.delete_mark
        } else if self.tagged.contains(&msg.id) {
            self.config.theme_colors.tag
        } else if msg.starred {
            self.config.theme_colors.star
        } else {
            scolor
        };

        if selected {
            format!("{}{}{}", flags, style::underline(&style::bold(&style::fg(&content, color))), style::bold(&style::fg(&padding, color)))
        } else if !msg.read {
            format!("{}{}", flags, style::bold(&style::fg(&full_content, color)))
        } else {
            format!("{}{}", flags, style::fg(&full_content, color))
        }
    }

    fn render_message_content(&mut self) {
        // Auto-mark as read when displayed in right pane
        let msg_ref = if self.show_threaded {
            self.display_messages.get(self.index)
        } else {
            self.filtered_messages.get(self.index)
        };
        if let Some(msg) = msg_ref {
            // Clear unseen protection if user navigated away and came back
            if self.unseen_ids.contains(&msg.id) && self.right_pane_msg_id != Some(msg.id) {
                self.unseen_ids.remove(&msg.id);
            }
            if !msg.read && !msg.is_header && msg.id > 0 && !self.unseen_ids.contains(&msg.id) {
                let id = msg.id;
                let metadata = msg.metadata.clone();
                // Fire-and-forget: DB write + maildir flag sync on background thread
                let _ = self.write_tx.send(DbWriteOp::MarkRead(id));
                let _ = self.write_tx.send(DbWriteOp::SyncMaildirFlag(metadata, id));
                self.browsed_ids.insert(id);
                // Update in-memory state
                if let Some(m) = self.filtered_messages.iter_mut().find(|m| m.id == id) {
                    m.read = true;
                }
                if self.show_threaded {
                    if let Some(m) = self.display_messages.get_mut(self.index) {
                        m.read = true;
                    }
                }
                self.stats_cache = None; // Invalidate stats
                // Update left pane and top bar to reflect read status
                self.render_message_list();
                self.render_top_bar();
            }
        }

        let messages = if self.show_threaded {
            &self.display_messages
        } else {
            &self.filtered_messages
        };
        if messages.is_empty() {
            self.right.set_text("");
            self.right.ix = 0;
            self.right.full_refresh();
            return;
        }

        // In threaded mode, if current item is a header, show section info
        if self.show_threaded {
            if let Some(m) = messages.get(self.index) {
                if m.is_header {
                    let tc = &self.config.theme_colors;
                    let subj = m.subject.as_deref().unwrap_or("Section");
                    let mut lines = Vec::new();
                    lines.push(style::bold(&style::fg(subj, tc.thread)));
                    lines.push(String::new());
                    lines.push(format!("{} {}", style::fg("Messages:", tc.header_date), m.content));
                    let (_, m_scolor) = source_info(&m.source_type, tc);
                    lines.push(format!("{} {}", style::fg("Type:", tc.header_date),
                        style::fg(&m.source_type, m_scolor)));
                    let is_collapsed = m.thread_id.as_ref()
                        .and_then(|name| self.section_collapsed.get(name))
                        .copied()
                        .unwrap_or(false);
                    lines.push(format!("{} {}", style::fg("State:", tc.header_date),
                        if is_collapsed { "Collapsed" } else { "Expanded" }));
                    lines.push(String::new());
                    lines.push(style::fg("ENTER/Space: Toggle collapse", tc.hint_fg));
                    lines.push(style::fg("h: Collapse", tc.hint_fg));
                    self.right.set_text(&lines.join("\n"));
                    self.right.ix = 0;
                    self.right.full_refresh();
                    if self.right.border { self.right.border_refresh(); }
                    return;
                }
            }
        }

        // Auto-load full content for selected message
        // In threaded mode, display_messages are clones, so load into filtered_messages
        // and re-clone if needed
        if self.show_threaded {
            if let Some(m) = self.display_messages.get(self.index) {
                if !m.full_loaded && m.id != 0 {
                    if let Some((content, html)) = self.db.get_message_content(m.id) {
                        // Update the display copy
                        if let Some(dm) = self.display_messages.get_mut(self.index) {
                            dm.content = content;
                            dm.html_content = html;
                            dm.full_loaded = true;
                        }
                    }
                }
            }
        } else if !self.filtered_messages[self.index].full_loaded {
            let msg_id = self.filtered_messages[self.index].id;
            if let Some((content, html)) = self.db.get_message_content(msg_id) {
                self.filtered_messages[self.index].content = content;
                self.filtered_messages[self.index].html_content = html;
                self.filtered_messages[self.index].full_loaded = true;
            }
        }

        let messages = if self.show_threaded {
            &self.display_messages
        } else {
            &self.filtered_messages
        };
        let msg = &messages[self.index];
        let tc = &self.config.theme_colors;
        let (_, scolor) = source_info(&msg.source_type, tc);

        let mut lines = Vec::new();

        // From
        let from_display = match &msg.sender_name {
            Some(name) => format!("{} <{}>", name, msg.sender),
            None => msg.sender.clone(),
        };
        lines.push(format!("{} {}", style::fg("From:", tc.header_from), style::fg(&from_display, tc.header_from)));

        // To (parse JSON recipients)
        let to_display = parse_json_recipients(&msg.recipients);
        if !to_display.is_empty() {
            lines.push(format!("{} {}", style::fg("To:", tc.header_from), style::fg(&to_display, tc.header_from)));
        }

        // Cc (parse JSON, skip if empty)
        if let Some(ref cc) = msg.cc {
            let cc_display = parse_json_recipients(cc);
            if !cc_display.is_empty() {
                lines.push(format!("{} {}", style::fg("Cc:", tc.header_from), style::fg(&cc_display, tc.header_from)));
            }
        }

        // Subject
        if let Some(ref subj) = msg.subject {
            let decoded_subj = sources::maildir::decode_rfc2047(subj);
            lines.push(format!("{} {}", style::bold(&style::fg("Subject:", tc.header_subj)), style::bold(&style::fg(&decoded_subj, tc.header_subj))));
        }

        // Date
        let full_date = format_timestamp(msg.timestamp, "%Y-%m-%d %H:%M");
        lines.push(format!("{} {}", style::fg("Date:", tc.header_date), style::fg(&full_date, tc.header_date)));

        // Type
        lines.push(format!("{} {}", style::fg("Type:", tc.header_date), style::fg(&msg.source_type, scolor)));

        // Labels
        if !msg.labels.is_empty() {
            let label_str = msg.labels.iter()
                .map(|l| format!("[{}]", l))
                .collect::<Vec<_>>()
                .join(" ");
            lines.push(format!("{} {}", style::fg("Labels:", tc.header_label), style::fg(&label_str, tc.header_label)));
        }

        // Separator
        lines.push(style::fg(&"\u{2500}".repeat(40), tc.separator));

        // Fix 4: Attachments (separate images from regular attachments)
        if !msg.attachments.is_empty() {
            let regular_atts: Vec<_> = msg.attachments.iter()
                .filter(|a| !is_image_attachment(a))
                .collect();
            let image_atts: Vec<_> = msg.attachments.iter()
                .filter(|a| is_image_attachment(a))
                .collect();

            if !regular_atts.is_empty() {
                lines.push(style::bold(&style::fg("Attachments:", tc.attachment)));
                for (i, att) in regular_atts.iter().enumerate() {
                    let fname = att["filename"].as_str()
                        .or_else(|| att["name"].as_str())
                        .unwrap_or("unknown");
                    let size = att["size"].as_u64()
                        .map(|s| format_file_size(s))
                        .unwrap_or_default();
                    let size_part = if size.is_empty() { String::new() } else { format!(" ({})", size) };
                    lines.push(style::fg(&format!("  [{}] {}{}", i + 1, fname, size_part), tc.attachment));
                }
                lines.push(style::fg("  Press 'v' to view/save attachments", tc.attachment));
                lines.push(String::new());
            }

            if !image_atts.is_empty() {
                let label = if image_atts.len() == 1 { "1 image".to_string() } else { format!("{} images", image_atts.len()) };
                lines.push(style::fg(&format!("{}, press V to view", label), tc.feedback_ok));
                lines.push(String::new());
            }
        }

        // Count images from HTML content too (when no image attachments)
        let has_image_atts = !msg.attachments.is_empty() && msg.attachments.iter().any(|a| is_image_attachment(a));
        if !has_image_atts {
            let html = msg.html_content.as_deref()
                .or_else(|| if msg.content.trim_start().starts_with('<') { Some(msg.content.as_str()) } else { None });
            if let Some(html) = html {
                let html_img_count = extract_image_urls(html).iter()
                    .filter(|u| u.starts_with("http"))
                    .count();
                if html_img_count > 0 {
                    let label = if html_img_count == 1 { "1 image".to_string() } else { format!("{} images", html_img_count) };
                    lines.push(style::fg(&format!("{}, press V to view", label), tc.feedback_ok));
                    lines.push(String::new());
                }
            }
        }

        // Detect MIME attachments embedded in raw content (when DB attachments field is empty)
        let mime_atts_to_inject = if msg.attachments.is_empty() && msg.content.contains("Content-Type:") {
            let atts = extract_mime_attachments(&msg.content, msg.id);
            if !atts.is_empty() {
                let regular: Vec<_> = atts.iter().filter(|a| !a["is_image"].as_bool().unwrap_or(false)).collect();
                let images: Vec<_> = atts.iter().filter(|a| a["is_image"].as_bool().unwrap_or(false)).collect();
                if !regular.is_empty() {
                    lines.push(style::bold(&style::fg("Attachments:", tc.attachment)));
                    for (i, att) in regular.iter().enumerate() {
                        let name = att["name"].as_str().unwrap_or("unknown");
                        let size = att["size"].as_u64().map(|s| format_file_size(s)).unwrap_or_default();
                        let size_part = if size.is_empty() { String::new() } else { format!(" ({})", size) };
                        lines.push(style::fg(&format!("  [{}] {}{}", i + 1, name, size_part), tc.attachment));
                    }
                    lines.push(style::fg("  Press 'v' to view/save attachments", tc.attachment));
                    lines.push(String::new());
                }
                if !images.is_empty() {
                    let label = if images.len() == 1 { "1 image".to_string() } else { format!("{} images", images.len()) };
                    lines.push(style::fg(&format!("{}, press V to view", label), tc.feedback_ok));
                    lines.push(String::new());
                }
                Some(atts)
            } else { None }
        } else { None };
        // Inject after msg borrow is done (below, after rendering)

        // HTML indicator
        let has_mime_html = msg.content.contains("Content-Type:") && msg.content.lines().any(|l| l.starts_with("--") && l.len() > 5);
        if msg.html_content.is_some() || has_mime_html {
            lines.push(style::fg("HTML mail, press x to open in browser", tc.html_hint));
            lines.push(String::new());
        }

        lines.push(String::new());

        // Content: extract from MIME, decode QP, detect HTML and parse
        let raw = &msg.content;
        // Try MIME multipart extraction first
        let looks_mime = raw.contains("boundary=")
            || (raw.contains("Content-Type:") && raw.lines().any(|l| l.starts_with("--") && l.len() > 5));
        let extracted = if looks_mime {
            extract_mime_text(raw).unwrap_or_else(|| raw.clone())
        } else if raw.contains("Content-Transfer-Encoding: quoted-printable") {
            // Single-part QP encoded
            let body_start = raw.find("\n\n").map(|p| p + 2)
                .or_else(|| raw.find("\r\n\r\n").map(|p| p + 4))
                .unwrap_or(0);
            decode_quoted_printable(&raw[body_start..])
        } else if looks_base64(raw) {
            // Raw base64 body (no MIME headers)
            sources::maildir::base64_decode(raw.trim())
                .and_then(|bytes| String::from_utf8(bytes).ok()
                    .or_else(|| Some(latin1_to_utf8(&sources::maildir::base64_decode(raw.trim()).unwrap_or_default()))))
                .unwrap_or_else(|| raw.clone())
        } else {
            raw.clone()
        };
        // Decode any remaining QP soft line breaks (=\n) in the extracted text
        let extracted = if extracted.contains("=\n") || extracted.contains("=\r\n") {
            decode_quoted_printable(&extracted)
        } else { extracted };
        let is_html_fallback = {
            let lc = extracted.to_lowercase();
            extracted.trim().is_empty() || lc.contains("html messages are not support")
                || lc.contains("not displayed") || lc.contains("html-e-post")
                || lc.contains("støtter ikke html") || lc.contains("does not support html")
                || extracted.trim().len() < 20
        };
        let content = if let Some(ref html) = msg.html_content {
            if is_html_fallback {
                html_to_text(html)
            } else if extracted.contains("<br") || extracted.contains("<p>") || extracted.contains("<p ") ||
                (extracted.trim_start().starts_with('<') && (extracted.contains("<html") || extracted.contains("<body") || extracted.contains("<div") || extracted.contains("<table"))) {
                html_to_text(&extracted)
            } else {
                extracted
            }
        } else if extracted.contains("<br") || extracted.contains("<p>") || extracted.contains("<p ") ||
            (extracted.trim_start().starts_with('<') && (extracted.contains("<html") || extracted.contains("<body") || extracted.contains("<div") || extracted.contains("<table"))) {
            html_to_text(&extracted)
        } else {
            extracted
        };
        let mut in_signature = false;
        let mut prev_blank = false;
        for line in content.lines() {
            // Collapse consecutive blank lines to at most one
            if line.trim().is_empty() {
                if prev_blank { continue; }
                prev_blank = true;
                lines.push(String::new());
                continue;
            }
            prev_blank = false;

            if line.starts_with("-- ") || line == "--" {
                in_signature = true;
            }
            let linked = hyperlink_urls(line);
            if in_signature {
                lines.push(style::fg(&linked, self.config.theme_colors.sig));
            } else if line.starts_with(">>>>") {
                lines.push(style::fg(&linked, self.config.theme_colors.quote4));
            } else if line.starts_with(">>>") {
                lines.push(style::fg(&linked, self.config.theme_colors.quote3));
            } else if line.starts_with(">>") {
                lines.push(style::fg(&linked, self.config.theme_colors.quote2));
            } else if line.starts_with('>') {
                lines.push(style::fg(&linked, self.config.theme_colors.quote1));
            } else {
                lines.push(linked);
            }
        }

        // Only reset scroll when viewing a different message
        let current_id = self.filtered_messages.get(self.index).map(|m| m.id);
        let msg_changed = current_id != self.right_pane_msg_id;
        self.right_pane_msg_id = current_id;

        self.right.set_text(&lines.join("\n"));
        if msg_changed {
            self.right.ix = 0;
            self.right.full_refresh();
            if self.right.border { self.right.border_refresh(); }
        } else {
            self.right.refresh();
        }

        // Inject MIME attachments into message (deferred to avoid borrow conflict)
        if let Some(atts) = mime_atts_to_inject {
            let idx = self.index;
            let messages = if self.show_threaded { &mut self.display_messages } else { &mut self.filtered_messages };
            if let Some(m) = messages.get_mut(idx) {
                m.attachments = atts;
            }
        }
    }

    fn render_bottom_bar(&mut self) {
        let version = format!("kastrup v{}", env!("CARGO_PKG_VERSION"));
        let tc = &self.config.theme_colors;
        let left = if let Some((ref msg, color)) = self.feedback_message {
            format!(" {}", style::fg(msg, color))
        } else {
            style::fg(
                " q:Quit | ?:Help | A:All | N:New | 0-9:Views | Space:Fold | t:Tag | T:All | s:Save | B:Browse | F:Fav",
                tc.hint_fg
            )
        };
        let left_w = crust::display_width(&left);
        let ver_w = version.len();
        let pad = (self.cols as usize).saturating_sub(left_w + ver_w + 1);
        self.bottom.say(&format!("{}{}{}", left, " ".repeat(pad), style::fg(&version, tc.hint_fg)));
    }

    // --- Source view rendering ---

    fn render_source_list(&mut self) {
        if self.sources_list.is_empty() {
            self.left.set_text(&style::fg("  No sources configured", self.config.theme_colors.no_msg));
            self.left.ix = 0;
            self.left.full_refresh();
            return;
        }

        let stats = self.db.get_source_stats();
        let mut lines = Vec::new();

        for (i, src) in self.sources_list.iter().enumerate() {
            let selected = i == self.index;
            let (icon, scolor) = source_info(&src.plugin_type, &self.config.theme_colors);
            let (total, unread) = stats.get(&src.id).copied().unwrap_or((0, 0));

            let enabled_mark = if src.enabled { " " } else { "x" };
            let unread_mark = if unread > 0 {
                style::fg(&format!(" ({})", unread), self.config.theme_colors.unread)
            } else {
                String::new()
            };

            let line_content = format!(" {} {} {} [{}/{}]{}",
                enabled_mark, icon, src.name, unread, total, unread_mark
            );

            if selected {
                lines.push(style::underline(&style::bold(&style::fg(&line_content, scolor))));
            } else {
                lines.push(style::fg(&line_content, scolor));
            }
        }

        self.left.set_text(&lines.join("\n"));
        self.left.ix = 0;
        self.left.full_refresh();
    }

    fn render_source_info(&mut self) {
        if self.sources_list.is_empty() {
            self.right.set_text("");
            self.right.ix = 0;
            self.right.full_refresh();
            return;
        }

        let src = &self.sources_list[self.index];
        let (_, scolor) = source_info(&src.plugin_type, &self.config.theme_colors);
        let stats = self.db.get_source_stats();
        let (total, unread) = stats.get(&src.id).copied().unwrap_or((0, 0));

        let tc = &self.config.theme_colors;
        let mut lines = Vec::new();
        lines.push(style::bold(&style::fg(&src.name, scolor)));
        lines.push(String::new());
        lines.push(format!("{} {}", style::fg("Type:", tc.header_date), style::fg(&src.plugin_type, scolor)));
        lines.push(format!("{} {}", style::fg("Enabled:", tc.header_date),
            if src.enabled { style::fg("yes", tc.feedback_ok) } else { style::fg("no", tc.delete_mark) }
        ));
        lines.push(format!("{} {} ({} unread)", style::fg("Messages:", tc.header_date), total, unread));
        lines.push(format!("{} {}s", style::fg("Poll interval:", tc.header_date), src.poll_interval));

        if let Some(ref ts) = src.last_sync {
            lines.push(format!("{} {}", style::fg("Last sync:", tc.header_date),
                format_timestamp(*ts, "%Y-%m-%d %H:%M")));
        }
        if let Some(ref err) = src.last_error {
            lines.push(format!("{} {}", style::fg("Last error:", 196), style::fg(err, 196)));
        }

        lines.push(String::new());
        lines.push(style::fg("Press ENTER to view messages from this source", tc.hint_fg));
        lines.push(style::fg("Press ESC to return to message view", tc.hint_fg));

        self.right.set_text(&lines.join("\n"));
        self.right.ix = 0;
        self.right.full_refresh();
    }
}

// --- Navigation ---

impl App {
    fn unlock_right_pane(&mut self) {
        self.right_pane_locked = false;
        self.showing_help = false;
        self.help_extended = false;
    }

    fn move_down(&mut self) {
        let limit = if self.in_source_view {
            self.sources_list.len()
        } else if self.show_threaded {
            self.display_messages.len()
        } else {
            self.filtered_messages.len()
        };
        if limit == 0 { return; }
        if self.index < limit - 1 {
            self.index += 1;
        } else {
            self.index = 0; // Wrap around
        }
        self.unlock_right_pane();
        if self.in_source_view {
            self.render_source_list();
            self.render_source_info();
        } else {
            self.render_message_list();
            self.render_message_content();
        }
    }

    fn move_up(&mut self) {
        if self.index > 0 {
            self.index -= 1;
        } else {
            // Wrap around
            let limit = if self.in_source_view {
                self.sources_list.len()
            } else if self.show_threaded {
                self.display_messages.len()
            } else {
                self.filtered_messages.len()
            };
            if limit > 0 { self.index = limit - 1; }
        }
        self.unlock_right_pane();
        if self.in_source_view {
            self.render_source_list();
            self.render_source_info();
        } else {
            self.render_message_list();
            self.render_message_content();
        }
    }

    fn go_first(&mut self) {
        self.index = 0;
        self.render_all();
    }

    fn go_last(&mut self) {
        let len = if self.show_threaded { self.display_messages.len() } else { self.filtered_messages.len() };
        self.index = len.saturating_sub(1);
        self.render_all();
    }

    fn page_down(&mut self) {
        let page = self.left.h as usize;
        let len = if self.show_threaded { self.display_messages.len() } else { self.filtered_messages.len() };
        self.index = (self.index + page).min(len.saturating_sub(1));
        self.render_all();
    }

    fn page_up(&mut self) {
        let page = self.left.h as usize;
        self.index = self.index.saturating_sub(page);
        self.render_all();
    }

    fn next_unread(&mut self) {
        let start = self.index + 1;
        for i in start..self.filtered_messages.len() {
            if !self.filtered_messages[i].read {
                self.index = i;
                self.render_all();
                return;
            }
        }
        self.set_feedback("No more unread messages", self.config.theme_colors.feedback_info);
    }

    fn prev_unread(&mut self) {
        if self.index == 0 { return; }
        for i in (0..self.index).rev() {
            if !self.filtered_messages[i].read {
                self.index = i;
                self.render_all();
                return;
            }
        }
        self.set_feedback("No previous unread message", self.config.theme_colors.feedback_info);
    }
}

// --- View switching ---

impl App {
    fn switch_to_view(&mut self, key: &str) {
        log::info(&format!("Switch to view: {}", key));
        self.current_view = key.to_string();
        self.active_folder = None;
        self.in_source_view = false;
        self.index = 0;

        // Restore per-view thread mode from DB settings
        let mode_key = format!("thread_mode_{}", key);
        match self.db.get_setting(&mode_key).as_deref() {
            Some("threaded") => { self.show_threaded = true; self.group_by_folder = false; }
            Some("folders") => { self.show_threaded = true; self.group_by_folder = true; }
            _ => { self.show_threaded = false; self.group_by_folder = false; } // "flat" or unset
        }

        let mut filters = Filters::default();

        match key {
            "A" | "N" | "*" => {
                // Built-in views: reset top_bg to default
                self.top.bg = self.config.theme_colors.top_bg;
                match key {
                    "N" => { filters.is_read = Some(false); }
                    "*" => { filters.is_starred = Some(true); }
                    _ => {} // "A" = no filters
                }
            }
            _ => {
                // Reset top_bg to default first, then override if view specifies
                self.top.bg = self.config.theme_colors.top_bg;

                // Check custom views from DB
                if let Some(view) = self.views.iter().find(|v| v.key_binding.as_deref() == Some(key)) {
                    if let Ok(f) = serde_json::from_str::<serde_json::Value>(&view.filters) {
                        if let Some(rules) = f["rules"].as_array() {
                            for rule in rules {
                                let field = rule["field"].as_str().unwrap_or("");
                                let value = &rule["value"];
                                match field {
                                    "read" => {
                                        filters.is_read = Some(!value.as_bool().unwrap_or(true));
                                    }
                                    "starred" => {
                                        filters.is_starred = value.as_bool();
                                    }
                                    "folder" => {
                                        filters.folder = value.as_str().map(|s| s.to_string());
                                    }
                                    "source_id" => {
                                        filters.source_id = value.as_i64();
                                    }
                                    "sender" => {
                                        filters.sender_pattern = value.as_str().map(|s| s.to_string());
                                    }
                                    "source_type" => {
                                        filters.source_type = value.as_str().map(|s| s.to_string());
                                    }
                                    _ => {}
                                }
                            }
                        }
                        // Per-view sort settings
                        if let Some(so) = f["view_sort_order"].as_str() {
                            self.sort_order = so.to_string();
                        }
                        if let Some(si) = f["view_sort_inverted"].as_bool() {
                            self.sort_inverted = si;
                        }
                        // Per-view top bar background color
                        if let Some(bg) = f["top_bg"].as_str() {
                            if let Ok(v) = bg.parse::<u16>() {
                                self.top.bg = v;
                            }
                        } else if let Some(bg) = f["top_bg"].as_u64() {
                            self.top.bg = bg as u16;
                        }
                    }
                }
            }
        }

        let limit = self.config.load_limit;
        self.filtered_messages = self.db.get_messages(&filters, limit, 0);
        // Populate source_type for each message
        for msg in &mut self.filtered_messages {
            if let Some(st) = self.source_type_map.get(&msg.source_id) {
                msg.source_type = st.clone();
            }
        }
        self.sort_messages();
        self.rebuild_display();
        self.left.full_refresh();
        self.right.full_refresh();
        self.render_all();
    }

    /// Reload messages for current view without resetting cursor position.
    fn refresh_current_view(&mut self) {
        let saved_id = self.filtered_messages.get(self.index).map(|m| m.id);
        let saved_index = self.index;
        let old_ids: Vec<i64> = self.filtered_messages.iter().map(|m| m.id).collect();
        let old_read: Vec<bool> = self.filtered_messages.iter().map(|m| m.read).collect();

        // Rebuild filters for the current view (same logic as switch_to_view but no index=0)
        let key = self.current_view.clone();
        let mut filters = Filters::default();
        match key.as_str() {
            "N" => { filters.is_read = Some(false); }
            "*" => { filters.is_starred = Some(true); }
            "A" => {
                // Preserve active folder filter (from folder browser)
                if let Some(ref folder) = self.active_folder {
                    filters.folder = Some(folder.clone());
                }
            }
            _ => {
                if let Some(view) = self.views.iter().find(|v| v.key_binding.as_deref() == Some(&key)) {
                    if let Ok(f) = serde_json::from_str::<serde_json::Value>(&view.filters) {
                        if let Some(rules) = f["rules"].as_array() {
                            for rule in rules {
                                let field = rule["field"].as_str().unwrap_or("");
                                let value = &rule["value"];
                                match field {
                                    "read" => { filters.is_read = Some(!value.as_bool().unwrap_or(true)); }
                                    "starred" => { filters.is_starred = value.as_bool(); }
                                    "folder" => { filters.folder = value.as_str().map(|s| s.to_string()); }
                                    "source_id" => { filters.source_id = value.as_i64(); }
                                    "sender" => { filters.sender_pattern = value.as_str().map(|s| s.to_string()); }
                                    "source_type" => { filters.source_type = value.as_str().map(|s| s.to_string()); }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        let limit = self.config.load_limit;
        self.filtered_messages = self.db.get_messages(&filters, limit, 0);
        for msg in &mut self.filtered_messages {
            if let Some(st) = self.source_type_map.get(&msg.source_id) {
                msg.source_type = st.clone();
            }
        }
        self.sort_messages();
        self.rebuild_display();

        // Restore position by message ID, fall back to saved index
        if let Some(id) = saved_id {
            if let Some(pos) = self.filtered_messages.iter().position(|m| m.id == id) {
                self.index = pos;
            } else {
                self.index = saved_index.min(self.filtered_messages.len().saturating_sub(1));
            }
        } else {
            self.index = saved_index.min(self.filtered_messages.len().saturating_sub(1));
        }

        // Skip render if nothing changed (avoids flicker on periodic refresh)
        let new_ids: Vec<i64> = self.filtered_messages.iter().map(|m| m.id).collect();
        let new_read: Vec<bool> = self.filtered_messages.iter().map(|m| m.read).collect();
        if new_ids == old_ids && new_read == old_read {
            return;
        }

        self.stats_cache = None;
        self.render_all();
    }

    fn show_sources(&mut self) {
        self.in_source_view = true;
        self.current_view = "S".to_string();
        self.index = 0;
        self.sources_list = self.db.get_sources(false);
        self.render_all();
    }
}

// --- Folder browser ---

impl App {
    fn show_folder_browser(&mut self) {
        self.folder_count_cache = self.db.all_folder_counts();
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let maildir_path = std::path::PathBuf::from(&home).join("Main/Maildir");
        let folder_names = discover_maildir_folders(&maildir_path);

        let tree = build_folder_tree(&folder_names);
        let mut display = flatten_folder_tree(&tree, "", 0, &self.folder_collapsed);

        if display.is_empty() {
            self.set_feedback("No maildir folders found", self.config.theme_colors.feedback_warn);
            return;
        }

        let result = self.folder_browser_loop(&mut display, false);

        if let Some(folder) = result {
            self.open_folder(&folder);
        } else {
            self.render_all();
        }
    }

    fn show_favorites_browser(&mut self) {
        // Prefill counts cache in a single grouped DB query instead of
        // one-per-folder as the user scrolls.
        self.folder_count_cache = self.db.all_folder_counts();
        let favorites = self.db.get_favorite_folders();
        if favorites.is_empty() {
            self.set_feedback(
                "No favorite folders. Use + in folder browser to add.",
                self.config.theme_colors.feedback_warn,
            );
            return;
        }

        let tree = build_folder_tree(&favorites);
        let mut display = flatten_folder_tree(&tree, "", 0, &self.folder_collapsed);

        let result = self.folder_browser_loop(&mut display, true);

        if let Some(folder) = result {
            self.open_folder(&folder);
        } else {
            self.render_all();
        }
    }

    fn folder_browser_loop(
        &mut self,
        display: &mut Vec<FolderEntry>,
        is_favorites: bool,
    ) -> Option<String> {
        let mut idx = 0usize;
        let favorites = self.db.get_favorite_folders();
        let mut fav_set: HashSet<String> = favorites.into_iter().collect();
        let tc = self.config.theme_colors.clone();

        loop {
            if display.is_empty() {
                self.set_feedback("No folders to display", tc.feedback_warn);
                return None;
            }

            // Render left pane
            let h = self.left.h as usize;
            let mut lines = Vec::new();
            for (i, f) in display.iter().enumerate() {
                let indent = "  ".repeat(f.depth);
                let arrow = if f.has_children {
                    if f.collapsed {
                        style::fg("\u{25B8} ", tc.hint_fg)
                    } else {
                        style::fg("\u{25BE} ", tc.hint_fg)
                    }
                } else {
                    "  ".to_string()
                };
                let star = if fav_set.contains(&f.full_name) {
                    style::fg("* ", tc.star)
                } else {
                    "  ".to_string()
                };

                if i == idx {
                    lines.push(format!(
                        "{}{}{}{}{}",
                        style::fg("\u{2192} ", tc.unread),
                        indent,
                        arrow,
                        star,
                        style::underline(&style::bold(&style::fg(&f.name, 255)))
                    ));
                } else {
                    lines.push(format!(
                        "  {}{}{}{}",
                        indent,
                        arrow,
                        star,
                        style::fg(&f.name, tc.hint_fg)
                    ));
                }
            }

            // Scrolloff
            let total = display.len();
            let scrolloff = 3usize;
            let mut start = self.left.ix;
            if total <= h {
                start = 0;
            } else if idx < start + scrolloff {
                start = idx.saturating_sub(scrolloff);
            } else if idx + scrolloff >= start + h {
                start = (idx + scrolloff + 1)
                    .saturating_sub(h)
                    .min(total.saturating_sub(h));
            }

            self.left.set_text(&lines.join("\n"));
            self.left.ix = start;
            self.left.full_refresh();
            if self.left.border {
                self.left.border_refresh();
            }

            // Render right pane: folder info
            if let Some(f) = display.get(idx) {
                let (total_msgs, unread) = self
                    .folder_count_cache
                    .entry(f.full_name.clone())
                    .or_insert_with(|| self.db.folder_message_count(&f.full_name))
                    .clone();
                let mut info = Vec::new();
                info.push(style::bold(&style::fg(
                    &format!("FOLDER: {}", f.full_name),
                    tc.unread,
                )));
                info.push(String::new());
                info.push(format!(
                    "{} {}",
                    style::fg("Messages:", tc.src_email),
                    style::fg(&total_msgs.to_string(), tc.src_email)
                ));
                let unread_color = if unread > 0 { tc.attachment } else { tc.hint_fg };
                info.push(format!(
                    "{} {}",
                    style::fg("Unread:", unread_color),
                    style::fg(&unread.to_string(), unread_color)
                ));
                info.push(String::new());
                info.push(style::fg("Enter/l: Open folder", tc.hint_fg));
                info.push(style::fg("h/l: Collapse/Expand", tc.hint_fg));
                info.push(style::fg("Space: Toggle collapse", tc.hint_fg));
                info.push(style::fg("+: Toggle favorite", tc.hint_fg));
                info.push(style::fg("F: Switch to favorites", tc.hint_fg));
                info.push(style::fg("ESC/q: Return", tc.hint_fg));
                self.right.set_text(&info.join("\n"));
                self.right.ix = 0;
                self.right.full_refresh();
                if self.right.border {
                    self.right.border_refresh();
                }
            }

            // Top bar
            let title = if is_favorites { "Favorites" } else { "Folder Browser" };
            let title_color = if is_favorites { tc.unread } else { tc.view_sources };
            self.top.say(&format!(
                "{}{}{}",
                style::fg(" Kastrup - ", tc.prefix_fg),
                style::bold(&style::fg(title, title_color)),
                style::fg(&format!(" [{} folders]", display.len()), tc.hint_fg),
            ));

            // Bottom bar
            self.bottom.say(&style::fg(
                " j/k:Navigate | Enter/l:Open | h:Collapse | Space:Toggle | F:Favorites | +:Fav | ESC:Back",
                tc.hint_fg,
            ));

            // Input
            let Some(key) = Input::getchr(None) else {
                continue;
            };
            match key.as_str() {
                "j" | "DOWN" => {
                    if !display.is_empty() {
                        idx = (idx + 1) % display.len();
                    }
                }
                "k" | "UP" => {
                    if !display.is_empty() {
                        idx = if idx == 0 {
                            display.len() - 1
                        } else {
                            idx - 1
                        };
                    }
                }
                "PgDOWN" => {
                    idx = (idx + h.saturating_sub(2)).min(display.len().saturating_sub(1));
                }
                "PgUP" => {
                    idx = idx.saturating_sub(h.saturating_sub(2));
                }
                "HOME" => {
                    idx = 0;
                }
                "END" => {
                    idx = display.len().saturating_sub(1);
                }
                "ENTER" | "l" | "RIGHT" => {
                    if let Some(f) = display.get(idx) {
                        return Some(f.full_name.clone());
                    }
                }
                "h" | "LEFT" => {
                    if let Some(f) = display.get(idx) {
                        if f.has_children && !f.collapsed {
                            self.folder_collapsed.insert(f.full_name.clone(), true);
                            self.rebuild_folder_display(display, is_favorites);
                            idx = idx.min(display.len().saturating_sub(1));
                        } else if f.depth > 0 {
                            // Go to parent
                            let parent = f
                                .full_name
                                .rsplitn(2, '.')
                                .nth(1)
                                .unwrap_or("")
                                .to_string();
                            if let Some(pi) = display.iter().position(|e| e.full_name == parent) {
                                idx = pi;
                            }
                        }
                    }
                }
                " " | "SPACE" => {
                    if let Some(f) = display.get(idx) {
                        if f.has_children {
                            if f.collapsed {
                                self.folder_collapsed.remove(&f.full_name);
                            } else {
                                self.folder_collapsed.insert(f.full_name.clone(), true);
                            }
                            self.rebuild_folder_display(display, is_favorites);
                            idx = idx.min(display.len().saturating_sub(1));
                        }
                    }
                }
                "+" => {
                    if let Some(f) = display.get(idx) {
                        let fname = f.full_name.clone();
                        let mut favs = self.db.get_favorite_folders();
                        if favs.contains(&fname) {
                            favs.retain(|x| x != &fname);
                            fav_set.remove(&fname);
                            self.set_feedback(
                                &format!("Removed {} from favorites", fname),
                                tc.feedback_ok,
                            );
                        } else {
                            favs.push(fname.clone());
                            fav_set.insert(fname.clone());
                            self.set_feedback(
                                &format!("Added {} to favorites", fname),
                                tc.feedback_ok,
                            );
                        }
                        self.db.save_favorite_folders(&favs);
                    }
                }
                "F" => {
                    if !is_favorites {
                        let favs = self.db.get_favorite_folders();
                        if favs.is_empty() {
                            self.set_feedback("No favorites", tc.feedback_warn);
                        } else {
                            let tree = build_folder_tree(&favs);
                            *display = flatten_folder_tree(&tree, "", 0, &self.folder_collapsed);
                            idx = 0;
                        }
                    }
                }
                "ESC" | "q" => {
                    return None;
                }
                "RESIZE" => {
                    self.handle_resize();
                }
                _ => {}
            }
        }
    }

    fn rebuild_folder_display(&self, display: &mut Vec<FolderEntry>, is_favorites: bool) {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let maildir_path = std::path::PathBuf::from(&home).join("Main/Maildir");
        let mut folder_names = discover_maildir_folders(&maildir_path);
        if is_favorites {
            let favs: HashSet<String> = self.db.get_favorite_folders().into_iter().collect();
            folder_names.retain(|f| favs.contains(f));
        }
        let tree = build_folder_tree(&folder_names);
        *display = flatten_folder_tree(&tree, "", 0, &self.folder_collapsed);
    }

    fn open_folder(&mut self, folder: &str) {
        self.current_view = "A".to_string();
        self.active_folder = Some(folder.to_string());
        self.in_source_view = false;
        self.index = 0;

        self.set_feedback(
            &format!("Loading {}...", folder),
            self.config.theme_colors.unread,
        );

        let mut filters = Filters::default();
        filters.folder = Some(folder.to_string());
        self.filtered_messages = self.db.get_messages(&filters, 500, 0);
        for msg in &mut self.filtered_messages {
            if let Some(st) = self.source_type_map.get(&msg.source_id) {
                msg.source_type = st.clone();
            }
        }
        self.sort_messages();
        self.rebuild_display();

        // Check if any custom view matches this folder and has a top_bg color
        self.top.bg = self.config.theme_colors.top_bg;
        for view in &self.views {
            if let Ok(f) = serde_json::from_str::<serde_json::Value>(&view.filters) {
                let matches = f["rules"].as_array().map(|rules| {
                    rules.iter().any(|r| {
                        r["field"].as_str() == Some("folder")
                            && r["value"].as_str().map(|v| folder.starts_with(v)).unwrap_or(false)
                    })
                }).unwrap_or(false);
                if matches {
                    if let Some(bg) = f["top_bg"].as_str().and_then(|s| s.parse::<u16>().ok()) {
                        self.top.bg = bg;
                    } else if let Some(bg) = f["top_bg"].as_u64() {
                        self.top.bg = bg as u16;
                    }
                    break;
                }
            }
        }

        self.set_feedback(
            &format!("Folder: {} ({} messages)", folder, self.filtered_messages.len()),
            self.config.theme_colors.feedback_ok,
        );
        self.render_all();
    }
}

// --- Sorting ---

impl App {
    fn sort_messages(&mut self) {
        match self.sort_order.as_str() {
            "latest" => {
                self.filtered_messages.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            }
            "alphabetical" => {
                self.filtered_messages.sort_by(|a, b| {
                    let sa = a.subject.as_deref().unwrap_or("");
                    let sb = b.subject.as_deref().unwrap_or("");
                    sa.to_lowercase().cmp(&sb.to_lowercase())
                });
            }
            "sender" | "from" => {
                self.filtered_messages.sort_by(|a, b| {
                    a.sender.to_lowercase().cmp(&b.sender.to_lowercase())
                });
            }
            "unread" => {
                self.filtered_messages.sort_by(|a, b| {
                    a.read.cmp(&b.read).then(b.timestamp.cmp(&a.timestamp))
                });
            }
            "source" => {
                self.filtered_messages.sort_by(|a, b| {
                    a.source_type.cmp(&b.source_type).then(b.timestamp.cmp(&a.timestamp))
                });
            }
            _ => {
                self.filtered_messages.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            }
        }
        if self.sort_inverted {
            self.filtered_messages.reverse();
        }
    }

    fn cycle_sort(&mut self) {
        let orders = ["latest", "alphabetical", "sender", "from", "unread", "source"];
        let idx = orders.iter().position(|&o| o == self.sort_order).unwrap_or(0);
        self.sort_order = orders[(idx + 1) % orders.len()].to_string();
        self.set_feedback(&format!("Sort: {}", self.sort_order), self.config.theme_colors.info_fg);
        self.sort_messages();
        self.rebuild_display();
        self.render_all();
    }

    fn toggle_sort_invert(&mut self) {
        self.sort_inverted = !self.sort_inverted;
        let label = if self.sort_inverted { "inverted" } else { "normal" };
        self.set_feedback(&format!("Sort direction: {}", label), self.config.theme_colors.info_fg);
        self.sort_messages();
        self.rebuild_display();
        self.render_all();
    }
}

// --- Threading ---

impl App {
    fn cycle_view_mode(&mut self) {
        let tc = &self.config.theme_colors;
        if !self.show_threaded && !self.group_by_folder {
            self.show_threaded = true;
            self.group_by_folder = false;
            self.set_feedback("View mode: Threaded", tc.feedback_ok);
        } else if self.show_threaded && !self.group_by_folder {
            self.group_by_folder = true;
            self.set_feedback("View mode: Folder-grouped", tc.feedback_ok);
        } else {
            self.show_threaded = false;
            self.group_by_folder = false;
            self.set_feedback("View mode: Flat", tc.feedback_ok);
        }
        // Persist per-view thread mode
        let mode = if self.group_by_folder { "folders" } else if self.show_threaded { "threaded" } else { "flat" };
        let mode_key = format!("thread_mode_{}", self.current_view);
        self.db.set_setting(&mode_key, mode);
        self.index = 0;
        self.rebuild_display();
        self.render_all();
    }

    fn rebuild_display(&mut self) {
        if !self.show_threaded {
            self.display_messages.clear();
            return;
        }
        let sections = if self.group_by_folder {
            organizer::organize_by_folder(&self.filtered_messages, self.sort_inverted)
        } else {
            organizer::organize_messages(&self.filtered_messages, &self.sort_order, self.sort_inverted)
        };

        self.display_messages.clear();
        for section in &sections {
            let is_collapsed = self.section_collapsed.get(&section.name).copied().unwrap_or(false);
            let mut header = Message::default_header();
            header.subject = Some(section.display_name.clone());
            header.content = format!("{} messages", section.messages.len());
            header.source_type = section.source_type.clone();
            header.is_header = true;
            header.read = section.unread_count == 0;
            // Store section name in thread_id for collapse tracking
            header.thread_id = Some(section.name.clone());
            if let Some(first_idx) = section.messages.first() {
                header.timestamp = self.filtered_messages[*first_idx].timestamp;
                header.source_id = self.filtered_messages[*first_idx].source_id;
            }
            self.display_messages.push(header);

            if !is_collapsed {
                for &idx in &section.messages {
                    let mut msg = self.filtered_messages[idx].clone();
                    // Don't clone heavy fields for display list (loaded on demand)
                    msg.content = String::new();
                    msg.html_content = None;
                    msg.metadata = serde_json::Value::Null;
                    msg.full_loaded = false;
                    self.display_messages.push(msg);
                }
            }
        }
    }

    fn toggle_collapse(&mut self) {
        if !self.show_threaded { return; }
        if let Some(msg) = self.display_messages.get(self.index) {
            if msg.is_header {
                if let Some(ref name) = msg.thread_id {
                    let name = name.clone();
                    let collapsed = self.section_collapsed.entry(name).or_insert(false);
                    *collapsed = !*collapsed;
                    self.rebuild_display();
                    self.render_all();
                }
            }
        }
    }

    fn collapse_current(&mut self) {
        if !self.show_threaded { return; }
        if let Some(msg) = self.display_messages.get(self.index) {
            if msg.is_header {
                if let Some(ref name) = msg.thread_id {
                    let name = name.clone();
                    self.section_collapsed.insert(name, true);
                    self.rebuild_display();
                    self.render_all();
                }
            }
        }
    }
}

// --- Message operations ---

impl App {
    fn open_message(&mut self) {
        if self.show_threaded {
            if let Some(msg) = self.display_messages.get_mut(self.index) {
                if msg.is_header { return; }
                self.browsed_ids.insert(msg.id);
                if !msg.read {
                    self.db.mark_as_read(msg.id);
                    msg.read = true;
                    // Also mark in filtered_messages
                    if let Some(fm) = self.filtered_messages.iter_mut().find(|m| m.id == msg.id) {
                        fm.read = true;
                    }
                }
                if !msg.full_loaded {
                    if let Some((content, html)) = self.db.get_message_content(msg.id) {
                        msg.content = content;
                        msg.html_content = html;
                        msg.full_loaded = true;
                    }
                }
            }
        } else if let Some(msg) = self.filtered_messages.get_mut(self.index) {
            self.browsed_ids.insert(msg.id);
            if !msg.read {
                self.db.mark_as_read(msg.id);
                msg.read = true;
            }
            if !msg.full_loaded {
                if let Some((content, html)) = self.db.get_message_content(msg.id) {
                    msg.content = content;
                    msg.html_content = html;
                    msg.full_loaded = true;
                }
            }
        }
        self.render_all();
    }

    fn toggle_read(&mut self) {
        // If any messages are tagged, operate on the tagged set (same idiom
        // kastrup already uses for forward/delete). This gives "T then R" as
        // bulk "mark-all-in-view-as-read": tag-all with T, then R decides
        // the new state from whether ALL tagged are already read.
        if !self.tagged.is_empty() {
            let tagged_ids: Vec<i64> = self.tagged.iter().copied().collect();
            let tagged_set: std::collections::HashSet<i64> = self.tagged.iter().copied().collect();
            // Flip direction: if all tagged are already read, mark unread;
            // otherwise mark read.
            let all_read = self.filtered_messages.iter()
                .filter(|m| tagged_set.contains(&m.id))
                .all(|m| m.read);
            let new_state = !all_read;
            for id in &tagged_ids {
                if new_state {
                    self.db.mark_as_read(*id);
                } else {
                    self.db.mark_as_unread(*id);
                }
            }
            for m in &mut self.filtered_messages {
                if tagged_set.contains(&m.id) { m.read = new_state; }
            }
            let label = if new_state { "read" } else { "unread" };
            self.set_feedback(
                &format!("Marked {} tagged as {}", tagged_ids.len(), label),
                self.config.theme_colors.feedback_ok);
            self.render_all();
            return;
        }
        if let Some(msg) = self.filtered_messages.get_mut(self.index) {
            let new_state = self.db.toggle_read(msg.id);
            msg.read = new_state;
            self.render_all();
        }
    }

    fn mark_all_read(&mut self) {
        // Build filters matching current view
        let filters = self.current_view_filters();
        self.db.mark_all_as_read(filters.as_ref());
        for msg in &mut self.filtered_messages {
            msg.read = true;
        }
        self.set_feedback("Marked all as read", self.config.theme_colors.feedback_ok);
        self.render_all();
    }

    fn toggle_star(&mut self) {
        if let Some(msg) = self.filtered_messages.get_mut(self.index) {
            let new_state = self.db.toggle_star(msg.id);
            msg.starred = new_state;
            self.render_all();
        }
    }

    fn toggle_tag(&mut self) {
        if let Some(msg) = self.filtered_messages.get(self.index) {
            let id = msg.id;
            if self.tagged.contains(&id) {
                self.tagged.remove(&id);
            } else {
                self.tagged.insert(id);
            }
            if self.index < self.filtered_messages.len().saturating_sub(1) {
                self.index += 1;
            }
            self.render_all();
        }
    }

    fn tag_all_toggle(&mut self) {
        if self.tagged.is_empty() {
            // Tag all
            for msg in &self.filtered_messages {
                self.tagged.insert(msg.id);
            }
        } else {
            // Untag all
            self.tagged.clear();
        }
        self.render_all();
    }

    fn toggle_delete_mark(&mut self) {
        if !self.tagged.is_empty() {
            // Mark all tagged messages for deletion
            for id in &self.tagged {
                self.delete_marked.insert(*id);
            }
            let count = self.tagged.len();
            self.tagged.clear();
            self.set_feedback(&format!("{} messages marked for deletion", count), self.config.theme_colors.feedback_warn);
            self.render_all();
        } else if let Some(msg) = self.filtered_messages.get(self.index) {
            let id = msg.id;
            if self.delete_marked.contains(&id) {
                self.delete_marked.remove(&id);
            } else {
                self.delete_marked.insert(id);
            }
            if self.index < self.filtered_messages.len().saturating_sub(1) {
                self.index += 1;
            }
            self.render_all();
        }
    }

    fn purge_deleted(&mut self) {
        if self.delete_marked.is_empty() { return; }

        // Remember current message to restore position after purge
        let current_id = self.filtered_messages.get(self.index).map(|m| m.id);

        let ids: Vec<i64> = self.delete_marked.iter().copied().collect();

        // Delete maildir files from disk
        for &id in &ids {
            if let Some(msg) = self.filtered_messages.iter().find(|m| m.id == id) {
                if let Some(file) = msg.metadata.get("maildir_file").and_then(|v| v.as_str()) {
                    let path = std::path::Path::new(file);
                    if path.exists() {
                        let _ = std::fs::remove_file(path);
                    } else if let Some(dir) = path.parent() {
                        // Filename may have changed (flag suffix). Find by base name.
                        let base = path.file_name().and_then(|f| f.to_str())
                            .and_then(|f| f.split(":2,").next())
                            .unwrap_or("");
                        if !base.is_empty() {
                            if let Ok(entries) = std::fs::read_dir(dir) {
                                for entry in entries.flatten() {
                                    let name = entry.file_name();
                                    if name.to_str().map(|n| n.starts_with(base)).unwrap_or(false) {
                                        let _ = std::fs::remove_file(entry.path());
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let _ = self.write_tx.send(DbWriteOp::DeleteMessages(ids.clone()));
        let count = ids.len();
        self.delete_marked.clear();

        // Find lowest position of a deleted message (before removing)
        let min_deleted_pos = ids.iter().filter_map(|id| {
            self.filtered_messages.iter().position(|m| m.id == *id)
        }).min().unwrap_or(0);

        self.filtered_messages.retain(|m| !ids.contains(&m.id));
        if self.show_threaded {
            self.display_messages.retain(|m| !ids.contains(&m.id));
        }

        // Land on the item now at the first deleted position
        self.index = min_deleted_pos.min(self.filtered_messages.len().saturating_sub(1));

        self.set_feedback(&format!("Purged {} messages", count), self.config.theme_colors.feedback_ok);
        self.render_all();
    }

    fn file_message(&mut self) {
        if self.filtered_messages.is_empty() { return; }

        // Build hint from save_folders
        let shortcuts = self.config.save_folders.clone();
        let mut keys: Vec<&String> = shortcuts.keys().collect();
        keys.sort();
        let hint: String = keys.iter()
            .map(|k| {
                let v = &shortcuts[*k];
                let short = v.rsplit('.').next().unwrap_or(v);
                format!("s{}:{}", k, short)
            })
            .collect::<Vec<_>>()
            .join(" ");
        let hint_display = if hint.is_empty() { String::new() } else { format!(" [{}]", hint) };

        let tagged_count = self.tagged.len();
        let tagged_hint = if tagged_count > 0 {
            format!(" ({} tagged)", tagged_count)
        } else {
            String::new()
        };

        self.set_feedback(
            &format!("Save to folder:{}{} B:Browse =:Config", hint_display, tagged_hint),
            self.config.theme_colors.unread,
        );

        // Wait for sub-key
        let Some(chr) = Input::getchr(Some(5)) else {
            self.render_bottom_bar();
            return;
        };

        if chr == "ESC" || chr == "\x1b" {
            self.render_bottom_bar();
            return;
        }

        if chr == "=" {
            self.configure_save_shortcuts();
            return;
        }

        // Determine destination folder
        let dest = if let Some(folder) = shortcuts.get(&chr) {
            folder.clone()
        } else if chr == "B" {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            let maildir_path = std::path::PathBuf::from(&home).join("Main/Maildir");
            let folder_names = discover_maildir_folders(&maildir_path);
            let tree = build_folder_tree(&folder_names);
            let mut browser_display = flatten_folder_tree(&tree, "", 0, &self.folder_collapsed);
            if let Some(picked) = self.folder_browser_loop(&mut browser_display, false) {
                self.handle_resize();
                picked
            } else {
                self.handle_resize();
                return;
            }
        } else if chr == "F" {
            let favs = self.db.get_favorite_folders();
            if favs.is_empty() {
                self.set_feedback("No favorites. Use + in folder browser.", self.config.theme_colors.feedback_warn);
                return;
            }
            let tree = build_folder_tree(&favs);
            let mut browser_display = flatten_folder_tree(&tree, "", 0, &self.folder_collapsed);
            if let Some(picked) = self.folder_browser_loop(&mut browser_display, true) {
                self.handle_resize();
                picked
            } else {
                self.handle_resize();
                return;
            }
        } else {
            // Type folder name
            let initial = if chr == "ENTER" { String::new() } else { chr.clone() };
            let typed = self.prompt("Move to folder: ", &initial);
            if typed.is_empty() { return; }
            typed
        };

        // Collect messages to file
        let msg_ids: Vec<i64> = if !self.tagged.is_empty() {
            self.filtered_messages.iter()
                .filter(|m| self.tagged.contains(&m.id))
                .map(|m| m.id)
                .collect()
        } else if let Some(msg) = self.filtered_messages.get(self.index) {
            vec![msg.id]
        } else {
            return;
        };

        if msg_ids.is_empty() { return; }

        let mut count = 0;
        let mut failed = 0;

        for &id in &msg_ids {
            match self.file_single_message(id, &dest) {
                Ok(_) => count += 1,
                Err(_) => failed += 1,
            }
        }

        // Remove filed messages from view
        self.filtered_messages.retain(|m| !msg_ids.contains(&m.id));
        if !self.tagged.is_empty() {
            for &id in &msg_ids { self.tagged.remove(&id); }
        }
        if self.index >= self.filtered_messages.len() {
            self.index = self.filtered_messages.len().saturating_sub(1);
        }

        let msg = format!(
            "Moved {} message{} to {}",
            count,
            if count != 1 { "s" } else { "" },
            dest
        );
        let color = if failed > 0 {
            self.config.theme_colors.attachment
        } else {
            self.config.theme_colors.feedback_ok
        };
        self.set_feedback(&msg, color);
        self.render_all();
    }

    fn file_single_message(&self, id: i64, dest: &str) -> Result<(), String> {
        // Get message from DB with metadata
        let msg = self.db.get_message(id).ok_or("Message not found")?;
        let mut meta = msg.metadata.clone();

        // Move maildir file on disk if applicable
        if let Some(file_path) = meta.get("maildir_file").and_then(|v| v.as_str()).map(String::from) {
            if std::path::Path::new(&file_path).exists() {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
                let maildir_root = std::path::PathBuf::from(&home).join("Main/Maildir");
                let dest_dir = maildir_root.join(format!(".{}", dest));
                let cur_dir = dest_dir.join("cur");
                let _ = std::fs::create_dir_all(&cur_dir);
                let _ = std::fs::create_dir_all(dest_dir.join("new"));
                let _ = std::fs::create_dir_all(dest_dir.join("tmp"));

                // Move file
                let filename = std::path::Path::new(&file_path)
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or("msg");
                let new_path = cur_dir.join(filename);
                if std::fs::rename(&file_path, &new_path).is_ok() {
                    meta["maildir_file"] =
                        serde_json::json!(new_path.to_string_lossy().to_string());
                    meta["maildir_folder"] = serde_json::json!(dest);
                }
            }
        }

        // Update folder + metadata in DB
        self.db.update_message_folder(id, dest, &meta);
        self.db.mark_as_read(id);

        Ok(())
    }

    fn configure_save_shortcuts(&mut self) {
        let mut shortcuts = self.config.save_folders.clone();

        loop {
            // Build display
            let mut lines = vec![
                style::bold(&style::fg("Save Folder Shortcuts", self.config.theme_colors.view_custom)),
                String::new(),
            ];
            let mut keys: Vec<&String> = shortcuts.keys().collect();
            keys.sort();
            for k in &keys {
                lines.push(format!("  s{} = {}", k, shortcuts[*k]));
            }
            if keys.is_empty() {
                lines.push(style::fg("  (none configured)", self.config.theme_colors.hint_fg));
            }
            lines.push(String::new());
            lines.push(style::fg(
                "Press 0-9 to set, d+key to delete, ESC to finish",
                self.config.theme_colors.hint_fg,
            ));

            self.right.set_text(&lines.join("\n"));
            self.right.ix = 0;
            self.right.full_refresh();
            if self.right.border { self.right.border_refresh(); }

            let Some(chr) = Input::getchr(None) else { continue };
            match chr.as_str() {
                "ESC" | "q" => break,
                "d" => {
                    self.set_feedback("Delete which key? (0-9)", self.config.theme_colors.feedback_warn);
                    if let Some(key) = Input::getchr(Some(3)) {
                        if shortcuts.remove(&key).is_some() {
                            self.set_feedback(
                                &format!("Removed shortcut s{}", key),
                                self.config.theme_colors.feedback_ok,
                            );
                        }
                    }
                }
                k if k.len() == 1
                    && k.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) =>
                {
                    let default = shortcuts.get(k).cloned().unwrap_or_default();
                    let folder = self.prompt(&format!("Folder for s{} (or 'b' to browse): ", k), &default);
                    if folder == "b" {
                        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
                        let maildir_path = std::path::PathBuf::from(&home).join("Main/Maildir");
                        let folder_names = discover_maildir_folders(&maildir_path);
                        let tree = build_folder_tree(&folder_names);
                        let mut browser_display = flatten_folder_tree(&tree, "", 0, &self.folder_collapsed);
                        if let Some(picked) = self.folder_browser_loop(&mut browser_display, false) {
                            shortcuts.insert(k.to_string(), picked);
                        }
                        self.handle_resize();
                    } else if !folder.is_empty() {
                        shortcuts.insert(k.to_string(), folder);
                    }
                }
                _ => {}
            }
        }

        self.config.save_folders = shortcuts;
        self.config.save();
        self.render_all();
    }

    fn copy_message_id(&mut self) {
        if let Some(msg) = self.filtered_messages.get(self.index) {
            let id_str = format!("heathrow:{}", msg.id);
            crust::clipboard_copy(&id_str, "clipboard");
            self.set_feedback(&format!("Copied: {}", id_str), self.config.theme_colors.feedback_ok);
        }
    }

    fn copy_right_pane(&self) {
        let text = self.right.text();
        crust::clipboard_copy(&crust::strip_ansi(text), "clipboard");
    }

    /// Build a Filters struct matching the current view (for mark-all-read)
    fn current_view_filters(&self) -> Option<Filters> {
        match self.current_view.as_str() {
            "A" => None,
            "N" => {
                let mut f = Filters::default();
                f.is_read = Some(false);
                Some(f)
            }
            _ => None,
        }
    }
}

// --- UI controls ---

impl App {
    fn cycle_width(&mut self) {
        self.width = if self.width >= 6 { 1 } else { self.width + 1 };
        self.config.pane_width = self.width;
        self.config.save();
        self.handle_resize();
        if self.left.border { self.left.border_refresh(); }
        if self.right.border { self.right.border_refresh(); }
    }

    fn cycle_width_reverse(&mut self) {
        self.width = if self.width <= 1 { 6 } else { self.width - 1 };
        self.config.pane_width = self.width;
        self.config.save();
        self.handle_resize();
        if self.left.border { self.left.border_refresh(); }
        if self.right.border { self.right.border_refresh(); }
    }

    fn cycle_border(&mut self) {
        self.border = (self.border + 1) % 4;
        self.config.border_style = self.border;
        self.config.save();
        self.handle_resize();
        if self.left.border { self.left.border_refresh(); }
        if self.right.border { self.right.border_refresh(); }
    }

    fn cycle_date_format(&mut self) {
        let formats = [
            "%b %e", "%d/%m %H:%M", "%m/%d %H:%M", "%Y-%m-%d %H:%M",
            "%d.%m %H:%M", "%d %b %H:%M", "%b %d %H:%M",
        ];
        let idx = formats.iter().position(|&f| f == self.date_format).unwrap_or(0);
        self.date_format = formats[(idx + 1) % formats.len()].to_string();
        self.render_all();
    }

    fn first_run_wizard(&mut self) {
        let tc = self.config.theme_colors.clone();
        self.render_all();

        let welcome = format!("{}\n\n\
{}\n\n\
{}\n\
{}\n\
{}\n\n\
{}\n\n\
{}\n\
{}\n\
{}\n\
{}\n\n\
{}\n",
            style::bold(&style::fg("Welcome to Kastrup!", tc.view_custom)),
            "A unified terminal messaging hub for all your communication.",
            style::fg("Kastrup connects to:", tc.unread),
            "  Email (Maildir), RSS feeds, Discord, Slack, Telegram,",
            "  WhatsApp, Messenger, Instagram, Reddit, WeeChat, and more.",
            style::fg("To get started, set up your first source:", tc.unread),
            style::fg("  1. Press S to open Sources view", tc.hint_fg),
            style::fg("  2. Press 'a' to add a new source", tc.hint_fg),
            style::fg("  3. For email, add a Maildir source pointing to ~/Maildir", tc.hint_fg),
            style::fg("  4. For RSS, add feeds by URL", tc.hint_fg),
            style::fg("Press any key to continue, or 'q' to quit.", tc.hint_fg),
        );

        self.right.set_text(&welcome);
        self.right.ix = 0;
        self.right.full_refresh();
        if self.right.border { self.right.border_refresh(); }

        self.top.say(&format!("{}{}",
            style::fg(" Kastrup - ", tc.prefix_fg),
            style::bold(&style::fg("Welcome", tc.view_custom))));

        self.bottom.say(&style::fg(
            " Press 'a' to add a Maildir source now, or any other key to continue",
            tc.hint_fg));

        if let Some(key) = Input::getchr(None) {
            if key == "q" || key == "Q" {
                self.running = false;
                return;
            }
            if key == "a" {
                // Quick Maildir setup
                let maildir_path = self.prompt("Maildir path: ", "~/Maildir");
                if !maildir_path.is_empty() {
                    let expanded = maildir_path.replace("~/",
                        &format!("{}/", std::env::var("HOME").unwrap_or_default()));
                    if std::path::Path::new(&expanded).is_dir() {
                        let now = database::now_secs();
                        let config_json = serde_json::json!({"path": expanded}).to_string();
                        let conn = self.db.conn.lock().unwrap();
                        let _ = conn.execute(
                            "INSERT INTO sources (name, plugin_type, enabled, config, capabilities, created_at, updated_at, poll_interval) \
                             VALUES (?, 'maildir', 1, ?, '[\"read\",\"send\"]', ?, ?, 30)",
                            rusqlite::params!["Local Maildir", config_json, now, now],
                        );
                        drop(conn);
                        self.source_type_map = self.db.get_source_type_map();
                        self.set_feedback("Maildir source added! Messages will sync on next poll.", tc.feedback_ok);
                    } else {
                        self.set_feedback(&format!("Path not found: {}", expanded), tc.feedback_warn);
                    }
                }
            }
        }
    }

    fn show_help(&mut self) {
        let help = format!("{}\n\n\
{}\n\
  j/k Up/Down    Navigate messages\n\
  h/Left         Collapse thread\n\
  Space          Toggle collapse\n\
  PgDn/PgUp      Page down/up\n\
  Home/End       First/last message\n\
  Enter          Open message (mark read)\n\
  n/p            Next/prev unread\n\
  J              Jump to date\n\
  G              Toggle threaded/flat view\n\n\
{}\n\
  A              All messages\n\
  N              New (unread)\n\
  S              Sources\n\
  0-9            Custom views\n\
  F1-F12         Extended views\n\
  F              Favorites browser\n\
  L              Load more messages\n\
  Ctrl-R         Refresh current view\n\
  Ctrl-F         Filter editor\n\
  K              Kill (close) view\n\n\
{}\n\
  R              Toggle read/unread\n\
  M              Mark all read\n\
  */-            Toggle star\n\
  t/T            Tag / tag all\n\
  Ctrl-T         Tag by regex\n\
  d              Mark for deletion\n\
  <              Purge deleted\n\
  u/U            Mark unseen\n\
  Shift-Space    Mark browsed\n\n\
{}\n\
  r              Reply\n\
  e              Reply in editor\n\
  g              Reply-all\n\
  f              Forward\n\
  m              Compose new\n\
  E              Edit draft\n\n\
{}\n\
  v              View/save attachments\n\
  V              Inline image\n\
  D              Download images to disk\n\
  x              Open in external app\n\
  X              Open HTML in browser\n\n\
{}\n\
  /              Search messages\n\
  l              Label message\n\
  s              File/save message\n\
  +              Add to favorites\n\
  I              AI assistant / plugins\n\
  Z              Timely actions\n\n\
{}\n\
  o              Cycle sort order\n\
  i              Invert sort\n\
  w/W            Cycle pane width forward/back\n\
  c              Set top bar color\n\
  B              Folder browser\n\
  Ctrl-B         Cycle border style\n\
  D              Cycle date format\n\
  C              Preferences\n\
  y/Y            Copy ID / copy content\n\
  Ctrl-L         Redraw\n\
  q              Quit",
            style::bold("Kastrup - Messaging Hub"),
            style::fg("Navigation", self.config.theme_colors.feedback_warn),
            style::fg("Views", self.config.theme_colors.feedback_warn),
            style::fg("Message Operations", self.config.theme_colors.feedback_warn),
            style::fg("Compose / Reply", self.config.theme_colors.feedback_warn),
            style::fg("Attachments / External", self.config.theme_colors.feedback_warn),
            style::fg("Search / Misc", self.config.theme_colors.feedback_warn),
            style::fg("UI", self.config.theme_colors.feedback_warn),
        );
        self.right.set_text(&help);
        self.right.ix = 0;
        self.right.full_refresh();
    }

    fn handle_resize(&mut self) {
        let (cols, rows) = Crust::terminal_size();
        self.cols = cols;
        self.rows = rows;
        let (top, left, right, bottom) = create_panes(cols, rows, self.width, self.border, &self.config);
        self.top = top;
        self.left = left;
        self.right = right;
        self.bottom = bottom;
        // Restore per-view top bar bg color
        self.restore_view_top_bg();
        Crust::clear_screen();
        self.render_all();
    }

    fn restore_view_top_bg(&mut self) {
        if let Some(vw) = self.views.iter().find(|v| v.key_binding.as_deref() == Some(&self.current_view)) {
            if let Ok(f) = serde_json::from_str::<serde_json::Value>(&vw.filters) {
                if let Some(bg) = f["top_bg"].as_str().and_then(|s| s.parse::<u16>().ok()) {
                    self.top.bg = bg;
                } else if let Some(bg) = f["top_bg"].as_u64() {
                    self.top.bg = bg as u16;
                }
            }
        }
    }

    fn force_redraw(&mut self) {
        self.handle_resize();
        if self.left.border { self.left.border_refresh(); }
        if self.right.border { self.right.border_refresh(); }
    }

    fn set_feedback(&mut self, msg: &str, color: u8) {
        // Auto-log errors and warnings
        if color == 196 { log::error(msg); }
        else if color == self.config.theme_colors.feedback_warn { log::warn(msg); }
        self.feedback_message = Some((msg.to_string(), color));
        self.feedback_expires = Some(std::time::Instant::now() + std::time::Duration::from_secs(3));
        self.render_bottom_bar();
    }

    /// Prompt in the bottom bar, always restore status bar after
    fn prompt(&mut self, label: &str, default: &str) -> String {
        let result = self.bottom.ask_with_bg(label, default, self.config.theme_colors.cmd_bg);
        // Restore bottom bar bg (ask_with_bg changes it to cmd_bg)
        self.bottom.bg = self.config.theme_colors.bottom_bg;
        // Force full redraw: editline bypasses prev_frame, so diff render misses the change
        self.bottom.full_refresh();
        self.render_bottom_bar();
        result
    }
}

// --- New feature methods ---

impl App {
    fn jump_to_date(&mut self) {
        let input = self.prompt("Jump to date (yyyy-mm-dd): ", "");
        self.render_bottom_bar();
        if input.is_empty() { return; }
        let parts: Vec<&str> = input.split('-').collect();
        if parts.len() == 3 {
            if let (Ok(y), Ok(m), Ok(d)) = (parts[0].parse::<i64>(), parts[1].parse::<i64>(), parts[2].parse::<i64>()) {
                // Approximate unix timestamp (good enough for jumping)
                let target_ts = ((y - 1970) * 365 + (y - 1969) / 4) * 86400 + (m - 1) * 30 * 86400 + (d - 1) * 86400;
                if let Some(pos) = self.filtered_messages.iter().position(|msg| msg.timestamp <= target_ts) {
                    self.index = pos;
                    self.render_all();
                } else {
                    self.set_feedback("No messages found at that date", self.config.theme_colors.feedback_warn);
                }
            } else {
                self.set_feedback("Invalid date format", 196);
            }
        } else {
            self.set_feedback("Use format: yyyy-mm-dd", 196);
        }
    }

    fn open_external(&mut self) {
        // x key: open in default browser (xdg-open / Firefox)
        // Ensure full content loaded for HTML
        if !self.filtered_messages.get(self.index).map(|m| m.full_loaded).unwrap_or(true) {
            let id = self.filtered_messages[self.index].id;
            if let Some((content, html)) = self.db.get_message_content(id) {
                self.filtered_messages[self.index].content = content;
                self.filtered_messages[self.index].html_content = html;
                self.filtered_messages[self.index].full_loaded = true;
            }
        }
        if let Some(msg) = self.filtered_messages.get(self.index) {
            if let Some(link) = msg.metadata.get("link").and_then(|v| v.as_str()) {
                let _ = std::process::Command::new("xdg-open").arg(link)
                    .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn();
                self.set_feedback("Opened in browser", self.config.theme_colors.feedback_ok);
            } else {
                // Get best HTML to display: html_content > MIME extraction > raw content
                let html = if let Some(ref h) = msg.html_content {
                    h.clone()
                } else if msg.content.contains("Content-Type:") || msg.content.lines().any(|l| l.starts_with("--") && l.len() > 5) {
                    extract_mime_html(&msg.content).unwrap_or_else(|| {
                        // Wrap raw text in HTML as last resort
                        let text = msg.content.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
                        format!("<html><head><meta charset=\"utf-8\"><style>body{{font-family:monospace;white-space:pre-wrap;padding:1em}}</style></head><body>{}</body></html>", text)
                    })
                } else if msg.content.contains("<html") || msg.content.contains("<body") || msg.content.trim_start().starts_with('<') {
                    msg.content.clone()
                } else {
                    self.set_feedback("No HTML content to open", self.config.theme_colors.feedback_warn);
                    return;
                };
                let path = format!("/tmp/kastrup_msg_{}.html", msg.id);
                if std::fs::write(&path, &html).is_ok() {
                    let _ = std::process::Command::new("xdg-open").arg(&path)
                        .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn();
                    self.set_feedback("Opened in browser", self.config.theme_colors.feedback_ok);
                }
            }
        }
    }

    fn open_in_browser(&mut self) {
        // X key: open in Scroll (terminal browser)
        if let Some(msg) = self.filtered_messages.get(self.index) {
            let url = if let Some(link) = msg.metadata.get("link").and_then(|v| v.as_str()) {
                Some(link.to_string())
            } else {
                let html = if let Some(ref h) = msg.html_content {
                    Some(h.clone())
                } else if msg.content.contains("Content-Type:") || msg.content.lines().any(|l| l.starts_with("--") && l.len() > 5) {
                    Some(extract_mime_html(&msg.content).unwrap_or_else(|| {
                        let text = msg.content.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
                        format!("<html><head><meta charset=\"utf-8\"><style>body{{font-family:monospace;white-space:pre-wrap;padding:1em}}</style></head><body>{}</body></html>", text)
                    }))
                } else if msg.content.contains("<html") || msg.content.contains("<body") || msg.content.trim_start().starts_with('<') {
                    Some(msg.content.clone())
                } else {
                    None
                };
                html.map(|h| {
                    let path = format!("/tmp/kastrup_msg_{}.html", msg.id);
                    let _ = std::fs::write(&path, &h);
                    format!("file://{}", path)
                })
            };

            if let Some(url) = url {
                Crust::cleanup();
                let _ = std::process::Command::new("scroll").arg(&url).status();
                Crust::init();
                Crust::clear_screen();
                self.handle_resize();
            } else {
                self.set_feedback("No content to open", self.config.theme_colors.feedback_warn);
            }
        }
    }

    fn load_more(&mut self) {
        let current_count = self.filtered_messages.len();
        let filters = self.build_current_filters();
        let more = self.db.get_messages(&filters, 500, current_count);
        if more.is_empty() {
            self.set_feedback("No more messages", self.config.theme_colors.feedback_info);
        } else {
            let count = more.len();
            for mut msg in more {
                if let Some(st) = self.source_type_map.get(&msg.source_id) {
                    msg.source_type = st.clone();
                }
                self.filtered_messages.push(msg);
            }
            self.sort_messages();
            self.rebuild_display();
            self.set_feedback(&format!("Loaded {} more messages", count), self.config.theme_colors.feedback_ok);
            self.render_all();
        }
    }

    fn build_current_filters(&self) -> Filters {
        let mut filters = Filters::default();
        match self.current_view.as_str() {
            "A" => {}
            "N" => { filters.is_read = Some(false); }
            "*" => { filters.is_starred = Some(true); }
            key => {
                if let Some(view) = self.views.iter().find(|v| v.key_binding.as_deref() == Some(key)) {
                    if let Ok(f) = serde_json::from_str::<serde_json::Value>(&view.filters) {
                        if let Some(rules) = f["rules"].as_array() {
                            for rule in rules {
                                let field = rule["field"].as_str().unwrap_or("");
                                let value = &rule["value"];
                                match field {
                                    "read" => { filters.is_read = Some(!value.as_bool().unwrap_or(true)); }
                                    "starred" => { filters.is_starred = value.as_bool(); }
                                    "folder" => { filters.folder = value.as_str().map(|s| s.to_string()); }
                                    "source_id" => { filters.source_id = value.as_i64(); }
                                    "sender" => { filters.sender_pattern = value.as_str().map(|s| s.to_string()); }
                                    "source_type" => { filters.source_type = value.as_str().map(|s| s.to_string()); }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
        filters
    }

    fn refresh_view(&mut self) {
        let view = self.current_view.clone();
        if self.in_source_view {
            self.show_sources();
        } else {
            self.switch_to_view(&view);
        }
        self.set_feedback("View refreshed", self.config.theme_colors.feedback_ok);
    }

    fn tag_by_regex(&mut self) {
        let pattern = self.prompt("Tag regex: ", "");
        self.render_bottom_bar();
        if pattern.is_empty() { return; }
        if let Ok(re) = regex::Regex::new(&pattern) {
            let mut count = 0;
            for msg in &self.filtered_messages {
                let sender = msg.sender_name.as_deref().unwrap_or(&msg.sender);
                let subject = msg.subject.as_deref().unwrap_or("");
                if re.is_match(sender) || re.is_match(subject) {
                    self.tagged.insert(msg.id);
                    count += 1;
                }
            }
            self.set_feedback(&format!("Tagged {} messages", count), self.config.theme_colors.feedback_ok);
            self.render_all();
        } else {
            self.set_feedback("Invalid regex", 196);
        }
    }

    fn search_prompt(&mut self) {
        let query = self.prompt("/", "");
        self.render_bottom_bar();
        if query.is_empty() { return; }

        // Try notmuch first
        let notmuch = std::process::Command::new("notmuch")
            .args(["search", "--format=json", "--output=summary", &query])
            .output();

        if let Ok(output) = notmuch {
            if output.status.success() {
                let json_str = String::from_utf8_lossy(&output.stdout);
                if let Ok(results) = serde_json::from_str::<Vec<serde_json::Value>>(&json_str) {
                    if !results.is_empty() {
                        // Search DB for matching content
                        let mut filters = Filters::default();
                        filters.content_pattern = Some(query.clone());
                        self.filtered_messages = self.db.get_messages(&filters, 500, 0);
                        for msg in &mut self.filtered_messages {
                            if let Some(st) = self.source_type_map.get(&msg.source_id) {
                                msg.source_type = st.clone();
                            }
                        }
                        self.index = 0;
                        self.set_feedback(&format!("Notmuch: {} results", self.filtered_messages.len()), self.config.theme_colors.feedback_ok);
                        self.render_all();
                        return;
                    }
                }
            }
        }

        // Fallback: simple DB substring search
        let lower = query.to_lowercase();
        if let Some(pos) = self.filtered_messages.iter().skip(self.index + 1).position(|msg| {
            let sender = msg.sender_name.as_deref().unwrap_or(&msg.sender);
            let subject = msg.subject.as_deref().unwrap_or("");
            sender.to_lowercase().contains(&lower)
                || subject.to_lowercase().contains(&lower)
                || msg.content.to_lowercase().contains(&lower)
        }) {
            self.index = self.index + 1 + pos;
            self.render_all();
        } else {
            self.set_feedback("No matches found", self.config.theme_colors.feedback_warn);
        }
    }

    fn set_view_color(&mut self) {
        let input = self.prompt("Top bar color (0-255): ", "");
        if let Ok(c) = input.parse::<u16>() {
            self.top.bg = c;
            // Persist to view's filters JSON in DB
            if let Some(vw) = self.views.iter().find(|v| v.key_binding.as_deref() == Some(&self.current_view)) {
                let mut f: serde_json::Value = serde_json::from_str(&vw.filters).unwrap_or(serde_json::json!({}));
                f["top_bg"] = serde_json::json!(c.to_string());
                let new_filters = serde_json::to_string(&f).unwrap_or_default();
                let conn = self.db.conn.lock().unwrap();
                let _ = conn.execute(
                    "UPDATE views SET filters = ?, updated_at = ? WHERE key_binding = ?",
                    rusqlite::params![new_filters, crate::database::now_secs(), self.current_view],
                );
            }
            self.render_top_bar();
        }
    }

    fn test_source(&mut self) {
        if let Some(src) = self.sources_list.get(self.index) {
            let name = src.name.clone();
            let err = src.last_error.clone();
            if let Some(err_msg) = err {
                self.set_feedback(&format!("Source has error: {}", err_msg), 196);
            } else {
                self.set_feedback(&format!("Source '{}' looks OK", name), self.config.theme_colors.feedback_ok);
            }
        }
    }

    fn toggle_source(&mut self) {
        if let Some(src) = self.sources_list.get(self.index) {
            let sid = src.id;
            let new_state = self.db.toggle_source_enabled(sid);
            // Refresh sources list
            self.sources_list = self.db.get_sources(false);
            let label = if new_state { "enabled" } else { "disabled" };
            self.set_feedback(&format!("Source {}", label), self.config.theme_colors.feedback_ok);
            self.render_source_list();
            self.render_source_info();
        }
    }

    // --- Source management (Batch C) ---

    fn add_source(&mut self) {
        let stype = self.prompt("Source type (maildir/rss): ", "maildir");
        if stype.is_empty() { return; }
        match stype.as_str() {
            "maildir" => {
                let name = self.prompt("Source name: ", "Local Maildir");
                if name.is_empty() { return; }
                let path = self.prompt("Maildir path: ", "~/Maildir");
                if path.is_empty() { return; }
                let expanded = path.replace("~/", &format!("{}/", std::env::var("HOME").unwrap_or_default()));
                let config = serde_json::json!({"path": expanded});
                self.db.add_source(&name, "maildir", &config.to_string(), "[\"read\",\"send\"]", 30);
                self.source_type_map = self.db.get_source_type_map();
                self.set_feedback(&format!("Added source: {}", name), self.config.theme_colors.feedback_ok);
            }
            "rss" => {
                let name = self.prompt("Source name: ", "RSS Feeds");
                if name.is_empty() { return; }
                let url = self.prompt("Feed URL: ", "");
                if url.is_empty() { return; }
                let config = serde_json::json!({"feeds": [{"url": url}]});
                self.db.add_source(&name, "rss", &config.to_string(), "[\"read\"]", 3600);
                self.source_type_map = self.db.get_source_type_map();
                self.set_feedback(&format!("Added source: {}", name), self.config.theme_colors.feedback_ok);
            }
            _ => {
                self.set_feedback(&format!("Unknown source type: {}", stype), self.config.theme_colors.feedback_warn);
            }
        }
        self.sources_list = self.db.get_sources(false);
        self.render_source_list();
        self.render_source_info();
    }

    fn edit_source(&mut self) {
        let (id, current_name) = match self.sources_list.get(self.index) {
            Some(s) => (s.id, s.name.clone()),
            None => return,
        };
        let name = self.prompt("Name: ", &current_name);
        if !name.is_empty() {
            let conn = self.db.conn.lock().unwrap();
            let _ = conn.execute("UPDATE sources SET name = ? WHERE id = ?", rusqlite::params![name, id]);
        }
        self.sources_list = self.db.get_sources(false);
        self.source_type_map = self.db.get_source_type_map();
        self.render_source_list();
        self.render_source_info();
    }

    fn delete_source(&mut self) {
        let src = match self.sources_list.get(self.index) { Some(s) => s, None => return };
        let name = src.name.clone();
        let id = src.id;
        self.set_feedback(&format!("Delete '{}' and all its messages? (y/n)", name), self.config.theme_colors.feedback_warn);
        if let Some(key) = Input::getchr(Some(5)) {
            if key == "y" || key == "Y" {
                let conn = self.db.conn.lock().unwrap();
                let _ = conn.execute("DELETE FROM messages WHERE source_id = ?", rusqlite::params![id]);
                let _ = conn.execute("DELETE FROM sources WHERE id = ?", rusqlite::params![id]);
                drop(conn);
                self.sources_list = self.db.get_sources(false);
                self.source_type_map = self.db.get_source_type_map();
                if self.index >= self.sources_list.len() { self.index = self.sources_list.len().saturating_sub(1); }
                self.set_feedback(&format!("Deleted: {}", name), self.config.theme_colors.feedback_ok);
            } else {
                self.set_feedback("Cancelled", self.config.theme_colors.feedback_info);
            }
        }
        self.render_source_list();
    }

    fn set_source_color(&mut self) {
        let src_id = match self.sources_list.get(self.index) { Some(s) => s.id, None => return };
        let input = self.prompt("Color (0-255): ", "");
        if let Ok(c) = input.parse::<u16>() {
            let conn = self.db.conn.lock().unwrap();
            let _ = conn.execute("UPDATE sources SET color = ? WHERE id = ?", rusqlite::params![c.to_string(), src_id]);
            drop(conn);
            self.sources_list = self.db.get_sources(false);
            self.render_source_list();
        }
    }

    fn set_source_poll_interval(&mut self) {
        let (src_id, current_interval) = match self.sources_list.get(self.index) {
            Some(s) => (s.id, s.poll_interval.to_string()),
            None => return,
        };
        let input = self.prompt("Poll interval (seconds): ", &current_interval);
        if let Ok(secs) = input.parse::<i64>() {
            let conn = self.db.conn.lock().unwrap();
            let _ = conn.execute("UPDATE sources SET poll_interval = ? WHERE id = ?", rusqlite::params![secs, src_id]);
            drop(conn);
            self.sources_list = self.db.get_sources(false);
            self.set_feedback(&format!("Poll interval set to {}s", secs), self.config.theme_colors.feedback_ok);
        }
    }

    // --- Labels, Unsee, Mark Browsed (Batch D) ---

    fn label_message(&mut self) {
        let tc = self.config.theme_colors.clone();
        let tagged_hint = if !self.tagged.is_empty() { format!(" ({} tagged)", self.tagged.len()) } else { String::new() };
        let action = self.prompt(&format!("Label{} (+add / -remove / ? list): ", tagged_hint), "+");
        if action.is_empty() { return; }

        if action.trim() == "?" {
            // Show all labels
            let labels: Vec<String> = {
                let conn = self.db.conn.lock().unwrap();
                let mut stmt = conn.prepare("SELECT DISTINCT json_each.value FROM messages, json_each(messages.labels) ORDER BY 1").unwrap();
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .unwrap().filter_map(|r| r.ok()).collect()
            };
            self.right.set_text(&format!("{}\n\n{}",
                style::bold(&style::fg("All Labels", tc.view_custom)),
                labels.join("\n")));
            self.right.ix = 0;
            self.right.full_refresh();
            if self.right.border { self.right.border_refresh(); }
            return;
        }

        let adding = !action.starts_with('-');
        let label_name = action.trim_start_matches('+').trim_start_matches('-').trim().to_string();
        if label_name.is_empty() { return; }

        let msg_ids: Vec<i64> = if !self.tagged.is_empty() {
            self.filtered_messages.iter().filter(|m| self.tagged.contains(&m.id)).map(|m| m.id).collect()
        } else {
            self.filtered_messages.get(self.index).map(|m| vec![m.id]).unwrap_or_default()
        };

        let mut count = 0;
        for &id in &msg_ids {
            if let Some(msg) = self.filtered_messages.iter_mut().find(|m| m.id == id) {
                if adding && !msg.labels.contains(&label_name) {
                    msg.labels.push(label_name.clone());
                    count += 1;
                } else if !adding {
                    if let Some(pos) = msg.labels.iter().position(|l| l == &label_name) {
                        msg.labels.remove(pos);
                        count += 1;
                    }
                }
                let labels_json = serde_json::to_string(&msg.labels).unwrap_or_default();
                let conn = self.db.conn.lock().unwrap();
                let _ = conn.execute("UPDATE messages SET labels = ? WHERE id = ?", rusqlite::params![labels_json, id]);
            }
        }

        if !self.tagged.is_empty() { self.tagged.clear(); }
        let verb = if adding { "Added" } else { "Removed" };
        self.set_feedback(&format!("{} '{}' on {} message(s)", verb, label_name, count), tc.feedback_ok);
        self.render_all();
    }

    fn unsee_message(&mut self) {
        if let Some(msg) = self.filtered_messages.get(self.index) {
            let id = msg.id;
            let mut metadata = msg.metadata.clone();
            self.browsed_ids.remove(&id);
            self.unseen_ids.insert(id);
            // Mark as unread in DB
            let _ = self.write_tx.send(DbWriteOp::MarkUnread(id));
            // Remove S flag from maildir filename on disk and update DB metadata
            if let Some(file) = metadata.get("maildir_file").and_then(|v| v.as_str()).map(String::from) {
                let old_path = std::path::Path::new(&file);
                if old_path.exists() && file.contains(":2,") {
                    // Remove S from flags portion
                    let (base, flags) = file.rsplit_once(":2,").unwrap_or((&file, ""));
                    let new_flags: String = flags.chars().filter(|&c| c != 'S').collect();
                    let new_file = format!("{}:2,{}", base, new_flags);
                    if new_file != file {
                        let new_path = std::path::Path::new(&new_file);
                        if std::fs::rename(old_path, new_path).is_ok() {
                            // Update metadata and external_id in DB to match new filename
                            metadata["maildir_file"] = serde_json::json!(&new_file);
                            let new_fname = new_path.file_name().and_then(|f| f.to_str()).unwrap_or("");
                            let conn = self.db.conn.lock().unwrap();
                            let _ = conn.execute(
                                "UPDATE messages SET metadata = ?, external_id = ? WHERE id = ?",
                                rusqlite::params![serde_json::to_string(&metadata).unwrap_or_default(), new_fname, id]
                            );
                            drop(conn);
                        }
                    }
                }
            }
            if let Some(m) = self.filtered_messages.iter_mut().find(|m| m.id == id) {
                m.read = false;
                m.metadata = metadata;
            }
            if self.show_threaded {
                if let Some(m) = self.display_messages.iter_mut().find(|m| m.id == id) {
                    m.read = false;
                }
            }
            self.stats_cache = None;
            self.set_feedback("Message marked unread", self.config.theme_colors.feedback_ok);
            self.render_all();
        }
    }

    fn mark_browsed_as_read(&mut self) {
        if self.browsed_ids.is_empty() {
            self.set_feedback("No browsed messages", self.config.theme_colors.feedback_info);
            return;
        }
        let count = self.browsed_ids.len();
        for &id in &self.browsed_ids.clone() {
            self.db.mark_as_read(id);
            if let Some(msg) = self.filtered_messages.iter_mut().find(|m| m.id == id) {
                msg.read = true;
            }
        }
        self.browsed_ids.clear();
        self.set_feedback(&format!("Marked {} browsed message(s) as read", count), self.config.theme_colors.feedback_ok);
        self.render_all();
    }

    // --- Filter Editor, Kill View (Batch F) ---

    fn edit_filter(&mut self) {
        let tc = self.config.theme_colors.clone();
        let view = self.views.iter().find(|v| v.key_binding.as_deref() == Some(&self.current_view));
        let current_filters = view.map(|v| v.filters.clone()).unwrap_or_default();

        let lines = vec![
            style::bold(&style::fg("Filter Editor", tc.view_custom)),
            String::new(),
            style::fg(&format!("View: {}", self.current_view), tc.info_fg),
            String::new(),
            style::fg("Current filters:", tc.hint_fg),
            style::fg(&current_filters, tc.info_fg),
            String::new(),
            style::fg("Press 'a' to add rule, 'd' to clear, ESC to close", tc.hint_fg),
        ];

        self.right.set_text(&lines.join("\n"));
        self.right.ix = 0;
        self.right.full_refresh();
        if self.right.border { self.right.border_refresh(); }

        loop {
            let Some(key) = Input::getchr(None) else { continue };
            match key.as_str() {
                "a" => {
                    let field = self.prompt("Field (folder/sender/source_id/read/starred): ", "folder");
                    let op = self.prompt("Operator (=/like/!=): ", "like");
                    let value = self.prompt("Value: ", "");
                    if !field.is_empty() && !value.is_empty() {
                        let mut f: serde_json::Value = serde_json::from_str(&current_filters).unwrap_or(serde_json::json!({"rules":[]}));
                        if let Some(rules) = f["rules"].as_array_mut() {
                            rules.push(serde_json::json!({"field": field, "op": op, "value": value}));
                        }
                        let new_filters = serde_json::to_string(&f).unwrap_or_default();
                        let conn = self.db.conn.lock().unwrap();
                        let _ = conn.execute("UPDATE views SET filters = ? WHERE key_binding = ?",
                            rusqlite::params![new_filters, self.current_view]);
                        drop(conn);
                        self.views = self.db.get_views();
                        self.set_feedback("Rule added", tc.feedback_ok);
                    }
                    break;
                }
                "d" => {
                    let f = serde_json::json!({"rules":[]});
                    let new_filters = serde_json::to_string(&f).unwrap_or_default();
                    let conn = self.db.conn.lock().unwrap();
                    let _ = conn.execute("UPDATE views SET filters = ? WHERE key_binding = ?",
                        rusqlite::params![new_filters, self.current_view]);
                    drop(conn);
                    self.views = self.db.get_views();
                    self.set_feedback("Filters cleared", tc.feedback_ok);
                    break;
                }
                "ESC" | "q" => break,
                _ => {}
            }
        }
        self.render_all();
    }

    fn kill_view(&mut self) {
        if self.current_view == "A" || self.current_view == "N" || self.current_view == "*" {
            self.set_feedback("Cannot delete built-in views", self.config.theme_colors.feedback_warn);
            return;
        }
        self.set_feedback(&format!("Delete view '{}'? (y/n)", self.current_view), self.config.theme_colors.feedback_warn);
        if let Some(key) = Input::getchr(Some(5)) {
            if key == "y" || key == "Y" {
                let conn = self.db.conn.lock().unwrap();
                let _ = conn.execute("DELETE FROM views WHERE key_binding = ?", rusqlite::params![self.current_view]);
                drop(conn);
                self.views = self.db.get_views();
                self.set_feedback("View deleted", self.config.theme_colors.feedback_ok);
                self.switch_to_view("A");
            } else {
                self.set_feedback("Cancelled", self.config.theme_colors.feedback_info);
            }
        }
    }

    // --- Edit Message (Batch G) ---

    fn edit_message(&mut self) {
        let msg = match self.filtered_messages.get(self.index) { Some(m) => m, None => return };
        let id = msg.id;
        // Ensure full content
        if !msg.full_loaded {
            if let Some((content, _html)) = self.db.get_message_content(id) {
                self.filtered_messages[self.index].content = content;
                self.filtered_messages[self.index].full_loaded = true;
            }
        }
        let content = self.filtered_messages[self.index].content.clone();
        let tmpfile = format!("/tmp/kastrup_edit_{}.txt", std::process::id());
        let _ = std::fs::write(&tmpfile, &content);

        let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vim".into());
        Crust::cleanup();
        let _ = std::process::Command::new("sh").arg("-c").arg(&format!("{} {}", editor, crust::shell_escape(&tmpfile))).status();
        Crust::init();
        Crust::clear_screen();

        if let Ok(edited) = std::fs::read_to_string(&tmpfile) {
            if edited.trim() != content.trim() {
                let conn = self.db.conn.lock().unwrap();
                let _ = conn.execute("UPDATE messages SET content = ? WHERE id = ?", rusqlite::params![edited, id]);
                drop(conn);
                self.filtered_messages[self.index].content = edited;
                self.set_feedback("Message updated", self.config.theme_colors.feedback_ok);
            }
        }
        let _ = std::fs::remove_file(&tmpfile);
        self.handle_resize();
    }

    fn show_preferences(&mut self) {
        let pw = 80u16.min(self.cols.saturating_sub(4));
        let ph = 20u16.min(self.rows.saturating_sub(6));
        let px = (self.cols.saturating_sub(pw)) / 2;
        let py = (self.rows.saturating_sub(ph)) / 2;
        let mut popup = Pane::new(px, py, pw, ph, 255, 235);
        popup.border = true;
        popup.border_refresh();

        let mut items: Vec<(&str, PrefType)> = vec![
            ("Default view", PrefType::Text(self.config.default_view.clone())),
            ("Color theme", PrefType::Choice(vec!["Default", "Mutt", "Ocean", "Forest", "Amber"], self.config.color_theme.clone())),
            ("Date format", PrefType::Choice(vec!["%b %e", "%d/%m %H:%M", "%m/%d %H:%M", "%Y-%m-%d %H:%M", "%d.%m %H:%M", "%d %b %H:%M", "%b %d %H:%M"], self.date_format.clone())),
            ("Sort order", PrefType::Choice(vec!["latest", "alphabetical", "sender", "from", "conversation", "unread", "source"], self.sort_order.clone())),
            ("Sort inverted", PrefType::Bool(self.sort_inverted)),
            ("Pane width", PrefType::Num(self.width as u8, 1, 6)),
            ("Border style", PrefType::Num(self.border, 0, 3)),
            ("Confirm purge", PrefType::Bool(self.config.confirm_purge)),
            ("Download folder", PrefType::Text(self.config.download_folder.clone())),
            ("Editor args", PrefType::Text(self.config.editor_args.clone())),
            ("Default email", PrefType::Text(self.config.default_email.clone())),
            ("SMTP command", PrefType::Text(self.config.smtp_command.clone())),
        ];

        let mut sel = 0usize;
        let mut dirty = false;
        let mut theme_preset_changed = false;

        loop {
            let mut lines = Vec::new();
            lines.push(format!(" {}", style::fg(&style::bold("Preferences"), self.config.theme_colors.view_custom)));
            lines.push(String::new());

            for (i, (label, ptype)) in items.iter().enumerate() {
                let label_fmt = format!("{:<18}", label);
                let max_val = (pw as usize).saturating_sub(26);
                let value_str = match ptype {
                    PrefType::Bool(v) => if *v { style::fg("Yes", self.config.theme_colors.feedback_ok) } else { style::fg("No", 196) },
                    PrefType::Choice(_, current) => style::fg(current, self.config.theme_colors.view_custom),
                    PrefType::Text(v) => if v.len() > max_val { format!("{}...", &v[..max_val.saturating_sub(3)]) } else { v.clone() },
                    PrefType::Num(v, _, _) => format!("{}", v),
                };
                if i == sel {
                    lines.push(format!(" {} \u{25C0} {} \u{25B6}", style::reverse(&label_fmt), value_str));
                } else {
                    lines.push(format!(" {}   {}  ", label_fmt, value_str));
                }
            }
            lines.push(String::new());
            lines.push(style::fg(" j/k navigate  l/h change  Enter edit  W:Save  ESC:Close", self.config.theme_colors.hint_fg));

            popup.set_text(&lines.join("\n"));
            popup.ix = 0;
            popup.full_refresh();

            let Some(key) = Input::getchr(None) else { continue };
            match key.as_str() {
                "ESC" | "q" => { dirty = false; break; }
                "W" => { break; } // dirty stays true, will save
                "j" | "DOWN" => { if sel < items.len() - 1 { sel += 1; } }
                "k" | "UP" => { if sel > 0 { sel -= 1; } }
                "l" | "RIGHT" => { next_pref(&mut items[sel].1); dirty = true; if sel == 1 { theme_preset_changed = true; } }
                "h" | "LEFT" => { prev_pref(&mut items[sel].1); dirty = true; if sel == 1 { theme_preset_changed = true; } }
                "ENTER" => {
                    if sel == 1 {
                        // Color theme: open theme color detail editor
                        self.show_theme_colors_popup();
                        dirty = true;
                        // Full redraw after theme color sub-popup
                        self.handle_resize();
                        popup.full_refresh();
                        popup.border_refresh();
                    } else {
                        let label = items[sel].0.to_string();
                        match &mut items[sel].1 {
                            PrefType::Text(val) => {
                                let new_val = self.prompt(&format!("{}: ", label), val);
                                if !new_val.is_empty() { *val = new_val; dirty = true; }
                                // Restore popup's bottom hint (prompt overwrites status bar)
                                self.bottom.say(&style::fg(" j/k navigate  l/h change  Enter edit  W:Save  ESC:Close", self.config.theme_colors.hint_fg));
                            }
                            _ => { next_pref(&mut items[sel].1); dirty = true; }
                        }
                    }
                }
                _ => {}
            }
        }

        // Apply settings back
        if dirty {
            for (label, ptype) in &items {
                match (*label, ptype) {
                    ("Default view", PrefType::Text(v)) => self.config.default_view = v.clone(),
                    ("Color theme", PrefType::Choice(_, v)) => {
                        self.config.color_theme = v.clone();
                        if theme_preset_changed {
                            self.config.theme_colors = config::ThemeColors::for_theme(v);
                        }
                    }
                    ("Date format", PrefType::Choice(_, v)) => { self.date_format = v.clone(); self.config.date_format = v.clone(); }
                    ("Sort order", PrefType::Choice(_, v)) => self.sort_order = v.clone(),
                    ("Sort inverted", PrefType::Bool(v)) => self.sort_inverted = *v,
                    ("Pane width", PrefType::Num(v, _, _)) => {
                        self.width = *v as u16;
                        self.handle_resize();
                    }
                    ("Border style", PrefType::Num(v, _, _)) => {
                        self.border = *v;
                        self.handle_resize();
                    }
                    ("Confirm purge", PrefType::Bool(v)) => self.config.confirm_purge = *v,
                    ("Download folder", PrefType::Text(v)) => self.config.download_folder = v.clone(),
                    ("Editor args", PrefType::Text(v)) => self.config.editor_args = v.clone(),
                    ("Default email", PrefType::Text(v)) => self.config.default_email = v.clone(),
                    ("SMTP command", PrefType::Text(v)) => self.config.smtp_command = v.clone(),
                    _ => {}
                }
            }
            self.config.save();
            self.sort_messages();
            self.rebuild_display();
        }
        self.handle_resize(); // Rebuild panes (restore_view_top_bg called inside)
        if self.left.border { self.left.border_refresh(); }
        if self.right.border { self.right.border_refresh(); }
        self.render_top_bar();
    }

    fn show_theme_colors_popup(&mut self) {
        let pw = 50u16.min(self.cols.saturating_sub(4));
        let ph = 34u16.min(self.rows.saturating_sub(4));
        let px = (self.cols.saturating_sub(pw)) / 2;
        let py = 3;
        let mut popup = Pane::new(px, py, pw, ph, 255, 235);
        popup.border = true;
        popup.scroll = true;
        popup.border_refresh();

        // Build editable color list from theme_colors
        let mut colors: Vec<(&str, u8)> = vec![
            ("Unread", self.config.theme_colors.unread),
            ("Read", self.config.theme_colors.read),
            ("Accent", self.config.theme_colors.accent),
            ("Thread", self.config.theme_colors.thread),
            ("DM", self.config.theme_colors.dm),
            ("Tag", self.config.theme_colors.tag),
            ("Star", self.config.theme_colors.star),
            ("Quote 1", self.config.theme_colors.quote1),
            ("Quote 2", self.config.theme_colors.quote2),
            ("Quote 3", self.config.theme_colors.quote3),
            ("Quote 4", self.config.theme_colors.quote4),
            ("Signature", self.config.theme_colors.sig),
            ("Link", self.config.theme_colors.link),
            ("Email", self.config.theme_colors.src_email),
            ("Discord", self.config.theme_colors.src_discord),
            ("Slack", self.config.theme_colors.src_slack),
            ("Telegram", self.config.theme_colors.src_telegram),
            ("WhatsApp", self.config.theme_colors.src_whatsapp),
            ("Reddit", self.config.theme_colors.src_reddit),
            ("RSS", self.config.theme_colors.src_rss),
            ("Web", self.config.theme_colors.src_web),
            ("Messenger", self.config.theme_colors.src_messenger),
            ("Instagram", self.config.theme_colors.src_instagram),
            ("WeeChat", self.config.theme_colors.src_weechat),
            ("Content fg", self.config.theme_colors.content_fg),
            ("Content bg", self.config.theme_colors.content_bg),
            ("List fg", self.config.theme_colors.list_fg),
            ("List bg", self.config.theme_colors.list_bg),
            ("Border fg", self.config.theme_colors.border_fg),
        ];

        let mut sel = 0usize;
        let mut save = false;

        loop {
            let mut lines = Vec::new();
            lines.push(format!(" {}", style::fg(&style::bold("Theme Colors"), self.config.theme_colors.view_custom)));
            lines.push(String::new());
            for (i, (label, val)) in colors.iter().enumerate() {
                let swatch = style::fg("\u{2588}\u{2588}\u{2588}", *val);
                let label_fmt = format!("{:<12}", label);
                if i == sel {
                    lines.push(format!(" {} \u{25C0} {} {:>3} \u{25B6}", style::reverse(&label_fmt), swatch, val));
                } else {
                    lines.push(format!(" {}   {} {:>3}  ", label_fmt, swatch, val));
                }
            }
            lines.push(String::new());
            lines.push(style::fg(" h/l:\u{00B1}1  H/L:\u{00B1}10  Enter:type  W:Save  ESC:Close", self.config.theme_colors.hint_fg));

            popup.set_text(&lines.join("\n"));
            // Scroll to keep selected item visible (sel + 2 for header lines)
            let vis_h = popup.h as usize;
            let item_line = sel + 2; // 2 header lines before items
            if item_line >= popup.ix + vis_h.saturating_sub(2) {
                popup.ix = (item_line + 3).saturating_sub(vis_h);
            } else if item_line < popup.ix + 1 {
                popup.ix = item_line.saturating_sub(1);
            }
            popup.full_refresh();

            let Some(key) = Input::getchr(None) else { continue };
            match key.as_str() {
                "ESC" | "q" => { save = false; break; }
                "W" => { save = true; break; }
                "j" | "DOWN" => { if sel < colors.len() - 1 { sel += 1; } }
                "k" | "UP" => { if sel > 0 { sel -= 1; } }
                "l" | "RIGHT" => { colors[sel].1 = (colors[sel].1 as u16 + 1).min(255) as u8; }
                "h" | "LEFT" => { colors[sel].1 = colors[sel].1.saturating_sub(1); }
                "L" => { colors[sel].1 = (colors[sel].1 as u16 + 10).min(255) as u8; }
                "H" => { colors[sel].1 = colors[sel].1.saturating_sub(10); }
                "ENTER" => {
                    let input = self.prompt("Color (0-255): ", &colors[sel].1.to_string());
                    self.render_bottom_bar();
                    if let Ok(v) = input.parse::<u8>() { colors[sel].1 = v; }
                }
                _ => {}
            }
        }

        if !save { return; }

        // Apply colors back
        let tc = &mut self.config.theme_colors;
        tc.unread = colors[0].1;   tc.read = colors[1].1;
        tc.accent = colors[2].1;   tc.thread = colors[3].1;
        tc.dm = colors[4].1;       tc.tag = colors[5].1;
        tc.star = colors[6].1;     tc.quote1 = colors[7].1;
        tc.quote2 = colors[8].1;   tc.quote3 = colors[9].1;
        tc.quote4 = colors[10].1;  tc.sig = colors[11].1;
        tc.link = colors[12].1;    tc.src_email = colors[13].1;
        tc.src_discord = colors[14].1;  tc.src_slack = colors[15].1;
        tc.src_telegram = colors[16].1; tc.src_whatsapp = colors[17].1;
        tc.src_reddit = colors[18].1;   tc.src_rss = colors[19].1;
        tc.src_web = colors[20].1;      tc.src_messenger = colors[21].1;
        tc.src_instagram = colors[22].1; tc.src_weechat = colors[23].1;
        tc.content_fg = colors[24].1;  tc.content_bg = colors[25].1;
        tc.list_fg = colors[26].1;     tc.list_bg = colors[27].1;
        tc.border_fg = colors[28].1;
        // Apply pane colors
        self.left.fg = tc.list_fg as u16;
        self.left.bg = tc.list_bg as u16;
        self.left.border_fg = Some(tc.border_fg as u16);
        self.right.fg = tc.content_fg as u16;
        self.right.bg = tc.content_bg as u16;
        self.right.border_fg = Some(tc.border_fg as u16);
        self.config.save();
        self.render_all();
    }
}

// --- Compose / Reply / Forward ---

impl App {
    /// Get the current folder of the selected message (for identity resolution).
    fn current_folder(&self) -> Option<String> {
        self.filtered_messages.get(self.index)
            .and_then(|m| m.folder.clone())
    }

    /// Get the identity for the current context (folder-hook match).
    fn current_identity(&self) -> Option<&Identity> {
        let folder = self.current_folder();
        self.config.identity_for_folder(folder.as_deref())
    }

    /// Get the "From:" identity string for composing.
    fn compose_from(&self) -> String {
        if let Some(ident) = self.current_identity() {
            if !ident.name.is_empty() {
                format!("{} <{}>", ident.name, ident.email)
            } else {
                ident.email.clone()
            }
        } else {
            self.config.default_email.clone()
        }
    }

    /// Get the email address (bare) for the identity.
    fn compose_email(&self) -> String {
        if let Some(ident) = self.current_identity() {
            ident.email.clone()
        } else {
            self.config.default_email.clone()
        }
    }

    /// Get signature text for the identity, if any.
    fn compose_signature(&self) -> String {
        if let Some(ident) = self.current_identity() {
            let sig = ident.signature();
            if !sig.is_empty() {
                return format!("-- \n{}", sig);
            }
        }
        String::new()
    }

    /// Get the SMTP command for the current identity.
    fn compose_smtp(&self) -> String {
        if let Some(ident) = self.current_identity() {
            if let Some(ref smtp) = ident.smtp {
                return smtp.clone();
            }
        }
        let home = std::env::var("HOME").unwrap_or_default();
        self.config.smtp_command.replace("~/", &format!("{}/", home))
    }

    /// Ensure the selected message has full content loaded.
    fn ensure_full_content(&mut self) {
        if self.index >= self.filtered_messages.len() { return; }
        if !self.filtered_messages[self.index].full_loaded {
            let msg_id = self.filtered_messages[self.index].id;
            if let Some((content, html)) = self.db.get_message_content(msg_id) {
                self.filtered_messages[self.index].content = content;
                self.filtered_messages[self.index].html_content = html;
                self.filtered_messages[self.index].full_loaded = true;
            }
        }
    }

    /// Render a sender template: substitute @conv, @msg, @to, @emoji placeholders.
    fn render_sender_template(template: &str, repl: &[(&str, &str)]) -> String {
        let mut out = template.to_string();
        for (k, v) in repl {
            out = out.replace(&format!("@{}", k), v);
        }
        out
    }

    /// Run an external sender command (shell). Pipes `body` to stdin when non-empty.
    /// Returns (success, combined_output).
    fn run_sender_command(&mut self, cmd: &str, body: Option<&str>) -> (bool, String) {
        use std::io::Write;
        use std::process::{Command, Stdio};
        let body = body.unwrap_or("");
        let mut child = match Command::new("sh").arg("-c").arg(cmd)
            .stdin(if !body.is_empty() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(c) => c,
            Err(e) => return (false, format!("spawn failed: {}", e)),
        };
        if !body.is_empty() {
            if let Some(stdin) = child.stdin.as_mut() {
                let _ = stdin.write_all(body.as_bytes());
            }
        }
        match child.wait_with_output() {
            Ok(out) => {
                let mut combined = String::from_utf8_lossy(&out.stdout).to_string();
                if !out.stderr.is_empty() {
                    combined.push_str(&String::from_utf8_lossy(&out.stderr));
                }
                (out.status.success(), combined)
            }
            Err(e) => (false, format!("wait failed: {}", e)),
        }
    }

    /// Look up a sender command for the given plugin_type + action, render with
    /// placeholder substitutions, run it, pipe body. On success, if a `sync`
    /// command is configured for the same plugin_type, invoke it too so kastrup
    /// can see the new state on the next view refresh.
    fn dispatch_external_action(
        &mut self,
        plugin_type: &str,
        action: &str,
        repl: &[(&str, &str)],
        body: Option<&str>,
    ) -> Result<(), String> {
        let cmd_template = self.config.senders
            .get(plugin_type)
            .and_then(|m| m.get(action))
            .cloned()
            .ok_or_else(|| format!("no sender config for plugin_type='{}' action='{}'", plugin_type, action))?;
        let cmd = Self::render_sender_template(&cmd_template, repl);
        log::info(&format!("external sender: plugin={} action={} cmd={}", plugin_type, action, cmd));
        let (ok, output) = self.run_sender_command(&cmd, body);
        if !ok {
            return Err(output.trim().to_string());
        }
        // Best-effort post-sync so the UI catches up; don't fail the caller if it errors.
        let sync_template = self.config.senders.get(plugin_type)
            .and_then(|m| m.get("sync"))
            .cloned();
        if let Some(sync_cmd) = sync_template {
            let _ = self.run_sender_command(&sync_cmd, None);
        }
        Ok(())
    }

    /// Open `$EDITOR` on a blank tempfile; return the trimmed body. The editor
    /// runs with the TUI torn down; on return we restore terminal state AND
    /// redraw every pane so the caller's set_feedback lands on a visible UI.
    fn edit_body_tempfile(&mut self) -> Option<String> {
        let tmpfile = format!("/tmp/kastrup_body_{}.txt", std::process::id());
        if std::fs::write(&tmpfile, "").is_err() { return None; }
        let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vim".into());
        Crust::cleanup();
        let _ = std::process::Command::new("sh").arg("-c")
            .arg(format!("{} {}", editor, crust::shell_escape(&tmpfile)))
            .status();
        Crust::init();
        Crust::clear_screen();
        self.handle_resize();
        self.render_all();
        let body = std::fs::read_to_string(&tmpfile).ok()?.trim_end().to_string();
        let _ = std::fs::remove_file(&tmpfile);
        if body.is_empty() { None } else { Some(body) }
    }

    /// Reply to the selected message via an external sender (workspace, etc).
    /// Returns true when handled — caller should skip the email reply flow.
    fn maybe_external_reply(&mut self) -> bool {
        if self.filtered_messages.is_empty() { return false; }
        // Snapshot selected-message fields up front — holding &self across
        // mutable calls (set_feedback / edit_body_tempfile / dispatch) would
        // trip the borrow checker.
        let (plugin_type, conv, msg_id, folder) = {
            let msg = &self.filtered_messages[self.index];
            (
                msg.source_type.clone(),
                msg.metadata.get("conversation_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                msg.external_id.clone(),
                msg.folder.clone().unwrap_or_default(),
            )
        };
        if !self.config.senders.get(&plugin_type).map(|m| m.contains_key("reply")).unwrap_or(false) {
            return false;
        }
        if conv.is_empty() {
            self.set_feedback("reply: no conversation_id in metadata",
                self.config.theme_colors.feedback_warn);
            return true;
        }
        self.set_feedback(&format!("Reply to {} — opening editor...", folder),
            self.config.theme_colors.accent);
        let Some(body) = self.edit_body_tempfile() else {
            self.set_feedback("reply cancelled", self.config.theme_colors.feedback_info);
            return true;
        };
        self.set_feedback(&format!("Sending reply to {}...", folder),
            self.config.theme_colors.accent);
        let result = self.dispatch_external_action(&plugin_type, "reply",
            &[("conv", &conv), ("msg", &msg_id), ("to", &msg_id)], Some(&body));
        match result {
            Ok(()) => {
                self.set_feedback(&format!("Reply sent to {}", folder),
                    self.config.theme_colors.feedback_ok);
                self.refresh_current_view();
            }
            Err(e) => self.set_feedback(&format!("Reply failed: {}", e),
                self.config.theme_colors.feedback_warn),
        }
        true
    }

    /// A reachable compose target within the current view.
    fn collect_compose_targets(&self) -> Vec<ComposeTarget> {
        let mut seen: std::collections::HashSet<(i64, String, String)> = std::collections::HashSet::new();
        let mut out: Vec<ComposeTarget> = Vec::new();
        for m in &self.filtered_messages {
            let plugin_type = m.source_type.clone();
            if !self.config.senders.get(&plugin_type)
                .map(|s| s.contains_key("send")).unwrap_or(false) { continue; }
            let conv = m.metadata.get("conversation_id").and_then(|v| v.as_str())
                .unwrap_or("").to_string();
            let folder = m.folder.clone().unwrap_or_default();
            if conv.is_empty() { continue; }
            let key = (m.source_id, folder.clone(), conv.clone());
            if !seen.insert(key) { continue; }
            out.push(ComposeTarget {
                plugin_type,
                conversation_id: conv,
                folder: if folder.is_empty() { "(unnamed)".into() } else { folder },
                source_id: m.source_id,
                recent_ts: m.timestamp,
            });
        }
        out.sort_by(|a, b| a.plugin_type.cmp(&b.plugin_type)
            .then(b.recent_ts.cmp(&a.recent_ts))
            .then(a.folder.cmp(&b.folder)));
        out
    }

    /// Render the cross-source picker in the right pane and prompt for a choice.
    fn pick_compose_target(&mut self, targets: &[ComposeTarget], default_ix: usize) -> Option<usize> {
        let tc = self.config.theme_colors.clone();
        let mut lines = vec![
            style::bold(&style::fg("Compose target:", tc.unread)),
            String::new(),
        ];
        let mut cur_type = String::new();
        for (i, t) in targets.iter().enumerate() {
            if t.plugin_type != cur_type {
                if !cur_type.is_empty() { lines.push(String::new()); }
                lines.push(style::fg(&format!("{}:", t.plugin_type), tc.accent));
                cur_type = t.plugin_type.clone();
            }
            let marker = if i == default_ix { "→" } else { " " };
            lines.push(format!(" {} {:>3}. {}", marker, i + 1, t.folder));
        }
        lines.push(String::new());
        lines.push(style::fg("Enter number, Enter=default, ESC=cancel", 245));
        self.right.set_text(&lines.join("\n"));
        self.right.full_refresh();

        let input = self.prompt(&format!("Target # [{}]: ", default_ix + 1), "");
        let trimmed = input.trim();
        if trimmed.is_empty() {
            Some(default_ix)
        } else if let Ok(n) = trimmed.parse::<usize>() {
            if n >= 1 && n <= targets.len() { Some(n - 1) } else { None }
        } else {
            None
        }
    }

    /// Compose a new message via an external sender. Inherits the current
    /// message's channel as default target; user presses `c` to pick another
    /// reachable channel from anywhere in the current view.
    /// Returns true when handled — false = fall through to email compose.
    fn maybe_external_compose(&mut self) -> bool {
        if self.filtered_messages.is_empty() { return false; }

        let targets = self.collect_compose_targets();
        if targets.is_empty() { return false; }

        // Default: the currently-selected message's channel if it's reachable,
        // otherwise the first listed (most recent in the dominant source).
        let selected = &self.filtered_messages[self.index];
        let default_ix = targets.iter().position(|t| {
            t.source_id == selected.source_id
                && selected.metadata.get("conversation_id").and_then(|v| v.as_str())
                    .map_or(false, |c| c == t.conversation_id)
        }).unwrap_or(0);

        // Only the selected message's own source is reachable from this cursor?
        // If so, only one target in the view → skip picker entirely.
        let target = if targets.len() == 1 {
            &targets[0]
        } else {
            let d = &targets[default_ix];
            let tc = self.config.theme_colors.clone();
            self.set_feedback(
                &format!("Compose to {} ({})?  Enter=yes  c=change  ESC=cancel", d.folder, d.plugin_type),
                tc.accent);
            let Some(key) = Input::getchr(None) else { return true };
            match key.as_str() {
                "ENTER" => &targets[default_ix],
                "c" | "C" => {
                    let Some(ix) = self.pick_compose_target(&targets, default_ix) else {
                        self.set_feedback("compose cancelled", self.config.theme_colors.feedback_info);
                        return true;
                    };
                    &targets[ix]
                }
                _ => {
                    self.set_feedback("compose cancelled", self.config.theme_colors.feedback_info);
                    return true;
                }
            }
        };

        let plugin_type = target.plugin_type.clone();
        let conv = target.conversation_id.clone();
        let folder = target.folder.clone();

        self.set_feedback(&format!("Compose to {} ({}) — opening editor...", folder, plugin_type),
            self.config.theme_colors.accent);
        let Some(body) = self.edit_body_tempfile() else {
            self.set_feedback("compose cancelled", self.config.theme_colors.feedback_info);
            return true;
        };
        self.set_feedback(&format!("Sending to {}...", folder),
            self.config.theme_colors.accent);
        let result = self.dispatch_external_action(&plugin_type, "send",
            &[("conv", &conv)], Some(&body));
        match result {
            Ok(()) => {
                self.set_feedback(&format!("Sent to {}", folder),
                    self.config.theme_colors.feedback_ok);
                self.refresh_current_view();
            }
            Err(e) => self.set_feedback(&format!("Send failed: {}", e),
                self.config.theme_colors.feedback_warn),
        }
        true
    }

    /// Prompt for an emoji and add/remove a reaction via external sender.
    fn external_react(&mut self, remove: bool) {
        if self.filtered_messages.is_empty() { return; }
        let msg = &self.filtered_messages[self.index];
        let plugin_type = msg.source_type.clone();
        let action = if remove { "unreact" } else { "react" };
        if !self.config.senders.get(&plugin_type).map(|m| m.contains_key(action)).unwrap_or(false) {
            self.set_feedback(
                &format!("{}: no sender for plugin_type='{}' action='{}'",
                    if remove { "unreact" } else { "react" }, plugin_type, action),
                self.config.theme_colors.feedback_warn);
            return;
        }
        let conv = msg.metadata.get("conversation_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let msg_id = msg.external_id.clone();
        if conv.is_empty() {
            self.set_feedback("react: no conversation_id in metadata",
                self.config.theme_colors.feedback_warn);
            return;
        }
        let prompt = if remove { "Remove reaction: " } else { "React with emoji: " };
        let emoji = self.prompt(prompt, "");
        let emoji = emoji.trim().to_string();
        if emoji.is_empty() {
            self.set_feedback("cancelled", self.config.theme_colors.feedback_info);
            return;
        }
        match self.dispatch_external_action(&plugin_type, action,
            &[("conv", &conv), ("msg", &msg_id), ("emoji", &emoji)], None)
        {
            Ok(()) => self.set_feedback(
                &format!("{} {}", if remove { "Removed" } else { "Reacted" }, emoji),
                self.config.theme_colors.feedback_ok),
            Err(e) => self.set_feedback(&format!("React failed: {}", e),
                self.config.theme_colors.feedback_warn),
        }
    }

    fn reply(&mut self, _force_editor: bool) {
        if self.maybe_external_reply() { return; }
        if self.filtered_messages.is_empty() { return; }
        self.ensure_full_content();
        let msg = &self.filtered_messages[self.index];
        self.compose_source_type = Some(msg.source_type.clone());
        self.pending_reply_id = Some(msg.id);

        let sender = msg.sender_name.as_deref().unwrap_or(&msg.sender);
        let subject = msg.subject.as_deref().unwrap_or("");
        let re_subject = if subject.starts_with("Re:") {
            subject.to_string()
        } else {
            format!("Re: {}", subject)
        };
        let date = format_timestamp(msg.timestamp, "%Y-%m-%d %H:%M");
        let from = self.compose_from();
        let reply_to = self.compose_email();
        let sig = self.compose_signature();

        let mut template = String::new();
        template.push_str(&format!("From: {}\n", from));
        template.push_str(&format!("To: {}\n", msg.sender));
        template.push_str("Cc: \n");
        template.push_str("Bcc: \n");
        template.push_str(&format!("Reply-To: {}\n", reply_to));
        template.push_str(&format!("Subject: {}\n", re_subject));
        template.push('\n');
        template.push('\n');
        template.push_str(&format!("On {}, {} wrote:\n", date, sender));

        // Get content, falling back to HTML conversion
        let content = self.get_display_content(msg);
        for line in content.lines() {
            template.push_str(&format!("> {}\n", line));
        }

        if !sig.is_empty() {
            template.push('\n');
            template.push_str(&sig);
            template.push('\n');
        }

        self.run_editor_compose_at(&template, None);
    }

    fn reply_all(&mut self) {
        if self.filtered_messages.is_empty() { return; }
        self.ensure_full_content();
        let msg = &self.filtered_messages[self.index];
        self.compose_source_type = Some(msg.source_type.clone());
        self.pending_reply_id = Some(msg.id);

        let sender = msg.sender_name.as_deref().unwrap_or(&msg.sender);
        let subject = msg.subject.as_deref().unwrap_or("");
        let re_subject = if subject.starts_with("Re:") {
            subject.to_string()
        } else {
            format!("Re: {}", subject)
        };
        let date = format_timestamp(msg.timestamp, "%Y-%m-%d %H:%M");
        let from = self.compose_from();
        let reply_to = self.compose_email();
        let my_email = reply_to.to_lowercase();
        let sig = self.compose_signature();

        // Build Cc from original recipients + cc, minus self and original sender
        let to_list = parse_json_recipients(&msg.recipients);
        let cc_list = msg.cc.as_deref().map(parse_json_recipients).unwrap_or_default();
        let all_cc: Vec<&str> = to_list
            .split(", ")
            .chain(cc_list.split(", "))
            .filter(|a| {
                !a.is_empty()
                    && !a.to_lowercase().contains(&my_email)
                    && !a.to_lowercase().contains(&msg.sender.to_lowercase())
            })
            .collect();
        let cc = all_cc.join(", ");

        let mut template = String::new();
        template.push_str(&format!("From: {}\n", from));
        template.push_str(&format!("To: {}\n", msg.sender));
        template.push_str(&format!("Cc: {}\n", cc));
        template.push_str("Bcc: \n");
        template.push_str(&format!("Reply-To: {}\n", reply_to));
        template.push_str(&format!("Subject: {}\n", re_subject));
        template.push('\n');
        template.push('\n');
        template.push_str(&format!("On {}, {} wrote:\n", date, sender));

        let content = self.get_display_content(msg);
        for line in content.lines() {
            template.push_str(&format!("> {}\n", line));
        }

        if !sig.is_empty() {
            template.push('\n');
            template.push_str(&sig);
            template.push('\n');
        }

        self.run_editor_compose_at(&template, None);
    }

    fn forward_inline(&mut self) {
        if self.filtered_messages.is_empty() { return; }
        self.compose_source_type = Some("email".to_string()); // forwarding is always email
        self.ensure_full_content();
        let msg = &self.filtered_messages[self.index];
        self.pending_forward_ids = vec![msg.id];

        let sender = msg.sender_name.as_deref().unwrap_or(&msg.sender);
        let subject = msg.subject.as_deref().unwrap_or("");
        let fwd_subject = if subject.starts_with("Fwd:") {
            subject.to_string()
        } else {
            format!("Fwd: {}", subject)
        };
        let date = format_timestamp(msg.timestamp, "%Y-%m-%d %H:%M");
        let from = self.compose_from();
        let reply_to = self.compose_email();
        let sig = self.compose_signature();

        let mut template = String::new();
        template.push_str(&format!("From: {}\n", from));
        template.push_str("To: \n");
        template.push_str("Cc: \n");
        template.push_str("Bcc: \n");
        template.push_str(&format!("Reply-To: {}\n", reply_to));
        template.push_str(&format!("Subject: {}\n", fwd_subject));
        template.push('\n');
        template.push('\n');
        template.push_str("---------- Forwarded message ----------\n");
        template.push_str(&format!("From: {}\n", sender));
        template.push_str(&format!("Date: {}\n", date));
        template.push_str(&format!("Subject: {}\n", subject));
        template.push('\n');

        let content = self.get_display_content(msg);
        template.push_str(&content);
        template.push('\n');

        if !sig.is_empty() {
            template.push('\n');
            template.push_str(&sig);
            template.push('\n');
        }

        // Collect original message attachments for forwarding
        self.pending_forward_attachments.clear();
        if let Some(m) = self.filtered_messages.get(self.index) {
            for att in &m.attachments {
                if let Some(path) = att.get("source_file").and_then(|v| v.as_str()) {
                    if std::path::Path::new(path).exists() {
                        self.pending_forward_attachments.push(path.to_string());
                    }
                }
            }
        }

        self.run_editor_compose_at(&template, Some(2)); // cursor on To: line
    }

    fn forward_tagged_inline(&mut self) {
        let tagged_ids: Vec<i64> = self.tagged.iter().copied().collect();
        if tagged_ids.is_empty() { return; }
        self.pending_forward_ids = tagged_ids.clone();

        let from = self.compose_from();
        let reply_to = self.compose_email();
        let sig = self.compose_signature();

        let subject = if tagged_ids.len() == 1 {
            let msg = self.filtered_messages.iter().find(|m| m.id == tagged_ids[0]);
            let subj = msg.and_then(|m| m.subject.as_deref()).unwrap_or("");
            format!("Fwd: {}", subj)
        } else {
            format!("Fwd: {} messages", tagged_ids.len())
        };

        let mut template = String::new();
        template.push_str(&format!("From: {}\n", from));
        template.push_str("To: \n");
        template.push_str("Cc: \n");
        template.push_str("Bcc: \n");
        template.push_str(&format!("Reply-To: {}\n", reply_to));
        template.push_str(&format!("Subject: {}\n", subject));
        template.push('\n');
        template.push('\n');

        // Load full content for each tagged message and append
        for &id in &tagged_ids {
            // Load content if needed
            if let Some(msg) = self.filtered_messages.iter_mut().find(|m| m.id == id) {
                if !msg.full_loaded {
                    if let Some((content, html)) = self.db.get_message_content(id) {
                        msg.content = content;
                        msg.html_content = html;
                        msg.full_loaded = true;
                    }
                }
            }
            if let Some(msg) = self.filtered_messages.iter().find(|m| m.id == id) {
                let sender = msg.sender_name.as_deref().unwrap_or(&msg.sender);
                let subj = msg.subject.as_deref().unwrap_or("");
                let date = format_timestamp(msg.timestamp, "%Y-%m-%d %H:%M");

                template.push_str("---------- Forwarded message ----------\n");
                template.push_str(&format!("From: {}\n", sender));
                template.push_str(&format!("Date: {}\n", date));
                template.push_str(&format!("Subject: {}\n", subj));
                template.push('\n');

                let content = self.get_display_content(msg);
                template.push_str(&content);
                template.push_str("\n\n");
            }
        }

        if !sig.is_empty() {
            template.push_str(&sig);
            template.push('\n');
        }

        self.run_editor_compose_at(&template, Some(2));
    }

    fn forward_attach(&mut self) {
        if self.filtered_messages.is_empty() { return; }
        let msg = &self.filtered_messages[self.index];
        self.compose_source_type = Some("email".to_string()); // forwarding is always email
        self.pending_forward_ids = vec![msg.id];

        let subject = msg.subject.as_deref().unwrap_or("");
        let fwd_subject = if subject.starts_with("Fwd:") { subject.to_string() } else { format!("Fwd: {}", subject) };
        let from = self.compose_from();
        let reply_to = self.compose_email();
        let sig = self.compose_signature();

        // Collect the maildir file as attachment
        self.pending_forward_attachments.clear();
        if let Some(file) = msg.metadata.get("maildir_file").and_then(|v| v.as_str()) {
            if std::path::Path::new(file).exists() {
                // Copy to temp with .eml extension
                let name = msg.subject.as_deref().unwrap_or("message").replace('/', "_");
                let eml_path = format!("/tmp/kastrup_fwd_{}.eml", msg.id);
                let _ = std::fs::copy(file, &eml_path);
                self.pending_forward_attachments.push(eml_path);
            }
        }
        // Also include any extracted MIME attachments
        for att in &msg.attachments {
            if let Some(path) = att.get("source_file").and_then(|v| v.as_str()) {
                if std::path::Path::new(path).exists() {
                    self.pending_forward_attachments.push(path.to_string());
                }
            }
        }

        let mut template = String::new();
        template.push_str(&format!("From: {}\n", from));
        template.push_str("To: \n");
        template.push_str("Cc: \n");
        template.push_str("Bcc: \n");
        template.push_str(&format!("Reply-To: {}\n", reply_to));
        template.push_str(&format!("Subject: {}\n", fwd_subject));
        template.push('\n');
        let att_count = self.pending_forward_attachments.len();
        if att_count == 1 {
            template.push_str("[Forwarded message attached]\n");
        } else if att_count > 1 {
            template.push_str(&format!("[{} forwarded attachments]\n", att_count));
        }
        template.push('\n');
        if !sig.is_empty() {
            template.push_str(&sig);
            template.push('\n');
        }
        self.run_editor_compose_at(&template, Some(2));
    }

    fn forward_tagged_attach(&mut self) {
        let tagged_ids: Vec<i64> = self.tagged.iter().copied().collect();
        if tagged_ids.is_empty() { return; }
        self.pending_forward_ids = tagged_ids.clone();

        let from = self.compose_from();
        let reply_to = self.compose_email();
        let sig = self.compose_signature();
        let subject = format!("Fwd: {} messages", tagged_ids.len());

        self.pending_forward_attachments.clear();
        for &id in &tagged_ids {
            if let Some(msg) = self.filtered_messages.iter().find(|m| m.id == id) {
                if let Some(file) = msg.metadata.get("maildir_file").and_then(|v| v.as_str()) {
                    if std::path::Path::new(file).exists() {
                        let eml_path = format!("/tmp/kastrup_fwd_{}.eml", id);
                        let _ = std::fs::copy(file, &eml_path);
                        self.pending_forward_attachments.push(eml_path);
                    }
                }
                for att in &msg.attachments {
                    if let Some(path) = att.get("source_file").and_then(|v| v.as_str()) {
                        if std::path::Path::new(path).exists() {
                            self.pending_forward_attachments.push(path.to_string());
                        }
                    }
                }
            }
        }

        let mut template = String::new();
        template.push_str(&format!("From: {}\n", from));
        template.push_str("To: \n");
        template.push_str("Cc: \n");
        template.push_str("Bcc: \n");
        template.push_str(&format!("Reply-To: {}\n", reply_to));
        template.push_str(&format!("Subject: {}\n", subject));
        template.push('\n');
        template.push_str(&format!("[{} forwarded messages attached]\n", tagged_ids.len()));
        template.push('\n');
        if !sig.is_empty() {
            template.push_str(&sig);
            template.push('\n');
        }
        self.run_editor_compose_at(&template, Some(2));
    }

    fn compose_to(&mut self, to: &str, subject: &str) {
        let from = self.compose_from();
        let reply_to = self.compose_email();
        let sig = self.compose_signature();

        let mut template = String::new();
        template.push_str(&format!("From: {}\n", from));
        template.push_str(&format!("To: {}\n", to));
        template.push_str("Cc: \n");
        template.push_str("Bcc: \n");
        template.push_str(&format!("Reply-To: {}\n", reply_to));
        template.push_str(&format!("Subject: {}\n", subject));
        template.push('\n');
        template.push('\n');

        if !sig.is_empty() {
            template.push_str(&sig);
            template.push('\n');
        }

        self.run_editor_compose_at(&template, None);
    }

    fn compose_new(&mut self) {
        if self.maybe_external_compose() { return; }
        self.pending_reply_id = None;
        // Check for postponed messages
        let conn = self.db.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM postponed", [], |r| r.get(0)).unwrap_or(0);
        drop(conn);

        if count > 0 {
            self.set_feedback(&format!("{} postponed draft(s). Recall? (y/n)", count), self.config.theme_colors.unread);
            if let Some(key) = Input::getchr(Some(5)) {
                if key == "y" || key == "Y" {
                    let conn = self.db.conn.lock().unwrap();
                    let draft: Option<(i64, String)> = conn.query_row(
                        "SELECT id, data FROM postponed ORDER BY created_at DESC LIMIT 1",
                        [], |r| Ok((r.get(0)?, r.get(1)?))
                    ).ok();
                    if let Some((draft_id, data)) = draft {
                        let _ = conn.execute("DELETE FROM postponed WHERE id = ?", rusqlite::params![draft_id]);
                        drop(conn);
                        self.run_editor_compose_at(&data, None);
                        return;
                    }
                }
            }
        }

        // Set compose source type from current message context
        self.compose_source_type = if !self.filtered_messages.is_empty() {
            Some(self.filtered_messages[self.index].source_type.clone())
        } else {
            Some("email".to_string())
        };

        // Normal compose
        let from = self.compose_from();
        let reply_to = self.compose_email();
        let sig = self.compose_signature();

        let mut template = String::new();
        template.push_str(&format!("From: {}\n", from));
        template.push_str("To: \n");
        template.push_str("Cc: \n");
        template.push_str("Bcc: \n");
        template.push_str(&format!("Reply-To: {}\n", reply_to));
        template.push_str("Subject: \n");
        template.push('\n');
        template.push('\n');

        if !sig.is_empty() {
            template.push_str(&sig);
            template.push('\n');
        }

        self.run_editor_compose_at(&template, Some(2)); // cursor on To: line
    }

    /// Get displayable text content from a message, converting HTML if needed.
    fn get_display_content(&self, msg: &Message) -> String {
        let raw = &msg.content;
        // MIME extraction + QP/base64 decoding (same logic as render_message_content)
        let looks_mime = raw.contains("Content-Type:")
            || raw.lines().any(|l| l.starts_with("--") && l.len() > 5);
        let extracted = if looks_mime {
            extract_mime_text(raw).unwrap_or_else(|| raw.clone())
        } else if raw.contains("Content-Transfer-Encoding: quoted-printable") {
            let body_start = raw.find("\n\n").map(|p| p + 2)
                .or_else(|| raw.find("\r\n\r\n").map(|p| p + 4))
                .unwrap_or(0);
            decode_quoted_printable(&raw[body_start..])
        } else if looks_base64(raw) {
            sources::maildir::base64_decode(raw.trim())
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .unwrap_or_else(|| raw.clone())
        } else {
            raw.clone()
        };
        if let Some(ref html) = msg.html_content {
            let lc = extracted.to_lowercase();
            if extracted.trim().is_empty() || lc.contains("html messages are not support")
                || lc.contains("not displayed") || lc.contains("html-e-post")
                || lc.contains("støtter ikke html") || lc.contains("does not support html")
                || extracted.trim().len() < 20 {
                return html_to_text(html);
            }
        }
        if extracted.contains("<br") || extracted.contains("<p>") || extracted.contains("<p ")
            || (extracted.trim_start().starts_with('<') && (extracted.contains("<html") || extracted.contains("<body"))) {
            html_to_text(&extracted)
        } else {
            extracted
        }
    }

    /// Show composed message summary in the right pane for review before sending.
    /// Mark pending_forward_ids as forwarded in metadata.
    fn mark_forwarded(&mut self) {
        for &id in &self.pending_forward_ids {
            // Update metadata in DB
            let conn = self.db.conn.lock().unwrap();
            let meta: Option<String> = conn.query_row(
                "SELECT metadata FROM messages WHERE id = ?", rusqlite::params![id],
                |r| r.get(0)
            ).ok();
            if let Some(meta_str) = meta {
                let mut meta_val: serde_json::Value = serde_json::from_str(&meta_str)
                    .unwrap_or(serde_json::json!({}));
                meta_val["forwarded"] = serde_json::json!(true);
                let _ = conn.execute("UPDATE messages SET metadata = ? WHERE id = ?",
                    rusqlite::params![meta_val.to_string(), id]);
            }
            drop(conn);
            // Update in-memory
            for msg in &mut self.filtered_messages {
                if msg.id == id {
                    msg.metadata["forwarded"] = serde_json::json!(true);
                }
            }
        }
        self.pending_forward_ids.clear();
    }

    fn mark_replied(&mut self) {
        let id = match self.pending_reply_id.take() {
            Some(id) => id,
            None => return,
        };
        // Update DB
        let conn = self.db.conn.lock().unwrap();
        let _ = conn.execute("UPDATE messages SET replied = 1 WHERE id = ?",
            rusqlite::params![id]);
        drop(conn);
        // Update in-memory
        for msg in &mut self.filtered_messages {
            if msg.id == id {
                msg.replied = true;
            }
        }
        if self.show_threaded {
            for msg in &mut self.display_messages {
                if msg.id == id { msg.replied = true; }
            }
        }
    }

    fn show_compose_review(&mut self, content: &str, attachments: &[String]) {
        let tc = self.config.theme_colors.clone();
        let mut lines = Vec::new();
        let mut from = String::new();
        let mut to = String::new();
        let mut cc = String::new();
        let mut bcc = String::new();
        let mut subject = String::new();
        let mut body_lines = Vec::new();
        let mut in_body = false;

        for line in content.lines() {
            if in_body {
                body_lines.push(line);
            } else if line.trim().is_empty() {
                in_body = true;
            } else if let Some(v) = line.strip_prefix("From: ") { from = v.to_string(); }
            else if let Some(v) = line.strip_prefix("To: ") { to = v.to_string(); }
            else if let Some(v) = line.strip_prefix("Cc: ") { cc = v.to_string(); }
            else if let Some(v) = line.strip_prefix("Bcc: ") { bcc = v.to_string(); }
            else if let Some(v) = line.strip_prefix("Subject: ") { subject = v.to_string(); }
        }

        lines.push(style::bold(&style::fg("Review message before sending", tc.unread)));
        lines.push(style::fg(&"\u{2500}".repeat(40), tc.separator));
        lines.push(format!("{} {}", style::fg("From:", tc.header_from), style::fg(&from, tc.header_from)));
        lines.push(format!("{} {}", style::bold(&style::fg("To:", 46)), style::bold(&style::fg(&to, 46))));
        if !cc.is_empty() {
            lines.push(format!("{} {}", style::fg("Cc:", 51), style::fg(&cc, 51)));
        }
        if !bcc.is_empty() {
            lines.push(format!("{} {}", style::fg("Bcc:", 245), style::fg(&bcc, 245)));
        }
        lines.push(format!("{} {}", style::bold(&style::fg("Subject:", tc.header_subj)), style::bold(&style::fg(&subject, tc.header_subj))));
        lines.push(style::fg(&"\u{2500}".repeat(40), tc.separator));

        if !attachments.is_empty() {
            lines.push(style::bold(&style::fg(&format!("Attachments ({})", attachments.len()), tc.attachment)));
            for (i, a) in attachments.iter().enumerate() {
                let name = std::path::Path::new(a).file_name()
                    .map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| a.clone());
                let size = std::fs::metadata(a).map(|m| format_file_size(m.len())).unwrap_or_default();
                lines.push(style::fg(&format!("  [{}] {} {}", i + 1, name, size), tc.attachment));
            }
            lines.push(String::new());
        }

        // Show body preview (first 30 lines)
        for (i, line) in body_lines.iter().enumerate() {
            if i > 30 { lines.push(style::fg("  ...", 245)); break; }
            lines.push(line.to_string());
        }

        self.right.set_text(&lines.join("\n"));
        self.right.ix = 0;
        self.right.full_refresh();
        if self.right.border { self.right.border_refresh(); }
    }

    /// Expand short names in To/Cc/Bcc to full addresses.
    /// For ambiguous matches, shows an interactive picker.
    fn expand_compose_addresses(&mut self, content: &str) -> String {
        let mut result = String::new();
        let mut in_body = false;
        for line in content.lines() {
            if in_body {
                result.push_str(line);
                result.push('\n');
            } else if line.trim().is_empty() {
                in_body = true;
                result.push('\n');
            } else if let Some(val) = line.strip_prefix("To: ") {
                result.push_str(&format!("To: {}\n", self.expand_address_field_interactive(val)));
            } else if let Some(val) = line.strip_prefix("Cc: ") {
                result.push_str(&format!("Cc: {}\n", self.expand_address_field_interactive(val)));
            } else if let Some(val) = line.strip_prefix("Bcc: ") {
                result.push_str(&format!("Bcc: {}\n", self.expand_address_field_interactive(val)));
            } else {
                result.push_str(line);
                result.push('\n');
            }
        }
        result
    }

    /// Expand addresses with interactive picker for ambiguous names.
    fn expand_address_field_interactive(&mut self, field: &str) -> String {
        field.split(',').map(|addr| {
            let addr = addr.trim();
            if addr.is_empty() || addr.contains('@') || addr.contains('<') {
                return addr.to_string();
            }
            // Try auto-expand first (single match)
            let expanded = self.expand_address_field(addr);
            if expanded != addr {
                return expanded;
            }
            // Multiple or no matches: show picker
            self.pick_address(addr).unwrap_or_else(|| addr.to_string())
        }).collect::<Vec<_>>().join(", ")
    }

    /// Expand a comma-separated address field. Each part that doesn't contain '@'
    /// is looked up in message history (case-insensitive substring match on sender_name).
    /// If exactly one match: auto-expand. If multiple: show picker in right pane.
    fn expand_address_field(&self, field: &str) -> String {
        field.split(',').map(|addr| {
            let addr = addr.trim();
            if addr.is_empty() || addr.contains('@') || addr.contains('<') {
                return addr.to_string();
            }
            // Look up in messages by sender_name, filtered by compose context
            let conn = self.db.conn.lock().unwrap();
            let is_email = self.compose_source_type.as_deref().unwrap_or("email") == "email";
            let matches: Vec<(String, String)> = if is_email {
                conn.prepare(
                    "SELECT DISTINCT sender, sender_name FROM messages \
                     WHERE (sender_name LIKE ?1 OR sender LIKE ?1) AND sender LIKE '%@%' \
                     ORDER BY timestamp DESC LIMIT 20"
                ).ok().and_then(|mut stmt| {
                    let pattern = format!("%{}%", addr);
                    stmt.query_map(rusqlite::params![pattern], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1).unwrap_or_default()))
                    }).ok().map(|rows| rows.filter_map(|r| r.ok()).collect())
                }).unwrap_or_default()
            } else {
                let stype = self.compose_source_type.as_deref().unwrap_or("");
                conn.prepare(
                    "SELECT DISTINCT m.sender, m.sender_name FROM messages m \
                     JOIN sources s ON m.source_id = s.id \
                     WHERE (m.sender_name LIKE ?1 OR m.sender LIKE ?1) AND s.source_type = ?2 \
                     ORDER BY m.timestamp DESC LIMIT 20"
                ).ok().and_then(|mut stmt| {
                    let pattern = format!("%{}%", addr);
                    stmt.query_map(rusqlite::params![pattern, stype], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1).unwrap_or_default()))
                    }).ok().map(|rows| rows.filter_map(|r| r.ok()).collect())
                }).unwrap_or_default()
            };
            drop(conn);

            // Deduplicate by sender
            let mut seen = std::collections::HashSet::new();
            let unique: Vec<_> = matches.into_iter().filter(|(email, _)| {
                let key = email.to_lowercase();
                if seen.contains(&key) { false } else { seen.insert(key); true }
            }).collect();

            if unique.len() == 1 {
                let (email, name) = &unique[0];
                if !name.is_empty() { format!("{} <{}>", name, email) }
                else { email.clone() }
            } else if unique.len() > 1 {
                // Multiple matches: user must pick (handled in show_compose_review)
                // For now, return as-is; the review screen will flag it
                addr.to_string()
            } else {
                addr.to_string()
            }
        }).collect::<Vec<_>>().join(", ")
    }

    /// Show address picker when a To/Cc field has an unresolved name.
    /// Called when user presses 'e' to re-edit from the review screen.
    fn pick_address(&mut self, query: &str) -> Option<String> {
        let conn = self.db.conn.lock().unwrap();
        let is_email = self.compose_source_type.as_deref().unwrap_or("email") == "email";
        let matches: Vec<(String, String)> = if is_email {
            conn.prepare(
                "SELECT DISTINCT sender, sender_name FROM messages \
                 WHERE (sender_name LIKE ?1 OR sender LIKE ?1) AND sender LIKE '%@%' \
                 ORDER BY timestamp DESC LIMIT 20"
            ).ok().and_then(|mut stmt| {
                let pattern = format!("%{}%", query);
                stmt.query_map(rusqlite::params![pattern], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1).unwrap_or_default()))
                }).ok().map(|rows| rows.filter_map(|r| r.ok()).collect())
            }).unwrap_or_default()
        } else {
            let stype = self.compose_source_type.as_deref().unwrap_or("");
            conn.prepare(
                "SELECT DISTINCT m.sender, m.sender_name FROM messages m \
                 JOIN sources s ON m.source_id = s.id \
                 WHERE (m.sender_name LIKE ?1 OR m.sender LIKE ?1) AND s.source_type = ?2 \
                 ORDER BY m.timestamp DESC LIMIT 20"
            ).ok().and_then(|mut stmt| {
                let pattern = format!("%{}%", query);
                stmt.query_map(rusqlite::params![pattern, stype], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1).unwrap_or_default()))
                }).ok().map(|rows| rows.filter_map(|r| r.ok()).collect())
            }).unwrap_or_default()
        };
        drop(conn);

        let mut seen = std::collections::HashSet::new();
        let unique: Vec<_> = matches.into_iter().filter(|(email, _)| {
            let key = email.to_lowercase();
            if seen.contains(&key) { false } else { seen.insert(key); true }
        }).collect();

        if unique.is_empty() { return None; }
        if unique.len() == 1 {
            let (email, name) = &unique[0];
            return Some(if !name.is_empty() { format!("{} <{}>", name, email) } else { email.clone() });
        }

        // Show picker in right pane
        let tc = self.config.theme_colors.clone();
        let mut lines = Vec::new();
        lines.push(style::bold(&style::fg(&format!("Select address for \"{}\":", query), tc.unread)));
        lines.push(String::new());
        for (i, (email, name)) in unique.iter().enumerate() {
            let display = if !name.is_empty() { format!("{} <{}>", name, email) } else { email.clone() };
            lines.push(format!("  {} {}", style::fg(&format!("{}", i + 1), 220), display));
        }
        lines.push(String::new());
        lines.push(style::fg("Press number to select, ESC to cancel", 245));

        self.right.set_text(&lines.join("\n"));
        self.right.ix = 0;
        self.right.full_refresh();

        loop {
            let Some(key) = Input::getchr(None) else { return None };
            if key == "ESC" { return None; }
            if let Ok(n) = key.parse::<usize>() {
                if n >= 1 && n <= unique.len() {
                    let (email, name) = &unique[n - 1];
                    return Some(if !name.is_empty() { format!("{} <{}>", name, email) } else { email.clone() });
                }
            }
        }
    }

    fn load_compose_plugins(&self) -> Vec<(String, String, String)> {
        let dir = home_dir().join(".heathrow/plugins/compose");
        let mut plugins = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Ok(content) = std::fs::read_to_string(entry.path()) {
                    let key = regex::Regex::new(r"key:\s*'([^']+)'").ok()
                        .and_then(|r| r.captures(&content))
                        .and_then(|c| c.get(1))
                        .map(|m| m.as_str().to_string());
                    let label = regex::Regex::new(r"label:\s*'([^']+)'").ok()
                        .and_then(|r| r.captures(&content))
                        .and_then(|c| c.get(1))
                        .map(|m| m.as_str().to_string());
                    let command = regex::Regex::new(r"command:\s*'([^']+)'").ok()
                        .and_then(|r| r.captures(&content))
                        .and_then(|c| c.get(1))
                        .map(|m| m.as_str().to_string());
                    if let (Some(k), Some(l), Some(c)) = (key, label, command) {
                        plugins.push((k, l, c));
                    }
                }
            }
        }
        plugins
    }

    fn run_editor_compose_at(&mut self, template: &str, start_line: Option<usize>) {
        let tmpfile = format!("/tmp/kastrup_compose_{}.eml", std::process::id());
        if std::fs::write(&tmpfile, template).is_err() {
            self.set_feedback("Failed to create temp file", 196);
            return;
        }

        let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vim".into());
        let editor_args = self.config.editor_args.clone();

        Crust::cleanup();

        // Cursor position: explicit or default to body start (after first blank line)
        let cursor_line = start_line.unwrap_or_else(|| {
            template.lines().position(|l| l.is_empty()).unwrap_or(0) + 2
        });

        // Build full command string and pass through sh -c to handle quoted args properly
        let escaped_file = crust::shell_escape(&tmpfile);
        let cmd_str = if editor.contains("vim") || editor.contains("vi") {
            format!("{} +{} {} {}", editor, cursor_line, editor_args, escaped_file)
        } else {
            format!("{} {} {}", editor, editor_args, escaped_file)
        };
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg(&cmd_str)
            .status();

        Crust::init();
        Crust::clear_screen();

        if let Ok(s) = status {
            if s.success() {
                if let Ok(content) = std::fs::read_to_string(&tmpfile) {
                    if content.trim() != template.trim() {
                        let tc = self.config.theme_colors.clone();
                        self.handle_resize();
                        self.render_all(); // redraw full UI before address picker

                        // Expand addresses in the composed content
                        let mut final_content = content.clone();
                        final_content = self.expand_compose_addresses(&final_content);
                        let _ = std::fs::write(&tmpfile, &final_content);

                        // Post-editor loop with compose plugins and attachments
                        let mut attachments: Vec<String> = std::mem::take(&mut self.pending_forward_attachments);
                        let plugins = self.load_compose_plugins();
                        loop {
                            // Show message summary in right pane
                            self.show_compose_review(&final_content, &attachments);

                            let plugin_hints: String = plugins.iter()
                                .map(|(k, l, _)| format!(" {}:{}", k, l)).collect();
                            let att_hint = if attachments.is_empty() { String::new() }
                                else { format!(" [{}att]", attachments.len()) };
                            let prompt_text = format!(
                                " Enter:Send  e:Re-edit  p:Postpone  a:Attach{}{} ESC:Cancel",
                                plugin_hints, att_hint);
                            self.bottom.say(&style::fg(&prompt_text, 226));
                            let Some(key) = Input::getchr(None) else { continue };
                            match key.as_str() {
                                "ENTER" => {
                                    let final_content = std::fs::read_to_string(&tmpfile)
                                        .unwrap_or_else(|_| content.clone());
                                    // Warn if subject is empty (like mutt)
                                    let has_subject = final_content.lines()
                                        .take_while(|l| !l.is_empty())
                                        .any(|l| l.strip_prefix("Subject: ").is_some_and(|s| !s.trim().is_empty()));
                                    if !has_subject {
                                        self.set_feedback("No subject. Send anyway? (y/n)", tc.feedback_warn);
                                        if let Some(k) = Input::getchr(Some(5)) {
                                            if k != "y" && k != "Y" {
                                                self.set_feedback("Aborted", tc.feedback_info);
                                                continue;
                                            }
                                        } else {
                                            continue;
                                        }
                                    }
                                    if attachments.is_empty() {
                                        self.handle_composed_message(&final_content);
                                    } else {
                                        self.handle_composed_message_with_attachments(&final_content, &attachments);
                                    }
                                    break;
                                }
                                "e" => {
                                    let _ = std::fs::remove_file(&tmpfile);
                                    self.run_editor_compose_at(&content, None);
                                    return;
                                }
                                "p" => {
                                    let conn = self.db.conn.lock().unwrap();
                                    let now = database::now_secs();
                                    let _ = conn.execute("INSERT INTO postponed (data, created_at) VALUES (?, ?)",
                                        rusqlite::params![content, now]);
                                    drop(conn);
                                    self.set_feedback("Message postponed", tc.feedback_ok);
                                    break;
                                }
                                "a" => {
                                    let path = self.prompt("Attach file (Enter=browse): ", "");
                                    if path.is_empty() {
                                        // Launch pointer in --pick mode
                                        let pick_file = format!("/tmp/kastrup_attach_{}.txt", std::process::id());
                                        let _ = std::fs::remove_file(&pick_file);
                                        Crust::cleanup();
                                        print!("\x1b[2J\x1b[H");
                                        let _ = std::io::Write::flush(&mut std::io::stdout());
                                        let _ = std::process::Command::new("pointer")
                                            .arg(format!("--pick={}", pick_file))
                                            .status();
                                        Crust::init();
                                        Crust::clear_screen();
                                        self.handle_resize();
                                        if let Ok(files) = std::fs::read_to_string(&pick_file) {
                                            for f in files.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()) {
                                                attachments.push(f);
                                            }
                                        }
                                        let _ = std::fs::remove_file(&pick_file);
                                        if !attachments.is_empty() {
                                            self.set_feedback(&format!("{} attachment(s)", attachments.len()), tc.feedback_ok);
                                        }
                                    } else {
                                        let expanded = path.replace("~/",
                                            &format!("{}/", std::env::var("HOME").unwrap_or_default()));
                                        if std::path::Path::new(&expanded).exists() {
                                            attachments.push(expanded);
                                            self.set_feedback(&format!("{} attachment(s)", attachments.len()), tc.feedback_ok);
                                        } else {
                                            self.set_feedback("File not found", tc.feedback_warn);
                                        }
                                    }
                                    continue;
                                }
                                "ESC" => {
                                    self.set_feedback("Cancelled", tc.feedback_info);
                                    break;
                                }
                                _ => {
                                    let plugin = plugins.iter().find(|(k, _, _)| k == key.as_str()).cloned();
                                    if let Some((_, label, command)) = plugin {
                                        let pick_file = format!("/tmp/kastrup_plugin_pick_{}.txt", std::process::id());
                                        let _ = std::fs::remove_file(&pick_file);
                                        let cmd = command.replace("%{pick_file}", &crust::shell_escape(&pick_file));
                                        Crust::cleanup();
                                        print!("\x1b[2J\x1b[H");
                                        let _ = std::process::Command::new("sh").arg("-c").arg(&cmd).status();
                                        Crust::init();
                                        Crust::clear_screen();
                                        if let Ok(files) = std::fs::read_to_string(&pick_file) {
                                            let paths: Vec<String> = files.lines()
                                                .map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect();
                                            if !paths.is_empty() {
                                                attachments.extend(paths);
                                                self.set_feedback(&format!("{}: {} file(s) attached", label, attachments.len()), tc.feedback_ok);
                                            }
                                        }
                                        let _ = std::fs::remove_file(&pick_file);
                                        self.handle_resize();
                                        continue;
                                    }
                                }
                            }
                        }
                    } else {
                        self.set_feedback(
                            "Cancelled (no changes)",
                            self.config.theme_colors.feedback_info,
                        );
                    }
                }
            }
        }

        let _ = std::fs::remove_file(&tmpfile);
        // Force full redraw after returning from editor (pane caches are stale)
        self.handle_resize();
    }

    fn handle_composed_message_with_attachments(&mut self, content: &str, attachments: &[String]) {
        let mut from = String::new();
        let mut to = String::new();
        let mut cc = String::new();
        let mut bcc = String::new();
        let mut subject = String::new();
        let mut reply_to = String::new();
        let mut body_lines = Vec::new();
        let mut in_body = false;
        for line in content.lines() {
            if in_body { body_lines.push(line); }
            else if line.trim().is_empty() { in_body = true; }
            else if let Some(val) = line.strip_prefix("From: ") { from = val.trim().to_string(); }
            else if let Some(val) = line.strip_prefix("To: ") { to = val.trim().to_string(); }
            else if let Some(val) = line.strip_prefix("Cc: ") { cc = val.trim().to_string(); }
            else if let Some(val) = line.strip_prefix("Bcc: ") { bcc = val.trim().to_string(); }
            else if let Some(val) = line.strip_prefix("Subject: ") { subject = val.trim().to_string(); }
            else if let Some(val) = line.strip_prefix("Reply-To: ") { reply_to = val.trim().to_string(); }
        }
        let body = body_lines.join("\n");
        if to.is_empty() || body.trim().is_empty() {
            self.set_feedback("Cancelled (empty To or body)", self.config.theme_colors.feedback_warn);
            return;
        }
        // Per-identity SMTP: check From header against identities
        let smtp = self.config.identities.iter()
            .find(|(_, id)| from.contains(&id.email))
            .and_then(|(_, id)| id.smtp.as_ref())
            .unwrap_or(&self.config.smtp_command);
        if smtp.is_empty() {
            self.set_feedback("No SMTP command configured (set in Preferences)", self.config.theme_colors.feedback_warn);
            return;
        }
        let boundary = format!("kastrup-boundary-{}", std::process::id());
        let mut rfc_msg = String::new();
        rfc_msg.push_str(&format!("From: {}\n", from));
        rfc_msg.push_str(&format!("To: {}\n", to));
        if !cc.is_empty() { rfc_msg.push_str(&format!("Cc: {}\n", cc)); }
        if !bcc.is_empty() { rfc_msg.push_str(&format!("Bcc: {}\n", bcc)); }
        if !reply_to.is_empty() { rfc_msg.push_str(&format!("Reply-To: {}\n", reply_to)); }
        rfc_msg.push_str(&format!("Subject: {}\n", subject));
        rfc_msg.push_str("MIME-Version: 1.0\n");
        rfc_msg.push_str(&format!("Content-Type: multipart/mixed; boundary=\"{}\"\n", boundary));
        rfc_msg.push('\n');
        rfc_msg.push_str(&format!("--{}\n", boundary));
        rfc_msg.push_str("Content-Type: text/plain; charset=UTF-8\n\n");
        rfc_msg.push_str(&body);
        rfc_msg.push('\n');
        for att_path in attachments {
            let fname = std::path::Path::new(att_path).file_name()
                .and_then(|f| f.to_str()).unwrap_or("attachment");
            if let Ok(data) = std::fs::read(att_path) {
                let encoded = base64_encode(&data);
                rfc_msg.push_str(&format!("--{}\n", boundary));
                rfc_msg.push_str(&format!("Content-Type: application/octet-stream; name=\"{}\"\n", fname));
                rfc_msg.push_str("Content-Transfer-Encoding: base64\n");
                rfc_msg.push_str(&format!("Content-Disposition: attachment; filename=\"{}\"\n\n", fname));
                for chunk in encoded.as_bytes().chunks(76) {
                    rfc_msg.push_str(std::str::from_utf8(chunk).unwrap_or(""));
                    rfc_msg.push('\n');
                }
            }
        }
        rfc_msg.push_str(&format!("--{}--\n", boundary));
        let smtp_tmpfile = format!("/tmp/kastrup_send_{}.eml", std::process::id());
        if std::fs::write(&smtp_tmpfile, &rfc_msg).is_err() {
            self.set_feedback("Failed to write send file", 196);
            return;
        }
        self.bottom.say(&style::fg(&format!(" Sending to {}...", to), 226));
        let home = std::env::var("HOME").unwrap_or_default();
        let smtp_expanded = smtp.replace("~/", &format!("{}/", home));
        let from_email = if let Some(lt) = from.find('<') {
            from[lt+1..].trim_end_matches('>').to_string()
        } else { from.clone() };
        let mut recipients = Vec::new();
        for addr in to.split(',').chain(cc.split(',')).chain(bcc.split(',')) {
            let addr = addr.trim();
            if addr.is_empty() { continue; }
            let email = if let Some(lt) = addr.find('<') {
                addr[lt+1..].trim_end_matches('>').to_string()
            } else { addr.to_string() };
            if email.contains('@') { recipients.push(email); }
        }
        let cmd = format!("{} -f {} -i {} < '{}'",
            smtp_expanded, from_email, recipients.join(" "), smtp_tmpfile);
        log::info(&format!("SMTP (with attachments): {} -> {} ({} att)", from_email, recipients.join(", "), attachments.len()));
        let result = std::process::Command::new("sh").arg("-c").arg(&cmd).output();
        match result {
            Ok(output) if output.status.success() => {
                let _ = std::fs::remove_file(&smtp_tmpfile);
                log::info(&format!("SMTP sent OK to {}", to));
                self.set_feedback(&format!("Sent to {} ({} attachment(s))", to, attachments.len()), self.config.theme_colors.feedback_ok);
                self.mark_forwarded();
                self.mark_replied();
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let msg = if stderr.trim().is_empty() {
                    format!("Send failed (exit {}). File: {}", output.status.code().unwrap_or(-1), smtp_tmpfile)
                } else {
                    format!("Send failed: {}", stderr.lines().next().unwrap_or("unknown error"))
                };
                self.set_feedback(&msg, 196);
            }
            Err(e) => {
                self.set_feedback(&format!("Send failed: {}", e), 196);
            }
        }
    }

    fn handle_composed_message(&mut self, content: &str) {
        // Parse headers and body
        let mut from = String::new();
        let mut to = String::new();
        let mut cc = String::new();
        let mut bcc = String::new();
        let mut subject = String::new();
        let mut reply_to = String::new();
        let mut body_lines = Vec::new();
        let mut in_body = false;

        for line in content.lines() {
            if in_body {
                body_lines.push(line);
            } else if line.trim().is_empty() {
                in_body = true;
            } else if let Some(val) = line.strip_prefix("From: ") {
                from = val.trim().to_string();
            } else if let Some(val) = line.strip_prefix("To: ") {
                to = val.trim().to_string();
            } else if let Some(val) = line.strip_prefix("Cc: ") {
                cc = val.trim().to_string();
            } else if let Some(val) = line.strip_prefix("Bcc: ") {
                bcc = val.trim().to_string();
            } else if let Some(val) = line.strip_prefix("Subject: ") {
                subject = val.trim().to_string();
            } else if let Some(val) = line.strip_prefix("Reply-To: ") {
                reply_to = val.trim().to_string();
            }
        }

        let body = body_lines.join("\n");

        if to.is_empty() || body.trim().is_empty() {
            self.set_feedback(
                "Cancelled (empty To or body)",
                self.config.theme_colors.feedback_warn,
            );
            return;
        }

        // Per-identity SMTP: check From header against identities
        let smtp = self.config.identities.iter()
            .find(|(_, id)| from.contains(&id.email))
            .and_then(|(_, id)| id.smtp.as_ref())
            .unwrap_or(&self.config.smtp_command);

        // Build RFC822-style message for SMTP
        if smtp.is_empty() {
            self.set_feedback(
                "No SMTP command configured (set in Preferences)",
                self.config.theme_colors.feedback_warn,
            );
            return;
        }

        let mut rfc_msg = String::new();
        rfc_msg.push_str(&format!("From: {}\n", from));
        rfc_msg.push_str(&format!("To: {}\n", to));
        if !cc.is_empty() {
            rfc_msg.push_str(&format!("Cc: {}\n", cc));
        }
        if !bcc.is_empty() {
            rfc_msg.push_str(&format!("Bcc: {}\n", bcc));
        }
        if !reply_to.is_empty() {
            rfc_msg.push_str(&format!("Reply-To: {}\n", reply_to));
        }
        rfc_msg.push_str(&format!("Subject: {}\n", subject));
        rfc_msg.push_str("MIME-Version: 1.0\n");
        rfc_msg.push_str("Content-Type: text/plain; charset=UTF-8\n");
        rfc_msg.push('\n');
        rfc_msg.push_str(&body);

        let smtp_tmpfile = format!("/tmp/kastrup_send_{}.eml", std::process::id());
        if std::fs::write(&smtp_tmpfile, &rfc_msg).is_err() {
            self.set_feedback("Failed to write send file", 196);
            return;
        }

        // Show sending feedback
        self.bottom.say(&style::fg(&format!(" Sending to {}...", to), 226));

        let home = std::env::var("HOME").unwrap_or_default();
        let smtp_expanded = smtp.replace("~/", &format!("{}/", home));
        // Extract bare from email for -f flag
        let from_email = if let Some(lt) = from.find('<') {
            from[lt+1..].trim_end_matches('>').to_string()
        } else { from.clone() };
        // Build recipient list: to + cc + bcc
        let mut recipients = Vec::new();
        for addr in to.split(',').chain(cc.split(',')).chain(bcc.split(',')) {
            let addr = addr.trim();
            if addr.is_empty() { continue; }
            let email = if let Some(lt) = addr.find('<') {
                addr[lt+1..].trim_end_matches('>').to_string()
            } else { addr.to_string() };
            if email.contains('@') { recipients.push(email); }
        }
        let cmd = format!("{} -f {} -i {} < '{}'",
            smtp_expanded, from_email, recipients.join(" "), smtp_tmpfile);
        log::info(&format!("SMTP: {} -> {}", from_email, recipients.join(", ")));
        let result = std::process::Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .output();

        match result {
            Ok(output) if output.status.success() => {
                let _ = std::fs::remove_file(&smtp_tmpfile);
                log::info(&format!("SMTP sent OK to {}", to));
                self.set_feedback(
                    &format!("Sent to {}", to),
                    self.config.theme_colors.feedback_ok,
                );
                // Mark forwarded/replied messages
                self.mark_forwarded();
                self.mark_replied();
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let msg = if stderr.trim().is_empty() {
                    format!("Send failed (exit {}). File: {}", output.status.code().unwrap_or(-1), smtp_tmpfile)
                } else {
                    format!("Send failed: {}", stderr.lines().next().unwrap_or("unknown error"))
                };
                self.set_feedback(&msg, 196);
                // Keep the file for debugging
            }
            Err(e) => {
                self.set_feedback(&format!("Send failed: {}", e), 196);
            }
        }
    }
}

// --- Attachment Viewing ---

impl App {
    fn view_attachments(&mut self) {
        if self.filtered_messages.is_empty() { return; }
        // Ensure full content loaded for MIME extraction
        self.ensure_full_content();
        // Try MIME extraction if attachments are empty
        if self.filtered_messages[self.index].attachments.is_empty() {
            let msg = &self.filtered_messages[self.index];
            if msg.content.contains("Content-Type:") {
                let atts = extract_mime_attachments(&msg.content, msg.id);
                if !atts.is_empty() {
                    self.filtered_messages[self.index].attachments = atts;
                }
            }
        }
        let msg = &self.filtered_messages[self.index];
        if msg.attachments.is_empty() {
            self.set_feedback("No attachments", self.config.theme_colors.feedback_warn);
            return;
        }

        let maildir_file = msg
            .metadata
            .get("maildir_file")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Filter out image attachments (those are handled by V key)
        let attachments: Vec<serde_json::Value> = msg.attachments.iter()
            .filter(|a| !is_image_attachment(a))
            .cloned()
            .collect();
        if attachments.is_empty() {
            self.set_feedback("No non-image attachments (press V for images)", self.config.theme_colors.feedback_info);
            return;
        }
        let mut att_index = 0usize;
        let mut att_tagged: HashSet<usize> = HashSet::new();

        loop {
            // Render attachment list in right pane
            let tc = &self.config.theme_colors;
            let mut lines = Vec::new();
            lines.push(style::bold(&style::fg("Attachments:", tc.attachment)));
            lines.push(String::new());

            for (i, att) in attachments.iter().enumerate() {
                let name = att
                    .get("name")
                    .or_else(|| att.get("filename"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unnamed");
                let size = att
                    .get("size")
                    .and_then(|v| v.as_u64())
                    .map(|s| format!(" ({})", format_file_size(s)))
                    .unwrap_or_default();
                let ctype = att
                    .get("content_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let tag = if att_tagged.contains(&i) {
                    style::fg("* ", tc.star)
                } else {
                    "  ".to_string()
                };

                if i == att_index {
                    lines.push(format!(
                        "{}{}{}",
                        style::fg("\u{2192} ", tc.unread),
                        tag,
                        style::bold(&style::fg(
                            &format!("{}{} {}", name, size, ctype),
                            255
                        ))
                    ));
                } else {
                    lines.push(format!(
                        "  {}{}",
                        tag,
                        style::fg(&format!("{}{} {}", name, size, ctype), 250)
                    ));
                }
            }

            let tagged_hint = if att_tagged.is_empty() {
                String::new()
            } else {
                format!("  ({} tagged)", att_tagged.len())
            };
            lines.push(String::new());
            lines.push(style::fg(
                &format!(
                    "t:Tag  T:All  o/Enter:Open  s:Save{}  ESC:Back",
                    tagged_hint
                ),
                self.config.theme_colors.hint_fg,
            ));

            self.right.set_text(&lines.join("\n"));
            self.right.ix = 0;
            self.right.full_refresh();
            if self.right.border {
                self.right.border_refresh();
            }

            self.bottom.say(&style::fg(
                " j/k:Navigate  t:Tag  T:Tag all  o:Open  s:Save  ESC:Back",
                self.config.theme_colors.hint_fg,
            ));

            let Some(key) = Input::getchr(None) else {
                continue;
            };
            match key.as_str() {
                "j" | "DOWN" => {
                    att_index = (att_index + 1) % attachments.len();
                }
                "k" | "UP" => {
                    att_index = if att_index == 0 {
                        attachments.len() - 1
                    } else {
                        att_index - 1
                    };
                }
                "t" => {
                    if att_tagged.contains(&att_index) {
                        att_tagged.remove(&att_index);
                    } else {
                        att_tagged.insert(att_index);
                    }
                    att_index = (att_index + 1) % attachments.len();
                }
                "T" => {
                    if att_tagged.len() == attachments.len() {
                        att_tagged.clear();
                    } else {
                        for i in 0..attachments.len() {
                            att_tagged.insert(i);
                        }
                    }
                }
                "o" | "ENTER" => {
                    self.extract_and_open_attachment(
                        maildir_file.as_deref(),
                        &attachments,
                        att_index,
                        true,
                    );
                }
                "s" => {
                    let targets: Vec<usize> = if att_tagged.is_empty() {
                        vec![att_index]
                    } else {
                        att_tagged.iter().copied().collect()
                    };
                    for &idx in &targets {
                        let name = attachments[idx]
                            .get("name")
                            .or_else(|| attachments[idx].get("filename"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("unnamed");
                        let dl = self.config.download_folder.replace(
                            '~',
                            &std::env::var("HOME").unwrap_or_default(),
                        );
                        let default_dest = format!("{}/{}", dl, name);
                        let dest = self.prompt("Save to: ", &default_dest);
                        if !dest.is_empty() {
                            self.extract_and_save_attachment(
                                maildir_file.as_deref(),
                                &attachments,
                                idx,
                                &dest,
                            );
                        }
                    }
                }
                "ESC" | "q" | "h" | "LEFT" => break,
                _ => {}
            }
        }

        self.render_all();
    }

    /// Extract an attachment from a maildir file and either open or save it.
    fn extract_and_open_attachment(
        &mut self,
        maildir_file: Option<&str>,
        attachments: &[serde_json::Value],
        idx: usize,
        open: bool,
    ) {
        let att = &attachments[idx];
        let name = att
            .get("name")
            .or_else(|| att.get("filename"))
            .and_then(|v| v.as_str())
            .unwrap_or("unnamed");
        let dest = format!("/tmp/kastrup_att_{}", name);

        // External-sender path: if the attachment carries a file_id AND the
        // source has an open_attachment template configured, dispatch to it.
        // Covers non-maildir sources like Dualog Workspace that resolve
        // attachments by server-side id.
        let file_id = att.get("file_id").and_then(|v| v.as_str()).map(|s| s.to_string());
        let plugin_type = self.filtered_messages.get(self.index)
            .map(|m| m.source_type.clone()).unwrap_or_default();
        let has_open_attachment = self.config.senders.get(&plugin_type)
            .map(|m| m.contains_key("open_attachment")).unwrap_or(false);
        if let (Some(fid), true) = (file_id, has_open_attachment) {
            self.set_feedback(&format!("Downloading {}...", name),
                self.config.theme_colors.accent);
            let res = self.dispatch_external_action(&plugin_type, "open_attachment",
                &[("file_id", &fid), ("name", name), ("dest", &dest)], None);
            match res {
                Ok(()) => {
                    if open {
                        let _ = std::process::Command::new("xdg-open").arg(&dest)
                            .stdin(std::process::Stdio::null())
                            .stdout(std::process::Stdio::null())
                            .stderr(std::process::Stdio::null())
                            .spawn();
                        self.set_feedback(&format!("Opened {}", name),
                            self.config.theme_colors.feedback_ok);
                    } else {
                        self.set_feedback(&format!("Saved to {}", dest),
                            self.config.theme_colors.feedback_ok);
                    }
                }
                Err(e) => self.set_feedback(&format!("Attachment download failed: {}", e),
                    self.config.theme_colors.feedback_warn),
            }
            return;
        }

        let Some(mf) = maildir_file else {
            self.set_feedback("No source file available", self.config.theme_colors.feedback_warn);
            return;
        };

        if !std::path::Path::new(mf).exists() {
            self.set_feedback("Mail file not found on disk", self.config.theme_colors.feedback_warn);
            return;
        }

        // Extract attachment using Python (always available, handles MIME properly)
        let py_script = format!(
            r#"
import email, sys, os
with open(sys.argv[1], 'rb') as f:
    msg = email.message_from_binary_file(f)
target = sys.argv[2]
dest = sys.argv[3]
for part in msg.walk():
    fn = part.get_filename()
    if fn and fn == target:
        data = part.get_payload(decode=True)
        if data:
            with open(dest, 'wb') as out:
                out.write(data)
            sys.exit(0)
# Fallback: try by index
idx = int(sys.argv[4])
i = 0
for part in msg.walk():
    if part.get_filename() or (part.get_content_maintype() != 'multipart' and part.get_content_maintype() != 'text'):
        if i == idx:
            data = part.get_payload(decode=True)
            if data:
                with open(dest, 'wb') as out:
                    out.write(data)
                sys.exit(0)
        i += 1
"#
        );

        let result = std::process::Command::new("python3")
            .arg("-c")
            .arg(&py_script)
            .arg(mf)
            .arg(name)
            .arg(&dest)
            .arg(idx.to_string())
            .output();

        let extracted = result.is_ok() && std::path::Path::new(&dest).exists();

        if !extracted {
            self.set_feedback(
                &format!("Could not extract: {}", name),
                self.config.theme_colors.feedback_warn,
            );
            return;
        }

        if open {
            let _ = std::process::Command::new("xdg-open")
                .arg(&dest)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn();
            self.set_feedback(
                &format!("Opened: {}", name),
                self.config.theme_colors.feedback_ok,
            );
        }
    }

    /// Extract an attachment and save to a specific destination path.
    fn extract_and_save_attachment(
        &mut self,
        maildir_file: Option<&str>,
        attachments: &[serde_json::Value],
        idx: usize,
        dest: &str,
    ) {
        let att = &attachments[idx];
        let name = att
            .get("name")
            .or_else(|| att.get("filename"))
            .and_then(|v| v.as_str())
            .unwrap_or("unnamed");
        let tmp_dest = format!("/tmp/kastrup_att_{}", name);

        // Extract to tmp first
        self.extract_and_open_attachment(maildir_file, attachments, idx, false);

        // Copy to final destination
        if std::fs::metadata(&tmp_dest).is_ok() {
            match std::fs::copy(&tmp_dest, dest) {
                Ok(_) => {
                    let _ = std::fs::remove_file(&tmp_dest);
                    self.set_feedback(
                        &format!("Saved: {}", dest),
                        self.config.theme_colors.feedback_ok,
                    );
                }
                Err(e) => {
                    self.set_feedback(
                        &format!("Save failed: {}", e),
                        self.config.theme_colors.feedback_warn,
                    );
                }
            }
        } else {
            self.set_feedback(
                &format!("Could not extract: {}", name),
                self.config.theme_colors.feedback_warn,
            );
        }
    }
}

// --- Inline Image Display ---

impl App {
    fn toggle_inline_image(&mut self) {
        if self.showing_image {
            self.clear_inline_image();
            self.render_message_content();
            return;
        }

        if self.filtered_messages.is_empty() { return; }

        // Ensure full content loaded
        if !self.filtered_messages[self.index].full_loaded {
            let msg_id = self.filtered_messages[self.index].id;
            if let Some((content, html)) = self.db.get_message_content(msg_id) {
                self.filtered_messages[self.index].content = content;
                self.filtered_messages[self.index].html_content = html;
                self.filtered_messages[self.index].full_loaded = true;
            }
        }
        let msg = &self.filtered_messages[self.index];

        // Collect image URLs
        let mut urls: Vec<String> = Vec::new();

        // From attachments (Discord/chat)
        for att in &msg.attachments {
            let url = att.get("url").or_else(|| att.get("proxy_url")).and_then(|v| v.as_str());
            if let Some(url) = url {
                if url.starts_with("http") && is_image_attachment(att) {
                    urls.push(url.to_string());
                }
            }
        }

        // From external-sender attachments (no URL, addressable by file_id):
        // dispatch the source's open_attachment template to download into a
        // cache file, then add a file:// URL. Covers workspace etc.
        let plugin_type = msg.source_type.clone();
        let has_open = self.config.senders.get(&plugin_type)
            .map(|m| m.contains_key("open_attachment")).unwrap_or(false);
        if has_open {
            let cache_dir = home_dir().join(".kastrup/image_cache");
            let _ = std::fs::create_dir_all(&cache_dir);
            let mut jobs: Vec<(String, String, String)> = Vec::new(); // (file_id, name, dest)
            for att in &msg.attachments {
                if !is_image_attachment(att) { continue; }
                let file_id = att.get("file_id").and_then(|v| v.as_str());
                let name = att.get("name").or_else(|| att.get("filename"))
                    .and_then(|v| v.as_str()).unwrap_or("image");
                if let Some(fid) = file_id {
                    let dest = cache_dir.join(format!("{}_{}", fid, name))
                        .to_string_lossy().to_string();
                    jobs.push((fid.to_string(), name.to_string(), dest));
                }
            }
            for (fid, name, dest) in jobs {
                if !std::path::Path::new(&dest).exists() {
                    let _ = self.dispatch_external_action(&plugin_type, "open_attachment",
                        &[("file_id", &fid), ("name", &name), ("dest", &dest)], None);
                }
                if std::path::Path::new(&dest).exists() {
                    urls.push(format!("file://{}", dest));
                }
            }
        }
        let msg = &self.filtered_messages[self.index];

        // From HTML content
        let html = msg.html_content.as_deref()
            .or_else(|| if msg.content.trim_start().starts_with('<') { Some(msg.content.as_str()) } else { None });
        if let Some(html) = html {
            for url in extract_image_urls(html) {
                if url.starts_with("http") {
                    urls.push(url);
                }
            }
        }

        // From MIME image parts (inline embedded images)
        if urls.is_empty() && msg.content.contains("image/") {
            let maildir_file = msg.metadata.get("maildir_file").and_then(|v| v.as_str()).map(String::from);
            if let Some(ref mf) = maildir_file {
                if std::path::Path::new(mf).exists() {
                    let cache_dir = home_dir().join(".kastrup/image_cache");
                    let _ = std::fs::create_dir_all(&cache_dir);
                    let py = format!(r#"
import email, sys, os
with open(sys.argv[1], 'rb') as f:
    msg = email.message_from_binary_file(f)
dest = sys.argv[2]
i = 0
for part in msg.walk():
    ct = part.get_content_type()
    if ct.startswith('image/'):
        data = part.get_payload(decode=True)
        if data:
            ext = ct.split('/')[-1].split(';')[0]
            path = os.path.join(dest, f'mime_img_{{i}}.{{ext}}')
            with open(path, 'wb') as out:
                out.write(data)
            print(path)
            i += 1
"#);
                    if let Ok(output) = std::process::Command::new("python3")
                        .arg("-c").arg(&py)
                        .arg(mf).arg(cache_dir.to_string_lossy().as_ref())
                        .output()
                    {
                        for line in String::from_utf8_lossy(&output.stdout).lines() {
                            let path = line.trim();
                            if !path.is_empty() {
                                urls.push(format!("file://{}", path));
                            }
                        }
                    }
                }
            }
        }

        urls.dedup();

        if urls.is_empty() {
            self.set_feedback("No images found", self.config.theme_colors.feedback_info);
            return;
        }

        self.set_feedback(&format!("Loading {} image(s)...", urls.len()), self.config.theme_colors.unread);

        // Download to cache
        let cache_dir = home_dir().join(".heathrow/image_cache");
        let _ = std::fs::create_dir_all(&cache_dir);

        let mut paths: Vec<String> = Vec::new();
        for url in urls.iter().take(10) {
            // Local file (from MIME extraction)
            if let Some(local) = url.strip_prefix("file://") {
                if std::path::Path::new(local).exists() {
                    paths.push(local.to_string());
                }
                continue;
            }

            let ext = url.rsplit('.').next()
                .and_then(|e| {
                    let e = e.split('?').next().unwrap_or(e);
                    if e.len() <= 5 { Some(e) } else { None }
                })
                .unwrap_or("jpg");
            let hash = simple_hash(url);
            let cache_path = cache_dir.join(format!("{}.{}", hash, ext));

            if cache_path.exists() && std::fs::metadata(&cache_path).map(|m| m.len() > 100).unwrap_or(false) {
                paths.push(cache_path.to_string_lossy().to_string());
                continue;
            }

            // Download
            let agent = ureq::AgentBuilder::new()
                .timeout_connect(std::time::Duration::from_secs(5))
                .timeout_read(std::time::Duration::from_secs(10))
                .build();
            if let Ok(resp) = agent.get(url).call() {
                let mut bytes = Vec::new();
                if std::io::Read::read_to_end(&mut resp.into_reader(), &mut bytes).is_ok() && bytes.len() > 100 {
                    let _ = std::fs::write(&cache_path, &bytes);
                    paths.push(cache_path.to_string_lossy().to_string());
                }
            }
        }

        if paths.is_empty() {
            self.set_feedback("Download failed", self.config.theme_colors.feedback_warn);
            return;
        }

        // Display using glow
        let display = glow::Display::new();
        if !display.supported() {
            self.set_feedback("Image display not supported in this terminal", self.config.theme_colors.feedback_warn);
            return;
        }

        let label = if paths.len() == 1 { "1 image".to_string() } else { format!("{} images", paths.len()) };
        self.right.set_text(&style::fg(&format!(" [{}]  D: download  ESC: return", label), self.config.theme_colors.hint_fg));
        self.right.full_refresh();
        if self.right.border { self.right.border_refresh(); }

        // If multiple images, use montage to composite (if available)
        let display_path = if paths.len() > 1 {
            let composite = cache_dir.join("composite.png");
            let cols = (paths.len() as f64).sqrt().ceil() as usize;
            let result = std::process::Command::new("montage")
                .args(&paths)
                .args(["-geometry", "+2+2", "-tile", &format!("{}x", cols), "-background", "none"])
                .arg(composite.to_str().unwrap_or("/tmp/composite.png"))
                .status();
            if result.map(|s| s.success()).unwrap_or(false) && composite.exists() {
                Some(composite.to_string_lossy().to_string())
            } else {
                None
            }
        } else {
            None
        };

        self.image_display = Some(display);
        if let Some(ref mut disp) = self.image_display {
            let img_x = self.right.x;
            let img_y = self.right.y + 1;
            let img_w = self.right.w.saturating_sub(2);
            let img_h = self.right.h.saturating_sub(2);

            if let Some(ref composite) = display_path {
                // Show composited image
                disp.show(composite, img_x, img_y, img_w, img_h);
            } else if paths.len() == 1 {
                // Single image
                disp.show(&paths[0], img_x, img_y, img_w, img_h);
            } else {
                // Multiple images, no montage: show each image in equal vertical slices
                let n = paths.len() as u16;
                let per_h = img_h / n;
                for (i, path) in paths.iter().enumerate() {
                    let i16 = i as u16;
                    let y = img_y + i16 * per_h;
                    let h = if i == paths.len() - 1 { img_h - i16 * per_h } else { per_h };
                    if h > 0 {
                        disp.show(path, img_x, y, img_w, h);
                    }
                }
            }
        }
        self.showing_image = true;
    }

    fn clear_inline_image(&mut self) {
        if !self.showing_image { return; }
        if let Some(ref mut disp) = self.image_display {
            disp.clear(self.right.x, self.right.y, self.right.w, self.right.h, self.cols, self.rows);
        }
        self.image_display = None;
        self.showing_image = false;
    }

    /// Collect every image URL referenced by the current message, from all
    /// sources: Discord-style attachments, HTML <img>, MIME inline images
    /// (the latter extracted to disk and returned as file:// URLs).
    fn collect_image_urls(&mut self) -> Vec<String> {
        if self.filtered_messages.is_empty() { return Vec::new(); }

        if !self.filtered_messages[self.index].full_loaded {
            let msg_id = self.filtered_messages[self.index].id;
            if let Some((content, html)) = self.db.get_message_content(msg_id) {
                self.filtered_messages[self.index].content = content;
                self.filtered_messages[self.index].html_content = html;
                self.filtered_messages[self.index].full_loaded = true;
            }
        }

        let msg = &self.filtered_messages[self.index];
        let mut urls: Vec<String> = Vec::new();

        for att in &msg.attachments {
            let url = att.get("url").or_else(|| att.get("proxy_url")).and_then(|v| v.as_str());
            if let Some(url) = url {
                if url.starts_with("http") && is_image_attachment(att) {
                    urls.push(url.to_string());
                }
            }
        }

        let html = msg.html_content.as_deref()
            .or_else(|| if msg.content.trim_start().starts_with('<') { Some(msg.content.as_str()) } else { None });
        if let Some(html) = html {
            for url in extract_image_urls(html) {
                if url.starts_with("http") {
                    urls.push(url);
                }
            }
        }

        if urls.is_empty() && msg.content.contains("image/") {
            let maildir_file = msg.metadata.get("maildir_file").and_then(|v| v.as_str()).map(String::from);
            if let Some(ref mf) = maildir_file {
                if std::path::Path::new(mf).exists() {
                    let cache_dir = home_dir().join(".kastrup/image_cache");
                    let _ = std::fs::create_dir_all(&cache_dir);
                    let py = r#"
import email, sys, os
with open(sys.argv[1], 'rb') as f:
    msg = email.message_from_binary_file(f)
dest = sys.argv[2]
i = 0
for part in msg.walk():
    ct = part.get_content_type()
    if ct.startswith('image/'):
        data = part.get_payload(decode=True)
        if data:
            ext = ct.split('/')[-1].split(';')[0]
            fname = part.get_filename() or f'mime_img_{i}.{ext}'
            path = os.path.join(dest, fname)
            with open(path, 'wb') as out:
                out.write(data)
            print(path)
            i += 1
"#;
                    if let Ok(output) = std::process::Command::new("python3")
                        .arg("-c").arg(py)
                        .arg(mf).arg(cache_dir.to_string_lossy().as_ref())
                        .output()
                    {
                        for line in String::from_utf8_lossy(&output.stdout).lines() {
                            let path = line.trim();
                            if !path.is_empty() {
                                urls.push(format!("file://{}", path));
                            }
                        }
                    }
                }
            }
        }

        urls.dedup();
        urls
    }

    /// Prompt for destination directory, then save `urls` there.
    /// Uses the on-disk image cache where possible; re-downloads otherwise.
    fn save_image_urls(&mut self, urls: &[String]) {
        if urls.is_empty() {
            self.set_feedback("No images selected", self.config.theme_colors.feedback_warn);
            return;
        }

        let default = format!("{}/Downloads", std::env::var("HOME").unwrap_or_default());
        let dest_input = self.prompt("Save images to: ", &default);
        if dest_input.is_empty() {
            self.set_feedback("Cancelled", self.config.theme_colors.feedback_info);
            return;
        }
        let dest_dir = dest_input.replace("~/",
            &format!("{}/", std::env::var("HOME").unwrap_or_default()));
        let dest_path = std::path::PathBuf::from(&dest_dir);
        if let Err(e) = std::fs::create_dir_all(&dest_path) {
            self.set_feedback(&format!("Can't create {}: {}", dest_dir, e), self.config.theme_colors.feedback_warn);
            return;
        }

        self.set_feedback(&format!("Downloading {} image(s)...", urls.len()), self.config.theme_colors.unread);

        let cache_dir = home_dir().join(".heathrow/image_cache");
        let _ = std::fs::create_dir_all(&cache_dir);

        let mut saved = 0usize;
        let mut failed = 0usize;
        for (i, url) in urls.iter().take(20).enumerate() {
            if let Some(local) = url.strip_prefix("file://") {
                let src = std::path::Path::new(local);
                if src.exists() {
                    let fname = src.file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| format!("image_{}.bin", i + 1));
                    let dest = unique_path(&dest_path.join(&fname));
                    if std::fs::copy(src, &dest).is_ok() { saved += 1; } else { failed += 1; }
                }
                continue;
            }

            let ext = url.rsplit('.').next()
                .and_then(|e| {
                    let e = e.split('?').next().unwrap_or(e);
                    if !e.is_empty() && e.len() <= 5 && e.chars().all(|c| c.is_alphanumeric()) {
                        Some(e.to_string())
                    } else { None }
                })
                .unwrap_or_else(|| "jpg".to_string());
            let fname_from_url = url.rsplit('/').next()
                .and_then(|s| s.split('?').next())
                .filter(|s| !s.is_empty() && s.len() < 200)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("image_{}.{}", i + 1, ext));
            let dest = unique_path(&dest_path.join(&fname_from_url));

            let hash = simple_hash(url);
            let cache_path = cache_dir.join(format!("{}.{}", hash, ext));
            if cache_path.exists() && std::fs::metadata(&cache_path).map(|m| m.len() > 100).unwrap_or(false) {
                if std::fs::copy(&cache_path, &dest).is_ok() { saved += 1; continue; }
            }

            let agent = ureq::AgentBuilder::new()
                .timeout_connect(std::time::Duration::from_secs(5))
                .timeout_read(std::time::Duration::from_secs(15))
                .build();
            if let Ok(resp) = agent.get(url).call() {
                let mut bytes = Vec::new();
                if std::io::Read::read_to_end(&mut resp.into_reader(), &mut bytes).is_ok() && bytes.len() > 100 {
                    if std::fs::write(&dest, &bytes).is_ok() {
                        saved += 1;
                        let _ = std::fs::write(&cache_path, &bytes);
                        continue;
                    }
                }
            }
            failed += 1;
        }

        let tc = self.config.theme_colors.clone();
        if failed > 0 {
            self.set_feedback(&format!("Saved {} to {} ({} failed)", saved, dest_dir, failed), tc.feedback_warn);
        } else {
            self.set_feedback(&format!("Saved {} image(s) to {}", saved, dest_dir), tc.feedback_ok);
        }
    }

    /// D key: saves image(s). With one image, saves it directly. With several,
    /// opens a picker where the user can tag specific images before saving.
    fn download_images(&mut self) {
        let urls = self.collect_image_urls();
        if urls.is_empty() {
            self.set_feedback("No images to download", self.config.theme_colors.feedback_warn);
            return;
        }
        if urls.len() == 1 {
            self.save_image_urls(&urls);
            return;
        }
        let selected = self.pick_images_loop(&urls);
        if !selected.is_empty() {
            self.save_image_urls(&selected);
        }
    }

    /// Tag-based picker for image URLs. Returns the selected URLs (tagged, or
    /// the currently highlighted one if nothing is tagged). Empty Vec = cancel.
    fn pick_images_loop(&mut self, urls: &[String]) -> Vec<String> {
        let was_showing = self.showing_image;
        if was_showing { self.clear_inline_image(); }

        let mut idx = 0usize;
        let mut tagged: HashSet<usize> = HashSet::new();
        let tc = self.config.theme_colors.clone();

        loop {
            let mut lines = Vec::new();
            lines.push(style::bold(&style::fg("Select images to download:", tc.attachment)));
            lines.push(String::new());
            for (i, url) in urls.iter().enumerate() {
                let label = image_display_label(url, i);
                let tag = if tagged.contains(&i) {
                    style::fg("* ", tc.star)
                } else { "  ".to_string() };
                if i == idx {
                    lines.push(format!("{}{}{}",
                        style::fg("\u{2192} ", tc.unread),
                        tag,
                        style::bold(&style::fg(&label, 255))));
                } else {
                    lines.push(format!("  {}{}", tag, style::fg(&label, 250)));
                }
            }
            lines.push(String::new());
            let tagged_hint = if tagged.is_empty() {
                String::new()
            } else {
                format!("  ({} tagged)", tagged.len())
            };
            lines.push(style::fg(
                &format!("j/k:Move  t:Tag  T:All  Enter/s:Save{}  ESC:Cancel", tagged_hint),
                tc.hint_fg));

            self.right.set_text(&lines.join("\n"));
            self.right.ix = 0;
            self.right.full_refresh();
            if self.right.border { self.right.border_refresh(); }

            let Some(key) = Input::getchr(None) else { continue };
            match key.as_str() {
                "ESC" | "q" => return Vec::new(),
                "j" | "DOWN" => { if idx + 1 < urls.len() { idx += 1; } }
                "k" | "UP" => { if idx > 0 { idx -= 1; } }
                "t" => {
                    if tagged.contains(&idx) { tagged.remove(&idx); }
                    else { tagged.insert(idx); }
                }
                "T" => {
                    if tagged.len() == urls.len() { tagged.clear(); }
                    else { tagged = (0..urls.len()).collect(); }
                }
                "ENTER" | "s" => {
                    if tagged.is_empty() {
                        return vec![urls[idx].clone()];
                    }
                    let mut sel: Vec<usize> = tagged.into_iter().collect();
                    sel.sort();
                    return sel.into_iter().map(|i| urls[i].clone()).collect();
                }
                _ => {}
            }
        }
    }
}

/// Short human label for an image URL (or file:// path) in the picker list.
fn image_display_label(url: &str, i: usize) -> String {
    if let Some(local) = url.strip_prefix("file://") {
        return std::path::Path::new(local).file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("image_{}", i + 1));
    }
    let fname = url.rsplit('/').next()
        .and_then(|s| s.split('?').next())
        .filter(|s| !s.is_empty())
        .unwrap_or("");
    if !fname.is_empty() && fname.len() < 60 {
        format!("{}  {}", fname, shorten_mid(url, 70))
    } else {
        shorten_mid(url, 100)
    }
}

fn shorten_mid(s: &str, max: usize) -> String {
    if s.len() <= max { return s.to_string(); }
    let half = (max - 3) / 2;
    format!("{}...{}", &s[..half], &s[s.len() - half..])
}

/// Return `path` if it doesn't exist, otherwise append `_1`, `_2`, ... before
/// the extension until an unused path is found.
fn unique_path(path: &std::path::Path) -> std::path::PathBuf {
    if !path.exists() { return path.to_path_buf(); }
    let parent = path.parent().unwrap_or(std::path::Path::new("."));
    let stem = path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
    let ext = path.extension().map(|e| e.to_string_lossy().to_string()).unwrap_or_default();
    for i in 1..1000 {
        let name = if ext.is_empty() {
            format!("{}_{}", stem, i)
        } else {
            format!("{}_{}.{}", stem, i, ext)
        };
        let candidate = parent.join(name);
        if !candidate.exists() { return candidate; }
    }
    path.to_path_buf()
}

// --- Batch I-N feature methods ---

impl App {
    // Load AI/tool plugins from ~/.kastrup/plugins/ or ~/.heathrow/plugins/
    fn load_ai_plugins(&self) -> Vec<(String, String, String)> {
        let dirs = [
            home_dir().join(".kastrup/plugins"),
            home_dir().join(".heathrow/plugins"),
        ];
        let mut plugins = Vec::new();
        for dir in &dirs {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if !path.is_file() { continue; }
                    if let Ok(content) = std::fs::read_to_string(&path) {
                        let key = regex::Regex::new(r"key:\s*'([^']+)'").ok()
                            .and_then(|r| r.captures(&content))
                            .and_then(|c| c.get(1))
                            .map(|m| m.as_str().to_string());
                        let label = regex::Regex::new(r"label:\s*'([^']+)'").ok()
                            .and_then(|r| r.captures(&content))
                            .and_then(|c| c.get(1))
                            .map(|m| m.as_str().to_string());
                        let command = regex::Regex::new(r"command:\s*'([^']+)'").ok()
                            .and_then(|r| r.captures(&content))
                            .and_then(|c| c.get(1))
                            .map(|m| m.as_str().to_string());
                        if let (Some(k), Some(l), Some(c)) = (key, label, command) {
                            // Skip if key already taken by another plugin
                            if !plugins.iter().any(|(pk, _, _): &(String, String, String)| pk == &k) {
                                plugins.push((k, l, c));
                            }
                        }
                    }
                }
            }
        }
        plugins
    }

    // Batch J: AI Assistant + plugins
    fn ai_assistant(&mut self) {
        let (is_header, sender, subject, content) = match self.filtered_messages.get(self.index) {
            Some(m) => (
                m.is_header,
                m.sender_name.as_deref().unwrap_or(&m.sender).to_string(),
                m.subject.as_deref().unwrap_or("").to_string(),
                if m.content.len() > 3000 { m.content[..3000].to_string() } else { m.content.clone() },
            ),
            None => return,
        };
        if is_header { return; }

        let plugins = self.load_ai_plugins();
        let tc = self.config.theme_colors.clone();

        let mut hint = String::from("AI: d=Draft  s=Summarize  t=Translate  a=Ask");
        for (k, l, _) in &plugins {
            hint.push_str(&format!("  {}={}", k, l));
        }
        self.set_feedback(&hint, tc.unread);

        let Some(key) = Input::getchr(Some(10)) else {
            self.render_bottom_bar();
            return;
        };

        // Check plugins first
        if let Some((_, label, command)) = plugins.iter().find(|(k, _, _)| k == key.as_str()).cloned() {
            self.run_ai_plugin(&label, &command);
            return;
        }

        let ai_prompt = match key.as_str() {
            "d" => format!("Draft a professional reply to this email.\nFrom: {}\nSubject: {}\n\n{}", sender, subject, content),
            "s" => format!("Summarize this message concisely.\nFrom: {}\nSubject: {}\n\n{}", sender, subject, content),
            "t" => format!("Translate this message to English.\nFrom: {}\nSubject: {}\n\n{}", sender, subject, content),
            "a" => {
                let question = self.prompt("Ask AI: ", "");
                if question.is_empty() { return; }
                format!("{}\n\nContext, email from {} about {}:\n{}", question, sender, subject, content)
            }
            _ => { self.render_bottom_bar(); return; }
        };

        self.set_feedback("Asking AI...", tc.unread);

        // Try claude CLI first, then curl to OpenAI
        let result = std::process::Command::new("claude")
            .arg("-p")
            .arg(&ai_prompt)
            .output();

        let response = if let Ok(output) = result {
            if output.status.success() {
                String::from_utf8_lossy(&output.stdout).to_string()
            } else {
                self.ai_fallback_openai(&ai_prompt)
            }
        } else {
            self.ai_fallback_openai(&ai_prompt)
        };

        if response.is_empty() { return; }

        self.right.set_text(&format!("{}\n\n{}",
            style::bold(&style::fg("AI Response", tc.view_custom)), response));
        self.right.ix = 0;
        self.right.full_refresh();
        if self.right.border { self.right.border_refresh(); }
        self.set_feedback("AI response shown in right pane", tc.feedback_ok);
    }

    fn run_ai_plugin(&mut self, label: &str, command: &str) {
        log::info(&format!("Running plugin: {}", label));
        let pick_file = format!("/tmp/kastrup_plugin_{}.txt", std::process::id());
        let _ = std::fs::remove_file(&pick_file);
        let cmd = command.replace("%{pick_file}", &pick_file);
        Crust::cleanup();
        print!("\x1b[2J\x1b[H");
        let _ = std::io::Write::flush(&mut std::io::stdout());
        let err_file = format!("/tmp/kastrup_plugin_err_{}.txt", std::process::id());
        let wrapped = format!("{} 2>'{}'", cmd, err_file);
        let status = std::process::Command::new("sh").arg("-c").arg(&wrapped).status();
        Crust::init();
        Crust::clear_screen();
        if let Ok(s) = &status {
            if !s.success() {
                let stderr = std::fs::read_to_string(&err_file).unwrap_or_default();
                let _ = std::fs::remove_file(&err_file);
                self.handle_resize();
                let first_line = stderr.lines().last().unwrap_or("unknown error");
                self.set_feedback(&format!("{} failed: {}", label, first_line), 196);
                return;
            }
        }
        let _ = std::fs::remove_file(&err_file);
        // Read picked files if any
        let mut picked = Vec::new();
        if let Ok(files) = std::fs::read_to_string(&pick_file) {
            picked = files.lines()
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty())
                .collect();
        }
        let _ = std::fs::remove_file(&pick_file);
        self.handle_resize();
        if picked.is_empty() {
            self.set_feedback(&format!("{}: done", label), self.config.theme_colors.feedback_info);
        } else {
            let tc = self.config.theme_colors.clone();
            let mut lines = Vec::new();
            lines.push(style::bold(&style::fg(label, tc.view_custom)));
            lines.push(String::new());
            for (i, path) in picked.iter().enumerate() {
                let fname = std::path::Path::new(path).file_name()
                    .and_then(|f| f.to_str()).unwrap_or(path);
                lines.push(format!("  {} {}", style::fg(&format!("{}", i + 1), 220), fname));
            }
            lines.push(String::new());
            lines.push(style::fg(&format!("{} file(s) selected", picked.len()), tc.hint_fg));
            self.right.set_text(&lines.join("\n"));
            self.right.ix = 0;
            self.right.full_refresh();
            if self.right.border { self.right.border_refresh(); }
            self.right_pane_locked = true;
            self.set_feedback(&format!("{}: {} file(s)", label, picked.len()), tc.feedback_ok);
        }
    }

    fn ai_fallback_openai(&mut self, ai_prompt: &str) -> String {
        let tc = self.config.theme_colors.clone();
        let api_key = std::fs::read_to_string("/home/.safe/openai.txt")
            .unwrap_or_default().trim().to_string();
        if api_key.is_empty() {
            self.set_feedback("No AI available (install claude CLI or set OpenAI key)", tc.feedback_warn);
            return String::new();
        }
        let body = serde_json::json!({
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": ai_prompt}],
            "max_tokens": 800
        });
        let resp = std::process::Command::new("curl")
            .args(["-s", "-X", "POST", "https://api.openai.com/v1/chat/completions",
                   "-H", "Content-Type: application/json",
                   "-H", &format!("Authorization: Bearer {}", api_key),
                   "-d", &body.to_string()])
            .output();
        if let Ok(o) = resp {
            let json_str = String::from_utf8_lossy(&o.stdout);
            serde_json::from_str::<serde_json::Value>(&json_str).ok()
                .and_then(|j| j["choices"][0]["message"]["content"].as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| {
                    self.set_feedback("AI request failed", tc.feedback_warn);
                    String::new()
                })
        } else {
            self.set_feedback("AI not available", tc.feedback_warn);
            String::new()
        }
    }

    // Batch K: Address Book
    fn address_book_menu(&mut self) {
        let tc = self.config.theme_colors.clone();
        self.set_feedback("Address book: a=Add sender  s=Search  l=List", tc.unread);
        let Some(key) = Input::getchr(Some(5)) else { self.render_bottom_bar(); return };

        match key.as_str() {
            "a" => {
                if let Some(msg) = self.filtered_messages.get(self.index) {
                    let name = msg.sender_name.as_deref().unwrap_or(&msg.sender).to_string();
                    let email = msg.sender.clone();
                    let conn = self.db.conn.lock().unwrap();
                    let now = database::now_secs();
                    let _ = conn.execute(
                        "INSERT OR IGNORE INTO contacts (name, primary_email, message_count, last_contact) VALUES (?, ?, 1, ?)",
                        rusqlite::params![name, email, now],
                    );
                    drop(conn);
                    self.set_feedback(&format!("Added: {} <{}>", name, email), tc.feedback_ok);
                }
            }
            "s" => {
                let query = self.prompt("Search contacts: ", "");
                if query.is_empty() { return; }
                let conn = self.db.conn.lock().unwrap();
                let mut stmt = conn.prepare(
                    "SELECT name, primary_email FROM contacts WHERE name LIKE ? OR primary_email LIKE ? ORDER BY name LIMIT 50"
                ).unwrap();
                let like = format!("%{}%", query);
                let results: Vec<String> = stmt.query_map(rusqlite::params![&like, &like], |r| {
                    let name: String = r.get(0)?;
                    let email: String = r.get(1)?;
                    Ok(format!("{} <{}>", name, email))
                }).unwrap().filter_map(|r| r.ok()).collect();
                drop(stmt);
                drop(conn);

                if results.is_empty() {
                    self.set_feedback("No contacts found", tc.feedback_info);
                } else {
                    self.right.set_text(&format!("{}\n\n{}",
                        style::bold(&style::fg("Contacts", tc.view_custom)),
                        results.join("\n")));
                    self.right.ix = 0;
                    self.right.full_refresh();
                    if self.right.border { self.right.border_refresh(); }
                }
            }
            "l" => {
                let conn = self.db.conn.lock().unwrap();
                let mut stmt = conn.prepare("SELECT name, primary_email FROM contacts ORDER BY name LIMIT 100").unwrap();
                let results: Vec<String> = stmt.query_map([], |r| {
                    let name: String = r.get(0)?;
                    let email: String = r.get(1)?;
                    Ok(format!("{} <{}>", name, email))
                }).unwrap().filter_map(|r| r.ok()).collect();
                drop(stmt);
                drop(conn);
                self.right.set_text(&format!("{}\n\n{}",
                    style::bold(&style::fg("All Contacts", tc.view_custom)),
                    if results.is_empty() { "(none)".to_string() } else { results.join("\n") }));
                self.right.ix = 0;
                self.right.full_refresh();
                if self.right.border { self.right.border_refresh(); }
            }
            _ => { self.render_bottom_bar(); }
        }
    }

    // Batch L: Calendar/Timely
    fn open_in_timely(&mut self) {
        let msg = match self.filtered_messages.get(self.index) {
            Some(m) => m.clone(),
            None => return,
        };

        let home = std::env::var("HOME").unwrap_or_default();
        let tock_home = std::path::PathBuf::from(&home).join(".tock");
        if !tock_home.is_dir() {
            self.set_feedback("Tock not configured (~/.tock missing)", self.config.theme_colors.feedback_warn);
            return;
        }

        // Try to extract date from ICS attachment in the maildir file
        let mut date_str = None;
        if let Some(file) = msg.metadata.get("maildir_file").and_then(|v| v.as_str()) {
            if std::path::Path::new(file).exists() {
                if let Ok(content) = std::fs::read_to_string(file) {
                    // Look for DTSTART in any VEVENT block
                    if let Some(vevent_start) = content.find("BEGIN:VEVENT") {
                        let vevent = &content[vevent_start..];
                        for line in vevent.lines() {
                            let l = line.trim();
                            if l.starts_with("DTSTART") {
                                // Extract YYYYMMDD from various formats
                                if let Some(colon) = l.find(':') {
                                    let val = &l[colon + 1..];
                                    if val.len() >= 8 {
                                        date_str = Some(format!("{}-{}-{}", &val[0..4], &val[4..6], &val[6..8]));
                                    }
                                }
                                break;
                            }
                        }
                    }

                    // Copy ICS parts to incoming dir
                    if content.contains("BEGIN:VCALENDAR") || content.contains("text/calendar") {
                        let incoming = tock_home.join("incoming");
                        let _ = std::fs::create_dir_all(&incoming);
                        // Extract ICS from MIME or use whole file if it's an ICS
                        if content.starts_with("BEGIN:VCALENDAR") {
                            let ics_path = incoming.join(format!("kastrup_{}.ics", msg.id));
                            if !ics_path.exists() {
                                let _ = std::fs::write(&ics_path, &content);
                            }
                        }
                    }
                }
            }
        }

        // Fallback: use message timestamp
        if date_str.is_none() && msg.timestamp > 0 {
            date_str = Some(format_timestamp(msg.timestamp, "%Y-%m-%d"));
        }

        let Some(date) = date_str else {
            self.set_feedback("Could not determine date", self.config.theme_colors.feedback_warn);
            return;
        };

        // Write goto file for Tock/Timely
        let goto_path = tock_home.join("goto");
        let _ = std::fs::write(&goto_path, &date);
        self.set_feedback(&format!("Sent to Tock: {}", date), self.config.theme_colors.feedback_ok);
    }

    // Batch M: Extended Help
    fn show_extended_help(&mut self) {
        let tc = self.config.theme_colors.clone();
        let mut lines = vec![
            style::bold(&style::fg("Kastrup, Extended Help", tc.view_custom)),
            String::new(),
            style::fg("Custom Key Bindings:", tc.unread),
        ];
        if self.config.custom_bindings.is_empty() {
            lines.push(style::fg("  (none configured)", tc.hint_fg));
        } else {
            for (key, cmd) in &self.config.custom_bindings {
                lines.push(format!("  {} = {}", style::fg(key, tc.info_fg), cmd));
            }
        }
        lines.push(String::new());
        lines.push(style::fg("Save Folder Shortcuts:", tc.unread));
        if self.config.save_folders.is_empty() {
            lines.push(style::fg("  (none, press s= to configure)", tc.hint_fg));
        } else {
            for (key, folder) in &self.config.save_folders {
                lines.push(format!("  s{} = {}", key, folder));
            }
        }
        lines.push(String::new());
        lines.push(style::fg("Identities:", tc.unread));
        if self.config.identities.is_empty() {
            lines.push(style::fg("  (none configured)", tc.hint_fg));
        } else {
            for (name, id) in &self.config.identities {
                lines.push(format!("  {} = {}", name, id.email));
            }
        }
        lines.push(String::new());
        lines.push(style::fg("Press ? to close, q to quit", tc.hint_fg));

        self.right.set_text(&lines.join("\n"));
        self.right.ix = 0;
        self.right.full_refresh();
        if self.right.border { self.right.border_refresh(); }
    }
}

// --- MIME / QP decoding ---

/// Extract readable text from raw MIME multipart content.
fn extract_mime_text(raw: &str) -> Option<String> {
    extract_mime_text_depth(raw, 0)
}

fn extract_mime_text_depth(raw: &str, depth: usize) -> Option<String> {
    if depth > 5 { return None; }
    // Detect MIME boundary: prefer first "--" line if content starts with one,
    // otherwise use boundary= attribute, fallback to first "--" line anywhere.
    let first_line = raw.lines().find(|l| !l.trim().is_empty());
    let boundary = if first_line.map(|l| l.starts_with("--") && l.len() > 5).unwrap_or(false) {
        // Content starts with a boundary line: use it as the primary boundary
        first_line.unwrap()[2..].trim_end_matches("--").trim().to_string()
    } else if let Some(pos) = raw.find("boundary=") {
        let rest = &raw[pos + 9..];
        let b = rest.trim_start_matches('"').split('"').next()
            .or_else(|| rest.split_whitespace().next())
            .unwrap_or("");
        if b.is_empty() { return None; }
        b.to_string()
    } else {
        raw.lines()
            .find(|l| l.starts_with("--") && l.len() > 5)
            .map(|l| l[2..].trim_end_matches(':').trim().to_string())?
    };

    let delimiter = format!("--{}", boundary);
    let parts: Vec<&str> = raw.split(&delimiter).collect();

    // Find text/plain part first, fall back to text/html, then text/calendar
    let mut text_part = None;
    let mut html_part = None;
    let mut cal_part = None;
    for part in &parts {
        let lower = part.to_lowercase();
        if let Some(header_end) = part.find("\n\n").or_else(|| part.find("\r\n\r\n")) {
            let headers = &part[..header_end];
            let body_start = if part[header_end..].starts_with("\r\n\r\n") { header_end + 4 } else { header_end + 2 };
            let body = &part[body_start..];
            let headers_lower = headers.to_lowercase();
            let is_qp = headers_lower.contains("quoted-printable");
            let is_b64 = headers_lower.contains("base64");

            // Detect charset for proper decoding
            let is_latin1 = headers_lower.contains("iso-8859") || headers_lower.contains("windows-1252");

            // Recurse into nested multipart parts
            if headers_lower.contains("multipart/") {
                if let Some(result) = extract_mime_text_depth(part, depth + 1) {
                    if text_part.is_none() { text_part = Some(result); }
                }
                continue;
            }

            if lower.contains("text/plain") {
                let decoded = if is_qp {
                    let bytes = decode_qp_bytes_body(body);
                    if is_latin1 { latin1_to_utf8(&bytes) } else { String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()) }
                } else if is_b64 {
                    let bytes = sources::maildir::base64_decode(body.trim()).unwrap_or_default();
                    if is_latin1 { latin1_to_utf8(&bytes) } else { String::from_utf8(bytes).unwrap_or_default() }
                } else { body.to_string() };
                if !decoded.trim().is_empty() { text_part = Some(decoded); }
            } else if lower.contains("text/html") {
                let decoded = if is_qp {
                    let bytes = decode_qp_bytes_body(body);
                    if is_latin1 { latin1_to_utf8(&bytes) } else { String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()) }
                } else if is_b64 {
                    let bytes = sources::maildir::base64_decode(body.trim()).unwrap_or_default();
                    if is_latin1 { latin1_to_utf8(&bytes) } else { String::from_utf8(bytes).unwrap_or_default() }
                } else { body.to_string() };
                html_part = Some(decoded);
            } else if lower.contains("text/calendar") && cal_part.is_none() {
                let decoded = if is_b64 {
                    sources::maildir::base64_decode(body.trim())
                        .and_then(|b| String::from_utf8(b).ok())
                        .unwrap_or_default()
                } else {
                    body.to_string()
                };
                if !decoded.is_empty() { cal_part = Some(parse_ical_summary(&decoded)); }
            }
        }
    }

    // Skip text/plain if it's just a "your client doesn't support HTML" fallback
    let text_is_fallback = text_part.as_ref().map(|t| {
        let lower = t.to_lowercase();
        lower.contains("html-e-poster") || lower.contains("html e-post")
            || lower.contains("doesn't support html") || lower.contains("does not support html")
            || lower.contains("not displayed") || lower.contains("html messages are not support")
            || (t.trim().lines().count() <= 3 && html_part.is_some())
    }).unwrap_or(false);

    let effective_text = if text_is_fallback { None } else { text_part };

    // If text_part contains HTML entities, decode them
    let body = effective_text
        .map(|t| {
            let has_entities = regex::Regex::new(r"&[a-zA-Z]+;|&#\d+;|&#x[0-9a-fA-F]+;")
                .map(|re| re.is_match(&t)).unwrap_or(false);
            if has_entities {
                html_to_text(&t)
            } else { t }
        })
        .or_else(|| html_part.map(|h| html_to_text(&h)).filter(|t| !t.trim().is_empty()));

    // When this is a calendar invite (text/calendar part present), put the
    // structured summary on top followed by the plain-text body. That way
    // the user sees "Title / When / Where / Organizer" first and the
    // Teams/Zoom join block below.
    match (cal_part, body) {
        (Some(cal), Some(text)) => Some(format!("{}\n\n---\n\n{}", cal, text)),
        (Some(cal), None)       => Some(cal),
        (None,      Some(text)) => Some(text),
        (None,      None)       => None,
    }
}

/// Extract raw HTML from MIME multipart content (for browser display).
fn extract_mime_html(raw: &str) -> Option<String> {
    extract_mime_html_depth(raw, 0)
}

fn extract_mime_html_depth(raw: &str, depth: usize) -> Option<String> {
    if depth > 5 { return None; }
    let first_line = raw.lines().find(|l| !l.trim().is_empty());
    let boundary = if first_line.map(|l| l.starts_with("--") && l.len() > 5).unwrap_or(false) {
        first_line.unwrap()[2..].trim_end_matches("--").trim().to_string()
    } else if let Some(pos) = raw.find("boundary=") {
        let rest = &raw[pos + 9..];
        rest.trim_start_matches('"').split('"').next()
            .or_else(|| rest.split_whitespace().next())?.to_string()
    } else {
        raw.lines()
            .find(|l| l.starts_with("--") && l.len() > 5)
            .map(|l| l[2..].trim_end_matches(':').trim().to_string())?
    };
    let delimiter = format!("--{}", boundary);
    let mut text_plain = None;
    for part in raw.split(&delimiter) {
        let lower = part.to_lowercase();
        if let Some(hdr_end) = part.find("\n\n").or_else(|| part.find("\r\n\r\n")) {
            let body_start = if part[hdr_end..].starts_with("\r\n\r\n") { hdr_end + 4 } else { hdr_end + 2 };
            let headers = &part[..hdr_end];
            let body = &part[body_start..];
            let is_qp = headers.to_lowercase().contains("quoted-printable");
            let decoded = if is_qp { decode_quoted_printable(body) } else { body.to_string() };

            let headers_lower = headers.to_lowercase();
            let is_b64 = headers_lower.contains("base64");
            let is_latin1 = headers_lower.contains("iso-8859") || headers_lower.contains("windows-1252");

            // Recurse into nested multipart
            if headers_lower.contains("multipart/") {
                if let Some(result) = extract_mime_html_depth(part, depth + 1) {
                    return Some(result);
                }
                continue;
            }

            let decoded = if is_qp {
                let bytes = decode_qp_bytes_body(body);
                if is_latin1 { latin1_to_utf8(&bytes) } else { String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()) }
            } else if is_b64 {
                let bytes = sources::maildir::base64_decode(body.trim()).unwrap_or_default();
                if is_latin1 { latin1_to_utf8(&bytes) } else { String::from_utf8(bytes).unwrap_or_default() }
            } else { body.to_string() };

            if lower.contains("text/html") {
                return Some(decoded);
            } else if lower.contains("text/plain") && !decoded.trim().is_empty() && text_plain.is_none() {
                text_plain = Some(decoded);
            }
        }
    }
    // No HTML part: wrap text/plain in basic HTML for browser display
    text_plain.map(|text| {
        format!("<html><head><meta charset=\"utf-8\"><style>body{{font-family:monospace;white-space:pre-wrap;padding:1em;background:#1a1a2e;color:#eee}}</style></head><body>{}</body></html>",
            text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;"))
    })
}

/// Parse an iCalendar string and return a colored, human-readable summary.
/// Matches VcalView output: WHAT/WHEN/WHERE/TIMEZONE/RECURRENCE/STATUS/ORGANIZER/PARTICIPANTS/DESCRIPTION.
fn parse_ical_summary(ical: &str) -> String {
    let mut method = String::new();
    let mut summary = String::new();
    let mut dtstart_raw = String::new();
    let mut dtend_raw = String::new();
    let mut timezone = String::new();
    let mut location = String::new();
    let mut organizer = String::new();
    let mut attendees: Vec<(String, String)> = Vec::new(); // (name, status)
    let mut status = String::new();
    let mut priority = String::new();
    let mut rrule = String::new();
    let mut description = String::new();
    let mut all_day = false;

    // Unfold continuation lines (RFC 5545)
    let unfolded = ical.replace("\r\n ", "").replace("\r\n\t", "").replace("\n ", "").replace("\n\t", "");

    for line in unfolded.lines() {
        let l = line.trim();
        if l.starts_with("METHOD:") { method = l[7..].to_string(); }
        else if l.starts_with("SUMMARY;") {
            if let Some(pos) = l.find(':') { summary = l[pos+1..].replace("\\n", "\n").replace("\\,", ","); }
        }
        else if l.starts_with("SUMMARY:") { summary = l[8..].replace("\\n", "\n").replace("\\,", ","); }
        else if l.starts_with("DTSTART") {
            if l.contains("VALUE=DATE:") { all_day = true; }
            if l.contains("TZID=") {
                if let Some(tz_start) = l.find("TZID=") {
                    let rest = &l[tz_start+5..];
                    timezone = rest.split(':').next().unwrap_or("").to_string();
                }
            }
            if let Some(pos) = l.find(':') { dtstart_raw = l[pos+1..].to_string(); }
        }
        else if l.starts_with("DTEND") {
            if let Some(pos) = l.find(':') { dtend_raw = l[pos+1..].to_string(); }
        }
        else if l.starts_with("LOCATION:") { location = l[9..].replace("\\n", " ").replace("\\,", ","); }
        else if l.starts_with("ORGANIZER") {
            if let Some(cn) = l.find("CN=") {
                let rest = &l[cn+3..];
                let name = rest.split(|c: char| c == ';' || c == ':').next().unwrap_or("");
                let email = if let Some(mailto) = l.to_lowercase().find("mailto:") {
                    let e = &l[mailto+7..];
                    format!(" <{}>", e.split(|c: char| c == ';' || c == '>' || c == '\n').next().unwrap_or(""))
                } else { String::new() };
                organizer = format!("{}{}", name, email);
            } else if let Some(mailto) = l.to_lowercase().find("mailto:") {
                organizer = l[mailto+7..].to_string();
            }
        }
        else if l.starts_with("ATTENDEE") {
            let name = if let Some(cn) = l.find("CN=") {
                let rest = &l[cn+3..];
                rest.split(|c: char| c == ';' || c == ':').next().unwrap_or("").to_string()
            } else if let Some(mailto) = l.to_lowercase().find("mailto:") {
                l[mailto+7..].split(|c: char| c == ';' || c == '\n').next().unwrap_or("").to_string()
            } else { continue; };
            let pstat = if l.contains("ACCEPTED") { "accepted".into() }
                else if l.contains("DECLINED") { "declined".into() }
                else if l.contains("TENTATIVE") { "tentative".into() }
                else if l.contains("NEEDS-ACTION") { "needs action".into() }
                else { String::new() };
            attendees.push((name, pstat));
        }
        else if l.starts_with("STATUS:") { status = l[7..].to_string(); }
        else if l.starts_with("PRIORITY:") {
            priority = match l[9..].trim() {
                "1" | "2" => "High".into(),
                "3" | "4" | "5" => "Normal".into(),
                "6" | "7" | "8" | "9" => "Low".into(),
                v => v.to_string(),
            };
        }
        else if l.starts_with("RRULE:") { rrule = parse_rrule_display(&l[6..]); }
        else if l.starts_with("DESCRIPTION:") {
            description = l[12..].replace("\\n", "\n").replace("\\,", ",").replace("\\;", ";");
        }
    }

    // Format date/time
    let fmt_dt = |s: &str| -> String {
        if s.len() >= 8 {
            let date = format!("{}-{}-{}", &s[0..4], &s[4..6], &s[6..8]);
            if s.len() >= 13 && s.contains('T') {
                let t_pos = s.find('T').unwrap_or(8);
                let time_part = &s[t_pos+1..];
                if time_part.len() >= 4 {
                    format!("{} {}:{}", date, &time_part[0..2], &time_part[2..4])
                } else { date }
            } else { date }
        } else { s.to_string() }
    };

    let weekday = if dtstart_raw.len() >= 8 {
        let y: i32 = dtstart_raw[0..4].parse().unwrap_or(2026);
        let m: u32 = dtstart_raw[4..6].parse().unwrap_or(1);
        let d: u32 = dtstart_raw[6..8].parse().unwrap_or(1);
        // Zeller's formula for day of week (0=Sun..6=Sat -> name)
        let (yy, mm) = if m <= 2 { (y - 1, m + 12) } else { (y, m) };
        let q = d as i32;
        let k = yy % 100; let j = yy / 100;
        let h = (q + (13 * (mm as i32 + 1)) / 5 + k + k / 4 + j / 4 + 5 * j) % 7;
        match ((h + 5) % 7 + 1) as u32 {
            1 => "Monday", 2 => "Tuesday", 3 => "Wednesday", 4 => "Thursday",
            5 => "Friday", 6 => "Saturday", _ => "Sunday",
        }
    } else { "" };

    let when = if all_day {
        if dtstart_raw == dtend_raw || dtend_raw.is_empty() {
            format!("{} ({}) - All day", fmt_dt(&dtstart_raw), weekday)
        } else {
            format!("{} to {} - All day", fmt_dt(&dtstart_raw), fmt_dt(&dtend_raw))
        }
    } else if !dtstart_raw.is_empty() {
        if dtend_raw.is_empty() {
            format!("{} ({})", fmt_dt(&dtstart_raw), weekday)
        } else {
            let sd = fmt_dt(&dtstart_raw);
            let ed = fmt_dt(&dtend_raw);
            if sd.len() > 10 && ed.len() > 10 && sd[..10] == ed[..10] {
                // Same day: show date once with time range
                format!("{} - {} ({})", sd, &ed[11..], weekday)
            } else {
                format!("{} - {} ({})", sd, ed, weekday)
            }
        }
    } else { String::new() };

    let kind = match method.to_uppercase().as_str() {
        "REPLY" => "Calendar Reply",
        "REQUEST" => "Calendar Invite",
        "CANCEL" => "Cancellation",
        "PUBLISH" => "Calendar Event",
        _ => "Calendar Event",
    };

    // Build colored output
    let lbl = |s: &str| style::fg(s, 51);   // cyan labels
    let val = |s: &str| style::fg(s, 252);   // light text
    let hi  = |s: &str| style::bold(&style::fg(s, 156)); // green bold

    let mut lines = Vec::new();
    lines.push(style::bold(&style::fg(&format!("[{}]", kind), 226)));
    lines.push(String::new());
    if !summary.is_empty() { lines.push(format!("{}  {}", lbl("WHAT:"), hi(&summary))); }
    if !when.is_empty() { lines.push(format!("{}  {}", lbl("WHEN:"), val(&when))); }
    if !timezone.is_empty() { lines.push(format!("{}  {}", lbl("  TZ:"), style::fg(&timezone, 245))); }
    if !location.is_empty() { lines.push(format!("{} {}", lbl("WHERE:"), val(&location))); }
    if !rrule.is_empty() { lines.push(format!("{} {}", lbl("RECUR:"), val(&rrule))); }
    if !status.is_empty() {
        let sc = match status.to_uppercase().as_str() {
            "CONFIRMED" => 46, "CANCELLED" | "CANCELED" => 196, "TENTATIVE" => 226,
            _ => 252,
        };
        lines.push(format!("{}  {}", lbl("STATUS:"), style::fg(&status, sc)));
    }
    if !priority.is_empty() { lines.push(format!("{}  {}", lbl("PRIORITY:"), val(&priority))); }
    lines.push(String::new());
    if !organizer.is_empty() { lines.push(format!("{} {}", lbl("ORGANIZER:"), val(&organizer))); }
    if !attendees.is_empty() {
        lines.push(format!("{}", lbl("PARTICIPANTS:")));
        for (name, pstat) in &attendees {
            let status_str = if pstat.is_empty() { String::new() }
                else {
                    let sc = match pstat.as_str() {
                        "accepted" => 46, "declined" => 196, "tentative" => 226, _ => 245,
                    };
                    format!(" ({})", style::fg(pstat, sc))
                };
            lines.push(format!("  {}{}", val(name), status_str));
        }
    }
    if !description.is_empty() {
        lines.push(String::new());
        lines.push(format!("{}", lbl("DESCRIPTION:")));
        for dline in description.lines() {
            lines.push(style::fg(dline, 248));
        }
    }
    lines.join("\n")
}

fn parse_rrule_display(rrule: &str) -> String {
    let mut parts = std::collections::HashMap::new();
    for p in rrule.split(';') {
        if let Some((k, v)) = p.split_once('=') { parts.insert(k.to_string(), v.to_string()); }
    }
    let interval = parts.get("INTERVAL").map(|v| v.as_str()).unwrap_or("1");
    let freq = parts.get("FREQ").map(|v| v.as_str()).unwrap_or("");
    let mut s = match freq {
        "DAILY" => if interval == "1" { "Daily".into() } else { format!("Every {} days", interval) },
        "WEEKLY" => if interval == "1" { "Weekly".into() } else { format!("Every {} weeks", interval) },
        "MONTHLY" => if interval == "1" { "Monthly".into() } else { format!("Every {} months", interval) },
        "YEARLY" => if interval == "1" { "Yearly".into() } else { format!("Every {} years", interval) },
        _ => freq.to_string(),
    };
    if let Some(count) = parts.get("COUNT") { s += &format!(" ({} times)", count); }
    if let Some(until) = parts.get("UNTIL") {
        if until.len() >= 8 { s += &format!(" (until {}-{}-{})", &until[0..4], &until[4..6], &until[6..8]); }
    }
    s
}

/// Extract MIME attachments from raw content, decode to temp files, return as JSON Value array
/// matching the DB attachment format so existing v/V handlers work.
fn extract_mime_attachments(content: &str, msg_id: i64) -> Vec<serde_json::Value> {
    let mut atts = Vec::new();
    // Find all boundaries in the content (including nested)
    let boundary_re = regex::Regex::new(r#"boundary="?([^"\s;]+)"?"#).unwrap();
    let mut boundaries = Vec::new();
    for cap in boundary_re.captures_iter(content) {
        boundaries.push(cap.get(1).unwrap().as_str().to_string());
    }
    // Also detect bare -- boundary lines
    for line in content.lines() {
        if line.starts_with("--") && line.len() > 5 {
            let b = line[2..].trim_end_matches("--").trim();
            if !b.is_empty() && !boundaries.contains(&b.to_string()) {
                boundaries.push(b.to_string());
            }
        }
    }

    for boundary in &boundaries {
        let delimiter = format!("--{}", boundary);
        for part in content.split(&delimiter) {
            let Some(hdr_end) = part.find("\n\n").or_else(|| part.find("\r\n\r\n")) else { continue };
            let headers = &part[..hdr_end];
            let body_start = if part[hdr_end..].starts_with("\r\n\r\n") { hdr_end + 4 } else { hdr_end + 2 };
            let body = &part[body_start..];
            let headers_lower = headers.to_lowercase();

            // Skip text/* and multipart/* parts
            if headers_lower.contains("text/plain") || headers_lower.contains("text/html")
                || headers_lower.contains("text/calendar") || headers_lower.contains("multipart/") {
                continue;
            }

            // Must have a Content-Type with a non-text type
            let ct_re = regex::Regex::new(r#"(?i)Content-Type:\s*([^;\n]+)"#).unwrap();
            let Some(ct_cap) = ct_re.captures(headers) else { continue };
            let ctype = ct_cap.get(1).unwrap().as_str().trim().to_string();

            // Get filename from name= or filename=
            let name_re = regex::Regex::new(r#"(?i)(?:name|filename)="([^"]+)""#).unwrap();
            let filename_raw = name_re.captures(headers)
                .map(|c| c.get(1).unwrap().as_str().to_string())
                .unwrap_or_else(|| format!("attachment_{}", atts.len() + 1));
            let filename = sources::maildir::decode_rfc2047(&filename_raw);

            // Skip if already found this filename
            if atts.iter().any(|a: &serde_json::Value| a["name"].as_str() == Some(&filename)) { continue; }

            // Decode body to temp file
            let is_b64 = headers_lower.contains("base64");
            let tmp_path = format!("/tmp/kastrup_att_{}_{}", msg_id, filename);
            if is_b64 {
                if let Some(bytes) = sources::maildir::base64_decode(body.trim()) {
                    let _ = std::fs::write(&tmp_path, &bytes);
                }
            } else {
                let _ = std::fs::write(&tmp_path, body);
            }

            let is_image = ctype.starts_with("image/");
            atts.push(serde_json::json!({
                "name": filename,
                "filename": filename,
                "content_type": ctype,
                "size": std::fs::metadata(&tmp_path).map(|m| m.len()).unwrap_or(0),
                "source_file": tmp_path,
                "url": format!("file://{}", tmp_path),
                "is_image": is_image,
            }));
        }
    }
    atts
}

/// Check if content looks like raw base64 (no MIME headers, just base64 lines).
fn looks_base64(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.len() < 20 { return false; }
    // Check first few lines: should be long lines of base64 chars only
    let mut b64_lines = 0;
    for line in trimmed.lines().take(5) {
        let l = line.trim();
        if l.is_empty() { continue; }
        if l.len() < 20 { return false; }
        if l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/' || b == b'=') {
            b64_lines += 1;
        } else {
            return false;
        }
    }
    b64_lines >= 2
}

/// Convert ISO-8859-1 / Windows-1252 bytes to UTF-8 string.
fn latin1_to_utf8(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| b as char).collect()
}

/// Decode quoted-printable to raw bytes (for charset-aware conversion).
fn decode_qp_bytes_body(s: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(s.len());
    let input = s.as_bytes();
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'=' {
            if i + 1 < input.len() && (input[i + 1] == b'\r' || input[i + 1] == b'\n') {
                i += 1;
                if i < input.len() && input[i] == b'\r' { i += 1; }
                if i < input.len() && input[i] == b'\n' { i += 1; }
            } else if i + 2 < input.len() {
                let hex = &s[i + 1..i + 3];
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    bytes.push(byte);
                    i += 3;
                } else {
                    bytes.push(b'=');
                    i += 1;
                }
            } else {
                bytes.push(b'=');
                i += 1;
            }
        } else {
            bytes.push(input[i]);
            i += 1;
        }
    }
    bytes
}

/// Decode quoted-printable encoding: =XX hex escapes, =\n soft line breaks
fn decode_quoted_printable(s: &str) -> String {
    let mut bytes = Vec::with_capacity(s.len());
    let input = s.as_bytes();
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'=' {
            if i + 1 < input.len() && (input[i + 1] == b'\r' || input[i + 1] == b'\n') {
                // Soft line break
                i += 1;
                if i < input.len() && input[i] == b'\r' { i += 1; }
                if i < input.len() && input[i] == b'\n' { i += 1; }
            } else if i + 2 < input.len() {
                let hex = &s[i + 1..i + 3];
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    bytes.push(byte);
                    i += 3;
                } else {
                    bytes.push(b'=');
                    i += 1;
                }
            } else {
                bytes.push(b'=');
                i += 1;
            }
        } else {
            bytes.push(input[i]);
            i += 1;
        }
    }
    String::from_utf8(bytes).unwrap_or_else(|e| {
        // Fall back to lossy conversion
        String::from_utf8_lossy(e.as_bytes()).into_owned()
    })
}

// --- HTML to text ---

fn html_to_text(html: &str) -> String {
    let mut result = String::new();
    let mut in_tag = false;
    let mut in_script = false;
    let mut in_style = false;
    let mut last_was_block = false;

    let lower = html.to_lowercase();
    let chars: Vec<char> = html.chars().collect();
    let lower_chars: Vec<char> = lower.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if in_tag {
            if chars[i] == '>' {
                in_tag = false;
            }
            i += 1;
            continue;
        }

        if chars[i] == '<' {
            let rest: String = lower_chars[i..].iter().take(20).collect();
            if rest.starts_with("<script") { in_script = true; }
            if rest.starts_with("</script") { in_script = false; }
            if rest.starts_with("<style") { in_style = true; }
            if rest.starts_with("</style") { in_style = false; }

            if rest.starts_with("<br") || rest.starts_with("<p")
                || rest.starts_with("</p") || rest.starts_with("<div")
                || rest.starts_with("</div") || rest.starts_with("<li")
                || rest.starts_with("<tr") || rest.starts_with("<h1")
                || rest.starts_with("<h2") || rest.starts_with("<h3")
                || rest.starts_with("<h4") || rest.starts_with("<h5")
                || rest.starts_with("<h6")
            {
                if !last_was_block {
                    result.push('\n');
                    last_was_block = true;
                }
            }

            in_tag = true;
            i += 1;
            continue;
        }

        if in_script || in_style {
            i += 1;
            continue;
        }

        // HTML entity decoding
        if chars[i] == '&' {
            let rest: String = chars[i..].iter().take(10).collect();
            // Find the entity (up to ';')
            let entity_end = chars[i..].iter().take(12).position(|&c| c == ';');
            if let Some(end) = entity_end {
                let entity: String = chars[i..i + end + 1].iter().collect();
                let decoded = match entity.as_str() {
                    "&amp;" => Some('&'), "&lt;" => Some('<'), "&gt;" => Some('>'),
                    "&quot;" => Some('"'), "&apos;" => Some('\''), "&nbsp;" => Some(' '),
                    "&ndash;" => Some('\u{2013}'), "&mdash;" => Some('\u{2014}'),
                    "&lsquo;" => Some('\u{2018}'), "&rsquo;" => Some('\u{2019}'),
                    "&ldquo;" => Some('\u{201C}'), "&rdquo;" => Some('\u{201D}'),
                    "&bull;" => Some('\u{2022}'), "&hellip;" => Some('\u{2026}'),
                    "&trade;" => Some('\u{2122}'), "&copy;" => Some('\u{00A9}'),
                    "&reg;" => Some('\u{00AE}'), "&deg;" => Some('\u{00B0}'),
                    "&sup1;" => Some('\u{00B9}'), "&sup2;" => Some('\u{00B2}'),
                    "&sup3;" => Some('\u{00B3}'), "&frac12;" => Some('\u{00BD}'),
                    "&zwnj;" | "&zwj;" => Some('\u{200C}'),
                    _ => None,
                };
                if let Some(c) = decoded {
                    if c != '\u{200C}' { result.push(c); } // skip zero-width chars
                    i += end + 1;
                    continue;
                }
            }
            // Numeric entities: &#NNN; or &#xHHH;
            if let Some(end) = entity_end {
                let entity: String = chars[i..i + end + 1].iter().collect();
                if entity.starts_with("&#") {
                    let num_str = &entity[2..entity.len() - 1];
                    let code = if num_str.starts_with('x') || num_str.starts_with('X') {
                        u32::from_str_radix(&num_str[1..], 16).ok()
                    } else {
                        num_str.parse::<u32>().ok()
                    };
                    if let Some(c) = code.and_then(char::from_u32) {
                        result.push(c);
                        i += end + 1;
                        continue;
                    }
                }
            }
        }

        last_was_block = false;
        result.push(chars[i]);
        i += 1;
    }

    // Clean up: collapse multiple blank lines, trim
    let mut cleaned = String::new();
    let mut blank_count = 0;
    for line in result.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            blank_count += 1;
            if blank_count <= 2 { cleaned.push('\n'); }
        } else {
            blank_count = 0;
            cleaned.push_str(trimmed);
            cleaned.push('\n');
        }
    }
    cleaned
}

// --- Utilities ---

fn source_info(source_type: &str, tc: &config::ThemeColors) -> (&'static str, u8) {
    match source_type {
        "discord" => ("\u{25C6}", tc.src_discord),
        "slack" => ("#", tc.src_slack),
        "telegram" => ("\u{2708}", tc.src_telegram),
        "whatsapp" => ("\u{25C9}", tc.src_whatsapp),
        "reddit" => ("\u{00AE}", tc.src_reddit),
        "email" | "maildir" | "imap" | "gmail" => ("\u{2709}", tc.src_email),
        "rss" => ("\u{25C8}", tc.src_rss),
        "web" | "webpage" => ("\u{25CE}", tc.src_web),
        "messenger" => ("\u{260E}", tc.src_messenger),
        "instagram" => ("\u{25C8}", tc.src_instagram),
        "weechat" | "workspace" => ("\u{2318}", tc.src_weechat),
        _ => ("\u{2022}", tc.src_default),
    }
}

/// Format a unix timestamp using a simple date format string.
/// Avoids the chrono dependency by computing date components manually.
fn format_timestamp(ts: i64, fmt: &str) -> String {
    if ts == 0 { return String::new(); }

    // Apply local timezone offset
    let utc_offset = local_utc_offset();
    let local_ts = ts + utc_offset;

    let secs = local_ts;
    let days = secs.div_euclid(86400);
    let (y, m, d) = days_to_ymd(days);
    let time_of_day = secs.rem_euclid(86400);
    let hours = time_of_day / 3600;
    let mins = (time_of_day % 3600) / 60;

    let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let month_name = months.get((m - 1) as usize).unwrap_or(&"???");

    match fmt {
        "%b %e" | "%b %-d" => format!("{} {:>2}", month_name, d),
        "%d/%m %H:%M" => format!("{:02}/{:02} {:02}:{:02}", d, m, hours, mins),
        "%m/%d %H:%M" => format!("{:02}/{:02} {:02}:{:02}", m, d, hours, mins),
        "%Y-%m-%d %H:%M" => format!("{}-{:02}-{:02} {:02}:{:02}", y, m, d, hours, mins),
        "%d.%m %H:%M" => format!("{:02}.{:02} {:02}:{:02}", d, m, hours, mins),
        "%d %b %H:%M" => format!("{:02} {} {:02}:{:02}", d, month_name, hours, mins),
        "%b %d %H:%M" => format!("{} {:02} {:02}:{:02}", month_name, d, hours, mins),
        _ => format!("{} {:>2}", month_name, d),
    }
}

/// Convert days since epoch to (year, month, day).
/// Algorithm from http://howardhinnant.github.io/date_algorithms.html
fn days_to_ymd(days: i64) -> (i64, i64, i64) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Get local UTC offset in seconds using libc
fn local_utc_offset() -> i64 {
    unsafe {
        let mut now: libc::time_t = 0;
        libc::time(&mut now);
        let mut tm: libc::tm = std::mem::zeroed();
        libc::localtime_r(&now, &mut tm);
        tm.tm_gmtoff as i64
    }
}

/// Truncate a plain string to at most `max` characters
fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{}\u{2026}", truncated)
    }
}

/// Parse a JSON array string like `["a@b.com","c@d.com"]` into a comma-separated display string.
/// Falls back to returning the raw string if parsing fails.
fn parse_json_recipients(raw: &str) -> String {
    let joined = if let Ok(arr) = serde_json::from_str::<Vec<String>>(raw) {
        arr.join(", ")
    } else {
        raw.to_string()
    };
    // Decode any RFC 2047 encoded-words (e.g. =?iso-8859-1?Q?...?=)
    if joined.contains("=?") {
        sources::maildir::decode_rfc2047(&joined)
    } else {
        joined
    }
}

/// Format a byte count into a human-readable file size
fn format_file_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

// --- Image helpers ---

fn home_dir() -> std::path::PathBuf {
    std::env::var("HOME").map(std::path::PathBuf::from).unwrap_or_else(|_| std::path::PathBuf::from("."))
}

/// Sync the Seen (S) flag to a maildir file on disk.
/// Maildir flags are in the filename: `unique:2,FLAGS` where S=Seen, F=Flagged, R=Replied.
/// If the file is in new/, move to cur/ and add the S flag.
fn sync_maildir_seen_flag(metadata: &serde_json::Value, db: &database::Database, msg_id: i64) {
    let Some(file_path) = metadata.get("maildir_file").and_then(|v| v.as_str()) else { return };
    let path = std::path::Path::new(file_path);
    if !path.exists() { return; }

    let filename = path.file_name().and_then(|f| f.to_str()).unwrap_or("");
    let parent = path.parent().unwrap_or(std::path::Path::new("."));
    let parent_name = parent.file_name().and_then(|f| f.to_str()).unwrap_or("");

    let new_filename = if filename.contains(":2,") {
        // Already has flags section - add S if not present
        if filename.contains('S') { return; } // Already seen
        let (base, flags) = filename.rsplit_once(":2,").unwrap();
        let mut flag_chars: Vec<char> = flags.chars().collect();
        flag_chars.push('S');
        flag_chars.sort(); // Maildir flags must be alphabetically sorted
        format!("{}:2,{}", base, flag_chars.into_iter().collect::<String>())
    } else {
        // No flags section yet - add one
        format!("{}:2,S", filename)
    };

    // If in new/, move to cur/
    let new_parent = if parent_name == "new" {
        parent.parent().unwrap_or(parent).join("cur")
    } else {
        parent.to_path_buf()
    };

    let new_path = new_parent.join(&new_filename);
    if std::fs::rename(path, &new_path).is_ok() {
        // Update the metadata in DB with new file path
        let mut new_meta = metadata.clone();
        new_meta["maildir_file"] = serde_json::json!(new_path.to_string_lossy().to_string());
        let meta_json = serde_json::to_string(&new_meta).unwrap_or_default();
        let conn = db.conn.lock().unwrap();
        let _ = conn.execute(
            "UPDATE messages SET metadata = ? WHERE id = ?",
            rusqlite::params![meta_json, msg_id],
        );
    }
}

/// Background version (called from writer thread)
fn sync_maildir_seen_flag_bg(metadata: &serde_json::Value, db: &database::Database, msg_id: i64) {
    sync_maildir_seen_flag(metadata, db, msg_id);
}

/// Check if a filename has an image extension
fn is_image_filename(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower.ends_with(".jpg") || lower.ends_with(".jpeg") || lower.ends_with(".png")
        || lower.ends_with(".gif") || lower.ends_with(".webp") || lower.ends_with(".bmp")
}

/// Check if an attachment JSON value represents an image
fn is_image_attachment(att: &serde_json::Value) -> bool {
    let ctype = att.get("content_type").and_then(|v| v.as_str()).unwrap_or("");
    let fname = att.get("name").or_else(|| att.get("filename")).and_then(|v| v.as_str()).unwrap_or("");
    ctype.starts_with("image") || is_image_filename(fname)
}

/// Extract image URLs from HTML content, skipping tracking pixels and tiny icons
fn extract_image_urls(html: &str) -> Vec<String> {
    let mut urls = Vec::new();
    let lower = html.to_lowercase();
    let mut pos = 0;
    while let Some(img_start) = lower[pos..].find("<img") {
        let abs = pos + img_start;
        if let Some(end) = lower[abs..].find('>') {
            let tag = &html[abs..abs + end + 1];
            // Extract src attribute
            if let Some(src_pos) = tag.to_lowercase().find("src=") {
                let rest = &tag[src_pos + 4..];
                let (delim, start) = if rest.starts_with('"') { ('"', 1) }
                    else if rest.starts_with('\'') { ('\'', 1) }
                    else { (' ', 0) };
                if let Some(end_pos) = rest[start..].find(delim) {
                    let url = &rest[start..start + end_pos];
                    // Skip tracking pixels, icons, spacers, logos, badges
                    let lower_url = url.to_lowercase();
                    if !lower_url.contains("track") && !lower_url.contains("pixel")
                        && !lower_url.contains("spacer") && !lower_url.contains("beacon")
                        && !lower_url.ends_with(".gif")
                        && !lower_url.contains("icon") && !lower_url.contains("logo")
                        && !lower_url.contains("badge") && !lower_url.contains("button")
                        && !lower_url.contains("social") && !lower_url.contains("facebook")
                        && !lower_url.contains("linkedin") && !lower_url.contains("twitter")
                        && !lower_url.contains("instagram")
                    {
                        // Skip small images by checking width/height attrs
                        let tag_lower = tag.to_lowercase();
                        let w: Option<u32> = tag_lower.find("width=").and_then(|p| {
                            tag[p+6..].trim_start_matches(&['"', '\''][..])
                                .split(|c: char| !c.is_ascii_digit()).next()?.parse().ok()
                        });
                        let h: Option<u32> = tag_lower.find("height=").and_then(|p| {
                            tag[p+7..].trim_start_matches(&['"', '\''][..])
                                .split(|c: char| !c.is_ascii_digit()).next()?.parse().ok()
                        });
                        if w.unwrap_or(100) > 40 && h.unwrap_or(100) > 40 {
                            urls.push(url.to_string());
                        }
                    }
                }
            }
            pos = abs + end + 1;
        } else {
            break;
        }
    }
    urls
}

/// Simple string hash for cache filenames
fn simple_hash(s: &str) -> String {
    let mut h: u64 = 5381;
    for b in s.bytes() {
        h = h.wrapping_mul(33).wrapping_add(b as u64);
    }
    format!("{:016x}", h)
}

/// Simple base64 encoding for MIME attachments
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}
