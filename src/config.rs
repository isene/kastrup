use std::collections::HashMap;
use std::path::PathBuf;

/// Color values for the UI theme
#[derive(Clone)]
pub struct ThemeColors {
    pub unread: u8,
    pub read: u8,
    pub accent: u8,
    pub thread: u8,
    pub dm: u8,
    pub tag: u8,
    pub star: u8,
    pub quote1: u8,
    pub quote2: u8,
    pub quote3: u8,
    pub quote4: u8,
    pub sig: u8,
    pub link: u8,
    /// Background for a collapsed thread's row, so a folded conversation
    /// reads as a band rather than one more line of text.
    pub fold_bg: u16,
    pub top_bg: u16,
    pub bottom_bg: u16,
    pub cmd_bg: u16,
    pub header_from: u8,
    pub header_subj: u8,
    pub header_date: u8,
    pub header_label: u8,
    pub separator: u8,
    pub attachment: u8,
    pub html_hint: u8,
    pub replied: u8,
    pub delete_mark: u8,
    pub attach_ind: u8,
    pub date_fg: u8,
    pub view_all: u8,
    pub view_new: u8,
    pub view_sources: u8,
    pub view_custom: u8,
    pub view_starred: u8,
    pub info_fg: u8,
    pub hint_fg: u8,
    pub prefix_fg: u8,
    pub no_msg: u8,
    pub feedback_warn: u8,
    pub feedback_ok: u8,
    pub feedback_info: u8,
    // Source type colors. Each source has a *row text color* (used
    // for sender / subject) and an optional *icon color* that, if set
    // to a value other than the row color, paints just the leading
    // glyph (e.g. `@` for email) without recolouring the rest of
    // the row.
    pub src_email: u8,
    pub src_email_icon: u8,
    pub src_discord: u8,
    pub src_discord_icon: u8,
    pub src_slack: u8,
    pub src_slack_icon: u8,
    pub src_telegram: u8,
    pub src_telegram_icon: u8,
    pub src_whatsapp: u8,
    pub src_whatsapp_icon: u8,
    pub src_reddit: u8,
    pub src_reddit_icon: u8,
    pub src_rss: u8,
    pub src_rss_icon: u8,
    pub src_web: u8,
    pub src_web_icon: u8,
    pub src_messenger: u8,
    pub src_messenger_icon: u8,
    pub src_instagram: u8,
    pub src_instagram_icon: u8,
    pub src_weechat: u8,
    pub src_weechat_icon: u8,
    pub src_sms: u8,
    pub src_sms_icon: u8,
    pub src_signal: u8,
    pub src_signal_icon: u8,
    pub src_linkedin: u8,
    pub src_linkedin_icon: u8,
    pub src_default: u8,
    pub src_default_icon: u8,
    /// Per-platform style overrides keyed by source_type / platform slug
    /// (e.g. a relay "Add app" slug). Value: (color, glyph). source_info
    /// consults this before its built-in matches, so any platform can get
    /// a custom colour/icon from ~/.kastrup/config.yml with no code change
    /// — and platform-specific names stay out of the source tree. Empty
    /// glyph keeps the default bullet. Loaded from config.yml only.
    pub source_styles: HashMap<String, (u8, String)>,
    pub content_fg: u8,
    pub content_bg: u8,
    pub list_fg: u8,
    pub list_bg: u8,
    pub border_fg: u8,
}

impl Default for ThemeColors {
    fn default() -> Self {
        Self {
            unread: 226, read: 249, accent: 10, thread: 255, dm: 201,
            tag: 14, star: 226,
            quote1: 114, quote2: 180, quote3: 139, quote4: 109,
            sig: 242, link: 4,
            fold_bg: 236, top_bg: 235, bottom_bg: 235, cmd_bg: 17,
            header_from: 2, header_subj: 1, header_date: 240,
            header_label: 51, separator: 238, attachment: 208,
            html_hint: 51, replied: 45, delete_mark: 88,
            attach_ind: 208, date_fg: 245,
            view_all: 226, view_new: 40, view_sources: 201,
            view_custom: 51, view_starred: 226,
            info_fg: 252, hint_fg: 245, prefix_fg: 248,
            no_msg: 245,
            feedback_warn: 220, feedback_ok: 40, feedback_info: 245,
            // Default row text colors per source (sender + subject).
            src_email: 39, src_discord: 99, src_slack: 35, src_telegram: 51,
            src_whatsapp: 40, src_reddit: 202, src_rss: 226, src_web: 208,
            src_messenger: 33, src_instagram: 205, src_weechat: 75,
            // SMS (phone gateway, native) green-cyan; Signal blue. Kept
            // distinct from src_whatsapp (40) and src_messenger (33),
            // which they otherwise visually collided with.
            // LinkedIn brand blue (25), distinct from messenger (33) /
            // signal (27) / telegram (51). Reddit reuses src_reddit (202).
            src_sms: 48, src_signal: 27, src_linkedin: 25, src_default: 15,
            // Icon colors. Default each icon to the same as the row
            // color so behaviour is unchanged from earlier builds —
            // the only outlier is email, where `@` reads better as
            // a quiet light-gray separator (244) than as a colored
            // glyph against the message line.
            src_email_icon: 244,
            src_discord_icon: 99, src_slack_icon: 35, src_telegram_icon: 51,
            src_whatsapp_icon: 40, src_reddit_icon: 202, src_rss_icon: 226,
            src_web_icon: 208, src_messenger_icon: 33, src_instagram_icon: 205,
            src_weechat_icon: 75,
            src_sms_icon: 48, src_signal_icon: 27, src_linkedin_icon: 25,
            src_default_icon: 15,
            source_styles: HashMap::new(),
            content_fg: 252, content_bg: 0, list_fg: 252, list_bg: 0, border_fg: 238,
        }
    }
}

impl ThemeColors {
    pub fn for_theme(name: &str) -> Self {
        let mut t = Self::default();
        match name {
            "Mutt" => { t.accent = 14; t.thread = 252; t.dm = 213; t.tag = 81; t.sig = 243; }
            "Ocean" => { t.unread = 51; t.accent = 45; t.thread = 45; t.dm = 171; t.tag = 87;
                         t.quote1 = 75; t.quote2 = 117; t.quote3 = 153; t.quote4 = 189; }
            "Forest" => { t.unread = 77; t.accent = 10; t.thread = 78; t.dm = 176; t.tag = 48;
                          t.quote1 = 114; t.quote2 = 150; t.quote3 = 186; t.quote4 = 222; }
            "Amber" => { t.unread = 220; t.accent = 226; t.thread = 214; t.dm = 209; t.tag = 214;
                         t.quote1 = 222; t.quote2 = 186; t.quote3 = 180; t.quote4 = 174; }
            _ => {} // Default theme
        }
        t
    }
}

/// A custom view definition from config
// Parsed from kastruprc `custom_views`, kept for backward compat; the DB
// `views` table is authoritative at runtime, so these fields aren't all read.
#[allow(dead_code)]
#[derive(Default, Clone)]
pub struct ViewDef {
    pub name: String,
    pub filters: String,
    pub sort_order: Option<String>,
}

/// An identity for sending messages
#[derive(Default, Clone)]
pub struct Identity {
    pub name: String,
    pub email: String,
    pub signature_path: String, // path to sig file/script (expanded ~)
    pub smtp: Option<String>,
}

impl Identity {
    /// "Name <email>", or the bare email when the identity has no name.
    pub fn from_line(&self) -> String {
        if self.name.is_empty() { self.email.clone() }
        else { format!("{} <{}>", self.name, self.email) }
    }

    /// Get signature text. If path is executable, run it each time for fresh output.
    /// If it's a file, read it. Preserves leading whitespace.
    pub fn signature(&self) -> String {
        if self.signature_path.is_empty() { return String::new(); }
        let p = std::path::Path::new(&self.signature_path);
        if !p.exists() { return String::new(); }
        use std::os::unix::fs::PermissionsExt;
        if p.metadata().map(|m| m.permissions().mode() & 0o111 != 0).unwrap_or(false) {
            std::process::Command::new(&self.signature_path).output()
                .ok().and_then(|o| String::from_utf8(o.stdout).ok())
                .unwrap_or_default()
                .trim_end().to_string()
        } else {
            std::fs::read_to_string(&self.signature_path).unwrap_or_default()
                .trim_end().to_string()
        }
    }
}

/// Application configuration loaded from ~/.kastrup/config.yml and ~/.kastrup/kastruprc
pub struct Config {
    pub color_theme: String,
    pub date_format: String,
    pub sort_order: String,
    pub sort_inverted: bool,
    pub pane_width: u16,
    pub border_style: u8,
    pub default_view: String,
    pub download_folder: String,
    pub editor_args: String,
    pub default_email: String,
    pub smtp_command: String,
    pub confirm_purge: bool,
    /// How many status lines to keep so a missed one can still be read.
    pub message_log: usize,
    /// Folder shared with the phone (Syncthing), holding one
    /// `mail-read-<device>.json` per device. Empty disables the exchange
    /// entirely — no stat, no query, no file.
    pub read_sync_dir: String,
    /// How far back the exchange reaches. Must cover the phone's own
    /// window, or a mark from a message the phone still shows would have
    /// nothing here to land on.
    pub read_sync_days: i64,
    pub load_limit: usize,
    #[allow(dead_code)] // back-compat config surface; DB views are authoritative
    pub custom_views: HashMap<String, ViewDef>,
    pub identities: HashMap<String, Identity>,
    pub folder_hooks: Vec<(String, String)>,
    pub custom_bindings: HashMap<String, String>,
    pub channel_names: HashMap<String, String>,
    pub save_folders: HashMap<String, String>,
    /// Per-source-type external sender commands. Map from plugin_type
    /// (e.g. "workspace", "discord") to a map of action → shell template.
    /// Placeholders: `@conv`, `@msg`, `@to`, `@emoji`. If the template does
    /// not include `@text`, kastrup pipes the composed body on stdin.
    ///
    /// Example in ~/.kastrup/config.yml:
    ///   senders:
    ///     workspace:
    ///       send:    "ws-bridge send --conv @conv --stdin"
    ///       reply:   "ws-bridge reply --conv @conv --to @msg --stdin"
    ///       react:   "ws-bridge react --conv @conv --msg @msg --emoji @emoji"
    ///       unreact: "ws-bridge unreact --conv @conv --msg @msg --emoji @emoji"
    ///       sync:    "ws-bridge sync"
    pub senders: HashMap<String, HashMap<String, String>>,
    /// Hand new non-mail rows to an outside indexer after each poll
    /// cycle. See `feeder.rs`. Example:
    ///   push:
    ///     url: http://localhost:8100
    ///     connector: <uuid>
    ///     key_file: /home/.safe/corpintel-push.key
    pub push: Option<crate::feeder::PushConfig>,
    /// Deliver a phone-gateway reply for `<platform>:<thread_key>` through a
    /// native chat target instead of the (phone-drained) gateway outbox.
    /// Key = `<platform>:<thread_key>`, value = a chat_send target:
    /// `channel:<id>` / `webhook:<name>` / `dm:<userId>` for discord, or a
    /// Slack channel for slack. The IDs live only in the local config.yml.
    pub gateway_routes: HashMap<String, String>,
    pub theme_colors: ThemeColors,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            color_theme: "Default".to_string(),
            date_format: "%b %e".to_string(),
            sort_order: "latest".to_string(),
            sort_inverted: false,
            pane_width: 3,
            border_style: 1,
            default_view: "A".to_string(),
            download_folder: "~/Downloads".to_string(),
            editor_args: String::new(),
            default_email: String::new(),
            smtp_command: String::new(),
            confirm_purge: false,
            message_log: 10,
            read_sync_dir: String::new(),
            read_sync_days: 60,
            load_limit: 500,
            custom_views: HashMap::new(),
            identities: HashMap::new(),
            folder_hooks: Vec::new(),
            custom_bindings: HashMap::new(),
            channel_names: HashMap::new(),
            save_folders: HashMap::new(),
            senders: HashMap::new(),
            push: None,
            gateway_routes: HashMap::new(),
            theme_colors: ThemeColors::default(),
        }
    }
}

impl Config {
    /// Save current settings to ~/.kastrup/config.yml
    /// Loads existing YAML first and merges, preserving unknown keys.
    pub fn save(&self) {
        let path = config_yml_path();
        let mut data: serde_json::Value = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_yaml::from_str(&s).ok())
            .unwrap_or(serde_json::json!({}));

        let tc = &self.theme_colors;

        data["version"] = serde_json::json!("0.1.0");
        data["ui"] = serde_json::json!({
            "width": self.pane_width,
            "border": self.border_style,
            "date_format": self.date_format,
            "color_theme": self.color_theme,
            "sort_order": self.sort_order,
            "sort_inverted": self.sort_inverted,
            "confirm_purge": self.confirm_purge,
            "message_log": self.message_log,
            "read_sync_dir": self.read_sync_dir,
            "read_sync_days": self.read_sync_days,
            "default_view": self.default_view,
            "editor_args": self.editor_args,
        });
        // Build the colors map manually — the json! macro's
        // recursion depth blows out around ~25 entries on stable.
        let mut colors_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
        let put = |m: &mut serde_json::Map<String, serde_json::Value>, k: &str, v: u8| {
            m.insert(k.to_string(), serde_json::json!(v));
        };
        put(&mut colors_map, "unread", tc.unread);
        put(&mut colors_map, "read", tc.read);
        put(&mut colors_map, "accent", tc.accent);
        put(&mut colors_map, "thread", tc.thread);
        put(&mut colors_map, "dm", tc.dm);
        put(&mut colors_map, "tag", tc.tag);
        put(&mut colors_map, "star", tc.star);
        put(&mut colors_map, "quote1", tc.quote1);
        put(&mut colors_map, "quote2", tc.quote2);
        put(&mut colors_map, "quote3", tc.quote3);
        put(&mut colors_map, "quote4", tc.quote4);
        put(&mut colors_map, "sig", tc.sig);
        put(&mut colors_map, "link", tc.link);
        // Source row + icon colors. icon variants embed in the
        // pre-styled glyph; row variants color the rest of the line.
        put(&mut colors_map, "src_email", tc.src_email);
        put(&mut colors_map, "src_email_icon", tc.src_email_icon);
        put(&mut colors_map, "src_discord", tc.src_discord);
        put(&mut colors_map, "src_discord_icon", tc.src_discord_icon);
        put(&mut colors_map, "src_slack", tc.src_slack);
        put(&mut colors_map, "src_slack_icon", tc.src_slack_icon);
        put(&mut colors_map, "src_telegram", tc.src_telegram);
        put(&mut colors_map, "src_telegram_icon", tc.src_telegram_icon);
        put(&mut colors_map, "src_whatsapp", tc.src_whatsapp);
        put(&mut colors_map, "src_whatsapp_icon", tc.src_whatsapp_icon);
        put(&mut colors_map, "src_reddit", tc.src_reddit);
        put(&mut colors_map, "src_reddit_icon", tc.src_reddit_icon);
        put(&mut colors_map, "src_rss", tc.src_rss);
        put(&mut colors_map, "src_rss_icon", tc.src_rss_icon);
        put(&mut colors_map, "src_web", tc.src_web);
        put(&mut colors_map, "src_web_icon", tc.src_web_icon);
        put(&mut colors_map, "src_messenger", tc.src_messenger);
        put(&mut colors_map, "src_messenger_icon", tc.src_messenger_icon);
        put(&mut colors_map, "src_instagram", tc.src_instagram);
        put(&mut colors_map, "src_instagram_icon", tc.src_instagram_icon);
        put(&mut colors_map, "src_weechat", tc.src_weechat);
        put(&mut colors_map, "src_weechat_icon", tc.src_weechat_icon);
        put(&mut colors_map, "src_sms", tc.src_sms);
        put(&mut colors_map, "src_sms_icon", tc.src_sms_icon);
        put(&mut colors_map, "src_signal", tc.src_signal);
        put(&mut colors_map, "src_signal_icon", tc.src_signal_icon);
        put(&mut colors_map, "src_linkedin", tc.src_linkedin);
        put(&mut colors_map, "src_linkedin_icon", tc.src_linkedin_icon);
        put(&mut colors_map, "src_default", tc.src_default);
        put(&mut colors_map, "src_default_icon", tc.src_default_icon);
        put(&mut colors_map, "content_fg", tc.content_fg);
        put(&mut colors_map, "content_bg", tc.content_bg);
        put(&mut colors_map, "list_fg", tc.list_fg);
        put(&mut colors_map, "list_bg", tc.list_bg);
        put(&mut colors_map, "border_fg", tc.border_fg);
        data["colors"] = serde_json::Value::Object(colors_map);
        data["default_email"] = serde_json::json!(self.default_email);
        data["smtp_command"] = serde_json::json!(self.smtp_command);
        data["download_folder"] = serde_json::json!(self.download_folder);

        // Save save_folders
        if !self.save_folders.is_empty() {
            let sf: serde_json::Map<String, serde_json::Value> = self.save_folders.iter()
                .map(|(k, v)| (k.clone(), serde_json::json!(v)))
                .collect();
            data["save_folders"] = serde_json::Value::Object(sf);
        }

        let yaml = serde_yaml::to_string(&data).unwrap_or_default();
        let _ = std::fs::write(path, yaml);
    }

    /// Load configuration from ~/.kastrup/config.yml and ~/.kastrup/kastruprc
    pub fn load() -> Self {
        let mut config = Config::default();
        config.load_rc();    // Base settings from kastruprc
        config.load_yaml();  // Saved overrides from config.yml win (colors, width, border, etc.)
        config
    }

    /// Load settings from ~/.kastrup/config.yml
    fn load_yaml(&mut self) {
        let path = config_yml_path();
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return,
        };
        let yaml: serde_yaml::Value = match serde_yaml::from_str(&content) {
            Ok(v) => v,
            Err(_) => return,
        };

        if let Some(ui) = yaml.get("ui") {
            if let Some(v) = ui.get("width").and_then(|v| v.as_u64()) {
                self.pane_width = v.min(6).max(1) as u16;
            }
            if let Some(v) = ui.get("border").and_then(|v| v.as_u64()) {
                self.border_style = v.min(3) as u8;
            }
            if let Some(v) = ui.get("default_view").and_then(|v| v.as_str()) {
                self.default_view = v.to_string();
            }
            if let Some(v) = ui.get("color_theme").and_then(|v| v.as_str()) {
                self.color_theme = v.to_string();
                // Apply theme preset (saved color overrides loaded below)
                self.theme_colors = ThemeColors::for_theme(v);
            }
            if let Some(v) = ui.get("date_format").and_then(|v| v.as_str()) {
                self.date_format = v.to_string();
            }
            if let Some(v) = ui.get("sort_order").and_then(|v| v.as_str()) {
                self.sort_order = v.to_string();
            }
            if let Some(v) = ui.get("sort_inverted").and_then(|v| v.as_bool()) {
                self.sort_inverted = v;
            }
            if let Some(v) = ui.get("confirm_purge").and_then(|v| v.as_bool()) {
                self.confirm_purge = v;
            }
            if let Some(v) = ui.get("message_log").and_then(|v| v.as_u64()) {
                self.message_log = v as usize;
            }
            if let Some(v) = ui.get("read_sync_dir").and_then(|v| v.as_str()) {
                self.read_sync_dir = v.to_string();
            }
            if let Some(v) = ui.get("read_sync_days").and_then(|v| v.as_i64()) {
                self.read_sync_days = v;
            }
            if let Some(v) = ui.get("editor_args").and_then(|v| v.as_str()) {
                self.editor_args = v.to_string();
            }
        }

        if let Some(v) = yaml.get("default_email").and_then(|v| v.as_str()) {
            self.default_email = v.to_string();
        }
        if let Some(v) = yaml.get("smtp_command").and_then(|v| v.as_str()) {
            self.smtp_command = v.to_string();
        }
        if let Some(v) = yaml.get("download_folder").and_then(|v| v.as_str()) {
            self.download_folder = v.to_string();
        }

        // Load save_folders
        if let Some(sf) = yaml.get("save_folders") {
            if let Some(map) = sf.as_mapping() {
                for (k, v) in map {
                    if let (Some(key), Some(val)) = (k.as_str(), v.as_str()) {
                        self.save_folders.insert(key.to_string(), val.to_string());
                    }
                }
            }
        }

        if let Some(p) = yaml.get("push").and_then(|p| p.as_mapping()) {
            let get = |k: &str| p.get(serde_yaml::Value::String(k.into()))
                .and_then(|v| v.as_str()).map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
            if let (Some(url), Some(connector), Some(key_file)) = (get("url"), get("connector"), get("key_file")) {
                self.push = Some(crate::feeder::PushConfig { url, connector, key_file });
            }
        }

        // Load per-plugin-type external senders (see field docs for shape).
        if let Some(s) = yaml.get("senders") {
            if let Some(by_type) = s.as_mapping() {
                for (type_key, actions) in by_type {
                    let Some(plugin_type) = type_key.as_str() else { continue };
                    let Some(action_map) = actions.as_mapping() else { continue };
                    let mut entry: HashMap<String, String> = HashMap::new();
                    for (ak, av) in action_map {
                        if let (Some(k), Some(v)) = (ak.as_str(), av.as_str()) {
                            entry.insert(k.to_string(), v.to_string());
                        }
                    }
                    if !entry.is_empty() {
                        self.senders.insert(plugin_type.to_string(), entry);
                    }
                }
            }
        }

        // Load saved color overrides (applied AFTER theme preset in Config::load)
        if let Some(colors) = yaml.get("colors") {
            let tc = &mut self.theme_colors;
            if let Some(v) = colors.get("unread").and_then(|v| v.as_u64()) { tc.unread = v as u8; }
            if let Some(v) = colors.get("read").and_then(|v| v.as_u64()) { tc.read = v as u8; }
            if let Some(v) = colors.get("accent").and_then(|v| v.as_u64()) { tc.accent = v as u8; }
            if let Some(v) = colors.get("thread").and_then(|v| v.as_u64()) { tc.thread = v as u8; }
            if let Some(v) = colors.get("dm").and_then(|v| v.as_u64()) { tc.dm = v as u8; }
            if let Some(v) = colors.get("tag").and_then(|v| v.as_u64()) { tc.tag = v as u8; }
            if let Some(v) = colors.get("star").and_then(|v| v.as_u64()) { tc.star = v as u8; }
            if let Some(v) = colors.get("quote1").and_then(|v| v.as_u64()) { tc.quote1 = v as u8; }
            if let Some(v) = colors.get("quote2").and_then(|v| v.as_u64()) { tc.quote2 = v as u8; }
            if let Some(v) = colors.get("quote3").and_then(|v| v.as_u64()) { tc.quote3 = v as u8; }
            if let Some(v) = colors.get("quote4").and_then(|v| v.as_u64()) { tc.quote4 = v as u8; }
            if let Some(v) = colors.get("sig").and_then(|v| v.as_u64()) { tc.sig = v as u8; }
            if let Some(v) = colors.get("link").and_then(|v| v.as_u64()) { tc.link = v as u8; }
            if let Some(v) = colors.get("src_email").and_then(|v| v.as_u64()) { tc.src_email = v as u8; }
            if let Some(v) = colors.get("src_email_icon").and_then(|v| v.as_u64()) { tc.src_email_icon = v as u8; }
            if let Some(v) = colors.get("src_discord").and_then(|v| v.as_u64()) { tc.src_discord = v as u8; }
            if let Some(v) = colors.get("src_discord_icon").and_then(|v| v.as_u64()) { tc.src_discord_icon = v as u8; }
            if let Some(v) = colors.get("src_slack").and_then(|v| v.as_u64()) { tc.src_slack = v as u8; }
            if let Some(v) = colors.get("src_slack_icon").and_then(|v| v.as_u64()) { tc.src_slack_icon = v as u8; }
            if let Some(v) = colors.get("src_telegram").and_then(|v| v.as_u64()) { tc.src_telegram = v as u8; }
            if let Some(v) = colors.get("src_telegram_icon").and_then(|v| v.as_u64()) { tc.src_telegram_icon = v as u8; }
            if let Some(v) = colors.get("src_whatsapp").and_then(|v| v.as_u64()) { tc.src_whatsapp = v as u8; }
            if let Some(v) = colors.get("src_whatsapp_icon").and_then(|v| v.as_u64()) { tc.src_whatsapp_icon = v as u8; }
            if let Some(v) = colors.get("src_reddit").and_then(|v| v.as_u64()) { tc.src_reddit = v as u8; }
            if let Some(v) = colors.get("src_reddit_icon").and_then(|v| v.as_u64()) { tc.src_reddit_icon = v as u8; }
            if let Some(v) = colors.get("src_rss").and_then(|v| v.as_u64()) { tc.src_rss = v as u8; }
            if let Some(v) = colors.get("src_rss_icon").and_then(|v| v.as_u64()) { tc.src_rss_icon = v as u8; }
            if let Some(v) = colors.get("src_web").and_then(|v| v.as_u64()) { tc.src_web = v as u8; }
            if let Some(v) = colors.get("src_web_icon").and_then(|v| v.as_u64()) { tc.src_web_icon = v as u8; }
            if let Some(v) = colors.get("src_messenger").and_then(|v| v.as_u64()) { tc.src_messenger = v as u8; }
            if let Some(v) = colors.get("src_messenger_icon").and_then(|v| v.as_u64()) { tc.src_messenger_icon = v as u8; }
            if let Some(v) = colors.get("src_instagram").and_then(|v| v.as_u64()) { tc.src_instagram = v as u8; }
            if let Some(v) = colors.get("src_instagram_icon").and_then(|v| v.as_u64()) { tc.src_instagram_icon = v as u8; }
            if let Some(v) = colors.get("src_weechat").and_then(|v| v.as_u64()) { tc.src_weechat = v as u8; }
            if let Some(v) = colors.get("src_weechat_icon").and_then(|v| v.as_u64()) { tc.src_weechat_icon = v as u8; }
            if let Some(v) = colors.get("src_sms").and_then(|v| v.as_u64()) { tc.src_sms = v as u8; }
            if let Some(v) = colors.get("src_sms_icon").and_then(|v| v.as_u64()) { tc.src_sms_icon = v as u8; }
            if let Some(v) = colors.get("src_signal").and_then(|v| v.as_u64()) { tc.src_signal = v as u8; }
            if let Some(v) = colors.get("src_signal_icon").and_then(|v| v.as_u64()) { tc.src_signal_icon = v as u8; }
            if let Some(v) = colors.get("src_linkedin").and_then(|v| v.as_u64()) { tc.src_linkedin = v as u8; }
            if let Some(v) = colors.get("src_linkedin_icon").and_then(|v| v.as_u64()) { tc.src_linkedin_icon = v as u8; }
            if let Some(v) = colors.get("src_default").and_then(|v| v.as_u64()) { tc.src_default = v as u8; }
            if let Some(v) = colors.get("src_default_icon").and_then(|v| v.as_u64()) { tc.src_default_icon = v as u8; }
            if let Some(v) = colors.get("content_fg").and_then(|v| v.as_u64()) { tc.content_fg = v as u8; }
            if let Some(v) = colors.get("content_bg").and_then(|v| v.as_u64()) { tc.content_bg = v as u8; }
            if let Some(v) = colors.get("list_fg").and_then(|v| v.as_u64()) { tc.list_fg = v as u8; }
            if let Some(v) = colors.get("list_bg").and_then(|v| v.as_u64()) { tc.list_bg = v as u8; }
            if let Some(v) = colors.get("border_fg").and_then(|v| v.as_u64()) { tc.border_fg = v as u8; }
        }

        // Per-platform style overrides. Shape in config.yml:
        //   source_styles:
        //     <platform-slug>:
        //       color: <0-255>
        //       glyph: "<single glyph>"   # optional; empty → default bullet
        if let Some(styles) = yaml.get("source_styles").and_then(|v| v.as_mapping()) {
            for (k, v) in styles {
                let Some(name) = k.as_str() else { continue };
                let color = v.get("color").and_then(|c| c.as_u64()).unwrap_or(0) as u8;
                let glyph = v.get("glyph").and_then(|g| g.as_str()).unwrap_or("").to_string();
                self.theme_colors.source_styles.insert(name.to_string(), (color, glyph));
            }
        }

        // Native delivery routes for phone-gateway replies. Shape:
        //   gateway_routes:
        //     "discord:Some Server #channel": "channel:123456789012345678"
        //     "discord:Other #room":          "webhook:myhook"
        if let Some(routes) = yaml.get("gateway_routes").and_then(|v| v.as_mapping()) {
            for (k, v) in routes {
                if let (Some(key), Some(val)) = (k.as_str(), v.as_str()) {
                    self.gateway_routes.insert(key.to_string(), val.to_string());
                }
            }
        }
    }

    /// Load settings from ~/.kastrup/kastruprc (Ruby-style config, pattern-matched)
    fn load_rc(&mut self) {
        let path = rc_path();
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return,
        };

        // Pre-process: join continuation lines (lines after identity/folder_hook that start with whitespace)
        let mut joined_lines: Vec<String> = Vec::new();
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                joined_lines.push(line.to_string());
                continue;
            }
            // Continuation: starts with whitespace and previous line ends with comma
            if (line.starts_with(' ') || line.starts_with('\t'))
                && !joined_lines.is_empty()
                && joined_lines.last().map(|l| l.trim_end().ends_with(',')).unwrap_or(false)
            {
                if let Some(last) = joined_lines.last_mut() {
                    last.push(' ');
                    last.push_str(trimmed);
                }
            } else {
                joined_lines.push(line.to_string());
            }
        }

        // Parse identities and folder_hooks from joined lines
        let home = std::env::var("HOME").unwrap_or_default();
        for jline in &joined_lines {
            let t = jline.trim();
            if t.starts_with("identity ") {
                // identity 'name', from: 'X', signature: 'Y', smtp: 'Z'
                let ident_re = regex::Regex::new(
                    r#"identity\s+['"](\w+)['"]\s*,\s*(.+)"#
                ).unwrap();
                if let Some(caps) = ident_re.captures(t) {
                    let iname = caps.get(1).unwrap().as_str().to_string();
                    let rest = caps.get(2).unwrap().as_str();
                    let from_re = regex::Regex::new(r#"from:\s*['"]([^'"]+)['"]"#).unwrap();
                    let sig_re = regex::Regex::new(r#"signature:\s*['"]([^'"]+)['"]"#).unwrap();
                    let smtp_re = regex::Regex::new(r#"smtp:\s*['"]([^'"]+)['"]"#).unwrap();

                    let from_val = from_re.captures(rest).map(|c| c.get(1).unwrap().as_str().to_string()).unwrap_or_default();
                    let sig_path = sig_re.captures(rest).map(|c| {
                        c.get(1).unwrap().as_str().replace("~/", &format!("{}/", home))
                    }).unwrap_or_default();
                    let smtp_val = smtp_re.captures(rest).map(|c| {
                        c.get(1).unwrap().as_str().replace("~/", &format!("{}/", home))
                    });

                    // Parse "Name <email>" from the from field
                    let (pname, pemail) = if let Some(lt) = from_val.find('<') {
                        (from_val[..lt].trim().to_string(), from_val[lt+1..].trim_end_matches('>').to_string())
                    } else {
                        (String::new(), from_val.clone())
                    };

                    self.identities.insert(iname, Identity {
                        name: pname,
                        email: pemail,
                        signature_path: sig_path,
                        smtp: smtp_val,
                    });
                }
            }
            if t.starts_with("folder_hook") {
                // folder_hook /pattern/i, 'identity_name'
                let fh_re = regex::Regex::new(
                    r#"folder_hook\s+/([^/]+)/\w*\s*,\s*['"](\w+)['"]"#
                ).unwrap();
                if let Some(caps) = fh_re.captures(t) {
                    let pattern = caps.get(1).unwrap().as_str().to_string();
                    let ident_name = caps.get(2).unwrap().as_str().to_string();
                    self.folder_hooks.push((pattern, ident_name));
                }
            }
        }

        let set_re = regex::Regex::new(r#"^\s*set\s+:(\w+)\s*,\s*(.+)$"#).unwrap();

        for line in joined_lines.iter() {
            let trimmed = line.trim();
            // Skip comments and empty lines
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            if let Some(caps) = set_re.captures(trimmed) {
                let key = caps.get(1).unwrap().as_str();
                let raw_val = strip_ruby_comment(caps.get(2).unwrap().as_str().trim());
                let val = strip_ruby_quotes(raw_val);

                match key {
                    "pane_width" => {
                        if let Ok(n) = val.parse::<u16>() {
                            self.pane_width = n.min(6).max(1);
                        }
                    }
                    "border_style" => {
                        if let Ok(n) = val.parse::<u8>() {
                            self.border_style = n.min(3);
                        }
                    }
                    "sort_order" => { self.sort_order = val.to_string(); }
                    "date_format" => { self.date_format = val.to_string(); }
                    "color_theme" => { self.color_theme = val.to_string(); }
                    "confirm_purge" => { self.confirm_purge = val == "true"; }
                    "message_log" => {
                        if let Ok(n) = val.parse::<usize>() { self.message_log = n; }
                    }
                    "read_sync_dir" => { self.read_sync_dir = val.to_string(); }
                    "read_sync_days" => {
                        if let Ok(n) = val.parse::<i64>() { self.read_sync_days = n; }
                    }
                    "load_limit" => {
                        if let Ok(n) = val.parse::<usize>() {
                            self.load_limit = n;
                        }
                    }
                    "default_view" => { self.default_view = val.to_string(); }
                    "default_email" => { self.default_email = val.to_string(); }
                    "smtp_command" => { self.smtp_command = val.to_string(); }
                    "download_folder" => { self.download_folder = val.to_string(); }
                    _ => {}
                }
            }

            // Parse theme color overrides: theme_color :key, value
            if trimmed.starts_with("theme_color") {
                let theme_re = regex::Regex::new(
                    r#"theme_color\s+:(\w+)\s*,\s*(\d+)"#
                ).unwrap();
                if let Some(caps) = theme_re.captures(trimmed) {
                    let ckey = caps.get(1).unwrap().as_str();
                    let cval: u16 = caps.get(2).unwrap().as_str().parse().unwrap_or(0);
                    match ckey {
                        "unread" => self.theme_colors.unread = cval as u8,
                        "read" => self.theme_colors.read = cval as u8,
                        "accent" => self.theme_colors.accent = cval as u8,
                        "thread" => self.theme_colors.thread = cval as u8,
                        "dm" => self.theme_colors.dm = cval as u8,
                        "tag" => self.theme_colors.tag = cval as u8,
                        "star" => self.theme_colors.star = cval as u8,
                        "quote1" => self.theme_colors.quote1 = cval as u8,
                        "quote2" => self.theme_colors.quote2 = cval as u8,
                        "quote3" => self.theme_colors.quote3 = cval as u8,
                        "quote4" => self.theme_colors.quote4 = cval as u8,
                        "sig" => self.theme_colors.sig = cval as u8,
                        "link" => self.theme_colors.link = cval as u8,
                        "fold_bg" => self.theme_colors.fold_bg = cval,
                        "top_bg" => self.theme_colors.top_bg = cval,
                        "bottom_bg" => self.theme_colors.bottom_bg = cval,
                        "cmd_bg" => self.theme_colors.cmd_bg = cval,
                        "header_from" => self.theme_colors.header_from = cval as u8,
                        "header_subj" => self.theme_colors.header_subj = cval as u8,
                        "header_date" => self.theme_colors.header_date = cval as u8,
                        "header_label" => self.theme_colors.header_label = cval as u8,
                        "separator" => self.theme_colors.separator = cval as u8,
                        "attachment" => self.theme_colors.attachment = cval as u8,
                        "html_hint" => self.theme_colors.html_hint = cval as u8,
                        "replied" => self.theme_colors.replied = cval as u8,
                        "delete_mark" => self.theme_colors.delete_mark = cval as u8,
                        "attach_ind" => self.theme_colors.attach_ind = cval as u8,
                        "date_fg" => self.theme_colors.date_fg = cval as u8,
                        "view_all" => self.theme_colors.view_all = cval as u8,
                        "view_new" => self.theme_colors.view_new = cval as u8,
                        "view_sources" => self.theme_colors.view_sources = cval as u8,
                        "view_custom" => self.theme_colors.view_custom = cval as u8,
                        "view_starred" => self.theme_colors.view_starred = cval as u8,
                        "info_fg" => self.theme_colors.info_fg = cval as u8,
                        "hint_fg" => self.theme_colors.hint_fg = cval as u8,
                        "prefix_fg" => self.theme_colors.prefix_fg = cval as u8,
                        "no_msg" => self.theme_colors.no_msg = cval as u8,
                        "feedback_warn" => self.theme_colors.feedback_warn = cval as u8,
                        "feedback_ok" => self.theme_colors.feedback_ok = cval as u8,
                        "feedback_info" => self.theme_colors.feedback_info = cval as u8,
                        "content_fg" => self.theme_colors.content_fg = cval as u8,
                        "content_bg" => self.theme_colors.content_bg = cval as u8,
                        "list_fg" => self.theme_colors.list_fg = cval as u8,
                        "list_bg" => self.theme_colors.list_bg = cval as u8,
                        "border_fg" => self.theme_colors.border_fg = cval as u8,
                        _ => {}
                    }
                }
            }

            // Parse identity blocks (multi-line: identity 'name', from: ..., signature: ..., smtp: ...)
            // Collected as single joined line from continuation lines
            // Parse folder_hook /pattern/i, 'identity_name'
            // (These are handled in a second pass below)

            // Parse bind :key, "command"
            if trimmed.starts_with("bind") {
                let bind_re = regex::Regex::new(
                    r#"bind\s+[:'"](\S+?)['"]?\s*,\s*['"](.*?)['"]"#
                ).unwrap();
                if let Some(caps) = bind_re.captures(trimmed) {
                    let bkey = caps.get(1).unwrap().as_str().to_string();
                    let bval = caps.get(2).unwrap().as_str().to_string();
                    self.custom_bindings.insert(bkey, bval);
                }
            }

            // Parse channel_name "pattern", "display_name"
            if trimmed.starts_with("channel_name") {
                let cn_re = regex::Regex::new(
                    r#"channel_name\s+['"](.*?)['"]\s*,\s*['"](.*?)['"]"#
                ).unwrap();
                if let Some(caps) = cn_re.captures(trimmed) {
                    let pattern = caps.get(1).unwrap().as_str().to_string();
                    let name = caps.get(2).unwrap().as_str().to_string();
                    self.channel_names.insert(pattern, name);
                }
            }
        }
    }
}

impl Config {
    /// Get identity for a folder name (first matching folder_hook wins, fallback to 'default')
    pub fn identity_for_folder(&self, folder: Option<&str>) -> Option<&Identity> {
        let folder = folder.unwrap_or("INBOX");
        for (pattern, ident_name) in &self.folder_hooks {
            // Case-insensitive prefix/contains match (simplified from Ruby regex)
            if let Ok(re) = regex::RegexBuilder::new(pattern).case_insensitive(true).build() {
                if re.is_match(folder) {
                    return self.identities.get(ident_name);
                }
            }
        }
        self.identities.get("default")
    }
}

/// Strip surrounding Ruby quotes from a value string
/// Strip Ruby inline comments: `'value' # comment` -> `'value'`
/// Respects quotes (won't strip # inside strings).
fn strip_ruby_comment(s: &str) -> &str {
    let mut in_quote = false;
    let mut quote_char = ' ';
    for (i, ch) in s.char_indices() {
        if in_quote {
            if ch == quote_char { in_quote = false; }
        } else if ch == '\'' || ch == '"' {
            in_quote = true;
            quote_char = ch;
        } else if ch == '#' {
            return s[..i].trim();
        }
    }
    s
}

fn strip_ruby_quotes(s: &str) -> &str {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\''))
        || (s.starts_with('"') && s.ends_with('"'))
    {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

/// Path to ~/.kastrup (config home, created if missing)
fn kastrup_home() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    let dir = PathBuf::from(home).join(".kastrup");
    if !dir.exists() { let _ = std::fs::create_dir_all(&dir); }
    dir
}

/// Path to ~/.kastrup/config.yml
fn config_yml_path() -> PathBuf {
    kastrup_home().join("config.yml")
}

/// Path to ~/.kastrup/kastruprc
fn rc_path() -> PathBuf {
    kastrup_home().join("kastruprc")
}
