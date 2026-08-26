mod chat_send;
mod completion_ipc;
mod config;
mod database;
mod email_send;
mod log;
mod mailfile;
mod message;
mod organizer;
mod poller;
mod read_sync;
mod source;
mod sources;
mod triage;

use crust::{Crust, Pane, Input};
use crust::style;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;

/// Widening a search to whole threads costs one `subject LIKE` per thread.
/// Past this many, the query stops being worth it and the hits stand alone.
const MAX_SEARCH_THREADS: usize = 40;

/// Background DB write operations (fire-and-forget from main thread).
/// Some variants (ToggleStar, UpdateFolder/Labels/Metadata,
/// MarkAllReadBulk) are the async-writer surface for ops the current
/// callers perform via direct single-row db calls; kept as the canonical
/// write API even though those callers don't route through here today.
#[allow(dead_code)]
enum DbWriteOp {
    MarkRead(i64),
    MarkUnread(i64),
    ToggleStar(i64),
    DeleteMessages(Vec<i64>),
    UpdateFolder(i64, String, serde_json::Value),
    UpdateLabels(i64, String),
    UpdateMetadata(i64, String),
    SyncMaildirFlag(serde_json::Value, i64),
    /// Mark-all-read end-to-end: collect unread maildir rows (scoped
    /// to maildir source_ids + `read = 0`, so the scan is tiny even
    /// on a 250k-message DB), flip read=1, then rename files +
    /// bulk-update metadata. The whole thing runs on the writer
    /// thread so the main loop never holds the conn mutex.
    MarkAllReadBulk {
        filters: Option<database::Filters>,
        maildir_source_ids: Vec<i64>,
    },
    /// Mark-read scoped to an explicit list of message ids — used by
    /// the `A` keypress to flip only what's actually visible in the
    /// current `filtered_messages` view (sticky search, conversation
    /// grouping, ad-hoc tag picks etc. don't always reduce to a
    /// `Filters` predicate). Same maildir-rename + metadata bulk-update
    /// path as `MarkAllReadBulk`, just scoped by `id IN (...)`.
    MarkReadByIds(Vec<i64>),
    SetSetting(String, String),
    Execute(String, Vec<String>), // raw SQL with string params
}

/// Name for the slow-write log line. Borrowed match, no allocation —
/// the writer takes it once per op, and only user actions produce ops.
fn write_op_label(op: &DbWriteOp) -> &'static str {
    match op {
        DbWriteOp::MarkRead(_) => "MarkRead",
        DbWriteOp::MarkUnread(_) => "MarkUnread",
        DbWriteOp::ToggleStar(_) => "ToggleStar",
        DbWriteOp::DeleteMessages(_) => "DeleteMessages",
        DbWriteOp::UpdateFolder(..) => "UpdateFolder",
        DbWriteOp::UpdateLabels(..) => "UpdateLabels",
        DbWriteOp::UpdateMetadata(..) => "UpdateMetadata",
        DbWriteOp::SyncMaildirFlag(..) => "SyncMaildirFlag",
        DbWriteOp::MarkAllReadBulk { .. } => "MarkAllReadBulk",
        DbWriteOp::MarkReadByIds(_) => "MarkReadByIds",
        DbWriteOp::SetSetting(..) => "SetSetting",
        DbWriteOp::Execute(..) => "Execute",
    }
}

use config::{Config, Identity};
use database::{Database, Filters};
use message::Message;
// Email plumbing, shared with the nomad phone app so the two cannot
// drift. Imported unqualified: every call site here predates the crate
// and reads the same either way.
use mail::html::html_to_text;
use mail::mime::{
    body_after_headers, decode_quoted_printable, latin1_to_utf8, looks_base64,
    looks_quoted_printable, normalize_line_endings,
};

/// One in-flight SMTP send. The shell child runs on a dedicated
/// thread and sends `(success, stderr)` back through `result_rx` when
/// it exits; main thread picks the result up in `pump_pending_send`
/// and finishes the transaction (sent-folder copy, tempfile cleanup,
/// reply / forward flag updates, feedback toast).
struct PendingSend {
    result_rx: std::sync::mpsc::Receiver<SendOutcome>,
    /// "To: …" display string for the toast.
    to_display: String,
    /// `/tmp/kastrup_send_<pid>.eml` — kept on failure for debugging,
    /// removed on success.
    tmpfile: String,
    /// Full RFC822 message, copied into the local Sent maildir on
    /// success.
    rfc_msg: String,
    /// Forward / reply book-keeping deferred until after the wire
    /// send actually succeeds. `forward_ids` populates `mark_forwarded`,
    /// `reply_id` triggers `mark_replied`.
    forward_ids: Vec<i64>,
    reply_id: Option<i64>,
    /// `Some(n)` if the send carries that many attachments; used in
    /// the success toast for the attach path. `None` keeps the plain
    /// "Sent to X" wording.
    attachment_count: Option<usize>,
    /// The compose-format draft (From/To/Cc/Bcc/Reply-To/Subject + body),
    /// NOT the assembled RFC. On send failure this is re-filed into the
    /// `postponed` table so the draft survives (VPN down, SMTP
    /// unreachable) and reappears in the `m` recall picker. Especially
    /// important for a recalled draft, whose durable copy was already
    /// consumed on load.
    compose_draft: String,
}

/// What the SMTP worker thread reports back. `Ok(())` means the
/// child exited 0; `Err(msg)` carries the first stderr line (or a
/// synthetic "exit N" string when stderr was empty) so the toast can
/// show something actionable.
type SendOutcome = Result<(), String>;

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

// --- Draft drop / recall ---

enum DraftSource {
    Postponed(i64),
    File(std::path::PathBuf),
    /// A row in `scheduled`: a draft with a time on it. Loading one in
    /// the picker cancels the schedule and hands the text back to the
    /// editor, same as recalling a postponed draft.
    Scheduled(i64),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DraftKind {
    Email,
    Slack,
    Discord,
    /// Any channel/room reachable through the weechat relay: IRC
    /// channels, Slack-via-weechat, Matrix rooms, Discord-bridge
    /// mirrors, etc. `Channel:` header carries the buffer's
    /// `full_name` (e.g. `python.slack.<workspace>.#general`).
    Weechat,
    /// Phone `relay` gateway: Instagram / Messenger / WhatsApp /
    /// Telegram / Signal / SMS. `Channel:` header carries
    /// `<platform>:<thread_key>`; the reply is queued to the gateway
    /// `outbox/` for the phone to fire.
    Gateway,
    /// A conversation reachable through the external `ws-bridge` CLI.
    /// `Conv:` header carries the conversation UUID (the send target);
    /// optional `Channel:` is a display label only; `Attach:` lines
    /// upload files with the body as the caption.
    Workspace,
}

impl DraftKind {
    fn tag(&self) -> &'static str {
        match self {
            DraftKind::Email   => "email",
            DraftKind::Slack   => "slack",
            DraftKind::Discord => "discord",
            DraftKind::Weechat => "weechat",
            DraftKind::Gateway => "gateway",
            DraftKind::Workspace => "workspace",
        }
    }

    /// Header key whose value becomes the picker's "subject" label.
    fn label_header(&self) -> &'static str {
        match self {
            DraftKind::Email   => "Subject",
            DraftKind::Slack   => "Channel",
            DraftKind::Discord => "Channel",
            DraftKind::Weechat => "Channel",
            DraftKind::Gateway => "Channel",
            DraftKind::Workspace => "Channel",
        }
    }

    /// Resolve from a stored `tag()` string.
    fn from_tag(t: &str) -> Self {
        match t {
            "slack"     => DraftKind::Slack,
            "discord"   => DraftKind::Discord,
            "weechat"   => DraftKind::Weechat,
            "gateway"   => DraftKind::Gateway,
            "workspace" => DraftKind::Workspace,
            _           => DraftKind::Email,
        }
    }

    /// Resolve from file extension. Unknown extensions → Email
    /// (back-compat with existing .eml files and old drops).
    fn from_path(p: &std::path::Path) -> Self {
        match p.extension().and_then(|e| e.to_str()) {
            Some("slack")   => DraftKind::Slack,
            Some("discord") => DraftKind::Discord,
            Some("weechat") => DraftKind::Weechat,
            Some("gateway") => DraftKind::Gateway,
            Some("workspace") => DraftKind::Workspace,
            _               => DraftKind::Email,
        }
    }
}

/// What the draft picker came back with. `New` and `Quit` used to be the
/// same `None`, so ESC and q both dropped the user into a fresh compose.
enum DraftPick {
    Load(usize),
    New,
    Quit,
}

struct DraftCandidate {
    source: DraftSource,
    kind: DraftKind,
    subject: String,
    body_preview: String,
    data: String,
    created_at: i64,
}

fn drafts_drop_dir() -> std::path::PathBuf {
    let home = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_default();
    home.join(".kastrup").join("drafts")
}

/// Map a candidate index to its picker key: 0-9, then a-z.
fn pick_key_for(i: usize) -> char {
    if i < 10 {
        (b'0' + i as u8) as char
    } else {
        (b'a' + (i - 10) as u8) as char
    }
}

/// Pull a subject + first-body-line preview from a draft's raw editor data.
/// For email, "subject" is the `Subject:` header; for slack, the
/// `Channel:` header value. Tolerates mixed-case header keys.
fn parse_draft_preview(data: &str, kind: DraftKind) -> (String, String) {
    let header_key = kind.label_header();
    let mut subject = String::new();
    let mut body_start = data.len();
    let mut idx = 0usize;
    for line in data.split_inclusive('\n') {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            body_start = idx + line.len();
            break;
        }
        if subject.is_empty() {
            let lower = trimmed.to_ascii_lowercase();
            let needle = format!("{}:", header_key.to_ascii_lowercase());
            if let Some(rest) = lower.strip_prefix(&needle) {
                // pull the original slice (case-preserved) at the same offset
                let val_start = trimmed.len() - rest.len();
                subject = trimmed[val_start..].trim().to_string();
            }
        }
        idx += line.len();
    }
    let mut body_preview = String::new();
    for raw in data[body_start..].lines() {
        let t = raw.trim();
        if !t.is_empty() {
            body_preview = t.to_string();
            break;
        }
    }
    if subject.is_empty() {
        subject = match kind {
            DraftKind::Email   => "(no subject)".to_string(),
            DraftKind::Slack   => "(no channel)".to_string(),
            DraftKind::Discord => "(no channel)".to_string(),
            DraftKind::Weechat => "(no channel)".to_string(),
            DraftKind::Gateway => "(no chat target)".to_string(),
            DraftKind::Workspace => "(no channel)".to_string(),
        };
    }
    (subject, body_preview)
}

/// Extract the `Channel:` header value from a chat (slack/discord)
/// draft. Returns None if missing — send paths use this to validate.
fn parse_chat_channel(data: &str) -> Option<String> {
    for line in data.lines() {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() { break; }
        let lower = trimmed.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("channel:") {
            let val_start = trimmed.len() - rest.len();
            let v = trimmed[val_start..].trim();
            if !v.is_empty() { return Some(v.to_string()); }
        }
    }
    None
}

/// Extract the `Conv:` header value from a workspace draft (the
/// conversation UUID, the send target). Same header-block scan as
/// `parse_chat_channel`. Returns None if missing.
fn parse_chat_conv(data: &str) -> Option<String> {
    for line in data.lines() {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() { break; }
        let lower = trimmed.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("conv:") {
            let val_start = trimmed.len() - rest.len();
            let v = trimmed[val_start..].trim();
            if !v.is_empty() { return Some(v.to_string()); }
        }
    }
    None
}

/// Build a (message_index, depth) ordering for an email thread
/// section. Walks the In-Reply-To tree depth-first so a reply appears
/// directly under its parent at one extra indent level. Falls back to
/// chronological order for messages whose parent isn't in this section
/// (orphans become extra roots at depth 0).
///
/// Lookup chain:
/// * `msg.thread_id` carries the RFC822 Message-Id (maildir source
///   convention; set on insert).
/// * `msg.metadata.in_reply_to` carries the parent's Message-Id when
///   the mail has the header.
fn build_thread_order(messages: &[Message], section_indices: &[usize]) -> Vec<(usize, u8)> {
    use std::collections::HashMap;
    // message_id → position in section_indices (so we can resolve a
    // parent's index quickly).
    let mut by_id: HashMap<String, usize> = HashMap::new();
    for &i in section_indices {
        if let Some(ref mid) = messages[i].thread_id {
            if !mid.is_empty() { by_id.insert(mid.clone(), i); }
        }
    }
    // Children per parent index; roots are messages whose
    // `in_reply_to` doesn't resolve to anything in this section.
    let mut children: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut roots: Vec<usize> = Vec::new();
    for &i in section_indices {
        let parent_mid = messages[i].metadata
            .get("in_reply_to")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().trim_matches(['<', '>']).to_string());
        match parent_mid.and_then(|pid| by_id.get(&pid).copied()) {
            Some(parent_idx) if parent_idx != i => {
                children.entry(parent_idx).or_default().push(i);
            }
            _ => roots.push(i),
        }
    }
    // Sort children chronologically (oldest reply first under the
    // parent — natural reading order).
    for kids in children.values_mut() {
        kids.sort_by(|&a, &b| messages[a].timestamp.cmp(&messages[b].timestamp));
    }
    // Sort roots newest-first to match the existing section ordering.
    roots.sort_by(|&a, &b| messages[b].timestamp.cmp(&messages[a].timestamp));

    // Iterative DFS to avoid recursion-depth panics on degenerate
    // chains (mailing-list threads can be hundreds of replies long).
    let mut out: Vec<(usize, u8)> = Vec::with_capacity(section_indices.len());
    let mut stack: Vec<(usize, u8)> = roots.into_iter().map(|i| (i, 0u8)).collect();
    stack.reverse(); // pop from end → process roots in declared order
    while let Some((idx, depth)) = stack.pop() {
        out.push((idx, depth));
        if let Some(kids) = children.get(&idx) {
            let next_depth = depth.saturating_add(1);
            for &kid in kids.iter().rev() {
                stack.push((kid, next_depth));
            }
        }
    }
    out
}

/// Decide whether a subscribed weechat buffer (`buf`, a full_name like
/// `irc.libera.#vim` or `python.slack.team.#chan`) should be merged into a
/// folders-mode view as an empty section. Unlike `folder_matches_filter`
/// this is SOURCE-aware: a branch admits a weechat buffer only when its
/// source dimension permits weechat-relay (source_id maps to a
/// weechat-relay source, source_type is "weechat-relay", or the branch
/// carries no source constraint) AND its folder dimension (if any) matches.
/// Without the source check, a branch like the Workspace `source_id=7` with
/// no folder filter would admit every IRC/Slack buffer into the view.
fn buffer_admitted_by_filter(
    buf: &str,
    f: &Filters,
    source_type_map: &std::collections::HashMap<i64, String>,
) -> bool {
    if let Some(branches) = &f.branches {
        return branches.iter().any(|b| buffer_admitted_by_filter(buf, b, source_type_map));
    }
    let source_ok = match (f.source_id, f.source_type.as_deref()) {
        (Some(sid), _) => source_type_map.get(&sid).map(|t| t == "weechat-relay").unwrap_or(false),
        (None, Some(st)) => st == "weechat-relay",
        (None, None) => true,
    };
    if !source_ok { return false; }
    if let Some(ref fold) = f.folder { return buf == fold; }
    if let Some(ref pat) = f.folder_pattern {
        return pat.split('|').map(|s| s.trim()).filter(|s| !s.is_empty()).any(|s| buf.contains(s));
    }
    true
}

/// True when a folders-mode section belongs to branch `f`. The section is
/// identified by its folder `name` and the `source_id` of its newest message
/// (`None` for an empty subscribed buffer). A branch admits the section when
/// its source dimension matches (source_id equal, source_type matching via
/// the source map, or no source constraint) AND its folder dimension (if any)
/// matches the name. Used to group sections by branch order so a multi-source
/// view stays grouped instead of interleaving by recency.
fn section_in_branch(
    name: &str,
    source_id: Option<i64>,
    f: &Filters,
    source_type_map: &std::collections::HashMap<i64, String>,
) -> bool {
    if let Some(branches) = &f.branches {
        return branches.iter().any(|b| section_in_branch(name, source_id, b, source_type_map));
    }
    let sid_ok = match f.source_id { Some(sid) => source_id == Some(sid), None => true };
    let stype_ok = match &f.source_type {
        Some(st) => source_id.and_then(|i| source_type_map.get(&i)).map(|t| t == st).unwrap_or(false),
        None => true,
    };
    if !(sid_ok && stype_ok) { return false; }
    if let Some(ref fold) = f.folder { return name == fold; }
    if let Some(ref pat) = f.folder_pattern {
        return pat.split('|').map(|s| s.trim()).filter(|s| !s.is_empty()).any(|s| name.contains(s));
    }
    true
}

/// Apply a JSON `rules` array onto a Filters. Each rule is
/// `{field, op, value}`. Unknown fields are silently ignored.
fn apply_view_rules(rules: &[serde_json::Value], filters: &mut Filters) {
    for rule in rules {
        let field = rule["field"].as_str().unwrap_or("");
        let op    = rule["op"].as_str().unwrap_or("=");
        let value = &rule["value"];
        match field {
            "read" => { filters.is_read = Some(!value.as_bool().unwrap_or(true)); }
            "starred" => { filters.is_starred = value.as_bool(); }
            "folder" => {
                if op == "like" {
                    filters.folder_pattern = value.as_str().map(|s| s.to_string());
                } else {
                    filters.folder = value.as_str().map(|s| s.to_string());
                }
            }
            "source_id" => { filters.source_id = value.as_i64(); }
            "sender" => { filters.sender_pattern = value.as_str().map(|s| s.to_string()); }
            "source_type" => { filters.source_type = value.as_str().map(|s| s.to_string()); }
            "platform" => { filters.platform = value.as_str().map(|s| s.to_string()); }
            _ => {}
        }
    }
}

/// Parse a view's `filters` JSON into a Filters value. Honors both:
///
/// * The legacy single-AND-group shape `{ "rules": [...] }`.
/// * The OR-of-AND-groups shape `{ "branches": [ {"rules": [...]},
///   {"rules": [...]}, ... ] }`. Each branch is an independent
///   Filters; results are unioned. Lets a view express a true cross-
///   source / cross-folder query — e.g. "mail in Customers.X" UNION
///   "Slack messages in #foo" UNION "anything from sender ~ ACME".
///
/// When both `branches` and `rules` are present, `branches` wins.
fn parse_view_filters_json(f: &serde_json::Value) -> Filters {
    let mut filters = Filters::default();
    if let Some(branches) = f["branches"].as_array() {
        let mut bs: Vec<Filters> = Vec::new();
        for b in branches {
            let mut sub = Filters::default();
            if let Some(rules) = b["rules"].as_array() {
                apply_view_rules(rules, &mut sub);
            }
            bs.push(sub);
        }
        if !bs.is_empty() {
            filters.branches = Some(bs);
        }
    } else if let Some(rules) = f["rules"].as_array() {
        apply_view_rules(rules, &mut filters);
    }
    filters
}

/// Pull every Slack file CDN URL out of a message body. Wee-slack
/// formats attachments as bare `https://files.slack.com/files-pri/...`
/// URLs or wrapped `<URL|displayed-name>` mrkdwn links. Both shapes
/// are normalised to the plain URL.
fn extract_slack_file_urls(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for cap in body.split_whitespace() {
        let trimmed = cap.trim_matches(|c: char| !c.is_ascii_graphic());
        // Strip mrkdwn brackets if any: `<url|name>` → `url`.
        let url = if let Some(rest) = trimmed.strip_prefix('<') {
            let rest = rest.trim_end_matches('>');
            rest.split_once('|').map(|(u, _)| u).unwrap_or(rest)
        } else {
            trimmed.trim_end_matches(['.', ',', ';', ':', '!', '?', ')', ']', '"', '\''])
        };
        if url.starts_with("https://files.slack.com/files-pri/")
            || url.starts_with("https://files.slack.com/files-tmb/")
        {
            if seen.insert(url.to_string()) {
                out.push(url.to_string());
            }
        }
    }
    out
}

/// Pull `(file_id, filename)` out of a Slack CDN URL like
/// `https://files.slack.com/files-pri/T0XXX-FYYY/foo.png`.
/// The file_id is the last path segment before the filename.
fn parse_slack_file_url(url: &str) -> Option<(String, String)> {
    let path = url.split("files.slack.com/").nth(1)?;
    // Strip query string if any.
    let path = path.split('?').next().unwrap_or(path);
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.len() < 3 { return None; }
    let filename = segments.last().copied()?.to_string();
    let id_seg = segments[segments.len() - 2];
    // id_seg looks like `T0XYZ-FABC123` — extract the F-id half if present.
    let file_id = id_seg.rsplit_once('-')
        .map(|(_, f)| f.to_string())
        .unwrap_or_else(|| id_seg.to_string());
    Some((file_id, filename))
}

/// Pull every http(s) URL out of a message body for the "open in browser"
/// fallback (chat / plain-text messages that carry no HTML or link
/// metadata). Handles bare URLs and Slack/weechat mrkdwn `<url|label>`
/// wraps, strips surrounding brackets and trailing punctuation, and dedups
/// preserving first-seen order. Reads the FULL url, not the shortened
/// display label, so the opened link is the real target.
fn extract_message_urls(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for cap in body.split_whitespace() {
        let t = cap.trim_matches(|c: char| !c.is_ascii_graphic());
        // Unwrap mrkdwn `<url|name>` / `<url>` to the bare url.
        let t = if let Some(rest) = t.strip_prefix('<') {
            let rest = rest.trim_end_matches('>');
            rest.split_once('|').map(|(u, _)| u).unwrap_or(rest)
        } else { t };
        let url = t.trim_start_matches(['(', '[', '{', '"', '\'', '<'])
            .trim_end_matches(['.', ',', ';', ':', '!', '?', ')', ']', '}', '"', '\'', '>']);
        if (url.starts_with("https://") || url.starts_with("http://")) && url.len() > 9
            && seen.insert(url.to_string())
        {
            out.push(url.to_string());
        }
    }
    out
}

/// Extract `Attach:` headers (one per line, before the blank-line
/// separator) from a chat draft. Values are file paths with `~/`
/// and `$HOME/` expansion. Used by `.slack` and `.weechat` drafts
/// to attach files alongside the message body.
fn parse_chat_attachments(data: &str) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let home = std::env::var("HOME").unwrap_or_default();
    for line in data.lines() {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() { break; }
        let lower = trimmed.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("attach:") {
            let val_start = trimmed.len() - rest.len();
            let v = trimmed[val_start..].trim();
            if v.is_empty() { continue; }
            let expanded = if let Some(r) = v.strip_prefix("~/") { format!("{}/{}", home, r) }
                else if let Some(r) = v.strip_prefix("$HOME/") { format!("{}/{}", home, r) }
                else { v.to_string() };
            out.push(std::path::PathBuf::from(expanded));
        }
    }
    out
}

/// Extract `Attach:` header lines from an EMAIL draft: returns the
/// draft without them plus the attachment paths. Lets a dropped
/// `.eml` draft (e.g. queued by a Claude session) carry attachments;
/// the paths feed the review screen's attachment list and the lines
/// never reach the wire. Chat drafts keep their `Attach:` lines —
/// their send functions parse them natively.
fn take_email_attach_headers(data: &str) -> (String, Vec<String>) {
    let atts: Vec<String> = parse_chat_attachments(data).into_iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    if atts.is_empty() { return (data.to_string(), atts); }
    let mut out = String::with_capacity(data.len());
    let mut in_body = false;
    for line in data.lines() {
        if !in_body {
            if line.trim().is_empty() { in_body = true; }
            else if line.to_ascii_lowercase().starts_with("attach:") { continue; }
        }
        out.push_str(line);
        out.push('\n');
    }
    (out, atts)
}

/// Extract the kastrup link pseudo-headers from a drop-file email
/// draft: `X-Kastrup-Reply-To: <id>` and `X-Kastrup-Forward-Of:
/// <id>[, <id>…]` (kastrup message ids). Returns the draft without
/// them plus the parsed ids; they feed pending_reply_id /
/// pending_forward_ids so the ←/→ arrows appear when the send
/// succeeds. Header block only, case-insensitive.
fn take_kastrup_link_headers(data: &str) -> (String, Option<i64>, Vec<i64>) {
    let mut reply: Option<i64> = None;
    let mut fwd: Vec<i64> = Vec::new();
    let mut out = String::with_capacity(data.len());
    let mut in_body = false;
    for line in data.lines() {
        if !in_body {
            if line.trim().is_empty() { in_body = true; }
            else {
                let lower = line.to_ascii_lowercase();
                if let Some(v) = lower.strip_prefix("x-kastrup-reply-to:") {
                    reply = v.trim().parse::<i64>().ok().or(reply);
                    continue;
                }
                if let Some(v) = lower.strip_prefix("x-kastrup-forward-of:") {
                    fwd.extend(v.split(',').filter_map(|s| s.trim().parse::<i64>().ok()));
                    continue;
                }
            }
        }
        out.push_str(line);
        out.push('\n');
    }
    (out, reply, fwd)
}

/// `/me <action>` → Some(action) with the prefix stripped; else None.
/// Single-line body only (multi-line messages with a `/me` first line
/// are treated as regular messages — Slack's chat.meMessage doesn't
/// accept newlines in the action text). Trailing whitespace stripped.
fn strip_me_prefix(body: &str) -> Option<&str> {
    let trimmed = body.trim_end();
    if trimmed.contains('\n') { return None; }
    let action = trimmed.strip_prefix("/me ")?.trim_start();
    if action.is_empty() { None } else { Some(action) }
}

/// Body of a chat draft: everything after the first blank line.
fn parse_chat_body(data: &str) -> String {
    if let Some(pos) = data.find("\n\n") {
        data[pos + 2..].trim_end_matches(['\r', '\n']).to_string()
    } else if let Some(pos) = data.find("\r\n\r\n") {
        data[pos + 4..].trim_end_matches(['\r', '\n']).to_string()
    } else {
        String::new()
    }
}

// --- Folder browser types ---

struct FolderEntry {
    name: String,
    full_name: String,
    depth: usize,
    has_children: bool,
    collapsed: bool,
}

/// Collapse bracketed-anchor patterns that plain-text mail parts emit
/// for `<a href>` into OSC 8 hyperlinks whose visible text is just the
/// anchor. Two forms are recognised:
///
///   `[anchor text <https://…>]`   — URL embedded inside the brackets
///   `[anchor text](<https://…>)`  — markdown-style; URL in following
///                                   parens, optionally wrapped in `< >`
///
/// Both forms allow the anchor and URL to be split across line breaks
/// (newsletter HTML→text emitters often do that). Patterns are
/// deliberately narrow — anchor cannot contain brackets — so they
/// don't chew up prose that happens to contain bracketed phrases.
/// Case-insensitive `strip_prefix` for RFC-style mail headers. Pass
/// the header name without the colon (e.g. "To", "Cc", "Bcc"); the
/// function matches `"<name>: "` regardless of case (`"To: "`,
/// `"to: "`, `"CC: "`, `"BCC: "`, …). Returns the value portion.
/// Centralised so all four compose-header parsers stay consistent —
/// the user can type any casing and the To/Cc/Bcc expansion catches it.
fn strip_header_ci<'a>(line: &'a str, header: &str) -> Option<&'a str> {
    let need = header.len() + 2; // "<name>: "
    if line.len() < need { return None; }
    let (prefix, rest) = line.split_at(need);
    let bytes = prefix.as_bytes();
    if bytes[header.len()] != b':' || bytes[header.len() + 1] != b' ' {
        return None;
    }
    if prefix[..header.len()].eq_ignore_ascii_case(header) {
        Some(rest)
    } else {
        None
    }
}

/// Apple Mail writes an attachment into the plain-text part as
/// `<name.pdf>`, in the position it held in the message — which for a
/// reply is glued to the end of the quoted signature, three of them run
/// together on one line with no space in front. Give each its own line,
/// under the quote prefix the line came with.
///
/// Only when they are stuck to text: a line that already keeps them
/// apart reads fine as it is. And only a name with an extension, so a
/// `<https://…>` or a `<name@host>` is left alone — the text is the
/// sender's, the line breaks are all this adds.
fn break_attachment_markers(body: &str) -> String {
    if !body.contains('<') { return body.to_string(); }
    let mut out = String::with_capacity(body.len() + 32);
    for line in body.lines() {
        match split_trailing_markers(line) {
            None => { out.push_str(line); }
            Some((rest, markers)) => {
                let quote: String = line
                    .chars()
                    .take_while(|c| *c == '>' || *c == ' ' || *c == '\t')
                    .collect();
                out.push_str(&rest);
                for m in markers {
                    out.push('\n');
                    out.push_str(&quote);
                    out.push_str(&m);
                }
            }
        }
        out.push('\n');
    }
    out
}

/// The run of `<name.ext>` markers a line ends with, and what is left of
/// the line — `None` unless there is at least one and it is glued to
/// text. See [`break_attachment_markers`].
fn split_trailing_markers(line: &str) -> Option<(String, Vec<String>)> {
    let mut rest = line.trim_end();
    let mut markers: Vec<String> = Vec::new();
    while rest.ends_with('>') {
        let start = match rest.rfind('<') { Some(i) => i, None => break };
        let inner = &rest[start + 1..rest.len() - 1];
        // A filename, and nothing that is plainly something else: an
        // address has an `@`, a URL has a scheme.
        if inner.is_empty() || inner.len() > 120 { break; }
        if inner.contains('@') || inner.contains("://") { break; }
        let has_ext = inner.rsplit_once('.').is_some_and(|(stem, ext)| {
            !stem.is_empty()
                && (1..=5).contains(&ext.len())
                && ext.chars().all(|c| c.is_ascii_alphanumeric())
        });
        if !has_ext { break; }
        markers.push(rest[start..].to_string());
        rest = &rest[..start];
    }
    // Glued, or there is nothing to fix: a line that is only markers, or
    // that puts a space before them, is already readable.
    if markers.is_empty() || rest.is_empty() || rest.ends_with(char::is_whitespace) {
        return None;
    }
    markers.reverse();
    Some((rest.to_string(), markers))
}

fn collapse_bracketed_links(body: &str) -> String {
    use std::sync::OnceLock;
    static EMBEDDED: OnceLock<regex::Regex> = OnceLock::new();
    static MD_ANGLE: OnceLock<regex::Regex> = OnceLock::new();
    static MD_BARE: OnceLock<regex::Regex> = OnceLock::new();
    let embedded = EMBEDDED.get_or_init(|| {
        regex::Regex::new(
            r"(?s)\[\s*([^\[\]]{1,200}?)\s*<(https?://[^<>\s]+)>\s*\]"
        ).unwrap()
    });
    // [label](<url>) — angle-bracketed URL form; allow whitespace inside since
    // the `>)` terminator is unambiguous (real-world malformed mail-merge URLs
    // sometimes embed personalisation text mid-URL).
    let md_angle = MD_ANGLE.get_or_init(|| {
        regex::Regex::new(
            r"(?s)\[\s*([^\[\]]{0,4000}?)\s*\]\(\s*<(https?://[^<>]{1,3000}?)>\s*\)"
        ).unwrap()
    });
    // [label](url) — bare URL. No whitespace, no brackets, no parens.
    let md_bare = MD_BARE.get_or_init(|| {
        regex::Regex::new(
            r"(?s)\[\s*([^\[\]]{0,4000}?)\s*\]\(\s*(https?://[^<>()\s]+)\s*\)"
        ).unwrap()
    });

    // Use private-use Unicode sentinels so subsequent regex passes don't see
    // brackets inside the OSC-8 escape and can match outer wrappers around
    // already-collapsed inner ones:
    //   \u{E000}URL\u{E001}LABEL\u{E002}
    // Sentinels are replaced with real OSC-8 escapes after all passes.
    fn anchor_visible(anchor: &str) -> String {
        // Strip inner sentinel blocks down to just their LABEL so a wrapping
        // [outer](url) takes the inner button's text as its visible label.
        let mut out = String::with_capacity(anchor.len());
        let mut chars = anchor.chars();
        while let Some(c) = chars.next() {
            if c == '\u{E000}' {
                for c in chars.by_ref() { if c == '\u{E001}' { break; } }
                for c in chars.by_ref() {
                    if c == '\u{E002}' { break; }
                    out.push(c);
                }
            } else {
                out.push(c);
            }
        }
        out
    }
    let collapse = |src: &str, re: &regex::Regex| -> String {
        let mut out = String::with_capacity(src.len());
        let mut last = 0;
        for m in re.captures_iter(src) {
            let whole = m.get(0).unwrap();
            let anchor_raw = m.get(1).map(|a| a.as_str()).unwrap_or("");
            let url_raw = m.get(2).unwrap().as_str();
            // Strip whitespace embedded mid-URL (mailmerge text injection).
            let url: String = url_raw.split_whitespace().collect();
            out.push_str(&src[last..whole.start()]);
            let anchor_vis = anchor_visible(anchor_raw);
            let anchor_clean = anchor_vis.trim();
            let visible = if anchor_clean.is_empty() || anchor_clean == url {
                if url.len() > 60 { shorten_url_label(&url) } else { url.clone() }
            } else {
                anchor_clean.split_whitespace().collect::<Vec<_>>().join(" ")
            };
            out.push('\u{E000}');
            out.push_str(&url);
            out.push('\u{E001}');
            out.push_str(&visible);
            out.push('\u{E002}');
            last = whole.end();
        }
        out.push_str(&src[last..]);
        out
    };

    let mut cur = collapse(body, embedded);
    // Iterate angle-form pass: outer wrappers around inner collapsed sentinels.
    for _ in 0..4 {
        let next = collapse(&cur, md_angle);
        if next == cur { break; }
        cur = next;
    }
    cur = collapse(&cur, md_bare);

    // Restore sentinels into real OSC-8 escapes.
    let mut out = String::with_capacity(cur.len());
    let mut chars = cur.chars();
    while let Some(c) = chars.next() {
        if c == '\u{E000}' {
            let mut url = String::new();
            for c in chars.by_ref() {
                if c == '\u{E001}' { break; }
                url.push(c);
            }
            let mut visible = String::new();
            for c in chars.by_ref() {
                if c == '\u{E002}' { break; }
                visible.push(c);
            }
            out.push_str(&style::hyperlink(&url, &style::underline(&visible)));
        } else {
            out.push(c);
        }
    }
    out
}

/// Shorten a URL for display: `host` or `host/…` so the visible label
/// stays on a single pane line. Glass scans OSC 8 link spans cell-by-cell
/// and only the cells on the URL's first wrapped row register as clickable;
/// keeping the visible text short keeps the entire link on one row.
fn shorten_url_label(url: &str) -> String {
    let after_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let (host, rest) = match after_scheme.split_once('/') {
        Some((h, r)) => (h, r),
        None => (after_scheme, ""),
    };
    if rest.is_empty() { host.to_string() } else { format!("{}/…", host) }
}

// color_emails moved to the shared `highlight` crate (highlight::email).
// Kastrup, scribe, and any future consumer share the exact same email
// tokenization so their output is byte-for-byte identical.

/// Build a header row with KEY bold and VALUE non-bold (mirrors scribe's
/// `HeaderBold` style). Both share the same fg color. Inline email
/// addresses inside the value are colored 177 with the outer color
/// restored after.
/// One header line, drawn the way scribe draws it.
///
/// `highlight::EmailLineStyle::HeaderBold` means the WHOLE line carries
/// the header colour with the key merely bolded on top. This coloured
/// only the key and handed the value to `color_emails`, which sets a
/// colour at each address and restores after it but never sets one to
/// begin with — so a value stayed in the terminal's default until its
/// first email address, and a Subject with no address stayed white
/// throughout. The same mail in scribe was fully coloured.
fn header_row(key: &str, value: &str, color: u8) -> String {
    format!(
        "{} {}{}{}",
        style::styled(key, Some(color), None, "b"),
        style::set_fg(color),
        highlight::color_emails(value, Some(color)),
        style::RESET,
    )
}

/// Wrap URLs in a line with OSC 8 hyperlink escapes so kitty keeps them
/// clickable even when the visible text wraps across multiple pane lines.
/// Skips regions already inside an OSC-8 escape (from collapse_bracketed_links)
/// to avoid nesting OSC-8 inside OSC-8 — kitty consumes nested escapes
/// greedily and eats following visible text.
fn hyperlink_urls(line: &str) -> String {
    use std::sync::OnceLock;
    static URL_RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = URL_RE.get_or_init(|| {
        regex::Regex::new(r#"https?://[^\s<>()\[\]{}\x00-\x1f\x7f]+[^\s<>()\[\]{}\x00-\x1f\x7f.,;:!?'"]"#)
            .unwrap()
    });
    let bytes = line.as_bytes();
    let mut out = String::with_capacity(line.len());
    let mut i = 0;
    let is_osc8_open = |b: &[u8], pos: usize| -> bool {
        pos + 4 < b.len() && b[pos] == 0x1b && b[pos+1] == b']'
            && b[pos+2] == b'8' && b[pos+3] == b';' && b[pos+4] == b';'
    };
    let find_st = |b: &[u8], from: usize| -> Option<usize> {
        let mut p = from;
        while p + 1 < b.len() {
            if b[p] == 0x1b && b[p+1] == b'\\' { return Some(p); }
            p += 1;
        }
        None
    };
    while i < bytes.len() {
        if is_osc8_open(bytes, i) {
            // Skip OPEN: \x1b]8;;URL\x1b\\
            let open_st = match find_st(bytes, i + 5) { Some(p) => p, None => break };
            let after_open = open_st + 2;
            // Find CLOSE: \x1b]8;; immediately followed by \x1b\\
            let mut p = after_open;
            let mut close_at = None;
            while p < bytes.len() {
                if is_osc8_open(bytes, p) && p + 6 < bytes.len()
                    && bytes[p+5] == 0x1b && bytes[p+6] == b'\\' {
                    close_at = Some(p);
                    break;
                }
                p += 1;
            }
            let close_end = match close_at { Some(p) => p + 7, None => bytes.len() };
            out.push_str(std::str::from_utf8(&bytes[i..close_end]).unwrap_or(""));
            i = close_end;
            continue;
        }
        // Not in OSC-8: scan ahead until next OSC-8 open or end of line.
        let mut chunk_end = i;
        while chunk_end < bytes.len() && !is_osc8_open(bytes, chunk_end) {
            chunk_end += 1;
        }
        let chunk = std::str::from_utf8(&bytes[i..chunk_end]).unwrap_or("");
        let mut last = 0;
        for m in re.find_iter(chunk) {
            out.push_str(&chunk[last..m.start()]);
            let url = m.as_str();
            // Shorten visible label for long URLs so the link stays on one
            // wrapped pane row (glass clickability requires single-row span).
            let label = if url.len() > 60 { shorten_url_label(url) } else { url.to_string() };
            // OSC 8 link with SGR underline around the visible text so users
            // can see where links are; SGR 24 turns underline off after.
            out.push_str(&style::hyperlink(url, &style::underline(&label)));
            last = m.end();
        }
        out.push_str(&chunk[last..]);
        i = chunk_end;
    }
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

fn pad_visible(s: &str, target: usize) -> String {
    let w = crust::display_width(s);
    if w >= target {
        s.to_string()
    } else {
        let mut out = String::with_capacity(s.len() + (target - w));
        out.push_str(s);
        for _ in 0..(target - w) { out.push(' '); }
        out
    }
}

/// How a muted (hidden) channel resurfaces. `m` mutes until any new
/// message; `M` mutes until a mention/highlight. Stored per hidden
/// channel alongside the mute timestamp so resurfacing is based on
/// activity *newer* than the mute (re-pressing re-stamps it). A
/// resurfaced channel re-hides on its own once its new messages are
/// read — the mute stays in force, the unread just stops surfacing it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HideMode {
    UntilNew,
    UntilHighlight,
}

impl HideMode {
    fn as_str(self) -> &'static str {
        match self { HideMode::UntilNew => "new", HideMode::UntilHighlight => "highlight" }
    }
    fn from_str(s: &str) -> HideMode {
        match s { "highlight" => HideMode::UntilHighlight, _ => HideMode::UntilNew }
    }
}

/// A muted channel in a view: its section name, the resurface mode, and
/// the unix-seconds timestamp when it was muted. Persisted as
/// `hidden_channels_<view>` (JSON array of `{name, mode, at}`).
#[derive(Clone)]
struct HiddenChannel {
    name: String,
    mode: HideMode,
    hidden_at: i64,
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

    last_db_refresh: std::time::Instant,
    /// Last time the periodic stuck-maildir reconcile ran (see the main
    /// loop). Throttles it to ~once every 2 min on the loop's existing
    /// wake — no new timer thread.
    last_reconcile: std::time::Instant,
    /// Read state shared with the phone. `None` when no folder is
    /// configured, and then nothing here runs at all.
    read_sync: Option<read_sync::ReadSync>,
    last_read_sync: std::time::Instant,
    /// Phone-gateway replies awaiting a delivery result from the relay:
    /// (request id, "<platform>:<thread_key>" label, queued-at, warned). The
    /// main loop polls the relay's status markers while this is non-empty and
    /// surfaces sent / couldn't-deliver; if nothing comes back in time it
    /// warns once that the reply is held (no live notification), then drops
    /// the entry after a longer timeout.
    pending_gateway_replies: Vec<(String, String, std::time::Instant, bool)>,

    // State
    running: bool,
    current_view: String,
    active_folder: Option<String>,
    /// Sticky filter from `:search` or `/`. While set, the periodic
    /// `refresh_current_view` polls this filter instead of the
    /// `current_view`'s rules — without it, search results were being
    /// blanked every 5 s by the dirty-DB reconciliation. Cleared by
    /// `switch_to_view`, `refresh_view`, or pressing Esc.
    active_search_filter: Option<Filters>,
    active_search_label: String,
    /// Last `\` find-in-view needle, so `\` then Enter (empty) jumps to the
    /// next match without retyping.
    last_find: String,
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
    /// Ids whose deletion is queued to the async DB writer but maybe not yet
    /// committed. A refresh that re-reads the DB filters these out so a poll
    /// racing the write can't resurrect just-purged messages. Self-clears each
    /// id once the DB stops returning it (delete committed).
    pending_deletes: HashSet<i64>,
    browsed_ids: HashSet<i64>,
    unseen_ids: HashSet<i64>,
    /// A muted channel that resurfaced and whose last unread the user just
    /// read. Holds the channel (folder) name. The view re-hides it once the
    /// cursor leaves the channel (deferred so the message being read doesn't
    /// vanish under the cursor). Cleared when honoured or invalidated.
    mute_recheck_pending: Option<String>,

    /// Optional asmite count-file writer, mirrored from
    /// `~/.gmail.conf`. When present, every read-state mutation
    /// triggers `sync_mail_count()` so the strip display reflects
    /// kastrup's own reads (not just gmail-idle deliveries).
    mailfile_cfg: Option<mailfile::MailfileConfig>,

    folder_collapsed: HashMap<String, bool>,
    folder_count_cache: HashMap<String, (i64, i64)>,

    feedback_message: Option<(String, u8)>,
    feedback_expires: Option<std::time::Instant>,
    /// When true, the current (non-expiring) feedback toast is cleared
    /// on the next user keypress instead of after a timeout. Set by
    /// `set_feedback_sticky` for send results, which must survive
    /// until the user actually looks — but shouldn't linger once they
    /// start interacting again.
    feedback_clear_on_key: bool,

    showing_image: bool,
    right_pane_msg_id: Option<i64>,
    /// True only while a background refresh (refresh_current_view) is
    /// repainting. Auto-mark-read in render_message_content is gated off
    /// it: a new message that lands under a STATIONARY cursor must not be
    /// marked read — only an explicit user navigation onto it marks it.
    suppress_automark_read: bool,
    /// Cached rendered body for the most-recent message rendered in the
    /// right pane. Re-rendering the SAME message (cursor bounce, resize,
    /// return-from-editor) reuses this instead of re-running the full
    /// MIME → html_to_text → collapse → linkify pipeline. Invalidated
    /// when msg.id or msg.content hash changes.
    body_cache: Option<(i64, u64, String)>,
    /// The AI answer currently shown in the right pane: (formatted
    /// pane text, URLs found in the raw response). While set, x / X
    /// follow the answer's links instead of the message's, and the
    /// pane can be restored after the URL picker. Cleared whenever
    /// the pane goes back to showing a message.
    ai_pane: Option<(String, Vec<String>)>,
    pending_forward_ids: Vec<i64>,
    pending_forward_attachments: Vec<String>,
    pending_reply_id: Option<i64>,
    /// In-flight SMTP send. The worker thread runs the shell command
    /// and posts its (ok, stderr) result through this channel; the
    /// main loop calls `pump_pending_send` each tick to finish the
    /// transaction (save_to_sent / remove tempfile / mark_replied /
    /// feedback toast). `None` means no send is currently in flight.
    /// We only allow one at a time — UI feedback when a second
    /// attempt starts before the first completes.
    pending_send: Option<PendingSend>,
    /// Earliest `scheduled.send_at`, cached so the idle loop costs one
    /// integer compare instead of a query per wake. `None` = nothing
    /// scheduled. Refreshed whenever the table changes.
    next_send_at: Option<i64>,
    /// When the cache was last re-read from the table. Rows can also be
    /// inserted from outside kastrup (a script, a Claude session), so the
    /// cache is refreshed once a minute — an indexed MIN() over a handful
    /// of rows, against a loop that already wakes every ten seconds.
    last_sched_check: i64,
    compose_source_type: Option<String>,
    /// Set by the recall path so an unmodified editor return still
    /// lands on the review screen (Send / Postpone / Cancel) instead
    /// of silently abandoning the draft.
    compose_force_review: bool,
    /// What kind of message is currently being composed. The review
    /// screen + Send / Postpone handlers dispatch on this. Defaults
    /// to Email; the recall path sets it from the candidate's kind.
    compose_kind: DraftKind,
    image_display: Option<glow::Display>,

    // Threading state
    show_threaded: bool,
    group_by_folder: bool,
    display_messages: Vec<Message>,
    section_collapsed: HashMap<String, bool>,
    /// User-pinned section order for the current view. Sections in
    /// this list rank above unpinned ones (which then sort by
    /// latest-message). Persisted per-view as `section_order_<key>`
    /// in the settings table. Reloaded on view switch.
    current_section_order: Vec<String>,
    /// Per-buffer nick set populated by the weechat-relay supervisor.
    /// Held here so future @-completion in compose can read it
    /// without re-fetching from the relay each time.
    nick_lists: sources::weechat_relay::NickLists,
    /// Every weechat buffer the supervisor is currently subscribed
    /// to. The Folders view merges this with message-derived
    /// sections so empty channels still appear (weechat-buflist
    /// parity).
    subscribed_buffers: sources::weechat_relay::SubscribedBuffers,
    /// Cache of `folder → unread-message count`, refreshed on the
    /// same 5s tick as the message list. Drives the inactive-view
    /// badges in the top bar: any custom view whose filter matches
    /// at least one folder with unread messages gets a key-only
    /// badge (e.g. `1 5 F2`).
    unread_cache: std::collections::HashMap<String, i64>,
    /// Cache of `source_id → unread-message count`, refreshed on the
    /// same tick as `unread_cache`. Source-scoped views (Messenger,
    /// RSS, etc.) carry no folder filter, so the folder cache can't
    /// answer "does this source have unread" — without this, those
    /// views' badges lit whenever ANY folder anywhere had unread.
    source_unread_cache: std::collections::HashMap<i64, i64>,
    /// Per-view "has unread?" flag, keyed by the view's key_binding.
    /// Computed from each view's REAL filter (via `db.view_has_unread`), so
    /// the inactive-view badges match what the view would actually show —
    /// unlike the coarse folder/source caches. Refreshed on the 5s tick and
    /// after any mark-read / view reload, so render just reads the map.
    view_unread_cache: std::collections::HashMap<String, bool>,
    last_highlight_refresh: std::time::Instant,
    /// Per-view list of muted (hidden) channels. Each carries a mode
    /// (resurface on any new message vs only on a mention/highlight)
    /// and the mute timestamp. Applied AFTER the all-buffers merge: a
    /// muted channel stays hidden until activity newer than the mute
    /// time matches its mode. Persisted as `hidden_channels_<view>`.
    current_hidden_channels: Vec<HiddenChannel>,
    /// Ctrl+U peek: when true, muted channels show too (dim muted tag).
    /// Session-only view state, never persisted.
    show_muted: bool,

    // Background poller
    poller: Option<poller::Poller>,
    poller_rx: Option<std::sync::mpsc::Receiver<poller::PollerEvent>>,
    write_tx: std_mpsc::Sender<DbWriteOp>,
    /// Request a message body from the DB read worker (by id); the body
    /// returns on read_res_rx. Never read bodies on the render thread.
    read_req_tx: std_mpsc::Sender<i64>,
    read_res_rx: std_mpsc::Receiver<(i64, String, Option<String>)>,
    /// Ids with a body load in flight, so render doesn't re-request each frame.
    content_loading: HashSet<i64>,
    // Flipped to true whenever the DB writer thread mutates a message row.
    // The 5s periodic refresh skips its get_messages() query when this is false.
    messages_dirty: Arc<AtomicBool>,

    // Help state
    showing_help: bool,
    help_extended: bool,
    right_pane_locked: bool,
}

/// Auto-register a Discord polling source the first time we see a bot
/// token in `~/.kastrup/.env`. Idempotent: no-op once a row exists,
/// or if the token isn't set yet.
fn ensure_discord_source(db: &Arc<Database>) {
    let secrets = chat_send::load_secrets();
    if secrets.discord_bot_token.is_none() { return; }
    let existing = db.get_sources(false);
    if existing.iter().any(|s| s.plugin_type == "discord") { return; }
    db.add_source(
        "Discord",
        "discord",
        "{}",
        "[\"read\",\"send\"]",
        300, // poll every 5 min
    );
}

/// Auto-register a Weechat-Relay source if the connection triplet is
/// present in `~/.kastrup/.env`. Mirrors live IRC + Slack channels +
/// Discord-bridge + Matrix rooms (anything weechat sees) into kastrup
/// as DB messages, one folder per buffer. As of M5, transport is a
/// long-lived push connection driven by `spawn_weechat_relay_supervisor`;
/// poll_interval is set very large so the regular poller skips it.
fn ensure_weechat_relay_source(db: &Arc<Database>) -> Option<i64> {
    let secrets = sources::weechat_relay::load_secrets_for_main();
    if !secrets.has_all() { return None; }
    let existing = db.get_sources(false);
    if let Some(s) = existing.iter().find(|s| s.plugin_type == "weechat-relay") {
        return Some(s.id);
    }
    db.add_source(
        "WeeChat Relay",
        "weechat-relay",
        "{}",
        "[\"read\"]",
        86400,    // 1 day — push connection does the real work
    );
    db.get_sources(false).into_iter()
        .find(|s| s.plugin_type == "weechat-relay")
        .map(|s| s.id)
}

/// Auto-register a Slack polling source the first time we see a user
/// token (via `SLACK_API_TOKEN` in `~/.kastrup/.env`, or the weechat
/// fallback). Idempotent.
fn ensure_slack_source(db: &Arc<Database>) {
    let secrets = chat_send::load_secrets();
    if secrets.slack_token.is_none() { return; }
    let existing = db.get_sources(false);
    if existing.iter().any(|s| s.plugin_type == "slack") { return; }
    db.add_source(
        "Slack",
        "slack",
        "{}",
        "[\"read\",\"send\"]",
        300,
    );
}

/// A message id as it is actually written down: `kastrup:7957849` is
/// how one gets copied out of a note or a chat, and the bare number is
/// what is left after trimming it. One parser, so the command line and
/// the `#` prompt cannot disagree about what counts.
fn parse_message_id(s: &str) -> Option<i64> {
    let s = s.trim();
    s.strip_prefix("kastrup:").unwrap_or(s).trim().parse().ok()
}

fn main() {
    // --help / --version answer before anything else, including the
    // no-terminal guard below: a CLI that cannot say what it is when
    // asked over a pipe is useless to any tool that asks (the fe2o3
    // launcher shows this text in its card popup).
    if std::env::args().skip(1).any(|a| a == "-h" || a == "--help") {
        println!("kastrup — unified terminal messaging hub (Fe2O3 suite)");
        println!();
        println!("Usage: kastrup [OPTIONS]");
        println!();
        println!("  kastrup:ID | ID       open that message (paste an id straight in)");
        println!("  --compose-to ADDR     open a compose window to ADDR");
        println!("  --subject TEXT        subject for --compose-to");
        println!("  --backfill-text       fill the decoded body for older messages");
        println!("  --weechat-probe       one-shot relay wire test");
        println!("  -v, --version         print version");
        println!("  -h, --help            this text");
        println!();
        println!("Email, RSS, weechat (Slack / IRC / WhatsApp), Discord and the phone");
        println!("gateway in one inbox. Views 1-9, + composes, m mutes, ? shows every key.");
        println!("Data lives in ~/.kastrup/; config in ~/.kastrup/config.yml.");
        return;
    }
    if std::env::args().skip(1).any(|a| a == "-v" || a == "--version") {
        println!("kastrup {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    // --backfill-text: fill `content_text` for rows that predate the
    // column. Before any TUI init, so progress goes to a plain stdout —
    // and outside the main loop, because it is a one-off that has no
    // business costing anything on a normal start.
    if std::env::args().any(|a| a == "--backfill-text") {
        let db = match Database::new() {
            Ok(d) => d,
            Err(e) => { eprintln!("backfill: {}", e); std::process::exit(1); }
        };
        let total = db.content_text_missing();
        if total == 0 {
            println!("backfill: nothing to do — every message has a decoded body.");
            return;
        }
        println!("backfill: {} message(s) to decode", total);
        let start = std::time::Instant::now();
        let mut done = 0usize;
        loop {
            let n = db.backfill_content_text(500);
            if n == 0 { break; }
            done += n;
            print!("\r  {}/{} ({:.0}%)", done, total, done as f64 * 100.0 / total as f64);
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        println!("\rbackfill: {} done in {:.1}s", done, start.elapsed().as_secs_f64());
        return;
    }

    // --weechat-probe: M1 one-shot wire test for the relay client.
    // Hijacks main() before any TUI / DB init so the output goes
    // straight to stdout/stderr with no alt-screen interference.
    if std::env::args().any(|a| a == "--weechat-probe") {
        match sources::weechat_relay::probe() {
            Ok(_) => std::process::exit(0),
            Err(e) => {
                eprintln!("weechat-probe FAILED: {}", e);
                std::process::exit(1);
            }
        }
    }
    // --weechat-tail <buffer>: M2 — print last 20 lines of a
    // buffer + subscribe + tail forever. Buffer arg matches full_name
    // exactly, or case-insensitive contains as a fallback.
    {
        let cli: Vec<String> = std::env::args().collect();
        if let Some(i) = cli.iter().position(|a| a == "--weechat-tail") {
            let buf = cli.get(i + 1).cloned().unwrap_or_default();
            if buf.is_empty() {
                eprintln!("usage: kastrup --weechat-tail <buffer-full-name>");
                std::process::exit(2);
            }
            match sources::weechat_relay::tail(&buf) {
                Ok(_) => std::process::exit(0),
                Err(e) => {
                    eprintln!("weechat-tail FAILED: {}", e);
                    std::process::exit(1);
                }
            }
        }
        // --weechat-tags <buffer>: diagnostic — dump raw prefix bytes
        // and tags_array for the last 5 lines of a buffer. Used when
        // sender-from-tags fallback turns up weird values.
        if let Some(i) = cli.iter().position(|a| a == "--weechat-tags") {
            let buf = cli.get(i + 1).cloned().unwrap_or_default();
            if buf.is_empty() {
                eprintln!("usage: kastrup --weechat-tags <buffer-full-name>");
                std::process::exit(2);
            }
            match sources::weechat_relay::dump_tags(&buf, 5) {
                Ok(_) => std::process::exit(0),
                Err(e) => {
                    eprintln!("weechat-tags FAILED: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
    // An interactive TUI needs a real terminal on stdin to read keys.
    // Without one (a pipe, /dev/null, or a stray `kastrup --help` from a
    // diagnostic shell) Input::getchr returns None instantly on EOF and
    // the main loop busy-spins at ~12% CPU forever, holding a core out of
    // deep C-states. Bail out cheap — nothing useful runs headless past
    // here. Costs one isatty() on the cold startup path, zero when idle.
    if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        eprintln!("kastrup: no terminal on stdin — this is an interactive TUI, nothing to do.");
        std::process::exit(0);
    }
    log::info(&format!("Kastrup v{} starting", env!("CARGO_PKG_VERSION")));
    // Parse CLI args: --compose-to EMAIL --subject SUBJECT, or mailto:URL
    let args: Vec<String> = std::env::args().collect();
    let mut compose_to: Option<String> = None;
    let mut compose_subject: Option<String> = None;
    let mut goto_message: Option<i64> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            // A message id, however it was copied. See parse_message_id.
            a if parse_message_id(a).is_some() => {
                goto_message = parse_message_id(a);
                i += 1;
            }
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

    // Phase-timing: emit a single line per startup phase so the next
    // slow run (anything double-digit ms is suspect on a hot path,
    // anything double-digit s is screaming for attention) tells us
    // exactly which step blew the budget. Hot when idle: log writes
    // ~6 lines once at startup, nothing on idle.
    let t0 = std::time::Instant::now();
    let mut phase = std::time::Instant::now();
    let log_phase = |name: &str, p: &mut std::time::Instant| {
        let now = std::time::Instant::now();
        log::info(&format!("startup phase: {} took {} ms (total {} ms)",
            name, now.duration_since(*p).as_millis(),
            now.duration_since(t0).as_millis()));
        *p = now;
    };

    Crust::init();
    Crust::set_app_identity("Kastrup");
    let (cols, rows) = Crust::terminal_size();
    log_phase("crust init + identity + termsize", &mut phase);

    let config = Config::load();
    log_phase("config load", &mut phase);
    let db = Arc::new(Database::new().expect("Failed to open kastrup database"));
    log_phase("database open + pragmas (incl. any WAL replay)", &mut phase);
    // Auto-register a Discord source if the user has a bot token in
    // ~/.kastrup/.env and no discord source yet. Saves a manual setup
    // step — incoming DMs to the bot start landing in kastrup on the
    // next poll tick.
    ensure_discord_source(&db);
    ensure_slack_source(&db);
    let weechat_relay_source_id = ensure_weechat_relay_source(&db);
    log_phase("ensure chat sources", &mut phase);
    let source_type_map = db.get_source_type_map();
    log_phase("source type map", &mut phase);
    let views = db.get_views();
    log_phase("views", &mut phase);

    let width = config.pane_width;
    let border = config.border_style;
    let (top, left, right, bottom) = create_panes(cols, rows, width, border, &config);

    // Load the asmite-count-file config (mirrors what gmail-idle writes
    // so the strip display drops the count when the user reads here too).
    // Cloned into the writer thread so unread-affecting ops can rewrite
    // the count file synchronously with the DB mutation.
    let mailfile_cfg_main: Option<mailfile::MailfileConfig> = {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        mailfile::MailfileConfig::load(std::path::Path::new(&home))
    };

    // Spawn background DB writer thread. We KEEP the JoinHandle this
    // time so the shutdown sequence can wait for the writer's final
    // WAL truncate (otherwise the OS kills the thread mid-checkpoint
    // when main returns, and the truncate-on-quit guarantee evaporates).
    let (write_tx, write_rx) = std_mpsc::channel::<DbWriteOp>();
    let writer_db = db.clone();
    let messages_dirty = Arc::new(AtomicBool::new(false));
    let writer_dirty = messages_dirty.clone();

    // M5: long-lived push connection to the weechat relay. Replaces
    // the 2-min poll path with kernel-parked blocking reads — zero
    // userspace cycles when idle, real-time delivery when active.
    // The shared `nick_lists` map is populated by the supervisor and
    // later read by the App for @-completion (M6.3).
    let nick_lists: sources::weechat_relay::NickLists =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let subscribed_buffers: sources::weechat_relay::SubscribedBuffers =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    // Shared handle to the live relay socket so the resume watchdog (main
    // loop) can shut it down after a suspend and force a reconnect.
    let relay_kill: Arc<std::sync::Mutex<Option<std::net::TcpStream>>> =
        Arc::new(std::sync::Mutex::new(None));
    if let Some(sid) = weechat_relay_source_id {
        sources::weechat_relay::spawn_supervisor(
            db.clone(), sid, messages_dirty.clone(),
            nick_lists.clone(), subscribed_buffers.clone(), relay_kill.clone(),
        );
    }
    // Editor completion socket at ~/.kastrup/completion.sock.
    // Blocking accept loop on a worker thread; only consumes cycles
    // when an editor actively connects to ask for @nick / #channel
    // matches. Safe to start unconditionally — bind failures (e.g.
    // ~/.kastrup unwritable) silently no-op.
    completion_ipc::start_server(nick_lists.clone(), subscribed_buffers.clone());
    let writer_mailfile = mailfile_cfg_main.clone();
    let writer_handle = std::thread::spawn(move || {
        while let Ok(op) = write_rx.recv() {
            // Track whether this op changes counts the asmite cares about
            // (unread or folder membership). Rewriting the count file on
            // every op would be wasteful; gate it.
            let mut counts_dirty = false;
            // Freeze watchdog: a write that waits seconds on the conn
            // mutex (or on cold pages inside sqlite) is invisible from
            // the UI side, so name it here. Two clock reads per op, and
            // ops only exist because the user did something.
            let op_label = write_op_label(&op);
            let op_start = std::time::Instant::now();
            match op {
                DbWriteOp::MarkRead(id) => { writer_db.mark_as_read(id); counts_dirty = true; }
                DbWriteOp::MarkUnread(id) => { writer_db.mark_as_unread(id); counts_dirty = true; }
                DbWriteOp::ToggleStar(id) => { writer_db.toggle_star(id); }
                DbWriteOp::DeleteMessages(ids) => { writer_db.delete_messages(&ids); counts_dirty = true; }
                DbWriteOp::UpdateFolder(id, folder, meta) => {
                    writer_db.update_message_folder(id, &folder, &meta);
                    counts_dirty = true;
                }
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
                DbWriteOp::MarkReadByIds(ids) => {
                    // Same shape as MarkAllReadBulk, but scoped to an
                    // explicit id set so we cover exactly what was on
                    // screen — no risk of touching anything outside the
                    // current view.
                    if !ids.is_empty() {
                        let targets = writer_db.collect_unread_maildir_targets_by_ids(&ids);
                        writer_db.mark_as_read_by_ids(&ids);
                        let mut updates: Vec<(String, i64)> = Vec::with_capacity(targets.len());
                        for (metadata, id) in &targets {
                            if let Some(new_meta) = rename_maildir_add_seen(metadata) {
                                let json = serde_json::to_string(&new_meta).unwrap_or_default();
                                updates.push((json, *id));
                            }
                        }
                        if !updates.is_empty() {
                            let conn = writer_db.conn.lock().unwrap();
                            let tx = conn.unchecked_transaction();
                            if let Ok(tx) = tx {
                                if let Ok(mut stmt) = tx.prepare("UPDATE messages SET metadata = ? WHERE id = ?") {
                                    for (json, id) in &updates {
                                        let _ = stmt.execute(rusqlite::params![json, id]);
                                    }
                                }
                                let _ = tx.commit();
                            }
                        }
                        counts_dirty = true;
                    }
                }
                DbWriteOp::MarkAllReadBulk { filters, maildir_source_ids } => {
                    // 1. Collect unread maildir targets BEFORE flipping read=1.
                    //    Filter is `source_id IN (maildir-ids) AND read=0` so
                    //    sqlite hits `idx_messages_read`/source index instead
                    //    of full-scanning 250k rows.
                    let targets = writer_db.collect_unread_maildir_targets(
                        filters.as_ref(), &maildir_source_ids,
                    );
                    // 2. Flip read=1 across the filter scope.
                    writer_db.mark_all_as_read(filters.as_ref());
                    // 3. Rename files OUTSIDE the conn lock so concurrent
                    //    reads aren't blocked during fs work.
                    let mut updates: Vec<(String, i64)> = Vec::with_capacity(targets.len());
                    for (metadata, id) in &targets {
                        if let Some(new_meta) = rename_maildir_add_seen(metadata) {
                            let json = serde_json::to_string(&new_meta).unwrap_or_default();
                            updates.push((json, *id));
                        }
                    }
                    // 4. Bulk metadata UPDATE under a single transaction.
                    if !updates.is_empty() {
                        let conn = writer_db.conn.lock().unwrap();
                        let tx = conn.unchecked_transaction();
                        if let Ok(tx) = tx {
                            if let Ok(mut stmt) = tx.prepare("UPDATE messages SET metadata = ? WHERE id = ?") {
                                for (json, id) in &updates {
                                    let _ = stmt.execute(rusqlite::params![json, id]);
                                }
                            }
                            let _ = tx.commit();
                        }
                    }
                    counts_dirty = true;
                }
                DbWriteOp::SetSetting(key, val) => { writer_db.set_setting(&key, &val); }
                DbWriteOp::Execute(sql, params) => {
                    let conn = writer_db.conn.lock().unwrap();
                    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
                    let _ = conn.execute(&sql, param_refs.as_slice());
                }
            }
            // Any op that reached here touched the DB; flag the view as dirty
            // so the periodic refresh actually fetches. SetSetting is a
            // false positive but cheap — one extra refresh vs 720/hour saved.
            writer_dirty.store(true, Ordering::Relaxed);
            if counts_dirty {
                if let Some(ref cfg) = writer_mailfile {
                    let counts = writer_db.unread_count_by_folder();
                    mailfile::write_count_file(cfg, &counts);
                }
            }
            let op_ms = op_start.elapsed().as_millis();
            if op_ms >= 500 {
                log::warn(&format!("slow db write {}: {} ms", op_label, op_ms));
            }
        }
        // Channel closed → main thread is shutting down. Force a
        // TRUNCATE checkpoint here so the WAL is empty when the next
        // launch opens the DB: no replay work on startup. Pays the
        // checkpoint cost once on the quit side, in the background,
        // after the user has already pressed q — they don't see it.
        let conn = writer_db.conn.lock().unwrap();
        let t = std::time::Instant::now();
        let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
        log::info(&format!("shutdown: wal_checkpoint(TRUNCATE) took {} ms",
            t.elapsed().as_millis()));
    });

    // ── DB read worker ──────────────────────────────────────────────
    // Message bodies are lazy-loaded on selection. Reading them on the
    // render thread parks it in D-state for seconds when kastrup.db has a
    // cold page under disk contention (kfreeze 2025-12-08). Serve those
    // reads here: the render thread requests by id and never blocks; the
    // body returns on read_res_rx, drained in the main loop. Cold when
    // idle — the thread blocks on recv(), same shape as the writer.
    let (read_req_tx, read_req_rx) = std_mpsc::channel::<i64>();
    let (read_res_tx, read_res_rx) =
        std_mpsc::channel::<(i64, String, Option<String>)>();
    let reader_db = db.clone();
    std::thread::spawn(move || {
        // Read bodies on a PRIVATE WAL connection, not the shared `conn`
        // mutex. A cold/large body read parks this thread in D-state, but it
        // holds no lock the writer or render thread needs — so the UI no
        // longer freezes behind it (see kfreeze: 27 s folio_wait under the
        // shared lock). Fall back to the shared path if the aux open fails.
        let aux = reader_db.open_aux_connection().ok();
        while let Ok(id) = read_req_rx.recv() {
            let loaded = match aux.as_ref() {
                Some(c) => crate::database::Database::get_message_content_conn(c, id),
                None => reader_db.get_message_content(id),
            };
            if let Some((content, html)) = loaded {
                let _ = read_res_tx.send((id, content, html));
            }
        }
    });

    // Read state exchange, resolved before `config` moves into App.
    let read_sync = read_sync::ReadSync::new(&config.read_sync_dir, config.read_sync_days);
    let mut app = App {
        top, left, right, bottom,
        cols, rows,
        db,
        config,
        source_type_map,
        last_db_refresh: std::time::Instant::now(),
        last_reconcile: std::time::Instant::now(),
        read_sync,
        last_read_sync: std::time::Instant::now(),
        pending_gateway_replies: Vec::new(),
        running: true,
        current_view: "A".to_string(),
        active_folder: None,
        active_search_filter: None,
        active_search_label: String::new(),
        last_find: String::new(),
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
        pending_deletes: HashSet::new(),
        browsed_ids: HashSet::new(),
        unseen_ids: HashSet::new(),
        mute_recheck_pending: None,
        mailfile_cfg: mailfile_cfg_main,
        folder_collapsed: HashMap::new(),
        folder_count_cache: HashMap::new(),
        feedback_message: None,
        feedback_expires: None,
        feedback_clear_on_key: false,
        showing_image: false,
        right_pane_msg_id: None,
        suppress_automark_read: false,
            body_cache: None,
            ai_pane: None,
        pending_forward_ids: Vec::new(),
        pending_forward_attachments: Vec::new(),
        pending_reply_id: None,
        pending_send: None,
        next_send_at: None,
        last_sched_check: 0,
        compose_source_type: None,
        compose_force_review: false,
        compose_kind: DraftKind::Email,
        image_display: None,
        show_threaded: false,
        group_by_folder: false,
        display_messages: Vec::new(),
        section_collapsed: HashMap::new(),
        current_section_order: Vec::new(),
        nick_lists: nick_lists.clone(),
        subscribed_buffers: subscribed_buffers.clone(),
        unread_cache: std::collections::HashMap::new(),
        source_unread_cache: std::collections::HashMap::new(),
        view_unread_cache: std::collections::HashMap::new(),
        last_highlight_refresh: std::time::Instant::now() - std::time::Duration::from_secs(60),
        current_hidden_channels: Vec::new(),
        show_muted: false,
        poller: None,
        poller_rx: None,
        write_tx,
        read_req_tx,
        read_res_rx,
        content_loading: HashSet::new(),
        messages_dirty,
        showing_help: false,
        help_extended: false,
        right_pane_locked: false,
    };

    // Apply config defaults
    app.sort_order = app.config.sort_order.clone();
    app.sort_inverted = app.config.sort_inverted;
    app.date_format = app.config.date_format.clone();
    log_phase("app construction + config defaults", &mut phase);

    // First-run wizard if database is empty
    if app.db.is_empty() && app.db.get_sources(false).is_empty() {
        app.first_run_wizard();
    }

    // Heavy startup chores moved off the boot path: sync_mail_count
    // (asmite count file refresh — single SQL aggregate, ~300 ms when
    // warm, multi-second under disk pressure) and the stuck-maildir
    // reconcile (`instr()` scan over the metadata column, 2-40 s in
    // observed runs) used to delay the first paint by 2-3 s on a good
    // day. Both run fine as fire-and-forget background work — the
    // mail-count file is consumed by the asmite which polls it, and
    // the reconcile just sends DbWriteOp::SyncMaildirFlag events that
    // the writer thread already serialises. UI now paints immediately;
    // these chores complete a few seconds later with no user-visible
    // effect beyond the asmite count catching up.
    {
        let db = app.db.clone();
        let write_tx = app.write_tx.clone();
        let mailfile_cfg = app.mailfile_cfg.clone();
        std::thread::Builder::new()
            .name("kastrup-startup-chores".into())
            .spawn(move || {
                let t = std::time::Instant::now();
                if let Some(cfg) = mailfile_cfg {
                    let counts = db.unread_count_by_folder();
                    mailfile::write_count_file(&cfg, &counts);
                }
                log::info(&format!(
                    "background: sync_mail_count took {} ms",
                    t.elapsed().as_millis()
                ));

                // Stuck-maildir reconcile: DB read=1 but metadata's
                // maildir_file still points into a new/ subdir (a read
                // whose new/→cur/ move slipped through, e.g. a rename that
                // lost a race). Runs on a dedicated aux connection so it
                // never blocks the UI. The same function also fires
                // periodically from the main loop, so a stray clears within
                // a couple minutes instead of lingering until next restart.
                let t = std::time::Instant::now();
                reconcile_stuck_maildir(&db, &write_tx);
                log::info(&format!(
                    "background: stuck-maildir reconcile took {} ms",
                    t.elapsed().as_millis()
                ));
            })
            .expect("spawn kastrup-startup-chores thread");
    }
    log_phase("background startup chores spawned", &mut phase);

    // Start background poller
    let (poller_tx, poller_rx) = std::sync::mpsc::channel();
    let poller = poller::Poller::start(app.db.clone(), poller_tx);
    app.poller = Some(poller);
    app.poller_rx = Some(poller_rx);
    log_phase("poller spawn", &mut phase);

    // Render-first, scrape-later: paint chrome (panes, borders, top/bottom
    // bars) into the alt-screen with whatever's in memory so the user sees a
    // populated UI instantly. Then run the heavier switch_to_view (DB query
    // for filtered_messages) and re-render. On a cold start with thousands
    // of messages this turns a noticeable startup pause into two paints
    // separated by a few ms — the first one looks like instant boot.
    if app.left.border { app.left.border_refresh(); }
    if app.right.border { app.right.border_refresh(); }
    app.render_all();

    // Load initial view (heavy DB query) — then repaint with real data.
    let default_view = app.config.default_view.clone();
    app.switch_to_view(&default_view);
    app.render_all();

    // A message id on the command line: show that one and open it.
    if let Some(id) = goto_message {
        app.goto_message(id);
    }

    // Handle --compose-to from CLI (e.g. from Tock)
    if let Some(to) = compose_to {
        let subj = compose_subject.unwrap_or_default();
        app.compose_to(&to, &subj);
    }

    // Anything scheduled in an earlier session: prime the cache so the
    // first idle wake can deliver what is already due.
    app.refresh_next_send_at();

    // Resume watchdog state: wall-clock at the previous loop turn.
    let mut last_wall = std::time::SystemTime::now();

    while app.running {
        // Pick up any completed background SMTP send. Cheap try_recv
        // when nothing's queued.
        app.pump_pending_send();

        // Apply any message bodies the DB read worker finished off-thread,
        // and re-render the right pane if the open message just got its body.
        // Cheap try_recv (no-op when nothing's loading).
        if app.drain_loaded_bodies() {
            app.render_message_content();
        }

        // Check feedback expiry
        if let Some(expires) = app.feedback_expires {
            if std::time::Instant::now() >= expires {
                app.feedback_message = None;
                app.feedback_expires = None;
                app.render_bottom_bar();
            }
        }

        // Idle wake cadence: stay snappy while a feedback toast is on
        // screen (so it expires on time) OR while a send is in flight
        // (so the "Sent" toast lands within a second of the wire send
        // succeeding). Otherwise sleep longer. New-mail toasts and DB
        // refreshes lag by up to this many seconds, which is fine for
        // a background-poll inbox.
        let timeout_secs: u64 = if app.feedback_expires.is_some() || app.pending_send.is_some() || !app.content_loading.is_empty() { 1 } else { 10 };
        let key = Input::getchr(Some(timeout_secs));

        // Resume watchdog. Battery-free: one vDSO clock read per turn, no new
        // timer or wakeup. The loop wakes at most every `timeout_secs` (≤10s),
        // so a wall-clock gap ≥30s means the machine was suspended. On resume,
        // threads parked across the suspend don't reliably self-heal — the
        // poller's CLOCK_MONOTONIC condvar timeout under-counts suspend, and
        // the relay's blocking read sits on a half-open socket TCP keepalive
        // can't kill. Kick both so mail + chat re-sync immediately.
        let wall_now = std::time::SystemTime::now();
        if wall_now.duration_since(last_wall).map(|g| g.as_secs() >= 30).unwrap_or(false) {
            crate::log::info("resume: suspend gap detected — waking poller + recycling relay");
            if let Some(p) = app.poller.as_ref() { p.wake(); }
            if let Ok(slot) = relay_kill.lock() {
                if let Some(s) = slot.as_ref() { let _ = s.shutdown(std::net::Shutdown::Both); }
            }
        }
        last_wall = wall_now;

        match key {
            Some(k) => {
                // A sticky send-result toast clears on the first
                // keypress after it appeared, so the user always gets
                // to see it but it doesn't linger once they resume.
                if app.feedback_clear_on_key {
                    app.feedback_message = None;
                    app.feedback_expires = None;
                    app.feedback_clear_on_key = false;
                    app.render_bottom_bar();
                }
                // Freeze watchdog. Everything the user experiences as a
                // hang happens between these two clock reads, so a
                // "kastrup froze" report can name the key that did it
                // instead of guessing. Same vDSO cost as the resume
                // watchdog above, and only on an actual keypress.
                let key_start = std::time::Instant::now();
                app.handle_key(&k);
                // A muted channel the user just caught up on re-hides once
                // they navigate off it (cheap no-op unless a recheck is armed).
                app.honor_pending_mute_rehide();
                let key_ms = key_start.elapsed().as_millis();
                if key_ms >= 250 {
                    log::warn(&format!("slow key '{}': {} ms", k, key_ms));
                }
            }
            None => {
                // Same watchdog for the idle tick: a background refresh
                // that stalls looks exactly like a hang to the user, and
                // this arm runs at most once per second.
                let tick_start = std::time::Instant::now();
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
                // Anything scheduled that has come due. Gated on a cached
                // timestamp, so this is one integer compare when nothing
                // is waiting — no new timer, no query per wake.
                app.send_due_scheduled();
                // Periodic DB refresh (skip when showing inline images).
                // Gated on messages_dirty so an idle kastrup doesn't rerun
                // get_messages() every 5s for no reason.
                if !app.showing_image && app.delete_marked.is_empty() && app.last_db_refresh.elapsed().as_secs() >= 5 {
                    app.last_db_refresh = std::time::Instant::now();
                    if app.messages_dirty.swap(false, Ordering::Relaxed) {
                        app.refresh_current_view();
                    }
                }
                // Refresh the inactive-view highlight cache on the same
                // cadence — independent of messages_dirty because a
                // background relay-fired notify-send for an OTHER view
                // doesn't always trip the dirty flag.
                if app.last_highlight_refresh.elapsed().as_secs() >= 5 {
                    app.last_highlight_refresh = std::time::Instant::now();
                    let new_unread = app.db.unread_count_by_folder();
                    let new_src_unread = app.db.unread_count_by_source();
                    // External writers (the systemd ws-bridge-listen writing
                    // Workspace rows straight into kastrup.db) don't trip
                    // messages_dirty, so the gated list refresh above never
                    // fires for them. This unread requery already runs on the
                    // existing loop wake — if its result changed, an external
                    // insert landed, so flag dirty and the next gated refresh
                    // surfaces it. No new query / timer / wakeup: idle cost is
                    // one in-memory comparison.
                    if new_unread != app.unread_cache || new_src_unread != app.source_unread_cache {
                        app.messages_dirty.store(true, Ordering::Relaxed);
                        // Unread changed from a source that bypasses the
                        // DbWriteOp counts_dirty gate (poller new mail,
                        // external/maildir writers marking read). Those never
                        // rewrite the asmite count file, so ~/.mail drifts
                        // until a UI op or restart. Reuse new_unread (already
                        // computed above) — NOT all_folder_counts, whose
                        // unfiltered full-table scan of the multi-GB DB froze
                        // the UI thread in folio_wait on a cold page cache.
                        if let Some(ref cfg) = app.mailfile_cfg {
                            mailfile::write_count_file(cfg, &new_unread);
                        }
                    }
                    app.unread_cache = new_unread;
                    app.source_unread_cache = new_src_unread;
                    app.refresh_view_unread_cache();
                    app.render_top_bar();
                }
                // Periodic stuck-maildir reconcile. A read whose new/→cur/
                // move slipped through leaves the asmite counting a phantom
                // unread until restart; re-run the cheap scan every ~2 min on
                // a short-lived thread so a stray self-clears. Piggybacks on
                // this existing loop wake — no new timer — and runs off the
                // UI thread so even a cold scan can't stall input.
                if app.last_reconcile.elapsed().as_secs() >= 120 {
                    app.last_reconcile = std::time::Instant::now();
                    let db = app.db.clone();
                    let wtx = app.write_tx.clone();
                    std::thread::spawn(move || reconcile_stuck_maildir(&db, &wtx));
                }
                // Read state with the phone. Gated twice over: no folder
                // configured and this is a null check; folder configured
                // and it is an atomic load plus a couple of stat()s. The
                // database is only touched when a mark actually moved.
                if app.last_read_sync.elapsed().as_secs() >= 5 {
                    app.last_read_sync = std::time::Instant::now();
                    let ours = app.db.take_read_dirty();
                    if let Some(ref mut rs) = app.read_sync {
                        if ours || rs.others_changed() {
                            if rs.sync(&app.db) > 0 {
                                // The phone moved something; our own
                                // write set the flag again, and it has
                                // already been published.
                                app.db.take_read_dirty();
                                app.messages_dirty.store(true, Ordering::Relaxed);
                            }
                        }
                    }
                }
                // Surface phone delivery results for queued gateway replies.
                // Only touches the filesystem while a reply is outstanding, so
                // it's cold once the phone has reported (or timed out).
                if !app.pending_gateway_replies.is_empty() {
                    app.poll_gateway_reply_status();
                }
                let tick_ms = tick_start.elapsed().as_millis();
                if tick_ms >= 250 {
                    log::warn(&format!("slow idle tick: {} ms", tick_ms));
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
    // Wait for the writer thread to finish draining the channel +
    // running its shutdown wal_checkpoint(TRUNCATE). Bounded by the
    // queued op count (normally 0 — every interactive op flushes
    // before quit) plus the checkpoint itself (~ms on a sub-MB WAL).
    // Worth the wait: next startup has zero WAL replay work.
    let t = std::time::Instant::now();
    let _ = writer_handle.join();
    log::info(&format!("writer thread joined in {} ms", t.elapsed().as_millis()));
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
            "DOWN" => { self.move_down(); }
            "UP" => { self.move_up(); }
            "LEFT" => {
                if self.show_threaded { self.collapse_current(); }
            }
            "RIGHT" => {
                if self.show_threaded { self.expand_current(); }
            }
            "a" => { self.mark_section_read(); }
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
            "C-SPACE" => {
                if self.show_threaded { self.toggle_collapse_all(); }
            }
            "n" => { self.next_unread(); }
            "p" => { self.prev_unread(); }
            "J" => { self.jump_to_date(); }
            "G" => { self.cycle_view_mode(); }
            "{" | "C-UP" => { self.move_section(-1); }
            "}" | "C-DOWN" => { self.move_section(1); }
            "C-HOME" => { self.reset_section_order(); }
            "C-U" => { self.toggle_show_muted(); }
            "C-N" => { self.pick_nick_to_clipboard(); }
            "C-G" => { self.pick_channel_to_clipboard(); }

            // View switching
            "=" => { self.switch_to_view("A"); }
            "N" => { self.switch_to_view("N"); }
            "S" => { self.search_command(); }
            "C-S" => { self.show_sources(); }
            "C-W" => { self.show_views_screen(); }
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
            "A" => { self.mark_all_read(); }
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
            "m" => { self.hide_current_channel(HideMode::UntilNew); }
            "M" => { self.hide_current_channel(HideMode::UntilHighlight); }
            "E" => { self.edit_message(); }

            // Attachments / external
            "v" => { self.view_attachments(); }
            "V" => { self.toggle_inline_image(); }
            "D" => { self.download_images(); }
            "x" => { self.open_html_in_scroll(); }
            "X" => { self.open_html_in_external_browser(); }

            // Search / filter
            "/" => { self.search_prompt(); }
            "\\" => { self.find_in_view(); }
            "@" => { self.address_book_menu(); }

            // Sort
            "o" => { self.cycle_sort(); }
            "i" => { self.toggle_sort_invert(); }

            // Labels / save / misc
            "l" => { self.label_message(); }
            "s" => { self.file_message(); }
            "+" => { self.compose_new(); }
            "k" => { self.external_react(false); }
            "I" => { self.claude_command(); }
            "Z" => { self.open_in_tock(); }
            "z" => { self.triage_message(); }

            // UI
            "w" => { self.cycle_width(); }
            "W" => { self.cycle_width_reverse(); }
            "H" => { self.set_view_color(); }
            "P" => { self.show_preferences(); }

            // Claude integration. Harmonized across Fe2O3: I = one-shot
            // claude -p ask (above), Ctrl+A = full CC session. c keeps the
            // richer AI menu (Draft/Summarize/Translate/Ask + plugins); C
            // stays an alias for the session for muscle memory.
            "C-A" => { self.chat_command(); }
            "c" => { self.ai_assistant(); }
            "C" => { self.chat_command(); }

            // Vim-style `:` command prompt — types out the colon command
            // explicitly (e.g. `:claude tighten this`, `:search …`,
            // `:chat`, `:q`). Each shortcut letter delegates here so the
            // semantics are identical regardless of entry path.
            ":" => { self.colon_command(); }
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
            "#" => { self.goto_message_prompt(); }
            "Y" | "C-Y" => {
                self.copy_right_pane();
                self.set_feedback("Right pane copied to clipboard",
                    self.config.theme_colors.feedback_ok);
            }
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

            // Esc: drop sticky search, reload current view.
            "ESC" => {
                if self.active_search_filter.is_some() {
                    self.active_search_filter = None;
                    self.active_search_label.clear();
                    let key = self.current_view.clone();
                    self.switch_to_view(&key);
                    self.set_feedback("search cleared", self.config.theme_colors.feedback_ok);
                }
            }

            // Quit. `q` refuses to exit while a background send is in
            // flight (lost it once to a closed terminal mid-oauth);
            // `Q` (shift-q) force-quits anyway. This is a guard, not a
            // confirmation prompt — the user gets a clear toast naming
            // what's still running.
            "q" => {
                if self.pending_send.is_some() {
                    self.set_feedback(
                        "A send is still in flight — wait for it to finish, or press Q to force-quit",
                        self.config.theme_colors.feedback_warn,
                    );
                } else {
                    self.running = false;
                }
            }
            "Q" => { self.running = false; }

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
                        resolve_source_type(&self.source_type_map, msg);
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
            "Y" | "C-Y" => { self.copy_right_pane(); }
            _ => {}
        }
    }
}

// --- Rendering ---

impl App {
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

    /// Recompute the per-view "has unread?" cache from each view's REAL
    /// filter — a DB EXISTS over non-archived unread, mirroring exactly what
    /// the view shows. Keyed by the view's key_binding; skips the built-in
    /// A/N/* derived views. Runs on the 5s tick and after any mark-read /
    /// view reload, NEVER in the render loop (the badge draw just reads the
    /// map). Replaces the old folder/source-cache heuristic, which ignored
    /// per-branch rules (platform, sender) and archived state and so lit
    /// badges for unread the view would never display.
    fn refresh_view_unread_cache(&mut self) {
        let mut map = std::collections::HashMap::new();
        for v in &self.views {
            let Some(key) = v.key_binding.clone() else { continue };
            if matches!(key.as_str(), "A" | "N" | "*") { continue; }
            let Ok(json) = serde_json::from_str::<serde_json::Value>(&v.filters) else {
                continue;
            };
            let filters = parse_view_filters_json(&json);
            map.insert(key, self.db.view_has_unread(&filters, self.config.load_limit as i64));
        }
        self.view_unread_cache = map;
    }

    fn render_top_bar(&mut self) {
        // Unread + total both scoped to the current view. The previous
        // "9543 unread / 12 msgs" mixed scopes — `9543` was the DB-wide
        // unread across every source (RSS feeds, every mailbox, every
        // IM channel) while `12` was just the filtered view — so the
        // ratio was meaningless. Count unread inside filtered_messages
        // instead. It's a small loop, runs once per render, no DB hop.
        let total = self.filtered_messages.len() as i64;
        let unread = self.filtered_messages.iter().filter(|m| !m.read).count() as i64;
        // Unread highlights = mentions/pings still un-read in this view.
        // Surfaced in the top bar as `!K` so a glance at the strip
        // tells you whether the current view has anything that
        // actually wants your attention vs. ambient chatter.
        let highlights = self.filtered_messages.iter()
            .filter(|m| !m.read
                && m.metadata.get("highlight").and_then(|v| v.as_bool()) == Some(true))
            .count() as i64;

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

        // Position indicator: always expressed as "message N of total", not
        // the display-row index. In threaded view, display_messages has
        // section headers interleaved with messages; counting those as
        // "positions" gave a number that drifted from the per-folder
        // "[N messages]" tallies the user sees. Map the cursor back to a
        // real message position, skipping headers.
        let (shown_pos, shown_total) = if self.show_threaded {
            let headers_up_to_and_incl = self.display_messages.iter()
                .take(self.index + 1)
                .filter(|m| m.is_header)
                .count() as i64;
            let pos = ((self.index as i64) + 1 - headers_up_to_and_incl).max(0);
            (pos, total)
        } else {
            ((self.index as i64) + 1, total)
        };
        let pos_label = if shown_total > 0 {
            style::fg(&format!(" [{}/{}]", shown_pos, shown_total), tc.info_fg)
        } else {
            style::fg(" [0/0]", tc.info_fg)
        };

        // Inactive-view badges: list (just the key, no count) every
        // custom view OTHER than the current one whose filter matches
        // at least one folder with unread messages. Excludes the
        // built-in `A`/`N`/`*` derived views — they overlap with
        // everything and would always be lit. The display is just the
        // view's key glyph: e.g. `1 5 F2`, in the unread colour.
        // Badge every other view flagged unread in view_unread_cache (kept in
        // sync on the 5s tick / after mark-read). Iterate self.views for
        // stable left-to-right order; cheap map lookups, no parse, no DB.
        let mut other_view_badges: Vec<String> = Vec::new();
        for v in &self.views {
            let Some(key) = v.key_binding.clone() else { continue };
            if key == self.current_view { continue; }
            if matches!(key.as_str(), "A" | "N" | "*") { continue; }
            if self.view_unread_cache.get(&key).copied().unwrap_or(false) {
                other_view_badges.push(style::fg(&key, tc.unread));
            }
        }

        let badges_str = if other_view_badges.is_empty() {
            String::new()
        } else {
            format!("{}  ", other_view_badges.join(" "))
        };

        let counts_str = style::fg(&format!("{} unread / {} msgs", unread, total), tc.info_fg);
        let right_info = if highlights > 0 {
            format!("{}{}  {}",
                badges_str,
                style::fg(&format!("!{}", highlights), tc.unread),
                counts_str)
        } else {
            format!("{}{}", badges_str, counts_str)
        };

        // Build top bar: " Kastrup - [key] ViewName [Sort] [Mode] [pos] ... N unread / T msgs"
        let prefix = style::fg(" Kastrup - ", tc.prefix_fg);
        // In-flight SMTP badge: shows immediately when a send is
        // spawned and stays up until `pump_pending_send` clears it,
        // giving the user a stable "still working" cue while they
        // navigate. Truncate the recipient at 30 chars so the badge
        // doesn't push the right-side counts off the bar.
        let send_badge = if let Some(ps) = &self.pending_send {
            let mut who = ps.to_display.clone();
            if crust::display_width(&who) > 30 {
                who = who.chars().take(28).collect::<String>();
                who.push_str("\u{2026}");
            }
            style::fg(&format!("  \u{2191} Sending to {}\u{2026}", who), tc.feedback_warn)
        } else {
            String::new()
        };
        let left_part = format!("{}{}{}{}{}{}", prefix, view_label, sort_label, mode_label, pos_label, send_badge);
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
        // full_refresh (not diff refresh): the selected row's underline can
        // persist across cursor moves when a slow render_message_content
        // (e.g. a fat OBOS HTML body) sits between two left-pane repaints.
        // Writing every row from scratch is cheap and avoids stale state.
        self.left.full_refresh();
        if self.left.border { self.left.border_refresh(); }
    }

    fn format_section_header(&self, msg: &Message, selected: bool, pane_w: usize) -> String {
        let tc = &self.config.theme_colors;
        let subject = msg.subject.as_deref().unwrap_or("Section");
        let is_collapsed = msg.thread_id.as_ref()
            .and_then(|name| self.section_collapsed.get(name))
            .copied()
            .unwrap_or(self.group_by_folder);
        let arrow = if is_collapsed { "\u{25B8}" } else { "\u{25BE}" };
        // Section header carries `highlight_count` in metadata so the
        // renderer can swap the unread `*` for a louder `!` when at
        // least one unread mention/ping lives inside.
        let highlight_count = msg.metadata.get("highlight_count")
            .and_then(|v| v.as_u64()).unwrap_or(0);
        let unread_mark = if highlight_count > 0 {
            style::fg(" !", tc.unread)
        } else if !msg.read {
            style::fg(" *", tc.unread)
        } else {
            String::new()
        };

        // Pick a chat-source theme colour from the display name (so a
        // `slack.<ws>.&team` header lights up in Slack colour, IRC
        // channels in the IRC colour, etc.). For a non-chat folder we
        // fall through to tc.thread.
        let chat_source = chat_source_type_for_display(subject);
        let (icon, _row) = source_info(chat_source.unwrap_or(&msg.source_type), tc);
        // For weechat-relay folders the source type lives in the display
        // name (chat_source). For gateway folders the section already
        // carries the resolved platform in source_type, so colour the
        // channel name with that source's fg too (whatsapp/sms/etc.);
        // genuinely unknown sources fall back to tc.thread via source_info.
        let channel_color = match chat_source {
            Some(st) => source_info(st, tc).1,
            None => source_info(&msg.source_type, tc).1,
        };

        let lead = style::bold(&style::fg(&format!("{} {} ", arrow, icon), tc.thread));
        let suffix = style::fg(&format!(" [{}]", msg.content), tc.hint_fg);
        // During a Ctrl+U peek, tag the channels that are only visible
        // because of the peek. The tag shows the mute mode via the key that
        // set it — [m] until-new, [M] until-mention — which is also the key
        // that unmutes it (same key on the header toggles off).
        let muted_tag = if self.show_muted {
            msg.thread_id.as_ref()
                .and_then(|n| self.current_hidden_channels.iter().find(|h| &h.name == n))
                .map(|h| match h.mode {
                    HideMode::UntilNew => style::fg(" [m]", tc.hint_fg),
                    HideMode::UntilHighlight => style::fg(" [M]", tc.hint_fg),
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        // Cap the name so the whole header is exactly ONE visual row.
        // render_message_list windows the list by item index assuming one
        // row per item; a name wide enough to wrap added a phantom row and
        // desynced the cursor highlight from the right-pane selection. The
        // trailing pad below handles the short case; this is the missing
        // pad-DOWN, and it truncates only the name so the count + unread
        // mark survive.
        let deco_w = crust::display_width(&lead)
            + crust::display_width(&suffix)
            + crust::display_width(&muted_tag)
            + crust::display_width(&unread_mark);
        let name = truncate_str(subject, pane_w.saturating_sub(deco_w));

        // Split the display name at the LAST `.` — everything up to and
        // including that dot is dimmed (the workspace / network prefix);
        // the tail is the channel name and keeps the source-themed colour.
        // No dot → no split, the whole name uses the source colour.
        let (dim_part, chan_part) = match name.rfind('.') {
            Some(idx) => (&name[..=idx], &name[idx + 1..]),
            None      => ("", name.as_str()),
        };
        let chan_styled = if selected {
            style::underline(&style::bold(&style::fg(chan_part, channel_color)))
        } else {
            style::bold(&style::fg(chan_part, channel_color))
        };
        let dim_styled = if dim_part.is_empty() {
            String::new()
        } else if selected {
            style::underline(&style::fg(dim_part, tc.hint_fg))
        } else {
            style::fg(dim_part, tc.hint_fg)
        };

        let content = format!("{}{}{}{}{}{}", lead, dim_styled, chan_styled, suffix, muted_tag, unread_mark);

        // Trailing pad so the row bg fills the pane width; padding is not
        // underlined so the underline hugs just the subject.
        let content_w = crust::display_width(&content);
        let padding = if pane_w > content_w { " ".repeat(pane_w - content_w) } else { String::new() };
        format!("{}{}", content, padding)
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

        // Sender column: 12 chars, plus a 1-char per-sender avatar in front
        // (own deterministic color). 12 + 1 (gap) + 1 (avatar) + 1 (gap) =
        // same 15-cell budget as the previous bare-sender layout.
        // For email replies (`thread_depth > 0`), the sender column is
        // shortened by the indent amount and the missing prefix appears
        // as a tree-drawing rail (└─) so the reply nests visually under
        // its parent. Depth caps at 4 to avoid eating the entire column.
        let depth_indent = (msg.thread_depth as usize).min(4);
        let (depth_prefix, sender_cap) = if depth_indent == 0 {
            (String::new(), 12usize)
        } else {
            let mut s = String::new();
            for _ in 0..depth_indent.saturating_sub(1) { s.push_str("  "); }
            s.push_str("└ ");
            (style::fg(&s, self.config.theme_colors.hint_fg),
             12usize.saturating_sub(depth_indent * 2))
        };
        let sender_display = msg.display_name();
        let sender_truncated = truncate_str(sender_display, sender_cap);
        let sender_padded = format!("{}{:<width$} ", depth_prefix, sender_truncated, width = sender_cap);

        // Subject fills remaining width (decode RFC 2047 encoded-words)
        let raw_subject = msg.subject.as_deref().unwrap_or("");
        let subject = sources::maildir::decode_rfc2047(raw_subject);
        // Calculate available width for subject
        // "N r I DDDDDD i a sender        subject"
        // 1+1+1+1+6+1+1+1+1+1+12+1 = 29 fixed chars (same as before; avatar
        // takes 1 + gap from sender column).
        let fixed = 29;
        let subj_w = pane_w.saturating_sub(fixed);
        let subject_truncated = truncate_str(&subject, subj_w);

        let flags = format!("{}{}{}", nflag, rflag, ind);

        // Compute outer color first so we can splice the avatar's own color
        // inline and then re-open the outer color afterward (an inner SGR
        // close would otherwise reset to default fg for the rest of the line).
        let color = if self.delete_marked.contains(&msg.id) {
            self.config.theme_colors.delete_mark
        } else if self.tagged.contains(&msg.id) {
            self.config.theme_colors.tag
        } else if msg.starred {
            self.config.theme_colors.star
        } else {
            scolor
        };

        // Per-sender avatar: 1 colored char between source icon and sender.
        let (avatar_ch, avatar_color) = sender_avatar(&msg.sender, msg.sender_name.as_deref());
        let avatar_inline = format!(
            "{}{}{}",
            style::set_fg(avatar_color),
            avatar_ch,
            style::set_fg(color)
        );

        // Build content. avatar_inline carries its own ANSI; everything
        // else is plain text and gets colored by the outer style::fg below.
        let content = format!("{} {} {} {}{}", date_padded, icon, avatar_inline, sender_padded, subject_truncated);

        // Pad to full width — display_width strips ANSI before measuring.
        let flags_w = crust::display_width(&flags);
        let content_w = crust::display_width(&content);
        let padding = if pane_w > flags_w + content_w + 1 {
            " ".repeat(pane_w - flags_w - content_w - 1)
        } else {
            String::new()
        };
        let full_content = format!("{}{}", content, padding);

        if selected {
            format!("{}{}{}", flags, style::underline(&style::bold(&style::fg(&content, color))), style::bold(&style::fg(&padding, color)))
        } else if !msg.read {
            format!("{}{}", flags, style::bold(&style::fg(&full_content, color)))
        } else {
            format!("{}{}", flags, style::fg(&full_content, color))
        }
    }

    /// Store a body the DB read worker finished into the in-memory message
    /// copies (display + filtered) and clear its in-flight marker. Clones the
    /// body only in threaded mode, where both vecs hold a copy.
    fn apply_loaded_body(&mut self, loaded: (i64, String, Option<String>)) -> i64 {
        let (id, content, html) = loaded;
        self.content_loading.remove(&id);
        if self.show_threaded {
            if let Some(m) = self.display_messages.iter_mut().find(|m| m.id == id) {
                m.content = content.clone();
                m.html_content = html.clone();
                m.full_loaded = true;
            }
        }
        if let Some(m) = self.filtered_messages.iter_mut().find(|m| m.id == id) {
            m.content = content;
            m.html_content = html;
            m.full_loaded = true;
        }
        id
    }

    /// Drain every body the DB read worker has finished. Returns true if the
    /// currently-selected message was among them, so the caller re-renders the
    /// right pane. Cheap try_recv; never blocks.
    fn drain_loaded_bodies(&mut self) -> bool {
        let cur_id = {
            let list = if self.show_threaded { &self.display_messages } else { &self.filtered_messages };
            list.get(self.index).map(|m| m.id)
        };
        let mut redraw = false;
        while let Ok(res) = self.read_res_rx.try_recv() {
            let id = self.apply_loaded_body(res);
            if Some(id) == cur_id { redraw = true; }
        }
        redraw
    }

    fn render_message_content(&mut self) {
        // The pane is going back to a message — any AI answer is gone.
        self.ai_pane = None;
        // Auto-mark as read when displayed in right pane
        let msg_ref = if self.show_threaded {
            self.display_messages.get(self.index)
        } else {
            self.filtered_messages.get(self.index)
        };
        if let Some(msg) = msg_ref {
            // Clear unseen protection if user navigated away and came back.
            // Skipped during a background refresh: when a new message lands
            // under a stationary cursor, right_pane_msg_id still points at the
            // PREVIOUS message, so this heuristic would mistake "arrived here"
            // for "navigated here" and wrongly mark the new message read.
            if !self.suppress_automark_read
                && self.unseen_ids.contains(&msg.id) && self.right_pane_msg_id != Some(msg.id) {
                self.unseen_ids.remove(&msg.id);
            }
            if !self.suppress_automark_read
                && !msg.read && !msg.is_header && msg.id > 0 && !self.unseen_ids.contains(&msg.id) {
                let id = msg.id;
                let metadata = msg.metadata.clone();
                let folder = msg.folder.clone();
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
                // If this read happened inside a muted channel that had
                // resurfaced, arm a deferred re-hide. We don't rebuild here:
                // the rest of this function still draws the right pane from
                // self.index, and hiding the channel now would yank the row
                // out from under the message being read. The main loop honours
                // the flag once the cursor leaves the channel.
                if self.group_by_folder {
                    if let Some(f) = folder.as_deref() {
                        if self.current_hidden_channels.iter().any(|h| h.name == f) {
                            self.mute_recheck_pending = Some(f.to_string());
                        }
                    }
                }
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
                        .unwrap_or(self.group_by_folder);
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

        // Lazy-load the full body OFF the render thread (see DB read worker).
        // Request by id and return immediately. A warm read is caught right
        // away by the short recv_timeout so the common case stays instant; a
        // cold/contended read falls through to async — the main loop drains it
        // on a later tick and re-renders, and the body shows "loading…" until
        // then. This keeps a cold kastrup.db page from freezing the UI.
        let need = (if self.show_threaded {
            self.display_messages.get(self.index)
        } else {
            self.filtered_messages.get(self.index)
        }).filter(|m| !m.full_loaded && m.id != 0).map(|m| m.id);
        if let Some(id) = need {
            if self.content_loading.insert(id) {
                let _ = self.read_req_tx.send(id);
            }
            if let Ok(res) =
                self.read_res_rx.recv_timeout(std::time::Duration::from_millis(40))
            {
                self.apply_loaded_body(res);
            }
        }

        // Clamp self.index in case a view refresh shrank the list while the
        // cursor was parked past the new end. Previously panicked at the
        // messages[self.index] below with "len is N but index is M".
        let list_len = if self.show_threaded {
            self.display_messages.len()
        } else {
            self.filtered_messages.len()
        };
        if list_len == 0 {
            self.right.set_text("");
            self.right.ix = 0;
            self.right.full_refresh();
            return;
        }
        if self.index >= list_len {
            self.index = list_len - 1;
        }
        let messages = if self.show_threaded {
            &self.display_messages
        } else {
            &self.filtered_messages
        };
        let msg = &messages[self.index];
        let tc = &self.config.theme_colors;
        let (_, scolor) = source_info(&msg.source_type, tc);

        // Body-render cache: re-rendering the same message (cursor bounce,
        // resize, return-from-editor) reuses the pre-styled lines instead
        // of running the full MIME → html_to_text → collapse → linkify
        // pipeline again. Fingerprint = content + html_content lengths;
        // changes if either body grew/shrank (which is what happens on
        // full-content lazy-load).
        let content_fp = (msg.content.len() as u64)
            ^ ((msg.html_content.as_ref().map_or(0, |h| h.len()) as u64) << 32);
        let cache_key = (msg.id, content_fp);
        let current_id = Some(msg.id);
        let msg_changed = current_id != self.right_pane_msg_id;
        if !msg_changed && msg.id != 0 {
            if let Some((cid, cfp, ref text)) = self.body_cache {
                if (cid, cfp) == cache_key {
                    // Cache hit — reuse rendered text; skip the heavy pipeline.
                    self.right.set_text(text);
                    self.right.full_refresh();
                    if self.right.border { self.right.border_refresh(); }
                    return;
                }
            }
        }

        let mut lines = Vec::new();

        // Headers — use header_row helper: KEY bold, VALUE non-bold, both
        // in the same color, with inline email addresses colored 177. This
        // matches scribe's email-mode rendering exactly so reading mail in
        // kastrup and composing in scribe produces visually identical text.
        let name = msg.display_name();
        let from_display = if name == msg.sender {
            msg.sender.clone()
        } else {
            format!("{} <{}>", name, msg.sender)
        };
        lines.push(header_row("From:", &from_display, tc.header_from));

        let to_display = parse_json_recipients(&msg.recipients);
        if !to_display.is_empty() {
            lines.push(header_row("To:", &to_display, tc.header_from));
        }

        let cc_display = msg.cc.as_ref()
            .map(|c| parse_json_recipients(c))
            .unwrap_or_default();
        if !cc_display.is_empty() {
            lines.push(header_row("Cc:", &cc_display, tc.header_from));
        }

        // Bcc: show only when meaningful. Either the DB row has it
        // explicitly (rare — MTAs typically strip Bcc from received
        // mail) OR none of the user's identity addresses appear in
        // To/Cc, in which case we synthesise "(you, hidden)" so the
        // user knows why the message landed in their inbox.
        let bcc_display = msg.bcc.as_ref()
            .map(|c| parse_json_recipients(c))
            .unwrap_or_default();
        if !bcc_display.is_empty() {
            lines.push(header_row("Bcc:", &bcc_display, tc.header_from));
        } else if msg.source_type == "email" || msg.source_type == "maildir" {
            let mine: Vec<String> = self.config.identities.values()
                .map(|i| i.email.to_ascii_lowercase())
                .filter(|e| !e.is_empty())
                .collect();
            if !mine.is_empty() {
                let to_lc = parse_json_recipients(&msg.recipients).to_ascii_lowercase();
                let cc_lc = cc_display.to_ascii_lowercase();
                let visible = format!("{} {}", to_lc, cc_lc);
                let in_visible = mine.iter().any(|e| visible.contains(e));
                if !in_visible {
                    lines.push(header_row("Bcc:", "(you, hidden)", tc.header_from));
                }
            }
        }

        if let Some(ref subj) = msg.subject {
            let decoded_subj = sources::maildir::decode_rfc2047(subj);
            lines.push(header_row("Subject:", &decoded_subj, tc.header_subj));
        }

        let full_date = format_timestamp(msg.timestamp, "%Y-%m-%d %H:%M");
        lines.push(header_row("Date:", &full_date, tc.header_date));

        // Type — value uses source-specific color; build manually since the
        // VALUE differs from the KEY color (header_row assumes one color).
        lines.push(format!("{} {}",
            style::bold(&style::fg("Type:", tc.header_date)),
            style::fg(&msg.source_type, scolor)));

        if !msg.labels.is_empty() {
            let label_str = msg.labels.iter()
                .map(|l| format!("[{}]", l))
                .collect::<Vec<_>>()
                .join(" ");
            lines.push(header_row("Labels:", &label_str, tc.header_label));
        }

        // Separator
        lines.push(style::fg(&"\u{2500}".repeat(40), tc.separator));

        // Body not loaded yet — the DB read worker is fetching it off-thread.
        // Show a hint instead of a blank pane; the main loop re-renders the
        // moment the body lands.
        if !msg.full_loaded && msg.id != 0 {
            lines.push(String::new());
            lines.push(style::fg("  loading…", tc.hint_fg));
        }

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
            lines.push(style::fg("HTML mail — x: open in scroll · X: open in browser", tc.html_hint));
            lines.push(String::new());
        }

        lines.push(String::new());

        // Content: extract from MIME, decode QP, detect HTML and parse
        let raw = &msg.content;
        // Relay / chat / gateway / RSS bodies are plain UTF-8 and must NOT pass
        // through email MIME/QP/base64 decoding. A Slack message whose URL
        // carries `=D7`/`=47`-style query params (joinCode=…&leagueId=…) trips
        // looks_quoted_printable (≥3 `=XX` hits) and gets QP-mangled — the
        // kastrup:7953506 "encoding" bug. Only e-mail sources get decoded.
        let is_email = matches!(
            self.source_type_map.get(&msg.source_id).map(String::as_str).unwrap_or(""),
            "email" | "maildir" | "imap" | "gmail"
        );
        // Try MIME multipart extraction first
        let looks_mime = raw.contains("boundary=")
            || (raw.contains("Content-Type:") && raw.lines().any(|l| l.starts_with("--") && l.len() > 5));
        let extracted = if !is_email {
            raw.clone()
        } else if looks_mime {
            // If MIME parsing finds no readable body (e.g. attachment-only
            // message where text/html is empty), fall back to an empty
            // string rather than dumping the raw multipart envelope into
            // the pane — the attachment list + image hint already rendered
            // above tell the user what's there.
            extract_mime_text(raw).unwrap_or_default()
        } else if looks_base64(raw) {
            // Raw base64 body (no MIME headers). Checked BEFORE the QP
            // branch because base64 payloads end with `==\n` for
            // 1-byte padding, which trips looks_quoted_printable's
            // earliest `s.contains("=\n")` check and routes the body
            // through the wrong decoder. base64 is a more specific
            // signal (5 lines of pure base64 chars, no other tokens),
            // so prefer it when it matches.
            sources::maildir::base64_decode(raw.trim())
                .and_then(|bytes| String::from_utf8(bytes).ok()
                    .or_else(|| Some(latin1_to_utf8(&sources::maildir::base64_decode(raw.trim()).unwrap_or_default()))))
                .unwrap_or_else(|| raw.clone())
        } else if raw.contains("Content-Transfer-Encoding: quoted-printable")
                  || looks_quoted_printable(raw) {
            // Single-part QP encoded. The first branch catches mails
            // stored with their MIME headers intact; the second
            // catches what the maildir parser leaves behind once it
            // has stripped headers (the Nordea bug — the body was
            // QP-encoded but the explicit CTE header was gone, so
            // we render `p=E5` instead of `på`).
            decode_quoted_printable(&raw[body_after_headers(raw)..])
        } else {
            raw.clone()
        };
        // (Previously: a second `decode_quoted_printable` pass that
        // re-ran on already-decoded text "to catch missed soft line
        // breaks". Removed because it's actively destructive on
        // bodies containing literal `=XY` sequences where X/Y happen
        // to be ASCII hex digits — e.g. URL query-strings, the
        // `============` markdown heading underline followed by a
        // newline, etc. The second pass mis-decodes them into raw
        // non-ASCII bytes that invalidate the UTF-8 stream, and the
        // function's last-resort `latin1_to_utf8` fallback then
        // mojibakes every Norwegian char (UTF-8 `å` shows as `Ã¥`).
        // QP decoding is done in `extract_mime_text` / the
        // single-part branch above; this guard added nothing
        // legitimate.)
        let is_html_fallback = {
            let lc = extracted.to_lowercase();
            extracted.trim().is_empty() || lc.contains("html messages are not support")
                || lc.contains("not displayed") || lc.contains("html-e-post")
                || lc.contains("støtter ikke html") || lc.contains("does not support html")
                || extracted.trim().len() < 20
        };
        let content = if let Some(ref html) = msg.html_content {
            // Prefer the HTML body when the plain-text part looks like a
            // stripped stub (common on RSS) or the HTML has structural
            // content the text part can't represent — notably tables.
            let html_has_table = html.contains("<table") || html.contains("<TABLE");
            if is_html_fallback || html_has_table {
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
        // Normalise line endings before we hand the body off to any
        // text-splitting step. Some legacy clients emit CR-only line
        // endings (no LF) inside base64'd text/plain parts; if we leave
        // those `\r` bytes in, the terminal honours each one as a
        // carriage-return when the right pane prints, jumping the cursor
        // to column 1 and overwriting the left pane with body fragments.
        let content = normalize_line_endings(content);
        // Apple Mail's inline `<name.pdf>` attachment markers onto lines
        // of their own, before anything measures a line's width.
        let content = break_attachment_markers(&content);
        // Detect and render Markdown tables in-place with Unicode box
        // borders. Non-table text passes through untouched, so the
        // subsequent quote/signature coloring still works.
        let pane_w = (self.right.w as usize).saturating_sub(4).max(20);
        let content = crust::text::format_markdown_tables(&content, pane_w);
        // Collapse plain-text bracketed-link syntax `[anchor <URL>]` (with
        // the URL possibly wrapped to the next line) into OSC 8 links
        // showing only the anchor, so the pane isn't dominated by long
        // tracking URLs that mail clients inline-expand.
        let content = collapse_bracketed_links(&content);

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
            // Determine the outer block color first so color_emails can
            // restore it after each email-address span.
            let outer = if in_signature {
                Some(self.config.theme_colors.sig)
            } else if line.starts_with(">>>>") {
                Some(self.config.theme_colors.quote4)
            } else if line.starts_with(">>>") {
                Some(self.config.theme_colors.quote3)
            } else if line.starts_with(">>") {
                Some(self.config.theme_colors.quote2)
            } else if line.starts_with('>') {
                Some(self.config.theme_colors.quote1)
            } else {
                None
            };
            // Apply email coloring (177 with restore-to-outer) before
            // hyperlink_urls so the URL pass — which also stops at \x1b
            // bytes — doesn't span across the email's color escapes.
            let with_emails = highlight::color_emails(line, outer);
            let linked = hyperlink_urls(&with_emails);
            match outer {
                Some(c) => lines.push(style::fg(&linked, c)),
                None    => lines.push(linked),
            }
        }

        // Reset scroll only when viewing a different message; reuse the
        // earlier-computed `current_id`/`msg_changed` so we don't recompute
        // — we already need them for the body cache below.
        self.right_pane_msg_id = current_id;

        let rendered = lines.join("\n");
        // Stash for next render of the same message + content fingerprint.
        if msg.id != 0 {
            self.body_cache = Some((cache_key.0, cache_key.1, rendered.clone()));
        }
        self.right.set_text(&rendered);
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
                " q:Quit | ?:Help | =:All | N:New | 0-9:Views | a/A:Read | Space:Fold | t/T:Tag | s:Save | B:Browse | F:Fav",
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
            // Keep the top bar's [pos/total] in sync with the cursor.
            self.render_top_bar();
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
            // Keep the top bar's [pos/total] in sync with the cursor.
            self.render_top_bar();
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

    /// Try to select an unread message at display row `i` (threaded view).
    /// Returns true if it selected one (and may have expanded+rebuilt a
    /// collapsed thread — so the caller must NOT keep iterating after a
    /// `true`). A `false` return performs no mutation, so surrounding loop
    /// indices stay valid. `pick_last` lands on the last unread within an
    /// expanded section (for prev_unread) instead of the first.
    fn try_select_unread_at(&mut self, i: usize, pick_last: bool) -> bool {
        let (is_header, read, name) = {
            let Some(m) = self.display_messages.get(i) else { return false; };
            (m.is_header, m.read, m.thread_id.clone())
        };
        if !is_header {
            if !read { self.index = i; return true; }
            return false;
        }
        if read { return false; }
        let name = name.unwrap_or_default();
        let collapsed = self.section_collapsed.get(&name)
            .copied().unwrap_or(self.group_by_folder);
        // Expanded unread section: its unread rows are scanned on their own.
        if !collapsed { return false; }
        self.section_collapsed.insert(name.clone(), false);
        self.rebuild_display();
        if let Some(h) = self.display_messages.iter()
            .position(|m| m.is_header && m.thread_id.as_deref() == Some(name.as_str()))
        {
            let mut target = h; // fallback (header claimed unread)
            for j in (h + 1)..self.display_messages.len() {
                if self.display_messages[j].is_header { break; }
                if !self.display_messages[j].read {
                    target = j;
                    if !pick_last { break; }
                }
            }
            self.index = target;
        }
        true
    }

    fn next_unread(&mut self) {
        let info = self.config.theme_colors.feedback_info;
        if !self.show_threaded {
            let n = self.filtered_messages.len();
            for i in (self.index + 1)..n {
                if !self.filtered_messages[i].read { self.index = i; self.render_all(); return; }
            }
            for i in 0..self.index {
                if !self.filtered_messages[i].read {
                    self.index = i;
                    self.set_feedback("Wrapped to first unread", info);
                    self.render_all();
                    return;
                }
            }
            self.set_feedback("No unread messages in view", info);
            return;
        }
        // Threaded/folders: cursor indexes display_messages, and unread
        // inside a COLLAPSED thread isn't a row — try_select_unread_at
        // expands and dives in. Start on the current row if it's a header
        // (so the key dives into a collapsed unread thread under the cursor),
        // else just past the current message. Then wrap to the top.
        let len = self.display_messages.len();
        let on_header = self.display_messages.get(self.index)
            .map(|m| m.is_header).unwrap_or(false);
        let start = if on_header { self.index } else { self.index + 1 };
        for i in start..len {
            if self.try_select_unread_at(i, false) { self.render_all(); return; }
        }
        for i in 0..self.index {
            if self.try_select_unread_at(i, false) {
                self.set_feedback("Wrapped to first unread", info);
                self.render_all();
                return;
            }
        }
        self.set_feedback("No unread messages in view", info);
    }

    fn prev_unread(&mut self) {
        let info = self.config.theme_colors.feedback_info;
        if !self.show_threaded {
            for i in (0..self.index).rev() {
                if !self.filtered_messages[i].read { self.index = i; self.render_all(); return; }
            }
            for i in ((self.index + 1)..self.filtered_messages.len()).rev() {
                if !self.filtered_messages[i].read {
                    self.index = i;
                    self.set_feedback("Wrapped to last unread", info);
                    self.render_all();
                    return;
                }
            }
            self.set_feedback("No unread messages in view", info);
            return;
        }
        let len = self.display_messages.len();
        for i in (0..self.index).rev() {
            if self.try_select_unread_at(i, true) { self.render_all(); return; }
        }
        for i in ((self.index + 1)..len).rev() {
            if self.try_select_unread_at(i, true) {
                self.set_feedback("Wrapped to last unread", info);
                self.render_all();
                return;
            }
        }
        self.set_feedback("No unread messages in view", info);
    }
}

// --- View switching ---

impl App {
    fn switch_to_view(&mut self, key: &str) {
        log::info(&format!("Switch to view: {}", key));
        // Unbound view key → no-op. Without this guard, pressing F4..F12
        // (or any other key not in self.views) silently falls through
        // to `Filters::default()` which matches every message — i.e.
        // identical to View A. Stay on the current view instead and
        // surface a hint so the keystroke wasn't lost in silence.
        let is_builtin = matches!(key, "A" | "N" | "*");
        let is_defined = self.views.iter().any(|v| v.key_binding.as_deref() == Some(key));
        if !is_builtin && !is_defined {
            self.set_feedback(
                &format!("No view bound to {}", key),
                self.config.theme_colors.feedback_warn,
            );
            return;
        }
        self.current_view = key.to_string();
        // Recompute the inactive-view unread badges from the DB. By the time
        // you switch views, any mark-read done in the view you're leaving has
        // committed, so the view you just cleared correctly shows no badge.
        self.refresh_view_unread_cache();
        self.active_folder = None;
        self.in_source_view = false;
        self.index = 0;
        // Switching views always abandons any sticky search.
        self.active_search_filter = None;
        self.active_search_label.clear();

        // Restore per-view thread mode from DB settings
        let mode_key = format!("thread_mode_{}", key);
        match self.db.get_setting(&mode_key).as_deref() {
            Some("threaded") => { self.show_threaded = true; self.group_by_folder = false; }
            Some("folders") => { self.show_threaded = true; self.group_by_folder = true; }
            Some("flat") => { self.show_threaded = false; self.group_by_folder = false; }
            // Never set: thread it. A conversation read as one collapsible
            // block is what a mail view is for, and the mode key cycles
            // straight back to flat for anyone who wants the old list.
            _ => { self.show_threaded = true; self.group_by_folder = false; }
        }
        // Restore per-view manual section order so the Folders view
        // opens with channels in the order the user last arranged.
        self.current_section_order = self.load_section_order(key);
        self.current_hidden_channels = self.load_hidden_channels(key);

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
                        // Honors both `rules: [...]` (single AND
                        // group) and `branches: [{rules}, ...]` (OR
                        // across independent groups).
                        filters = parse_view_filters_json(&f);
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
            resolve_source_type(&self.source_type_map, msg);
        }
        self.sort_messages();
        self.rebuild_display();
        self.left.full_refresh();
        self.right.full_refresh();
        self.render_all();
    }

    /// Reload messages for current view without resetting cursor position.
    /// Remove messages whose deletion is queued but not yet committed, so a
    /// refresh racing the async writer can't resurrect them. Each id self-
    /// clears once the DB no longer returns it (the delete committed).
    fn drop_pending_deletes(&mut self) {
        if self.pending_deletes.is_empty() { return; }
        let present: HashSet<i64> = self.filtered_messages.iter().map(|m| m.id).collect();
        self.pending_deletes.retain(|id| present.contains(id));
        if !self.pending_deletes.is_empty() {
            self.filtered_messages.retain(|m| !self.pending_deletes.contains(&m.id));
        }
    }

    fn refresh_current_view(&mut self) {
        let saved_id = self.filtered_messages.get(self.index).map(|m| m.id);
        let saved_index = self.index;
        let old_ids: Vec<i64> = self.filtered_messages.iter().map(|m| m.id).collect();
        let old_read: Vec<bool> = self.filtered_messages.iter().map(|m| m.read).collect();

        // Sticky search filter wins over the current_view's rules. The
        // periodic poll fires every 5s and used to silently overwrite
        // search results — by re-running the saved filter here, the
        // search list keeps refreshing as new matching messages arrive.
        if let Some(ref f) = self.active_search_filter {
            let limit = self.config.load_limit;
            self.filtered_messages = self.db.get_messages(f, limit, 0);
            self.drop_pending_deletes();
            for msg in &mut self.filtered_messages {
                resolve_source_type(&self.source_type_map, msg);
            }
            // Newly arrived unread messages get put in unseen_ids so the
            // background refresh tick can't auto-mark them read just because
            // the cursor's index happens to land on the new row. The user
            // has to actually navigate to them (existing render-side logic
            // at line 1319-1322 clears the protection on navigate-away-and-back).
            let old_id_set: HashSet<i64> = old_ids.iter().copied().collect();
            for msg in &self.filtered_messages {
                if !msg.read && !old_id_set.contains(&msg.id) {
                    self.unseen_ids.insert(msg.id);
                }
            }
            // Best-effort cursor preservation: keep cursor on same id
            // if it survived; otherwise stay at the same index slot.
            if let Some(id) = saved_id {
                if let Some(pos) = self.filtered_messages.iter().position(|m| m.id == id) {
                    self.index = pos;
                } else {
                    self.index = saved_index.min(self.filtered_messages.len().saturating_sub(1));
                }
            }
            // Mute warnings about unread caches we don't use here.
            let _ = old_read;
            self.sort_messages();
            self.rebuild_display();
            self.left.full_refresh();
            self.suppress_automark_read = true;
            self.render_all();
            self.suppress_automark_read = false;
            return;
        }

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
                        filters = parse_view_filters_json(&f);
                    }
                }
            }
        }

        let limit = self.config.load_limit;
        self.filtered_messages = self.db.get_messages(&filters, limit, 0);
        self.drop_pending_deletes();
        for msg in &mut self.filtered_messages {
            resolve_source_type(&self.source_type_map, msg);
        }
        // Only re-sort when the id set actually changed (new messages
        // arrived or some were purged). If only read-state flipped, keep
        // the previous in-memory order — otherwise the "unread" sort
        // mode shuffles the just-read message down and the cursor ends
        // up on a different row than the user last navigated to.
        let old_id_set: std::collections::HashSet<i64> = old_ids.iter().copied().collect();
        let new_id_set: std::collections::HashSet<i64> =
            self.filtered_messages.iter().map(|m| m.id).collect();
        // Newly arrived unread messages get added to unseen_ids so the
        // background refresh tick can't auto-mark them read just because
        // the cursor's index happens to land on the new row. Existing
        // render-side logic at line 1319-1322 clears the protection
        // when the user explicitly navigates away and back.
        for msg in &self.filtered_messages {
            if !msg.read && !old_id_set.contains(&msg.id) {
                self.unseen_ids.insert(msg.id);
            }
        }
        if old_id_set == new_id_set && !old_ids.is_empty() {
            let pos: std::collections::HashMap<i64, usize> =
                old_ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
            self.filtered_messages.sort_by_key(|m|
                pos.get(&m.id).copied().unwrap_or(usize::MAX));
        } else {
            self.sort_messages();
        }
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

        // The restores above clamp to filtered_messages, but in threaded
        // mode self.index tracks display_messages (shorter when sections
        // are collapsed). Clamp to the active list so the cursor can't be
        // left past the end — that out-of-range index is what crashed
        // Ctrl+Space (toggle_collapse_all) after a background refresh.
        let active_len = if self.show_threaded {
            self.display_messages.len()
        } else {
            self.filtered_messages.len()
        };
        if self.index >= active_len {
            self.index = active_len.saturating_sub(1);
        }

        // Skip render if nothing changed (avoids flicker on periodic refresh)
        let new_ids: Vec<i64> = self.filtered_messages.iter().map(|m| m.id).collect();
        let new_read: Vec<bool> = self.filtered_messages.iter().map(|m| m.read).collect();
        if new_ids == old_ids && new_read == old_read {
            return;
        }

        self.suppress_automark_read = true;
        self.render_all();
        self.suppress_automark_read = false;
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
        // Browsing a specific folder via B / F is always a flat,
        // chronological list of mails in that folder — no per-channel
        // sections, no threading rail. Folders / threaded modes only
        // make sense in the combined Views (3, 4) where multiple
        // source folders contribute. Reset here so a previous
        // Views-mode setting doesn't leak in.
        self.show_threaded = false;
        self.group_by_folder = false;

        self.set_feedback(
            &format!("Loading {}...", folder),
            self.config.theme_colors.unread,
        );

        let mut filters = Filters::default();
        filters.folder = Some(folder.to_string());
        self.filtered_messages = self.db.get_messages(&filters, 500, 0);
        for msg in &mut self.filtered_messages {
            resolve_source_type(&self.source_type_map, msg);
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
        let mut sections = if self.group_by_folder {
            organizer::organize_by_folder(&self.filtered_messages, self.sort_inverted, &self.current_section_order)
        } else {
            organizer::organize_messages(&self.filtered_messages, &self.sort_order, self.sort_inverted)
        };

        // Folders mode: merge in every subscribed weechat buffer that
        // matches the current view filter, so empty channels still
        // render as sections (weechat-buflist parity). Then drop any
        // section the user has explicitly hidden for this view.
        //
        // Skip the merge entirely when the user is browsing a specific
        // maildir folder via B / F — that's a pure mail folder view,
        // and chat channels would never have messages in it. Without
        // this guard, every subscribed channel renders as a `[0]`
        // section alongside the mail folder.
        if self.group_by_folder && self.active_folder.is_none() {
            let view_filters = self.build_current_filters();
            // Merge in every subscribed weechat buffer this view admits, so
            // empty channels still render as sections (weechat-buflist
            // parity). Admission is decided per buffer by
            // `buffer_admitted_by_filter`, which (unlike the old
            // `folder_matches_filter` gate) is source-aware: a branch that
            // carries a non-weechat `source_id` (e.g. Workspace source_id=7)
            // and no folder filter no longer matches every folder, so IRC /
            // Slack buffers stop leaking into a view once it grows a
            // non-weechat source branch.
            let have_folder: std::collections::HashSet<String> = sections.iter()
                .map(|s| s.name.clone()).collect();
            let bufs = self.subscribed_buffers.lock().unwrap().clone();
            for buf in bufs {
                if have_folder.contains(&buf.full_name) { continue; }
                if !buffer_admitted_by_filter(&buf.full_name, &view_filters, &self.source_type_map) { continue; }
                sections.push(organizer::Section {
                    section_type: "channel".to_string(),
                    display_name: organizer::pretty_folder_name_public(&buf.full_name),
                    name: buf.full_name,
                    source_type: "folder".to_string(),
                    messages: Vec::new(),
                    unread_count: 0,
                });
            }
            // Re-sort: pinned channels first (in section_order), then
            // the rest by latest message timestamp. Empty sections
            // have no messages so they fall to the bottom of the
            // unpinned tier.
            let pin_rank: std::collections::HashMap<&str, usize> = self.current_section_order.iter()
                .enumerate().map(|(i, s)| (s.as_str(), i)).collect();
            let filtered = &self.filtered_messages;
            // When the view asks to sort by source (e.g. the phone gateway
            // view), group the unpinned tier by platform first, then by
            // latest activity within each platform.
            let by_source = self.sort_order == "source";
            // For a branches view, group sections by which branch they belong
            // to, in branch order, so a multi-source view (e.g. Dualog: mail,
            // then Workspace, then Slack) stays cleanly grouped instead of
            // interleaving channels by recency. A section's branch is the
            // first one whose source + folder dimensions admit it (matched on
            // the source_id of its newest message); pins and recency then
            // order within each group, and new channels land in their own
            // branch's group automatically.
            let view_branches = view_filters.branches.clone();
            let stmap = &self.source_type_map;
            let branch_idx = |s: &organizer::Section| -> usize {
                let Some(bs) = view_branches.as_ref() else { return 0 };
                let sid = s.messages.iter()
                    .max_by_key(|&&i| filtered[i].timestamp)
                    .map(|&i| filtered[i].source_id);
                bs.iter().position(|b| section_in_branch(&s.name, sid, b, stmap)).unwrap_or(bs.len())
            };
            sections.sort_by(|a, b| {
                let gia = branch_idx(a);
                let gib = branch_idx(b);
                if gia != gib { return gia.cmp(&gib); }
                let ra = pin_rank.get(a.name.as_str()).copied();
                let rb = pin_rank.get(b.name.as_str()).copied();
                match (ra, rb) {
                    (Some(ia), Some(ib)) => ia.cmp(&ib),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => {
                        let la = a.messages.iter().map(|&i| filtered[i].timestamp).max().unwrap_or(0);
                        let lb = b.messages.iter().map(|&i| filtered[i].timestamp).max().unwrap_or(0);
                        if by_source {
                            a.source_type.cmp(&b.source_type).then(lb.cmp(&la))
                        } else {
                            lb.cmp(&la)
                        }
                    }
                }
            });
            // Apply the mute list. A muted channel stays hidden unless it has
            // an UNREAD message *newer* than its mute timestamp matching its
            // mode (any message for UntilNew, a highlight/mention for
            // UntilHighlight). So it resurfaces on the right new activity and
            // re-hides on its own once the user has read it — no manual
            // unmute+remute. (`mute_recheck_pending` triggers the rebuild that
            // applies this once the cursor leaves the channel.)
            if !self.show_muted && !self.current_hidden_channels.is_empty() {
                let muted = &self.current_hidden_channels;
                sections.retain(|s| {
                    match muted.iter().find(|h| h.name == s.name) {
                        None => true,
                        Some(h) => s.messages.iter()
                            .filter_map(|&i| filtered.get(i))
                            .any(|m| !m.read && m.timestamp > h.hidden_at && match h.mode {
                                HideMode::UntilNew => true,
                                HideMode::UntilHighlight => m.metadata
                                    .get("highlight").and_then(|v| v.as_bool()) == Some(true),
                            }),
                    }
                });
            }
        }
        let sections = sections;

        self.display_messages.clear();
        for section in &sections {
            // In Folders mode, default to COLLAPSED so the left pane
            // looks like weechat's buflist (one row per channel). User
            // toggles persist in section_collapsed and override the
            // default for that section.
            let is_collapsed = self.section_collapsed.get(&section.name).copied()
                .unwrap_or(self.group_by_folder);
            let mut header = Message::default_header();
            header.subject = Some(section.display_name.clone());
            // Count UNREAD highlights inside the section so the header
            // can show `!` instead of `*` when at least one mention is
            // waiting. metadata.`highlight` is set by the weechat-relay
            // source when the relay flagged the line for the user.
            let highlight_count: u64 = section.messages.iter()
                .filter_map(|&i| self.filtered_messages.get(i))
                .filter(|m| !m.read
                    && m.metadata.get("highlight").and_then(|v| v.as_bool()) == Some(true))
                .count() as u64;
            header.metadata = serde_json::json!({ "highlight_count": highlight_count });
            // Compact `[N/M]` counter so a wall of collapsed channels
            // stays readable. The `*` unread marker on the right is
            // appended by `format_section_header`.
            header.content = if section.unread_count > 0 {
                format!("{}/{}", section.unread_count, section.messages.len())
            } else {
                format!("{}", section.messages.len())
            };
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
                // For email/maildir thread sections, replace the flat
                // chronological ordering with a DFS over the
                // In-Reply-To tree so replies appear indented under
                // the parent they answered. Other section types stay
                // flat — chat sources don't carry reliable parent
                // metadata in the current relay protocol.
                let ordered: Vec<(usize, u8)> = if section.section_type == "thread" {
                    build_thread_order(&self.filtered_messages, &section.messages)
                } else {
                    section.messages.iter().map(|&i| (i, 0u8)).collect()
                };

                for (idx, depth) in ordered {
                    let src = &self.filtered_messages[idx];
                    self.display_messages.push(Message {
                        id: src.id,
                        source_id: src.source_id,
                        external_id: src.external_id.clone(),
                        thread_id: src.thread_id.clone(),
                        parent_id: src.parent_id,
                        sender: src.sender.clone(),
                        sender_name: src.sender_name.clone(),
                        recipients: src.recipients.clone(),
                        cc: src.cc.clone(),
                        bcc: src.bcc.clone(),
                        subject: src.subject.clone(),
                        content: String::new(),
                        html_content: None,
                        timestamp: src.timestamp,
                        received_at: src.received_at,
                        read: src.read,
                        starred: src.starred,
                        archived: src.archived,
                        labels: src.labels.clone(),
                        attachments: src.attachments.clone(),
                        metadata: serde_json::Value::Null,
                        folder: src.folder.clone(),
                        replied: src.replied,
                        source_type: src.source_type.clone(),
                        is_header: false,
                        full_loaded: false,
                        thread_depth: depth,
                    });
                }
            }
        }
    }

    fn toggle_collapse(&mut self) {
        if !self.show_threaded { return; }
        // If cursor is on a child message, walk back to find its section
        // header and collapse THAT — the user's intent when Space-folding
        // from inside an expanded section. Also move the cursor to the
        // header so it's visible after the collapse.
        let (header_ix, section_name) = {
            let Some(current) = self.display_messages.get(self.index) else { return };
            if current.is_header {
                (self.index, current.thread_id.clone())
            } else {
                let mut ix = self.index;
                while ix > 0 && !self.display_messages[ix].is_header {
                    ix -= 1;
                }
                let name = self.display_messages.get(ix)
                    .filter(|m| m.is_header)
                    .and_then(|m| m.thread_id.clone());
                (ix, name)
            }
        };
        let Some(name) = section_name else { return };
        let was_on_child = self.index != header_ix;
        // When invoked from a child, we want to collapse (not toggle) so a
        // subsequent Space doesn't re-expand the section we just collapsed.
        // When on the header itself, toggle (expand if collapsed).
        if was_on_child {
            self.section_collapsed.insert(name, true);
        } else {
            // First touch of a never-toggled section: pretend it was at
            // the current mode's default so the toggle flips correctly
            // (collapsed → expanded in Folders mode, expanded → collapsed
            // everywhere else).
            let default_collapsed = self.group_by_folder;
            let collapsed = self.section_collapsed.entry(name).or_insert(default_collapsed);
            *collapsed = !*collapsed;
        }
        self.index = header_ix;
        self.rebuild_display();
        // Re-find the header post-rebuild in case indices shifted.
        if self.index >= self.display_messages.len() {
            self.index = self.display_messages.len().saturating_sub(1);
        }
        self.render_all();
    }

    /// Put every thread away and open only the one holding `id`, cursor on
    /// that message. What a search hit should look like: the conversation
    /// in context, the matching mail selected inside it.
    fn reveal_in_threads(&mut self, id: i64) {
        if !self.show_threaded { return; }
        // Which section holds it — read off the expanded layout, because a
        // collapsed one no longer lists its messages.
        let mut want: Option<String> = None;
        for m in &self.display_messages {
            if m.is_header { want = m.thread_id.clone(); }
            else if m.id == id { break; }
        }
        let Some(want) = want else { return };
        let names: Vec<String> = self.display_messages.iter()
            .filter(|m| m.is_header)
            .filter_map(|m| m.thread_id.clone())
            .collect();
        for n in names { self.section_collapsed.insert(n, true); }
        self.section_collapsed.insert(want, false);
        self.rebuild_display();
        self.index = self.display_messages.iter()
            .position(|m| !m.is_header && m.id == id)
            .unwrap_or(0);
    }

    /// Toggle collapse on every section in the threaded display. If
    /// any section is currently expanded, this collapses them all.
    /// If they're already all collapsed, it expands them. Cursor lands
    /// on the section header it started inside so the view doesn't
    /// teleport.
    fn toggle_collapse_all(&mut self) {
        if !self.show_threaded { return; }
        // Collect the unique section names in display order.
        let names: Vec<String> = self.display_messages.iter()
            .filter(|m| m.is_header)
            .filter_map(|m| m.thread_id.clone())
            .collect();
        if names.is_empty() { return; }
        // If every section is already collapsed, expand them all.
        // Otherwise collapse the lot.
        let all_collapsed = names.iter()
            .all(|n| *self.section_collapsed.get(n).unwrap_or(&false));
        let target = !all_collapsed;
        for n in &names {
            self.section_collapsed.insert(n.clone(), target);
        }
        // Snap the cursor to the section it was in so a follow-up
        // expand puts the user back where they were.
        let cursor_section: Option<String> = {
            // Clamp before indexing: a background refresh can leave
            // self.index past the end of display_messages in threaded
            // mode (it tracks display_messages, but refresh clamps to
            // filtered_messages). Without this, the [ix] below panics.
            let mut ix = self.index.min(self.display_messages.len().saturating_sub(1));
            while ix > 0 && !self.display_messages[ix].is_header { ix -= 1; }
            self.display_messages.get(ix)
                .filter(|m| m.is_header)
                .and_then(|m| m.thread_id.clone())
        };
        self.rebuild_display();
        if self.index >= self.display_messages.len() {
            self.index = self.display_messages.len().saturating_sub(1);
        }
        if let Some(name) = cursor_section {
            if let Some(pos) = self.display_messages.iter()
                .position(|m| m.is_header && m.thread_id.as_deref() == Some(name.as_str()))
            {
                self.index = pos;
            }
        }
        let n = names.len();
        let msg = if target {
            format!("Collapsed {} sections", n)
        } else {
            format!("Expanded {} sections", n)
        };
        self.set_feedback(&msg, self.config.theme_colors.feedback_info);
        self.render_all();
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

    /// Expand the section the cursor is currently on. Counterpart of
    /// `collapse_current` — bound to `l`/Right for vim-style hjkl
    /// navigation in Folders mode.
    fn expand_current(&mut self) {
        if !self.show_threaded { return; }
        let Some(msg) = self.display_messages.get(self.index) else { return };
        if !msg.is_header { return; }
        let Some(name) = msg.thread_id.clone() else { return };
        self.section_collapsed.insert(name, false);
        self.rebuild_display();
        self.render_all();
    }

    /// Load the per-view list of channels to hide from the Folders
    /// view. Persisted as `hidden_channels_<key>` JSON array.
    fn load_hidden_channels(&self, view_key: &str) -> Vec<HiddenChannel> {
        let setting_key = format!("hidden_channels_{}", view_key);
        let Some(raw) = self.db.get_setting(&setting_key) else { return Vec::new() };
        let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) else { return Vec::new() };
        let Some(arr) = val.as_array() else { return Vec::new() };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64).unwrap_or(0);
        arr.iter().filter_map(|e| {
            // Legacy format: bare "name" string (old permanent hide).
            // Migrate to UntilNew muted as of load time, so it stays
            // hidden until fresh activity rather than vanishing forever.
            if let Some(name) = e.as_str() {
                return Some(HiddenChannel { name: name.to_string(), mode: HideMode::UntilNew, hidden_at: now });
            }
            let name = e.get("name").and_then(|v| v.as_str())?.to_string();
            let mode = HideMode::from_str(e.get("mode").and_then(|v| v.as_str()).unwrap_or("new"));
            let hidden_at = e.get("at").and_then(|v| v.as_i64()).unwrap_or(now);
            Some(HiddenChannel { name, mode, hidden_at })
        }).collect()
    }

    fn save_hidden_channels(&self) {
        let setting_key = format!("hidden_channels_{}", self.current_view);
        let arr: Vec<serde_json::Value> = self.current_hidden_channels.iter()
            .map(|h| serde_json::json!({ "name": h.name, "mode": h.mode.as_str(), "at": h.hidden_at }))
            .collect();
        let json = serde_json::to_string(&serde_json::Value::Array(arr)).unwrap_or_else(|_| "[]".into());
        self.db.set_setting(&setting_key, &json);
    }

    /// Mute the channel under the cursor. `m` → `UntilNew` (resurface
    /// on any new message), `M` → `UntilHighlight` (resurface only on a
    /// mention/highlight). Pressing the SAME mode key on an already-
    /// muted channel un-mutes it; the OTHER mode key switches the mode
    /// (and re-stamps the mute time). Persists per view. Ctrl+U toggles a
    /// non-destructive peek that shows muted channels alongside the rest.
    fn hide_current_channel(&mut self, mode: HideMode) {
        if !self.group_by_folder {
            self.set_feedback("Mute works in the channel (folders) view",
                self.config.theme_colors.feedback_info);
            return;
        }
        let Some(msg) = self.display_messages.get(self.index) else { return };
        if !msg.is_header {
            self.set_feedback("Put the cursor on a channel header to mute it",
                self.config.theme_colors.feedback_info);
            return;
        }
        let Some(section_name) = msg.thread_id.clone() else { return };
        let label = section_name.rsplit_once('.').map(|(_, c)| c).unwrap_or(&section_name).to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64).unwrap_or(0);
        let feedback = match self.current_hidden_channels.iter_mut().find(|h| h.name == section_name) {
            Some(existing) if existing.mode == mode => {
                // Same key again → un-mute.
                self.current_hidden_channels.retain(|h| h.name != section_name);
                format!("Unmuted {}", label)
            }
            Some(existing) => {
                // Other key → switch mode, re-stamp.
                existing.mode = mode;
                existing.hidden_at = now;
                match mode {
                    HideMode::UntilNew => format!("Muted {} until new message", label),
                    HideMode::UntilHighlight => format!("Muted {} until mention", label),
                }
            }
            None => {
                self.current_hidden_channels.push(HiddenChannel {
                    name: section_name.clone(), mode, hidden_at: now,
                });
                match mode {
                    HideMode::UntilNew => format!("Muted {} until new message", label),
                    HideMode::UntilHighlight => format!("Muted {} until mention", label),
                }
            }
        };
        self.save_hidden_channels();
        self.set_feedback(&feedback, self.config.theme_colors.feedback_ok);
        self.rebuild_display();
        if self.index >= self.display_messages.len() {
            self.index = self.display_messages.len().saturating_sub(1);
        }
        self.render_all();
    }

    /// Un-mute every channel in this view. Bound to Ctrl+U.
    /// Ctrl+U — toggle between "all channels" (muted included, shown with a
    /// dim muted tag) and the normal unmuted-only view. Non-destructive: the
    /// mute list is untouched; to actually unmute one, peek with Ctrl+U and
    /// press `m` on its header.
    fn toggle_show_muted(&mut self) {
        if !self.group_by_folder { return; }
        self.show_muted = !self.show_muted;
        let n = self.current_hidden_channels.len();
        if self.show_muted {
            self.set_feedback(&format!("Showing all channels ({} muted)", n),
                self.config.theme_colors.feedback_info);
        } else {
            self.set_feedback("Showing unmuted channels",
                self.config.theme_colors.feedback_info);
        }
        self.rebuild_display();
        self.render_all();
    }

    /// Folder (section) name the cursor currently sits in, or None.
    /// Folders view is always threaded, so the cursor indexes
    /// `display_messages`; a header carries its section name in `thread_id`,
    /// a message carries it in `folder`.
    fn cursor_section_name(&self) -> Option<String> {
        let m = self.display_messages.get(self.index)?;
        if m.is_header { m.thread_id.clone() } else { m.folder.clone() }
    }

    /// Re-hide a muted channel that the user just caught up on, once the
    /// cursor has left it. Armed by the auto-mark-read path; called by the
    /// main loop after each key. Deferring until the cursor leaves keeps the
    /// message being read from vanishing under the cursor. Self-corrects if
    /// the channel was unmuted or the view changed in the meantime.
    fn honor_pending_mute_rehide(&mut self) {
        let Some(ch) = self.mute_recheck_pending.clone() else { return };
        if !self.group_by_folder
            || !self.current_hidden_channels.iter().any(|h| h.name == ch) {
            self.mute_recheck_pending = None;
            return;
        }
        // Still inside the channel — wait until the user moves off it.
        if self.cursor_section_name().as_deref() == Some(ch.as_str()) {
            return;
        }
        self.mute_recheck_pending = None;
        // Preserve the cursor on the same row by id: hiding the channel
        // removes rows, so the bare index would point at a different message.
        // Headers carry id 0, so only restore real messages by id; otherwise
        // fall back to the clamped index below.
        let saved_id = self.display_messages.get(self.index)
            .filter(|m| !m.is_header && m.id > 0).map(|m| m.id);
        self.rebuild_display();
        if let Some(id) = saved_id {
            if let Some(pos) = self.display_messages.iter().position(|m| m.id == id) {
                self.index = pos;
            }
        }
        if self.index >= self.display_messages.len() {
            self.index = self.display_messages.len().saturating_sub(1);
        }
        self.render_message_list();
        self.render_top_bar();
    }

    /// Load the user's hand-pinned section order for `view_key`. Stored
    /// as `section_order_<key>` in the settings table as a JSON array
    /// of folder names. Empty list when nothing's been pinned yet.
    fn load_section_order(&self, view_key: &str) -> Vec<String> {
        let setting_key = format!("section_order_{}", view_key);
        let Some(raw) = self.db.get_setting(&setting_key) else { return Vec::new() };
        serde_json::from_str::<Vec<String>>(&raw).unwrap_or_default()
    }

    /// Persist the current section order for the current view.
    fn save_section_order(&self) {
        let setting_key = format!("section_order_{}", self.current_view);
        let json = serde_json::to_string(&self.current_section_order).unwrap_or_else(|_| "[]".into());
        self.db.set_setting(&setting_key, &json);
    }

    /// Discard the manual section order for the current view so
    /// channels fall back to the default "latest message first" sort.
    /// Bound to Ctrl+Home. Only meaningful in Folder-grouped view.
    fn reset_section_order(&mut self) {
        if !self.group_by_folder { return; }
        if self.current_section_order.is_empty() {
            self.set_feedback(
                "No manual order set for this view",
                self.config.theme_colors.feedback_info,
            );
            return;
        }
        self.current_section_order.clear();
        self.save_section_order();
        self.rebuild_display();
        self.set_feedback(
            "Section order reset to latest-first",
            self.config.theme_colors.feedback_ok,
        );
        self.render_all();
    }

    /// Move the section under the cursor one slot up (delta=-1) or
    /// down (delta=+1). Bound to Ctrl+Up / Ctrl+Down on a section
    /// header. Persists the new order to settings. Only works in
    /// Folder-grouped view — sections in Threaded view are derived
    /// from subjects and don't have a stable folder identifier.
    fn move_section(&mut self, delta: i32) {
        if !self.group_by_folder { return; }
        let Some(msg) = self.display_messages.get(self.index) else { return };
        if !msg.is_header { return; }
        let Some(section_name) = msg.thread_id.clone() else { return };

        // Build the list of currently-visible section names in their
        // current display order (used as the baseline for an order
        // that has never been pinned).
        let visible: Vec<String> = self.display_messages.iter()
            .filter(|m| m.is_header)
            .filter_map(|m| m.thread_id.clone())
            .collect();

        // Start from the persisted order, then append any visible
        // section that isn't already in it. This lets a brand-new
        // channel be moved up without having to touch every other
        // channel first.
        let mut order = self.current_section_order.clone();
        for name in &visible {
            if !order.contains(name) { order.push(name.clone()); }
        }

        let Some(pos) = order.iter().position(|n| n == &section_name) else { return };
        let new_pos = pos as i32 + delta;
        if new_pos < 0 || new_pos as usize >= order.len() { return; }
        order.swap(pos, new_pos as usize);

        self.current_section_order = order;
        self.save_section_order();
        self.rebuild_display();

        // Follow the moved section: find its new index in display_messages.
        for (i, m) in self.display_messages.iter().enumerate() {
            if m.is_header && m.thread_id.as_deref() == Some(section_name.as_str()) {
                self.index = i;
                break;
            }
        }
        self.render_all();
    }

    /// Mark every message in the section under the cursor as read.
    /// Section is the channel-grouped header in Folders mode, or the
    /// thread/subject group in Threaded mode. Scopes the existing
    /// MarkAllReadBulk path by adding the section's folder to the
    /// view filter.
    fn mark_section_read(&mut self) {
        if !self.show_threaded { return; }
        // Walk back from cursor to the section header.
        let mut ix = self.index;
        while ix > 0 && !self.display_messages[ix].is_header { ix -= 1; }
        let Some(header) = self.display_messages.get(ix).filter(|m| m.is_header) else { return };
        let label = header.thread_id.clone()
            .map(|s| s.rsplit_once('.').map(|(_, c)| c.to_string()).unwrap_or(s))
            .unwrap_or_else(|| "section".to_string());

        // Collect the unread message ids in this display section
        // (header+1 until the next header). Works in any threaded
        // view — folder-grouped or thread/subject-grouped — because
        // it operates on what's actually on screen, not a SQL folder
        // predicate. (The old folder-filter path bailed in non-folder
        // threaded views like RSS, so `a` did nothing there.)
        let mut ids: Vec<i64> = Vec::new();
        let mut j = ix + 1;
        while j < self.display_messages.len() && !self.display_messages[j].is_header {
            let m = &self.display_messages[j];
            if !m.read { ids.push(m.id); }
            j += 1;
        }
        if ids.is_empty() {
            self.set_feedback(
                &format!("Nothing unread in {}", label),
                self.config.theme_colors.feedback_warn,
            );
            return;
        }
        let n = ids.len();
        let idset: std::collections::HashSet<i64> = ids.iter().copied().collect();
        let _ = self.write_tx.send(DbWriteOp::MarkReadByIds(ids));
        // Flip BOTH stores: filtered_messages is canonical, but the
        // threaded view renders from display_messages.
        for msg in &mut self.filtered_messages {
            if idset.contains(&msg.id) { msg.read = true; }
        }
        for msg in &mut self.display_messages {
            if idset.contains(&msg.id) { msg.read = true; }
        }
        self.set_feedback(
            &format!("Marked {} as read in {}", n, label),
            self.config.theme_colors.feedback_ok,
        );
        self.sync_mail_count();
        self.render_all();
    }
}

// --- Message operations ---

impl App {
    /// Rewrite the asmite count file from current DB state. No-op when
    /// `~/.gmail.conf` doesn't expose `$mailfile`/`$mailboxes`. Called
    /// after every synchronous read-state mutation; the async writer
    /// thread does the same after `MarkRead`/`MarkUnread`/`Delete`/
    /// `UpdateFolder` so both paths stay in sync.
    fn sync_mail_count(&self) {
        if let Some(ref cfg) = self.mailfile_cfg {
            // unread_count_by_folder (WHERE read=0, index-backed) — NOT
            // all_folder_counts, whose unfiltered full-table scan of the
            // multi-GB DB stalled this (main-thread) call in folio_wait.
            let counts = self.db.unread_count_by_folder();
            mailfile::write_count_file(cfg, &counts);
        }
    }

    fn open_message(&mut self) {
        let mut became_read = false;
        if self.show_threaded {
            if let Some(msg) = self.display_messages.get_mut(self.index) {
                if msg.is_header { return; }
                self.browsed_ids.insert(msg.id);
                if !msg.read {
                    self.db.mark_as_read(msg.id);
                    msg.read = true;
                    became_read = true;
                    // Pair the maildir move new/→cur/+Seen with the read
                    // flip, like the auto-read path does. Without this the
                    // file lingers in new/ and gmail-idle keeps counting it,
                    // so the asmite shows phantom unread until next restart.
                    let _ = self.write_tx.send(DbWriteOp::SyncMaildirFlag(msg.metadata.clone(), msg.id));
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
                became_read = true;
                // Pair the maildir move with the read flip (see above).
                let _ = self.write_tx.send(DbWriteOp::SyncMaildirFlag(msg.metadata.clone(), msg.id));
            }
            if !msg.full_loaded {
                if let Some((content, html)) = self.db.get_message_content(msg.id) {
                    msg.content = content;
                    msg.html_content = html;
                    msg.full_loaded = true;
                }
            }
        }
        if became_read { self.sync_mail_count(); }
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
            // When marking read, pair the maildir move new/→cur/+Seen so
            // the asmite (gmail-idle) count clears too. No-op for non-maildir
            // (chat) messages — rename_maildir_add_seen returns None.
            if new_state {
                let to_sync: Vec<(serde_json::Value, i64)> = self.filtered_messages.iter()
                    .filter(|m| tagged_set.contains(&m.id))
                    .map(|m| (m.metadata.clone(), m.id))
                    .collect();
                for (meta, id) in to_sync {
                    let _ = self.write_tx.send(DbWriteOp::SyncMaildirFlag(meta, id));
                }
            }
            let label = if new_state { "read" } else { "unread" };
            self.set_feedback(
                &format!("Marked {} tagged as {}", tagged_ids.len(), label),
                self.config.theme_colors.feedback_ok);
            self.sync_mail_count();
            self.render_all();
            return;
        }
        if let Some(msg) = self.filtered_messages.get_mut(self.index) {
            let new_state = self.db.toggle_read(msg.id);
            msg.read = new_state;
            if new_state {
                let _ = self.write_tx.send(DbWriteOp::SyncMaildirFlag(msg.metadata.clone(), msg.id));
            }
            self.sync_mail_count();
            self.render_all();
        }
    }

    fn mark_all_read(&mut self) {
        let okcol = self.config.theme_colors.feedback_ok;
        let warncol = self.config.theme_colors.feedback_warn;

        // Scope strictly to what's visible in the current view. Using
        // `filtered_messages` as the source of truth means search,
        // conversation grouping, ad-hoc tagging, and other in-memory
        // narrowings are all respected — no chance of "A" reaching a
        // message that isn't on screen.
        let ids: Vec<i64> = self.filtered_messages.iter()
            .filter(|m| !m.read)
            .map(|m| m.id)
            .collect();
        if ids.is_empty() {
            self.set_feedback("Nothing unread in this view", warncol);
            return;
        }
        let n = ids.len();

        // Writer thread does the SQL UPDATE, the maildir-flag rename,
        // and the bulk metadata bump in one shot. Main thread flips the
        // in-memory rows immediately so the UI reflects "read" without
        // waiting on the writer.
        let _ = self.write_tx.send(DbWriteOp::MarkReadByIds(ids));
        for msg in &mut self.filtered_messages {
            msg.read = true;
        }
        // Threaded view renders from display_messages (clones), so the
        // unread markers won't clear until these are updated too —
        // otherwise the `N` flags linger until the next rebuild (which
        // a cursor move triggers, hence "only clears when I move").
        for msg in &mut self.display_messages {
            msg.read = true;
        }
        self.set_feedback(&format!("Marked {} as read", n), okcol);
        self.sync_mail_count();
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
        // Threaded views (e.g. View 4) index `display_messages`, which
        // includes synthetic section headers; flat views index
        // `filtered_messages` directly. Reading filtered_messages by
        // self.index unconditionally tagged the wrong row (or nothing)
        // in threaded views. Resolve the real id from the right list
        // and skip header rows.
        let len = if self.show_threaded {
            self.display_messages.len()
        } else {
            self.filtered_messages.len()
        };
        let (selected_id, is_header) = if self.show_threaded {
            match self.display_messages.get(self.index) {
                Some(m) if m.is_header => (None, true),
                Some(m) => (Some(m.id), false),
                None => (None, false),
            }
        } else {
            (self.filtered_messages.get(self.index).map(|m| m.id), false)
        };
        if let Some(id) = selected_id {
            if !is_header {
                if self.tagged.contains(&id) {
                    self.tagged.remove(&id);
                } else {
                    self.tagged.insert(id);
                }
            }
        }
        // Advance past the current row (real or header) so repeated `t`
        // walks the list, matching flat-view behaviour.
        if self.index + 1 < len {
            self.index += 1;
        }
        self.render_all();
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
            return;
        }
        // In threaded view, the cursor indexes display_messages (which
        // includes synthetic section headers). Resolve the real message
        // from the right list, and skip section headers — they aren't
        // real rows and can't be deleted.
        let (selected_id, is_header, at_end) = if self.show_threaded {
            match self.display_messages.get(self.index) {
                Some(m) if m.is_header => (None, true, false),
                Some(m) => (Some(m.id), false, self.index + 1 >= self.display_messages.len()),
                None => (None, false, false),
            }
        } else {
            match self.filtered_messages.get(self.index) {
                Some(m) => (Some(m.id), false, self.index + 1 >= self.filtered_messages.len()),
                None => (None, false, false),
            }
        };
        if is_header {
            self.set_feedback("Cannot delete a section header",
                self.config.theme_colors.feedback_warn);
            return;
        }
        let Some(id) = selected_id else { return };
        if self.delete_marked.contains(&id) {
            self.delete_marked.remove(&id);
        } else {
            self.delete_marked.insert(id);
        }
        if !at_end { self.index += 1; }
        self.render_all();
    }

    fn purge_deleted(&mut self) {
        if self.delete_marked.is_empty() { return; }

        // Phase clock for the freeze watchdog (logged at the end, only
        // when the purge was slow enough for the user to feel it).
        let t_start = std::time::Instant::now();
        let ids: Vec<i64> = self.delete_marked.iter().copied().collect();
        let id_set: std::collections::HashSet<i64> = ids.iter().copied().collect();

        // Pre-build id→msg lookup so the per-id scan is O(1) instead of O(N).
        // For 14 deletes × 20 000 messages this drops 280 K compares to a
        // single linear scan + HashMap lookups.
        let mut id_to_msg: std::collections::HashMap<i64, &Message> =
            std::collections::HashMap::with_capacity(ids.len());
        for m in &self.filtered_messages {
            if id_set.contains(&m.id) { id_to_msg.insert(m.id, m); }
        }

        // Cache `read_dir` results per parent directory. The slow path
        // (filename's flag suffix changed since metadata was captured)
        // previously walked the entire maildir per ID. Now: at most one
        // scan per unique directory across the whole purge, and entries
        // are looked up by base-name in a HashMap.
        let mut dir_cache: std::collections::HashMap<
            std::path::PathBuf,
            std::collections::HashMap<String, std::path::PathBuf>,
        > = std::collections::HashMap::new();

        for &id in &ids {
            // Fast path is the current view; fall back to the DB when the
            // id isn't in it. A marked message can drop out of
            // `filtered_messages` before the purge runs (view switch,
            // search/filter change, auto-refresh), and skipping it here
            // deleted the DB row while leaving the maildir file on disk
            // forever — the row's external_id then lands in
            // deleted_external_ids, so the poller never re-ingests it and
            // the orphan is invisible to kastrup but still on disk.
            let file = match id_to_msg.get(&id) {
                Some(m) => m.metadata.get("maildir_file")
                    .and_then(|v| v.as_str()).map(str::to_string),
                None => self.db.get_message_metadata(id).and_then(|m| {
                    m.get("maildir_file")
                        .and_then(|v| v.as_str()).map(str::to_string)
                }),
            };
            let Some(file) = file else { continue };
            let path = std::path::Path::new(&file);
            if path.exists() {
                let _ = std::fs::remove_file(path);
                continue;
            }
            let Some(dir) = path.parent() else { continue };
            let base = match path.file_name().and_then(|f| f.to_str())
                .and_then(|f| f.split(":2,").next()) {
                Some(b) if !b.is_empty() => b.to_string(),
                _ => continue,
            };
            let dir_buf = dir.to_path_buf();
            let by_base = dir_cache.entry(dir_buf.clone()).or_insert_with(|| {
                let mut m = std::collections::HashMap::new();
                if let Ok(entries) = std::fs::read_dir(&dir_buf) {
                    for entry in entries.flatten() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        let b = name.split(":2,").next().unwrap_or(&name).to_string();
                        m.insert(b, entry.path());
                    }
                }
                m
            });
            if let Some(p) = by_base.get(&base) {
                let _ = std::fs::remove_file(p);
            }
        }

        let ms_files = t_start.elapsed().as_millis();
        let _ = self.write_tx.send(DbWriteOp::DeleteMessages(ids.clone()));
        // Guard against an auto-refresh re-reading the DB before the writer
        // commits and resurrecting these rows. Cleared per-id once gone.
        self.pending_deletes.extend(ids.iter().copied());
        let count = ids.len();
        self.delete_marked.clear();

        // Capture cursor anchors BEFORE mutating. In threaded/folders view
        // the cursor indexes display_messages (section headers + expanded
        // rows). Section headers are synthetic (id 0), so we anchor by the
        // section *name* (thread_id), which survives a rebuild:
        //   * own_section  — the thread the deleted rows belong to. If it
        //     still has messages after the purge (partial delete) we land
        //     back on it.
        //   * prev_section — the thread directly above it. If the deleted
        //     thread is now empty (whole-thread delete) we land here, so the
        //     cursor ends up on the thread above — what the user expects.
        let (own_section, prev_section): (Option<String>, Option<String>) = if self.show_threaded {
            match ids.iter()
                .filter_map(|id| self.display_messages.iter().position(|m| m.id == *id))
                .min()
            {
                Some(p) => {
                    let mut h = p;
                    while h > 0 && !self.display_messages[h].is_header { h -= 1; }
                    let own = self.display_messages.get(h)
                        .filter(|m| m.is_header)
                        .and_then(|m| m.thread_id.clone());
                    let mut prev = None;
                    let mut a = h;
                    while a > 0 {
                        a -= 1;
                        if self.display_messages[a].is_header {
                            prev = self.display_messages[a].thread_id.clone();
                            break;
                        }
                    }
                    (own, prev)
                }
                None => (None, None),
            }
        } else { (None, None) };
        let min_deleted_pos = ids.iter()
            .filter_map(|id| self.filtered_messages.iter().position(|m| m.id == *id))
            .min().unwrap_or(0);

        self.filtered_messages.retain(|m| !id_set.contains(&m.id));

        if self.show_threaded {
            // Rebuild so the emptied section's header is dropped immediately
            // (a manual retain left an orphan header that lingered until the
            // next keypress), then restore the cursor near the removed thread.
            self.rebuild_display();
            let pos_of = |name: &str, dm: &[Message]| -> Option<usize> {
                dm.iter().position(|m| m.is_header && m.thread_id.as_deref() == Some(name))
            };
            let len = self.display_messages.len();
            self.index = own_section.as_deref().and_then(|n| pos_of(n, &self.display_messages))
                .or_else(|| prev_section.as_deref().and_then(|n| pos_of(n, &self.display_messages)))
                .unwrap_or(0)
                .min(len.saturating_sub(1));
        } else {
            let len = self.filtered_messages.len();
            self.index = min_deleted_pos.min(len.saturating_sub(1));
        }

        let ms_list = t_start.elapsed().as_millis();
        self.set_feedback(&format!("Purged {} messages", count), self.config.theme_colors.feedback_ok);
        self.render_all();
        let ms_total = t_start.elapsed().as_millis();
        if ms_total >= 250 {
            log::warn(&format!(
                "slow purge of {} message(s): {} ms total (files {} ms, list {} ms, render {} ms)",
                count, ms_total, ms_files, ms_list - ms_files, ms_total - ms_list));
        }
    }

    fn file_message(&mut self) {
        if self.filtered_messages.is_empty() { return; }

        // Guard: refuse to file chat / non-maildir messages. The save
        // shortcuts map to maildir folder paths (e.g. "Archive",
        // "Projects.X"); applying that to a Slack/Discord/IRC message would
        // just rewrite its DB folder column to a maildir path, hiding
        // it from its real channel while never producing an actual
        // archived file. Email is identified by a `maildir_file` key
        // in metadata. Bulk-file (tagged) accepts mixed selections —
        // file_single_message gates each id individually.
        //
        // The cursor lookup is mode-aware: in threaded/folders view
        // self.index points into display_messages (which includes
        // section header pseudo-rows); cross-reference back to the
        // real Message via filtered_messages.iter().find(id) so we
        // inspect the right metadata. Pre-fix this read
        // filtered_messages[self.index] directly, which lands on a
        // random message when the cursor sits on a section header.
        if self.tagged.is_empty() {
            let current_id_and_header = if self.show_threaded {
                self.display_messages.get(self.index)
                    .map(|m| (m.id, m.is_header))
            } else {
                self.filtered_messages.get(self.index)
                    .map(|m| (m.id, false))
            };
            match current_id_and_header {
                Some((_, true)) => {
                    self.set_feedback(
                        "Save (s) needs a message — cursor is on a section header",
                        self.config.theme_colors.feedback_warn,
                    );
                    return;
                }
                Some((id, false)) => {
                    let msg = self.filtered_messages.iter().find(|m| m.id == id);
                    let is_mail = msg
                        .map(|m| m.metadata.get("maildir_file")
                            .and_then(|v| v.as_str())
                            .map(|s| !s.is_empty())
                            .unwrap_or(false))
                        .unwrap_or(false);
                    if !is_mail {
                        self.set_feedback(
                            "Save (s) only applies to email — this is a chat message",
                            self.config.theme_colors.feedback_warn,
                        );
                        return;
                    }
                }
                None => return,
            }
        }

        // Build hint from save_folders
        let shortcuts = self.config.save_folders.clone();
        let mut keys: Vec<&String> = shortcuts.keys().collect();
        keys.sort();
        let hint: String = keys.iter()
            .map(|k| {
                let v = &shortcuts[*k];
                // Show the last two path elements so e.g.
                // AA.Customers.Dualog.Archive reads as Dualog.Archive and the
                // several *.Archive targets stay distinguishable.
                let parts: Vec<&str> = v.split('.').collect();
                let short = if parts.len() > 2 {
                    parts[parts.len() - 2..].join(".")
                } else {
                    v.to_string()
                };
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
            log::info("file_message: sub-key timeout (no save)");
            self.render_bottom_bar();
            return;
        };
        log::info(&format!("file_message: sub-key = {:?}", chr));

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

        // Collect messages to file. Cursor lookup is mode-aware: in
        // threaded/folders view self.index points into display_messages
        // (with section-header pseudo-rows); fall back to
        // filtered_messages by id so we route the actual message
        // under the cursor, not whatever happens to sit at the same
        // numeric offset in filtered_messages.
        let msg_ids: Vec<i64> = if !self.tagged.is_empty() {
            self.filtered_messages.iter()
                .filter(|m| self.tagged.contains(&m.id))
                .map(|m| m.id)
                .collect()
        } else {
            let cursor_id = if self.show_threaded {
                self.display_messages.get(self.index)
                    .filter(|m| !m.is_header)
                    .map(|m| m.id)
            } else {
                self.filtered_messages.get(self.index).map(|m| m.id)
            };
            match cursor_id {
                Some(id) => vec![id],
                None => return,
            }
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
        // Rebuild the threaded / folders display from the pruned
        // filtered_messages — render_all renders from display_messages
        // in those modes, not from filtered_messages, so without this
        // rebuild the moved message stays painted on screen until the
        // next view switch or kastrup restart.
        self.rebuild_display();
        // Clamp the cursor against whichever list the current mode
        // actually paints. Without this, the cursor can land past the
        // end of display_messages and the right pane stops updating.
        let display_len = if self.show_threaded {
            self.display_messages.len()
        } else {
            self.filtered_messages.len()
        };
        if display_len == 0 {
            self.index = 0;
        } else if self.index >= display_len {
            self.index = display_len - 1;
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
        let old_folder = msg.folder.clone().unwrap_or_default();
        let mut meta = msg.metadata.clone();

        // Reject non-mail messages — they have no maildir_file and
        // shouldn't have their DB folder rewritten to a maildir path.
        // This also fires for any tagged-bulk save that includes chat
        // messages, leaving them untouched.
        let has_maildir_file = meta.get("maildir_file")
            .and_then(|v| v.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if !has_maildir_file {
            log::info(&format!(
                "file_single_message: id={} skipped (not a mail message; folder={})",
                id, old_folder));
            return Err("not a mail message".into());
        }

        let mut rename_status = "(no maildir file)".to_string();
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
                match std::fs::rename(&file_path, &new_path) {
                    Ok(()) => {
                        rename_status = format!("renamed → {}", new_path.display());
                        meta["maildir_file"] =
                            serde_json::json!(new_path.to_string_lossy().to_string());
                        meta["maildir_folder"] = serde_json::json!(dest);
                    }
                    Err(e) => {
                        rename_status = format!("rename FAILED ({}): {} → {}",
                            e, file_path, new_path.display());
                    }
                }
            } else {
                rename_status = format!("source missing: {}", file_path);
            }
        }

        // Update folder + metadata in DB
        self.db.update_message_folder(id, dest, &meta);
        self.db.mark_as_read(id);

        log::info(&format!("file_single_message: id={} {} → {} | {}",
            id, old_folder, dest, rename_status));

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

    /// Scan the current message body for chat-source file URLs (Slack
    /// today; same hook can extend to Discord / etc.) and inject them
    /// as synthetic attachment entries on the message. `v` and `V`
    /// then operate on chat attachments through the same code path
    /// they already use for email attachments — one set of keys, all
    /// message types.
    ///
    /// Skips when the message already has attachments populated. URLs
    /// are tagged with a `kastrup_remote: true` marker so the open /
    /// save handlers know to fetch-on-demand instead of extracting
    /// from a maildir file.
    fn enrich_attachments_from_chat_urls(&mut self) {
        let Some(idx) = self.current_filtered_index() else { return; };
        let msg = &self.filtered_messages[idx];
        if !msg.attachments.is_empty() { return; }
        let urls = extract_slack_file_urls(&msg.content);
        if urls.is_empty() { return; }
        let mut synth: Vec<serde_json::Value> = Vec::new();
        for url in urls {
            let (file_id, filename) = parse_slack_file_url(&url)
                .unwrap_or_else(|| ("unknown".into(), "attachment".into()));
            let ext = filename.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
            let content_type = match ext.as_str() {
                "png"  => "image/png",
                "jpg" | "jpeg" => "image/jpeg",
                "gif"  => "image/gif",
                "webp" => "image/webp",
                "svg"  => "image/svg+xml",
                "pdf"  => "application/pdf",
                _      => "application/octet-stream",
            };
            synth.push(serde_json::json!({
                "name": filename,
                "url":  url,
                "content_type": content_type,
                "file_id": file_id,
                "kastrup_remote": true,
            }));
        }
        if !synth.is_empty() {
            self.filtered_messages[idx].attachments = synth;
        }
    }

    /// Picker for an `@nick` reference. Opens a prompt; user types a
    /// substring; the first matching nick (case-insensitive, prefix
    /// match preferred) is copied to the clipboard as `@<nick>` so
    /// they can paste into the compose editor with Shift+Insert.
    /// When the cursor sits on a chat message, that channel's nick
    /// list is searched first.
    fn pick_nick_to_clipboard(&mut self) {
        let tc = self.config.theme_colors.clone();
        // Build candidate list: current channel's nicks first, then
        // every other channel's nicks, deduped.
        let current_folder = self.filtered_messages.get(self.index)
            .and_then(|m| m.folder.clone());
        let lists = self.nick_lists.lock().unwrap().clone();
        if lists.is_empty() {
            self.set_feedback("No nicks captured yet — supervisor still seeding",
                tc.feedback_warn);
            return;
        }
        let mut ordered: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        if let Some(ref f) = current_folder {
            if let Some(set) = lists.get(f) {
                for n in set { if seen.insert(n.clone()) { ordered.push(n.clone()); } }
            }
        }
        for (_, set) in &lists {
            for n in set { if seen.insert(n.clone()) { ordered.push(n.clone()); } }
        }

        let hint = ordered.iter().take(8).cloned().collect::<Vec<_>>().join(" ");
        self.bottom.say(&style::fg(&format!("Nicks: {} …", hint), tc.hint_fg));
        let query = self.prompt("@", "");
        self.render_bottom_bar();
        let q = query.trim();
        if q.is_empty() { return; }
        let q_lower = q.to_ascii_lowercase();

        let m = ordered.iter()
            .find(|n| n.to_ascii_lowercase() == q_lower)
            .or_else(|| ordered.iter().find(|n| n.to_ascii_lowercase().starts_with(&q_lower)))
            .or_else(|| ordered.iter().find(|n| n.to_ascii_lowercase().contains(&q_lower)));

        match m {
            Some(nick) => {
                let out = format!("@{}", nick);
                crust::clipboard_copy(&out, "clipboard");
                self.set_feedback(
                    &format!("Copied {} — paste with Shift+Insert", out),
                    tc.feedback_ok,
                );
            }
            None => {
                // No match — fall back to user's literal input so
                // even a typo gets pasted (lets them invent a nick
                // for someone the supervisor hasn't observed yet).
                let out = format!("@{}", q);
                crust::clipboard_copy(&out, "clipboard");
                self.set_feedback(
                    &format!("No match — copied literal {}", out),
                    tc.feedback_info,
                );
            }
        }
    }

    /// Picker for a `#channel` reference. Same shape as
    /// `pick_nick_to_clipboard` but over subscribed_buffers. The copied
    /// string uses the channel's short_name (e.g. `#general` or
    /// `#announcements`), which Slack auto-resolves when posted.
    fn pick_channel_to_clipboard(&mut self) {
        let tc = self.config.theme_colors.clone();
        let bufs = self.subscribed_buffers.lock().unwrap().clone();
        if bufs.is_empty() {
            self.set_feedback("No channels captured yet — supervisor still seeding",
                tc.feedback_warn);
            return;
        }
        // Display name = short_name (e.g. `#general`, `&postgresql`,
        // `mikael,okv`). Slack/IRC both render `#name` style refs in
        // posted text correctly.
        let names: Vec<String> = bufs.iter().map(|b| b.short_name.clone()).collect();

        let hint = names.iter().take(8).cloned().collect::<Vec<_>>().join(" ");
        self.bottom.say(&style::fg(&format!("Channels: {} …", hint), tc.hint_fg));
        let query = self.prompt("#", "");
        self.render_bottom_bar();
        let q = query.trim().trim_start_matches('#');
        if q.is_empty() { return; }
        let q_lower = q.to_ascii_lowercase();

        let m = names.iter()
            .find(|n| n.trim_start_matches(['#','&']).to_ascii_lowercase() == q_lower)
            .or_else(|| names.iter().find(|n| n.trim_start_matches(['#','&'])
                .to_ascii_lowercase().starts_with(&q_lower)))
            .or_else(|| names.iter().find(|n| n.to_ascii_lowercase().contains(&q_lower)));

        let raw = m.cloned().unwrap_or_else(|| format!("#{}", q));
        // Force `#` prefix for Slack/IRC channels even if buffer
        // short_name uses `&` (wee-slack's private-channel marker).
        let normalized = if raw.starts_with('#') {
            raw
        } else if raw.starts_with('&') {
            format!("#{}", raw.trim_start_matches('&'))
        } else if !raw.is_empty() {
            // bare name (DM) — leave as-is, user can wrap @ if they
            // really want a DM reference.
            raw
        } else {
            format!("#{}", q)
        };
        crust::clipboard_copy(&normalized, "clipboard");
        self.set_feedback(
            &format!("Copied {} — paste with Shift+Insert", normalized),
            tc.feedback_ok,
        );
    }

    fn copy_message_id(&mut self) {
        // Resolve the current message via current_filtered_index so threaded/
        // folders view (where self.index points into display_messages, which
        // includes section-header rows) yields the right id — not a row N
        // positions off in filtered_messages.
        let id = self.current_filtered_index()
            .and_then(|i| self.filtered_messages.get(i))
            .map(|m| m.id);
        match id {
            Some(id) => {
                let id_str = format!("kastrup:{}", id);
                crust::clipboard_copy(&id_str, "clipboard");
                self.set_feedback(&format!("Copied: {}", id_str), self.config.theme_colors.feedback_ok);
            }
            None => self.set_feedback(
                "Copy id needs a message — cursor is on a section header",
                self.config.theme_colors.feedback_warn),
        }
    }

    fn copy_right_pane(&self) {
        let text = self.right.text();
        crust::clipboard_copy(&crust::strip_ansi(text), "clipboard");
    }

}

// --- UI controls ---

impl App {
    fn cycle_width(&mut self) {
        self.width = if self.width >= 6 { 1 } else { self.width + 1 };
        self.config.pane_width = self.width;
        self.config.save();
        self.rebuild_panes();
    }

    fn cycle_width_reverse(&mut self) {
        self.width = if self.width <= 1 { 6 } else { self.width - 1 };
        self.config.pane_width = self.width;
        self.config.save();
        self.rebuild_panes();
    }

    fn cycle_border(&mut self) {
        self.border = (self.border + 1) % 4;
        self.config.border_style = self.border;
        self.config.save();
        self.rebuild_panes();
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
  Up/Down        Navigate messages\n\
  Left/Right     Collapse / expand thread\n\
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
  Ctrl-S         Sources management\n\
  Ctrl-W         Views overview\n\
  0-9            Custom views\n\
  F1-F12         Extended views\n\
  F              Favorites browser\n\
  L              Load more messages\n\
  Ctrl-R         Refresh current view\n\
  Ctrl-F         Filter editor\n\
  K              Kill (close) view\n\n\
{}\n\
  R              Toggle read/unread\n\
  a              Mark section read\n\
  A              Mark all read\n\
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
  +              Compose new (also lists postponed + scheduled drafts)\n\
  E              Edit draft\n\
  S              (in the send review) schedule instead of sending now\n\
  k              Add emoji reaction (chat)\n\n\
{}\n\
  v              View/save attachments\n\
  V              Inline image\n\
  D              Download images to disk\n\
  x              Open in external app\n\
  X              Open HTML in browser\n\n\
{}\n\
  /              Search messages (DB content substring, sticky)\n\
  #              Go to message by id (kastrup:7957849 or 7957849)\n\
  S              :search (claude → Filters → message list)\n\
  l              Label message\n\
  s              File/save message\n\
  m / M          Mute channel: until new msg / until mention\n\
  Ctrl-U         Toggle: all channels / unmuted only\n\
  I              claude -p ask (one-shot, response in right pane)\n\
  Ctrl-A         Full Claude session (suspend, message context)\n\
  c              AI assistant menu (draft/summarize/translate + plugins)\n\
  C              Full Claude session (alias of Ctrl-A)\n\
  :              Colon command (claude/chat/search/triage/q)\n\
  Esc            Clear sticky search, return to current view\n\
  z              AI triage → tock calendar or ~/.tasks/todo.hl\n\
  Z              Tock action (regex date capture)\n\
  :triage        Show triage history (last 20)\n\n\
{}\n\
  o              Cycle sort order\n\
  i              Invert sort\n\
  w/W            Cycle pane width forward/back\n\
  H              Set top bar (view) colour\n\
  B              Folder browser\n\
  Ctrl-B         Cycle border style\n\
  P              Preferences\n\
  y/Y            Copy ID / copy content\n\
  @              Address book\n\
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

    /// Read-only Views overview rendered into the right pane (like
    /// `show_help`). Triggered by `Ctrl-W` or `:views`. Lists every
    /// view's key, name and a compact description of what it matches,
    /// so the whole layout is visible at a glance for rearranging.
    fn show_views_screen(&mut self) {
        // Order: built-ins (A/N/*), then digit views 0-9, then F-keys,
        // then anything else — so the list reads in key order, not DB id.
        fn sort_key(key: &str) -> (u8, i64, String) {
            match key {
                "A" => (0, 0, String::new()),
                "N" => (0, 1, String::new()),
                "*" => (0, 2, String::new()),
                k if k.len() == 1 && k.as_bytes()[0].is_ascii_digit() =>
                    (1, (k.as_bytes()[0] - b'0') as i64, String::new()),
                k if k.len() > 1 && k.starts_with('F')
                    && k[1..].chars().all(|c| c.is_ascii_digit()) =>
                    (2, k[1..].parse::<i64>().unwrap_or(0), String::new()),
                k => (3, 0, k.to_string()),
            }
        }
        let warn = self.config.theme_colors.feedback_warn;
        let mut views = self.db.get_views();
        views.sort_by_key(|v| sort_key(v.key_binding.as_deref().unwrap_or("")));
        let mut lines: Vec<String> = Vec::new();
        lines.push(style::bold("Kastrup - Views"));
        lines.push(String::new());
        lines.push(style::fg("  Key  View                Matches", warn));
        for v in &views {
            let key = v.key_binding.clone().unwrap_or_else(|| "-".into());
            let summary = self.summarize_view_filter(&v.filters);
            lines.push(format!("  {} {:<19} {}",
                style::fg(&format!("{:<3}", key), warn), v.name, summary));
        }
        lines.push(String::new());
        lines.push(style::fg(
            "  (read-only — switch with a view key, or open a message to dismiss)",
            warn));
        self.right.set_text(&lines.join("\n"));
        self.right.ix = 0;
        self.right.full_refresh();
    }

    /// Compact human description of a view's `filters` JSON for the
    /// Views overview. Handles a bare rules array, a
    /// `{branches:[{rules}]}` OR-set, or `{}` (all). Maps `source_id`
    /// to the source name.
    fn summarize_view_filter(&self, filters_json: &str) -> String {
        let parsed: serde_json::Value =
            serde_json::from_str(filters_json).unwrap_or(serde_json::Value::Null);
        let sources = &self.sources_list;
        let describe = |rules: &[serde_json::Value]| -> String {
            rules.iter().map(|r| {
                let f = r.get("field").and_then(|x| x.as_str()).unwrap_or("?");
                let op = r.get("op").and_then(|x| x.as_str()).unwrap_or("=");
                let raw = match r.get("value") {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(serde_json::Value::Bool(b)) => b.to_string(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                if f == "source_id" {
                    if let Ok(id) = raw.parse::<i64>() {
                        if let Some(s) = sources.iter().find(|s| s.id == id) {
                            return format!("src:{}", s.name);
                        }
                    }
                    return format!("src:{}", raw);
                }
                let val = if raw.chars().count() > 28 {
                    format!("{}…", raw.chars().take(28).collect::<String>())
                } else { raw };
                format!("{}{}{}", f, if op == "like" { "~" } else { "=" }, val)
            }).collect::<Vec<_>>().join(" & ")
        };
        if let Some(branches) = parsed.get("branches").and_then(|b| b.as_array()) {
            branches.iter()
                .filter_map(|b| b.get("rules").and_then(|r| r.as_array()))
                .map(|r| describe(r))
                .collect::<Vec<_>>().join("  |  ")
        } else if let Some(rules) = parsed.as_array() {
            describe(rules)
        } else if let Some(rules) = parsed.get("rules").and_then(|r| r.as_array()) {
            describe(rules)
        } else {
            "all messages".into()
        }
    }

    fn handle_resize(&mut self) {
        let (cols, rows) = Crust::terminal_size();
        // Pane recreation discards prev_frame and forces a full repaint of
        // every cell on the next render — only do it when the terminal really
        // resized. The post-editor return path used to drop ~50ms here for no
        // reason because the size hadn't actually changed.
        //
        // Layout-config changes (cycle_width, cycle_border) must NOT use this
        // path — they need pane recreation but cols/rows haven't changed.
        // They call `rebuild_panes()` instead.
        if cols != self.cols || rows != self.rows {
            self.cols = cols;
            self.rows = rows;
            let (top, left, right, bottom) = create_panes(cols, rows, self.width, self.border, &self.config);
            self.top = top;
            self.left = left;
            self.right = right;
            self.bottom = bottom;
            self.restore_view_top_bg();
        } else {
            // Size unchanged: panes kept their prev_frame from before the
            // screen wipe. Without invalidating, the next `say()` diff-render
            // sees "no change" and writes nothing — top/bottom bars stay
            // invisible until something else triggers a re-render. Mark all
            // panes stale so render_all repaints fully.
            self.top.invalidate();
            self.left.invalidate();
            self.right.invalidate();
            self.bottom.invalidate();
        }
        Crust::clear_screen();
        // clear_screen wipes the pane borders too; redraw them before content
        // so the right pane border isn't missing after compose / external editor.
        if self.left.border { self.left.border_refresh(); }
        if self.right.border { self.right.border_refresh(); }
        self.render_all();
    }

    /// Rebuild every pane and redraw — used when the LAYOUT changes
    /// (`cycle_width`, `cycle_border`) but the terminal size has not. The
    /// size-aware shortcut in `handle_resize` would otherwise leave the
    /// panes at their old geometry and the change would only take effect
    /// after a kastrup restart.
    fn rebuild_panes(&mut self) {
        let (cols, rows) = Crust::terminal_size();
        self.cols = cols;
        self.rows = rows;
        let (top, left, right, bottom) = create_panes(cols, rows, self.width, self.border, &self.config);
        self.top = top;
        self.left = left;
        self.right = right;
        self.bottom = bottom;
        self.restore_view_top_bg();
        Crust::clear_screen();
        if self.left.border { self.left.border_refresh(); }
        if self.right.border { self.right.border_refresh(); }
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
        self.set_feedback_for(msg, color, std::time::Duration::from_secs(3));
    }

    /// Same as `set_feedback` but caller-controlled expiry. Long-
    /// running operations (mark-all-read flag sync, big folder
    /// moves, etc.) should set a generous timeout so the "still
    /// working" line stays visible until the work is actually done —
    /// the default 3-second expiry silently flips back to the key
    /// hints while a background op is still running, hiding the
    /// fact that kastrup is busy.
    fn set_feedback_for(&mut self, msg: &str, color: u8, expires_in: std::time::Duration) {
        // Auto-log errors and warnings
        let is_error = color == 196 || color == self.config.theme_colors.feedback_warn;
        if color == 196 { log::error(msg); }
        else if color == self.config.theme_colors.feedback_warn { log::warn(msg); }
        self.feedback_message = Some((msg.to_string(), color));
        if is_error {
            // Errors/warnings must stay readable: persist until the user's
            // next keypress instead of silently expiring after a few seconds
            // (a transient send/DNS error otherwise vanishes before it can be
            // read). The keypress that dismisses it is swallowed, not acted on.
            self.feedback_expires = None;
            self.feedback_clear_on_key = true;
        } else {
            self.feedback_expires = Some(std::time::Instant::now() + expires_in);
            self.feedback_clear_on_key = false;
        }
        self.render_bottom_bar();
    }

    /// Feedback that never auto-expires — stays put until the user's
    /// next keypress clears it (see the main loop's key branch). Use
    /// for results the user MUST see even if they glanced away: send
    /// success/failure, unrecoverable errors. Counterpart to
    /// `set_feedback`, which auto-clears after 3 seconds and is fine
    /// for confirmation toasts the user is actively watching for.
    fn set_feedback_sticky(&mut self, msg: &str, color: u8) {
        if color == 196 { log::error(msg); }
        else if color == self.config.theme_colors.feedback_warn { log::warn(msg); }
        self.feedback_message = Some((msg.to_string(), color));
        self.feedback_expires = None;
        self.feedback_clear_on_key = true;
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

    fn open_html_in_external_browser(&mut self) {
        // X key: open the message in the system default browser (xdg-open →
        // typically Firefox). In priority order: a "link" metadata field
        // (RSS / web sources), then the message's HTML rendered to a temp
        // file, then — for chat / plain-text messages with neither — the
        // URL(s) found in the body. The last case is what lets you reach a
        // Slack link whose on-screen label is truncated to `host.com/…`.
        //
        // An AI answer on screen takes priority: follow ITS links.
        match self.pick_ai_pane_url() {
            None => {}                       // no AI answer with links
            Some(None) => return,            // picker cancelled
            Some(Some(url)) => {
                let _ = std::process::Command::new("xdg-open").arg(&url)
                    .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn();
                self.set_feedback(&format!("Opened {}", shorten_url_label(&url)),
                    self.config.theme_colors.feedback_ok);
                return;
            }
        }
        self.ensure_full_loaded();
        let Some(idx) = self.current_filtered_index() else {
            self.set_feedback("No message selected", self.config.theme_colors.feedback_warn);
            return;
        };
        // Pull everything out while the immutable borrow is alive.
        let (link, html, mid, urls) = match self.filtered_messages.get(idx) {
            Some(msg) => (
                msg.metadata.get("link").and_then(|v| v.as_str()).map(|s| s.to_string()),
                best_html_for_message(msg),
                msg.id,
                extract_message_urls(&self.get_display_content(msg)),
            ),
            None => return,
        };
        let spawn = |target: &str| {
            let _ = std::process::Command::new("xdg-open").arg(target)
                .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn();
        };
        if let Some(link) = link {
            spawn(&link);
            self.set_feedback("Opened in browser", self.config.theme_colors.feedback_ok);
            return;
        }
        if let Some(html) = html {
            let path = format!("/tmp/kastrup_msg_{}.html", mid);
            if std::fs::write(&path, &html).is_ok() {
                spawn(&path);
                self.set_feedback("Opened in browser", self.config.theme_colors.feedback_ok);
            }
            return;
        }
        match urls.len() {
            0 => self.set_feedback("No HTML or link in this message", self.config.theme_colors.feedback_warn),
            1 => {
                spawn(&urls[0]);
                self.set_feedback(&format!("Opened {}", shorten_url_label(&urls[0])),
                    self.config.theme_colors.feedback_ok);
            }
            _ => {
                if let Some(i) = self.pick_url(&urls) {
                    spawn(&urls[i]);
                    self.set_feedback(&format!("Opened {}", shorten_url_label(&urls[i])),
                        self.config.theme_colors.feedback_ok);
                }
                // Restore the message view after the picker overlay.
                self.render_message_content();
            }
        }
    }

    /// Numbered picker for the URLs in a message body. Renders the list in
    /// the right pane and reads a digit (1-9, 0 = 10th). Returns the chosen
    /// index, or None on ESC. The caller restores the message view.
    fn pick_url(&mut self, urls: &[String]) -> Option<usize> {
        let tc = self.config.theme_colors.clone();
        let mut lines: Vec<String> = Vec::with_capacity(urls.len() + 3);
        lines.push(style::bold(&style::fg(
            &format!("{} links in this message", urls.len()), tc.view_custom)));
        lines.push(String::new());
        for (i, u) in urls.iter().enumerate().take(10) {
            let label = if i == 9 { "0".to_string() } else { (i + 1).to_string() };
            lines.push(format!("  {}  {}",
                style::bold(&style::fg(&format!("[{}]", label), tc.unread)), u));
        }
        lines.push(String::new());
        lines.push(style::fg("Press a digit to open the link, ESC = cancel", tc.hint_fg));
        self.right.set_text(&lines.join("\n"));
        self.right.ix = 0;
        self.right.full_refresh();
        if self.right.border { self.right.border_refresh(); }
        loop {
            let Some(chr) = Input::getchr(None) else { continue };
            match chr.as_str() {
                "ESC" | "q" => return None,
                d if d.len() == 1 && d.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) => {
                    let n: usize = d.parse().unwrap_or(0);
                    let i = if n == 0 { 9 } else { n - 1 };
                    if i < urls.len() { return Some(i); }
                }
                _ => {}
            }
        }
    }

    /// x / X while an AI answer is on screen: resolve which of ITS
    /// links to open. Outer None = no AI answer with links (caller
    /// falls through to the message path); Some(None) = the picker
    /// was cancelled (caller stops); Some(Some(url)) = open this.
    fn pick_ai_pane_url(&mut self) -> Option<Option<String>> {
        let urls = match &self.ai_pane {
            Some((_, urls)) if !urls.is_empty() => urls.clone(),
            _ => return None,
        };
        if urls.len() == 1 { return Some(Some(urls[0].clone())); }
        let chosen = self.pick_url(&urls).map(|i| urls[i].clone());
        self.restore_ai_pane();
        Some(chosen)
    }

    fn open_html_in_scroll(&mut self) {
        // x key: open the message in scroll (tier 1, no JS) — stays in the
        // terminal. Same source priority as X: link metadata, then HTML
        // rendered to a temp file, then the URL(s) found in a chat / plain
        // body (so a truncated Slack link is reachable here too).
        //
        // An AI answer on screen takes priority: follow ITS links.
        match self.pick_ai_pane_url() {
            None => {}                       // no AI answer with links
            Some(None) => return,            // picker cancelled
            Some(Some(url)) => {
                Crust::cleanup();
                let _ = std::process::Command::new("scroll").arg(&url).status();
                Crust::init();
                Crust::set_app_identity("Kastrup");
                Crust::clear_screen();
                // handle_resize repaints the message pane (which clears
                // ai_pane) — keep the answer and put it back on top.
                let saved = self.ai_pane.take();
                self.handle_resize();
                self.ai_pane = saved;
                self.restore_ai_pane();
                return;
            }
        }
        self.ensure_full_loaded();
        let Some(idx) = self.current_filtered_index() else {
            self.set_feedback("No message selected", self.config.theme_colors.feedback_warn);
            return;
        };
        let (link, html, mid, urls) = match self.filtered_messages.get(idx) {
            Some(msg) => (
                msg.metadata.get("link").and_then(|v| v.as_str()).map(|s| s.to_string()),
                best_html_for_message(msg),
                msg.id,
                extract_message_urls(&self.get_display_content(msg)),
            ),
            None => return,
        };
        let target = if let Some(link) = link {
            Some(link)
        } else if let Some(h) = html {
            let path = format!("/tmp/kastrup_msg_{}.html", mid);
            let _ = std::fs::write(&path, &h);
            Some(format!("file://{}", path))
        } else {
            match urls.len() {
                0 => None,
                1 => Some(urls[0].clone()),
                _ => {
                    let chosen = self.pick_url(&urls).map(|i| urls[i].clone());
                    self.render_message_content();
                    chosen
                }
            }
        };
        if let Some(target) = target {
            Crust::cleanup();
            let _ = std::process::Command::new("scroll").arg(&target).status();
            Crust::init();
            Crust::clear_screen();
            self.handle_resize();
        } else {
            self.set_feedback("No content to open", self.config.theme_colors.feedback_warn);
        }
    }

    /// Lazy-load the full message content from the DB if the in-memory
    /// row is just the listing snapshot. Both x/X paths need the full
    /// body before they can extract HTML.
    fn ensure_full_loaded(&mut self) {
        let Some(idx) = self.current_filtered_index() else { return; };
        if !self.filtered_messages[idx].full_loaded {
            let id = self.filtered_messages[idx].id;
            if let Some((content, html)) = self.db.get_message_content(id) {
                self.filtered_messages[idx].content = content;
                self.filtered_messages[idx].html_content = html;
                self.filtered_messages[idx].full_loaded = true;
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
                resolve_source_type(&self.source_type_map, &mut msg);
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
                        filters = parse_view_filters_json(&f);
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
                let sender = msg.display_name();
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

    /// `/` — search across the entire SQLite DB, all sources. The
    /// query is wrapped in `%…%` and passed as a content_pattern
    /// filter; the filter is persisted as the active sticky search
    /// so the 5s background refresh doesn't blank the results. The
    /// previous implementation ran `notmuch search` first, but
    /// notmuch only indexes maildir (4 of 5 source types missed) and
    /// kastrup already indexes everything into the DB on ingest, so
    /// keeping a second Xapian index in lock-step was wasted work
    /// (and one more `notmuch new` subprocess spawn per delivery).
    /// `#` — go to a message by its id: the other end of `y`, and where
    /// a `kastrup:7957849` from a note or a chat gets pasted.
    fn goto_message_prompt(&mut self) {
        let input = self.prompt("Message id: ", "");
        self.render_bottom_bar();
        if input.trim().is_empty() { return; }
        match parse_message_id(&input) {
            Some(id) => self.goto_message(id),
            None => self.set_feedback(
                &format!("Not a message id: {}", input.trim()),
                self.config.theme_colors.feedback_warn),
        }
    }

    /// Show one message and open it — `kastrup 7957849`, or the
    /// `kastrup:7957849` an id is pasted as.
    ///
    /// A sticky filter rather than a list built by hand: the five-second
    /// reconciliation re-runs the current view's rules and would blank
    /// anything else, exactly as it once blanked search results.
    fn goto_message(&mut self, id: i64) {
        let filters = Filters { message_id: Some(id), ..Default::default() };
        self.filtered_messages = self.db.get_messages(&filters, 1, 0);
        for msg in &mut self.filtered_messages {
            resolve_source_type(&self.source_type_map, msg);
        }
        if self.filtered_messages.is_empty() {
            self.set_feedback(&format!("No message {}", id),
                self.config.theme_colors.feedback_warn);
            self.render_all();
            return;
        }
        self.index = 0;
        self.show_threaded = false;
        self.active_search_label = format!("kastrup:{}", id);
        self.active_search_filter = Some(filters);
        self.set_feedback(&format!("kastrup:{}  (Esc clears)", id),
            self.config.theme_colors.feedback_ok);
        self.open_message();
    }

    fn search_prompt(&mut self) {
        let query = self.prompt("/", "");
        self.render_bottom_bar();
        if query.is_empty() { return; }

        // Seed with the current view's scope (source_type / source_id /
        // folder / folder_pattern) so `/` in F1 stays inside Slack, `/`
        // in F2 stays inside IRC, etc. The user expects search to
        // respect the view they're looking at. View "A" (all) is the
        // only one with no scope — that gives the old global behaviour.
        let mut filters = self.build_current_filters();
        // The view filter may carry `is_read` or `is_starred` from a
        // view definition. Those constrain who-shows-up, not what-to-
        // search-for, so we drop them for the search itself.
        filters.is_read = None;
        filters.is_starred = None;
        filters.content_pattern = Some(format!("%{}%", query));
        self.filtered_messages = self.db.get_messages(&filters, 500, 0);
        for msg in &mut self.filtered_messages {
            resolve_source_type(&self.source_type_map, msg);
        }
        self.index = 0;
        let n = self.filtered_messages.len();
        // Threaded view: a hit deep inside a conversation is worth little
        // without the conversation. Widen to whole threads, keep the first
        // hit so the cursor can land on the mail that actually matched, and
        // make the widened set the sticky filter — the five-second
        // reconciliation re-runs it, and a hand-built list would be blanked.
        let mut hit = self.filtered_messages.first().map(|m| m.id);
        let mut threads = 0usize;
        if self.show_threaded && !self.group_by_folder && n > 0 {
            let mut subs: Vec<String> = Vec::new();
            for m in &self.filtered_messages {
                if matches!(m.source_type.as_str(),
                    "discord" | "slack" | "weechat" | "workspace" | "rss"
                    | "messenger" | "instagram" | "whatsapp" | "telegram") { continue; }
                let sub = database::normalise_subject(m.subject.as_deref().unwrap_or(""));
                if !sub.is_empty() && !subs.contains(&sub) { subs.push(sub); }
            }
            threads = subs.len();
            if !subs.is_empty() && subs.len() <= MAX_SEARCH_THREADS {
                let mut wide = self.build_current_filters();
                wide.is_read = None;
                wide.is_starred = None;
                wide.subjects = Some(subs);
                let widened = self.db.get_messages(&wide, 2000, 0);
                if !widened.is_empty() {
                    self.filtered_messages = widened;
                    for msg in &mut self.filtered_messages {
                        resolve_source_type(&self.source_type_map, msg);
                    }
                    filters = wide;
                }
            } else if subs.len() > MAX_SEARCH_THREADS {
                hit = None;
                self.set_feedback(
                    &format!("{} threads matched — showing the hits only", subs.len()),
                    self.config.theme_colors.feedback_warn);
            }
        }
        let scope = if filters.folder.is_some() || filters.folder_pattern.is_some()
                       || filters.source_type.is_some() || filters.source_id.is_some()
        { format!("/{} in [{}]", query, self.current_view) }
        else { format!("/{}", query) };
        self.active_search_label = scope.clone();
        self.active_search_filter = Some(filters);
        // Threaded view renders display_messages — rebuild it from the
        // search hits or the pane keeps showing the old sections.
        self.rebuild_display();
        if let Some(id) = hit {
            if !self.group_by_folder { self.reveal_in_threads(id); }
        }
        if n > 0 {
            let where_ = if threads > 1 { format!(" in {} threads", threads) } else { String::new() };
            self.set_feedback(
                &format!("{} → {} match{}{}  (Esc clears)",
                    scope, n, if n == 1 { "" } else { "es" }, where_),
                self.config.theme_colors.feedback_ok);
        } else {
            self.set_feedback(&format!("{} → no matches", scope),
                self.config.theme_colors.feedback_warn);
        }
        self.render_all();
    }

    /// Simple find-in-view: prompt for a string and jump the cursor to the
    /// NEXT message in the current view whose sender / subject / preview
    /// contains it (case-insensitive), wrapping around. Unlike `/` — which
    /// FILTERS the view down to matches — this keeps the full view and only
    /// moves the cursor, revealing a match hidden inside a collapsed thread.
    /// Press `\` then Enter (empty input) to repeat the last find.
    fn find_in_view(&mut self) {
        let input = self.prompt("\\find: ", "");
        self.render_bottom_bar();
        let needle = if input.trim().is_empty() {
            self.last_find.clone()
        } else {
            self.last_find = input.trim().to_string();
            self.last_find.clone()
        };
        if needle.is_empty() { return; }
        let n = self.filtered_messages.len();
        if n == 0 {
            self.set_feedback("No messages in view", self.config.theme_colors.feedback_warn);
            return;
        }
        let needle_lc = needle.to_lowercase();
        // Start just past the current message and wrap, so repeated `\`
        // cycles through every match in the view.
        let start = self.current_filtered_index().map(|i| i + 1).unwrap_or(0);
        let mut hit: Option<i64> = None;
        for off in 0..n {
            let i = (start + off) % n;
            if msg_matches(&self.filtered_messages[i], &needle_lc) {
                hit = Some(self.filtered_messages[i].id);
                break;
            }
        }
        match hit {
            Some(id) => {
                self.reveal_message(id);
                self.set_feedback(&format!("\\{}", needle), self.config.theme_colors.feedback_info);
                self.render_all();
            }
            None => self.set_feedback(
                &format!("Not found in view: {}", needle),
                self.config.theme_colors.feedback_warn),
        }
    }

    /// Move the cursor onto the message with `target_id`. In threaded/folders
    /// view, if it's hidden inside a collapsed section, expand that section
    /// first (folders mode: the section IS the message's folder) and rebuild.
    fn reveal_message(&mut self, target_id: i64) {
        if !self.show_threaded {
            if let Some(pos) = self.filtered_messages.iter().position(|m| m.id == target_id) {
                self.index = pos;
            }
            return;
        }
        if let Some(pos) = self.display_messages.iter()
            .position(|m| !m.is_header && m.id == target_id)
        {
            self.index = pos;
            return;
        }
        // Collapsed: expand the message's section, then re-locate.
        let section = self.filtered_messages.iter().find(|m| m.id == target_id)
            .map(|m| if self.group_by_folder {
                m.folder.clone().unwrap_or_else(|| "INBOX".to_string())
            } else {
                m.thread_id.clone().or_else(|| m.folder.clone()).unwrap_or_default()
            });
        if let Some(name) = section {
            if !name.is_empty() {
                self.section_collapsed.insert(name, false);
                self.rebuild_display();
            }
        }
        if let Some(pos) = self.display_messages.iter()
            .position(|m| !m.is_header && m.id == target_id)
            .or_else(|| self.display_messages.iter().position(|m| m.id == target_id))
        {
            self.index = pos;
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
            // Mirror the on-cursor mark-as-read path (line ~2194):
            // DB flip + maildir flag sync as a paired write so the
            // filesystem file gets the `S` flag (and moves out of
            // new/ into cur/). Without the SyncMaildirFlag half,
            // the message ends up read=1 in the DB while the file
            // stays in new/, and the asmite (which counts new/)
            // reports it as still unread.
            let metadata = self.filtered_messages.iter()
                .find(|m| m.id == id)
                .map(|m| m.metadata.clone())
                .unwrap_or(serde_json::Value::Null);
            let _ = self.write_tx.send(DbWriteOp::MarkRead(id));
            if !metadata.is_null() {
                let _ = self.write_tx.send(DbWriteOp::SyncMaildirFlag(metadata, id));
            }
            if let Some(msg) = self.filtered_messages.iter_mut().find(|m| m.id == id) {
                msg.read = true;
            }
        }
        self.browsed_ids.clear();
        self.set_feedback(&format!("Marked {} browsed message(s) as read", count), self.config.theme_colors.feedback_ok);
        self.sync_mail_count();
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
                        self.refresh_view_unread_cache();
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
                    self.refresh_view_unread_cache();
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
                self.refresh_view_unread_cache();
                self.set_feedback("View deleted", self.config.theme_colors.feedback_ok);
                self.switch_to_view("A");
            } else {
                self.set_feedback("Cancelled", self.config.theme_colors.feedback_info);
            }
        }
    }

    // --- Edit Message (Batch G) ---

    fn edit_message(&mut self) {
        let Some(idx) = self.current_filtered_index() else { return; };
        let msg = &self.filtered_messages[idx];
        let id = msg.id;
        // Ensure full content
        if !msg.full_loaded {
            if let Some((content, _html)) = self.db.get_message_content(id) {
                self.filtered_messages[idx].content = content;
                self.filtered_messages[idx].full_loaded = true;
            }
        }
        let content = self.filtered_messages[idx].content.clone();
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

    // `dirty` is a save/discard flag set across the input loop; only the
    // W (save) / ESC (cancel) exits are read, so per-branch sets are
    // deliberately not all read.
    #[allow(unused_assignments)]
    fn show_preferences(&mut self) {
        let pw = 90u16.min(self.cols.saturating_sub(2));
        let ph = 38u16.min(self.rows.saturating_sub(2));
        let px = (self.cols.saturating_sub(pw)) / 2;
        let py = (self.rows.saturating_sub(ph)) / 2;
        let mut popup = Pane::new(px, py, pw, ph, 255, 235);
        popup.border = true;
        popup.scroll = false;
        popup.border_refresh();

        let lw: usize = 36usize.min((pw as usize).saturating_sub(30)).max(20);
        let rw: usize = (pw as usize).saturating_sub(lw + 5);

        let mut prefs: Vec<(&str, PrefType)> = vec![
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

        let collect_colors = |tc: &config::ThemeColors| -> Vec<(&'static str, u8)> {
            vec![
                ("Unread", tc.unread),
                ("Read", tc.read),
                ("Accent", tc.accent),
                ("Thread", tc.thread),
                ("DM", tc.dm),
                ("Tag", tc.tag),
                ("Star", tc.star),
                ("Quote 1", tc.quote1),
                ("Quote 2", tc.quote2),
                ("Quote 3", tc.quote3),
                ("Quote 4", tc.quote4),
                ("Signature", tc.sig),
                ("Link", tc.link),
                ("Email row", tc.src_email),
                ("Email icon", tc.src_email_icon),
                ("Discord row", tc.src_discord),
                ("Discord icon", tc.src_discord_icon),
                ("Slack row", tc.src_slack),
                ("Slack icon", tc.src_slack_icon),
                ("Telegram row", tc.src_telegram),
                ("Telegram icon", tc.src_telegram_icon),
                ("WhatsApp row", tc.src_whatsapp),
                ("WhatsApp icon", tc.src_whatsapp_icon),
                ("Reddit row", tc.src_reddit),
                ("Reddit icon", tc.src_reddit_icon),
                ("RSS row", tc.src_rss),
                ("RSS icon", tc.src_rss_icon),
                ("Web row", tc.src_web),
                ("Web icon", tc.src_web_icon),
                ("Messenger row", tc.src_messenger),
                ("Messenger icon", tc.src_messenger_icon),
                ("Instagram row", tc.src_instagram),
                ("Instagram icon", tc.src_instagram_icon),
                ("SMS row", tc.src_sms),
                ("SMS icon", tc.src_sms_icon),
                ("Signal row", tc.src_signal),
                ("Signal icon", tc.src_signal_icon),
                ("LinkedIn row", tc.src_linkedin),
                ("LinkedIn icon", tc.src_linkedin_icon),
                ("WeeChat row", tc.src_weechat),
                ("WeeChat icon", tc.src_weechat_icon),
                ("Default row", tc.src_default),
                ("Default icon", tc.src_default_icon),
                ("Header from", tc.header_from),
                ("Header subj", tc.header_subj),
                ("Header date", tc.header_date),
                ("Header label", tc.header_label),
                ("Separator", tc.separator),
                ("Attachment", tc.attachment),
                ("HTML hint", tc.html_hint),
                ("Replied", tc.replied),
                ("Delete mark", tc.delete_mark),
                ("Attach ind", tc.attach_ind),
                ("Date fg", tc.date_fg),
                ("View all", tc.view_all),
                ("View new", tc.view_new),
                ("View sources", tc.view_sources),
                ("View custom", tc.view_custom),
                ("View starred", tc.view_starred),
                ("Info fg", tc.info_fg),
                ("Hint fg", tc.hint_fg),
                ("Prefix fg", tc.prefix_fg),
                ("No msg", tc.no_msg),
                ("Feedback warn", tc.feedback_warn),
                ("Feedback ok", tc.feedback_ok),
                ("Feedback info", tc.feedback_info),
                ("Content fg", tc.content_fg),
                ("Content bg", tc.content_bg),
                ("List fg", tc.list_fg),
                ("List bg", tc.list_bg),
                ("Border fg", tc.border_fg),
            ]
        };

        let mut colors = collect_colors(&self.config.theme_colors);

        let body_rows = (ph as usize).saturating_sub(4);
        let mut pref_sel = 0usize;
        let mut color_sel = 0usize;
        let mut active_left = true;
        let mut color_scroll = 0usize;
        let mut dirty = false;

        let footer = format!(
            " {}",
            style::fg(
                "\u{2191}\u{2193}\u{2190}\u{2192}: nav  h/l: \u{00B1}1  H/L: \u{00B1}10  Enter: edit  W: Save  ESC: Close",
                self.config.theme_colors.hint_fg
            )
        );

        loop {
            // Auto-scroll right column
            if body_rows > 0 {
                if color_sel < color_scroll { color_scroll = color_sel; }
                if color_sel >= color_scroll + body_rows {
                    color_scroll = color_sel + 1 - body_rows;
                }
            }

            let mut lines: Vec<String> = Vec::with_capacity(body_rows + 4);
            lines.push(format!(" {}", style::fg(&style::bold("Settings"), self.config.theme_colors.view_custom)));
            lines.push(String::new());

            let can_scroll_up = color_scroll > 0;
            let can_scroll_down = color_scroll + body_rows < colors.len();

            for row in 0..body_rows {
                // Left side
                let left_raw = if row < prefs.len() {
                    let (label, ptype) = &prefs[row];
                    let label_pad = format!("{:<14}", label);
                    let val_max = lw.saturating_sub(22);
                    let value_str = match ptype {
                        PrefType::Bool(v) => if *v { style::fg("Yes", self.config.theme_colors.feedback_ok) } else { style::fg("No", 196) },
                        PrefType::Choice(_, current) => style::fg(current, self.config.theme_colors.view_custom),
                        PrefType::Text(v) => if v.len() > val_max { format!("{}...", &v[..val_max.saturating_sub(3)]) } else { v.clone() },
                        PrefType::Num(v, _, _) => format!("{}", v),
                    };
                    if active_left && pref_sel == row {
                        format!(" {} \u{25C0} {} \u{25B6}", style::reverse(&label_pad), value_str)
                    } else {
                        format!(" {}   {}  ", label_pad, value_str)
                    }
                } else {
                    String::new()
                };
                let left_padded = pad_visible(&left_raw, lw);

                // Right side
                let right_raw = {
                    let idx = color_scroll + row;
                    if idx < colors.len() {
                        let (label, val) = colors[idx];
                        let swatch = style::fg("\u{2588}\u{2588}\u{2588}", val);
                        let label_pad = format!("{:<16}", label);
                        if !active_left && color_sel == idx {
                            format!(" {} \u{25C0} {} {:>3} \u{25B6}", style::reverse(&label_pad), swatch, val)
                        } else {
                            format!(" {}   {} {:>3}  ", label_pad, swatch, val)
                        }
                    } else {
                        String::new()
                    }
                };
                let marker = if row == 0 && can_scroll_up {
                    style::fg("\u{25B3}", self.config.theme_colors.hint_fg)
                } else if row + 1 == body_rows && can_scroll_down {
                    style::fg("\u{25BD}", self.config.theme_colors.hint_fg)
                } else {
                    " ".to_string()
                };
                let right_padded = pad_visible(&right_raw, rw.saturating_sub(1));

                lines.push(format!("{} \u{2502} {}{}", left_padded, right_padded, marker));
            }
            lines.push(String::new());
            lines.push(footer.clone());

            popup.set_text(&lines.join("\n"));
            popup.ix = 0;
            popup.full_refresh();

            let Some(key) = Input::getchr(None) else { continue };
            match key.as_str() {
                "ESC" | "q" => { dirty = false; break; }
                "W" => { dirty = true; break; }
                "UP" | "k" => {
                    if active_left {
                        if pref_sel > 0 { pref_sel -= 1; }
                    } else if color_sel > 0 { color_sel -= 1; }
                }
                "DOWN" | "j" => {
                    if active_left {
                        if pref_sel + 1 < prefs.len() { pref_sel += 1; }
                    } else if color_sel + 1 < colors.len() { color_sel += 1; }
                }
                "LEFT" => { active_left = true; }
                "RIGHT" => { active_left = false; }
                "l" => {
                    if active_left {
                        next_pref(&mut prefs[pref_sel].1);
                        dirty = true;
                        if prefs[pref_sel].0 == "Color theme" {
                            if let PrefType::Choice(_, theme) = &prefs[pref_sel].1 {
                                colors = collect_colors(&config::ThemeColors::for_theme(theme));
                            }
                        }
                    } else {
                        colors[color_sel].1 = (colors[color_sel].1 as u16 + 1).min(255) as u8;
                        dirty = true;
                    }
                }
                "h" => {
                    if active_left {
                        prev_pref(&mut prefs[pref_sel].1);
                        dirty = true;
                        if prefs[pref_sel].0 == "Color theme" {
                            if let PrefType::Choice(_, theme) = &prefs[pref_sel].1 {
                                colors = collect_colors(&config::ThemeColors::for_theme(theme));
                            }
                        }
                    } else {
                        colors[color_sel].1 = colors[color_sel].1.saturating_sub(1);
                        dirty = true;
                    }
                }
                "L" => {
                    if !active_left {
                        colors[color_sel].1 = (colors[color_sel].1 as u16 + 10).min(255) as u8;
                        dirty = true;
                    }
                }
                "H" => {
                    if !active_left {
                        colors[color_sel].1 = colors[color_sel].1.saturating_sub(10);
                        dirty = true;
                    }
                }
                "ENTER" => {
                    if active_left {
                        let label = prefs[pref_sel].0.to_string();
                        match &mut prefs[pref_sel].1 {
                            PrefType::Text(val) => {
                                let new_val = self.prompt(&format!("{}: ", label), val);
                                if !new_val.is_empty() { *val = new_val; dirty = true; }
                                self.bottom.say(&footer);
                            }
                            _ => {
                                next_pref(&mut prefs[pref_sel].1);
                                dirty = true;
                                if label == "Color theme" {
                                    if let PrefType::Choice(_, theme) = &prefs[pref_sel].1 {
                                        colors = collect_colors(&config::ThemeColors::for_theme(theme));
                                    }
                                }
                            }
                        }
                    } else {
                        let input = self.prompt("Color (0-255): ", &colors[color_sel].1.to_string());
                        self.bottom.say(&footer);
                        if let Ok(v) = input.parse::<u8>() { colors[color_sel].1 = v; dirty = true; }
                    }
                }
                _ => {}
            }
        }

        if !dirty {
            self.handle_resize();
            if self.left.border { self.left.border_refresh(); }
            if self.right.border { self.right.border_refresh(); }
            self.render_top_bar();
            return;
        }

        // Apply prefs
        for (label, ptype) in &prefs {
            match (*label, ptype) {
                ("Default view", PrefType::Text(v)) => self.config.default_view = v.clone(),
                ("Color theme", PrefType::Choice(_, v)) => self.config.color_theme = v.clone(),
                ("Date format", PrefType::Choice(_, v)) => { self.date_format = v.clone(); self.config.date_format = v.clone(); }
                ("Sort order", PrefType::Choice(_, v)) => self.sort_order = v.clone(),
                ("Sort inverted", PrefType::Bool(v)) => self.sort_inverted = *v,
                ("Pane width", PrefType::Num(v, _, _)) => self.width = *v as u16,
                ("Border style", PrefType::Num(v, _, _)) => self.border = *v,
                ("Confirm purge", PrefType::Bool(v)) => self.config.confirm_purge = *v,
                ("Download folder", PrefType::Text(v)) => self.config.download_folder = v.clone(),
                ("Editor args", PrefType::Text(v)) => self.config.editor_args = v.clone(),
                ("Default email", PrefType::Text(v)) => self.config.default_email = v.clone(),
                ("SMTP command", PrefType::Text(v)) => self.config.smtp_command = v.clone(),
                _ => {}
            }
        }

        // Apply colors
        let tc = &mut self.config.theme_colors;
        for (label, val) in &colors {
            let v = *val;
            match *label {
                "Unread" => tc.unread = v,
                "Read" => tc.read = v,
                "Accent" => tc.accent = v,
                "Thread" => tc.thread = v,
                "DM" => tc.dm = v,
                "Tag" => tc.tag = v,
                "Star" => tc.star = v,
                "Quote 1" => tc.quote1 = v,
                "Quote 2" => tc.quote2 = v,
                "Quote 3" => tc.quote3 = v,
                "Quote 4" => tc.quote4 = v,
                "Signature" => tc.sig = v,
                "Link" => tc.link = v,
                "Email row" => tc.src_email = v,
                "Email icon" => tc.src_email_icon = v,
                "Discord row" => tc.src_discord = v,
                "Discord icon" => tc.src_discord_icon = v,
                "Slack row" => tc.src_slack = v,
                "Slack icon" => tc.src_slack_icon = v,
                "Telegram row" => tc.src_telegram = v,
                "Telegram icon" => tc.src_telegram_icon = v,
                "WhatsApp row" => tc.src_whatsapp = v,
                "WhatsApp icon" => tc.src_whatsapp_icon = v,
                "Reddit row" => tc.src_reddit = v,
                "Reddit icon" => tc.src_reddit_icon = v,
                "RSS row" => tc.src_rss = v,
                "RSS icon" => tc.src_rss_icon = v,
                "Web row" => tc.src_web = v,
                "Web icon" => tc.src_web_icon = v,
                "Messenger row" => tc.src_messenger = v,
                "Messenger icon" => tc.src_messenger_icon = v,
                "Instagram row" => tc.src_instagram = v,
                "Instagram icon" => tc.src_instagram_icon = v,
                "SMS row" => tc.src_sms = v,
                "SMS icon" => tc.src_sms_icon = v,
                "Signal row" => tc.src_signal = v,
                "Signal icon" => tc.src_signal_icon = v,
                "LinkedIn row" => tc.src_linkedin = v,
                "LinkedIn icon" => tc.src_linkedin_icon = v,
                "WeeChat row" => tc.src_weechat = v,
                "WeeChat icon" => tc.src_weechat_icon = v,
                "Default row" => tc.src_default = v,
                "Default icon" => tc.src_default_icon = v,
                "Header from" => tc.header_from = v,
                "Header subj" => tc.header_subj = v,
                "Header date" => tc.header_date = v,
                "Header label" => tc.header_label = v,
                "Separator" => tc.separator = v,
                "Attachment" => tc.attachment = v,
                "HTML hint" => tc.html_hint = v,
                "Replied" => tc.replied = v,
                "Delete mark" => tc.delete_mark = v,
                "Attach ind" => tc.attach_ind = v,
                "Date fg" => tc.date_fg = v,
                "View all" => tc.view_all = v,
                "View new" => tc.view_new = v,
                "View sources" => tc.view_sources = v,
                "View custom" => tc.view_custom = v,
                "View starred" => tc.view_starred = v,
                "Info fg" => tc.info_fg = v,
                "Hint fg" => tc.hint_fg = v,
                "Prefix fg" => tc.prefix_fg = v,
                "No msg" => tc.no_msg = v,
                "Feedback warn" => tc.feedback_warn = v,
                "Feedback ok" => tc.feedback_ok = v,
                "Feedback info" => tc.feedback_info = v,
                "Content fg" => tc.content_fg = v,
                "Content bg" => tc.content_bg = v,
                "List fg" => tc.list_fg = v,
                "List bg" => tc.list_bg = v,
                "Border fg" => tc.border_fg = v,
                _ => {}
            }
        }

        // Apply pane colors
        self.left.fg = tc.list_fg as u16;
        self.left.bg = tc.list_bg as u16;
        self.left.border_fg = Some(tc.border_fg as u16);
        self.right.fg = tc.content_fg as u16;
        self.right.bg = tc.content_bg as u16;
        self.right.border_fg = Some(tc.border_fg as u16);

        self.config.save();
        self.sort_messages();
        self.rebuild_display();
        self.handle_resize();
        if self.left.border { self.left.border_refresh(); }
        if self.right.border { self.right.border_refresh(); }
        self.render_top_bar();
    }

}

// --- Compose / Reply / Forward ---

impl App {
    /// Get the current folder of the selected message (for identity resolution).
    fn current_folder(&self) -> Option<String> {
        // Resolve through the threaded display→filtered mapping so a
        // section header or out-of-range display index doesn't yield the
        // wrong message / None (the v0.1.181-class index bug). Then fall
        // back to the current view's own folder filter, so composing a
        // fresh mail in e.g. View 4 (Dualog) still picks the folder_hook
        // identity even with no row selected or the cursor on a header.
        if let Some(idx) = self.current_filtered_index() {
            if let Some(folder) = self.filtered_messages.get(idx).and_then(|m| m.folder.clone()) {
                return Some(folder);
            }
        }
        self.current_view_folder()
    }

    /// The folder the current view filters on, used as the compose
    /// identity context when no message is selected (cursor on a section
    /// header), so View 4 → `AA.Customers.Dualog` → the `dualog`
    /// folder_hook. Filters are stored either flat (`{"rules":[…]}`) or
    /// as a union (`{"branches":[{"rules":[…]},…]}`); return the first
    /// `folder`-field rule's value.
    fn current_view_folder(&self) -> Option<String> {
        let vw = self.views.iter()
            .find(|v| v.key_binding.as_deref() == Some(&self.current_view))?;
        let f: serde_json::Value = serde_json::from_str(&vw.filters).ok()?;
        fn folder_in(rules: &serde_json::Value) -> Option<String> {
            rules.as_array()?.iter().find_map(|r| {
                if r.get("field").and_then(|x| x.as_str()) == Some("folder") {
                    r.get("value").and_then(|x| x.as_str()).map(|s| s.to_string())
                } else { None }
            })
        }
        if let Some(folder) = f.get("rules").and_then(folder_in) {
            return Some(folder);
        }
        f.get("branches")?.as_array()?.iter()
            .find_map(|br| br.get("rules").and_then(folder_in))
    }

    /// Get the identity for the current context (folder-hook match).
    fn current_identity(&self) -> Option<&Identity> {
        let folder = self.current_folder();
        self.config.identity_for_folder(folder.as_deref())
    }

    /// Get the "From:" identity string for composing.
    fn compose_from(&self) -> String {
        if let Some(ident) = self.current_identity() {
            ident.from_line()
        } else {
            self.config.default_email.clone()
        }
    }

    /// The identity that sent this message, when the sender is one of
    /// the user's own addresses (i.e. a message the user sent).
    fn identity_for_sender(&self, sender: &str) -> Option<&Identity> {
        let s = sender.to_ascii_lowercase();
        self.config.identities.values()
            .find(|i| !i.email.is_empty() && s.contains(&i.email.to_ascii_lowercase()))
    }

    /// From / Reply-To / signature for a reply: the given identity when
    /// Some (follow-up on the user's own message), else the current one.
    fn compose_identity(&self, own: Option<&Identity>) -> (String, String, String) {
        match own {
            Some(id) => {
                let s = id.signature();
                (id.from_line(), id.email.clone(),
                 if s.is_empty() { String::new() } else { format!("-- \n{}", s) })
            }
            None => (self.compose_from(), self.compose_email(), self.compose_signature()),
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
    /// Ensure a message at an explicit `filtered_messages`
    /// index. Reply / forward must use this in threaded view, where
    /// `self.index` points into `display_messages` (which has header
    /// pseudo-rows) and the corresponding filtered_messages entry sits
    /// at a different position.
    fn ensure_full_content_at(&mut self, idx: usize) {
        if idx >= self.filtered_messages.len() { return; }
        if !self.filtered_messages[idx].full_loaded {
            let msg_id = self.filtered_messages[idx].id;
            if let Some((content, html)) = self.db.get_message_content(msg_id) {
                self.filtered_messages[idx].content = content;
                self.filtered_messages[idx].html_content = html;
                self.filtered_messages[idx].full_loaded = true;
            }
        }
    }

    /// Resolve the cursor position to an index into `filtered_messages`,
    /// the canonical full-detail message store. In flat view this is
    /// just `self.index`. In threaded view `self.index` indexes
    /// `display_messages` (section headers + cloned message rows) and
    /// we have to look the underlying message up by id. Returns `None`
    /// when the cursor sits on a section-header row (no real message)
    /// or either list is empty.
    ///
    /// Reply / forward / etc. MUST go through this — using
    /// `self.filtered_messages[self.index]` directly produces the
    /// "reply to email landed in slack draft" bug, because the
    /// numeric index lines up with a completely unrelated message
    /// elsewhere in the flat list.
    fn current_filtered_index(&self) -> Option<usize> {
        let id = if self.show_threaded {
            let m = self.display_messages.get(self.index)?;
            if m.is_header { return None; }
            m.id
        } else {
            self.filtered_messages.get(self.index)?.id
        };
        self.filtered_messages.iter().position(|m| m.id == id)
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
        // handle_resize() does clear_screen + create_panes (if size changed) +
        // render_all in one pass — no need for the duplicate clear / render_all
        // we used to do, which doubled the post-editor repaint cost.
        self.handle_resize();
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
        let Some(idx) = self.current_filtered_index() else { return false; };
        let (plugin_type, conv, msg_id, folder) = {
            let msg = &self.filtered_messages[idx];
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

    /// Index of the compose target matching the conversation under the
    /// cursor — the selected message, or (folders/threaded mode) the channel
    /// header it sits on. `None` when the cursor isn't on a reachable
    /// external-compose target (e.g. a weechat-relay channel or a mail
    /// folder), so the caller can defer to another compose path.
    fn cursor_compose_target_ix(&self, targets: &[ComposeTarget]) -> Option<usize> {
        let (sid, conv, folder): (i64, String, String) = match self.current_filtered_index() {
            Some(idx) => {
                let m = self.filtered_messages.get(idx)?;
                (m.source_id,
                 m.metadata.get("conversation_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                 m.folder.clone().unwrap_or_default())
            }
            None => {
                // Header row: its section/folder name lives in thread_id.
                let h = self.display_messages.get(self.index).filter(|m| m.is_header)?;
                (h.source_id, String::new(), h.thread_id.clone().unwrap_or_default())
            }
        };
        targets.iter().position(|t|
            t.source_id == sid
            && ((!conv.is_empty() && conv == t.conversation_id)
                || (!folder.is_empty() && folder == t.folder)))
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

        // Only handle `+` here when the cursor is actually on a conversation
        // that is a reachable external-compose target (Workspace, gateway, …).
        // The cursor may be on a selected message OR, in folders/threaded
        // mode, the channel header it sits on. If it's on a weechat-relay
        // channel (Slack, IRC) or a mail folder, none of these targets match,
        // so defer to the weechat / email compose paths instead of defaulting
        // to the first listed target — that default is what made `+` on a
        // Slack channel header offer to compose to an unrelated Workspace DM.
        let Some(default_ix) = self.cursor_compose_target_ix(&targets) else {
            return false;
        };

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
        // Resolve via current_filtered_index so we react to the message under
        // the cursor in threaded view, not a mis-indexed filtered_messages row.
        let Some(idx) = self.current_filtered_index() else { return; };
        let msg = &self.filtered_messages[idx];
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
        let Some(idx) = self.current_filtered_index() else {
            // Cursor on a section header in threaded view, or nothing
            // selected — there's no message to reply to.
            self.set_feedback(
                "Reply needs a message — cursor is on a section header",
                self.config.theme_colors.feedback_warn,
            );
            return;
        };
        self.ensure_full_content_at(idx);
        let msg = &self.filtered_messages[idx];
        self.compose_source_type = Some(msg.source_type.clone());
        self.pending_reply_id = Some(msg.id);

        // Weechat-relay reply: route EVERY relay buffer (Slack, IRC,
        // Discord-bridge, Matrix, WhatsApp, …) through the relay's
        // `input` command (kind=Weechat). weechat posts the line under
        // its own identity, so a Slack reply appears AS THE USER with
        // no "via wee-slack" app badge — exactly like typing in weechat.
        //
        // (We used to send `python.slack.*` via the Slack Web API, but
        // an xoxp token stamps a bot-style "via wee-slack" attribution
        // and the clean xoxc/xoxd browser pair rotates every few hours.
        // The relay path sidesteps both: weechat owns the auth.)
        //
        // Caveat: if wee-slack has auto-closed an inactive Slack DM
        // buffer, the relay drops input to it silently — the same limit
        // weechat itself has (you'd reopen the DM there too).
        if msg.source_type == "weechat-relay" {
            if let Some(folder) = msg.folder.clone() {
                self.set_feedback(
                    &format!("Reply target: {} (relay)", folder),
                    self.config.theme_colors.feedback_info,
                );
                self.compose_kind = DraftKind::Weechat;
                let template = format!("Channel: {}\n\n", folder);
                self.run_editor_compose_at_full(&template, Some(3), Some(1), true);
                self.compose_kind = DraftKind::Email;
                return;
            }
        }

        // Discord (native bot). A channel reply posts inline via channel:<id> —
        // exactly what the weechat/discord-irc bridge did, as the bot in the
        // channel. A DM reply posts via dm:<author> (the bot-DM path).
        //
        // Skip gateway-relayed Discord: resolve_source_type maps a gateway
        // message with platform=discord to source_type "discord", but the
        // phone relay only captured a display name — there's no
        // discord_channel_id/author_id for the bot API. Let it fall through
        // to the gateway reply path below (reply via the live notification).
        if msg.source_type == "discord"
            && msg.metadata.get("source").and_then(|v| v.as_str()) != Some("gateway") {
            let chan = msg.metadata.get("discord_channel_id").and_then(|v| v.as_str()).unwrap_or("");
            let author = msg.metadata.get("discord_author_id").and_then(|v| v.as_str()).unwrap_or("");
            let is_channel = msg.metadata.get("is_channel").and_then(|v| v.as_bool()).unwrap_or(false);
            let target = if is_channel && !chan.is_empty() {
                format!("channel:{}", chan)
            } else if !author.is_empty() {
                format!("dm:{}", author)
            } else if !chan.is_empty() {
                format!("channel:{}", chan)
            } else {
                self.set_feedback("Discord reply: message missing channel/author",
                    self.config.theme_colors.feedback_warn);
                return;
            };
            let label = msg.recipients.clone();
            self.set_feedback(
                &format!("Reply to {} (Discord)", if label.is_empty() { target.clone() } else { label }),
                self.config.theme_colors.feedback_info);
            self.compose_kind = DraftKind::Discord;
            let template = format!("Channel: {}\n\n", target);
            self.run_editor_compose_at_full(&template, Some(3), Some(1), true);
            self.compose_kind = DraftKind::Email;
            return;
        }

        // Phone gateway reply (Instagram / Messenger / WhatsApp / Telegram
        // / Signal / SMS). The reply target is the thread_key the phone
        // captured; the relay matches it to a live notification (chat
        // apps) or sends natively (SMS). Carried as `Channel:
        // <platform>:<thread_key>`; sent via the gateway outbox.
        if msg.metadata.get("source").and_then(|v| v.as_str()) == Some("gateway") {
            let platform = msg.metadata.get("platform").and_then(|v| v.as_str())
                .unwrap_or("").to_string();
            let thread_key = msg.metadata.get("thread_key").and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| msg.thread_id.clone())
                .unwrap_or_default();
            if platform.is_empty() || thread_key.is_empty() {
                self.set_feedback("Gateway reply: message missing platform/thread",
                    self.config.theme_colors.feedback_warn);
                return;
            }
            let hint = if platform == "sms" {
                format!("SMS reply to {} (native — any number)", thread_key)
            } else {
                format!("Reply to {} on {} — needs a live notification on the phone",
                    thread_key, platform)
            };
            self.set_feedback(&hint, self.config.theme_colors.feedback_info);
            self.compose_kind = DraftKind::Gateway;
            let template = format!("Channel: {}:{}\n\n", platform, thread_key);
            self.run_editor_compose_at_full(&template, Some(3), Some(1), true);
            self.compose_kind = DraftKind::Email;
            return;
        }

        let sender = msg.display_name();
        let subject = msg.subject.as_deref().unwrap_or("");
        let re_subject = if subject.starts_with("Re:") {
            subject.to_string()
        } else {
            format!("Re: {}", subject)
        };
        let date = format_timestamp(msg.timestamp, "%Y-%m-%d %H:%M");
        // Replying to a message the user sent is a follow-up: same
        // To/Cc as the original, sent from the same identity.
        let own = self.identity_for_sender(&msg.sender).cloned();
        let (from, reply_to, sig) = self.compose_identity(own.as_ref());
        let (to, cc) = if own.is_some() {
            (parse_json_recipients(&msg.recipients),
             msg.cc.as_deref().map(parse_json_recipients).unwrap_or_default())
        } else {
            (msg.sender.clone(), String::new())
        };

        let mut template = String::new();
        template.push_str(&format!("From: {}\n", from));
        template.push_str(&format!("To: {}\n", to));
        template.push_str(&format!("Cc: {}\n", cc));
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
        let Some(idx) = self.current_filtered_index() else {
            self.set_feedback(
                "Reply-all needs a message — cursor is on a section header",
                self.config.theme_colors.feedback_warn,
            );
            return;
        };
        self.ensure_full_content_at(idx);
        let msg = &self.filtered_messages[idx];
        self.compose_source_type = Some(msg.source_type.clone());
        self.pending_reply_id = Some(msg.id);

        let sender = msg.display_name();
        let subject = msg.subject.as_deref().unwrap_or("");
        let re_subject = if subject.starts_with("Re:") {
            subject.to_string()
        } else {
            format!("Re: {}", subject)
        };
        let date = format_timestamp(msg.timestamp, "%Y-%m-%d %H:%M");
        // Group-replying to a message the user sent is a follow-up:
        // same To/Cc as the original, sent from the same identity.
        let own = self.identity_for_sender(&msg.sender).cloned();
        let (from, reply_to, sig) = self.compose_identity(own.as_ref());

        let to_list = parse_json_recipients(&msg.recipients);
        let cc_list = msg.cc.as_deref().map(parse_json_recipients).unwrap_or_default();
        let (to, cc) = if own.is_some() {
            (to_list, cc_list)
        } else {
            // Build Cc from original recipients + cc, minus self and original sender
            let my_email = reply_to.to_lowercase();
            let all_cc: Vec<&str> = to_list
                .split(", ")
                .chain(cc_list.split(", "))
                .filter(|a| {
                    !a.is_empty()
                        && !a.to_lowercase().contains(&my_email)
                        && !a.to_lowercase().contains(&msg.sender.to_lowercase())
                })
                .collect();
            (msg.sender.clone(), all_cc.join(", "))
        };

        let mut template = String::new();
        template.push_str(&format!("From: {}\n", from));
        template.push_str(&format!("To: {}\n", to));
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

    /// The inline-forward fences. Same width, so they read as a pair:
    /// the closing one says where the forwarded mail stops and whatever
    /// is written below it begins.
    const FWD_BEGIN: &'static str = "---------- Forwarded message ----------\n";
    const FWD_END: &'static str = "-------- End forwarded message --------\n";

    fn forward_inline(&mut self) {
        let Some(idx) = self.current_filtered_index() else {
            self.set_feedback(
                "Forward needs a message — cursor is on a section header",
                self.config.theme_colors.feedback_warn,
            );
            return;
        };
        self.compose_source_type = Some("email".to_string()); // forwarding is always email
        self.ensure_full_content_at(idx);
        let msg = &self.filtered_messages[idx];
        self.pending_forward_ids = vec![msg.id];

        let sender = msg.display_name();
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
        template.push_str(Self::FWD_BEGIN);
        template.push_str(&format!("From: {}\n", sender));
        template.push_str(&format!("Date: {}\n", date));
        template.push_str(&format!("Subject: {}\n", subject));
        template.push('\n');

        let content = self.get_display_content(msg);
        template.push_str(&content);
        if !content.ends_with('\n') { template.push('\n'); }
        template.push_str(Self::FWD_END);

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

        // Match `m` (compose_new): land after "To: " in Insert mode.
        self.run_editor_compose_at_full(&template, Some(2), Some(5), true);
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
                let sender = msg.display_name();
                let subj = msg.subject.as_deref().unwrap_or("");
                let date = format_timestamp(msg.timestamp, "%Y-%m-%d %H:%M");

                template.push_str(Self::FWD_BEGIN);
                template.push_str(&format!("From: {}\n", sender));
                template.push_str(&format!("Date: {}\n", date));
                template.push_str(&format!("Subject: {}\n", subj));
                template.push('\n');

                let content = self.get_display_content(msg);
                template.push_str(&content);
                if !content.ends_with('\n') { template.push('\n'); }
                template.push_str(Self::FWD_END);
                template.push('\n');
            }
        }

        if !sig.is_empty() {
            template.push_str(&sig);
            template.push('\n');
        }

        self.run_editor_compose_at_full(&template, Some(2), Some(5), true);
    }

    fn forward_attach(&mut self) {
        let Some(idx) = self.current_filtered_index() else {
            self.set_feedback(
                "Forward needs a message — cursor is on a section header",
                self.config.theme_colors.feedback_warn,
            );
            return;
        };
        let msg = &self.filtered_messages[idx];
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
        self.run_editor_compose_at_full(&template, Some(2), Some(5), true);
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
        self.run_editor_compose_at_full(&template, Some(2), Some(5), true);
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

    /// Gather draft candidates: rows from `postponed` table + any
    /// .eml files dropped under `~/.kastrup/drafts/`. Newest first.
    /// File-drop is the integration path for external tools (Claude
    /// sessions, scripts) — see CLAUDE.md.
    fn collect_draft_candidates(&self) -> Vec<DraftCandidate> {
        let mut out: Vec<DraftCandidate> = Vec::new();
        // DB-side: postponed rows
        let conn = self.db.conn.lock().unwrap();
        if let Ok(mut stmt) = conn.prepare(
            "SELECT id, data, created_at FROM postponed ORDER BY created_at DESC"
        ) {
            let rows = stmt.query_map([], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))
            });
            if let Ok(rows) = rows {
                for row in rows.flatten() {
                    let (id, data, ts) = row;
                    let kind = DraftKind::Email;
                    let (subject, body_preview) = parse_draft_preview(&data, kind);
                    out.push(DraftCandidate {
                        source: DraftSource::Postponed(id),
                        kind, subject, body_preview, data, created_at: ts,
                    });
                }
            }
        }
        // Scheduled rows: same picker, with the time they will go.
        if let Ok(mut stmt) = conn.prepare(
            "SELECT id, kind, data, send_at FROM scheduled ORDER BY send_at"
        ) {
            let rows = stmt.query_map([], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?, r.get::<_, i64>(3)?))
            });
            if let Ok(rows) = rows {
                for (id, tag, data, at) in rows.flatten() {
                    let kind = DraftKind::from_tag(&tag);
                    let (subject, body_preview) = parse_draft_preview(&data, kind);
                    out.push(DraftCandidate {
                        source: DraftSource::Scheduled(id),
                        kind,
                        subject: format!("⏰ {} · {}", fmt_send_at(at), subject),
                        body_preview,
                        data,
                        created_at: at,
                    });
                }
            }
        }
        drop(conn);
        // File-side: drop folder
        let dir = drafts_drop_dir();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for ent in entries.flatten() {
                let path = ent.path();
                if !path.is_file() { continue; }
                let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if name.starts_with('.') { continue; }
                let data = match std::fs::read_to_string(&path) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let ts = ent.metadata().ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                let kind = DraftKind::from_path(&path);
                let (subject, body_preview) = parse_draft_preview(&data, kind);
                out.push(DraftCandidate {
                    source: DraftSource::File(path),
                    kind, subject, body_preview, data, created_at: ts,
                });
            }
        }
        out.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        out
    }

    /// Park a draft in `scheduled` and report when it will go. If the
    /// insert fails the text is NOT lost: it falls back to `postponed`,
    /// and the user is told, because a message that reports "Scheduled"
    /// and then does not exist is the worst outcome here.
    fn schedule_draft(&mut self, kind: DraftKind, data: &str, at: i64) {
        let now = database::now_secs();
        let stored = {
            let conn = self.db.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO scheduled (kind, data, send_at, created_at) VALUES (?, ?, ?, ?)",
                rusqlite::params![kind.tag(), data, at, now],
            )
        };
        let tc = self.config.theme_colors.clone();
        match stored {
            Ok(_) => {
                self.refresh_next_send_at();
                self.set_feedback(&format!("Scheduled for {}", fmt_send_at(at)), tc.feedback_ok);
            }
            Err(e) => {
                log::info(&format!("schedule failed: {}", e));
                let saved = {
                    let conn = self.db.conn.lock().unwrap();
                    conn.execute(
                        "INSERT INTO postponed (data, created_at) VALUES (?, ?)",
                        rusqlite::params![data, now],
                    ).is_ok()
                };
                let note = if saved {
                    format!("Could not schedule ({}) — draft postponed instead, press + to recall", e)
                } else {
                    format!("Could not schedule or save the draft: {}", e)
                };
                self.set_feedback_sticky(&note, tc.feedback_warn);
            }
        }
    }

    /// Re-read the earliest due time. One query, only when the table
    /// changed — the idle loop then compares an i64 and moves on.
    fn refresh_next_send_at(&mut self) {
        let conn = self.db.conn.lock().unwrap();
        self.next_send_at = conn
            .query_row("SELECT MIN(send_at) FROM scheduled", [], |r| r.get::<_, Option<i64>>(0))
            .ok()
            .flatten();
    }

    /// Send whatever has come due. Called from the idle arm of the main
    /// loop, which already wakes every few seconds — no new timer.
    ///
    /// One message per turn: the email path routes through the single
    /// `pending_send` slot, and holding the rest until the next wake is
    /// simpler than queueing behind it.
    fn send_due_scheduled(&mut self) {
        let now = database::now_secs();
        if now - self.last_sched_check >= 60 {
            self.last_sched_check = now;
            self.refresh_next_send_at();
        }
        match self.next_send_at {
            Some(at) if at <= now => {}
            _ => return,
        }
        let due: Option<(i64, String, String)> = {
            let conn = self.db.conn.lock().unwrap();
            conn.query_row(
                "SELECT id, kind, data FROM scheduled WHERE send_at <= ? ORDER BY send_at LIMIT 1",
                rusqlite::params![now],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            ).ok()
        };
        let Some((id, tag, data)) = due else {
            self.refresh_next_send_at();
            return;
        };
        let kind = DraftKind::from_tag(&tag);
        // Email goes out through the same background SMTP machinery the
        // interactive path uses; the chat kinds send inline and report.
        let outcome: Result<String, String> = match kind {
            DraftKind::Email => {
                if self.pending_send.is_some() {
                    return; // a send is already in flight; try again next wake
                }
                let (data, atts) = take_email_attach_headers(&data);
                let atts: Vec<String> = atts.into_iter()
                    .filter(|a| std::path::Path::new(a).exists()).collect();
                if atts.is_empty() {
                    self.handle_composed_message(&data);
                } else {
                    self.handle_composed_message_with_attachments(&data, &atts);
                }
                Ok(String::new())
            }
            DraftKind::Slack     => self.send_slack_draft(&data).map(|c| format!("Sent to {}", c)),
            DraftKind::Discord   => self.send_discord_draft(&data).map(|c| format!("Sent to discord {}", c)),
            DraftKind::Weechat   => self.send_weechat_draft(&data).map(|c| format!("Sent to weechat {}", c)),
            DraftKind::Gateway   => self.send_gateway_draft(&data),
            DraftKind::Workspace => self.send_workspace_draft(&data),
        };
        let tc = self.config.theme_colors.clone();
        match outcome {
            Ok(msg) => {
                {
                    let conn = self.db.conn.lock().unwrap();
                    let _ = conn.execute("DELETE FROM scheduled WHERE id = ?", rusqlite::params![id]);
                }
                if !msg.is_empty() {
                    log::info(&format!("scheduled send: {}", msg));
                    self.set_feedback(&format!("{} (scheduled)", msg), tc.feedback_ok);
                }
            }
            Err(e) => {
                // Keep the row and push it out five minutes: a scheduled
                // send that fails because the VPN is down should retry,
                // not vanish or spin.
                log::info(&format!("scheduled send failed: {}", e));
                {
                    let conn = self.db.conn.lock().unwrap();
                    let _ = conn.execute(
                        "UPDATE scheduled SET send_at = ?, last_error = ? WHERE id = ?",
                        rusqlite::params![now + 300, e, id],
                    );
                }
                self.set_feedback(&format!("Scheduled send failed: {} (retrying)", e), tc.feedback_warn);
            }
        }
        self.refresh_next_send_at();
    }

    /// Drop a draft from its backing store after the user loads it.
    fn consume_draft(&self, source: &DraftSource) {
        match source {
            DraftSource::Postponed(id) => {
                let conn = self.db.conn.lock().unwrap();
                let _ = conn.execute(
                    "DELETE FROM postponed WHERE id = ?",
                    rusqlite::params![id],
                );
            }
            DraftSource::File(path) => {
                let _ = std::fs::remove_file(path);
            }
            DraftSource::Scheduled(id) => {
                let conn = self.db.conn.lock().unwrap();
                let _ = conn.execute(
                    "DELETE FROM scheduled WHERE id = ?",
                    rusqlite::params![id],
                );
            }
        }
    }

    /// Render the draft picker into the right pane. Pure draw.
    /// Shows the kind tag (email/slack/...) only when the list is
    /// mixed; pure-email lists stay clean.
    fn render_draft_picker(&mut self, candidates: &[DraftCandidate]) {
        let tc = self.config.theme_colors.clone();
        let mut lines: Vec<String> = Vec::with_capacity(candidates.len() + 4);
        lines.push(style::bold(&style::fg(
            &format!("{} pending draft(s)", candidates.len()),
            tc.view_custom,
        )));
        lines.push(String::new());
        let mixed = candidates.iter().any(|c| c.kind != candidates[0].kind);
        for (i, c) in candidates.iter().enumerate().take(36) {
            let key = pick_key_for(i);
            let mut preview = c.body_preview.clone();
            if preview.chars().count() > 60 {
                let cut: String = preview.chars().take(57).collect();
                preview = format!("{}…", cut);
            }
            let kind_tag = if mixed {
                format!("{} ", style::fg(&format!("[{:<5}]", c.kind.tag()), tc.hint_fg))
            } else {
                String::new()
            };
            lines.push(format!(
                "  {}  {}{}",
                style::bold(&style::fg(&format!("[{}]", key), tc.unread)),
                kind_tag,
                c.subject,
            ));
            if !preview.is_empty() {
                lines.push(format!("        {}", style::fg(&preview, tc.hint_fg)));
            }
        }
        lines.push(String::new());
        lines.push(style::fg(
            "[letter/digit] load · d+key delete · n or ESC new message · q quit",
            tc.hint_fg,
        ));
        self.right.set_text(&lines.join("\n"));
        self.right.ix = 0;
        self.right.full_refresh();
        if self.right.border { self.right.border_refresh(); }
    }

    /// Show a numbered picker in the right pane: a letter/digit loads
    /// that draft, `n` or ESC starts a fresh message, `q` backs out to
    /// the message list. `d<key>` deletes a candidate (file-drop →
    /// unlink, postponed / scheduled → row DELETE), then redraws.
    fn pick_draft(&mut self, candidates: &[DraftCandidate]) -> DraftPick {
        let mut list: Vec<DraftCandidate> = candidates.iter().map(|c| DraftCandidate {
            source: match &c.source {
                DraftSource::Postponed(id) => DraftSource::Postponed(*id),
                DraftSource::File(p) => DraftSource::File(p.clone()),
                DraftSource::Scheduled(id) => DraftSource::Scheduled(*id),
            },
            kind: c.kind,
            subject: c.subject.clone(),
            body_preview: c.body_preview.clone(),
            data: c.data.clone(),
            created_at: c.created_at,
        }).collect();
        self.render_draft_picker(&list);
        loop {
            let Some(chr) = Input::getchr(None) else { continue };
            match chr.as_str() {
                "q" | "Q" => return DraftPick::Quit,
                "ESC" | "n" | "N" => return DraftPick::New,
                "d" | "D" => {
                    self.set_feedback(
                        "Delete which? (letter/digit, ESC=cancel)",
                        self.config.theme_colors.feedback_warn,
                    );
                    let Some(k2) = Input::getchr(Some(5)) else {
                        self.render_draft_picker(&list);
                        continue;
                    };
                    if k2.len() != 1 {
                        self.render_draft_picker(&list);
                        continue;
                    }
                    let ch = k2.chars().next().unwrap();
                    let idx = match ch {
                        '0'..='9' => Some((ch as u8 - b'0') as usize),
                        'a'..='z' => Some(10 + (ch as u8 - b'a') as usize),
                        _ => None,
                    };
                    if let Some(i) = idx {
                        if i < list.len() {
                            self.consume_draft(&list[i].source);
                            list.remove(i);
                            if list.is_empty() { return DraftPick::New; }
                            self.render_draft_picker(&list);
                            continue;
                        }
                    }
                    self.render_draft_picker(&list);
                }
                k if k.len() == 1 => {
                    let ch = k.chars().next().unwrap();
                    let idx = match ch {
                        '0'..='9' => Some((ch as u8 - b'0') as usize),
                        'a'..='z' => Some(10 + (ch as u8 - b'a') as usize),
                        _ => None,
                    };
                    if let Some(i) = idx {
                        if i < list.len() {
                            // Mirror the chosen candidate back into the
                            // caller's slice via index match. Since we may
                            // have deleted entries, find the corresponding
                            // index in the original `candidates` slice by
                            // matching against the live `list` entry.
                            let chosen = &list[i];
                            for (j, c) in candidates.iter().enumerate() {
                                let same = match (&c.source, &chosen.source) {
                                    (DraftSource::Postponed(a), DraftSource::Postponed(b)) => a == b,
                                    (DraftSource::File(a), DraftSource::File(b)) => a == b,
                                    (DraftSource::Scheduled(a), DraftSource::Scheduled(b)) => a == b,
                                    _ => false,
                                };
                                if same { return DraftPick::Load(j); }
                            }
                            return DraftPick::New;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn compose_new(&mut self) {
        if self.maybe_external_compose() { return; }
        self.pending_reply_id = None;

        // Drafts may come from the `postponed` table (kastrup-side
        // postpone via `p` from the review screen) or from
        // ~/.kastrup/drafts/*.eml (external drop, e.g. a Claude
        // session writing a draft for the user to review). Treat
        // them uniformly.
        let candidates = self.collect_draft_candidates();

        if !candidates.is_empty() {
            // Always show the picker — even for a single draft, the
            // subject + body-preview context is more useful than a
            // y/n prompt with just the subject in the status line.
            match self.pick_draft(&candidates) {
                DraftPick::Load(i) => {
                    let c = &candidates[i];
                    log::info("recall: draft picked, consuming");
                    self.consume_draft(&c.source);
                    log::info("recall: draft consumed");
                    let mut data = c.data.clone();
                    let kind = c.kind;
                    // X-Kastrup-Reply-To / X-Kastrup-Forward-Of pseudo-
                    // headers let a drop-file draft (e.g. from a Claude
                    // session) link back to the original message, so the
                    // ←/→ arrows appear once the send succeeds. Stripped
                    // here so they never reach the outgoing RFC822.
                    if kind == DraftKind::Email {
                        let (stripped, reply_id, fwd_ids) = take_kastrup_link_headers(&data);
                        data = stripped;
                        if reply_id.is_some() { self.pending_reply_id = reply_id; }
                        if !fwd_ids.is_empty() { self.pending_forward_ids = fwd_ids; }
                    }
                    self.run_editor_compose_recalled(&data, kind);
                    return;
                }
                // q backs all the way out; ESC / n fall through to a
                // fresh compose. Repaint, or the picker's pane sits
                // there looking like it is still waiting for a key.
                DraftPick::Quit => {
                    self.render_message_content();
                    self.render_bottom_bar();
                    return;
                }
                DraftPick::New => {}
            }
        }

        // Set compose source type from current message context.
        // current_filtered_index() handles threaded-view's display_messages
        // indirection — using self.index directly into filtered_messages
        // sampled the wrong message's source_type in conversation view.
        self.compose_source_type = match self.current_filtered_index() {
            Some(idx) => Some(self.filtered_messages[idx].source_type.clone()),
            None => Some("email".to_string())
        };

        // Weechat-relay compose: route every relay buffer (Slack, IRC,
        // Discord-bridge, Matrix, …) through the relay `input` command
        // so weechat posts under the user's own identity — Slack lines
        // appear as the user, no "via wee-slack" badge (see reply()).
        let weechat_target = self.compose_weechat_target_from_context();
        if let Some(channel) = weechat_target {
            self.compose_kind = DraftKind::Weechat;
            let template = format!("Channel: {}\n\n", channel);
            self.run_editor_compose_at_full(&template, Some(3), Some(1), true);
            self.compose_kind = DraftKind::Email;  // reset for next time
            return;
        }

        // Normal compose (email).
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

        // `m` = compose new. Land on line 2 (the empty `To: `), at column
        // 5 (right after `To: `), in Insert mode so the next keystroke
        // types the recipient. Reply/forward paths still use the plain
        // `run_editor_compose_at` because their To: is already filled in.
        self.run_editor_compose_at_full(&template, Some(2), Some(5), true);
    }

    /// If the currently-selected message or the current view points
    /// at a weechat-relay buffer, return that buffer's `full_name`
    /// (the value the relay's `input` command expects). Otherwise
    /// `None` so the caller falls through to the email compose path.
    fn compose_weechat_target_from_context(&self) -> Option<String> {
        // Selected message wins — it's the most specific signal. Route
        // through current_filtered_index() so threaded view resolves through
        // display_messages.
        if let Some(idx) = self.current_filtered_index() {
            let msg = self.filtered_messages.get(idx)?;
            if msg.source_type == "weechat-relay" {
                if let Some(folder) = msg.folder.as_deref() {
                    if !folder.is_empty() { return Some(folder.to_string()); }
                }
            }
            return None;
        }
        // No selected message — the cursor may be on a channel header
        // (folders/threaded mode). Compose to that channel: a weechat-relay
        // header carries its source_type and stashes the buffer full_name in
        // thread_id. This is what lets `+` on a Slack channel header start a
        // new message to the channel (not a reply to anyone).
        let h = self.display_messages.get(self.index).filter(|m| m.is_header)?;
        if h.source_type == "weechat-relay" {
            if let Some(folder) = h.thread_id.as_deref() {
                if !folder.is_empty() { return Some(folder.to_string()); }
            }
        }
        None
    }

    /// Get displayable text content from a message, converting HTML if needed.
    fn get_display_content(&self, msg: &Message) -> String {
        let raw = &msg.content;
        // MIME extraction + QP/base64 decoding (same logic as render_message_content).
        // Only e-mail sources are decoded; relay/chat/gateway/RSS bodies are
        // plain UTF-8 and would be corrupted (Slack `=D7`/`=47` URL params trip
        // QP detection — kastrup:7953506).
        let is_email = matches!(
            self.source_type_map.get(&msg.source_id).map(String::as_str).unwrap_or(""),
            "email" | "maildir" | "imap" | "gmail"
        );
        // Same test as render_message_content, and it has to stay the same:
        // a lone `--…` line is not a boundary. A plain-text mail carrying
        // "---------- Forwarded message ----------" was read as multipart,
        // extraction found nothing, and the body came back empty — which is
        // fine in the pane (the attachment list stands in) but silently
        // dropped the quoted text from every reply. kastrup:7966376.
        let looks_mime = raw.contains("boundary=")
            || (raw.contains("Content-Type:")
                && raw.lines().any(|l| l.starts_with("--") && l.len() > 5));
        let extracted = if !is_email {
            raw.clone()
        } else if looks_mime {
            // Same attachment-only fallback as render_message_content —
            // prefer an empty body over dumping raw MIME into yank/search.
            extract_mime_text(raw).unwrap_or_default()
        } else if looks_base64(raw) {
            // Checked BEFORE QP because base64 payloads end with
            // `==\n` and trip looks_quoted_printable's early
            // `s.contains("=\n")` check. See render_message_content
            // for the same ordering and rationale.
            sources::maildir::base64_decode(raw.trim())
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .unwrap_or_else(|| raw.clone())
        } else if raw.contains("Content-Transfer-Encoding: quoted-printable")
                  || looks_quoted_printable(raw) {
            // Header-bearing form first, otherwise the bare-body
            // heuristic — see `render_message_content` for the same
            // shape and the same reason it has to live here too
            // (yank / snippet / search go through this path).
            decode_quoted_printable(&raw[body_after_headers(raw)..])
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

    /// `In-Reply-To` and `References` for a reply we are about to send.
    /// Empty when this is not a reply. Without them the recipient's mail
    /// client cannot thread the answer, and our own sent copy comes back
    /// from Gmail with nothing to link it to.
    fn reply_headers(&self) -> String {
        let Some(id) = self.pending_reply_id else { return String::new() };
        let Some(meta) = self.db.get_message_metadata(id) else { return String::new() };
        let Some(mid) = meta.get("message_id").and_then(|v| v.as_str()) else {
            return String::new();
        };
        if mid.is_empty() { return String::new(); }
        let mut refs: Vec<String> = meta.get("references")
            .and_then(|v| v.as_str())
            .map(|s| s.split_whitespace().map(|t| t.to_string()).collect())
            .unwrap_or_default();
        refs.push(format!("<{}>", mid));
        format!("In-Reply-To: <{}>\nReferences: {}\n", mid, refs.join(" "))
    }

    /// What this outgoing mail answers, when the draft never said. A
    /// drop-file written outside kastrup has no `X-Kastrup-Reply-To`, and
    /// the user still expects the original to show its arrow.
    ///
    /// Only for a subject that actually carries a reply prefix — otherwise
    /// a fresh mail that happens to share a subject would claim a parent.
    fn infer_reply_target(&self, subject: &str, to: &str, cc: &str) -> Option<i64> {
        let subject = subject.trim();
        if database::normalise_subject(subject) == subject { return None; }
        let recipients = format!("{} {}", to, cc);
        let conn = self.db.conn.lock().unwrap();
        database::find_reply_target(
            &conn, None, None, subject, &recipients, database::now_secs(), 0,
        )
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
            else if let Some(v) = strip_header_ci(line, "To") { to = v.to_string(); }
            else if let Some(v) = strip_header_ci(line, "Cc") { cc = v.to_string(); }
            else if let Some(v) = strip_header_ci(line, "Bcc") { bcc = v.to_string(); }
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
            } else if let Some(val) = strip_header_ci(line, "To") {
                result.push_str(&format!("To: {}\n", self.expand_address_field_interactive(val)));
            } else if let Some(val) = strip_header_ci(line, "Cc") {
                result.push_str(&format!("Cc: {}\n", self.expand_address_field_interactive(val)));
            } else if let Some(val) = strip_header_ci(line, "Bcc") {
                result.push_str(&format!("Bcc: {}\n", self.expand_address_field_interactive(val)));
            } else {
                result.push_str(line);
                result.push('\n');
            }
        }
        result
    }

    /// Expand addresses with interactive picker for ambiguous names.
    /// A short name (no `@`, no `<`) MUST be resolved before sending —
    /// silently letting "siv" or "phanit" through means the message is
    /// undeliverable. The resolution waterfall:
    ///   1. Single DB match → auto-expand.
    ///   2. Multiple DB matches → numbered picker.
    ///   3. No matches OR picker cancelled → bottom-bar prompt for the
    ///      full address (the user types it manually).
    /// If the user ESCs the prompt too, the short name is left as-is
    /// and the review-screen pre-flight (see `unresolved_addresses_in`)
    /// blocks the send until it's fixed.
    fn expand_address_field_interactive(&mut self, field: &str) -> String {
        field.split(',').map(|addr| {
            let addr = addr.trim();
            if addr.is_empty() || addr.contains('@') || addr.contains('<') {
                return addr.to_string();
            }
            let expanded = self.expand_address_field(addr);
            if expanded != addr {
                return expanded;
            }
            if let Some(picked) = self.pick_address(addr) {
                return picked;
            }
            let typed = self.prompt(
                &format!("Address for '{}' (ESC to keep): ", addr),
                addr,
            );
            let typed = typed.trim();
            if typed.is_empty() { addr.to_string() } else { typed.to_string() }
        }).collect::<Vec<_>>().join(", ")
    }

    /// Scan a composed message for any header value (To/Cc/Bcc) that
    /// still holds an unresolved short name — i.e. a comma-separated
    /// part with neither `@` nor `<`. Returns the offending bare names
    /// so the caller can block sending and tell the user which to fix.
    /// An empty Vec means every recipient is a real address.
    fn unresolved_addresses_in(&self, content: &str) -> Vec<String> {
        let mut bad = Vec::new();
        for raw in content.lines() {
            if raw.trim().is_empty() { break; } // headers end at first blank
            let val_opt = strip_header_ci(raw, "To")
                .or_else(|| strip_header_ci(raw, "Cc"))
                .or_else(|| strip_header_ci(raw, "Bcc"));
            let Some(val) = val_opt else { continue };
            for part in val.split(',') {
                let part = part.trim();
                if part.is_empty() { continue; }
                if !part.contains('@') && !part.contains('<') {
                    bad.push(part.to_string());
                }
            }
        }
        bad
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
            // Look up in messages by sender_name, filtered by compose context.
            // Email composes (from any email-family source: maildir, gmail,
            // imap, …) all search the full email address book — restricting
            // a maildir-origin reply to only-maildir contacts hides every
            // gmail/imap correspondent from the picker. Non-email composes
            // (Discord, Slack, RSS, etc.) filter to the same source plugin
            // so an IM compose doesn't suggest an email address.
            let conn = self.db.conn.lock().unwrap();
            let stype = self.compose_source_type.as_deref().unwrap_or("email");
            let is_email = matches!(stype, "email" | "maildir" | "imap" | "gmail");
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
                // sources.plugin_type, not source_type — the column was
                // renamed in v0.1.82 and this branch was missed.
                conn.prepare(
                    "SELECT DISTINCT m.sender, m.sender_name FROM messages m \
                     JOIN sources s ON m.source_id = s.id \
                     WHERE (m.sender_name LIKE ?1 OR m.sender LIKE ?1) AND s.plugin_type = ?2 \
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
        // Mirror the address-pool rules from expand_address_field: an email
        // compose (any of email/maildir/imap/gmail) searches every email
        // contact; other source types filter to their own plugin family.
        let conn = self.db.conn.lock().unwrap();
        let stype = self.compose_source_type.as_deref().unwrap_or("email");
        let is_email = matches!(stype, "email" | "maildir" | "imap" | "gmail");
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
            // sources.plugin_type, not source_type — the column was
            // renamed in v0.1.82 and this branch was missed.
            conn.prepare(
                "SELECT DISTINCT m.sender, m.sender_name FROM messages m \
                 JOIN sources s ON m.source_id = s.id \
                 WHERE (m.sender_name LIKE ?1 OR m.sender LIKE ?1) AND s.plugin_type = ?2 \
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
        let dir = home_dir().join(".kastrup/plugins/compose");
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
        self.run_editor_compose_at_full(template, start_line, None, false);
    }

    /// Send a slack-kind draft via the Slack Web API. Tokens come
    /// from ~/.kastrup/.env (or fall back to ~/.weechat/plugins.conf).
    /// Returns the resolved channel/DM label on success.
    fn send_slack_draft(&self, data: &str) -> Result<String, String> {
        let channel_raw = parse_chat_channel(data)
            .ok_or_else(|| "missing Channel: header".to_string())?;
        let body = parse_chat_body(data);
        if body.is_empty() {
            return Err("body is empty".to_string());
        }
        let secrets = chat_send::load_secrets();
        let token = secrets.slack_token.as_ref()
            .ok_or_else(|| "no SLACK_API_TOKEN in ~/.kastrup/.env".to_string())?;
        let cookie = secrets.slack_cookie.as_deref();
        let channel_id = chat_send::slack_resolve_channel(token, cookie, &channel_raw)?;
        // /me action: route to chat.meMessage with the prefix stripped.
        // Matches IRC convention; Slack renders these in italics with no
        // leading nick, same as wee-slack's `/me` does.
        if let Some(action) = strip_me_prefix(&body) {
            chat_send::send_slack_me(token, cookie, &channel_id, action)?;
        } else {
            chat_send::send_slack(token, cookie, &channel_id, &body)?;
        }
        // Attachments: one Slack `files.upload` per `Attach:` header.
        // Posted after the main message so they appear visually below
        // it. Each upload uses the same Bearer/Cookie auth as the
        // main send. Errors per-file are logged but don't abort the
        // overall send — partial delivery is better than zero.
        for path in parse_chat_attachments(data) {
            if !path.exists() {
                log::info(&format!("slack: skipping attach (not found): {}", path.display()));
                continue;
            }
            if let Err(e) = chat_send::slack_upload_file(
                token, cookie, &channel_id, &path, "",
            ) {
                log::info(&format!("slack: files.upload {}: {}", path.display(), e));
            }
        }
        Ok(channel_raw)
    }

    /// Send a discord-kind draft via webhook or bot API.
    /// Channel: targets — channel:<id>, dm:<userId>, webhook:<name>.
    /// Attach: headers (one per line, ~ expanded) upload alongside the
    /// message body via a single multipart POST so the file(s) and the
    /// caption appear as one Discord message instead of two.
    /// Make a just-sent Discord message visible. We send via the bot, and the
    /// ingest path deliberately skips the bot's own posts (anti-echo), so an
    /// outbound message would otherwise never appear in the thread. Insert the
    /// row here, at send time, so a `channel:`/`dm:` send shows immediately.
    fn record_outbound_discord(&mut self, draft: &str) {
        let Some(target) = parse_chat_channel(draft) else { return };
        let body = parse_chat_body(draft);
        if body.trim().is_empty() { return; }
        let target = target.trim();

        let (folder, channel_id, is_channel) =
            if let Some(cid) = target.strip_prefix("channel:") {
                let cid = cid.trim().to_string();
                let folder = crate::sources::discord::folder_for_channel(&cid)
                    .unwrap_or_else(|| "Discord".to_string());
                (folder, cid, true)
            } else if let Some(uid) = target.strip_prefix("dm:") {
                let uid = uid.trim().to_string();
                let folder = crate::sources::discord::peer_name(&uid).unwrap_or_else(|| uid.clone());
                (folder, uid, false)
            } else {
                return; // webhook — no thread to attach to
            };

        let src_id = self.sources_list.iter().find(|s| s.plugin_type == "discord").map(|s| s.id)
            .or_else(|| self.db.get_sources(false).iter().find(|s| s.plugin_type == "discord").map(|s| s.id))
            .unwrap_or(0);
        if src_id == 0 { return; }

        let now = crate::database::now_secs();
        let subject = body.lines().map(|l| l.trim()).find(|l| !l.is_empty())
            .map(|l| { let mut s: String = l.chars().take(80).collect();
                       if l.chars().count() > 80 { s.push('…'); } s })
            .unwrap_or_default();

        let md = crate::sources::MessageData {
            external_id: format!("out-discord-{}-{}", channel_id, now),
            sender: "me".to_string(),
            sender_name: Some("GeirIsene".to_string()),
            recipients: folder.clone(),
            cc: None, bcc: None,
            subject: Some(subject),
            content: body,
            html_content: None,
            timestamp: now,
            labels: vec!["discord".to_string()],
            attachments: vec![],
            metadata: serde_json::json!({
                "discord_channel_id": channel_id,
                "source_type": "discord",
                "is_channel": is_channel,
                "platform": "discord",
                "outbound": true,
            }),
            folder: Some(folder),
            thread_id: Some(channel_id),
        };
        self.db.insert_message(src_id, &md);
        self.messages_dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        self.refresh_current_view();
    }

    fn send_discord_draft(&self, data: &str) -> Result<String, String> {
        let target = parse_chat_channel(data)
            .ok_or_else(|| "missing Channel: header".to_string())?;
        let body = parse_chat_body(data);
        let attachments = parse_chat_attachments(data);
        let live_attachments: Vec<std::path::PathBuf> = attachments.into_iter()
            .filter(|p| {
                if p.exists() { true }
                else { log::info(&format!("discord: skipping attach (not found): {}", p.display())); false }
            })
            .collect();
        if body.is_empty() && live_attachments.is_empty() {
            return Err("body is empty".to_string());
        }
        let secrets = chat_send::load_secrets();
        // Discord has no native /me — convert to italicised third-person
        // text. Underscore-wrapping renders as italics across desktop /
        // mobile / web clients and stays readable in raw form too.
        let effective_body: String = match strip_me_prefix(&body) {
            Some(action) => format!("_{}_", action),
            None => body.clone(),
        };
        if live_attachments.is_empty() {
            // Plain text send — single API call.
            return chat_send::send_discord(&secrets, &target, &effective_body);
        }

        // Attachment path: one multipart POST per target type, so the
        // file(s) and caption land as a single Discord message.
        let target_t = target.trim();
        if let Some(name) = target_t.strip_prefix("webhook:") {
            let key = name.trim().to_ascii_lowercase();
            if let Some(url) = secrets.discord_webhooks.get(&key) {
                chat_send::discord_upload_files_to_webhook(url, &effective_body, &live_attachments)?;
                return Ok(format!("webhook:{}", key));
            }
            let cid = chat_send::discord_channel_for_webhook(&secrets, &key)?;
            let token = secrets.discord_bot_token.as_ref()
                .ok_or_else(|| "DISCORD_BOT_TOKEN not set".to_string())?;
            chat_send::discord_upload_files_to_channel(token, &cid, &effective_body, &live_attachments)?;
            return Ok(format!("channel:{}", cid));
        }
        if let Some(cid) = target_t.strip_prefix("channel:") {
            let token = secrets.discord_bot_token.as_ref()
                .ok_or_else(|| "DISCORD_BOT_TOKEN not set".to_string())?;
            chat_send::discord_upload_files_to_channel(token, cid.trim(), &effective_body, &live_attachments)?;
            return Ok(format!("channel:{}", cid.trim()));
        }
        if let Some(uid) = target_t.strip_prefix("dm:") {
            let token = secrets.discord_bot_token.as_ref()
                .ok_or_else(|| "DISCORD_BOT_TOKEN not set".to_string())?;
            let cid = chat_send::discord_create_dm_pub(token, uid.trim())?;
            chat_send::discord_upload_files_to_channel(token, &cid, &effective_body, &live_attachments)?;
            return Ok(format!("dm:{}", uid.trim()));
        }
        if target_t.chars().all(|c| c.is_ascii_digit()) {
            let token = secrets.discord_bot_token.as_ref()
                .ok_or_else(|| "DISCORD_BOT_TOKEN not set".to_string())?;
            chat_send::discord_upload_files_to_channel(token, target_t, &effective_body, &live_attachments)?;
            return Ok(format!("channel:{}", target_t));
        }
        Err(format!("unrecognised discord target: {}", target_t))
    }

    /// Send a weechat-kind draft. Connects to the relay, types the
    /// body into the named buffer via `input <full_name> <body>`.
    /// Returns the channel name on success so the status line shows
    /// "Sent to <full_name>".
    ///
    /// Connection is opened fresh per send for now (M4 simplicity);
    /// M5's supervised long-lived connection lets us reuse the same
    /// socket for sends + receives so this round-trips in ~50ms.
    fn send_weechat_draft(&self, data: &str) -> Result<String, String> {
        let channel = parse_chat_channel(data)
            .ok_or_else(|| "missing Channel: header".to_string())?;
        let body = parse_chat_body(data);
        if body.is_empty() {
            return Err("body is empty".to_string());
        }
        let secrets = sources::weechat_relay::load_secrets_for_main();
        let host = secrets.host.ok_or("WEECHAT_RELAY_HOST not set")?;
        let port = secrets.port.ok_or("WEECHAT_RELAY_PORT not set")?;
        let pass = secrets.password.ok_or("WEECHAT_RELAY_PASSWORD not set")?;
        let mut c = sources::weechat_relay::Connection::connect(&host, port)?;
        let _ = c.handshake()?;
        c.init_plain(&pass)?;
        c.input_by_name(&channel, &body)?;
        Ok(channel)
    }

    /// The gateway source's `config` (carries `gateway_dir`), or `{}`
    /// if no gateway source exists — `queue_reply` then defaults to
    /// `~/.kastrup/gateway`.
    fn gateway_source_config(&self) -> serde_json::Value {
        self.db.get_sources(false).into_iter()
            .find(|s| s.plugin_type == "gateway")
            .map(|s| s.config)
            .unwrap_or_else(|| serde_json::json!({}))
    }

    /// Send a gateway-kind draft: queue a reply to the phone `relay`
    /// outbox. `Channel:` carries `<platform>:<thread_key>`; the body
    /// is the message text. The relay fires it against a live
    /// notification (chat apps) or via SmsManager (SMS). Returns the
    /// `<platform>:<thread_key>` target on success.
    fn send_gateway_draft(&mut self, data: &str) -> Result<String, String> {
        let channel = parse_chat_channel(data)
            .ok_or_else(|| "missing Channel: header".to_string())?;
        let (platform, thread_key) = channel.split_once(':')
            .ok_or_else(|| "Channel must be <platform>:<thread_key>".to_string())?;
        let platform = platform.trim().to_string();
        let thread_key = thread_key.trim().to_string();
        if platform.is_empty() || thread_key.is_empty() {
            return Err("empty platform or thread_key".to_string());
        }
        let body = parse_chat_body(data);
        if body.is_empty() {
            return Err("body is empty".to_string());
        }
        // If a native delivery route is configured for this gateway target,
        // send it through the real chat API instead of the phone outbox. The
        // route value is a chat_send target, so reuse the native discord send
        // path (also handles /me, webhooks, attachments). This posts as the
        // bot/app, not the user — opt-in only.
        let route_key = format!("{}:{}", platform, thread_key);
        if platform == "discord" {
            if let Some(target) = self.config.gateway_routes.get(&route_key).cloned() {
                let native = format!("Channel: {}\n\n{}", target, body);
                return self.send_discord_draft(&native)
                    .map(|label| format!("Sent AS BOT to discord {} — not from your account; may land in their Message Requests", label));
            }
        }

        // Default: hand the reply to the phone. The relay fires the thread's
        // cached notification reply action so it posts as the user, then
        // reports the outcome back via outbox_status/. Track the request so
        // the main loop can surface "delivered" / "couldn't deliver" rather
        // than leaving a silent "queued".
        let cfg = self.gateway_source_config();
        let id = sources::gateway::queue_reply(&cfg, &platform, &thread_key, &body)?;
        let target = format!("{}:{}", platform, thread_key);
        self.pending_gateway_replies.push((id, target.clone(), std::time::Instant::now(), false));
        Ok(format!("⏳ Queued to phone — awaiting delivery to {} (not sent yet)", target))
    }

    /// Send a workspace-kind draft via the external `ws-bridge` CLI.
    /// `Conv:` (required) is the conversation UUID — the send target.
    /// `Channel:` is a display label only (ignored here). `Attach:`
    /// lines upload files; the body becomes the caption on the FIRST
    /// file only, so it isn't repeated under every attachment. With no
    /// `Attach:` line it's a plain text send (body piped on stdin).
    ///
    /// ws-bridge is exec'd with an argv array, never a shell string,
    /// so a multi-line caption and paths containing spaces survive
    /// intact. Event-driven (runs only on user send), so no idle cost.
    fn send_workspace_draft(&mut self, data: &str) -> Result<String, String> {
        use std::io::Write;
        use std::process::{Command, Stdio};

        let conv = parse_chat_conv(data)
            .ok_or_else(|| "missing Conv: header (conversation UUID)".to_string())?;
        let body = parse_chat_body(data);
        let live: Vec<std::path::PathBuf> = parse_chat_attachments(data).into_iter()
            .filter(|p| {
                if p.exists() { true }
                else { log::info(&format!("workspace: skipping attach (not found): {}", p.display())); false }
            })
            .collect();

        if body.is_empty() && live.is_empty() {
            return Err("body is empty".to_string());
        }

        // Text-only: `ws-bridge send --conv <UUID> --stdin` (body on stdin).
        if live.is_empty() {
            let mut child = Command::new("ws-bridge")
                .args(["send", "--conv", &conv, "--stdin"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| format!("ws-bridge spawn failed: {}", e))?;
            if let Some(stdin) = child.stdin.as_mut() {
                let _ = stdin.write_all(body.as_bytes());
            }
            let out = child.wait_with_output()
                .map_err(|e| format!("ws-bridge wait failed: {}", e))?;
            if !out.status.success() {
                return Err(format!("ws-bridge send: {}",
                    String::from_utf8_lossy(&out.stderr).trim()));
            }
            return Ok("workspace: sent".to_string());
        }

        // With attachments: one `ws-bridge upload` per file. The caption
        // rides the FIRST file only.
        let n = live.len();
        for (i, path) in live.iter().enumerate() {
            let mut args: Vec<String> = vec![
                "upload".into(), "--conv".into(), conv.clone(),
                "--file".into(), path.to_string_lossy().into_owned(),
            ];
            if i == 0 && !body.is_empty() {
                args.push("--caption".into());
                args.push(body.clone());
            }
            let out = Command::new("ws-bridge")
                .args(&args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .map_err(|e| format!("ws-bridge spawn failed: {}", e))?;
            if !out.status.success() {
                return Err(format!("ws-bridge upload {}: {}", path.display(),
                    String::from_utf8_lossy(&out.stderr).trim()));
            }
        }
        Ok(format!("workspace: sent +{} file(s)", n))
    }

    /// Drain delivery-status markers the relay wrote for our queued gateway
    /// replies and surface the outcome. Called from the idle loop only while
    /// `pending_gateway_replies` is non-empty (piggybacks the existing wake,
    /// no new timer). Entries the phone never reports on are dropped after a
    /// generous window so the poll goes cold again.
    fn poll_gateway_reply_status(&mut self) {
        let cfg = self.gateway_source_config();
        for (id, status, reason, st_target) in sources::gateway::poll_reply_status(&cfg) {
            // Prefer the target we were tracking; fall back to the one the relay
            // stamped on the status, so a manual send made hours later — after
            // we stopped tracking — still reports accurately.
            let label = match self.pending_gateway_replies.iter().position(|(pid, _, _, _)| *pid == id) {
                Some(pos) => self.pending_gateway_replies.remove(pos).1,
                None if !st_target.is_empty() => st_target.clone(),
                None => continue,
            };
            if status == "sent" {
                let how = if reason == "manual" { "you sent it manually" } else { "as you, via phone" };
                self.set_feedback(&format!("✓ Sent to {} ({})", label, how),
                    self.config.theme_colors.feedback_ok);
            } else {
                let why = match reason.as_str() {
                    "discarded" => "you discarded it".to_string(),
                    "expired_unsent" => "expired unsent".to_string(),
                    "" => "no live notification".to_string(),
                    r => r.replace('_', " "),
                };
                self.set_feedback(
                    &format!("✗ NOT sent to {} ({}) — resend, or reply in the app", label, why),
                    self.config.theme_colors.feedback_warn);
            }
        }
        // No result yet after ~2 min → the relay is holding the reply (no live
        // notification for the thread). Warn once (sticky, via feedback_warn) so
        // the user knows it is NOT sent yet, but keep tracking in case it lands.
        let now = std::time::Instant::now();
        let mut held: Vec<String> = Vec::new();
        for p in self.pending_gateway_replies.iter_mut() {
            if !p.3 && now.duration_since(p.2).as_secs() >= 120 {
                p.3 = true;
                held.push(p.1.clone());
            }
        }
        for target in held {
            self.set_feedback(
                &format!("⏳ Held on phone for {} — NOT sent yet (no live notification); resend or reply in the app", target),
                self.config.theme_colors.feedback_warn);
        }
        // A reply the phone never confirmed within the window has NO delivery
        // receipt — treat it as NOT sent rather than silently forgetting it (the
        // old behaviour let an undelivered reply look sent / queued forever).
        // Report a durable failure for each so the user knows to resend, then
        // stop tracking so the poll goes cold.
        let expired: Vec<String> = self.pending_gateway_replies.iter()
            .filter(|(_, _, queued, _)| now.duration_since(*queued).as_secs() >= 300)
            .map(|(_, target, _, _)| target.clone())
            .collect();
        self.pending_gateway_replies.retain(|(_, _, queued, _)| now.duration_since(*queued).as_secs() < 300);
        for target in expired {
            self.set_feedback(
                &format!("✗ NOT sent to {} — the phone never confirmed delivery. Resend, or reply in the app.", target),
                self.config.theme_colors.feedback_warn);
        }
    }

    /// Recall-path entry: open a previously-saved draft. Forces the
    /// review screen even when the editor returns with no changes,
    /// because `zz` (save+quit, unmodified) on a recalled draft must
    /// not abandon it — the user already wrote it, they're confirming.
    /// `kind` determines how the review/send path dispatches.
    fn run_editor_compose_recalled(&mut self, template: &str, kind: DraftKind) {
        self.compose_force_review = true;
        self.compose_kind = kind;
        self.run_editor_compose_at_full(template, None, None, false);
        self.compose_force_review = false;
        self.compose_kind = DraftKind::Email;
    }

    /// Same as `run_editor_compose_at` plus column hint and "start in
    /// Insert mode". Both extras are scribe-only — the column gets
    /// passed as `--col N` (1-indexed chars), insert as `--insert`.
    /// vim/vi/nvim ignore the unknown flags via shell expansion (we
    /// just don't add them); other editors aren't sent the flags
    /// either, so this stays compatible with whatever `$EDITOR` is.
    fn run_editor_compose_at_full(
        &mut self,
        template: &str,
        start_line: Option<usize>,
        start_col: Option<usize>,
        start_insert: bool,
    ) {
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
        // vim, vi, and scribe (Fe2O3 modal editor) all accept `+N` as the
        // open-at-line argument. Other editors get the bare invocation.
        let editor_short = std::path::Path::new(&editor).file_name()
            .and_then(|s| s.to_str()).unwrap_or(&editor);
        let is_vim_family = editor_short == "vim" || editor_short == "vi" || editor_short == "nvim";
        let supports_plus = is_vim_family || editor_short == "scribe";
        let is_scribe = editor_short == "scribe";
        // editor_args (e.g. `-c "set ft=mail"`) is vim-syntax. Don't pass it
        // to non-vim editors — scribe reads `-c` as a filename. Strip args
        // for the editor family that doesn't understand them.
        let args = if is_vim_family { editor_args.as_str() } else { "" };
        // scribe auto-enables spell on Email-kind files; that fires
        // hunspell + does an initial pass over the buffer, costing
        // hundreds of ms before the editor is usable. The compose
        // flow doesn't need spell-on-open — pass --no-spell so the
        // user lands on the body instantly. They can :set spell once
        // they're done typing if they want a final check.
        let mut scribe_extra = String::new();
        if is_scribe {
            scribe_extra.push_str(" --no-spell");
            if let Some(c) = start_col {
                scribe_extra.push_str(&format!(" --col {}", c));
            }
            // Every kastrup→scribe compose lands in Insert mode so the
            // user types immediately: reply, forward, new, recalled, on
            // any channel/template. start_insert (which still drives the
            // vim path below) is ignored for scribe; all compose paths
            // funnel through here.
            scribe_extra.push_str(" --insert");
        }
        // For vim, simulate insert-after-To: with `-c startinsert` plus a
        // column move. Cheap and worth it: `m` is the daily compose path.
        let vim_extra = if is_vim_family && (start_col.is_some() || start_insert) {
            let mut extra = String::new();
            if let Some(c) = start_col {
                extra.push_str(&format!(" -c \"normal! {}|\"", c));
            }
            if start_insert {
                extra.push_str(" -c startinsert");
            }
            extra
        } else {
            String::new()
        };
        let cmd_str = if supports_plus {
            format!("{} +{}{}{} {} {}", editor, cursor_line, scribe_extra, vim_extra, args, escaped_file)
        } else {
            format!("{}{}{} {} {}", editor, scribe_extra, vim_extra, args, escaped_file)
        };
        log::info(&format!("compose: launching editor: {}", cmd_str));
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg(&cmd_str)
            .status();
        log::info(&format!("compose: editor returned ok={}",
            status.as_ref().map(|s| s.success()).unwrap_or(false)));

        Crust::init();
        // No explicit clear_screen here: handle_resize() below clears + redraws
        // in one go, and skipping the duplicate avoids ~30-50ms of redundant
        // clear+repaint when the user lands back from the editor.

        if let Ok(s) = status {
            if s.success() {
                if let Ok(content) = std::fs::read_to_string(&tmpfile) {
                    // Normally an unchanged tmpfile means "user opened
                    // editor, typed nothing, quit" → abandon. But for a
                    // recalled draft the user already wrote the content
                    // earlier; `zz` with no edits must take them to the
                    // review screen so they can Send / Postpone /
                    // Cancel rather than silently dropping the work.
                    let recalled = self.compose_force_review;
                    if recalled || content.trim() != template.trim() {
                        let tc = self.config.theme_colors.clone();
                        self.handle_resize(); // single redraw; render_all is inside

                        // Expand addresses in the composed content (email only).
                        // Slack drafts carry a `Channel:` pseudo-header and a body
                        // — they have nothing for the address resolver to expand.
                        let mut final_content = content.clone();
                        if self.compose_kind == DraftKind::Email {
                            log::info("compose: expanding addresses");
                            final_content = self.expand_compose_addresses(&final_content);
                            log::info("compose: addresses expanded");
                        }
                        let _ = std::fs::write(&tmpfile, &final_content);

                        // Post-editor loop with compose plugins and attachments
                        let mut attachments: Vec<String> = std::mem::take(&mut self.pending_forward_attachments);
                        // Email `Attach:` header lines (dropped draft or typed
                        // in the editor) become real attachments.
                        if self.compose_kind == DraftKind::Email {
                            let (stripped, atts) = take_email_attach_headers(&final_content);
                            if !atts.is_empty() {
                                final_content = stripped;
                                let _ = std::fs::write(&tmpfile, &final_content);
                                for a in atts {
                                    if std::path::Path::new(&a).exists() { attachments.push(a); }
                                    else { self.set_feedback(&format!("Attach not found: {}", a), tc.feedback_warn); }
                                }
                            }
                        }
                        let plugins = self.load_compose_plugins();
                        // Last send-attempt error, persisted across the
                        // loop iteration so the review pane can show
                        // it. The bottom bar gets clobbered by the
                        // key-hint prompt on the next tick, so a
                        // bottom-bar-only feedback flashes for a frame
                        // and disappears — leaving the user thinking
                        // "nothing happened" after Enter.
                        let mut last_send_error: Option<String> = None;
                        loop {
                            // Show message summary in right pane
                            self.show_compose_review(&final_content, &attachments);
                            if let Some(ref err) = last_send_error {
                                let tc2 = self.config.theme_colors.clone();
                                let mut t = self.right.text().to_string();
                                t.push('\n');
                                t.push('\n');
                                t.push_str(&style::bold(&style::fg(
                                    &format!("✗ {}", err), tc2.feedback_warn,
                                )));
                                self.right.set_text(&t);
                                self.right.full_refresh();
                                if self.right.border { self.right.border_refresh(); }
                            }

                            let plugin_hints: String = plugins.iter()
                                .map(|(k, l, _)| format!(" {}:{}", k, l)).collect();
                            let att_hint = if attachments.is_empty() { String::new() }
                                else { format!(" [{}att]", attachments.len()) };
                            let prompt_text = format!(
                                " Enter:Send  S:Schedule  e:Re-edit  p:Postpone  a:Attach{}{} ESC:Cancel",
                                plugin_hints, att_hint);
                            self.bottom.say(&style::fg(&prompt_text, 226));
                            let Some(key) = Input::getchr(None) else { continue };
                            match key.as_str() {
                                "ENTER" => {
                                    let final_content = std::fs::read_to_string(&tmpfile)
                                        .unwrap_or_else(|_| content.clone());
                                    match self.compose_kind {
                                        DraftKind::Slack => {
                                            match self.send_slack_draft(&final_content) {
                                                Ok(channel) => {
                                                    self.set_feedback(
                                                        &format!("Sent to {}", channel),
                                                        tc.feedback_ok,
                                                    );
                                                    break;
                                                }
                                                Err(msg) => {
                                                    last_send_error = Some(
                                                        format!("Slack send failed: {}", msg)
                                                    );
                                                    self.set_feedback(
                                                        last_send_error.as_deref().unwrap_or(""),
                                                        tc.feedback_warn,
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                        DraftKind::Discord => {
                                            match self.send_discord_draft(&final_content) {
                                                Ok(label) => {
                                                    // Ingest skips our own bot's posts, so the
                                                    // send is otherwise invisible — record it now.
                                                    self.record_outbound_discord(&final_content);
                                                    self.set_feedback(
                                                        &format!("Sent to discord {}", label),
                                                        tc.feedback_ok,
                                                    );
                                                    break;
                                                }
                                                Err(msg) => {
                                                    last_send_error = Some(
                                                        format!("Discord send failed: {}", msg)
                                                    );
                                                    self.set_feedback(
                                                        last_send_error.as_deref().unwrap_or(""),
                                                        tc.feedback_warn,
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                        DraftKind::Weechat => {
                                            match self.send_weechat_draft(&final_content) {
                                                Ok(channel) => {
                                                    self.set_feedback(
                                                        &format!("Sent to weechat {}", channel),
                                                        tc.feedback_ok,
                                                    );
                                                    break;
                                                }
                                                Err(msg) => {
                                                    last_send_error = Some(
                                                        format!("Weechat send failed: {}", msg)
                                                    );
                                                    self.set_feedback(
                                                        last_send_error.as_deref().unwrap_or(""),
                                                        tc.feedback_warn,
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                        DraftKind::Gateway => {
                                            match self.send_gateway_draft(&final_content) {
                                                Ok(msg) => {
                                                    self.set_feedback(&msg, tc.feedback_ok);
                                                    break;
                                                }
                                                Err(msg) => {
                                                    last_send_error = Some(
                                                        format!("Gateway reply failed: {}", msg)
                                                    );
                                                    self.set_feedback(
                                                        last_send_error.as_deref().unwrap_or(""),
                                                        tc.feedback_warn,
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                        DraftKind::Workspace => {
                                            match self.send_workspace_draft(&final_content) {
                                                Ok(msg) => {
                                                    self.set_feedback(&msg, tc.feedback_ok);
                                                    break;
                                                }
                                                Err(msg) => {
                                                    last_send_error = Some(
                                                        format!("Workspace send failed: {}", msg)
                                                    );
                                                    self.set_feedback(
                                                        last_send_error.as_deref().unwrap_or(""),
                                                        tc.feedback_warn,
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                        DraftKind::Email => {}
                                    }
                                    // Block sending if any recipient is a bare short
                                    // name. The user asked for a hard stop here:
                                    // silently delivering to "siv" or "phanit"
                                    // gives the MTA an undeliverable address and
                                    // burns a "Sent" entry on the relayed copy.
                                    let unresolved = self.unresolved_addresses_in(&final_content);
                                    if !unresolved.is_empty() {
                                        self.set_feedback(
                                            &format!("Unresolved address: {}. Press e to fix.", unresolved.join(", ")),
                                            tc.feedback_warn,
                                        );
                                        continue;
                                    }
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
                                    // Re-edit. The initial-compose path treats
                                    // "quit with no changes" as an abandon (it
                                    // was a fresh draft the user clearly didn't
                                    // want to send); re-edit must treat the same
                                    // gesture as "looks good, never mind" — the
                                    // user already committed to the message at
                                    // the review screen. Re-launch the editor
                                    // on the current (expanded) tmpfile rather
                                    // than recursing back into the raw template,
                                    // so they edit the version they're about to
                                    // send. On return: if the file is unchanged,
                                    // fall through to the review loop again
                                    // (no re-expansion needed); if changed,
                                    // re-run address expansion before re-showing
                                    // review.
                                    let pre_edit = std::fs::read_to_string(&tmpfile)
                                        .unwrap_or_else(|_| final_content.clone());
                                    Crust::cleanup();
                                    let escaped_re = crust::shell_escape(&tmpfile);
                                    let editor_short_re = std::path::Path::new(&editor).file_name()
                                        .and_then(|s| s.to_str()).unwrap_or(editor.as_str());
                                    let is_vim_re = matches!(editor_short_re, "vim" | "vi" | "nvim");
                                    let scribe_re = if editor_short_re == "scribe" { " --no-spell" } else { "" };
                                    let args_re = if is_vim_re { editor_args.as_str() } else { "" };
                                    let cmd_re = format!("{}{} {} {}", editor, scribe_re, args_re, escaped_re);
                                    let _ = std::process::Command::new("sh")
                                        .arg("-c").arg(&cmd_re).status();
                                    Crust::init();
                                    self.handle_resize();
                                    let post_edit = std::fs::read_to_string(&tmpfile)
                                        .unwrap_or_else(|_| pre_edit.clone());
                                    if post_edit.trim() != pre_edit.trim() {
                                        final_content = self.expand_compose_addresses(&post_edit);
                                        if self.compose_kind == DraftKind::Email {
                                            let (stripped, atts) = take_email_attach_headers(&final_content);
                                            if !atts.is_empty() {
                                                final_content = stripped;
                                                for a in atts {
                                                    if std::path::Path::new(&a).exists() { attachments.push(a); }
                                                    else { self.set_feedback(&format!("Attach not found: {}", a), tc.feedback_warn); }
                                                }
                                            }
                                        }
                                        let _ = std::fs::write(&tmpfile, &final_content);
                                    }
                                    continue;
                                }
                                "S" => {
                                    let when = self.prompt(
                                        "Send at (08:00, tomorrow 09:00, +2h, 2026-07-28 08:00): ", "");
                                    match parse_send_at(&when) {
                                        Some(at) => {
                                            let mut final_content = std::fs::read_to_string(&tmpfile)
                                                .unwrap_or_else(|_| content.clone());
                                            // Email: carry the attachments into the
                                            // scheduled row as Attach: headers;
                                            // send_due_scheduled re-extracts them.
                                            if self.compose_kind == DraftKind::Email && !attachments.is_empty() {
                                                let lines: String = attachments.iter()
                                                    .map(|a| format!("Attach: {}\n", a)).collect();
                                                final_content = format!("{}{}", lines, final_content);
                                            }
                                            let kind = self.compose_kind;
                                            self.schedule_draft(kind, &final_content, at);
                                            break;
                                        }
                                        None => {
                                            if !when.trim().is_empty() {
                                                self.set_feedback(
                                                    "Not a time I understand — try 08:00, tomorrow 09:00, +2h",
                                                    tc.feedback_warn);
                                            }
                                            continue;
                                        }
                                    }
                                }
                                "p" => {
                                    let now = database::now_secs();
                                    match self.compose_kind {
                                        DraftKind::Slack | DraftKind::Discord | DraftKind::Weechat | DraftKind::Gateway | DraftKind::Workspace => {
                                            // The `postponed` DB table is email-
                                            // shaped (data → editor template).
                                            // Non-email drafts round-trip through
                                            // the drop folder so the kind survives.
                                            let ext = match self.compose_kind {
                                                DraftKind::Slack   => "slack",
                                                DraftKind::Discord => "discord",
                                                DraftKind::Weechat => "weechat",
                                                DraftKind::Gateway => "gateway",
                                                DraftKind::Workspace => "workspace",
                                                _ => "eml",
                                            };
                                            let dir = drafts_drop_dir();
                                            let _ = std::fs::create_dir_all(&dir);
                                            let path = dir.join(format!("postponed_{}.{}", now, ext));
                                            match std::fs::write(&path, &content) {
                                                Ok(_) => self.set_feedback(
                                                    &format!("{} draft postponed", ext),
                                                    tc.feedback_ok,
                                                ),
                                                Err(e) => self.set_feedback(
                                                    &format!("Postpone failed: {}", e), tc.feedback_warn),
                                            }
                                        }
                                        DraftKind::Email => {
                                            let conn = self.db.conn.lock().unwrap();
                                            let _ = conn.execute("INSERT INTO postponed (data, created_at) VALUES (?, ?)",
                                                rusqlite::params![content, now]);
                                            drop(conn);
                                            self.set_feedback("Message postponed", tc.feedback_ok);
                                        }
                                    }
                                    break;
                                }
                                "a" => {
                                    let path = self.prompt("Attach file (Enter=browse): ", "");
                                    if path.is_empty() {
                                        // Launch pointer in --pick mode
                                        let pick_file = format!("/tmp/kastrup_attach_{}.txt", std::process::id());
                                        let _ = std::fs::remove_file(&pick_file);
                                        Crust::cleanup();
                                        Crust::clear_screen();
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
                                        Crust::clear_screen();
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

    /// Write the freshly-sent RFC822 message into a month-bucketed
    /// Sent maildir (`~/Maildir/.Sent.YYYY-MM/cur/<unique>:2,S`),
    /// creating the folder skeleton (`cur` / `new` / `tmp`) if it
    /// doesn't exist yet. Layout mirrors the standard Maildir+ scheme
    /// the user's downstream tools (notmuch, RTFM browsing, etc.)
    /// already understand. Silent no-op on error — failure to archive
    /// must not look like a send failure to the user.
    fn save_to_sent(&self, rfc_msg: &str) {
        let home = match std::env::var("HOME") { Ok(h) => h, Err(_) => return };
        // YYYY-MM via `date` — kastrup doesn't pull in chrono and the
        // manual calendar arithmetic from SystemTime would be more
        // code than this one subprocess per send is worth.
        let ym_out = std::process::Command::new("date").arg("+%Y-%m").output();
        let ym = match ym_out {
            Ok(o) if o.status.success() => {
                String::from_utf8_lossy(&o.stdout).trim().to_string()
            }
            _ => return,
        };
        if ym.is_empty() { return; }

        let folder = std::path::PathBuf::from(&home)
            .join("Main/Maildir")
            .join(format!(".Sent.{}", ym));
        for sub in ["cur", "new", "tmp"] {
            let _ = std::fs::create_dir_all(folder.join(sub));
        }

        // Unique filename: <epoch_secs>.<pid>_<seq>.<hostname>:2,S
        // `:2,S` flags the message as Seen so it doesn't appear unread
        // when the maildir poller picks it up. The seq counter is the
        // last 6 digits of nanoseconds — gives uniqueness within a
        // process for back-to-back sends without needing a counter.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let seq = (now.subsec_nanos() / 1000) % 1_000_000;
        let pid = std::process::id();
        let hostname = std::process::Command::new("hostname").output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "localhost".to_string());
        let fname = format!("{}.{}_{}.{}:2,S", secs, pid, seq, hostname);
        let _ = std::fs::write(folder.join("cur").join(&fname), rfc_msg);
    }

    /// Kick off an SMTP send in the background. The native send runs
    /// on a dedicated worker thread so a slow oauth refresh or a TCP
    /// open-timeout no longer freezes the TUI — main loop stays
    /// responsive and `pump_pending_send` finishes the transaction
    /// when the worker's result lands on the mpsc channel.
    ///
    /// Calls into `email_send` directly — no shell subprocess, no Ruby
    /// `gmail_smtp` / `dmail_smtp`, no Python `oauth2.py`. The transport
    /// is chosen from `smtp_spec` (the resolved identity's `smtp`
    /// value): `smtp://host:port` → native plain relay, anything else
    /// → Gmail XOAUTH2. Both paths live in `src/email_send.rs`.
    ///
    /// Returns `true` when the send was queued; `false` if another
    /// send is already in flight (caller should leave the tempfile
    /// alone and surface a "busy" toast).
    fn spawn_smtp_send(
        &mut self,
        from_email: String,
        recipients: Vec<String>,
        to_display: String,
        tmpfile: String,
        rfc_msg: String,
        smtp_spec: String,
        forward_ids: Vec<i64>,
        reply_id: Option<i64>,
        attachment_count: Option<usize>,
        compose_draft: String,
    ) -> bool {
        if self.pending_send.is_some() {
            self.set_feedback(
                "Another send is already in flight — wait for it to finish",
                self.config.theme_colors.feedback_warn,
            );
            return false;
        }
        let (tx, rx) = std::sync::mpsc::channel::<SendOutcome>();
        let worker_rfc = rfc_msg.clone();
        std::thread::spawn(move || {
            let safedir = email_send::default_safedir();
            let transport = email_send::transport_for(&smtp_spec);
            let outcome = email_send::send_email(
                &safedir, &from_email, &recipients, worker_rfc.as_bytes(), &transport,
            );
            // Receiver may have been dropped if kastrup is shutting
            // down mid-send; that's fine, the user has bigger
            // problems than a missing toast.
            let _ = tx.send(outcome);
        });
        self.pending_send = Some(PendingSend {
            result_rx: rx,
            to_display: to_display.clone(),
            tmpfile,
            rfc_msg,
            forward_ids,
            reply_id,
            attachment_count,
            compose_draft,
        });
        // Persistent "Sending..." badge in the top bar — survives any
        // bottom-bar feedback the user might trigger while the send is
        // in flight. Render now so it appears the instant the worker
        // starts (the worker thread itself doesn't touch the UI).
        self.render_top_bar();
        true
    }

    /// Check whether the pending SMTP send has finished. Called from
    /// the main event loop on every tick (keypress or timeout). Cheap
    /// when nothing's queued — single `try_recv` on the channel.
    fn pump_pending_send(&mut self) {
        let Some(ps) = self.pending_send.as_ref() else { return; };
        let outcome = match ps.result_rx.try_recv() {
            Ok(o) => o,
            Err(std::sync::mpsc::TryRecvError::Empty) => return,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                // Worker thread died without sending a result. Treat
                // as failure; surface what we can.
                Err("send worker disconnected".to_string())
            }
        };
        // Take ownership so we can move tmpfile / rfc_msg out and
        // re-borrow `self` mutably below.
        let ps = self.pending_send.take().unwrap();
        match outcome {
            Ok(()) => {
                self.save_to_sent(&ps.rfc_msg);
                let _ = std::fs::remove_file(&ps.tmpfile);
                log::info(&format!("SMTP sent OK to {}", ps.to_display));
                let toast = if let Some(n) = ps.attachment_count {
                    format!("Sent to {} ({} attachment(s))", ps.to_display, n)
                } else {
                    format!("Sent to {}", ps.to_display)
                };
                // Sticky: the user fired off a send and may have looked
                // away while it completed. Both success and failure
                // stay on the status bar until the next keypress.
                self.set_feedback_sticky(&toast, self.config.theme_colors.feedback_ok);
                // Restore the forward/reply book-keeping the synchronous
                // path used to do inline.
                if !ps.forward_ids.is_empty() {
                    let saved = std::mem::replace(&mut self.pending_forward_ids, ps.forward_ids);
                    self.mark_forwarded();
                    self.pending_forward_ids = saved;
                }
                if ps.reply_id.is_some() {
                    let saved = std::mem::replace(&mut self.pending_reply_id, ps.reply_id);
                    self.mark_replied();
                    self.pending_reply_id = saved;
                }
            }
            Err(msg) => {
                // Re-file the draft into `postponed` so a failed send (VPN
                // down, SMTP unreachable) never loses it — it reappears in
                // the `m` recall picker, exactly like Postpone. Critical for
                // a recalled draft, whose durable copy was consumed on load.
                // The compose-format text is used (NOT rfc_msg / tmpfile,
                // which hold assembled MIME that won't round-trip).
                log::info(&format!("SMTP send failed for {}: {}", ps.to_display, msg));
                let saved = if !ps.compose_draft.trim().is_empty() {
                    let now = database::now_secs();
                    let conn = self.db.conn.lock().unwrap();
                    let ok = conn.execute(
                        "INSERT INTO postponed (data, created_at) VALUES (?, ?)",
                        rusqlite::params![ps.compose_draft, now],
                    ).is_ok();
                    drop(conn);
                    ok
                } else {
                    false
                };
                let note = if saved {
                    // Safely in `postponed` now — drop the ephemeral RFC tmpfile.
                    let _ = std::fs::remove_file(&ps.tmpfile);
                    format!("Send failed to {}: {}. Draft saved (press m to recall)",
                        ps.to_display, msg)
                } else {
                    // Couldn't re-file: keep the tmpfile as a last resort.
                    format!("Send failed to {}: {} (draft kept at {})",
                        ps.to_display, msg, ps.tmpfile)
                };
                self.set_feedback_sticky(&note, 196);
            }
        }
        // Clear the in-flight badge from the top bar.
        self.render_top_bar();
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
            else if let Some(val) = strip_header_ci(line, "To") { to = val.trim().to_string(); }
            else if let Some(val) = strip_header_ci(line, "Cc") { cc = val.trim().to_string(); }
            else if let Some(val) = strip_header_ci(line, "Bcc") { bcc = val.trim().to_string(); }
            else if let Some(val) = line.strip_prefix("Subject: ") { subject = val.trim().to_string(); }
            else if let Some(val) = line.strip_prefix("Reply-To: ") { reply_to = val.trim().to_string(); }
        }
        let body = body_lines.join("\n");
        if to.is_empty() || body.trim().is_empty() {
            self.set_feedback("Cancelled (empty To or body)", self.config.theme_colors.feedback_warn);
            return;
        }
        // Per-identity SMTP: match the From header to an identity to
        // pick its transport spec (e.g. dualog → smtp://relay). Owned
        // String so the borrow of self.config ends before the later
        // &mut self spawn call.
        let smtp_spec: String = self.config.identities.iter()
            .find(|(_, id)| from.contains(&id.email))
            .and_then(|(_, id)| id.smtp.as_ref())
            .unwrap_or(&self.config.smtp_command)
            .clone();
        if smtp_spec.is_empty() {
            self.set_feedback("No SMTP command configured (set in Preferences)", self.config.theme_colors.feedback_warn);
            return;
        }
        let boundary = format!("kastrup-boundary-{}", std::process::id());
        let (date_hdr, msgid) = rfc822_date_and_msgid(&from);
        let mut rfc_msg = String::new();
        rfc_msg.push_str(&format!("From: {}\n", from));
        rfc_msg.push_str(&format!("Date: {}\n", date_hdr));
        rfc_msg.push_str(&format!("Message-ID: {}\n", msgid));
        rfc_msg.push_str(&format!("To: {}\n", to));
        if !cc.is_empty() { rfc_msg.push_str(&format!("Cc: {}\n", cc)); }
        if !bcc.is_empty() { rfc_msg.push_str(&format!("Bcc: {}\n", bcc)); }
        if !reply_to.is_empty() { rfc_msg.push_str(&format!("Reply-To: {}\n", reply_to)); }
        rfc_msg.push_str(&format!("Subject: {}\n", subject));
        rfc_msg.push_str(&self.reply_headers());
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
        log::info(&format!("SMTP (with attachments): {} -> {} ({} att)", from_email, recipients.join(", "), attachments.len()));
        // Hand off to the worker thread — native SMTP; main loop stays
        // interactive while the worker does the transport (oauth +
        // TLS for Gmail, or a plain relay connect for smtp:// specs).
        let forward_ids = std::mem::take(&mut self.pending_forward_ids);
        let reply_id = self.pending_reply_id.take()
            .or_else(|| self.infer_reply_target(&subject, &to, &cc));
        let att_n = attachments.len();
        self.spawn_smtp_send(
            from_email, recipients, to, smtp_tmpfile, rfc_msg, smtp_spec,
            forward_ids, reply_id, Some(att_n), content.to_string(),
        );
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
            } else if let Some(val) = strip_header_ci(line, "To") {
                to = val.trim().to_string();
            } else if let Some(val) = strip_header_ci(line, "Cc") {
                cc = val.trim().to_string();
            } else if let Some(val) = strip_header_ci(line, "Bcc") {
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

        // Per-identity SMTP: match the From header to an identity to
        // pick its transport spec. Owned String so the self.config
        // borrow ends before the later &mut self spawn call.
        let smtp_spec: String = self.config.identities.iter()
            .find(|(_, id)| from.contains(&id.email))
            .and_then(|(_, id)| id.smtp.as_ref())
            .unwrap_or(&self.config.smtp_command)
            .clone();

        // Build RFC822-style message for SMTP
        if smtp_spec.is_empty() {
            self.set_feedback(
                "No SMTP command configured (set in Preferences)",
                self.config.theme_colors.feedback_warn,
            );
            return;
        }

        let (date_hdr, msgid) = rfc822_date_and_msgid(&from);
        let mut rfc_msg = String::new();
        rfc_msg.push_str(&format!("From: {}\n", from));
        rfc_msg.push_str(&format!("Date: {}\n", date_hdr));
        rfc_msg.push_str(&format!("Message-ID: {}\n", msgid));
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
        rfc_msg.push_str(&self.reply_headers());
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
        log::info(&format!("SMTP: {} -> {}", from_email, recipients.join(", ")));
        let forward_ids = std::mem::take(&mut self.pending_forward_ids);
        let reply_id = self.pending_reply_id.take()
            .or_else(|| self.infer_reply_target(&subject, &to, &cc));
        self.spawn_smtp_send(
            from_email, recipients, to, smtp_tmpfile, rfc_msg, smtp_spec,
            forward_ids, reply_id, None, content.to_string(),
        );
    }
}

// --- Attachment Viewing ---

impl App {
    fn view_attachments(&mut self) {
        let Some(idx) = self.current_filtered_index() else { return; };
        // Ensure full content loaded for MIME extraction
        self.ensure_full_content_at(idx);
        // Try MIME extraction if attachments are empty
        if self.filtered_messages[idx].attachments.is_empty() {
            let msg = &self.filtered_messages[idx];
            if msg.content.contains("Content-Type:") {
                let atts = extract_mime_attachments(&msg.content, msg.id);
                if !atts.is_empty() {
                    self.filtered_messages[idx].attachments = atts;
                }
            }
        }
        // Chat message URL → synthetic attachments. Lets `v` work
        // uniformly for email AND chat (Slack files etc.).
        self.enrich_attachments_from_chat_urls();
        let msg = &self.filtered_messages[idx];
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
                    "t:Tag  T:All  o/Enter:Open  s:Save  p:Open PDF  P:Save PDF{}  ESC:Back",
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
                " j/k:Navigate  t:Tag  T:Tag all  o:Open  s:Save  p/P:Open/Save as PDF  ESC:Back",
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
                "p" => {
                    let name = attachments[att_index]
                        .get("name")
                        .or_else(|| attachments[att_index].get("filename"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("unnamed").to_string();
                    if !is_office_doc(&name) {
                        self.set_feedback("Not an office document",
                            self.config.theme_colors.feedback_warn);
                    } else {
                        self.extract_and_open_attachment(
                            maildir_file.as_deref(), &attachments, att_index, false);
                        if let Some(pdf) = self.office_to_pdf(&att_temp_path(&name)) {
                            let pdf_name = pdf_file_name(&name);
                            self.open_attachment_file(&pdf, &pdf_name);
                        }
                    }
                }
                "P" => {
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
                            .unwrap_or("unnamed").to_string();
                        if !is_office_doc(&name) {
                            self.set_feedback(&format!("Not an office document: {}", name),
                                self.config.theme_colors.feedback_warn);
                            continue;
                        }
                        self.extract_and_open_attachment(
                            maildir_file.as_deref(), &attachments, idx, false);
                        let Some(pdf) = self.office_to_pdf(&att_temp_path(&name)) else {
                            continue;
                        };
                        let dl = self.config.download_folder.replace(
                            '~',
                            &std::env::var("HOME").unwrap_or_default(),
                        );
                        let default_dest = format!("{}/{}", dl, pdf_file_name(&name));
                        let dest = self.prompt("Save PDF to: ", &default_dest);
                        if !dest.is_empty() {
                            match std::fs::copy(&pdf, &dest) {
                                Ok(_) => self.set_feedback(&format!("Saved: {}", dest),
                                    self.config.theme_colors.feedback_ok),
                                Err(e) => self.set_feedback(&format!("Save failed: {}", e),
                                    self.config.theme_colors.feedback_warn),
                            }
                        }
                    }
                }
                "ESC" | "q" | "h" | "LEFT" => break,
                _ => {}
            }
        }

        self.render_all();
    }

    /// Open a downloaded attachment file. If the desktop handler for the
    /// file's MIME type is a terminal app (e.g. scribe for text/plain),
    /// `xdg-open` launches it detached with no terminal and nothing shows —
    /// run it in-terminal instead (suspend the TUI, run, restore). GUI
    /// handlers (images, PDFs) still go through xdg-open, detached.
    fn open_attachment_file(&mut self, path: &str, name: &str) {
        if let Some(exec) = Self::terminal_handler_exec(path) {
            let esc = crust::shell_escape(path);
            let has_field = ["%f", "%F", "%u", "%U"].iter().any(|f| exec.contains(f));
            let mut cmd = exec.replace("%f", &esc).replace("%F", &esc)
                .replace("%u", &esc).replace("%U", &esc);
            if !has_field { cmd = format!("{} {}", cmd, esc); }
            Crust::cleanup();
            let _ = std::process::Command::new("sh").arg("-c").arg(&cmd).status();
            Crust::init();
            Crust::set_app_identity("Kastrup");
            Crust::clear_screen();
            self.render_all();
        } else {
            let _ = std::process::Command::new("xdg-open").arg(path)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn();
        }
        self.set_feedback(&format!("Opened {}", name), self.config.theme_colors.feedback_ok);
    }

    /// If the default desktop handler for `path`'s MIME type is a terminal
    /// app, return its `Exec` line; else `None` (caller should xdg-open).
    fn terminal_handler_exec(path: &str) -> Option<String> {
        let mime = Self::cmd_stdout("xdg-mime", &["query", "filetype", path])?;
        let mime = mime.trim();
        if mime.is_empty() { return None; }
        let desktop = Self::cmd_stdout("xdg-mime", &["query", "default", mime])?;
        let desktop = desktop.trim();
        if desktop.is_empty() { return None; }
        let home = std::env::var("HOME").unwrap_or_default();
        let dirs = [
            format!("{}/.local/share/applications", home),
            "/usr/share/applications".to_string(),
            "/usr/local/share/applications".to_string(),
        ];
        let content = dirs.iter()
            .find_map(|d| std::fs::read_to_string(format!("{}/{}", d, desktop)).ok())?;
        let mut terminal = false;
        let mut exec = None;
        for line in content.lines() {
            let l = line.trim();
            if let Some(v) = l.strip_prefix("Terminal=") {
                terminal = v.trim().eq_ignore_ascii_case("true");
            } else if let Some(v) = l.strip_prefix("Exec=") {
                if exec.is_none() { exec = Some(v.trim().to_string()); }
            }
        }
        if terminal { exec } else { None }
    }

    fn cmd_stdout(cmd: &str, args: &[&str]) -> Option<String> {
        std::process::Command::new(cmd).args(args).output().ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
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
        let dest = att_temp_path(name);

        // Remote-URL path: synthetic attachments injected by
        // `enrich_attachments_from_chat_urls` carry a `url` plus
        // `kastrup_remote: true`. Fetch with the per-source auth and
        // either open or stash. Cached under
        // `~/.kastrup/attachments/<file-id>_<name>` so repeated opens
        // skip the round-trip.
        if att.get("kastrup_remote").and_then(|v| v.as_bool()) == Some(true) {
            if let Some(url) = att.get("url").and_then(|v| v.as_str()) {
                let file_id = att.get("file_id").and_then(|v| v.as_str()).unwrap_or("rem");
                let home = std::env::var("HOME").unwrap_or_default();
                let dir = std::path::PathBuf::from(home).join(".kastrup").join("attachments");
                let _ = std::fs::create_dir_all(&dir);
                let safe_name: String = name.chars()
                    .map(|c| if c.is_alphanumeric() || ".-_".contains(c) { c } else { '_' })
                    .collect();
                let cached = dir.join(format!("{}_{}", file_id, safe_name));

                if !cached.exists() {
                    self.set_feedback(&format!("Downloading {}…", name),
                        self.config.theme_colors.feedback_info);
                    let source_type = att.get("source_type").and_then(|v| v.as_str()).unwrap_or("slack");
                    let secrets = chat_send::load_secrets();
                    let mut req = ureq::get(url);
                    // Slack: files.slack.com URLs need Bearer + d-cookie auth.
                    // Discord CDN, Instagram CDN: URLs are pre-signed (or
                    // public) — adding auth headers can cause 4xx responses
                    // and leaks the Slack credential to unrelated origins.
                    match source_type {
                        "slack" => {
                            if let Some(t) = secrets.slack_token.as_deref() {
                                req = req.set("Authorization", &format!("Bearer {}", t));
                            }
                            if let Some(c) = secrets.slack_cookie.as_deref() {
                                req = req.set("Cookie", &format!("d={}", c));
                            }
                        }
                        "discord" => {
                            req = req.set("User-Agent",
                                "kastrup (https://github.com/isene/kastrup, 0.1)");
                        }
                        _ => {}
                    }
                    match req.call() {
                        Ok(resp) => {
                            let mut buf: Vec<u8> = Vec::new();
                            if std::io::copy(&mut resp.into_reader(), &mut buf).is_err()
                                || std::fs::write(&cached, &buf).is_err()
                            {
                                self.set_feedback("Download write failed",
                                    self.config.theme_colors.feedback_warn);
                                return;
                            }
                        }
                        Err(e) => {
                            self.set_feedback(&format!("HTTP error: {}", e),
                                self.config.theme_colors.feedback_warn);
                            return;
                        }
                    }
                }

                if open {
                    self.open_attachment_file(cached.to_string_lossy().as_ref(), name);
                } else {
                    // Save → copy cached file to user-chosen dest above.
                    if let Err(e) = std::fs::copy(&cached, &dest) {
                        self.set_feedback(&format!("Copy failed: {}", e),
                            self.config.theme_colors.feedback_warn);
                    } else {
                        self.set_feedback(&format!("Saved to {}", dest),
                            self.config.theme_colors.feedback_ok);
                    }
                }
                return;
            }
        }

        // External-sender path: if the attachment carries a file_id AND the
        // source has an open_attachment template configured, dispatch to it.
        // Covers non-maildir sources (e.g. workspace bridges) that resolve
        // attachments by server-side id.
        let file_id = att.get("file_id").and_then(|v| v.as_str()).map(|s| s.to_string());
        // Resolve the source via the threaded display→filtered mapping. Using
        // self.index raw read the wrong message in Folders view, so the
        // workspace open_attachment template wasn't found and the download
        // silently no-op'd (v0.1.181-class index bug).
        let plugin_type = self.current_filtered_index()
            .and_then(|i| self.filtered_messages.get(i))
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
                        self.open_attachment_file(&dest, name);
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

        // Fast path: `extract_mime_attachments` already decoded the
        // full attachment to a temp file and stashed its path in the
        // `source_file` field. Use it directly instead of paying for
        // a second Python-based MIME walk, which was both slower and
        // a constant source of subtle filename-matching bugs (folded
        // header names, base64-encoded filenames, etc.). The Python
        // re-extraction stays below as a fallback for attachment
        // payloads that don't carry `source_file` — those come from
        // other source plugins that populate `msg.attachments` with
        // their own field set.
        let cached = att.get("source_file").and_then(|v| v.as_str())
            .filter(|p| !p.is_empty() && std::path::Path::new(p).exists());
        if let Some(src) = cached {
            if let Err(e) = std::fs::copy(src, &dest) {
                self.set_feedback(&format!("Copy to {} failed: {}", dest, e),
                    self.config.theme_colors.feedback_warn);
                return;
            }
            if open {
                self.open_attachment_file(&dest, name);
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

        // Extract attachment using Python (always available, handles MIME properly).
        //
        // Filename normalisation: Python's `email.message.Message.get_filename()`
        // returns the value with the RFC 2822 folded newline still in
        // place (e.g. `"…_May\n 2026.docx"`). Our Rust side unfolds
        // headers before storing, so the strings disagree even though
        // they refer to the same file. Run both through the same
        // whitespace-collapse before comparing — that way a folded
        // filename matches its unfolded counterpart, and double
        // spaces inside a name are ignored too.
        //
        // The Index-fallback path was removed: when filename match
        // fails it would pick whatever the N-th `walk()` part happened
        // to be — typically the first inline image, since `walk()`
        // visits the multipart container plus every part in order,
        // independent of how the UI filters images. That produced
        // silent truncated-file extractions for any folded filename.
        // Now an unmatched name returns a clean failure the caller
        // can surface via `set_feedback`.
        let py_script = format!(
            r#"
import email, sys, re
def norm(s):
    return re.sub(r'\s+', ' ', s).strip() if s else ''
with open(sys.argv[1], 'rb') as f:
    msg = email.message_from_binary_file(f)
target = norm(sys.argv[2])
dest = sys.argv[3]
for part in msg.walk():
    if norm(part.get_filename()) == target and target:
        data = part.get_payload(decode=True)
        if data:
            with open(dest, 'wb') as out:
                out.write(data)
            sys.exit(0)
sys.exit(2)
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
        let tmp_dest = att_temp_path(name);

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

    /// Convert an office document (already extracted to `src`) to PDF
    /// via headless LibreOffice, returning the produced PDF path.
    /// Runs with a private user profile under ~/.kastrup/lo_profile —
    /// with the default profile, `--convert-to` silently no-ops
    /// whenever a LibreOffice GUI instance is running.
    fn office_to_pdf(&mut self, src: &str) -> Option<String> {
        if !std::path::Path::new(src).exists() {
            return None;
        }
        self.set_feedback("Converting to PDF…", self.config.theme_colors.feedback_info);
        let pdf = std::path::Path::new(src).with_extension("pdf");
        let _ = std::fs::remove_file(&pdf); // never serve a stale conversion
        let home = std::env::var("HOME").unwrap_or_default();
        let out = std::process::Command::new("soffice")
            .arg("--headless")
            .arg(format!("-env:UserInstallation=file://{}/.kastrup/lo_profile", home))
            .arg("--convert-to").arg("pdf")
            .arg("--outdir").arg("/tmp")
            .arg(src)
            .output();
        if out.is_ok() && pdf.exists() {
            Some(pdf.to_string_lossy().into_owned())
        } else {
            let err = out.err().map(|e| e.to_string())
                .unwrap_or_else(|| "no PDF produced".into());
            self.set_feedback(&format!("PDF conversion failed: {}", err),
                self.config.theme_colors.feedback_warn);
            None
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

        // In threaded (Folders) view self.index points into display_messages;
        // resolve the message's position in filtered_messages like `v` does.
        // Indexing filtered_messages[self.index] directly here made V inspect
        // the WRONG message in Folders view → "No images found" while the
        // count (rendered from the right message) said otherwise.
        let Some(fidx) = self.current_filtered_index() else { return; };

        // Ensure full content loaded
        if !self.filtered_messages[fidx].full_loaded {
            let msg_id = self.filtered_messages[fidx].id;
            if let Some((content, html)) = self.db.get_message_content(msg_id) {
                self.filtered_messages[fidx].content = content;
                self.filtered_messages[fidx].html_content = html;
                self.filtered_messages[fidx].full_loaded = true;
            }
        }
        // Same chat-URL → synthetic-attachment enrichment as `v`, so
        // a Slack image attachment is rendered inline by `V` exactly
        // like an email's image attachment.
        self.enrich_attachments_from_chat_urls();
        let msg = &self.filtered_messages[fidx];

        // Collect image URLs
        let mut urls: Vec<String> = Vec::new();

        // From attachments (Discord/chat)
        for att in &msg.attachments {
            let url = att.get("url").or_else(|| att.get("proxy_url")).and_then(|v| v.as_str());
            if let Some(url) = url {
                if url.starts_with("http") && is_image_attachment(att) {
                    urls.push(url.to_string());
                }
            } else if is_image_attachment(att) {
                // Local-file attachment (e.g. phone-gateway media synced to
                // disk): render straight from the path, no download.
                if let Some(p) = att.get("path").and_then(|v| v.as_str()) {
                    if std::path::Path::new(p).exists() {
                        urls.push(format!("file://{}", p));
                    }
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
        let msg = &self.filtered_messages[fidx];

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

        // From MIME image parts (inline embedded images). Use the same
        // in-memory extractor as the image COUNT (extract_mime_attachments)
        // so "N images" and what V/save collect can never disagree. The old
        // path shelled python at the on-disk maildir file, which failed
        // whenever that pointer was stale (message moved new/→cur/) even
        // though the count — reading msg.content — still saw the images.
        // Bonus: no python3 subprocess on every press.
        if urls.is_empty() && msg.content.contains("image/") {
            urls.extend(mime_image_file_urls(&msg.content, msg.id));
        }

        urls.dedup();

        if urls.is_empty() {
            self.set_feedback("No images found", self.config.theme_colors.feedback_info);
            return;
        }

        // Download to cache. Local files (file://) and already-cached URLs
        // are served instantly. Remote URLs go to a small thread pool so
        // 10 images aren't a 10×timeout serial wait on the main loop.
        let cache_dir = home_dir().join(".kastrup/image_cache");
        let _ = std::fs::create_dir_all(&cache_dir);

        // Pass 1: classify URLs (local / cached / needs-download).
        let mut paths: Vec<String> = Vec::new();
        let mut to_fetch: Vec<(String, std::path::PathBuf)> = Vec::new();
        for url in urls.iter().take(10) {
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
            to_fetch.push((url.clone(), cache_path));
        }

        // Pass 2: parallel download. Cap parallelism at 4 (matches typical
        // browser per-host connection limit and keeps memory + load sane).
        if !to_fetch.is_empty() {
            self.set_feedback(
                &format!("Loading {} image(s)...", paths.len() + to_fetch.len()),
                self.config.theme_colors.unread,
            );
            let workers = to_fetch.len().min(4);
            let queue = std::sync::Arc::new(std::sync::Mutex::new(to_fetch));
            let results = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
            let mut handles = Vec::with_capacity(workers);
            for _ in 0..workers {
                let q = queue.clone();
                let r = results.clone();
                handles.push(std::thread::spawn(move || {
                    loop {
                        let job = { q.lock().unwrap().pop() };
                        let Some((url, cache_path)) = job else { break; };
                        let agent = ureq::AgentBuilder::new()
                            .timeout_connect(std::time::Duration::from_secs(5))
                            .timeout_read(std::time::Duration::from_secs(10))
                            .build();
                        if let Ok(resp) = agent.get(&url).call() {
                            let mut bytes = Vec::new();
                            if std::io::Read::read_to_end(&mut resp.into_reader(), &mut bytes).is_ok()
                                && bytes.len() > 100
                            {
                                let _ = std::fs::write(&cache_path, &bytes);
                                r.lock().unwrap().push(cache_path.to_string_lossy().to_string());
                            }
                        }
                    }
                }));
            }
            for h in handles { let _ = h.join(); }
            let downloaded = std::mem::take(&mut *results.lock().unwrap());
            paths.extend(downloaded);
        } else if !paths.is_empty() {
            self.set_feedback(&format!("Loading {} image(s)...", paths.len()),
                              self.config.theme_colors.unread);
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
            } else if is_image_attachment(att) {
                // Local-file attachment (e.g. phone-gateway media synced to
                // disk): render straight from the path, no download.
                if let Some(p) = att.get("path").and_then(|v| v.as_str()) {
                    if std::path::Path::new(p).exists() {
                        urls.push(format!("file://{}", p));
                    }
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

        // Inline embedded images — same in-memory extractor as the count
        // and as toggle_inline_image (see mime_image_file_urls). Replaces
        // the old python-on-maildir-file path that broke on stale new/→cur/
        // pointers and disagreed with the displayed "N images" count.
        if urls.is_empty() && msg.content.contains("image/") {
            urls.extend(mime_image_file_urls(&msg.content, msg.id));
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

        // Honour the configured download folder (e.g. ~/Dl) instead of a
        // hardcoded ~/Downloads. Matches the single-attachment save path.
        let default = self.config.download_folder.replace(
            '~', &std::env::var("HOME").unwrap_or_default());
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

        let cache_dir = home_dir().join(".kastrup/image_cache");
        let _ = std::fs::create_dir_all(&cache_dir);

        // Pass 1: classify (local copy / cached copy / needs-download).
        // Cached and local copies happen synchronously since they're just
        // a copy() call.
        let mut saved = 0usize;
        let mut failed = 0usize;
        struct Job {
            url: String,
            cache_path: std::path::PathBuf,
            dest: std::path::PathBuf,
        }
        let mut jobs: Vec<Job> = Vec::new();
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
            jobs.push(Job { url: url.clone(), cache_path, dest });
        }

        // Pass 2: parallel download (cap 4 concurrent).
        if !jobs.is_empty() {
            let workers = jobs.len().min(4);
            let queue = std::sync::Arc::new(std::sync::Mutex::new(jobs));
            let counts = std::sync::Arc::new(std::sync::Mutex::new((0usize, 0usize))); // (saved, failed)
            let mut handles = Vec::with_capacity(workers);
            for _ in 0..workers {
                let q = queue.clone();
                let c = counts.clone();
                handles.push(std::thread::spawn(move || {
                    loop {
                        let job = { q.lock().unwrap().pop() };
                        let Some(Job { url, cache_path, dest }) = job else { break; };
                        let agent = ureq::AgentBuilder::new()
                            .timeout_connect(std::time::Duration::from_secs(5))
                            .timeout_read(std::time::Duration::from_secs(15))
                            .build();
                        let mut ok = false;
                        if let Ok(resp) = agent.get(&url).call() {
                            let mut bytes = Vec::new();
                            if std::io::Read::read_to_end(&mut resp.into_reader(), &mut bytes).is_ok()
                                && bytes.len() > 100
                            {
                                if std::fs::write(&dest, &bytes).is_ok() {
                                    let _ = std::fs::write(&cache_path, &bytes);
                                    ok = true;
                                }
                            }
                        }
                        let mut g = c.lock().unwrap();
                        if ok { g.0 += 1; } else { g.1 += 1; }
                    }
                }));
            }
            for h in handles { let _ = h.join(); }
            let g = counts.lock().unwrap();
            saved += g.0;
            failed += g.1;
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
    // Load AI/tool plugins from ~/.kastrup/plugins/
    fn load_ai_plugins(&self) -> Vec<(String, String, String)> {
        let dirs = [
            home_dir().join(".kastrup/plugins"),
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
        let (is_header, sender, subject, content) = match self.current_filtered_index().and_then(|i| self.filtered_messages.get(i)) {
            Some(m) => (
                m.is_header,
                m.display_name().to_string(),
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

        // Try claude CLI first, then curl to OpenAI.
        // stdin must be /dev/null: kastrup runs in raw-mode TTY, so
        // claude inherits that fd and prints "Warning: no stdin data
        // received in 3s, proceeding without it." onto stdout, which
        // contaminates the response we then try to parse.
        let result = std::process::Command::new("claude")
            .arg("-p")
            .arg(&ai_prompt)
            .stdin(std::process::Stdio::null())
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

        self.show_ai_response(
            &style::bold(&style::fg("AI Response", tc.view_custom)), &response);
        self.set_feedback("AI response shown in right pane", tc.feedback_ok);
    }

    /// Show an AI answer in the right pane. URLs become one-row OSC 8
    /// links (long labels shortened, so glass can click them despite
    /// its pane-blind row scan); the raw URLs are remembered in
    /// `ai_pane` so x / X follow the answer's links while it is on
    /// screen, and so the pane can be restored after the URL picker.
    fn show_ai_response(&mut self, header: &str, response: &str) {
        let urls = extract_message_urls(response);
        let linked: String = response.lines()
            .map(hyperlink_urls).collect::<Vec<_>>().join("\n");
        let text = format!("{}\n\n{}", header, linked);
        self.ai_pane = Some((text.clone(), urls));
        self.right.set_text(&text);
        self.right.ix = 0;
        self.right.full_refresh();
        if self.right.border { self.right.border_refresh(); }
    }

    /// Re-show the stored AI answer (after the URL picker overlay).
    fn restore_ai_pane(&mut self) {
        if let Some((text, _)) = &self.ai_pane {
            let text = text.clone();
            self.right.set_text(&text);
            self.right.full_refresh();
            if self.right.border { self.right.border_refresh(); }
        }
    }

    /// `c` — vim/scribe-style `:claude PROMPT`. Prompts the user, then
    /// delegates. Empty prompt cancels.
    fn claude_command(&mut self) {
        let user_prompt = self.prompt(":claude ", "");
        self.render_bottom_bar();
        if user_prompt.trim().is_empty() { return; }
        self.run_claude_with_prompt(&user_prompt);
    }

    /// Body of `:claude` — pipe the current message body + `user_prompt`
    /// through `claude -p`, show the response in the right pane. Used
    /// both by the `c` shortcut and by the `:` colon-command dispatch.
    fn run_claude_with_prompt(&mut self, user_prompt: &str) {
        let (is_header, msg_id, thread_id, sender, subject, content) = match self.current_filtered_index().and_then(|i| self.filtered_messages.get(i)) {
            Some(m) => (
                m.is_header,
                m.id,
                m.thread_id.clone(),
                m.display_name().to_string(),
                m.subject.as_deref().unwrap_or("").to_string(),
                if m.content.len() > 8000 { m.content[..8000].to_string() } else { m.content.clone() },
            ),
            None => return,
        };
        if is_header { return; }
        let user_prompt = user_prompt.to_string();

        let tc = self.config.theme_colors.clone();
        self.set_feedback("Asking claude…", tc.unread);
        // Force a footer paint so the user sees the status while
        // claude -p runs (can take 5-30s).
        use std::io::Write as _;
        let _ = std::io::stdout().flush();

        // Reference line so a Claude Code session with the kastrup
        // skill loaded can pull thread / sender history live. Plain
        // `claude -p` without the skill ignores this and works off the
        // inline content below — no regression either way.
        let mut ref_line = format!("Message reference: kastrup:{}", msg_id);
        if let Some(ref tid) = thread_id {
            if !tid.is_empty() {
                ref_line.push_str(&format!(" (thread: {})", tid));
            }
        }
        ref_line.push_str(" — pull thread / sender history via the kastrup skill if available.");

        let full_prompt = format!(
            "{}\n\n{}\n\nContext, email from {} about \"{}\":\n{}",
            user_prompt, ref_line, sender, subject, content
        );

        let result = std::process::Command::new("claude")
            .arg("-p")
            .arg(&full_prompt)
            .stdin(std::process::Stdio::null())
            .output();
        let response = if let Ok(output) = result {
            if output.status.success() {
                String::from_utf8_lossy(&output.stdout).to_string()
            } else {
                self.ai_fallback_openai(&full_prompt)
            }
        } else {
            self.ai_fallback_openai(&full_prompt)
        };
        if response.is_empty() {
            self.set_feedback("claude returned empty response", tc.feedback_warn);
            return;
        }

        let header = format!("{}\n{}",
            style::bold(&style::fg("claude", tc.view_custom)),
            style::fg(&format!("> {}", user_prompt), tc.unread));
        self.show_ai_response(&header, response.trim_end());
        self.set_feedback("claude response shown in right pane", tc.feedback_ok);
    }

    /// `S` — `:search`. Prompts the user, then delegates.
    fn search_command(&mut self) {
        let query = self.prompt(":search ", "");
        self.render_bottom_bar();
        if query.trim().is_empty() { return; }
        self.run_search_with_query(&query);
    }

    /// Body of `:search` — natural-language query → claude translates
    /// to a `Filters` JSON spec → filter pipeline runs the query
    /// against the kastrup DB. Live source list is included in the
    /// prompt so claude can resolve names. On parse failure, falls
    /// back to a content_pattern substring search using the raw query.
    fn run_search_with_query(&mut self, query: &str) {
        let query = query.to_string();
        let tc = self.config.theme_colors.clone();
        // Build a compact live source roster so claude can resolve names.
        // Also derive the distinct set of plugin_type values present in the
        // DB — `source_type` is filtered by EXACT plugin_type match, so the
        // prompt must use the actual schema values (e.g. "maildir" for
        // email accounts, not the friendly word "email").
        let mut roster = String::new();
        let mut types_set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for src in &self.sources_list {
            roster.push_str(&format!("- id={} type={} name=\"{}\"\n",
                src.id, src.plugin_type, src.name));
            types_set.insert(src.plugin_type.clone());
        }
        if roster.is_empty() {
            roster.push_str("(no sources registered)\n");
        }
        let types_list = if types_set.is_empty() {
            "(none)".to_string()
        } else {
            types_set.iter().map(|t| format!("\"{}\"", t))
                .collect::<Vec<_>>().join(" | ")
        };

        let system_prompt = format!(
            "You are a search assistant for kastrup, a unified messaging hub backed by a SQLite \
            kastrup.db. Given a user's natural-language query, output a single JSON object \
            matching this Rust struct (omit fields that aren't constrained):\n\
            \n\
            {{\n\
            \"source_id\": int|null,           // exact source id\n\
            \"source_ids\": [int]|null,        // list of source ids\n\
            \"is_read\": bool|null,            // true=only read, false=only unread\n\
            \"is_starred\": bool|null,\n\
            \"folder\": str|null,              // exact maildir folder name\n\
            \"sender_pattern\": str|null,      // SQL LIKE pattern, e.g. \"%bob%\"\n\
            \"source_type\": str|null,         // EXACT plugin_type (see roster); valid values: {}\n\
            \"content_pattern\": str|null      // SQL LIKE pattern matching subject+body\n\
            }}\n\
            \n\
            Rules:\n\
            - Output ONLY the JSON, no markdown fences, no commentary.\n\
            - Use SQL LIKE wildcards (%) liberally for substring matching.\n\
            - Preserve special characters (æ, ø, å, é, etc.) verbatim in patterns —\n\
              do NOT transliterate them to ASCII.\n\
            - For \"unread\" / \"new\" set is_read=false.\n\
            - For \"starred\" / \"flagged\" set is_starred=true.\n\
            - For \"email\" / \"mail\" set source_type to whichever roster type\n\
              represents email (typically \"maildir\"); never invent values not in\n\
              the valid-values list above.\n\
            - Only set source_type when the user explicitly constrains the\n\
              channel; sender_pattern alone is usually enough.\n\
            - Map source/channel mentions against the roster below.\n\
            \n\
            Available sources:\n{}\n\
            User query: {}",
            types_list, roster, query
        );

        self.set_feedback("Asking claude…", tc.unread);
        use std::io::Write as _;
        let _ = std::io::stdout().flush();

        let result = std::process::Command::new("claude")
            .arg("-p")
            .arg(&system_prompt)
            .stdin(std::process::Stdio::null())
            .output();
        let raw = if let Ok(output) = result {
            if output.status.success() {
                String::from_utf8_lossy(&output.stdout).to_string()
            } else {
                self.ai_fallback_openai(&system_prompt)
            }
        } else {
            self.ai_fallback_openai(&system_prompt)
        };
        if raw.trim().is_empty() {
            self.set_feedback("claude returned empty response", tc.feedback_warn);
            return;
        }

        // Robust JSON extraction: locate the outermost `{ … }` and
        // parse just that. This survives markdown fences, leading
        // warnings (the 3-s "no stdin data" notice was the original
        // 0-result trigger), and any other preamble/postamble noise.
        // Walk the bytes tracking quote state so a `}` inside a string
        // doesn't close the object early.
        let json_slice = extract_json_object(&raw);
        let mut filters = match json_slice.and_then(|s|
            serde_json::from_str::<serde_json::Value>(s).ok())
        {
            Some(v) => self.filters_from_json(&v),
            None => {
                log::info(&format!(
                    "search: claude response unparseable, falling back to substring. raw={:?}",
                    raw.chars().take(300).collect::<String>()
                ));
                // Fallback: treat the raw query as a content substring.
                let mut f = Filters::default();
                f.content_pattern = Some(format!("%{}%", query));
                f
            }
        };
        // Belt-and-braces: if claude returned a totally empty filter,
        // fall back to substring search instead of returning the whole
        // DB.
        let any_set = filters.source_id.is_some()
            || filters.source_ids.is_some()
            || filters.is_read.is_some()
            || filters.is_starred.is_some()
            || filters.folder.is_some()
            || filters.sender_pattern.is_some()
            || filters.source_type.is_some()
            || filters.content_pattern.is_some();
        if !any_set {
            filters.content_pattern = Some(format!("%{}%", query));
        }

        // Apply + persist so the 5s refresh doesn't blank the results.
        self.filtered_messages = self.db.get_messages(&filters, 500, 0);
        for msg in &mut self.filtered_messages {
            resolve_source_type(&self.source_type_map, msg);
        }
        self.index = 0;
        let n = self.filtered_messages.len();
        let summary = self.filter_summary(&filters);
        self.active_search_label = format!(":search “{}”", query);
        self.active_search_filter = Some(filters);
        // Threaded view renders display_messages — rebuild it from the
        // search hits or the pane keeps showing the old sections.
        self.rebuild_display();
        self.set_feedback(
            &format!(":search → {} match{}  [{}]  (Esc clears)",
                n,
                if n == 1 { "" } else { "es" },
                summary),
            tc.feedback_ok);
        self.render_all();
    }

    /// Decode a JSON object (claude's response) into a `Filters` struct.
    /// Unknown / missing fields are left as None.
    fn filters_from_json(&self, v: &serde_json::Value) -> Filters {
        let mut f = Filters::default();
        if let Some(x) = v.get("source_id").and_then(|x| x.as_i64()) { f.source_id = Some(x); }
        if let Some(arr) = v.get("source_ids").and_then(|x| x.as_array()) {
            let ids: Vec<i64> = arr.iter().filter_map(|e| e.as_i64()).collect();
            if !ids.is_empty() { f.source_ids = Some(ids); }
        }
        if let Some(x) = v.get("is_read").and_then(|x| x.as_bool()) { f.is_read = Some(x); }
        if let Some(x) = v.get("is_starred").and_then(|x| x.as_bool()) { f.is_starred = Some(x); }
        if let Some(x) = v.get("folder").and_then(|x| x.as_str()) {
            if !x.is_empty() { f.folder = Some(x.to_string()); }
        }
        if let Some(x) = v.get("sender_pattern").and_then(|x| x.as_str()) {
            if !x.is_empty() { f.sender_pattern = Some(x.to_string()); }
        }
        if let Some(x) = v.get("source_type").and_then(|x| x.as_str()) {
            if !x.is_empty() { f.source_type = Some(x.to_string()); }
        }
        if let Some(x) = v.get("content_pattern").and_then(|x| x.as_str()) {
            if !x.is_empty() { f.content_pattern = Some(x.to_string()); }
        }
        f
    }

    /// Compact one-line summary of which Filters fields are active —
    /// shown in the feedback bar after `:search` so the user can see
    /// how claude interpreted their query.
    fn filter_summary(&self, f: &Filters) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(id) = f.source_id { parts.push(format!("source={}", id)); }
        if let Some(ref ids) = f.source_ids { parts.push(format!("sources={:?}", ids)); }
        if let Some(b) = f.is_read { parts.push(format!("read={}", b)); }
        if let Some(b) = f.is_starred { parts.push(format!("starred={}", b)); }
        if let Some(ref s) = f.folder { parts.push(format!("folder={}", s)); }
        if let Some(ref s) = f.sender_pattern { parts.push(format!("from={}", s)); }
        if let Some(ref s) = f.source_type { parts.push(format!("type={}", s)); }
        if let Some(ref s) = f.content_pattern { parts.push(format!("text={}", s)); }
        if parts.is_empty() { "no filter".to_string() } else { parts.join(" ") }
    }

    /// `:` — generic colon-command prompt. Lets the user type any
    /// command verb explicitly instead of hunting for the shortcut
    /// (`:claude PROMPT`, `:search QUERY`, `:chat`, `:q`/`:quit`). Each
    /// verb dispatches to the same code path as its shortcut, so
    /// behaviour stays identical regardless of how the user invoked it.
    /// Unknown verbs surface a feedback warning rather than failing
    /// silently.
    fn colon_command(&mut self) {
        let raw = self.prompt(":", "");
        self.render_bottom_bar();
        let line = raw.trim();
        if line.is_empty() { return; }
        // Split into verb + remainder. `splitn(2, char::is_whitespace)`
        // preserves the rest of the line as-is so prompts that include
        // their own colons / arguments aren't mangled.
        let mut it = line.splitn(2, char::is_whitespace);
        let verb = it.next().unwrap_or("");
        let rest = it.next().unwrap_or("").trim_start();
        match verb {
            "claude" => {
                if rest.is_empty() {
                    self.set_feedback(":claude needs a prompt", self.config.theme_colors.feedback_warn);
                } else {
                    self.run_claude_with_prompt(rest);
                }
            }
            "chat" => { self.chat_command(); }
            "search" => {
                if rest.is_empty() {
                    self.set_feedback(":search needs a query", self.config.theme_colors.feedback_warn);
                } else {
                    self.run_search_with_query(rest);
                }
            }
            "triage" => { self.show_triage_history(); }
            "views" => { self.show_views_screen(); }
            "q" | "quit" => {
                if self.pending_send.is_some() {
                    self.set_feedback(
                        "A send is still in flight — :Q to force-quit, or wait",
                        self.config.theme_colors.feedback_warn,
                    );
                } else {
                    self.running = false;
                }
            }
            "Q" => { self.running = false; }
            other => {
                self.set_feedback(&format!("unknown command: {}", other),
                    self.config.theme_colors.feedback_warn);
            }
        }
    }

    /// `C` — scribe-style `:chat`. Snapshots the current message to a
    /// tempfile, suspends kastrup, execs `claude` interactively with
    /// an initial prompt that points at the snapshot. On exit, the
    /// terminal is handed back to kastrup and the snapshot is removed.
    fn chat_command(&mut self) {
        let (is_header, msg_id, thread_id, sender, subject, content) = match self.current_filtered_index().and_then(|i| self.filtered_messages.get(i)) {
            Some(m) => (
                m.is_header,
                m.id,
                m.thread_id.clone(),
                m.display_name().to_string(),
                m.subject.as_deref().unwrap_or("").to_string(),
                m.content.clone(),
            ),
            None => return,
        };
        if is_header {
            self.set_feedback(":chat needs a message selected", self.config.theme_colors.feedback_warn);
            return;
        }

        let pid = std::process::id();
        let tmpfile = format!("/tmp/kastrup-chat-{}.txt", pid);
        let snapshot = format!(
            "kastrup:{}\nFrom: {}\nSubject: {}\n\n{}\n",
            msg_id, sender, subject, content
        );
        if std::fs::write(&tmpfile, &snapshot).is_err() {
            self.set_feedback("could not write chat snapshot", self.config.theme_colors.feedback_warn);
            return;
        }

        // Reference the message both by its kastrup:ID handle (so the
        // kastrup skill can pull thread / sender history / related
        // messages live from the DB) and by the inline tempfile snapshot
        // (a no-tools fallback). Thread id, when present, lets the skill
        // pull the rest of the thread without re-deriving it.
        let thread_hint = match thread_id.as_deref() {
            Some(tid) if !tid.is_empty() => format!(" Thread id: {}.", tid),
            _ => String::new(),
        };
        let initial = format!(
            "I'm reading email kastrup:{} in kastrup. Use the kastrup skill to pull \
            thread / sender history / related messages from the DB when useful.{} \
            The current message body is also snapshotted to {} for quick reference. \
            When you're done, /exit returns me to kastrup.",
            msg_id, thread_hint, tmpfile
        );

        // Hand the terminal off to claude. Bracketed-paste mode would
        // interfere with claude's input handling, so disable it for
        // the duration. Mirrors scribe's run_chat_session.
        use std::io::Write as _;
        Crust::disable_bracketed_paste();
        let _ = std::io::stdout().flush();
        Crust::cleanup();
        Crust::clear_screen();

        let _ = std::process::Command::new("claude")
            .arg(&initial)
            .status();

        // Restore kastrup's terminal state and force a full repaint.
        Crust::init();
        Crust::enable_bracketed_paste();
        let _ = std::io::stdout().flush();
        let _ = std::fs::remove_file(&tmpfile);
        self.handle_resize();
        self.set_feedback("back from chat", self.config.theme_colors.feedback_ok);
    }

    fn run_ai_plugin(&mut self, label: &str, command: &str) {
        log::info(&format!("Running plugin: {}", label));
        let pick_file = format!("/tmp/kastrup_plugin_{}.txt", std::process::id());
        let _ = std::fs::remove_file(&pick_file);
        let cmd = command.replace("%{pick_file}", &pick_file);
        Crust::cleanup();
        Crust::clear_screen();
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
                    let name = msg.display_name().to_string();
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

    /// Read Tock's calendar list and let the user pick one. Returns the
    /// chosen calendar id (Enter on empty input = the configured
    /// default), or None if the user cancels with ESC. Returns Some(1)
    /// silently when Tock's DB or config can't be read so Z still
    /// works in fresh installs.
    fn pick_tock_calendar(&mut self, tock_home: &std::path::Path) -> Option<i64> {
        // Calendars from tock.db
        let db_path = tock_home.join("tock.db");
        let calendars: Vec<(i64, String)> = rusqlite::Connection::open(&db_path)
            .ok()
            .and_then(|c| {
                let mut stmt = c.prepare(
                    "SELECT id, name FROM calendars WHERE enabled = 1 ORDER BY id"
                ).ok()?;
                let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
                    .ok()?
                    .filter_map(|r| r.ok())
                    .collect::<Vec<_>>();
                Some(rows)
            })
            .unwrap_or_default();
        if calendars.is_empty() { return Some(1); }

        // Default from ~/.tock/config.yml: line "default_calendar: <n>"
        let default_id = std::fs::read_to_string(tock_home.join("config.yml"))
            .ok()
            .and_then(|s| {
                s.lines().find_map(|l| {
                    l.trim().strip_prefix("default_calendar:")
                        .and_then(|v| v.trim().parse::<i64>().ok())
                })
            })
            .unwrap_or(calendars[0].0);
        let default_ix = calendars.iter().position(|(id, _)| *id == default_id)
            .unwrap_or(0);

        let tc = self.config.theme_colors.clone();
        let mut lines = vec![
            style::bold(&style::fg("Pick Tock calendar:", tc.unread)),
            String::new(),
        ];
        for (i, (_, name)) in calendars.iter().enumerate() {
            let marker = if i == default_ix { "→" } else { " " };
            lines.push(format!(" {} {:>3}. {}", marker, i + 1, name));
        }
        lines.push(String::new());
        lines.push(style::fg("Enter number, Enter=default, ESC=cancel", 245));
        self.right.set_text(&lines.join("\n"));
        self.right.full_refresh();

        let input = self.prompt(&format!("Calendar # [{}]: ", default_ix + 1), "");
        let trimmed = input.trim();
        if trimmed.is_empty() {
            Some(calendars[default_ix].0)
        } else if let Ok(n) = trimmed.parse::<usize>() {
            if n >= 1 && n <= calendars.len() { Some(calendars[n - 1].0) } else { None }
        } else {
            None
        }
    }

    // Batch L: Calendar/Tock
    fn open_in_tock(&mut self) {
        let msg = match self.current_filtered_index().and_then(|i| self.filtered_messages.get(i)) {
            Some(m) => m.clone(),
            None => return,
        };

        let home = std::env::var("HOME").unwrap_or_default();
        let tock_home = std::path::PathBuf::from(&home).join(".tock");
        if !tock_home.is_dir() {
            self.set_feedback("Tock not configured (~/.tock missing)", self.config.theme_colors.feedback_warn);
            return;
        }

        // Source 1: ICS attachment in the maildir file.
        let mut date_ymd: Option<(i32, u32, u32)> = None;
        let mut time_hm: Option<(u32, u32)> = None;
        let mut ics_passthrough: Option<String> = None;
        if let Some(file) = msg.metadata.get("maildir_file").and_then(|v| v.as_str()) {
            if std::path::Path::new(file).exists() {
                if let Ok(content) = std::fs::read_to_string(file) {
                    if let Some(vevent_start) = content.find("BEGIN:VEVENT") {
                        let vevent = &content[vevent_start..];
                        for line in vevent.lines() {
                            let l = line.trim();
                            if l.starts_with("DTSTART") {
                                if let Some(colon) = l.find(':') {
                                    let val = &l[colon + 1..];
                                    if val.len() >= 8 {
                                        let y: i32 = val[0..4].parse().unwrap_or(0);
                                        let m: u32 = val[4..6].parse().unwrap_or(0);
                                        let d: u32 = val[6..8].parse().unwrap_or(0);
                                        if y > 0 && m > 0 && d > 0 {
                                            date_ymd = Some((y, m, d));
                                        }
                                        if val.len() >= 13 {
                                            let h: u32 = val[9..11].parse().unwrap_or(0);
                                            let mi: u32 = val[11..13].parse().unwrap_or(0);
                                            if h < 24 && mi < 60 { time_hm = Some((h, mi)); }
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                    // If the message itself is a full ICS file, pass it
                    // through verbatim instead of synthesising one.
                    if content.starts_with("BEGIN:VCALENDAR") {
                        ics_passthrough = Some(content);
                    }
                }
            }
        }

        // Source 2: scan the rendered message body for a future date.
        if date_ymd.is_none() {
            let body_text = self.get_display_content(&msg);
            if let Some((y, m, d, time)) = scan_for_future_event(&body_text) {
                date_ymd = Some((y, m, d));
                time_hm = time;
            }
        }

        // Source 3: fall back to the message arrival timestamp (date only).
        if date_ymd.is_none() && msg.timestamp > 0 {
            let local_ts = msg.timestamp + local_utc_offset();
            let days = local_ts.div_euclid(86400);
            let (y, m, d) = days_to_ymd(days);
            date_ymd = Some((y as i32, m as u32, d as u32));
        }

        let Some((y, m, d)) = date_ymd else {
            self.set_feedback("Could not determine date", self.config.theme_colors.feedback_warn);
            return;
        };

        // Ask which calendar to add the event to. ESC cancels the whole
        // action so we don't insert into the wrong calendar.
        let cal_id = match self.pick_tock_calendar(&tock_home) {
            Some(id) => id,
            None => {
                self.set_feedback("Z cancelled", self.config.theme_colors.feedback_info);
                return;
            }
        };

        // Drop an ICS file in ~/.tock/incoming/ so Tock picks it up as an
        // event on the resolved date. This is a Z-triggered, explicit
        // user action — kastrup never creates events on its own.
        let incoming = tock_home.join("incoming");
        let _ = std::fs::create_dir_all(&incoming);
        let subject = msg.subject.clone().unwrap_or_else(|| "(no subject)".into());
        let path = incoming.join(format!("kastrup_msg_{}.ics", msg.id));
        let snippet: String = self.get_display_content(&msg).lines()
            .find(|l| !l.trim().is_empty()).unwrap_or("").chars().take(200).collect();
        let ics_body = if let Some(passthrough) = ics_passthrough {
            inject_tock_calendar_id(&passthrough, cal_id)
        } else {
            let uid = format!("kastrup-{}-{}", msg.id, y * 10000 + m as i32 * 100 + d as i32);
            inject_tock_calendar_id(
                &build_ics_event(&uid, &subject, &snippet, y, m, d, time_hm),
                cal_id,
            )
        };
        let event_written = std::fs::write(&path, ics_body).is_ok();

        // Also write goto so a running Tock navigates to the day.
        let date = format!("{:04}-{:02}-{:02}", y, m, d);
        let goto_path = tock_home.join("goto");
        let _ = std::fs::write(&goto_path, &date);

        let time_lbl = time_hm.map(|(h, mi)| format!(" {:02}:{:02}", h, mi)).unwrap_or_default();
        let verb = if event_written { "Event sent to Tock" } else { "Sent to Tock" };
        self.set_feedback(
            &format!("{}: {}{}", verb, date, time_lbl),
            self.config.theme_colors.feedback_ok,
        );
    }

    /// AI triage — Ctrl+t. Shells out to ~/.kastrup/triage.sh which
    /// calls `claude --print` with the message context; gets back a
    /// JSON array of action objects (calendar / todo / clarify);
    /// shows a multi-pick preview; commits selected actions to tock
    /// (ICS in ~/.tock/incoming/) and/or ~/.tasks/todo.hl.
    fn triage_message(&mut self) {
        let tc = self.config.theme_colors.clone();

        // Resolve the cursor to a real message (skip section headers
        // in folders/threaded view — same lookup as the save flow).
        let cursor_id = if self.show_threaded {
            self.display_messages.get(self.index)
                .filter(|m| !m.is_header).map(|m| m.id)
        } else {
            self.filtered_messages.get(self.index).map(|m| m.id)
        };
        let msg_id = match cursor_id {
            Some(id) => id,
            None => {
                self.set_feedback("Triage: no message under cursor",
                    tc.feedback_warn);
                return;
            }
        };
        let msg = match self.filtered_messages.iter().find(|m| m.id == msg_id) {
            Some(m) => m.clone(),
            None => return,
        };

        // Optional user-supplied hint. Enter with empty input skips
        // (Claude triages from the message body alone). The hint is
        // useful when the actionable item isn't IN the body — e.g.
        // the message is a secure-PDF link the user wants to follow
        // up on by a specific date. ESC at the hint prompt cancels
        // the whole triage BEFORE spending tokens on a Claude call
        // the user no longer wants.
        let hint = self.prompt(
            "Triage hint (Enter to skip, ESC to cancel): ", "");
        if self.bottom.last_escaped {
            self.set_feedback("Triage cancelled", tc.feedback_info);
            self.render_bottom_bar();
            return;
        }
        self.render_bottom_bar();

        // Build context JSON for the wrapper.
        let body = self.get_display_content(&msg);
        let body_short: String = body.chars().take(4000).collect();
        let today = {
            let secs = database::now_secs() + local_utc_offset();
            let (y, m, d) = days_to_ymd(secs / 86400);
            format!("{:04}-{:02}-{:02}", y, m, d)
        };
        let home = std::env::var("HOME").unwrap_or_default();
        let todo_path = std::path::PathBuf::from(&home).join(".tasks/todo.hl");
        let mut context = serde_json::json!({
            "subject":    msg.subject.clone().unwrap_or_default(),
            "sender":     msg.sender_name.clone().unwrap_or_else(|| msg.sender.clone()),
            "folder":     msg.folder.clone().unwrap_or_default(),
            "body":       body_short,
            "today":      today,
            "tz":         "Europe/Oslo",
            "calendars":  triage::read_calendars(),
            "categories": triage::read_categories(&todo_path),
        });
        if !hint.trim().is_empty() {
            context["user_hint"] = serde_json::json!(hint.trim());
        }

        self.set_feedback("Triaging with Claude... (~5s)", tc.unread);
        self.render_bottom_bar();

        let actions = match triage::run_triage(&context.to_string()) {
            Ok(a) => a,
            Err(e) => {
                self.set_feedback(&format!("Triage failed: {}", e),
                    tc.feedback_warn);
                return;
            }
        };

        if actions.is_empty() {
            self.set_feedback("Triage: no actionable items found",
                tc.feedback_info);
            // Still log the call so the history shows "ran but nothing
            // to commit" — useful debugging when you press z and
            // nothing happens.
            let _ = triage::append_log(triage::LogEntry {
                msg_id,
                folder: &msg.folder.clone().unwrap_or_default(),
                sender: &msg.sender_name.clone().unwrap_or_else(|| msg.sender.clone()),
                subject: msg.subject.as_deref().unwrap_or(""),
                hint: if hint.trim().is_empty() { None } else { Some(hint.trim()) },
                results: &[],
            });
            return;
        }

        self.triage_preview_and_commit(msg_id, &msg, hint.trim().to_string(), actions, &todo_path);
    }

    /// Multi-pick preview screen for triage actions.
    /// Space toggles, j/k moves, Enter commits selected, Esc cancels.
    fn triage_preview_and_commit(
        &mut self,
        msg_id: i64,
        msg: &message::Message,
        hint: String,
        actions: Vec<triage::Action>,
        todo_path: &std::path::Path,
    ) {
        let tc = self.config.theme_colors.clone();
        // Pre-select all non-clarify items.
        let mut selected: Vec<bool> = actions.iter()
            .map(|a| !matches!(a, triage::Action::Clarify { .. }))
            .collect();
        let mut cursor = 0usize;

        loop {
            // Render preview into the right pane.
            let mut lines: Vec<String> = Vec::new();
            lines.push(style::bold(&style::fg(
                &format!("Triage — message #{} ({} actions)",
                    msg_id, actions.len()),
                tc.view_custom)));
            lines.push(String::new());
            for (i, a) in actions.iter().enumerate() {
                let marker = if selected[i] { "[x]" } else { "[ ]" };
                let arrow = if i == cursor { "→ " } else { "  " };
                let label = a.short_label();
                let line = format!("{}{} {}", arrow, marker, label);
                lines.push(if i == cursor {
                    style::bold(&style::fg(&line, tc.unread))
                } else {
                    style::fg(&line, tc.info_fg)
                });
            }
            lines.push(String::new());
            lines.push(style::fg(
                "Space:toggle  j/k:move  Enter:commit selected  Esc:cancel",
                tc.hint_fg));
            self.right.set_text(&lines.join("\n"));
            self.right.ix = 0;
            self.right.full_refresh();
            if self.right.border { self.right.border_refresh(); }

            let Some(key) = Input::getchr(None) else { continue };
            match key.as_str() {
                "ESC" | "q" => {
                    self.set_feedback("Triage cancelled", tc.feedback_info);
                    self.render_all();
                    return;
                }
                "j" | "DOWN" => {
                    if cursor + 1 < actions.len() { cursor += 1; }
                }
                "k" | "UP" => {
                    if cursor > 0 { cursor -= 1; }
                }
                " " | "SPACE" => {
                    selected[cursor] = !selected[cursor];
                }
                "ENTER" => break,
                _ => {}
            }
        }

        // Commit selected actions. Collect (action, status) tuples so
        // we can log the whole decision at the end — including the
        // unselected items, which show as "skipped" in the log so the
        // user can see what they declined.
        let mut committed = 0u32;
        let mut failed = 0u32;
        let home = std::env::var("HOME").unwrap_or_default();
        let tock_home = std::path::PathBuf::from(&home).join(".tock");
        let mut log_results: Vec<(triage::Action, String)> = Vec::new();

        let mut committed_cal = 0usize;
        let mut committed_todo = 0usize;
        for (i, a) in actions.iter().enumerate() {
            if !selected[i] {
                log_results.push((a.clone(), "skipped".to_string()));
                continue;
            }
            let res: Result<&'static str, String> = match a {
                triage::Action::Todo { category, text } => {
                    triage::append_todo(todo_path, category, text)
                        .map(|_| "todo")
                }
                triage::Action::Calendar { title, when, duration_min, calendar } => {
                    self.commit_calendar(msg_id, &tock_home,
                        title, when, *duration_min, calendar.as_deref())
                        .map(|_| "calendar")
                }
                triage::Action::Clarify { question } => {
                    Err(format!("clarify needs manual follow-up: {}", question))
                }
            };
            match res {
                Ok("calendar") => { committed += 1; committed_cal += 1;
                    log_results.push((a.clone(), "committed".to_string())); }
                Ok(_)          => { committed += 1; committed_todo += 1;
                    log_results.push((a.clone(), "committed".to_string())); }
                Err(e) => {
                    failed += 1;
                    log::info(&format!("triage commit failed: {}", e));
                    log_results.push((a.clone(), format!("failed: {}", e)));
                }
            }
        }

        // Persist the decision so :triage can show recent history.
        let _ = triage::append_log(triage::LogEntry {
            msg_id,
            folder: &msg.folder.clone().unwrap_or_default(),
            sender: &msg.sender_name.clone().unwrap_or_else(|| msg.sender.clone()),
            subject: msg.subject.as_deref().unwrap_or(""),
            hint: if hint.is_empty() { None } else { Some(&hint) },
            results: &log_results,
        });

        // Spell out where the commits actually went so the user knows
        // tock events are queued (visible only after tock runs
        // watch_incoming on next start / idle tick), while todos are
        // live in ~/.tasks/todo.hl immediately.
        let mut parts: Vec<String> = Vec::new();
        if committed_cal > 0 {
            parts.push(format!("{} queued for tock (open tock to import)",
                committed_cal));
        }
        if committed_todo > 0 {
            parts.push(format!("{} appended to todo.hl", committed_todo));
        }
        if committed == 0 && failed == 0 {
            parts.push("nothing selected".to_string());
        }
        if failed > 0 {
            parts.push(format!("{} failed (see log)", failed));
        }
        let summary = format!("Triage: {}", parts.join("; "));
        let color = if failed > 0 {
            tc.feedback_warn
        } else {
            tc.feedback_ok
        };
        self.set_feedback(&summary, color);
        self.render_all();
    }

    /// Build a single-event ICS and drop it in ~/.tock/incoming/.
    /// `when` is ISO8601 ("YYYY-MM-DDTHH:MM:SS+HH:MM"); we parse the
    /// date + time components and let local_utc_offset() resolve TZ
    /// implicitly via the existing ICS path. Returns a label on success.
    fn commit_calendar(
        &mut self,
        msg_id: i64,
        tock_home: &std::path::Path,
        title: &str,
        when: &str,
        duration_min: u32,
        calendar: Option<&str>,
    ) -> Result<String, String> {
        // Parse ISO8601 "YYYY-MM-DDTHH:MM:SS..." — first 19 chars.
        if when.len() < 16 {
            return Err(format!("invalid when: {}", when));
        }
        let y: i32 = when[0..4].parse().map_err(|_| "bad year")?;
        let m: u32 = when[5..7].parse().map_err(|_| "bad month")?;
        let d: u32 = when[8..10].parse().map_err(|_| "bad day")?;
        let h: u32 = when[11..13].parse().map_err(|_| "bad hour")?;
        let mi: u32 = when[14..16].parse().map_err(|_| "bad minute")?;

        // Resolve calendar name → numeric id via tock.db. Both
        // lookups reject local calendars, so we can't fall back to
        // id=1 (Personal) — that would defeat "show up on my phone".
        // If tock has no cloud calendars at all, surface as an error
        // instead of silently dropping into a local-only event.
        let cal_id = calendar
            .and_then(|name| triage_lookup_calendar_id(tock_home, name))
            .or_else(|| triage_default_calendar_id(tock_home))
            .ok_or_else(|| "no cloud calendar available in tock.db".to_string())?;

        let incoming = tock_home.join("incoming");
        let _ = std::fs::create_dir_all(&incoming);
        let path = incoming.join(format!("kastrup_triage_{}_{}.ics",
            msg_id, when.replace(':', "").replace('+', "p")));

        let uid = format!("kastrup-triage-{}-{}", msg_id,
            y * 10000 + m as i32 * 100 + d as i32);
        let ics = inject_tock_calendar_id(
            &build_ics_event_dur(&uid, title, "",
                y, m, d, Some((h, mi)), duration_min),
            cal_id,
        );
        std::fs::write(&path, ics)
            .map_err(|e| format!("write ics: {}", e))?;

        // Nudge tock to navigate to this date.
        let goto_path = tock_home.join("goto");
        let _ = std::fs::write(&goto_path,
            format!("{:04}-{:02}-{:02}", y, m, d));

        Ok(format!("calendar: {} ({:04}-{:02}-{:02} {:02}:{:02})",
            title, y, m, d, h, mi))
    }

    /// `:triage` — display the contents of ~/.kastrup/triage.log in
    /// the right pane. The log holds the most recent 20 triage
    /// decisions (z-key invocations) — one block per call showing
    /// what message it was, the user's hint (if any), and each
    /// resulting action with its commit status. Esc returns to the
    /// message view.
    fn show_triage_history(&mut self) {
        let tc = self.config.theme_colors.clone();
        let home = std::env::var("HOME").unwrap_or_default();
        let path = std::path::PathBuf::from(&home).join(".kastrup/triage.log");
        let body = std::fs::read_to_string(&path).unwrap_or_else(|_|
            "(no triage history yet — press z on a message to triage with Claude)".to_string());

        let header = style::bold(&style::fg(
            "Triage history (most recent 20)", tc.view_custom));
        self.right.set_text(&format!("{}\n\n{}", header, body));
        self.right.ix = 0;
        self.right.full_refresh();
        if self.right.border { self.right.border_refresh(); }
        self.set_feedback("Press any key to return", tc.hint_fg);
        let _ = Input::getchr(None);
        self.render_all();
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

/// Find the outermost JSON object literal in `s` and return a slice
/// covering it (inclusive of the braces). Walks bytes tracking string
/// state so a brace inside `"…"` doesn't close the object early.
/// Returns None if the input has no balanced object.
fn extract_json_object(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    let start = bytes.iter().position(|&b| b == b'{')?;
    let mut depth: i32 = 0;
    let mut in_str = false;
    let mut escape = false;
    for (i, &b) in bytes.iter().enumerate().skip(start) {
        if escape { escape = false; continue; }
        if in_str {
            match b {
                b'\\' => escape = true,
                b'"'  => in_str = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_str = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 { return Some(&s[start..=i]); }
            }
            _ => {}
        }
    }
    None
}

/// The multipart walk lives in the shared crate now. The calendar
/// renderer stays here: how an invite should look depends on the
/// display and on taste, and the crate deliberately holds no colours.
fn extract_mime_text(raw: &str) -> Option<String> {
    mail::mime::extract_mime_text_with(raw, &parse_ical_summary)
}



/// Pick the most useful HTML representation of a message for the
/// "open in scroll / browser" path. Tries, in order:
///   1. `msg.html_content` if it has a real body (catches truncated
///      DB rows that contain only a DOCTYPE — those are useless and
///      we want to fall through).
///   2. MIME-extracted HTML from raw content (text/html part of a
///      multipart payload).
///   3. Raw content if it looks like HTML on its own.
///   4. Plain text wrapped in a minimal HTML page so the user sees
///      *something* rather than a blank scroll.
/// Returns None only if there's literally no message body at all.
fn best_html_for_message(msg: &Message) -> Option<String> {
    let has_real_body = |s: &str| -> bool {
        let lower = s.to_ascii_lowercase();
        lower.contains("<body") && s.len() > 200
    };
    if let Some(h) = msg.html_content.as_ref() {
        if has_real_body(h) { return Some(h.clone()); }
        // else: fall through — the row is degenerate (e.g. DOCTYPE only)
    }
    if msg.content.contains("Content-Type:")
        || msg.content.lines().any(|l| l.starts_with("--") && l.len() > 5)
    {
        if let Some(h) = extract_mime_html(&msg.content) {
            if has_real_body(&h) || !h.is_empty() { return Some(h); }
        }
    }
    let trimmed = msg.content.trim_start();
    if trimmed.starts_with("<html") || trimmed.starts_with("<body") || trimmed.starts_with('<') {
        if has_real_body(&msg.content) { return Some(msg.content.clone()); }
    }
    // Headerless QP-encoded single-part text/html body. Older rows
    // came in through a maildir parser that dropped Content-Type /
    // Content-Transfer-Encoding, leaving the body raw — `=\n` soft
    // breaks and `=3D`-style escapes intact, and starting with the
    // greeting line ("Hi X,<p>...") rather than a `<` tag. The
    // starts-with check above misses these. Sniff for QP + HTML
    // tags, decode, and wrap in a minimal <html><body> if needed
    // so scroll / ff get a complete document.
    if looks_qp_html(&msg.content) {
        let decoded = decode_quoted_printable(&msg.content);
        if looks_like_html(&decoded) {
            let wrapped = if decoded.to_ascii_lowercase().contains("<body") {
                decoded
            } else {
                format!(
                    "<html><head><meta charset=\"utf-8\"></head><body>{}</body></html>",
                    decoded
                )
            };
            return Some(wrapped);
        }
    }
    // Headerless base64-encoded HTML body. Some senders (e.g.
    // DocuSign / Signant notifications) ship the entire mail as
    // `Content-Transfer-Encoding: base64` with no multipart wrapper,
    // so by the time we land here `msg.content` is a wall of base64.
    // Decode + sniff for an HTML-shaped body; if it looks like one,
    // hand the decoded HTML to scroll instead of pre-formatting the
    // base64 source.
    if looks_base64(&msg.content) {
        if let Some(decoded) = sources::maildir::base64_decode(msg.content.trim())
            .and_then(|bytes| String::from_utf8(bytes).ok())
        {
            if has_real_body(&decoded) { return Some(decoded); }
        }
    }
    // Plain-text fallback. Wrap in a minimal HTML page so even a
    // text-only newsletter renders as something the user can read. Bare
    // http(s) URLs become real <a href> anchors so scroll (and a browser)
    // can follow them — otherwise the obvious link is just dead text.
    if msg.content.trim().is_empty() { return None; }
    let body = linkify_plain_to_html(&msg.content);
    Some(format!(
        "<html><head><meta charset=\"utf-8\"></head><body><pre>{}</pre></body></html>",
        body
    ))
}

/// HTML-escape the three structural characters.
fn html_escape_basic(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// Escape plain text to HTML, wrapping bare http(s) URLs in <a href>
/// anchors so scroll / a browser can follow them. One O(n) pass.
fn linkify_plain_to_html(text: &str) -> String {
    let mut out = String::new();
    let mut last = 0;
    let mut from = 0;
    while let Some(rel) = text[from..].find("http") {
        let pos = from + rel;
        let slice = &text[pos..];
        if slice.starts_with("http://") || slice.starts_with("https://") {
            out.push_str(&html_escape_basic(&text[last..pos]));
            let end = slice
                .find(|c: char| c.is_whitespace() || matches!(c, '<' | '>' | '"' | '\'' | '`'))
                .unwrap_or(slice.len());
            let mut url = &slice[..end];
            // Drop trailing sentence punctuation that isn't part of the URL.
            while let Some(c) = url.chars().last() {
                if matches!(c, '.' | ',' | ';' | ':' | '!' | '?' | ')' | ']') {
                    url = &url[..url.len() - c.len_utf8()];
                } else { break; }
            }
            let eu = html_escape_basic(url);
            out.push_str(&format!("<a href=\"{}\">{}</a>", eu, eu));
            last = pos + url.len();
            from = last;
        } else {
            from = pos + 4;
        }
    }
    out.push_str(&html_escape_basic(&text[last..]));
    out
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
                decode_body_bytes(&bytes, is_latin1)
            } else if is_b64 {
                let bytes = sources::maildir::base64_decode(body.trim()).unwrap_or_default();
                decode_body_bytes(&bytes, is_latin1)
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

/// An iCalendar part, coloured for the right pane.
///
/// The reading is [`mail::ical`], shared with the phone app so the two
/// cannot drift on what a `DTSTART` means; only the colours are here.
fn parse_ical_summary(ical: &str) -> String {
    let e = mail::ical::Event::parse(ical);

    let lbl = |s: &str| style::fg(s, 51);   // cyan labels
    let val = |s: &str| style::fg(s, 252);   // light text
    let hi  = |s: &str| style::bold(&style::fg(s, 156)); // green bold

    let mut lines = Vec::new();
    lines.push(style::bold(&style::fg(&format!("[{}]", e.kind()), 226)));
    lines.push(String::new());
    if !e.summary.is_empty() { lines.push(format!("{}  {}", lbl("WHAT:"), hi(&e.summary))); }
    let when = e.when();
    if !when.is_empty() { lines.push(format!("{}  {}", lbl("WHEN:"), val(&when))); }
    if !e.timezone.is_empty() { lines.push(format!("{}  {}", lbl("  TZ:"), style::fg(&e.timezone, 245))); }
    if !e.location.is_empty() { lines.push(format!("{} {}", lbl("WHERE:"), val(&e.location))); }
    if !e.recurrence.is_empty() { lines.push(format!("{} {}", lbl("RECUR:"), val(&e.recurrence))); }
    if !e.status.is_empty() {
        let sc = match e.status.to_uppercase().as_str() {
            "CONFIRMED" => 46, "CANCELLED" | "CANCELED" => 196, "TENTATIVE" => 226,
            _ => 252,
        };
        lines.push(format!("{}  {}", lbl("STATUS:"), style::fg(&e.status, sc)));
    }
    if !e.priority.is_empty() { lines.push(format!("{}  {}", lbl("PRIORITY:"), val(&e.priority))); }
    lines.push(String::new());
    if !e.organizer.is_empty() { lines.push(format!("{} {}", lbl("ORGANIZER:"), val(&e.organizer))); }
    if !e.attendees.is_empty() {
        lines.push(lbl("PARTICIPANTS:").to_string());
        for (name, pstat) in &e.attendees {
            let status_str = if pstat.is_empty() { String::new() } else {
                let sc = match pstat.as_str() {
                    "accepted" => 46, "declined" => 196, "tentative" => 226, _ => 245,
                };
                format!(" ({})", style::fg(pstat, sc))
            };
            lines.push(format!("  {}{}", val(name), status_str));
        }
    }
    if !e.description.is_empty() {
        lines.push(String::new());
        lines.push(lbl("DESCRIPTION:").to_string());
        for dline in e.description.lines() {
            lines.push(style::fg(dline, 248));
        }
    }
    lines.join("\n")
}

/// Extract inline MIME image parts from a raw message `content` and
/// materialise them into the image cache, returning `file://` URLs.
/// Shared by the `V`-key inline display and the image-save collector so
/// both always agree with the rendered "N images" count — all three now
/// read the same in-memory `extract_mime_attachments`. Pure parse: no
/// python subprocess and no dependence on the on-disk maildir-file path
/// (which goes stale when a message moves new/→cur/).
fn mime_image_file_urls(content: &str, msg_id: i64) -> Vec<String> {
    let cache_dir = home_dir().join(".kastrup/image_cache");
    let _ = std::fs::create_dir_all(&cache_dir);
    let mut out = Vec::new();
    for (i, att) in extract_mime_attachments(content, msg_id).into_iter().enumerate() {
        if !att["is_image"].as_bool().unwrap_or(false) { continue; }
        let Some(src) = att["source_file"].as_str() else { continue };
        // Give the cache copy an extension from the content-type so glow
        // selects the right decoder (the extracted tmp file is often
        // extension-less for Content-ID inline images).
        let ext = att["content_type"].as_str().unwrap_or("image/png")
            .rsplit('/').next().unwrap_or("png")
            .split([';', ' ']).next().unwrap_or("png")
            .trim();
        let ext = if ext.is_empty() { "png" } else { ext };
        let dest = cache_dir.join(format!("mime_{}_{}.{}", msg_id, i, ext));
        if std::fs::copy(src, &dest).is_ok() {
            out.push(format!("file://{}", dest.display()));
        }
    }
    out
}

/// The files hanging off a message, decoded to `/tmp` and described in
/// the JSON shape the rest of this file reads.
///
/// The walk itself is `fe2o3-mail`'s, shared with the phone app, so the
/// two cannot disagree about what a message carries. What stays here is
/// the part that is not shared: putting the bytes somewhere the desktop
/// can open them. `source_file` is consumed a few lines below and copied
/// to `att_temp_path`, so its shape is private to this pair.
fn extract_mime_attachments(content: &str, msg_id: i64) -> Vec<serde_json::Value> {
    mail::attach::list(content).into_iter().enumerate().map(|(i, a)| {
        // Indexed: two attachments may legitimately share a name, and
        // the old dedup-by-name silently dropped the second.
        let tmp_path = format!("/tmp/kastrup_att_{}_{}_{}", msg_id, i, a.filename);
        if let Some(bytes) = mail::attach::bytes(content, i) {
            let _ = std::fs::write(&tmp_path, &bytes);
        }
        serde_json::json!({
            "name": a.filename,
            "filename": a.filename,
            "content_type": a.mime_type,
            "size": a.size,
            "source_file": tmp_path,
            "url": format!("file://{}", tmp_path),
            "is_image": a.mime_type.starts_with("image/"),
        })
    }).collect()
}

/// Build a `/tmp/kastrup_att_*` path for an attachment, sanitising the
/// filename. Attachment names can carry spaces and other shell-unsafe
/// characters (e.g. `"202606 Workshop Agenda .pdf"`). Some sources resolve
/// the download through an external command template, where an unquoted,
/// space-laden path gets word-split by the shell: the file lands at the
/// first token (extension lost) while kastrup then tries to open the full
/// path that was never written. Collapse everything outside `[alnum].-_`
/// to `_` so the written path and the opened path always agree, and the
/// extension survives.
fn att_temp_path(name: &str) -> String {
    let safe: String = name.chars()
        .map(|c| if c.is_alphanumeric() || ".-_".contains(c) { c } else { '_' })
        .collect();
    format!("/tmp/kastrup_att_{}", safe)
}

/// Office-type documents LibreOffice can render to PDF (attachment
/// view `p`/`P` keys).
fn is_office_doc(name: &str) -> bool {
    let ext = std::path::Path::new(name).extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
        .unwrap_or_default();
    matches!(ext.as_str(),
        "doc" | "docx" | "odt" | "ott" | "rtf" |
        "xls" | "xlsx" | "ods" | "ots" |
        "ppt" | "pptx" | "odp" | "otp")
}

/// `Report May.docx` → `Report May.pdf`
fn pdf_file_name(name: &str) -> String {
    let stem = std::path::Path::new(name).file_stem()
        .and_then(|s| s.to_str()).unwrap_or("attachment");
    format!("{}.pdf", stem)
}

/// Quick HTML sniffer: returns true if `s` contains at least two
/// distinct opening tags from a curated common-tag list. Two-tag
/// floor avoids false positives on stray `<foo>` typos in plain
/// text (a single `<3` heart, `<insert name>` placeholder, etc.).
fn looks_like_html(s: &str) -> bool {
    let lower = s.to_ascii_lowercase();
    const TAGS: &[&str] = &[
        "<p>", "<p ", "<a ", "<a\n", "<a\t", "<br", "<div", "<span",
        "<b>", "<i>", "<u>", "<em", "<strong", "<ul", "<ol", "<li",
        "<h1", "<h2", "<h3", "<table", "<tr", "<td", "<img", "<hr",
        "<blockquote", "<pre", "<code",
    ];
    let mut hits = 0;
    for t in TAGS {
        if lower.contains(t) {
            hits += 1;
            if hits >= 2 { return true; }
        }
    }
    false
}

/// True iff `s` looks like a quoted-printable-encoded body that
/// contains HTML. Pre-decode probe used by `best_html_for_message`
/// to recover single-part text/html messages whose headers were
/// stripped at parse time.
fn looks_qp_html(s: &str) -> bool {
    let b = s.as_bytes();
    let mut has_qp = false;
    let mut i = 0;
    while i + 1 < b.len() {
        if b[i] == b'=' {
            // `=\n` / `=\r\n` soft line break.
            if b[i + 1] == b'\n' || b[i + 1] == b'\r' { has_qp = true; break; }
            // `=XX` where XX are ASCII hex digits.
            if i + 2 < b.len()
                && b[i + 1].is_ascii_hexdigit()
                && b[i + 2].is_ascii_hexdigit()
            {
                has_qp = true;
                break;
            }
        }
        i += 1;
    }
    has_qp && looks_like_html(s)
}



/// Decode a body byte buffer using the declared MIME charset, but
/// don't blindly trust the declaration. Many senders mark UTF-8
/// content as `charset=iso-8859-1` or `windows-1252` (this is
/// rampant on transactional mail). Strategy:
///
/// 1. If `declared_latin1` is false (charset says UTF-8 or wasn't
///    set), interpret as UTF-8, lossy-decode on error.
/// 2. If `declared_latin1` is true, FIRST try strict UTF-8. If
///    the bytes happen to be valid UTF-8 that's almost certainly
///    what they really are — Norwegian "påminnelse" (UTF-8
///    `0xC3 0xA5`) would otherwise come through as the mojibake
///    `pÃ¥minnelse` after a literal latin1→utf-8 lift.
/// 3. Only fall through to `latin1_to_utf8` when strict UTF-8
///    fails, i.e. the bytes are genuinely 8-bit Latin-1.
fn decode_body_bytes(bytes: &[u8], declared_latin1: bool) -> String {
    if declared_latin1 {
        if let Ok(s) = std::str::from_utf8(bytes) {
            return s.to_string();
        }
        return latin1_to_utf8(bytes);
    }
    String::from_utf8(bytes.to_vec())
        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
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
                let b1 = input[i + 1];
                let b2 = input[i + 2];
                if b1.is_ascii_hexdigit() && b2.is_ascii_hexdigit() {
                    let hex = [b1, b2];
                    // SAFETY: both bytes are ASCII hex digits -> valid UTF-8
                    let hex_str = std::str::from_utf8(&hex).unwrap();
                    if let Ok(byte) = u8::from_str_radix(hex_str, 16) {
                        bytes.push(byte);
                        i += 3;
                    } else {
                        bytes.push(b'=');
                        i += 1;
                    }
                } else {
                    // Bare `=` not followed by ASCII hex (e.g. preceding a
                    // UTF-8 multi-byte char) — emit literally.
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




// --- HTML to text ---









// --- Utilities ---

/// Returns `(pre-styled icon, row text color)`. The icon string
/// embeds its own SGR color so that — even when the caller wraps
/// the whole row in a different `style::fg(row_color)` — the icon
/// shows the configured `src_<source>_icon` colour while sender /
/// subject text continues in `src_<source>`. Both colors are
/// user-configurable per source via `~/.kastrup/config.yml`.
/// Heuristic: given a section's display name (after pretty_folder_name
/// stripping), return the matching chat source_type so the renderer
/// can theme it. Returns None for folders that don't look like chat
/// transports — e.g. mail folders like `Personal` or `Work.Archive`.
fn chat_source_type_for_display(display: &str) -> Option<&'static str> {
    if display.starts_with("slack.") { return Some("slack"); }
    if display.starts_with("discord-bridge.") { return Some("discord"); }
    if display.starts_with("matrix.") { return Some("telegram"); /* closest theme */ }
    if display.starts_with("whatsapp.") { return Some("whatsapp"); }
    // IRC networks: any of the common server short-names that survive
    // pretty_folder_name's `irc.` strip.
    for irc in ["libera.", "oftc.", "efnet.", "bitlbee.", "freenode.",
                "dalnet.", "undernet.", "rizon."]
    {
        if display.starts_with(irc) { return Some("weechat"); }
    }
    None
}

/// Resolve a message's display source-type. Usually the source's
/// plugin_type, but gateway (phone relay) messages carry the real
/// platform in metadata — surface that (whatsapp/instagram/messenger/
/// telegram/signal/sms) so per-platform colour, icon, and the "source"
/// sort work in the Phone view instead of everything reading as
/// "gateway".
fn resolve_source_type(map: &std::collections::HashMap<i64, String>, msg: &mut Message) {
    let Some(st) = map.get(&msg.source_id) else { return };
    if st == "gateway" {
        if let Some(p) = msg.metadata.get("platform").and_then(|v| v.as_str()) {
            if !p.is_empty() { msg.source_type = p.to_string(); return; }
        }
    }
    msg.source_type = st.clone();
}

/// Reconcile "stuck" maildir files: messages that are read=1 in the DB
/// but whose metadata `maildir_file` still points into a `new/` subdir
/// (their new/→cur/ move slipped through — a rename that lost a race, or
/// a read on an older build). gmail-idle counts `new/`, so each stray
/// shows as a phantom unread in the asmite until it's moved. Sends a
/// `SyncMaildirFlag` for each, which performs the rename + metadata fix.
///
/// Cheap and UI-safe: a dedicated aux connection (never the shared
/// Mutex), bounded to recently-ingested rows by rowid (primary key), so
/// it's sub-second even cold. Safe to call repeatedly.
/// Case-insensitive substring match for find-in-view (`\`): tests the
/// sender, sender name, subject and the loaded content preview. Cheap —
/// all in-memory fields, no DB hit.
fn msg_matches(m: &Message, needle_lc: &str) -> bool {
    m.subject.as_deref().unwrap_or("").to_lowercase().contains(needle_lc)
        || m.sender.to_lowercase().contains(needle_lc)
        || m.sender_name.as_deref().unwrap_or("").to_lowercase().contains(needle_lc)
        || m.content.to_lowercase().contains(needle_lc)
}

fn reconcile_stuck_maildir(
    db: &database::Database,
    write_tx: &std::sync::mpsc::Sender<DbWriteOp>,
) {
    let conn = match db.open_aux_connection() {
        Ok(c) => c,
        Err(e) => { log::info(&format!("reconcile: aux conn failed: {}", e)); return; }
    };
    let mut stmt = conn.prepare(
        "SELECT id, metadata FROM messages \
         WHERE id > (SELECT COALESCE(MAX(id),0) FROM messages) - 20000 \
           AND read = 1 AND metadata IS NOT NULL \
           AND instr(metadata, '\"maildir_file\":') > 0 \
           AND instr(metadata, '/new/') > 0"
    ).ok();
    let mut stuck: Vec<(serde_json::Value, i64)> = Vec::new();
    if let Some(stmt) = stmt.as_mut() {
        if let Ok(rows) = stmt.query_map([], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?))
        }) {
            for row in rows.flatten() {
                let (id, meta_opt) = row;
                let Some(meta_str) = meta_opt else { continue };
                let Ok(meta) = serde_json::from_str::<serde_json::Value>(&meta_str) else { continue };
                if meta.get("maildir_file").and_then(|v| v.as_str())
                    .map(|p| p.contains("/new/")).unwrap_or(false)
                {
                    stuck.push((meta, id));
                }
            }
        }
    }
    drop(stmt);
    drop(conn);
    if !stuck.is_empty() {
        log::info(&format!(
            "reconcile: {} stuck maildir file(s) (read=1, still in new/)",
            stuck.len()
        ));
        for (meta, id) in stuck {
            let _ = write_tx.send(DbWriteOp::SyncMaildirFlag(meta, id));
        }
    }
}

fn source_info(source_type: &str, tc: &config::ThemeColors) -> (String, u8) {
    // Config-driven per-platform override (config.yml `source_styles`).
    // Lets any platform — including relay "Add app" slugs we deliberately
    // keep out of the source tree — get a custom colour/icon. Empty glyph
    // falls back to the default bullet.
    if let Some((color, glyph)) = tc.source_styles.get(source_type) {
        let g = if glyph.is_empty() { "\u{2022}" } else { glyph.as_str() };
        let styled = style::fg(g, *color);
        return (styled, *color);
    }
    let (glyph, icon_color, row_color) = match source_type {
        "discord"  => ("\u{25C6}", tc.src_discord_icon,  tc.src_discord),
        "slack"    => ("#",        tc.src_slack_icon,    tc.src_slack),
        "telegram" => ("\u{2708}", tc.src_telegram_icon, tc.src_telegram),
        "whatsapp" => ("\u{25C9}", tc.src_whatsapp_icon, tc.src_whatsapp),
        "reddit"   => ("\u{00AE}", tc.src_reddit_icon,   tc.src_reddit),
        "linkedin" => ("\u{24C1}", tc.src_linkedin_icon, tc.src_linkedin),
        // `@` instead of the old U+2709 ✉ envelope: single-cell,
        // text-weight, doesn't dominate the row.
        "email" | "maildir" | "imap" | "gmail" => ("@", tc.src_email_icon, tc.src_email),
        "rss"      => ("\u{25C8}", tc.src_rss_icon,      tc.src_rss),
        "web" | "webpage" => ("\u{25CE}", tc.src_web_icon, tc.src_web),
        "messenger" => ("\u{260E}", tc.src_messenger_icon, tc.src_messenger),
        "instagram" => ("\u{25C8}", tc.src_instagram_icon, tc.src_instagram),
        // SMS (native) and Signal arrive via the phone gateway.
        "sms"      => ("\u{260F}", tc.src_sms_icon, tc.src_sms),
        "signal"   => ("\u{25C7}", tc.src_signal_icon, tc.src_signal),
        "weechat" | "workspace" => ("\u{2318}", tc.src_weechat_icon, tc.src_weechat),
        _ => ("\u{2022}", tc.src_default_icon, tc.src_default),
    };
    // \x1b[39m resets only the foreground (not bg/style) so the
    // row's outer style continues unaffected after the icon.
    let styled = style::fg(glyph, icon_color);
    (styled, row_color)
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

/// Inverse of days_to_ymd: convert (year, month, day) to days since epoch.
fn ymd_to_days(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Scan plain text for the earliest future date (with optional time).
/// Returns (Y, M, D, Option<(H, Min)>) when found.
/// Recognises DD.MM.YYYY (Nordic), YYYY-MM-DD (ISO), DD/MM/YYYY (EU).
/// Time formats: "kl. HH:MM", "kl. HH.MM", "HH:MM" appearing on the same
/// rendered line or within ~80 bytes after the date match.
fn scan_for_future_event(text: &str) -> Option<(i32, u32, u32, Option<(u32, u32)>)> {
    use regex::Regex;

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).ok()?.as_secs() as i64;
    let today_days = now_secs / 86400;

    // (regex, year_idx, month_idx, day_idx)
    let date_patterns: [(Regex, usize, usize, usize); 3] = [
        (Regex::new(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b").unwrap(), 3, 2, 1),
        (Regex::new(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b").unwrap(), 1, 2, 3),
        (Regex::new(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b").unwrap(), 3, 2, 1),
    ];

    // (?i) case-insensitive, optional period after kl
    let time_re = Regex::new(r"(?i)(?:kl\.?\s*)?(\d{1,2})[:.](\d{2})\b").ok()?;

    let mut best: Option<(i64, usize, i32, u32, u32)> = None;
    for (re, yi, mi, di) in date_patterns.iter() {
        for cap in re.captures_iter(text) {
            let m_full = match cap.get(0) { Some(m) => m, None => continue };
            let y: i32 = match cap.get(*yi).and_then(|m| m.as_str().parse().ok()) { Some(v) => v, None => continue };
            let mo: u32 = match cap.get(*mi).and_then(|m| m.as_str().parse().ok()) { Some(v) => v, None => continue };
            let d: u32 = match cap.get(*di).and_then(|m| m.as_str().parse().ok()) { Some(v) => v, None => continue };
            if !(1..=12).contains(&mo) || !(1..=31).contains(&d) { continue; }
            if !(2000..=2200).contains(&y) { continue; }
            let days = ymd_to_days(y as i64, mo as i64, d as i64);
            if days < today_days { continue; }
            // Prefer earliest; on tie, prefer earliest position.
            let take = match best {
                Some((bd, _, _, _, _)) if bd <= days => false,
                _ => true,
            };
            if take {
                best = Some((days, m_full.end(), y, mo, d));
            }
        }
    }

    let (_, after_pos, y, mo, d) = best?;

    // Look for a time within the same rendered line OR in the next 80 bytes,
    // whichever comes first.
    let after = &text[after_pos..];
    let line_end = after.find('\n').unwrap_or(after.len());
    let window_end = std::cmp::min(line_end.max(80), after.len());
    let window = &after[..window_end];
    let time = time_re.captures(window).and_then(|c| {
        let h: u32 = c.get(1)?.as_str().parse().ok()?;
        let min: u32 = c.get(2)?.as_str().parse().ok()?;
        if h < 24 && min < 60 { Some((h, min)) } else { None }
    });

    Some((y, mo, d, time))
}

/// Insert `X-TOCK-CALENDAR-ID:<n>` into each VEVENT in an ICS body so
/// Tock's importer can route the event to a specific calendar.
fn inject_tock_calendar_id(ics: &str, cal_id: i64) -> String {
    let needle = "BEGIN:VEVENT";
    let line = format!("X-TOCK-CALENDAR-ID:{}", cal_id);
    let mut out = String::with_capacity(ics.len() + 64);
    let mut rest = ics;
    while let Some(idx) = rest.find(needle) {
        let end = idx + needle.len();
        out.push_str(&rest[..end]);
        // After BEGIN:VEVENT, find the line terminator (\r\n or \n) and
        // insert our line immediately after.
        let after = &rest[end..];
        let (term, term_len) = if after.starts_with("\r\n") {
            ("\r\n", 2)
        } else if after.starts_with('\n') {
            ("\n", 1)
        } else {
            ("\r\n", 0)
        };
        out.push_str(term);
        out.push_str(&line);
        out.push_str(term);
        rest = &after[term_len..];
    }
    out.push_str(rest);
    out
}

/// Generate a minimal ICS file body for a one-off event with optional time.
/// Time given => 30-minute appointment; no time => all-day.
fn build_ics_event(uid: &str, summary: &str, description: &str,
                   y: i32, m: u32, d: u32, time: Option<(u32, u32)>) -> String {
    let now_stamp = {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0) as i64;
        let days = secs / 86400;
        let (yy, mm, dd) = days_to_ymd(days);
        let tod = secs.rem_euclid(86400);
        let h = tod / 3600;
        let mi = (tod % 3600) / 60;
        let s = tod % 60;
        format!("{:04}{:02}{:02}T{:02}{:02}{:02}Z", yy, mm, dd, h, mi, s)
    };
    let escape = |s: &str| s.replace('\\', r"\\").replace(';', r"\;")
        .replace(',', r"\,").replace('\n', r"\n");
    let summary = escape(summary);
    let description = escape(description);

    match time {
        Some((h, mi)) => {
            let end_min = mi + 30;
            let (eh, em) = if end_min >= 60 { (h + 1, end_min - 60) } else { (h, end_min) };
            format!(
                "BEGIN:VCALENDAR\r\n\
                 VERSION:2.0\r\n\
                 PRODID:-//Kastrup//Z-action//EN\r\n\
                 CALSCALE:GREGORIAN\r\n\
                 BEGIN:VEVENT\r\n\
                 UID:{uid}\r\n\
                 DTSTAMP:{now_stamp}\r\n\
                 DTSTART:{y:04}{m:02}{d:02}T{h:02}{mi:02}00\r\n\
                 DTEND:{y:04}{m:02}{d:02}T{eh:02}{em:02}00\r\n\
                 SUMMARY:{summary}\r\n\
                 DESCRIPTION:{description}\r\n\
                 END:VEVENT\r\n\
                 END:VCALENDAR\r\n"
            )
        }
        None => format!(
            "BEGIN:VCALENDAR\r\n\
             VERSION:2.0\r\n\
             PRODID:-//Kastrup//Z-action//EN\r\n\
             CALSCALE:GREGORIAN\r\n\
             BEGIN:VEVENT\r\n\
             UID:{uid}\r\n\
             DTSTAMP:{now_stamp}\r\n\
             DTSTART;VALUE=DATE:{y:04}{m:02}{d:02}\r\n\
             DTEND;VALUE=DATE:{y:04}{m:02}{d:02}\r\n\
             SUMMARY:{summary}\r\n\
             DESCRIPTION:{description}\r\n\
             END:VEVENT\r\n\
             END:VCALENDAR\r\n"
        ),
    }
}

/// Triage variant of build_ics_event that honours an explicit
/// duration in minutes (not the fixed 30-min default of the Z path).
fn build_ics_event_dur(uid: &str, summary: &str, description: &str,
                       y: i32, m: u32, d: u32, time: Option<(u32, u32)>,
                       duration_min: u32) -> String {
    let now_stamp = {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0) as i64;
        let days = secs / 86400;
        let (yy, mm, dd) = days_to_ymd(days);
        let tod = secs.rem_euclid(86400);
        let h = tod / 3600;
        let mi = (tod % 3600) / 60;
        let s = tod % 60;
        format!("{:04}{:02}{:02}T{:02}{:02}{:02}Z", yy, mm, dd, h, mi, s)
    };
    let escape = |s: &str| s.replace('\\', r"\\").replace(';', r"\;")
        .replace(',', r"\,").replace('\n', r"\n");
    let summary = escape(summary);
    let description = escape(description);
    let dur = if duration_min == 0 { 30 } else { duration_min };

    match time {
        Some((h, mi)) => {
            let total = mi + dur;
            let (eh, em) = (h + total / 60, total % 60);
            format!(
                "BEGIN:VCALENDAR\r\n\
                 VERSION:2.0\r\n\
                 PRODID:-//Kastrup//triage//EN\r\n\
                 CALSCALE:GREGORIAN\r\n\
                 BEGIN:VEVENT\r\n\
                 UID:{uid}\r\n\
                 DTSTAMP:{now_stamp}\r\n\
                 DTSTART:{y:04}{m:02}{d:02}T{h:02}{mi:02}00\r\n\
                 DTEND:{y:04}{m:02}{d:02}T{eh:02}{em:02}00\r\n\
                 SUMMARY:{summary}\r\n\
                 DESCRIPTION:{description}\r\n\
                 END:VEVENT\r\n\
                 END:VCALENDAR\r\n"
            )
        }
        None => format!(
            "BEGIN:VCALENDAR\r\n\
             VERSION:2.0\r\n\
             PRODID:-//Kastrup//triage//EN\r\n\
             CALSCALE:GREGORIAN\r\n\
             BEGIN:VEVENT\r\n\
             UID:{uid}\r\n\
             DTSTAMP:{now_stamp}\r\n\
             DTSTART;VALUE=DATE:{y:04}{m:02}{d:02}\r\n\
             DTEND;VALUE=DATE:{y:04}{m:02}{d:02}\r\n\
             SUMMARY:{summary}\r\n\
             DESCRIPTION:{description}\r\n\
             END:VEVENT\r\n\
             END:VCALENDAR\r\n"
        ),
    }
}

/// Resolve a tock calendar name → numeric id by querying tock.db.
/// Restricted to cloud-synced calendars — local calendars would
/// never reach the user's phone, which is the whole point of going
/// through the calendar at all. Returns None if the name doesn't
/// match any enabled cloud calendar (caller falls back to the
/// default cloud calendar). Read-only access; tock can have the DB
/// open in WAL mode concurrently without locking issues.
fn triage_lookup_calendar_id(tock_home: &std::path::Path, name: &str) -> Option<i64> {
    let db = tock_home.join("tock.db");
    let conn = rusqlite::Connection::open_with_flags(
        &db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ).ok()?;
    conn.query_row(
        "SELECT id FROM calendars \
         WHERE name = ? AND source_type != 'local' AND enabled = 1 \
         LIMIT 1",
        rusqlite::params![name],
        |r| r.get::<_, i64>(0),
    ).ok()
}

/// Pick the default triage calendar: the lowest-id enabled cloud
/// calendar. tock.config.default_calendar is intentionally ignored
/// here — that setting often points at the local "Personal"
/// calendar which would defeat the whole "show up on my phone"
/// goal. Returns None only if tock.db has zero cloud calendars.
fn triage_default_calendar_id(tock_home: &std::path::Path) -> Option<i64> {
    let db = tock_home.join("tock.db");
    let conn = rusqlite::Connection::open_with_flags(
        &db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ).ok()?;
    conn.query_row(
        "SELECT id FROM calendars \
         WHERE source_type != 'local' AND enabled = 1 \
         ORDER BY id LIMIT 1",
        [],
        |r| r.get::<_, i64>(0),
    ).ok()
}

/// Build an RFC 5322 `Date:` value and a `Message-ID:` for an
/// outgoing message. The Gmail submission path backfills both when
/// absent, but the external relay path (dmail_smtp → internal MTA)
/// does not — without them the relayed mail is malformed and strict
/// receivers may junk or reject it. The old Ruby helpers added these
/// via the Mail gem; match that. `from` may be `Name <addr>` or a
/// bare address; the Message-ID domain is taken from its `@` part.
fn rfc822_date_and_msgid(from: &str) -> (String, String) {
    const WDAY: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MON: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                             "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs() as i64;
    let date = unsafe {
        let mut t: libc::time_t = secs as libc::time_t;
        let mut tm: libc::tm = std::mem::zeroed();
        libc::localtime_r(&mut t as *mut _, &mut tm);
        let off = tm.tm_gmtoff as i64;
        let (sign, ao) = if off < 0 { ('-', -off) } else { ('+', off) };
        format!(
            "{}, {:02} {} {} {:02}:{:02}:{:02} {}{:02}{:02}",
            WDAY[(tm.tm_wday as usize) % 7],
            tm.tm_mday,
            MON[(tm.tm_mon as usize) % 12],
            tm.tm_year + 1900,
            tm.tm_hour, tm.tm_min, tm.tm_sec,
            sign, ao / 3600, (ao % 3600) / 60,
        )
    };
    let domain = from.rsplit('@').next()
        .map(|s| s.trim_end_matches('>').trim())
        .filter(|s| !s.is_empty() && !s.contains(' '))
        .unwrap_or("localhost");
    let msgid = format!("<{}.{}.{}@{}>", secs, std::process::id(), dur.subsec_nanos(), domain);
    (date, msgid)
}

/// Parse a send-at expression into a unix timestamp, local time.
///
/// Accepts what a person would actually type at a prompt:
///   `+2h`, `+90m`, `+3d`     relative to now
///   `08:00`                  today if it is still ahead, else tomorrow
///   `tomorrow 08:00`         or just `tomorrow` (09:00)
///   `2026-07-28 08:00`       explicit, `T` also accepted between them
fn parse_send_at(input: &str) -> Option<i64> {
    let t = input.trim().to_lowercase();
    if t.is_empty() {
        return None;
    }
    let now = database::now_secs();
    // Relative: +N with a unit.
    if let Some(rest) = t.strip_prefix('+') {
        let split = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
        let (num, unit) = rest.split_at(split);
        let n: i64 = num.parse().ok()?;
        let mult = match unit.trim() {
            "" | "m" | "min" | "mins" | "minutes" => 60,
            "h" | "hr" | "hrs" | "hours" => 3600,
            "d" | "day" | "days" => 86_400,
            "w" | "week" | "weeks" => 604_800,
            _ => return None,
        };
        return Some(now + n * mult);
    }
    // Absolute: an optional date word and an optional HH:MM, any order.
    let mut date: Option<String> = None;
    let mut time: Option<String> = None;
    let mut tomorrow = false;
    for word in t.split([' ', ',']).filter(|w| !w.is_empty()) {
        if word.starts_with("tomorrow") || word == "tmr" {
            tomorrow = true;
        } else if word.matches('-').count() == 2 {
            // "2026-07-28T08:00" arrives as one word; split the clock off.
            match word.split_once('t') {
                Some((day, clock)) if clock.contains(':') => {
                    date = Some(day.to_string());
                    time = Some(clock.to_string());
                }
                _ => date = Some(word.to_string()),
            }
        } else if word.contains(':') {
            time = Some(word.to_string());
        }
    }
    let (hh, mm) = match time {
        Some(ref hm) => {
            let mut it = hm.split(':');
            (it.next()?.parse::<i32>().ok()?, it.next()?.parse::<i32>().ok()?)
        }
        // A bare date means that morning.
        None => (9, 0),
    };
    if !(0..24).contains(&hh) || !(0..60).contains(&mm) {
        return None;
    }
    if date.is_none() && time.is_none() && !tomorrow {
        return None;
    }
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    unsafe {
        let mut base: libc::time_t = now as libc::time_t;
        libc::localtime_r(&mut base as *mut _, &mut tm);
    }
    let explicit_date = date.is_some() || tomorrow;
    if let Some(d) = date {
        let mut it = d.split('-');
        let y: i32 = it.next()?.parse().ok()?;
        let mo: i32 = it.next()?.parse().ok()?;
        let da: i32 = it.next()?.parse().ok()?;
        tm.tm_year = y - 1900;
        tm.tm_mon = mo - 1;
        tm.tm_mday = da;
    } else if tomorrow {
        tm.tm_mday += 1;
    }
    tm.tm_hour = hh;
    tm.tm_min = mm;
    tm.tm_sec = 0;
    tm.tm_isdst = -1; // let libc work out DST for that date
    let ts = unsafe { libc::mktime(&mut tm) } as i64;
    if ts <= 0 {
        return None;
    }
    // A bare time that has already gone by today means tomorrow.
    if !explicit_date && ts <= now {
        return Some(ts + 86_400);
    }
    Some(ts)
}

/// "Mon 08:00" for this week, "2026-08-14 08:00" beyond it.
fn fmt_send_at(ts: i64) -> String {
    const WDAY: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    unsafe {
        let mut t: libc::time_t = ts as libc::time_t;
        let mut tm: libc::tm = std::mem::zeroed();
        libc::localtime_r(&mut t as *mut _, &mut tm);
        let days = (ts - database::now_secs()) / 86_400;
        if days < 6 {
            format!("{} {:02}:{:02}", WDAY[(tm.tm_wday as usize) % 7], tm.tm_hour, tm.tm_min)
        } else {
            format!("{:04}-{:02}-{:02} {:02}:{:02}",
                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min)
        }
    }
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

/// Per-sender ASCII avatar: a single uppercase initial in a deterministic
/// color drawn from a curated palette. `sender` is the canonical key (so
/// `alice@example.com` always gets the same color regardless of
/// `sender_name` variations across messages). The initial prefers
/// `sender_name`'s first letter when present (display name people will
/// recognise) and falls back to the email user-part.
fn sender_avatar(sender: &str, sender_name: Option<&str>) -> (char, u8) {
    let initial = sender_name
        .and_then(|n| n.trim().chars().next())
        .or_else(|| sender.split('@').next().and_then(|u| u.chars().next()))
        .unwrap_or('?')
        .to_ascii_uppercase();
    // FNV-1a 64-bit on the lowercased email so capitalisation drift across
    // mailers doesn't shuffle a contact's color from message to message.
    let mut h: u64 = 0xcbf29ce484222325;
    for b in sender.to_ascii_lowercase().bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    // 32 visually-distinct bright 256-color indices. No dim/gray/very-dark
    // entries — the avatar must be readable on the pane bg.
    const PALETTE: [u8; 32] = [
        9, 10, 11, 12, 13, 14, 33, 39, 45, 51, 75, 81, 87, 99, 105, 111,
        117, 141, 147, 159, 165, 171, 177, 183, 189, 201, 207, 213, 219, 225, 226, 220,
    ];
    let color = PALETTE[(h as usize) % PALETTE.len()];
    (initial, color)
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
/// Add the maildir `S` flag for one message and persist the new path.
///
/// The filesystem half lives in `rename_maildir_add_seen` — including
/// its recovery for a file that has already moved out from under us.
/// This used to be a second, older copy of that logic WITHOUT the
/// recovery: it returned early on a missing path, so a message whose
/// file had been filed elsewhere never got its metadata corrected. The
/// 2-minute reconcile then re-found the same rows forever (4 of them,
/// every tick, since June). One implementation, one behaviour.
fn sync_maildir_seen_flag(metadata: &serde_json::Value, db: &database::Database, msg_id: i64) {
    let Some(new_meta) = rename_maildir_add_seen(metadata) else { return };
    let meta_json = serde_json::to_string(&new_meta).unwrap_or_default();
    let conn = db.conn.lock().unwrap();
    let _ = conn.execute(
        "UPDATE messages SET metadata = ? WHERE id = ?",
        rusqlite::params![meta_json, msg_id],
    );
}

/// Background version (called from writer thread)
fn sync_maildir_seen_flag_bg(metadata: &serde_json::Value, db: &database::Database, msg_id: i64) {
    sync_maildir_seen_flag(metadata, db, msg_id);
}

/// Pure-filesystem half of `sync_maildir_seen_flag`: rename the file
/// to add the `S` flag (and bubble new/ → cur/), return the updated
/// metadata Value with `maildir_file` pointing at the new path. No
/// DB writes — the caller can batch them.
///
/// Returns `None` when there's nothing to do (file missing, already
/// flagged Seen, or rename failed). The caller treats `None` as "skip
/// this id" without touching the DB.
fn rename_maildir_add_seen(metadata: &serde_json::Value) -> Option<serde_json::Value> {
    let file_path = metadata.get("maildir_file").and_then(|v| v.as_str())?;
    let path = std::path::Path::new(file_path);
    let filename = path.file_name().and_then(|f| f.to_str())?;
    let parent = path.parent().unwrap_or(std::path::Path::new("."));
    let parent_name = parent.file_name().and_then(|f| f.to_str()).unwrap_or("");

    // Slow path: the file isn't where the metadata says. Happens when
    // Courier IMAP / mu / mbsync / etc moved the message from new/ to
    // cur/ without informing kastrup. We had 88 of these accumulate
    // and they kept getting "reconciled" every startup because the
    // rename below would silently fail (NotFound) and the metadata
    // never got updated. Recovery: look in the sibling cur/ for any
    // file whose name starts with the maildir base (everything up to
    // and including ":2,"), update the metadata to point at the real
    // location, and add the S flag if it isn't there yet.
    if !path.exists() {
        if parent_name != "new" { return None; }
        let info_pos = filename.find(":2,")?;
        let base_with_marker = &filename[..info_pos + 3];
        let cur_dir = parent.parent()?.join("cur");
        let entries = std::fs::read_dir(&cur_dir).ok()?;
        let mut found: Option<std::path::PathBuf> = None;
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(base_with_marker) {
                    found = Some(entry.path());
                    break;
                }
            }
        }
        let cur_path = found?;
        let cur_filename = cur_path.file_name().and_then(|f| f.to_str())?;
        let final_path = if cur_filename.contains('S') {
            cur_path
        } else {
            let (b, flags) = cur_filename.rsplit_once(":2,")?;
            let mut flag_chars: Vec<char> = flags.chars().collect();
            flag_chars.push('S');
            flag_chars.sort();
            let new_name = format!("{}:2,{}", b, flag_chars.into_iter().collect::<String>());
            let new_path = cur_dir.join(&new_name);
            // If the rename fails (permission, race, anything), keep
            // the metadata pointing at the real (unflagged) location
            // anyway — that's still strictly better than leaving it
            // pointing at the non-existent new/ path.
            if std::fs::rename(&cur_path, &new_path).is_ok() { new_path } else { cur_path }
        };
        let mut new_meta = metadata.clone();
        new_meta["maildir_file"] = serde_json::json!(final_path.to_string_lossy().to_string());
        return Some(new_meta);
    }

    let new_filename = if filename.contains(":2,") {
        if filename.contains('S') { return None; } // already Seen
        let (base, flags) = filename.rsplit_once(":2,").unwrap();
        let mut flag_chars: Vec<char> = flags.chars().collect();
        flag_chars.push('S');
        flag_chars.sort();
        format!("{}:2,{}", base, flag_chars.into_iter().collect::<String>())
    } else {
        format!("{}:2,S", filename)
    };

    let new_parent = if parent_name == "new" {
        parent.parent().unwrap_or(parent).join("cur")
    } else {
        parent.to_path_buf()
    };

    let new_path = new_parent.join(&new_filename);
    if std::fs::rename(path, &new_path).is_err() { return None; }

    let mut new_meta = metadata.clone();
    new_meta["maildir_file"] = serde_json::json!(new_path.to_string_lossy().to_string());
    Some(new_meta)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sender_with_no_name_is_just_the_address() {
        let mut m = Message::default_header();
        m.sender = "us@example.com".to_string();
        // Bare `From: us@example.com` parses to an empty name, not none.
        m.sender_name = Some(String::new());
        assert_eq!(m.display_name(), "us@example.com");
        m.sender_name = Some("   ".to_string());
        assert_eq!(m.display_name(), "us@example.com");
        // A name that only repeats the address is the same twice over.
        m.sender_name = Some("us@example.com".to_string());
        assert_eq!(m.display_name(), "us@example.com");
        // And a real name is a real name.
        m.sender_name = Some("Rons Org".to_string());
        assert_eq!(m.display_name(), "Rons Org");
    }

    #[test]
    fn attach_headers_come_out_of_an_email_draft() {
        let draft = "From: Geir Isene <geir@isene.com>\n\
                     To: alice@example.com\n\
                     Attach: /tmp/one.pdf\n\
                     Subject: Follow-up\n\
                     Attach: /tmp/two.png\n\
                     \n\
                     Body with an inline Attach: /tmp/not-a-header.pdf mention.\n";
        let (stripped, atts) = take_email_attach_headers(draft);
        assert_eq!(atts, vec!["/tmp/one.pdf", "/tmp/two.png"]);
        assert!(!stripped.contains("Attach: /tmp/one.pdf"));
        assert!(!stripped.contains("Attach: /tmp/two.png"));
        // Body text is untouched, headers keep their order.
        assert!(stripped.contains("not-a-header.pdf"));
        assert!(stripped.starts_with("From: Geir Isene"));
        assert!(stripped.contains("Subject: Follow-up\n\nBody"));
        // No Attach: lines → draft passes through unchanged.
        let plain = "To: bob@example.com\n\nHi\n";
        let (same, none) = take_email_attach_headers(plain);
        assert_eq!(same, plain);
        assert!(none.is_empty());
    }

    #[test]
    fn kastrup_link_headers_come_out_of_a_drop_draft() {
        let draft = "From: geir@isene.com\n\
                     X-Kastrup-Reply-To: 7964934\n\
                     To: alice@example.com\n\
                     X-Kastrup-Forward-Of: 11, 12\n\
                     \n\
                     X-Kastrup-Reply-To: 99 in the body stays.\n";
        let (stripped, reply, fwd) = take_kastrup_link_headers(draft);
        assert_eq!(reply, Some(7964934));
        assert_eq!(fwd, vec![11, 12]);
        assert!(!stripped.contains("X-Kastrup-Reply-To: 7964934"));
        assert!(!stripped.contains("Forward-Of"));
        assert!(stripped.contains("X-Kastrup-Reply-To: 99 in the body stays."));
        assert!(stripped.starts_with("From: geir@isene.com\nTo: alice@example.com"));
    }

    #[test]
    fn attachment_markers_get_their_own_line() {
        // The real shape: Apple Mail glues them to the quoted signature.
        let got = break_attachment_markers(
            "> { } Geir :: http://isene.com<a-Fotografi.pdf><CV-EN.pdf><CV-NO.pdf>",
        );
        assert_eq!(
            got,
            "> { } Geir :: http://isene.com\n\
             > <a-Fotografi.pdf>\n\
             > <CV-EN.pdf>\n\
             > <CV-NO.pdf>\n",
        );
    }

    #[test]
    fn only_when_glued_and_only_filenames() {
        // Already separated, or a line that is only markers: leave alone.
        for line in [
            "See attached <CV-EN.pdf>",
            "<CV-EN.pdf>",
            // Not filenames: an address, a URL, a bare word.
            "mail me at<geir@isene.com>",
            "my site is<https://isene.com>",
            "the tag is<div>",
            // An extension is one to five alphanumerics, no more.
            "ends in<name.toolongext>",
        ] {
            assert_eq!(break_attachment_markers(line), format!("{}\n", line), "{}", line);
        }
    }

    fn hm(ts: i64) -> (i32, i32) {
        unsafe {
            let mut t: libc::time_t = ts as libc::time_t;
            let mut tm: libc::tm = std::mem::zeroed();
            libc::localtime_r(&mut t as *mut _, &mut tm);
            (tm.tm_hour, tm.tm_min)
        }
    }

    /// The four rows the reconcile kept re-finding: metadata says the
    /// file is in new/, the file is really in the sibling cur/ with the
    /// S flag already on it. The recovery has to point the metadata at
    /// the real path, otherwise the reconcile finds the same row again
    /// every two minutes, forever.
    #[test]
    fn seen_flag_recovers_a_file_that_already_moved() {
        let root = std::env::temp_dir()
            .join(format!("kastrup-seen-{}", std::process::id()));
        let new_dir = root.join("new");
        let cur_dir = root.join("cur");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&new_dir).unwrap();
        std::fs::create_dir_all(&cur_dir).unwrap();
        let real = cur_dir.join("1780310872.821587_4978_49.juba:2,S");
        std::fs::write(&real, "body").unwrap();

        let meta = serde_json::json!({
            "maildir_file": new_dir.join("1780310872.821587_4978_49.juba:2,")
                .to_string_lossy().to_string(),
        });
        let fixed = rename_maildir_add_seen(&meta).expect("recovery should find cur/");
        assert_eq!(fixed["maildir_file"].as_str().unwrap(),
                   real.to_string_lossy());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The bug this exists to stop: a short reply whose whole substance
    /// is the first paragraph, stored without headers because the
    /// maildir parser already stripped them.
    /// scribe draws a header line wholly in the header colour with the
    /// key bolded. kastrup coloured only the key, so a From: value went
    /// white until its first email address and a Subject stayed white
    /// throughout.
    #[test]
    fn a_header_row_colours_the_whole_value() {
        let row = header_row("From:", "Kumar S <k@example.com>", 2);
        assert!(row.contains(&format!("{}Kumar S", crust::style::set_fg(2))),
                "the value has to open in the header colour: {:?}", row);
        let subject = header_row("Subject:", "No address in here", 1);
        assert!(subject.contains(&format!("{}No address in here", crust::style::set_fg(1))),
                "a value with no address is coloured too: {:?}", subject);
    }

    #[test]
    fn a_headerless_body_keeps_its_first_paragraph() {
        let body = "Hei Geir\n\
                    Det ser greit ut. Har n=C3=A5 sendt inn fullmakt, =\n\
                    s=C3=A5 da skal det v=C3=A6re i orden. Mitt kontonr. 1234 56 78901\n\
                    \n\
                    Mvh\n";
        assert_eq!(body_after_headers(body), 0, "no headers here to skip");
        let decoded = decode_quoted_printable(&body[body_after_headers(body)..]);
        assert!(decoded.starts_with("Hei Geir"), "got: {}", &decoded[..20.min(decoded.len())]);
        assert!(decoded.contains("1234 56 78901"), "the account number survives");
        assert!(decoded.contains("være i orden"), "soft breaks and =XX still decode");
    }

    #[test]
    fn a_real_header_block_is_still_skipped() {
        let mail = "Content-Type: text/plain\n\
                    Content-Transfer-Encoding: quoted-printable\n\
                    X-Folded: one\n\
                    \ttwo\n\
                    \n\
                    Hei\n";
        let at = body_after_headers(mail);
        assert!(at > 0);
        assert_eq!(&mail[at..], "Hei\n");
    }

    #[test]
    fn relative_offsets() {
        let now = database::now_secs();
        assert_eq!(parse_send_at("+2h").unwrap() - now, 7200);
        assert_eq!(parse_send_at("+90m").unwrap() - now, 5400);
        assert_eq!(parse_send_at("+45").unwrap() - now, 2700);
        assert_eq!(parse_send_at("+3d").unwrap() - now, 259_200);
        assert_eq!(parse_send_at("+1w").unwrap() - now, 604_800);
    }

    #[test]
    fn clock_times_land_on_the_clock() {
        let ts = parse_send_at("08:30").unwrap();
        assert_eq!(hm(ts), (8, 30));
        // Always in the future: today if it is still ahead, else tomorrow.
        assert!(ts > database::now_secs());
        assert!(ts - database::now_secs() <= 86_400);
    }

    #[test]
    fn tomorrow_is_a_day_ahead() {
        let ts = parse_send_at("tomorrow 09:00").unwrap();
        assert_eq!(hm(ts), (9, 0));
        let bare = parse_send_at("tomorrow").unwrap();
        assert_eq!(hm(bare), (9, 0)); // a bare date means that morning
    }

    #[test]
    fn explicit_dates() {
        let ts = parse_send_at("2030-03-14 15:09").unwrap();
        assert_eq!(hm(ts), (15, 9));
        assert_eq!(parse_send_at("2030-03-14t15:09").map(hm), Some((15, 9)));
    }

    #[test]
    fn nonsense_is_rejected() {
        for bad in ["", "   ", "later", "+2x", "25:00", "08:99", "banana"] {
            assert!(parse_send_at(bad).is_none(), "accepted {bad:?}");
        }
    }
}
