//! Hand new rows to an outside indexer, one small POST per poll cycle.
//!
//! The indexer (Corporate Intelligence, or anything with the same push
//! route) digests on its own schedule; kastrup only tells it what has
//! arrived. Mail is left out: the indexer reads the Maildir itself.
//!
//! Cost when idle: nothing. The poller calls in only after a cycle that
//! inserted rows, and a cycle that inserted only mail sends nothing. A
//! watermark in the settings table says what has been sent, so a failed
//! POST is retried next cycle and nothing goes twice.
//!
//! The answer to a POST carries the indexer's outbox: replies the user
//! approved there. Each becomes a draft file in ~/.kastrup/drafts, in the
//! format of the channel the original came in on, and the `+` picker
//! shows it. The indexer marks them handed as it answers, so none comes
//! twice. `kastrup --push-now` also drains the outbox by GET.

use std::sync::Arc;
use crate::database::Database;

/// The `push:` block of ~/.kastrup/config.yml. Absent block, no feeder.
#[derive(Clone, Debug)]
pub struct PushConfig {
    /// Base URL of the indexer, e.g. `http://localhost:8100`.
    pub url: String,
    /// The push connector's id; the route is `{url}/api/push/{connector}`.
    pub connector: String,
    /// File holding the connector's key, sent as `X-Push-Key`.
    pub key_file: String,
    /// `From:` on the mail drafts the outbox produces (default_email).
    pub from: String,
}

const WATERMARK: &str = "push_sent_up_to";
const BATCH: usize = 200;

/// One row as the push route wants it.
fn record(r: &crate::database::PushRow) -> serde_json::Value {
    let ext = format!("kastrup:{}", r.id);
    let author = match (&r.sender_name, r.sender.as_str()) {
        (Some(n), s) if !n.trim().is_empty() && n.trim() != s => format!("{} <{}>", n.trim(), s),
        (_, s) => s.to_string(),
    };
    let container = match &r.folder {
        Some(f) if !f.is_empty() => format!("{}/{}", r.source_name, f),
        _ => r.source_name.clone(),
    };
    serde_json::json!({
        "external_id": ext,
        "kind": "message",
        "container": container,
        "title": r.subject.clone().unwrap_or_default(),
        "author": author,
        "recipients": r.recipients.clone().unwrap_or_default(),
        "body": r.body,
        "occurred_at": iso8601_utc(r.timestamp),
        "thread_id": r.thread_id.clone().unwrap_or_default(),
        "url": ext,
        "attributes": { "source": r.plugin_type, "kastrup_id": r.id },
    })
}

/// `2026-09-03T14:05:09Z` from a unix timestamp.
fn iso8601_utc(ts: i64) -> String {
    let days = ts.div_euclid(86400);
    let (y, m, d) = crate::days_to_ymd(days);
    let t = ts.rem_euclid(86400);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, t / 3600, (t % 3600) / 60, t % 60)
}

/// Send what arrived since the watermark. Returns (rows sent, ms, drafts
/// written from the outbox), or None when there was nothing to send.
/// Never raises: a failed POST logs one line and leaves the watermark
/// where it was.
pub fn push_new(db: &Arc<Database>, cfg: &PushConfig) -> Option<(usize, u128, usize)> {
    let key = std::fs::read_to_string(&cfg.key_file).ok()?.trim().to_string();
    if key.is_empty() { return None; }
    // First run: start from now. What came before was loaded by hand, and
    // the indexer dedups on external_id in any case.
    let mark: i64 = match db.get_setting(WATERMARK).and_then(|v| v.parse().ok()) {
        Some(m) => m,
        None => {
            let top = db.max_message_id();
            db.set_setting(WATERMARK, &top.to_string());
            return None;
        }
    };
    let t0 = std::time::Instant::now();
    let mut sent = 0usize;
    let mut drafts = 0usize;
    let mut mark = mark;
    loop {
        let rows = db.rows_after(mark, BATCH);
        if rows.is_empty() { break; }
        let last = rows.last().map(|r| r.id).unwrap_or(mark);
        let body: Vec<serde_json::Value> = rows.iter().map(record).collect();
        let url = format!("{}/api/push/{}", cfg.url.trim_end_matches('/'), cfg.connector);
        let resp = sources::http_agent()
            .post(&url)
            .set("X-Push-Key", &key)
            .set("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(5))
            .send_string(&serde_json::Value::Array(body).to_string());
        match resp {
            Ok(r) => {
                sent += rows.len();
                mark = last;
                db.set_setting(WATERMARK, &mark.to_string());
                if let Some(items) = r.into_string().ok()
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                    .and_then(|v| v.get("outbox").and_then(|o| o.as_array()).cloned()) {
                    drafts += write_outbox(db, cfg, &items);
                }
                if rows.len() < BATCH { break; }
            }
            Err(e) => {
                crate::log::info(&format!("push: {} rows failed: {}", rows.len(), e));
                break;
            }
        }
    }
    if sent == 0 { return None; }
    let ms = t0.elapsed().as_millis();
    crate::log::info(&format!("push: {} rows, {} ms", sent, ms));
    Some((sent, ms, drafts))
}

/// Fetch the outbox without pushing (for `--push-now`). Returns the
/// number of drafts written.
pub fn pull_outbox(db: &Arc<Database>, cfg: &PushConfig) -> usize {
    let Some(key) = std::fs::read_to_string(&cfg.key_file).ok().map(|k| k.trim().to_string()) else { return 0 };
    if key.is_empty() { return 0; }
    let url = format!("{}/api/push/{}/outbox", cfg.url.trim_end_matches('/'), cfg.connector);
    let resp = sources::http_agent().get(&url).set("X-Push-Key", &key)
        .timeout(std::time::Duration::from_secs(5)).call();
    match resp {
        Ok(r) => {
            let items = r.into_string().ok()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .map(|v| match v {
                    serde_json::Value::Array(a) => a,
                    other => other.get("outbox").and_then(|o| o.as_array()).cloned().unwrap_or_default(),
                }).unwrap_or_default();
            write_outbox(db, cfg, &items)
        }
        Err(e) => { crate::log::info(&format!("outbox: {}", e)); 0 }
    }
}

/// One draft file per outbox item. Items that name no reachable target
/// are logged and dropped; the indexer already counts them as handed.
fn write_outbox(db: &Arc<Database>, cfg: &PushConfig, items: &[serde_json::Value]) -> usize {
    if items.is_empty() { return 0; }
    let dir = crate::drafts_drop_dir();
    let _ = std::fs::create_dir_all(&dir);
    let mut n = 0usize;
    for item in items {
        let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("x");
        let Some((ext, text)) = draft_for(db, cfg, item) else {
            crate::log::info(&format!("outbox: {} has no target, dropped", id));
            continue;
        };
        let name = format!("ci_{}.{}", id.chars().take(8).collect::<String>(), ext);
        let tmp = dir.join(format!(".{}.tmp", name));
        if std::fs::write(&tmp, text).is_ok() && std::fs::rename(&tmp, dir.join(&name)).is_ok() {
            n += 1;
        }
    }
    if n > 0 { crate::log::info(&format!("outbox: {} draft(s) written", n)); }
    n
}

/// The file extension and full text for one approved reply. A reply
/// answers the kastrup row named by in_reply_to; a new message goes to
/// the channel the counterpart last wrote from. Workspace gets Conv /
/// ReplyTo, the other chat kinds a `Channel:` line via chat_target, and
/// mail (or nothing to go on) an .eml the user completes at review.
fn draft_for(db: &Arc<Database>, cfg: &PushConfig, item: &serde_json::Value) -> Option<(&'static str, String)> {
    let s = |k: &str| item.get(k).and_then(|v| v.as_str()).map(|v| v.trim().to_string()).unwrap_or_default();
    let body = s("body");
    if body.is_empty() { return None; }
    let to = s("to");
    let channel = s("channel");
    let reply_id: Option<i64> = s("in_reply_to").strip_prefix("kastrup:").and_then(|n| n.parse().ok());
    let mut original = match reply_id {
        Some(id) => db.get_message(id),
        None if !to.is_empty() && !channel.is_empty() && channel != "email" =>
            db.latest_message_from(&to, &channel).and_then(|id| db.get_message(id)),
        None => None,
    };
    if let Some(m) = original.as_mut() {
        m.source_type = db.get_source_type_map().get(&m.source_id).cloned().unwrap_or_default();
    }
    if let Some(m) = &original {
        if m.source_type == "workspace" {
            let conv = m.metadata.get("conversation_id").and_then(|v| v.as_str()).unwrap_or("");
            if conv.is_empty() { return None; }
            let mut head = format!("Conv: {}\n", conv);
            if reply_id.is_some() { head.push_str(&format!("ReplyTo: {}\n", m.external_id)); }
            if let Some(f) = m.folder.as_deref().filter(|f| !f.is_empty()) {
                head.push_str(&format!("Channel: {}\n", f));
            }
            return Some(("workspace", format!("{}\n{}\n", head, body)));
        }
        if let Some(Ok((kind, target, _))) = crate::chat_target(m) {
            return Some((kind.tag(), format!("Channel: {}\n\n{}\n", target, body)));
        }
    }
    let (to_line, subject, links) = match &original {
        Some(m) => {
            let subj = s("subject");
            let subj = if !subj.is_empty() { subj } else {
                let orig = m.subject.clone().unwrap_or_default();
                if orig.starts_with("Re:") { orig } else { format!("Re: {}", orig) }
            };
            let mut links = format!("X-Kastrup-Reply-To: {}\n", m.id);
            if let Some(mid) = m.thread_id.as_deref().filter(|t| !t.is_empty()) {
                links.push_str(&format!("In-Reply-To: <{}>\n", mid.trim_matches(|c| c == '<' || c == '>')));
            }
            (if to.is_empty() { m.sender.clone() } else { to }, subj, links)
        }
        None => (to, s("subject"), String::new()),
    };
    if to_line.is_empty() { return None; }
    Some(("eml", format!("From: {}\nTo: {}\nCc: {}\nSubject: {}\n{}\n{}\n",
        cfg.from, to_line, s("cc"), subject, links, body)))
}

use crate::sources;
