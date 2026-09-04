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
//! One way only: the indexer answers on its own channels. Nothing it
//! sends comes back through kastrup.

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
        "attributes": attributes(r),
    })
}

/// What the row is, and the ids an answer would need: the conversation
/// for Workspace, the channel for Discord, the buffer for a relay. The
/// indexer replies on its own connectors, and these say where.
fn attributes(r: &crate::database::PushRow) -> serde_json::Value {
    let mut a = serde_json::json!({ "source": r.plugin_type, "kastrup_id": r.id });
    let m = |k: &str| r.metadata.get(k).and_then(|v| v.as_str()).filter(|v| !v.is_empty());
    if !r.external_id.is_empty() { a["message_id"] = r.external_id.clone().into(); }
    if let Some(v) = m("conversation_id") { a["conversation_id"] = v.into(); }
    if let Some(v) = m("discord_channel_id") { a["channel_id"] = v.into(); }
    if let Some(v) = m("thread_key") { a["thread_key"] = v.into(); }
    if r.plugin_type == "weechat-relay" {
        if let Some(f) = r.folder.as_deref().filter(|f| !f.is_empty()) { a["buffer"] = f.into(); }
    }
    a
}

/// `2026-09-03T14:05:09Z` from a unix timestamp.
fn iso8601_utc(ts: i64) -> String {
    let days = ts.div_euclid(86400);
    let (y, m, d) = crate::days_to_ymd(days);
    let t = ts.rem_euclid(86400);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, t / 3600, (t % 3600) / 60, t % 60)
}

/// Send what arrived since the watermark. Returns (rows sent, ms), or
/// None when there was nothing to send. Never raises: a failed POST
/// logs one line and leaves the watermark where it was.
pub fn push_new(db: &Arc<Database>, cfg: &PushConfig) -> Option<(usize, u128)> {
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
            Ok(_) => {
                sent += rows.len();
                mark = last;
                db.set_setting(WATERMARK, &mark.to_string());
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
    Some((sent, ms))
}

use crate::sources;
