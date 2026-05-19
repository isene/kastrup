//! Slack polling source.
//!
//! Pulls DMs (and any explicitly-watched channels) for the user
//! identified by `SLACK_API_TOKEN` in `~/.kastrup/.env`. Falls back
//! to extracting the `xoxp-…` token from `~/.weechat/plugins.conf`
//! when the env file doesn't set it (see `chat_send::load_secrets`).
//!
//! By default the poller only walks IM + MPIM (direct messages and
//! group DMs) — that's the small-N, high-signal set. The user can
//! opt-in extra channels by name or ID via the source's config:
//!
//! ```json
//! { "watch_channels": ["#general", "C0123ABCDEF"] }
//! ```
//!
//! Per-message external_id = `"<channel_id>:<ts>"` so the dedup set
//! in poller.rs can survive channel reshuffles. Author's own
//! messages are skipped (already represented in the user's outbox).

use std::collections::HashSet;
use crate::sources::MessageData;
use crate::chat_send;

const API: &str = "https://slack.com/api";

pub fn sync_slack(config: &serde_json::Value, known_ids: &HashSet<String>) -> Vec<MessageData> {
    let secrets = chat_send::load_secrets();
    let Some(token) = secrets.slack_token.as_ref() else {
        return Vec::new();
    };
    let bearer = format!("Bearer {}", token);

    // Own user id, to skip self-authored messages.
    let self_id = match auth_test(&bearer) {
        Some(id) => id,
        None => return Vec::new(),
    };

    // 1. DM / group-DM channels (always polled).
    let mut channels: Vec<(String, String)> = list_dm_channels(&bearer);

    // 2. Explicit watch_channels from config: names (`#general`) or
    // IDs. Names are resolved via conversations.list against the
    // public/private channel set.
    if let Some(watch) = config.get("watch_channels").and_then(|v| v.as_array()) {
        let watch_names: Vec<String> = watch.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        if !watch_names.is_empty() {
            let resolved = resolve_channels(&bearer, &watch_names);
            for r in resolved {
                if !channels.iter().any(|(id, _)| id == &r.0) {
                    channels.push(r);
                }
            }
        }
    }

    let mut out: Vec<MessageData> = Vec::new();
    for (cid, label) in channels {
        let msgs = match fetch_history(&bearer, &cid, 20) {
            Some(v) => v,
            None => continue,
        };
        for m in msgs {
            // Skip subtypes that aren't actual messages (joins, etc.)
            if let Some(sub) = m["subtype"].as_str() {
                if !["bot_message", "thread_broadcast", "me_message"].contains(&sub) {
                    continue;
                }
            }
            let ts = m["ts"].as_str().unwrap_or("");
            if ts.is_empty() { continue; }
            let ext_id = format!("{}:{}", cid, ts);
            if known_ids.contains(&ext_id) { continue; }

            let user_id = m["user"].as_str().unwrap_or("").to_string();
            if user_id == self_id { continue; }

            let content = decode_slack_text(m["text"].as_str().unwrap_or(""));
            if content.is_empty() && m["attachments"].as_array().map(|a| a.is_empty()).unwrap_or(true)
                && m["files"].as_array().map(|a| a.is_empty()).unwrap_or(true)
            {
                continue;
            }

            // Resolve a display name. Cheap on misses (returns the ID).
            let author_name = lookup_user_name(&bearer, &user_id);

            // Float-ts → unix seconds.
            let timestamp = ts.split('.').next()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);

            let attachments: Vec<serde_json::Value> = m["files"].as_array()
                .map(|arr| arr.iter().map(|f| serde_json::json!({
                    "filename": f["name"].as_str().unwrap_or(""),
                    "url":      f["url_private"].as_str()
                                  .or_else(|| f["permalink"].as_str())
                                  .unwrap_or(""),
                    "size":     f["size"].as_i64().unwrap_or(0),
                })).collect())
                .unwrap_or_default();

            let metadata = serde_json::json!({
                "slack_channel_id": cid,
                "slack_user_id":    user_id,
                "slack_ts":         ts,
                "source_type":      "slack",
            });

            let subject = if content.is_empty() {
                format!("Message from {}", author_name)
            } else {
                let line = content.lines()
                    .map(|l| l.trim())
                    .find(|l| !l.is_empty())
                    .unwrap_or(content.as_str());
                let mut s: String = line.chars().take(80).collect();
                if line.chars().count() > 80 { s.push('…'); }
                s
            };

            out.push(MessageData {
                external_id: ext_id,
                sender: user_id.clone(),
                sender_name: Some(if author_name.is_empty() {
                    "Slack user".to_string()
                } else { author_name.clone() }),
                recipients: label.clone(),
                cc: None,
                bcc: None,
                subject: Some(subject),
                content,
                html_content: None,
                timestamp,
                labels: vec!["slack".to_string()],
                attachments,
                metadata,
                folder: Some("Slack".to_string()),
                thread_id: Some(cid.clone()),
            });
        }
    }
    out
}

// --- API helpers -----------------------------------------------------------

fn auth_test(bearer: &str) -> Option<String> {
    let resp = ureq::post(&format!("{}/auth.test", API))
        .set("Authorization", bearer)
        .call().ok()?
        .into_string().ok()?;
    let v: serde_json::Value = serde_json::from_str(&resp).ok()?;
    if !v["ok"].as_bool().unwrap_or(false) { return None; }
    v["user_id"].as_str().map(|s| s.to_string())
}

/// Return (channel_id, human label) for every DM and group-DM the
/// token can see.
fn list_dm_channels(bearer: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let mut url = format!("{}/conversations.list?limit=200&types=im,mpim", API);
        if let Some(c) = &cursor {
            url.push_str("&cursor=");
            url.push_str(c);
        }
        let Ok(resp) = ureq::get(&url).set("Authorization", bearer).call() else { break };
        let Ok(text) = resp.into_string() else { break };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else { break };
        if !v["ok"].as_bool().unwrap_or(false) { break; }
        if let Some(chans) = v["channels"].as_array() {
            for ch in chans {
                let id = ch["id"].as_str().unwrap_or("").to_string();
                if id.is_empty() { continue; }
                // IM channels carry a "user" field with the peer's
                // user_id; we resolve to a name for the label.
                let label = if ch["is_im"].as_bool().unwrap_or(false) {
                    let peer = ch["user"].as_str().unwrap_or("");
                    lookup_user_name(bearer, peer)
                } else if ch["is_mpim"].as_bool().unwrap_or(false) {
                    ch["name"].as_str().unwrap_or("group dm").to_string()
                } else {
                    format!("#{}", ch["name"].as_str().unwrap_or(""))
                };
                out.push((id, label));
            }
        }
        cursor = v["response_metadata"]["next_cursor"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if cursor.is_none() { break; }
    }
    out
}

/// Resolve a mixed list of `#name` / `C…` / bare-name entries to
/// `(channel_id, label)`. Names get one paginated walk of
/// conversations.list; IDs are passed through with a lookup.
fn resolve_channels(bearer: &str, items: &[String]) -> Vec<(String, String)> {
    // Split into IDs vs names.
    let mut wanted_names: HashSet<String> = HashSet::new();
    let mut ids: Vec<String> = Vec::new();
    for raw in items {
        let s = raw.trim();
        if s.is_empty() { continue; }
        if (s.starts_with('C') || s.starts_with('G')) && s.len() >= 9
            && s[1..].chars().all(|c| c.is_ascii_alphanumeric())
        {
            ids.push(s.to_string());
        } else {
            wanted_names.insert(s.trim_start_matches('#').to_ascii_lowercase());
        }
    }
    let mut out: Vec<(String, String)> = Vec::new();
    for id in ids {
        out.push((id.clone(), format!("#{}", id)));
    }
    if wanted_names.is_empty() { return out; }
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let mut url = format!(
            "{}/conversations.list?limit=1000&types=public_channel,private_channel",
            API
        );
        if let Some(c) = &cursor {
            url.push_str("&cursor=");
            url.push_str(c);
        }
        let Ok(resp) = ureq::get(&url).set("Authorization", bearer).call() else { break };
        let Ok(text) = resp.into_string() else { break };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else { break };
        if !v["ok"].as_bool().unwrap_or(false) { break; }
        if let Some(chans) = v["channels"].as_array() {
            for ch in chans {
                let name = ch["name"].as_str().unwrap_or("").to_ascii_lowercase();
                if wanted_names.contains(&name) {
                    let id = ch["id"].as_str().unwrap_or("").to_string();
                    if !id.is_empty() {
                        out.push((id, format!("#{}", name)));
                    }
                }
            }
        }
        cursor = v["response_metadata"]["next_cursor"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if cursor.is_none() { break; }
    }
    out
}

fn fetch_history(bearer: &str, channel_id: &str, limit: u32) -> Option<Vec<serde_json::Value>> {
    let url = format!("{}/conversations.history?channel={}&limit={}", API, channel_id, limit);
    let resp = ureq::get(&url)
        .set("Authorization", bearer)
        .call().ok()?
        .into_string().ok()?;
    let v: serde_json::Value = serde_json::from_str(&resp).ok()?;
    if !v["ok"].as_bool().unwrap_or(false) { return None; }
    v["messages"].as_array().cloned()
}

/// Cheap user.info lookup. We don't cache — Slack's tier-3 limit
/// (~50/min) is plenty for typical DM volume, and the cost of a
/// process-lifetime cache that goes stale on display-name changes
/// is more than the saved syscalls. Returns the user_id itself when
/// the lookup fails so the picker still shows *something*.
fn lookup_user_name(bearer: &str, user_id: &str) -> String {
    if user_id.is_empty() { return String::new(); }
    let url = format!("{}/users.info?user={}", API, user_id);
    let Ok(resp) = ureq::get(&url).set("Authorization", bearer).call() else {
        return user_id.to_string();
    };
    let Ok(text) = resp.into_string() else { return user_id.to_string() };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else {
        return user_id.to_string();
    };
    if !v["ok"].as_bool().unwrap_or(false) { return user_id.to_string(); }
    let prof = &v["user"]["profile"];
    let name = prof["display_name"].as_str()
        .filter(|s| !s.is_empty())
        .or_else(|| prof["real_name"].as_str())
        .or_else(|| v["user"]["name"].as_str())
        .unwrap_or(user_id);
    name.to_string()
}

/// Replace Slack's `<@U…>` / `<#C…>` / `<https://…|label>` /
/// `<https://…>` formatting with readable text. Slack stores
/// messages with these inline tokens; ignoring them dumps raw IDs
/// into the right-pane render and leaves links un-clickable.
fn decode_slack_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'<' {
            if let Some(end_rel) = s[i..].find('>') {
                let inner = &s[i + 1..i + end_rel];
                // `<!channel>` / `<!here>` → `@channel` / `@here`
                if let Some(name) = inner.strip_prefix('!') {
                    out.push('@');
                    out.push_str(name.split('|').next().unwrap_or(name));
                    i += end_rel + 1;
                    continue;
                }
                // `<@U…|label>` or `<@U…>`
                if let Some(rest) = inner.strip_prefix('@') {
                    let label = rest.split('|').nth(1).unwrap_or_else(|| rest.split('|').next().unwrap_or(rest));
                    out.push('@');
                    out.push_str(label);
                    i += end_rel + 1;
                    continue;
                }
                // `<#C…|label>` or `<#C…>`
                if let Some(rest) = inner.strip_prefix('#') {
                    let label = rest.split('|').nth(1).unwrap_or_else(|| rest.split('|').next().unwrap_or(rest));
                    out.push('#');
                    out.push_str(label);
                    i += end_rel + 1;
                    continue;
                }
                // URL with optional label: `<url|label>` → `label (url)`
                let (url, label) = match inner.split_once('|') {
                    Some((u, l)) => (u, Some(l)),
                    None => (inner, None),
                };
                if url.starts_with("http://") || url.starts_with("https://") || url.starts_with("mailto:") {
                    if let Some(l) = label {
                        out.push_str(l);
                        out.push_str(" (");
                        out.push_str(url);
                        out.push(')');
                    } else {
                        out.push_str(url);
                    }
                    i += end_rel + 1;
                    continue;
                }
                // Fall through: keep the literal `<…>`.
                out.push('<');
                out.push_str(inner);
                out.push('>');
                i += end_rel + 1;
                continue;
            }
        }
        // Decode the three Slack-special entities; pass everything else.
        if s[i..].starts_with("&amp;")  { out.push('&'); i += 5; continue; }
        if s[i..].starts_with("&lt;")   { out.push('<'); i += 4; continue; }
        if s[i..].starts_with("&gt;")   { out.push('>'); i += 4; continue; }
        // Otherwise copy the next char's bytes (must respect UTF-8).
        let ch = match s[i..].chars().next() {
            Some(c) => c,
            None => break,
        };
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}
