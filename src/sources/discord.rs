//! Discord DM polling source.
//!
//! Lists the bot's DM channels via `GET /users/@me/channels`, then for
//! each one pulls the newest ~20 messages from
//! `GET /channels/<id>/messages` and emits any whose Discord message
//! id isn't already in `known_ids`. The poller layer takes care of
//! per-tick cadence and dedup persistence.
//!
//! The bot must have at least one DM channel that's seen activity for
//! Discord to surface it in `users/@me/channels`. New DMs to/from the
//! bot create channels automatically, so the polling covers both
//! sides without explicit subscription.
//!
//! Auth: reads `DISCORD_BOT_TOKEN` via `chat_send::load_secrets()`
//! (same as the send path).

use std::collections::HashSet;
use crate::sources::MessageData;
use crate::chat_send;

/// Discord's REST API base for v10. Hardcoded to avoid a config knob.
const API: &str = "https://discord.com/api/v10";

pub fn sync_discord(config: &serde_json::Value, known_ids: &HashSet<String>) -> Vec<MessageData> {
    let secrets = chat_send::load_secrets();
    let Some(token) = secrets.discord_bot_token.as_ref() else {
        return Vec::new();
    };
    let auth = if token.starts_with("Bot ") || token.starts_with("Bearer ") {
        token.to_string()
    } else {
        format!("Bot {}", token)
    };

    // Bot's own identity — needed to tag incoming-vs-outgoing.
    let bot_user_id = match fetch_self_id(&auth) {
        Some(id) => id,
        None => return Vec::new(),
    };

    // List DM channels the bot already knows. Bots can't actually enumerate
    // their DMs (this returns empty in practice), so we also open the channel
    // for each known peer — user IDs in ~/.kastrup/discord_dm_peers, seeded by
    // hand and auto-appended whenever we send a bot DM. That's what lets a
    // reply to the bot reach kastrup at all.
    let mut channels = list_dm_channels(&auth).unwrap_or_default();
    let mut seen_cids: HashSet<String> = channels.iter()
        .filter_map(|c| c["id"].as_str().map(String::from)).collect();
    for uid in load_dm_peers() {
        if let Some(ch) = open_dm_channel(&auth, &uid) {
            if let Some(cid) = ch["id"].as_str() {
                if seen_cids.insert(cid.to_string()) { channels.push(ch); }
            }
        }
    }

    // Folder the DMs land in — defaults to PassionFruits so bot replies show in
    // View 3 alongside the relayed Discord DMs. Override via the source config.
    let folder = config.get("folder").and_then(|f| f.as_str())
        .filter(|s| !s.is_empty()).unwrap_or("PassionFruits").to_string();

    let mut out: Vec<MessageData> = Vec::new();
    for ch in channels {
        let cid = match ch["id"].as_str() {
            Some(s) => s,
            None => continue,
        };
        // Build a human label for the channel (the other recipient's name).
        let peer_label = channel_peer_label(&ch, &bot_user_id);

        let msgs = match fetch_messages(&auth, cid, 20) {
            Some(v) => v,
            None => continue,
        };
        for m in msgs {
            let mid = m["id"].as_str().unwrap_or("");
            if mid.is_empty() { continue; }
            if known_ids.contains(mid) { continue; }

            let author_id = m["author"]["id"].as_str().unwrap_or("").to_string();
            // Skip messages the bot itself sent — they're already
            // visible through the kastrup compose flow; mirroring them
            // back into the inbox would double-count outgoing traffic.
            if author_id == bot_user_id { continue; }

            let author_name = m["author"]["global_name"].as_str()
                .filter(|s| !s.is_empty())
                .or_else(|| m["author"]["username"].as_str())
                .unwrap_or("")
                .to_string();
            let content = m["content"].as_str().unwrap_or("").to_string();
            let ts_iso = m["timestamp"].as_str().unwrap_or("");
            let timestamp = parse_iso8601_to_unix(ts_iso).unwrap_or(0);

            // attachments → JSON array of {filename, url, size, content_type,
            // kastrup_remote, source_type}. kastrup_remote+source_type tell
            // the V/v fetch path to download directly from Discord's CDN
            // (no auth — URLs are pre-signed) instead of expecting a
            // maildir on disk.
            let attachments: Vec<serde_json::Value> = m["attachments"].as_array()
                .map(|arr| arr.iter().enumerate().map(|(i, a)| {
                    let filename = a["filename"].as_str().unwrap_or("");
                    let ct = a["content_type"].as_str()
                        .map(String::from)
                        .unwrap_or_else(|| guess_content_type(filename));
                    serde_json::json!({
                        "filename":       filename,
                        "url":            a["url"].as_str().unwrap_or(""),
                        "size":           a["size"].as_i64().unwrap_or(0),
                        "content_type":   ct,
                        "file_id":        format!("{}_{}", mid, i),
                        "kastrup_remote": true,
                        "source_type":    "discord",
                    })
                }).collect())
                .unwrap_or_default();

            // Carry channel + author IDs in metadata so the reply
            // path can construct a `.discord` template without
            // re-querying Discord.
            let metadata = serde_json::json!({
                "discord_channel_id": cid,
                "discord_author_id":  author_id,
                "discord_message_id": mid,
                "source_type":        "discord",
            });

            let subject = if content.is_empty() {
                format!("DM from {}", author_name)
            } else {
                // First non-empty line of body, truncated, as a faux subject
                let line = content.lines()
                    .map(|l| l.trim())
                    .find(|l| !l.is_empty())
                    .unwrap_or(content.as_str());
                let mut s: String = line.chars().take(80).collect();
                if line.chars().count() > 80 { s.push('…'); }
                s
            };

            out.push(MessageData {
                external_id: mid.to_string(),
                sender: author_id.clone(),
                sender_name: Some(if author_name.is_empty() {
                    "Discord user".to_string()
                } else { author_name.clone() }),
                recipients: peer_label.clone(),
                cc: None,
                bcc: None,
                subject: Some(subject),
                content,
                html_content: None,
                timestamp,
                labels: vec!["discord".to_string()],
                attachments,
                metadata,
                folder: Some(folder.clone()),
                thread_id: Some(cid.to_string()),
            });
        }
    }
    out
}

fn fetch_self_id(auth: &str) -> Option<String> {
    let resp = super::http_agent().get(&format!("{}/users/@me", API))
        .set("Authorization", auth)
        .set("User-Agent", "kastrup (https://github.com/isene/kastrup, 0.1)")
        .call().ok()?
        .into_string().ok()?;
    let v: serde_json::Value = serde_json::from_str(&resp).ok()?;
    v["id"].as_str().map(|s| s.to_string())
}

/// Cheap MIME guess from filename extension. Discord's API only
/// populates `content_type` for newer uploads; older attachments
/// arrive with the field absent. The V/v image check at fetch
/// time needs `content_type` OR an image-suffix filename, so we
/// fill in the gap here rather than at the call site.
fn guess_content_type(filename: &str) -> String {
    let ext = filename.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "png"           => "image/png",
        "jpg" | "jpeg"  => "image/jpeg",
        "gif"           => "image/gif",
        "webp"          => "image/webp",
        "bmp"           => "image/bmp",
        "tiff" | "tif"  => "image/tiff",
        "svg"           => "image/svg+xml",
        "mp4"           => "video/mp4",
        "webm"          => "video/webm",
        "mov"           => "video/quicktime",
        "mp3"           => "audio/mpeg",
        "ogg"           => "audio/ogg",
        "wav"           => "audio/wav",
        "pdf"           => "application/pdf",
        "txt"           => "text/plain",
        "json"          => "application/json",
        _               => "application/octet-stream",
    }.to_string()
}

fn list_dm_channels(auth: &str) -> Option<Vec<serde_json::Value>> {
    let resp = super::http_agent().get(&format!("{}/users/@me/channels", API))
        .set("Authorization", auth)
        .set("User-Agent", "kastrup (https://github.com/isene/kastrup, 0.1)")
        .call().ok()?
        .into_string().ok()?;
    let v: serde_json::Value = serde_json::from_str(&resp).ok()?;
    v.as_array().cloned()
}

/// Path to the peer file: Discord user IDs whose bot DMs we poll.
pub fn dm_peers_path() -> std::path::PathBuf {
    std::path::PathBuf::from(std::env::var("HOME").unwrap_or_default())
        .join(".kastrup/discord_dm_peers")
}

/// User IDs to poll for bot-DM replies. One id per line; first whitespace
/// token wins (so `<id>  # inline note` works); blank and `#` lines skipped.
fn load_dm_peers() -> Vec<String> {
    std::fs::read_to_string(dm_peers_path()).ok().map(|s| {
        s.lines().filter_map(|l| {
            let t = l.trim();
            if t.is_empty() || t.starts_with('#') { return None; }
            t.split_whitespace().next().map(String::from)
        }).collect()
    }).unwrap_or_default()
}

/// Record a peer so its future bot-DM replies get polled. Idempotent;
/// called from the send path on every `dm:<userId>` bot send.
pub fn remember_dm_peer(user_id: &str) {
    let uid = user_id.trim();
    if uid.is_empty() || !uid.chars().all(|c| c.is_ascii_digit()) { return; }
    if load_dm_peers().iter().any(|p| p == uid) { return; }
    let path = dm_peers_path();
    if let Some(dir) = path.parent() { let _ = std::fs::create_dir_all(dir); }
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
        let _ = writeln!(f, "{}", uid);
    }
}

/// Open (or fetch the existing) 1:1 DM channel for a recipient user id.
fn open_dm_channel(auth: &str, recipient_id: &str) -> Option<serde_json::Value> {
    let body = format!("{{\"recipient_id\":\"{}\"}}", recipient_id);
    let resp = super::http_agent().post(&format!("{}/users/@me/channels", API))
        .set("Authorization", auth)
        .set("User-Agent", "kastrup (https://github.com/isene/kastrup, 0.1)")
        .set("Content-Type", "application/json")
        .send_string(&body).ok()?
        .into_string().ok()?;
    serde_json::from_str(&resp).ok()
}

fn fetch_messages(auth: &str, channel_id: &str, limit: u32) -> Option<Vec<serde_json::Value>> {
    let url = format!("{}/channels/{}/messages?limit={}", API, channel_id, limit);
    let resp = super::http_agent().get(&url)
        .set("Authorization", auth)
        .set("User-Agent", "kastrup (https://github.com/isene/kastrup, 0.1)")
        .call().ok()?
        .into_string().ok()?;
    let v: serde_json::Value = serde_json::from_str(&resp).ok()?;
    v.as_array().cloned()
}

/// Best-effort name for the "other end" of a DM channel.
/// Group DMs aggregate names with commas; 1:1 DMs use the single
/// non-bot recipient's display name.
fn channel_peer_label(ch: &serde_json::Value, bot_id: &str) -> String {
    let mut names: Vec<String> = Vec::new();
    if let Some(arr) = ch["recipients"].as_array() {
        for r in arr {
            let id = r["id"].as_str().unwrap_or("");
            if id == bot_id { continue; }
            let name = r["global_name"].as_str()
                .filter(|s| !s.is_empty())
                .or_else(|| r["username"].as_str())
                .unwrap_or("")
                .to_string();
            if !name.is_empty() { names.push(name); }
        }
    }
    if names.is_empty() {
        ch["name"].as_str().unwrap_or("").to_string()
    } else {
        names.join(", ")
    }
}

/// Parse an RFC3339-ish ISO8601 timestamp to unix seconds.
/// Discord stamps look like "2026-05-19T17:40:09.123000+00:00".
fn parse_iso8601_to_unix(s: &str) -> Option<i64> {
    if s.is_empty() { return None; }
    // Cheap parser: split into Y, M, D, h, m, s. We ignore subsecond.
    // Tail can be "Z" or "+HH:MM" / "-HH:MM" — Discord uses +00:00 in
    // practice, so treat everything as UTC.
    let bytes = s.as_bytes();
    if bytes.len() < 19 { return None; }
    let y: i64 = s.get(0..4)?.parse().ok()?;
    let mo: u32 = s.get(5..7)?.parse().ok()?;
    let d: u32 = s.get(8..10)?.parse().ok()?;
    let h: u32 = s.get(11..13)?.parse().ok()?;
    let mi: u32 = s.get(14..16)?.parse().ok()?;
    let se: u32 = s.get(17..19)?.parse().ok()?;
    let days = ymd_to_days(y, mo, d);
    Some(days * 86400 + (h as i64) * 3600 + (mi as i64) * 60 + (se as i64))
}

/// Howard Hinnant's days_from_civil for converting Y/M/D to unix-day.
fn ymd_to_days(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u64;
    let m_adj = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153u64 * (m_adj as u64) + 2) / 5 + (d as u64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}
