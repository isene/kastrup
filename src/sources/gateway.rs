use super::MessageData;
use std::collections::HashSet;
use std::path::PathBuf;

/// Gateway source: drains uniform message JSON files dropped by the phone
/// `relay` app (com.isene.relay) into a Syncthing-synced inbound dir. This
/// replaces the Marionette-based instagram/messenger scrapers — the phone's
/// NotificationListener captures incoming DMs and writes them here.
///
/// Each file under `<gateway_dir>/inbound/`:
///   {"platform":"messenger","thread_key":"Alice","sender":"Alice",
///    "text":"hi","timestamp":1716900000,"group":false}
///
/// Source `config`: { "gateway_dir": "~/.kastrup/gateway" } (default if unset).
/// `~/` and `$HOME` expand. Files are drained (deleted) on read, mirroring
/// tock's `incoming/` import — `known_ids` still dedups within a batch.
pub fn sync_gateway(config: &serde_json::Value, known_ids: &HashSet<String>) -> Vec<MessageData> {
    let base = config
        .get("gateway_dir")
        .and_then(|v| v.as_str())
        .map(expand_tilde)
        .unwrap_or_else(default_dir);
    let inbound = base.join("inbound");
    if !inbound.is_dir() {
        return Vec::new();
    }

    let entries = match std::fs::read_dir(&inbound) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };

    let mut messages = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else {
            // Possibly half-synced; leave it for the next pass.
            continue;
        };

        let platform = v.get("platform").and_then(|x| x.as_str()).unwrap_or("");
        let thread_key = v.get("thread_key").and_then(|x| x.as_str()).unwrap_or("");
        let body = v.get("text").and_then(|x| x.as_str()).unwrap_or("");
        let sender = v.get("sender").and_then(|x| x.as_str()).unwrap_or(thread_key);
        let ts = v.get("timestamp").and_then(|x| x.as_i64()).unwrap_or_else(now_secs);
        let group = v.get("group").and_then(|x| x.as_bool()).unwrap_or(false);

        // Optional media: array of {file, mime}. `file` is relative to
        // <gateway_dir>/inbound/. The relay writes the bitmap pulled off
        // the phone notification (still images / photo previews only —
        // see the gateway contract). Syncthing carries the file alongside
        // the JSON, but the two can arrive out of order: if a referenced
        // media file hasn't synced yet, treat the WHOLE message as
        // not-ready and leave the JSON for the next pass (do NOT drain).
        let media_refs: Vec<(PathBuf, String)> = v.get("media")
            .and_then(|m| m.as_array())
            .map(|arr| arr.iter().filter_map(|m| {
                let file = m.get("file").and_then(|x| x.as_str())?;
                let mime = m.get("mime").and_then(|x| x.as_str()).unwrap_or("image/jpeg");
                Some((inbound.join(file), mime.to_string()))
            }).collect())
            .unwrap_or_default();
        if media_refs.iter().any(|(p, _)| !p.exists()) {
            // Media still in flight — retry next tick without consuming.
            continue;
        }

        // Drain the JSON now that any media has landed. A crash between this
        // delete and the poller's DB insert would drop one message;
        // acceptable for the drop-folder pattern (same as tock incoming/).
        let _ = std::fs::remove_file(&path);

        // Allow media-only messages through (e.g. a photo with empty text):
        // require text OR at least one media file.
        if platform.is_empty() || thread_key.is_empty()
            || (body.is_empty() && media_refs.is_empty())
        {
            for (p, _) in &media_refs { let _ = std::fs::remove_file(p); }
            continue;
        }

        let label = match platform {
            "messenger" => "Messenger",
            "instagram" => "Instagram",
            "whatsapp" => "WhatsApp",
            "telegram" => "Telegram",
            "signal" => "Signal",
            "sms" => "SMS",
            "linkedin" => "LinkedIn",
            "reddit" => "Reddit",
            other => other,
        };

        let ext_id = format!("gw_{}_{}_{}", platform, thread_key, ts);
        if known_ids.contains(&ext_id) {
            for (p, _) in &media_refs { let _ = std::fs::remove_file(p); }
            continue;
        }

        // Move each synced media file out of the volatile inbound/ dir into
        // a stable store kastrup owns (OUTSIDE the synced gateway/ folder so
        // the move doesn't just re-sync), and describe it as a local-path
        // attachment so the existing inline-image display path renders it.
        let media_dir = home_dir().join(".kastrup").join("gateway_media");
        let _ = std::fs::create_dir_all(&media_dir);
        let mut attachments: Vec<serde_json::Value> = Vec::new();
        for (i, (src, mime)) in media_refs.iter().enumerate() {
            let ext = mime.rsplit('/').next().unwrap_or("jpg");
            let name = format!("{}_{}.{}", ext_id, i, ext);
            let dest = media_dir.join(&name);
            if std::fs::rename(src, &dest).is_err()
                && std::fs::copy(src, &dest).map(|_| { let _ = std::fs::remove_file(src); }).is_err()
            {
                continue;
            }
            attachments.push(serde_json::json!({
                "name": name,
                "content_type": mime,
                "path": dest.to_string_lossy().to_string(),
            }));
        }

        messages.push(MessageData {
            external_id: ext_id,
            sender: sender.to_string(),
            sender_name: Some(sender.to_string()),
            recipients: thread_key.to_string(),
            cc: None,
            bcc: None,
            subject: Some(thread_key.to_string()),
            content: body.to_string(),
            html_content: None,
            timestamp: ts,
            labels: vec![label.to_string()],
            attachments,
            metadata: serde_json::json!({
                "thread_key": thread_key,
                "platform": platform,
                "group": group,
                "source": "gateway",
            }),
            folder: Some(thread_key.to_string()),
            thread_id: Some(thread_key.to_string()),
        });
    }

    messages
}

fn gateway_base(config: &serde_json::Value) -> std::path::PathBuf {
    config
        .get("gateway_dir")
        .and_then(|v| v.as_str())
        .map(expand_tilde)
        .unwrap_or_else(default_dir)
}

/// Queue an outbound reply for the phone to fire. Writes a request file to
/// `<gateway_dir>/outbox/<id>.json`; the relay app matches it to the thread's
/// cached notification RemoteInput and sends (so it posts as the user). The
/// relay reports the outcome back in `<gateway_dir>/outbox_status/<id>.json`.
/// Returns the request `id` so the caller can correlate that status.
pub fn queue_reply(
    config: &serde_json::Value,
    platform: &str,
    thread_key: &str,
    text: &str,
) -> Result<String, String> {
    let outbox = gateway_base(config).join("outbox");
    std::fs::create_dir_all(&outbox).map_err(|e| format!("create outbox: {e}"))?;
    let id = format!("{}-{}", now_secs(), rand_suffix());
    let req = serde_json::json!({
        "id": id,
        "platform": platform,
        "thread_key": thread_key,
        "text": text,
        "ts": now_secs(),
    });
    std::fs::write(outbox.join(format!("{id}.json")), req.to_string())
        .map_err(|e| format!("write reply request: {e}"))?;
    Ok(id)
}

/// Drain delivery-status markers the relay wrote and return `(id, status,
/// reason)` for each (status `"sent"`/`"failed"`; reason e.g.
/// `"no_live_notification"`). Reads BOTH protocols the relay may use and
/// consumes (deletes) every marker so a result is reported at most once:
///   * `<gateway>/sent/<id>.json.ack`   `{request, ok, ts}` — the relay's
///     long-standing per-request ack. `request` is the outbox filename, so
///     the id is `request` minus `.json`; `ok` maps to sent/failed.
///   * `<gateway>/outbox_status/<id>.json` `{id, status, reason}` — the newer
///     richer shape.
pub fn poll_reply_status(config: &serde_json::Value) -> Vec<(String, String, String)> {
    let base = gateway_base(config);
    let mut out = Vec::new();

    // Legacy per-request ack the relay already writes.
    if let Ok(rd) = std::fs::read_dir(base.join("sent")) {
        for entry in rd.flatten() {
            let path = entry.path();
            if !path.to_string_lossy().ends_with(".json.ack") { continue; }
            if let Ok(text) = std::fs::read_to_string(&path) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    let req = v.get("request").and_then(|x| x.as_str()).unwrap_or_default();
                    let id = req.strip_suffix(".json").unwrap_or(req).to_string();
                    let ok = v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false);
                    if !id.is_empty() {
                        out.push((id,
                            if ok { "sent".into() } else { "failed".into() },
                            if ok { String::new() } else { "no_live_notification".into() }));
                    }
                }
            }
            let _ = std::fs::remove_file(&path);
        }
    }

    // Newer richer status shape.
    if let Ok(rd) = std::fs::read_dir(base.join("outbox_status")) {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") { continue; }
            if let Ok(text) = std::fs::read_to_string(&path) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    let id = v.get("id").and_then(|x| x.as_str()).unwrap_or_default().to_string();
                    let status = v.get("status").and_then(|x| x.as_str()).unwrap_or_default().to_string();
                    let reason = v.get("reason").and_then(|x| x.as_str()).unwrap_or_default().to_string();
                    if !id.is_empty() { out.push((id, status, reason)); }
                }
            }
            let _ = std::fs::remove_file(&path);
        }
    }

    out
}

fn rand_suffix() -> String {
    // Cheap unique-ish suffix without pulling in a uuid crate.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{:08x}", nanos)
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn home_dir() -> PathBuf {
    std::env::var("HOME").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("."))
}

fn default_dir() -> PathBuf {
    home_dir().join(".kastrup").join("gateway")
}

fn expand_tilde(p: &str) -> PathBuf {
    let p = p.trim();
    if let Some(rest) = p.strip_prefix("~/") {
        return home_dir().join(rest);
    }
    if let Some(rest) = p.strip_prefix("$HOME/") {
        return home_dir().join(rest);
    }
    PathBuf::from(p)
}
