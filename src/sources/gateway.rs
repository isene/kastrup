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

        // Drain the file regardless (consumed). A crash between this delete and
        // the poller's DB insert would drop one message; acceptable for the
        // drop-folder pattern (same as tock incoming/).
        let _ = std::fs::remove_file(&path);

        if platform.is_empty() || thread_key.is_empty() || body.is_empty() {
            continue;
        }

        let label = match platform {
            "messenger" => "Messenger",
            "instagram" => "Instagram",
            "whatsapp" => "WhatsApp",
            "telegram" => "Telegram",
            "signal" => "Signal",
            other => other,
        };

        let ext_id = format!("gw_{}_{}_{}", platform, thread_key, ts);
        if known_ids.contains(&ext_id) {
            continue;
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
            attachments: Vec::new(),
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

/// Queue an outbound reply for the phone to fire. Writes a request file to
/// `<gateway_dir>/outbox/`; the relay app matches it to a live notification's
/// RemoteInput and sends. Works only for a thread with an active notification.
pub fn queue_reply(
    config: &serde_json::Value,
    platform: &str,
    thread_key: &str,
    text: &str,
) -> Result<(), String> {
    let base = config
        .get("gateway_dir")
        .and_then(|v| v.as_str())
        .map(expand_tilde)
        .unwrap_or_else(default_dir);
    let outbox = base.join("outbox");
    std::fs::create_dir_all(&outbox).map_err(|e| format!("create outbox: {e}"))?;
    let req = serde_json::json!({
        "platform": platform,
        "thread_key": thread_key,
        "text": text,
    });
    let name = format!("{}-{}.json", now_secs(), rand_suffix());
    std::fs::write(outbox.join(name), req.to_string())
        .map_err(|e| format!("write reply request: {e}"))
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
