use super::MessageData;
use std::collections::HashSet;
use std::path::PathBuf;

/// Sync Messenger DMs via Heathrow's Marionette-based Python script.
/// Connects to a running Firefox instance on Marionette port 2828,
/// scrapes the Messenger tab for thread list and snippets.
pub fn sync_messenger(_config: &serde_json::Value, known_ids: &HashSet<String>) -> Vec<MessageData> {
    let script_path = home_dir()
        .join("Main/G/GIT-isene/heathrow/lib/heathrow/sources/messenger_fetch_marionette.py");
    if !script_path.exists() { return Vec::new(); }

    // Run with timeout to avoid hanging
    let output = std::process::Command::new("timeout")
        .args(["30", "python3"])
        .arg(&script_path)
        .stderr(std::process::Stdio::null())
        .output();

    let Ok(output) = output else { return Vec::new() };
    if !output.status.success() { return Vec::new(); }

    let json_str = String::from_utf8_lossy(&output.stdout);
    if json_str.trim().is_empty() { return Vec::new(); }

    let Ok(data) = serde_json::from_str::<serde_json::Value>(&json_str) else { return Vec::new() };

    // Check for error in response
    if data.get("error").and_then(|e| e.as_str()).is_some() { return Vec::new(); }

    let threads = data.get("threads")
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();

    let mut messages = Vec::new();
    let now = now_secs();

    for thread in &threads {
        let thread_id = match thread.get("id").and_then(|v| v.as_str()) {
            Some(id) if id.chars().all(|c| c.is_ascii_digit()) => id.to_string(),
            _ => continue,
        };

        let thread_name = thread.get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();
        if thread_name.is_empty() { continue; }

        let unread = thread.get("unread").and_then(|v| v.as_bool()).unwrap_or(false);

        // Process explicit messages array if present
        let thread_messages = thread.get("messages")
            .and_then(|m| m.as_array())
            .cloned()
            .unwrap_or_default();

        if !thread_messages.is_empty() {
            for msg in &thread_messages {
                let text = msg.get("text").and_then(|v| v.as_str()).unwrap_or("").trim();
                if text.is_empty() { continue; }
                if is_ui_garbage(text) { continue; }

                let msg_id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let sender = msg.get("sender").and_then(|v| v.as_str()).unwrap_or(&thread_name).to_string();
                let timestamp = msg.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(now);

                // Match Heathrow format: msng_{thread_id}_{msg_id}
                let ext_id = if msg_id.is_empty() {
                    format!("msng_{}_{}", thread_id, timestamp)
                } else {
                    format!("msng_{}_{}", thread_id, msg_id)
                };

                if known_ids.contains(&ext_id) { continue; }

                messages.push(MessageData {
                    external_id: ext_id,
                    sender: sender.clone(),
                    sender_name: Some(sender),
                    recipients: thread_name.clone(),
                    cc: None,
                    subject: Some(thread_name.clone()),
                    content: text.to_string(),
                    html_content: None,
                    timestamp,
                    labels: vec!["Messenger".to_string()],
                    attachments: Vec::new(),
                    metadata: serde_json::json!({
                        "thread_id": thread_id,
                        "message_id": msg_id,
                        "platform": "messenger",
                    }),
                    folder: Some(thread_name.clone()),
                    thread_id: Some(thread_id.clone()),
                });
            }
        } else {
            // Fallback: use snippet from sidebar scrape
            let snippet = thread.get("snippet")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();

            // Skip encryption notices
            if snippet.to_lowercase().contains("messages and calls are secured")
                || snippet.to_lowercase().contains("end-to-end encrypted")
            {
                continue;
            }

            if snippet.is_empty() && !unread { continue; }
            if snippet.is_empty() { continue; }

            let ext_id = format!("msng_{}_last_{}", thread_id, thread_id);
            if known_ids.contains(&ext_id) { continue; }

            messages.push(MessageData {
                external_id: ext_id,
                sender: thread_name.clone(),
                sender_name: Some(thread_name.clone()),
                recipients: thread_name.clone(),
                cc: None,
                subject: Some(thread_name.clone()),
                content: snippet,
                html_content: None,
                timestamp: now,
                labels: vec!["Messenger".to_string()],
                attachments: Vec::new(),
                metadata: serde_json::json!({
                    "thread_id": thread_id,
                    "message_id": format!("last_{}", thread_id),
                    "platform": "messenger",
                }),
                folder: Some(thread_name.clone()),
                thread_id: Some(thread_id.clone()),
            });
        }
    }

    messages
}

fn is_ui_garbage(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.starts_with("today at ")
        || lower.starts_with("yesterday at ")
        || lower.starts_with("enter, message sent")
        || lower.starts_with("you sent")
        || lower.starts_with("you replied")
        || lower.starts_with("you reacted")
        || text.len() < 2
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
