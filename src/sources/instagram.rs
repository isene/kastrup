use super::MessageData;
use std::collections::HashSet;
use std::path::PathBuf;

/// Sync Instagram DMs via Heathrow's Marionette-based Python script.
/// Connects to Firefox on Marionette port 2828, fetches the Instagram
/// private API inbox endpoint through the logged-in session.
pub fn sync_instagram(_config: &serde_json::Value, known_ids: &HashSet<String>) -> Vec<MessageData> {
    let script_path = home_dir()
        .join("Main/G/GIT-isene/heathrow/lib/heathrow/sources/instagram_fetch.py");
    if !script_path.exists() { return Vec::new(); }

    // Run with timeout to avoid hanging
    let output = std::process::Command::new("timeout")
        .args(["15", "python3"])
        .arg(&script_path)
        .stderr(std::process::Stdio::null())
        .output();

    let Ok(output) = output else { return Vec::new() };
    if !output.status.success() { return Vec::new(); }

    let json_str = String::from_utf8_lossy(&output.stdout);
    if json_str.trim().is_empty() { return Vec::new(); }

    let Ok(data) = serde_json::from_str::<serde_json::Value>(&json_str) else { return Vec::new() };

    // Check for errors
    if data.get("error").and_then(|e| e.as_str()).is_some() { return Vec::new(); }
    if data.get("status").and_then(|s| s.as_str()) != Some("ok") { return Vec::new(); }

    let threads = data.get("inbox")
        .and_then(|i| i.get("threads"))
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();

    let mut messages = Vec::new();

    for thread in &threads {
        let thread_id = match thread.get("thread_id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => continue,
        };

        // Build display name from participants
        let users = thread.get("users")
            .and_then(|u| u.as_array())
            .cloned()
            .unwrap_or_default();

        let thread_title = thread.get("thread_title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let display_name = if !thread_title.is_empty() {
            thread_title
        } else {
            let names: Vec<String> = users.iter().filter_map(|u| {
                let full = u.get("full_name").and_then(|n| n.as_str()).unwrap_or("");
                if !full.is_empty() { Some(full.to_string()) }
                else { u.get("username").and_then(|n| n.as_str()).map(|s| s.to_string()) }
            }).collect();
            if names.is_empty() { "Instagram DM".to_string() } else { names.join(", ") }
        };

        let items = thread.get("items")
            .and_then(|i| i.as_array())
            .cloned()
            .unwrap_or_default();

        for item in &items {
            let item_id = match item.get("item_id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(),
                None => continue,
            };

            let item_type = item.get("item_type")
                .and_then(|v| v.as_str())
                .unwrap_or("text");

            // Timestamp in microseconds from Instagram API
            let ts_raw = item.get("timestamp")
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0);
            let timestamp = ts_raw / 1_000_000;

            // Find sender name by user_id
            let user_id = item.get("user_id").and_then(|v| v.as_u64()).unwrap_or(0);
            let sender_name = users.iter()
                .find(|u| u.get("pk").and_then(|p| p.as_u64()).unwrap_or(0) == user_id)
                .and_then(|u| {
                    let full = u.get("full_name").and_then(|n| n.as_str()).unwrap_or("");
                    if !full.is_empty() { Some(full.to_string()) }
                    else { u.get("username").and_then(|n| n.as_str()).map(|s| s.to_string()) }
                })
                .unwrap_or_else(|| display_name.clone());

            // Extract content based on item_type (matching Heathrow's logic)
            let content = extract_content(item, item_type);
            if content.is_empty() { continue; }

            // Match Heathrow format: ig_{thread_id}_{item_id}
            let ext_id = format!("ig_{}_{}", thread_id, item_id);
            if known_ids.contains(&ext_id) { continue; }

            // Extract attachments
            let attachments = extract_attachments(item, item_type, &item_id);

            let is_group = thread.get("is_group").and_then(|v| v.as_bool()).unwrap_or(false);

            messages.push(MessageData {
                external_id: ext_id,
                sender: sender_name.clone(),
                sender_name: Some(sender_name),
                recipients: display_name.clone(),
                cc: None,
                subject: Some(display_name.clone()),
                content,
                html_content: None,
                timestamp,
                labels: vec!["Instagram".to_string()],
                attachments,
                metadata: serde_json::json!({
                    "thread_id": thread_id,
                    "item_id": item_id,
                    "is_group": is_group,
                    "platform": "instagram",
                }),
                folder: Some(display_name.clone()),
                thread_id: Some(thread_id.clone()),
            });
        }
    }

    messages
}

fn extract_content(item: &serde_json::Value, item_type: &str) -> String {
    match item_type {
        "text" => item.get("text").and_then(|t| t.as_str()).unwrap_or("").to_string(),
        "media" | "media_share" => {
            let media = item.get("media")
                .or_else(|| item.get("media_share"))
                .unwrap_or(&serde_json::Value::Null);
            let caption = media.pointer("/caption/text")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            format!("[Media] {}", caption).trim().to_string()
        }
        "raven_media" => "[Disappearing photo/video]".to_string(),
        "voice_media" => "[Voice message]".to_string(),
        "animated_media" => "[GIF]".to_string(),
        "clip" => {
            let caption = item.pointer("/clip/clip/caption/text")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            format!("[Reel] {}", caption).trim().to_string()
        }
        "story_share" => {
            let title = item.pointer("/story_share/title")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            format!("[Shared story] {}", title).trim().to_string()
        }
        "link" => {
            let text = item.pointer("/link/text")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            let url = item.pointer("/link/link_context/link_url")
                .and_then(|u| u.as_str())
                .unwrap_or("");
            format!("{} {}", text, url).trim().to_string()
        }
        "like" => "\u{2764}\u{fe0f}".to_string(),
        "reel_share" => {
            let text = item.pointer("/reel_share/text")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            format!("[Reel share] {}", text).trim().to_string()
        }
        "xma" => "[Shared content]".to_string(),
        _ => {
            // Fallback: try text field, otherwise show type
            item.get("text")
                .and_then(|t| t.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("[{}]", item_type))
        }
    }
}

fn extract_attachments(item: &serde_json::Value, item_type: &str, _item_id: &str) -> Vec<serde_json::Value> {
    let mut attachments = Vec::new();

    let (media, name) = match item_type {
        "media" | "media_share" => {
            let m = item.get("media").or_else(|| item.get("media_share"));
            (m, "image.jpg")
        }
        "raven_media" => {
            let m = item.pointer("/visual_media/media");
            (m, "disappearing.jpg")
        }
        "clip" => {
            let m = item.pointer("/clip/clip");
            (m, "reel_thumbnail.jpg")
        }
        "story_share" => {
            let m = item.pointer("/story_share/media");
            (m, "story.jpg")
        }
        "animated_media" => {
            // GIFs use a different structure
            let url = item.pointer("/animated_media/images/fixed_height/url")
                .and_then(|u| u.as_str());
            if let Some(url) = url {
                attachments.push(serde_json::json!({
                    "url": url,
                    "content_type": "image/gif",
                    "name": "animation.gif",
                }));
            }
            return attachments;
        }
        "xma" => {
            // Shared content preview
            if let Some(xma_arr) = item.get("xma").and_then(|x| x.as_array()) {
                if let Some(first) = xma_arr.first() {
                    let url = first.pointer("/preview_url_info/url")
                        .or_else(|| first.get("header_icon_url"))
                        .and_then(|u| u.as_str());
                    if let Some(url) = url {
                        attachments.push(serde_json::json!({
                            "url": url,
                            "content_type": "image/jpeg",
                            "name": "shared.jpg",
                        }));
                    }
                }
            }
            return attachments;
        }
        _ => return attachments,
    };

    if let Some(media) = media {
        // Pick best quality candidate
        if let Some(candidates) = media.pointer("/image_versions2/candidates").and_then(|c| c.as_array()) {
            let best = candidates.iter()
                .max_by_key(|c| {
                    let w = c.get("width").and_then(|v| v.as_u64()).unwrap_or(0);
                    let h = c.get("height").and_then(|v| v.as_u64()).unwrap_or(0);
                    w * h
                });
            if let Some(best) = best {
                if let Some(url) = best.get("url").and_then(|u| u.as_str()) {
                    let content_type = match media.get("media_type").and_then(|v| v.as_u64()) {
                        Some(2) => "video/mp4",
                        _ => "image/jpeg",
                    };
                    attachments.push(serde_json::json!({
                        "url": url,
                        "content_type": content_type,
                        "name": name,
                    }));
                }
            }
        }
    }

    attachments
}

fn home_dir() -> PathBuf {
    std::env::var("HOME").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("."))
}
