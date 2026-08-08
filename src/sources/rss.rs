use super::MessageData;
use std::collections::HashSet;

// The parsing is fe2o3-feed's, shared with the phone app. What stays
// here is the fetch: curl, because it already handles the redirects and
// TLS quirks that feeds in the wild throw.

pub fn sync_rss(feeds: &[serde_json::Value], known_ids: &HashSet<String>) -> Vec<MessageData> {
    let mut messages = Vec::new();

    for feed_config in feeds {
        let url = match feed_config.get("url").and_then(|v| v.as_str()) {
            Some(u) => u,
            None => continue,
        };
        let feed_title = feed_config.get("title").and_then(|v| v.as_str()).unwrap_or(url);

        // Fetch feed via curl (avoids HTTP library issues)
        let output = std::process::Command::new("curl")
            .args(["-s", "-L", "--max-time", "10", url])
            .output();
        let Ok(output) = output else { continue };
        if !output.status.success() { continue; }
        let xml = String::from_utf8_lossy(&output.stdout);

        for item in feed::parse(&xml, feed_title, url) {
            if known_ids.contains(&item.id) { continue; }
            messages.push(MessageData {
                external_id: item.id,
                sender: item.author,
                sender_name: Some(feed_title.to_string()),
                recipients: feed_title.to_string(),
                cc: None,
                bcc: None,
                subject: Some(item.title).filter(|t| !t.is_empty()),
                content: item.text,
                html_content: Some(item.html),
                timestamp: item.published,
                labels: vec![feed_title.to_string()],
                attachments: Vec::new(),
                metadata: serde_json::json!({
                    "feed_title": feed_title,
                    "feed_url": url,
                    "link": item.link,
                }),
                folder: Some(feed_title.to_string()),
                thread_id: None,
            });
        }
    }

    messages
}

