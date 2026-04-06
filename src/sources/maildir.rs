use super::MessageData;
use std::path::{Path, PathBuf};
use std::collections::HashSet;

pub fn sync_maildir(maildir_path: &str, known_ids: &HashSet<String>) -> Vec<MessageData> {
    let root = Path::new(maildir_path);
    if !root.is_dir() { return Vec::new(); }

    let mut messages = Vec::new();

    // Discover folders
    let mut folders: Vec<(String, PathBuf)> = vec![("INBOX".to_string(), root.to_path_buf())];
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.starts_with('.') || name == "." || name == ".." { continue; }
            let path = entry.path();
            if !path.is_dir() { continue; }
            if !path.join("cur").is_dir() && !path.join("new").is_dir() { continue; }
            folders.push((name[1..].to_string(), path));
        }
    }

    for (folder_name, folder_path) in &folders {
        for subdir in &["cur", "new"] {
            let dir = folder_path.join(subdir);
            if !dir.is_dir() { continue; }
            let Ok(entries) = std::fs::read_dir(&dir) else { continue };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() { continue; }
                let filename = path.file_name().and_then(|f| f.to_str()).unwrap_or("").to_string();

                // Check if already known (use bare filename to match Heathrow format)
                if known_ids.contains(&filename) { continue; }
                // Also check Heathrow's maildir_ prefixed format
                let prefixed = format!("maildir_{}_{}", folder_name, &filename);
                if known_ids.contains(&prefixed) { continue; }

                // Parse email headers
                if let Some(msg) = parse_maildir_file(&path, folder_name, &filename) {
                    messages.push(msg);
                }
            }
        }
    }

    messages
}

fn parse_maildir_file(path: &Path, folder: &str, filename: &str) -> Option<MessageData> {
    let content = std::fs::read_to_string(path).ok()?;

    // Parse headers (everything before first blank line)
    let mut from = String::new();
    let mut from_name = None;
    let mut to = String::new();
    let mut cc = None;
    let mut subject = None;
    let mut date_str = String::new();
    let mut message_id = None;
    let mut in_reply_to = None;
    let mut references = None;
    let mut content_type = String::new();

    let mut in_headers = true;
    let mut body_lines = Vec::new();
    let mut current_header = String::new();

    for line in content.lines() {
        if in_headers {
            if line.is_empty() {
                // Process last header
                process_header(&current_header, &mut from, &mut from_name, &mut to, &mut cc,
                    &mut subject, &mut date_str, &mut message_id, &mut in_reply_to,
                    &mut references, &mut content_type);
                in_headers = false;
                continue;
            }
            if line.starts_with(' ') || line.starts_with('\t') {
                // Continuation of previous header
                current_header.push(' ');
                current_header.push_str(line.trim());
            } else {
                // New header, process previous
                if !current_header.is_empty() {
                    process_header(&current_header, &mut from, &mut from_name, &mut to, &mut cc,
                        &mut subject, &mut date_str, &mut message_id, &mut in_reply_to,
                        &mut references, &mut content_type);
                }
                current_header = line.to_string();
            }
        } else {
            body_lines.push(line);
        }
    }

    let body = body_lines.join("\n");

    // Parse flags from filename (format: unique:2,FLAGS)
    let flags = filename.rsplit(':').next().unwrap_or("");
    let _seen = flags.contains('S');
    let _flagged = flags.contains('F');
    let _replied = flags.contains('R');

    // Parse timestamp from Date header
    let timestamp = parse_date(&date_str).unwrap_or_else(|| {
        // Fallback: use file mtime
        std::fs::metadata(path).ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    });

    // Use Heathrow's format: maildir_{folder}_{filename}
    let ext_id = format!("maildir_{}_{}", folder, filename);

    let mut metadata = serde_json::json!({
        "maildir_file": path.to_string_lossy(),
        "maildir_folder": folder,
    });
    if let Some(ref mid) = message_id { metadata["message_id"] = serde_json::json!(mid); }
    if let Some(ref irt) = in_reply_to { metadata["in_reply_to"] = serde_json::json!(irt); }
    if let Some(ref refs) = references { metadata["references"] = serde_json::json!(refs); }

    Some(MessageData {
        external_id: ext_id,
        sender: from,
        sender_name: from_name,
        recipients: to,
        cc,
        subject,
        content: body,
        html_content: None,
        timestamp,
        labels: vec![folder.to_string()],
        attachments: Vec::new(),
        metadata,
        folder: Some(folder.to_string()),
        thread_id: message_id,
    })
}

#[allow(clippy::too_many_arguments)]
fn process_header(header: &str, from: &mut String, from_name: &mut Option<String>,
    to: &mut String, cc: &mut Option<String>, subject: &mut Option<String>,
    date: &mut String, message_id: &mut Option<String>,
    in_reply_to: &mut Option<String>, references: &mut Option<String>,
    _content_type: &mut String)
{
    if let Some(val) = header.strip_prefix("From: ").or_else(|| header.strip_prefix("from: ")) {
        let val = val.trim();
        // Parse "Name <email>" format
        if let Some(lt) = val.find('<') {
            *from_name = Some(val[..lt].trim().trim_matches('"').to_string());
            *from = val[lt+1..].trim_end_matches('>').to_string();
        } else {
            *from = val.to_string();
        }
    } else if let Some(val) = header.strip_prefix("To: ").or_else(|| header.strip_prefix("to: ")) {
        *to = val.trim().to_string();
    } else if let Some(val) = header.strip_prefix("Cc: ").or_else(|| header.strip_prefix("cc: ")) {
        *cc = Some(val.trim().to_string());
    } else if let Some(val) = header.strip_prefix("Subject: ").or_else(|| header.strip_prefix("subject: ")) {
        *subject = Some(val.trim().to_string());
    } else if let Some(val) = header.strip_prefix("Date: ").or_else(|| header.strip_prefix("date: ")) {
        *date = val.trim().to_string();
    } else if let Some(val) = header.strip_prefix("Message-ID: ").or_else(|| header.strip_prefix("Message-Id: ")).or_else(|| header.strip_prefix("message-id: ")) {
        *message_id = Some(val.trim().trim_matches(&['<', '>'][..]).to_string());
    } else if let Some(val) = header.strip_prefix("In-Reply-To: ").or_else(|| header.strip_prefix("in-reply-to: ")) {
        *in_reply_to = Some(val.trim().trim_matches(&['<', '>'][..]).to_string());
    } else if let Some(val) = header.strip_prefix("References: ").or_else(|| header.strip_prefix("references: ")) {
        *references = Some(val.trim().to_string());
    } else if let Some(val) = header.strip_prefix("Content-Type: ").or_else(|| header.strip_prefix("content-type: ")) {
        *_content_type = val.trim().to_string();
    }
}

fn parse_date(date_str: &str) -> Option<i64> {
    let s = date_str.trim();
    if s.is_empty() { return None; }

    // Parse RFC 2822: "Thu, 3 Apr 2026 09:15:00 +0200"
    // Also handles: "3 Apr 2026 09:15:00 +0200" (no day name)

    // Strip day name if present
    let s = if let Some(pos) = s.find(',') { s[pos+1..].trim() } else { s };

    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 4 { return None; }

    let day: i64 = parts[0].parse().ok()?;
    let month = match parts[1].to_lowercase().as_str() {
        "jan" => 1, "feb" => 2, "mar" => 3, "apr" => 4,
        "may" => 5, "jun" => 6, "jul" => 7, "aug" => 8,
        "sep" => 9, "oct" => 10, "nov" => 11, "dec" => 12,
        _ => return None,
    };
    let year: i64 = parts[2].parse().ok()?;

    let time_parts: Vec<&str> = parts[3].split(':').collect();
    let hour: i64 = time_parts.get(0)?.parse().ok()?;
    let min: i64 = time_parts.get(1)?.parse().ok()?;
    let sec: i64 = time_parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

    // Parse timezone offset if present
    let tz_offset: i64 = if let Some(tz) = parts.get(4) {
        let tz = tz.trim();
        if tz.len() >= 4 && (tz.starts_with('+') || tz.starts_with('-')) {
            let sign: i64 = if tz.starts_with('-') { -1 } else { 1 };
            let h: i64 = tz[1..3].parse().unwrap_or(0);
            let m: i64 = tz[3..5].parse().unwrap_or(0);
            sign * (h * 3600 + m * 60)
        } else {
            0
        }
    } else {
        0
    };

    // Convert to unix timestamp using Howard Hinnant's algorithm
    let mut y = year;
    let mut m = month as i64;
    if m <= 2 { y -= 1; m += 12; }
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m - 3) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;

    Some(days * 86400 + hour * 3600 + min * 60 + sec - tz_offset)
}
