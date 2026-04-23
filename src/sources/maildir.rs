use super::MessageData;
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

pub fn sync_maildir(maildir_path: &str, known_ids: &HashSet<String>, last_sync: i64) -> Vec<MessageData> {
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

    // Build a SystemTime threshold from last_sync (with 2s slack for FS mtime granularity).
    // last_sync == 0 means "never synced" — fall through and scan everything.
    let threshold: Option<SystemTime> = if last_sync > 0 {
        UNIX_EPOCH.checked_add(Duration::from_secs(last_sync.saturating_sub(2) as u64))
    } else {
        None
    };

    for (folder_name, folder_path) in &folders {
        for subdir in &["cur", "new"] {
            let dir = folder_path.join(subdir);
            if !dir.is_dir() { continue; }
            // mtime gate: skip subdirs that haven't changed since last_sync.
            // Maildir delivery writes to new/ and moves to cur/, both of which bump
            // the directory mtime, so we catch all new/modified messages this way.
            if let Some(thr) = threshold {
                if let Ok(meta) = std::fs::metadata(&dir) {
                    if let Ok(mt) = meta.modified() {
                        if mt <= thr { continue; }
                    }
                }
            }
            let Ok(entries) = std::fs::read_dir(&dir) else { continue };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() { continue; }
                let filename = path.file_name().and_then(|f| f.to_str()).unwrap_or("").to_string();

                // Check if already known: exact match, prefixed, or base (ignoring flags)
                if known_ids.contains(&filename) { continue; }
                let prefixed = format!("maildir_{}_{}", folder_name, &filename);
                if known_ids.contains(&prefixed) { continue; }
                // Strip flags (:2,XYZ) and check base with common flag variants
                let base = filename.split(":2,").next().unwrap_or(&filename);
                let base_pre = format!("maildir_{}_{}", folder_name, base);
                if known_ids.contains(&format!("{}:2,", base))
                    || known_ids.contains(&format!("{}:2,S", base))
                    || known_ids.contains(&format!("{}:2,", base_pre))
                    || known_ids.contains(&format!("{}:2,S", base_pre))
                    || known_ids.contains(base)
                    || known_ids.contains(&base_pre)
                { continue; }

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

    for raw_line in content.lines() {
        let line = raw_line.trim_end_matches('\r');
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
            *from_name = Some(decode_rfc2047(val[..lt].trim().trim_matches('"')));
            *from = val[lt+1..].trim_end_matches('>').to_string();
        } else {
            *from = val.to_string();
        }
    } else if let Some(val) = header.strip_prefix("To: ").or_else(|| header.strip_prefix("to: ")) {
        *to = val.trim().to_string();
    } else if let Some(val) = header.strip_prefix("Cc: ").or_else(|| header.strip_prefix("cc: ")) {
        *cc = Some(val.trim().to_string());
    } else if let Some(val) = header.strip_prefix("Subject: ").or_else(|| header.strip_prefix("subject: ")) {
        *subject = Some(decode_rfc2047(val.trim()));
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

/// Decode RFC 2047 encoded-words: =?charset?encoding?text?=
pub fn decode_rfc2047(s: &str) -> String {
    if !s.contains("=?") { return s.to_string(); }
    let mut result = String::new();
    let mut rest = s;
    while let Some(start) = rest.find("=?") {
        result.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        // Format: charset?encoding?encoded_text?=
        // Find first ? (end of charset), second ? (end of encoding), then ?= (terminator)
        let mut qmarks = Vec::new();
        for (i, b) in after.bytes().enumerate() {
            if b == b'?' { qmarks.push(i); }
            if qmarks.len() >= 3 { break; }
        }
        // Need at least 2 '?' for charset?encoding?, then find ?= after the encoded text
        if qmarks.len() >= 2 {
            let charset_end = qmarks[0];
            let enc_end = qmarks[1];
            let _charset = &after[..charset_end];
            let encoding = &after[charset_end + 1..enc_end];
            let text_start = enc_end + 1;
            // Find ?= after the encoded text
            if let Some(term) = after[text_start..].find("?=") {
                let encoded = &after[text_start..text_start + term];
                let decoded_bytes = match encoding.to_lowercase().as_str() {
                    "b" => base64_decode(encoded),
                    "q" => Some(decode_qp_bytes(encoded)),
                    _ => None,
                };
                if let Some(bytes) = decoded_bytes {
                    let text = String::from_utf8(bytes.clone())
                        .unwrap_or_else(|_| bytes.iter().map(|&b| b as char).collect());
                    result.push_str(&text);
                } else {
                    result.push_str(&rest[start..start + 2 + text_start + term + 2]);
                }
                rest = &after[text_start + term + 2..];
                // Skip whitespace between adjacent encoded words
                if rest.starts_with(' ') || rest.starts_with("\r\n ") || rest.starts_with("\n ") {
                    let trimmed = rest.trim_start();
                    if trimmed.starts_with("=?") { rest = trimmed; }
                }
            } else {
                result.push_str("=?");
                rest = after;
            }
        } else {
            result.push_str("=?");
            rest = after;
        }
    }
    result.push_str(rest);
    result
}

pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let table: [u8; 128] = {
        let mut t = [255u8; 128];
        for (i, &c) in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".iter().enumerate() {
            t[c as usize] = i as u8;
        }
        t
    };
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0;
    for &b in s.as_bytes() {
        if b == b'=' || b == b'\n' || b == b'\r' || b == b' ' { continue; }
        if b >= 128 || table[b as usize] == 255 { continue; }
        buf = (buf << 6) | table[b as usize] as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

fn decode_qp_bytes(s: &str) -> Vec<u8> {
    let mut result = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'_' {
            result.push(b' ');
            i += 1;
        } else if bytes[i] == b'=' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(std::str::from_utf8(&bytes[i+1..i+3]).unwrap_or(""), 16) {
                result.push(b);
                i += 3;
            } else {
                result.push(bytes[i]);
                i += 1;
            }
        } else {
            result.push(bytes[i]);
            i += 1;
        }
    }
    result
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
