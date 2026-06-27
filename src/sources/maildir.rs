use super::MessageData;
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

pub fn sync_maildir(maildir_path: &str, known_ids: &HashSet<String>, last_sync: i64) -> Vec<MessageData> {
    let root = Path::new(maildir_path);
    if !root.is_dir() { return Vec::new(); }

    let mut messages = Vec::new();

    // Build a folder-agnostic dedup set of bare maildir basenames.
    // The known_ids set holds full ext_ids like
    //   `maildir_INBOX_1715407823.M123P12.host:2,RS`
    // plus the folder-prefixed base without flags
    //   `maildir_INBOX_1715407823.M123P12.host`
    // Neither catches the case where the user MOVES the file from
    // INBOX to a different folder (Save / Archive): the folder
    // component flips and the prefixed form no longer matches.
    // Strip both the leading `maildir_<folder>_` AND the trailing
    // `:2,FLAGS` so we end up with just the maildir basename
    // (`<epoch>.<unique>.<host>`) and store that. Then a moved file
    // is recognised as known regardless of which folder it lives in
    // now.
    let known_bases: HashSet<String> = known_ids.iter()
        .filter_map(|k| {
            let no_flags = k.split(":2,").next().unwrap_or(k);
            let no_prefix = no_flags.strip_prefix("maildir_").unwrap_or(no_flags);
            // Folder names can contain underscores, so scan from the
            // right for the FIRST `<10+ digits>.` run (maildir epoch)
            // and take everything from there.
            extract_maildir_basename(no_prefix).map(str::to_string)
        })
        .collect();

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
                // Folder-move dedup: same physical message in a
                // different maildir folder. The bare basename is the
                // stable identity; if we've seen it elsewhere, this
                // is the post-Save / post-Archive view of the same
                // message — skip.
                if let Some(b) = extract_maildir_basename(base) {
                    if known_bases.contains(b) { continue; }
                }

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
    // Read as bytes and decode lossily, NOT read_to_string: a mail with
    // a non-UTF-8 body (charset=windows-1252 / latin-1 with 8bit transfer
    // encoding — common in marketing mail) is invalid UTF-8, so
    // read_to_string returns Err and the whole message was silently
    // dropped — never ingested, while gmail-idle still counted the new/
    // file (asmite vs kastrup unread-count mismatch). Headers are ASCII
    // so they parse fine; lossy only touches stray body bytes.
    let content = String::from_utf8_lossy(&std::fs::read(path).ok()?).into_owned();

    // Parse headers (everything before first blank line)
    let mut from = String::new();
    let mut from_name = None;
    let mut to = String::new();
    let mut cc = None;
    let mut bcc = None;
    let mut subject = None;
    let mut date_str = String::new();
    let mut message_id = None;
    let mut in_reply_to = None;
    let mut references = None;
    let mut content_type = String::new();
    let mut content_transfer_encoding = String::new();

    let mut in_headers = true;
    let mut body_lines = Vec::new();
    let mut current_header = String::new();

    for raw_line in content.lines() {
        let line = raw_line.trim_end_matches('\r');
        if in_headers {
            if line.is_empty() {
                // Process last header
                process_header(&current_header, &mut from, &mut from_name, &mut to, &mut cc, &mut bcc,
                    &mut subject, &mut date_str, &mut message_id, &mut in_reply_to,
                    &mut references, &mut content_type, &mut content_transfer_encoding);
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
                    process_header(&current_header, &mut from, &mut from_name, &mut to, &mut cc, &mut bcc,
                        &mut subject, &mut date_str, &mut message_id, &mut in_reply_to,
                        &mut references, &mut content_type, &mut content_transfer_encoding);
                }
                current_header = line.to_string();
            }
        } else {
            body_lines.push(line);
        }
    }

    let body = body_lines.join("\n");

    // Single-part text/html message: decode the body at parse time
    // and stash it as html_content so the renderer and the
    // open-in-scroll path get a ready-to-display document without
    // having to undo Content-Transfer-Encoding on every render.
    // Multipart messages are left alone — extract_mime_html /
    // extract_mime_text handle those lazily.
    let lower_ct = content_type.to_ascii_lowercase();
    let html_content = if lower_ct.starts_with("text/html") && !lower_ct.contains("multipart/") {
        let lower_cte = content_transfer_encoding.to_ascii_lowercase();
        let bytes = if lower_cte.contains("quoted-printable") {
            decode_qp_body_bytes(&body)
        } else if lower_cte.contains("base64") {
            base64_decode(body.trim()).unwrap_or_else(|| body.as_bytes().to_vec())
        } else {
            body.as_bytes().to_vec()
        };
        // Lossy UTF-8: the few stray non-UTF-8 bytes in random
        // newsletters shouldn't block the whole render. Strict
        // decode would drop those messages back to the plain-text
        // fallback in best_html_for_message.
        Some(String::from_utf8_lossy(&bytes).into_owned())
    } else {
        None
    };

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
        bcc,
        subject,
        content: body,
        html_content,
        timestamp,
        labels: vec![folder.to_string()],
        attachments: Vec::new(),
        metadata,
        folder: Some(folder.to_string()),
        thread_id: message_id,
    })
}

/// Case-insensitive `Header-Name:` strip. Discards the `"NAME:"` /
/// `"NAME: "` prefix and returns the trimmed value. Mail clients are
/// allowed to spell headers in any case (Outlook in particular sends
/// `CC:` and `BCC:` in all-caps), so a case-sensitive `strip_prefix`
/// silently drops valid headers and the DB row ends up with NULL.
fn strip_header<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let colon = line.find(':')?;
    if line[..colon].eq_ignore_ascii_case(name) {
        // Skip the colon and any single space after it; leave the
        // rest as-is so the existing callers can `.trim()`.
        let mut rest = &line[colon + 1..];
        if rest.starts_with(' ') { rest = &rest[1..]; }
        Some(rest)
    } else { None }
}

#[allow(clippy::too_many_arguments)]
fn process_header(header: &str, from: &mut String, from_name: &mut Option<String>,
    to: &mut String, cc: &mut Option<String>, bcc: &mut Option<String>,
    subject: &mut Option<String>,
    date: &mut String, message_id: &mut Option<String>,
    in_reply_to: &mut Option<String>, references: &mut Option<String>,
    content_type: &mut String, content_transfer_encoding: &mut String)
{
    if let Some(val) = strip_header(header, "From") {
        let val = val.trim();
        if let Some(lt) = val.find('<') {
            *from_name = Some(decode_rfc2047(val[..lt].trim().trim_matches('"')));
            *from = val[lt+1..].trim_end_matches('>').to_string();
        } else {
            *from = val.to_string();
        }
    } else if let Some(val) = strip_header(header, "To") {
        *to = val.trim().to_string();
    } else if let Some(val) = strip_header(header, "Cc") {
        *cc = Some(val.trim().to_string());
    } else if let Some(val) = strip_header(header, "Bcc") {
        *bcc = Some(val.trim().to_string());
    } else if let Some(val) = strip_header(header, "Subject") {
        *subject = Some(decode_rfc2047(val.trim()));
    } else if let Some(val) = strip_header(header, "Date") {
        *date = val.trim().to_string();
    } else if let Some(val) = strip_header(header, "Message-ID")
        .or_else(|| strip_header(header, "Message-Id"))
    {
        *message_id = Some(val.trim().trim_matches(&['<', '>'][..]).to_string());
    } else if let Some(val) = strip_header(header, "In-Reply-To") {
        *in_reply_to = Some(val.trim().trim_matches(&['<', '>'][..]).to_string());
    } else if let Some(val) = strip_header(header, "References") {
        *references = Some(val.trim().to_string());
    } else if let Some(val) = strip_header(header, "Content-Type") {
        *content_type = val.trim().to_string();
    } else if let Some(val) = strip_header(header, "Content-Transfer-Encoding") {
        *content_transfer_encoding = val.trim().to_string();
    }
}

/// Decode a Content-Transfer-Encoding: quoted-printable BODY (not a
/// header encoded-word). Different from `decode_qp_bytes` in two
/// ways: `=\r\n` / `=\n` is a soft line break (consume both, emit
/// nothing), and `_` stays a literal underscore (the header
/// convention of mapping `_` → space does not apply to bodies).
fn decode_qp_body_bytes(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'=' && i + 1 < b.len() {
            if b[i + 1] == b'\n' { i += 2; continue; }
            if b[i + 1] == b'\r' {
                i += 2;
                if i < b.len() && b[i] == b'\n' { i += 1; }
                continue;
            }
            if i + 2 < b.len()
                && b[i + 1].is_ascii_hexdigit()
                && b[i + 2].is_ascii_hexdigit()
            {
                let pair = std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("");
                if let Ok(byte) = u8::from_str_radix(pair, 16) {
                    out.push(byte);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(b[i]);
        i += 1;
    }
    out
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

/// Extract the stable maildir basename from a string that may be
/// `<folder>_<basename>` (where folder can itself contain `_`), just
/// `<basename>`, or anything else. Maildir basenames per DJB's spec
/// begin with `<unix-epoch-seconds>` (10+ digits today, ≤2001 had
/// fewer) followed by `.`. We scan from the LEFT for the FIRST
/// digit-run-followed-by-`.` and treat everything from there to the
/// end (minus any `:2,FLAGS` suffix) as the basename. Returns None
/// when no such anchor is found (unusual — degraded gracefully by
/// the caller's fallback dedup checks).
fn extract_maildir_basename(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    let n = bytes.len();
    let mut i = 0;
    while i < n {
        // Find a digit run.
        if bytes[i].is_ascii_digit() {
            let start = i;
            while i < n && bytes[i].is_ascii_digit() { i += 1; }
            // 10 digits = today's epoch width. Accept 9-12 to be tolerant.
            let run_len = i - start;
            if (9..=12).contains(&run_len) && i < n && bytes[i] == b'.' {
                // Strip optional trailing `:2,FLAGS`.
                let tail = s[start..].split(":2,").next().unwrap_or(&s[start..]);
                return Some(tail);
            }
        } else {
            i += 1;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basename_extracted_from_prefixed_id() {
        let s = "maildir_INBOX_1715407823.M123P12.host:2,RS";
        assert_eq!(extract_maildir_basename(s), Some("1715407823.M123P12.host"));
    }

    #[test]
    fn basename_extracted_when_folder_has_underscores() {
        let s = "maildir_Project.Archive_1715407823.M123P12.host:2,S";
        assert_eq!(extract_maildir_basename(s), Some("1715407823.M123P12.host"));
        let s = "maildir_some_dotted_folder_1715407823.M123P12.host";
        assert_eq!(extract_maildir_basename(s), Some("1715407823.M123P12.host"));
    }

    #[test]
    fn basename_unchanged_when_no_prefix() {
        let s = "1715407823.M123P12.host";
        assert_eq!(extract_maildir_basename(s), Some("1715407823.M123P12.host"));
    }

    #[test]
    fn basename_none_when_no_epoch_anchor() {
        let s = "not-a-maildir-name";
        assert_eq!(extract_maildir_basename(s), None);
    }

    #[test]
    fn non_utf8_body_still_parses() {
        // windows-1252 / latin-1 8bit body: 0x92 (right single quote),
        // 0x97 (em dash), 0xa0 (nbsp) are invalid UTF-8. read_to_string
        // would error and drop the whole message; from_utf8_lossy keeps
        // it. ASCII headers must still parse correctly.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"From: \"BMW UK\" <bmwuk@service.bmw.com>\r\n");
        bytes.extend_from_slice(b"Subject: Your BMW is in need of attention\r\n");
        bytes.extend_from_slice(b"Message-ID: <abc123@service.bmw.com>\r\n");
        bytes.extend_from_slice(b"Date: Sat, 27 Jun 2026 15:41:42 +0100\r\n");
        bytes.extend_from_slice(b"\r\n");
        bytes.extend_from_slice(b"Hello\x92 world \x97 done\xa0now");

        let dir = std::env::temp_dir().join("kastrup_w1252_test");
        let _ = std::fs::create_dir_all(&dir);
        let fp = dir.join("1782571311.test_1.host:2,");
        std::fs::write(&fp, &bytes).unwrap();

        let msg = parse_maildir_file(&fp, "Geir", "1782571311.test_1.host:2,");
        let _ = std::fs::remove_file(&fp);

        let msg = msg.expect("non-UTF-8 mail must still parse (not be dropped)");
        assert_eq!(msg.subject.as_deref(), Some("Your BMW is in need of attention"));
        assert!(msg.sender.contains("bmwuk@service.bmw.com"));
    }
}
