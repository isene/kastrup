use super::MessageData;
use std::collections::HashSet;
use std::path::PathBuf;

/// Sync messages from WeeChat log files.
/// Reads `~/.weechat/logs/`, filters buffers by glob pattern,
/// and returns the last N lines per buffer as messages.
pub fn sync_weechat(config: &serde_json::Value, known_ids: &HashSet<String>) -> Vec<MessageData> {
    let buffer_filter = config.get("buffer_filter")
        .and_then(|v| v.as_str())
        .unwrap_or("*");
    let lines_per_buffer = config.get("lines_per_buffer")
        .and_then(|v| v.as_u64())
        .unwrap_or(50) as usize;

    let log_dir = home_dir().join(".weechat/logs");
    if !log_dir.is_dir() { return Vec::new(); }

    let filter_patterns: Vec<&str> = buffer_filter.split(',').map(|s| s.trim()).collect();
    let mut messages = Vec::new();

    let Ok(entries) = std::fs::read_dir(&log_dir) else { return messages };
    for entry in entries.flatten() {
        let filename = entry.file_name().to_string_lossy().to_string();
        if !filename.ends_with(".weechatlog") { continue; }

        let buffer_name = filename.trim_end_matches(".weechatlog");

        // Skip system buffers
        if buffer_name == "core.weechat" { continue; }
        if buffer_name.starts_with("relay.")
            || buffer_name.starts_with("fset.")
            || buffer_name.starts_with("script.")
            || buffer_name.starts_with("irc.server.") { continue; }

        // Check against filter (simple glob: trailing * matches prefix)
        if !matches_filter(buffer_name, &filter_patterns) { continue; }

        let path = entry.path();
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let platform = detect_platform(buffer_name);
        let is_dm = !buffer_name.contains(".#");
        let short_name = buffer_name.rsplit('.').next().unwrap_or(buffer_name);
        let channel_name = format_channel_name(buffer_name, short_name, &platform);
        let label = platform_label(&platform);

        // Take last N lines
        let log_lines: Vec<&str> = content.lines().collect();
        let start = log_lines.len().saturating_sub(lines_per_buffer);

        for line in &log_lines[start..] {
            let parts: Vec<&str> = line.splitn(3, '\t').collect();
            if parts.len() < 3 { continue; }

            let date_str = parts[0].trim();
            let nick_raw = parts[1].trim();
            let message = parts[2];

            // Skip system messages (joins, parts, quits, mode changes)
            if is_system_nick(nick_raw) { continue; }

            let nick = strip_weechat_colors(nick_raw);
            if nick.is_empty() { continue; }

            let timestamp = match parse_weechat_date(date_str) {
                Some(t) if t > 0 => t,
                _ => continue,
            };

            // Match Heathrow format: weechat_{md5hex}
            let hash_input = format!("{}_{}_{}_{}", buffer_name, timestamp, nick, &message[..message.len().min(81)]);
            let ext_id = format!("weechat_{}", md5_hex(&hash_input));

            if known_ids.contains(&ext_id) { continue; }

            let subject_preview = message[..message.len().min(200)].replace('\n', " ");

            messages.push(MessageData {
                external_id: ext_id,
                sender: nick.clone(),
                sender_name: Some(nick.clone()),
                recipients: channel_name.clone(),
                cc: None,
                subject: Some(subject_preview),
                content: message.to_string(),
                html_content: None,
                timestamp,
                labels: vec![label.to_string()],
                attachments: Vec::new(),
                metadata: serde_json::json!({
                    "buffer": buffer_name,
                    "channel_name": channel_name,
                    "nick": nick,
                    "is_dm": is_dm,
                    "platform": platform,
                }),
                folder: Some(channel_name.clone()),
                thread_id: Some(buffer_name.to_string()),
            });
        }
    }

    messages
}

/// Simple glob matching: "irc.*" matches "irc.libera.#rust", "*" matches all.
fn matches_filter(name: &str, patterns: &[&str]) -> bool {
    patterns.iter().any(|p| {
        let p = p.trim();
        if p == "*" { return true; }
        if let Some(prefix) = p.strip_suffix('*') {
            name.starts_with(prefix)
        } else {
            name == p
        }
    })
}

fn detect_platform(buffer_name: &str) -> String {
    if buffer_name.starts_with("python.slack.") { "slack".into() }
    else if buffer_name.starts_with("irc.") { "irc".into() }
    else { "weechat".into() }
}

fn format_channel_name(buffer_name: &str, short_name: &str, platform: &str) -> String {
    match platform {
        "irc" => {
            let parts: Vec<&str> = buffer_name.split('.').collect();
            let net = parts.get(1).unwrap_or(&"irc");
            format!("{}/{}", net, short_name)
        }
        "slack" => {
            let parts: Vec<&str> = buffer_name.split('.').collect();
            let ws = parts.get(2).unwrap_or(&"slack");
            let chan = if parts.len() > 3 { parts[3..].join(".") } else { short_name.to_string() };
            format!("{}/{}", ws, chan)
        }
        _ => short_name.to_string(),
    }
}

fn platform_label(platform: &str) -> &str {
    match platform {
        "slack" => "Slack",
        "irc" => "IRC",
        _ => "WeeChat",
    }
}

fn is_system_nick(nick: &str) -> bool {
    matches!(nick, "-->" | "<--" | "--" | "*" | " *" | "")
        || nick.starts_with("-->")
}

/// Strip WeeChat color codes from text.
/// Format: \x19 followed by type char + digits, \x1C resets.
fn strip_weechat_colors(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x19' {
            // Skip color sequence: type char + optional ~ or @ + digits
            if let Some(&next) = chars.peek() {
                if next == '\x1c' {
                    chars.next(); // reset sequence
                } else {
                    chars.next(); // type char (F, B, *, _, /, |, etc.)
                    // Skip optional ~ or @
                    if let Some(&d) = chars.peek() {
                        if d == '~' || d == '@' { chars.next(); }
                    }
                    // Skip digits
                    while let Some(&d) = chars.peek() {
                        if d.is_ascii_digit() { chars.next(); } else { break; }
                    }
                }
            }
            continue;
        }
        // Skip other control characters
        if ch < '\x20' && ch != '\t' && ch != '\n' { continue; }
        result.push(ch);
    }
    result.trim().to_string()
}

/// Parse WeeChat log date: "2026-04-06 10:15:32"
fn parse_weechat_date(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.split(&[' ', '-', ':'][..]).collect();
    if parts.len() < 6 { return None; }
    let y: i64 = parts[0].parse().ok()?;
    let mo: i64 = parts[1].parse().ok()?;
    let d: i64 = parts[2].parse().ok()?;
    let h: i64 = parts[3].parse().ok()?;
    let mi: i64 = parts[4].parse().ok()?;
    let sec: i64 = parts[5].parse().ok()?;

    // Howard Hinnant civil_from_days algorithm
    let mut year = y;
    let mut month = mo;
    if month <= 2 { year -= 1; month += 12; }
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let doy = (153 * (month - 3) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(days * 86400 + h * 3600 + mi * 60 + sec)
}

/// Simple MD5 hex digest (djb2-based, matching Heathrow's Digest::MD5).
/// Since we lack an MD5 crate, use a good hash with low collision rate.
fn md5_hex(s: &str) -> String {
    // Use FNV-1a 128-bit folded to produce a 32-char hex string
    let mut h1: u64 = 0xcbf29ce484222325;
    let mut h2: u64 = 0x9e3779b97f4a7c15;
    for b in s.bytes() {
        h1 ^= b as u64;
        h1 = h1.wrapping_mul(0x100000001b3);
        h2 ^= b as u64;
        h2 = h2.wrapping_mul(0x00000100000001b3);
    }
    format!("{:016x}{:016x}", h1, h2)
}

fn home_dir() -> PathBuf {
    std::env::var("HOME").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("."))
}
