use super::MessageData;

pub fn sync_rss(feeds: &[serde_json::Value]) -> Vec<MessageData> {
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

        let items = parse_feed_items(&xml, feed_title, url);
        messages.extend(items);
    }

    messages
}

fn parse_feed_items(xml: &str, feed_title: &str, feed_url: &str) -> Vec<MessageData> {
    let mut messages = Vec::new();

    // Detect Atom vs RSS
    let is_atom = xml.contains("<feed") && xml.contains("xmlns=\"http://www.w3.org/2005/Atom\"");

    if is_atom {
        for entry in xml.split("<entry").skip(1) {
            let title = extract_tag(entry, "title");
            let link = extract_attr(entry, "link", "href")
                .or_else(|| extract_tag(entry, "link"));
            let content = extract_tag(entry, "content")
                .or_else(|| extract_tag(entry, "summary"))
                .unwrap_or_default();
            let author = extract_tag(entry, "name")
                .or_else(|| extract_tag(entry, "author"));
            let updated = extract_tag(entry, "updated")
                .or_else(|| extract_tag(entry, "published"));
            let id = extract_tag(entry, "id");

            let ext_id = format!("rss_{}",
                simple_hash(id.as_deref().or(link.as_deref()).unwrap_or(title.as_deref().unwrap_or(""))));

            let timestamp = updated.as_deref().and_then(parse_rss_date).unwrap_or(0);

            messages.push(MessageData {
                external_id: ext_id,
                sender: author.unwrap_or_else(|| feed_title.to_string()),
                sender_name: Some(feed_title.to_string()),
                recipients: feed_title.to_string(),
                cc: None,
                subject: title,
                content: strip_html_simple(&content),
                html_content: Some(content),
                timestamp,
                labels: vec![feed_title.to_string()],
                attachments: Vec::new(),
                metadata: serde_json::json!({
                    "feed_title": feed_title,
                    "feed_url": feed_url,
                    "link": link,
                }),
                folder: Some(feed_title.to_string()),
                thread_id: None,
            });
        }
    } else {
        // Parse RSS items
        for item in xml.split("<item").skip(1) {
            let title = extract_tag(item, "title");
            let link = extract_tag(item, "link");
            let description = extract_tag(item, "description")
                .or_else(|| extract_tag(item, "content:encoded"));
            let author = extract_tag(item, "author")
                .or_else(|| extract_tag(item, "dc:creator"));
            let pub_date = extract_tag(item, "pubDate");
            let guid = extract_tag(item, "guid");

            let ext_id = format!("rss_{}",
                simple_hash(guid.as_deref().or(link.as_deref()).unwrap_or(title.as_deref().unwrap_or(""))));

            let timestamp = pub_date.as_deref().and_then(parse_rss_date).unwrap_or(0);
            let content = description.unwrap_or_default();

            messages.push(MessageData {
                external_id: ext_id,
                sender: author.unwrap_or_else(|| feed_title.to_string()),
                sender_name: Some(feed_title.to_string()),
                recipients: feed_title.to_string(),
                cc: None,
                subject: title,
                content: strip_html_simple(&content),
                html_content: Some(content),
                timestamp,
                labels: vec![feed_title.to_string()],
                attachments: Vec::new(),
                metadata: serde_json::json!({
                    "feed_title": feed_title,
                    "feed_url": feed_url,
                    "link": link,
                }),
                folder: Some(feed_title.to_string()),
                thread_id: None,
            });
        }
    }

    messages
}

fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)?;
    let after_open = xml[start..].find('>')?;
    let content_start = start + after_open + 1;
    let end = xml[content_start..].find(&close)?;
    let content = &xml[content_start..content_start + end];
    // Decode CDATA
    let content = if content.starts_with("<![CDATA[") && content.ends_with("]]>") {
        &content[9..content.len()-3]
    } else {
        content
    };
    Some(decode_entities(content.trim()))
}

fn extract_attr(xml: &str, tag: &str, attr: &str) -> Option<String> {
    let pattern = format!("<{}", tag);
    let start = xml.find(&pattern)?;
    let tag_end = xml[start..].find('>')?;
    let tag_content = &xml[start..start + tag_end];
    let attr_pattern = format!("{}=\"", attr);
    let attr_start = tag_content.find(&attr_pattern)?;
    let value_start = attr_start + attr_pattern.len();
    let value_end = tag_content[value_start..].find('"')?;
    Some(tag_content[value_start..value_start + value_end].to_string())
}

fn decode_entities(s: &str) -> String {
    s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
        .replace("&quot;", "\"").replace("&apos;", "'").replace("&#39;", "'")
}

fn strip_html_simple(html: &str) -> String {
    let mut result = String::new();
    let mut in_tag = false;
    for ch in html.chars() {
        if ch == '<' { in_tag = true; continue; }
        if ch == '>' { in_tag = false; continue; }
        if !in_tag { result.push(ch); }
    }
    decode_entities(&result)
}

fn parse_rss_date(date_str: &str) -> Option<i64> {
    let s = date_str.trim();
    if s.is_empty() { return None; }

    // Try RFC 2822: "Thu, 3 Apr 2026 09:15:00 +0200"
    if let Some(ts) = parse_rfc2822(s) { return Some(ts); }

    // Try ISO 8601 / Atom: "2026-04-03T09:15:00Z" or "2026-04-03T09:15:00+02:00"
    parse_iso8601(s)
}

fn parse_rfc2822(s: &str) -> Option<i64> {
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

fn parse_iso8601(s: &str) -> Option<i64> {
    // "2026-04-03T09:15:00Z" or "2026-04-03T09:15:00+02:00"
    let (datetime, tz_part) = if s.ends_with('Z') {
        (&s[..s.len()-1], "+00:00")
    } else if s.len() > 6 {
        let last6 = &s[s.len()-6..];
        if (last6.starts_with('+') || last6.starts_with('-')) && last6.contains(':') {
            (&s[..s.len()-6], last6)
        } else {
            (s, "+00:00")
        }
    } else {
        (s, "+00:00")
    };

    let date_time: Vec<&str> = datetime.split('T').collect();
    if date_time.len() < 2 { return None; }

    let date_parts: Vec<&str> = date_time[0].split('-').collect();
    if date_parts.len() < 3 { return None; }
    let year: i64 = date_parts[0].parse().ok()?;
    let month: i64 = date_parts[1].parse().ok()?;
    let day: i64 = date_parts[2].parse().ok()?;

    let time_parts: Vec<&str> = date_time[1].split(':').collect();
    let hour: i64 = time_parts.get(0)?.parse().ok()?;
    let min: i64 = time_parts.get(1)?.parse().ok()?;
    let sec: i64 = time_parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

    // Parse timezone offset
    let tz_offset: i64 = {
        let tz = tz_part.trim();
        if tz.len() >= 5 && (tz.starts_with('+') || tz.starts_with('-')) {
            let sign: i64 = if tz.starts_with('-') { -1 } else { 1 };
            let parts: Vec<&str> = tz[1..].split(':').collect();
            let h: i64 = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(0);
            let m: i64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            sign * (h * 3600 + m * 60)
        } else {
            0
        }
    };

    let mut y = year;
    let mut m = month;
    if m <= 2 { y -= 1; m += 12; }
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m - 3) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;

    Some(days * 86400 + hour * 3600 + min * 60 + sec - tz_offset)
}

fn simple_hash(s: &str) -> String {
    let mut h: u64 = 5381;
    for b in s.bytes() { h = h.wrapping_mul(33).wrapping_add(b as u64); }
    format!("{:016x}", h)
}
