use crate::message::Message;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Section {
    pub section_type: String,   // "channel", "dm_section", "thread"
    pub name: String,           // channel_id, sender, thread subject
    pub display_name: String,   // human-readable
    pub source_type: String,    // "discord", "slack", "maildir", etc.
    pub messages: Vec<usize>,   // indices into the original messages vec
    pub unread_count: usize,
}

pub fn organize_messages(
    messages: &[Message],
    sort_order: &str,
    sort_inverted: bool,
) -> Vec<Section> {
    let mut sections: Vec<Section> = Vec::new();
    let mut thread_map: HashMap<String, usize> = HashMap::new(); // thread_key -> section index
    let mut channel_map: HashMap<String, usize> = HashMap::new();
    let mut dm_messages: Vec<usize> = Vec::new();

    for (i, msg) in messages.iter().enumerate() {
        match msg.source_type.as_str() {
            "discord" | "slack" | "weechat" | "workspace" => {
                // Group by channel
                let is_dm = msg.metadata.get("is_dm").and_then(|v| v.as_bool()).unwrap_or(false)
                    || msg.recipients.contains("DM");
                if is_dm {
                    dm_messages.push(i);
                } else {
                    let channel = msg.metadata.get("channel_id")
                        .or_else(|| msg.metadata.get("channel_name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(&msg.recipients);
                    let key = format!("{}_{}", msg.source_type, channel);
                    if let Some(&idx) = channel_map.get(&key) {
                        sections[idx].messages.push(i);
                        if !msg.read { sections[idx].unread_count += 1; }
                    } else {
                        let display = msg.subject.as_deref()
                            .or_else(|| msg.metadata.get("channel_name").and_then(|v| v.as_str()))
                            .unwrap_or(channel).to_string();
                        let idx = sections.len();
                        channel_map.insert(key, idx);
                        sections.push(Section {
                            section_type: "channel".to_string(),
                            name: channel.to_string(),
                            display_name: display,
                            source_type: msg.source_type.clone(),
                            messages: vec![i],
                            unread_count: if msg.read { 0 } else { 1 },
                        });
                    }
                }
            }
            "messenger" | "instagram" | "whatsapp" | "telegram" => {
                // Group as DMs by sender
                let key = msg.sender.clone();
                if let Some(&idx) = thread_map.get(&key) {
                    sections[idx].messages.push(i);
                    if !msg.read { sections[idx].unread_count += 1; }
                } else {
                    let idx = sections.len();
                    thread_map.insert(key.clone(), idx);
                    let display = msg.sender_name.as_deref().unwrap_or(&msg.sender).to_string();
                    sections.push(Section {
                        section_type: "dm_section".to_string(),
                        name: key,
                        display_name: display,
                        source_type: msg.source_type.clone(),
                        messages: vec![i],
                        unread_count: if msg.read { 0 } else { 1 },
                    });
                }
            }
            "rss" => {
                // Group by feed (use folder or source subject pattern)
                let feed = msg.folder.as_deref()
                    .or_else(|| msg.metadata.get("feed_title").and_then(|v| v.as_str()))
                    .unwrap_or("RSS");
                let key = format!("rss_{}", feed);
                if let Some(&idx) = channel_map.get(&key) {
                    sections[idx].messages.push(i);
                    if !msg.read { sections[idx].unread_count += 1; }
                } else {
                    let idx = sections.len();
                    channel_map.insert(key, idx);
                    sections.push(Section {
                        section_type: "channel".to_string(),
                        name: feed.to_string(),
                        display_name: feed.to_string(),
                        source_type: "rss".to_string(),
                        messages: vec![i],
                        unread_count: if msg.read { 0 } else { 1 },
                    });
                }
            }
            _ => {
                // Email/maildir: group by thread_id or subject
                let thread_key = msg.thread_id.as_deref()
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| {
                        // Fall back to subject-based threading (strip Re:/Fwd:)
                        let subj = msg.subject.as_deref().unwrap_or("");
                        let clean = subj.trim_start_matches("Re: ").trim_start_matches("Fwd: ")
                            .trim_start_matches("RE: ").trim_start_matches("FW: ");
                        format!("subj_{}", clean)
                    });
                if let Some(&idx) = thread_map.get(&thread_key) {
                    sections[idx].messages.push(i);
                    if !msg.read { sections[idx].unread_count += 1; }
                } else {
                    let idx = sections.len();
                    thread_map.insert(thread_key, idx);
                    let subj = msg.subject.as_deref().unwrap_or("(no subject)").to_string();
                    sections.push(Section {
                        section_type: "thread".to_string(),
                        name: subj.clone(),
                        display_name: subj,
                        source_type: msg.source_type.clone(),
                        messages: vec![i],
                        unread_count: if msg.read { 0 } else { 1 },
                    });
                }
            }
        }
    }

    // Add DM section if any DMs
    if !dm_messages.is_empty() {
        let unread = dm_messages.iter().filter(|&&i| !messages[i].read).count();
        sections.push(Section {
            section_type: "dm_section".to_string(),
            name: "Direct Messages".to_string(),
            display_name: "Direct Messages".to_string(),
            source_type: "mixed".to_string(),
            messages: dm_messages,
            unread_count: unread,
        });
    }

    // Sort sections
    sort_sections(&mut sections, messages, sort_order);
    if sort_inverted { sections.reverse(); }

    // Sort messages within each section by timestamp (newest first)
    for section in &mut sections {
        section.messages.sort_by(|&a, &b| messages[b].timestamp.cmp(&messages[a].timestamp));
        if sort_inverted { section.messages.reverse(); }
    }

    sections
}

pub fn organize_by_folder(messages: &[Message], sort_inverted: bool) -> Vec<Section> {
    let mut folder_map: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, msg) in messages.iter().enumerate() {
        let folder = msg.folder.as_deref().unwrap_or("INBOX").to_string();
        folder_map.entry(folder).or_default().push(i);
    }
    let mut sections: Vec<Section> = folder_map.into_iter().map(|(folder, indices)| {
        let unread = indices.iter().filter(|&&i| !messages[i].read).count();
        Section {
            section_type: "channel".to_string(),
            name: folder.clone(),
            display_name: folder,
            source_type: "folder".to_string(),
            messages: indices,
            unread_count: unread,
        }
    }).collect();
    sections.sort_by(|a, b| a.name.cmp(&b.name));
    if sort_inverted { sections.reverse(); }
    for section in &mut sections {
        section.messages.sort_by(|&a, &b| messages[b].timestamp.cmp(&messages[a].timestamp));
    }
    sections
}

fn sort_sections(sections: &mut [Section], messages: &[Message], sort_order: &str) {
    match sort_order {
        "alphabetical" => sections.sort_by(|a, b| a.display_name.to_lowercase().cmp(&b.display_name.to_lowercase())),
        "unread" => sections.sort_by(|a, b| b.unread_count.cmp(&a.unread_count).then(a.display_name.cmp(&b.display_name))),
        "source" => sections.sort_by(|a, b| a.source_type.cmp(&b.source_type).then(a.display_name.cmp(&b.display_name))),
        _ => {
            // "latest" - sort by newest message in section
            sections.sort_by(|a, b| {
                let latest_a = a.messages.iter().map(|&i| messages[i].timestamp).max().unwrap_or(0);
                let latest_b = b.messages.iter().map(|&i| messages[i].timestamp).max().unwrap_or(0);
                latest_b.cmp(&latest_a)
            });
        }
    }
}
