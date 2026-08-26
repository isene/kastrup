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
                // Email/maildir: one section per conversation, keyed on the
                // subject with every Re: / Sv: / Fwd: stripped.
                //
                // NOT on thread_id. The maildir importer stores the message's
                // OWN Message-Id there, so keying on it gave every mail a
                // thread of one and threads never appeared in a mail view.
                // Replies are still indented under their parent afterwards,
                // by build_thread_order walking In-Reply-To.
                let subj = msg.subject.as_deref().unwrap_or("");
                let clean = crate::database::normalise_subject(subj);
                let display = if clean.is_empty() { "(no subject)".to_string() } else { clean.clone() };
                let thread_key = format!("subj_{}", clean);
                if let Some(&idx) = thread_map.get(&thread_key) {
                    sections[idx].messages.push(i);
                    if !msg.read { sections[idx].unread_count += 1; }
                } else {
                    let idx = sections.len();
                    thread_map.insert(thread_key, idx);
                    sections.push(Section {
                        section_type: "thread".to_string(),
                        name: display.clone(),
                        display_name: display,
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

pub fn organize_by_folder(
    messages: &[Message],
    sort_inverted: bool,
    pinned_order: &[String],
) -> Vec<Section> {
    let mut folder_map: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, msg) in messages.iter().enumerate() {
        let folder = msg.folder.as_deref().unwrap_or("INBOX").to_string();
        folder_map.entry(folder).or_default().push(i);
    }
    let mut sections: Vec<Section> = folder_map.into_iter().map(|(folder, indices)| {
        let unread = indices.iter().filter(|&&i| !messages[i].read).count();
        let display_name = pretty_folder_name(&folder);
        // Derive the section's source_type from its most-recent message so
        // the "source" sort groups conversations by platform and section
        // headers can be coloured/iconed per source (whatsapp, sms, slack…).
        let source_type = indices.iter()
            .max_by_key(|&&i| messages[i].timestamp)
            .map(|&i| messages[i].source_type.clone())
            .unwrap_or_else(|| "folder".to_string());
        Section {
            section_type: "channel".to_string(),
            name: folder,
            display_name,
            source_type,
            messages: indices,
            unread_count: unread,
        }
    }).collect();
    // Two-tier sort:
    //   1. Pinned channels (in the order given by pinned_order)
    //   2. Everything else, by latest-message timestamp (descending)
    // The user's `section_order` persists their hand-pinned channels;
    // newly-active unpinned channels still float to just below them.
    let pin_rank: HashMap<&str, usize> = pinned_order.iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();
    sections.sort_by(|a, b| {
        let ra = pin_rank.get(a.name.as_str()).copied();
        let rb = pin_rank.get(b.name.as_str()).copied();
        match (ra, rb) {
            (Some(ia), Some(ib)) => ia.cmp(&ib),
            (Some(_),  None)     => std::cmp::Ordering::Less,
            (None,     Some(_))  => std::cmp::Ordering::Greater,
            (None,     None)     => {
                let la = a.messages.iter().map(|&i| messages[i].timestamp).max().unwrap_or(0);
                let lb = b.messages.iter().map(|&i| messages[i].timestamp).max().unwrap_or(0);
                lb.cmp(&la)
            }
        }
    });
    if sort_inverted { sections.reverse(); }
    for section in &mut sections {
        section.messages.sort_by(|&a, &b| messages[b].timestamp.cmp(&messages[a].timestamp));
    }
    sections
}

/// Public re-export of `pretty_folder_name` so callers outside
/// `organize_by_folder` (e.g. main's merge of empty subscribed
/// buffers) can produce the same display string.
pub fn pretty_folder_name_public(folder: &str) -> String {
    pretty_folder_name(folder)
}

/// Strip the leading transport word (`python.`, `irc.`) from a
/// weechat-relay folder so the section header reads
/// `slack.example.&team` instead of `python.slack.example.&team`.
/// We keep the workspace segment so the renderer can dim it; the
/// DB folder stays untouched so view filters still match.
fn pretty_folder_name(folder: &str) -> String {
    for prefix in ["python.", "irc.server.", "irc."] {
        if let Some(rest) = folder.strip_prefix(prefix) {
            return rest.to_string();
        }
    }
    folder.to_string()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;

    fn mail(id: i64, subject: &str, thread_id: Option<&str>, ts: i64) -> Message {
        Message {
            id,
            subject: Some(subject.into()),
            thread_id: thread_id.map(String::from),
            timestamp: ts,
            source_type: "maildir".into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_conversation_is_one_section() {
        // The third mail carries its own Message-Id in thread_id, the way
        // the current maildir importer writes it. That used to split it off
        // into a thread of one.
        let msgs = vec![
            mail(1, "Dualog Insight", None, 100),
            mail(2, "RE: Dualog Insight", None, 200),
            mail(3, "Sv: RE: Dualog Insight", Some("its-own-message-id"), 300),
            mail(4, "Fwd: Dualog Insight", None, 400),
            mail(5, "Something else", None, 500),
        ];
        let sections = organize_messages(&msgs, "timestamp", false);
        let thread: Vec<_> = sections.iter()
            .filter(|s| s.display_name == "Dualog Insight").collect();
        assert_eq!(thread.len(), 1, "one section for the conversation");
        assert_eq!(thread[0].messages.len(), 4, "all four mails in it");
        assert_eq!(sections.len(), 2, "the unrelated mail stays on its own");

        // The section name is what the collapse map keys on, so it has to
        // be the stripped subject, not whichever mail happened to be first.
        assert_eq!(thread[0].name, "Dualog Insight");
        println!("sections: {:?}", sections.iter()
            .map(|s| (s.display_name.clone(), s.messages.len())).collect::<Vec<_>>());
    }

    #[test]
    fn a_missing_subject_still_groups() {
        let msgs = vec![mail(1, "", None, 100), mail(2, "Re:", None, 200)];
        let sections = organize_messages(&msgs, "timestamp", false);
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].display_name, "(no subject)");
    }
}
