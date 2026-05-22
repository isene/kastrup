//! Unix domain socket server for editor-side `@nick` / `#channel`
//! completion. Editors (scribe, vim) connect when the user hits
//! `<tab>` mid-`@…` or mid-`#…`, ask kastrup for matches, and insert
//! the chosen one.
//!
//! Protocol is a single request line, single response, then close.
//! Request grammar (case-insensitive verb):
//!
//!   NICKS [<substr>]            → every observed nick, prefix-first
//!   NICKS_IN <folder> [<substr>]→ nicks for one folder only, then
//!                                 fall back to all
//!   CHANNELS [<substr>]         → subscribed-buffer short names
//!
//! Response: one match per line, sorted with prefix matches first.
//! Empty response = no matches. EOF after the last line.
//!
//! Battery rule: the server is purely event-driven — a blocking
//! `accept()` parks the thread until an editor connects, and each
//! served request closes the socket. No polling, no timers.

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::HashMap;

use crate::sources::weechat_relay::{NickLists, SubscribedBuffers};

fn socket_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_default();
    PathBuf::from(home).join(".kastrup").join("completion.sock")
}

/// Bind the completion socket and start the accept loop. Idempotent
/// for the caller — silently returns if the socket can't be created
/// (e.g. permission denied, parent dir missing). On bind, stale socket
/// files from previous runs are removed first.
pub fn start_server(nick_lists: NickLists, buffers: SubscribedBuffers) {
    let path = socket_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(&path);
    let listener = match UnixListener::bind(&path) {
        Ok(l) => l,
        Err(_) => return,
    };
    // chmod 600 — only the owning user should see live nick lists.
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(&path,
        std::fs::Permissions::from_mode(0o600));

    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue; };
            let nl = nick_lists.clone();
            let bf = buffers.clone();
            // Per-connection thread: the work is small (single line
            // I/O, two HashMap reads) but isolating it means a slow
            // editor can't stall a second editor's completion.
            thread::spawn(move || {
                let _ = serve_one(stream, &nl, &bf);
            });
        }
    });
}

fn serve_one(
    stream: UnixStream,
    nick_lists: &Arc<Mutex<HashMap<String, BTreeSet<String>>>>,
    buffers: &SubscribedBuffers,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let response = handle_request(line.trim(), nick_lists, buffers);
    writer.write_all(response.as_bytes())?;
    Ok(())
}

fn handle_request(
    req: &str,
    nick_lists: &Arc<Mutex<HashMap<String, BTreeSet<String>>>>,
    buffers: &SubscribedBuffers,
) -> String {
    let mut parts = req.splitn(2, ' ');
    let verb = parts.next().unwrap_or("").to_ascii_uppercase();
    let rest = parts.next().unwrap_or("").trim();

    match verb.as_str() {
        "NICKS" => {
            let lists = nick_lists.lock().unwrap().clone();
            let mut all: BTreeSet<String> = BTreeSet::new();
            for set in lists.values() {
                for n in set { all.insert(n.clone()); }
            }
            format_matches(all.into_iter().collect(), rest)
        }
        "NICKS_IN" => {
            let mut sub = rest.splitn(2, ' ');
            let folder = sub.next().unwrap_or("").trim();
            let substr = sub.next().unwrap_or("").trim();
            let lists = nick_lists.lock().unwrap().clone();
            let mut all: Vec<String> = Vec::new();
            let mut seen: BTreeSet<String> = BTreeSet::new();
            if let Some(set) = lists.get(folder) {
                for n in set {
                    if seen.insert(n.clone()) { all.push(n.clone()); }
                }
            }
            // Fall back to every other folder so the editor still
            // gets useful suggestions in a non-chat buffer.
            for (k, set) in &lists {
                if k == folder { continue; }
                for n in set {
                    if seen.insert(n.clone()) { all.push(n.clone()); }
                }
            }
            format_matches(all, substr)
        }
        "CHANNELS" => {
            let snap = buffers.lock().unwrap().clone();
            let mut all: Vec<String> = snap.into_iter()
                .map(|b| b.short_name).collect();
            all.sort();
            all.dedup();
            format_matches(all, rest)
        }
        _ => String::new(),
    }
}

/// Rank matches: exact case-insensitive equal first, then
/// prefix-matches, then substring matches. Empty substr → return the
/// candidate list as-is (caller-sorted). Output is newline-separated
/// with a trailing newline.
fn format_matches(candidates: Vec<String>, substr: &str) -> String {
    if substr.is_empty() {
        let mut out = candidates.join("\n");
        if !out.is_empty() { out.push('\n'); }
        return out;
    }
    let q = substr.to_ascii_lowercase();
    let mut exact: Vec<&String> = Vec::new();
    let mut prefix: Vec<&String> = Vec::new();
    let mut contains: Vec<&String> = Vec::new();
    for c in &candidates {
        let cl = c.to_ascii_lowercase();
        if cl == q { exact.push(c); }
        else if cl.starts_with(&q) { prefix.push(c); }
        else if cl.contains(&q) { contains.push(c); }
    }
    let mut out: Vec<&String> = Vec::new();
    out.extend(exact);
    out.extend(prefix);
    out.extend(contains);
    let mut s = out.into_iter().cloned().collect::<Vec<_>>().join("\n");
    if !s.is_empty() { s.push('\n'); }
    s
}
