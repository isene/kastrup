//! Hand new rows to an outside indexer, one small POST per poll cycle.
//!
//! The indexer (Corporate Intelligence, or anything with the same push
//! route) digests on its own schedule; kastrup only tells it what has
//! arrived. Mail is left out: the indexer reads the Maildir itself.
//!
//! Cost when idle: nothing. The poller calls in only after a cycle that
//! inserted rows, and a cycle that inserted only mail sends nothing. A
//! watermark in the settings table says what has been sent, so a failed
//! POST is retried next cycle and nothing goes twice.
//!
//! One way only: the indexer answers on its own channels. Nothing it
//! sends comes back through kastrup.
//!
//! The indexer need not run. When nothing answers at its address and the
//! `push:` block names a `folder`, the batch goes there as a file and the
//! watermark moves as after a POST. The indexer reads the folder when it
//! next runs. An answer that refuses the batch, a wrong key or a server
//! error, never goes to the folder: the row waits and the log says why.

use std::sync::Arc;
use crate::database::Database;

/// The `push:` block of ~/.kastrup/config.yml. Absent block, no feeder.
#[derive(Clone, Debug)]
pub struct PushConfig {
    /// Base URL of the indexer, e.g. `http://localhost:8100`.
    pub url: String,
    /// The push connector's id; the route is `{url}/api/push/{connector}`.
    pub connector: String,
    /// File holding the connector's key, sent as `X-Push-Key`.
    pub key_file: String,
    /// Optional: a folder that takes each batch as a file while nothing
    /// answers at `url`. It must exist.
    pub folder: Option<String>,
}

const WATERMARK: &str = "push_sent_up_to";
const BATCH: usize = 200;
/// The most one batch may weigh; the indexer refuses a bigger file.
const MAX_BYTES: usize = 25 * 1024 * 1024;

/// One row as the push route wants it.
fn record(r: &crate::database::PushRow) -> serde_json::Value {
    let ext = format!("kastrup:{}", r.id);
    let author = match (&r.sender_name, r.sender.as_str()) {
        (Some(n), s) if !n.trim().is_empty() && n.trim() != s => format!("{} <{}>", n.trim(), s),
        (_, s) => s.to_string(),
    };
    let container = match &r.folder {
        Some(f) if !f.is_empty() => format!("{}/{}", r.source_name, f),
        _ => r.source_name.clone(),
    };
    serde_json::json!({
        "external_id": ext,
        "kind": "message",
        "container": container,
        "title": r.subject.clone().unwrap_or_default(),
        "author": author,
        "recipients": recipients(r),
        "body": r.body,
        "occurred_at": iso8601_utc(r.timestamp),
        "thread_id": r.thread_id.clone().unwrap_or_default(),
        "url": ext,
        "attributes": attributes(r),
    })
}

/// Who the message went to. The column holds a JSON array for every
/// chat source, so sending it as text made the indexer read the whole
/// `["Ada Lovelace"]` as one person's name. A real list goes as a list;
/// anything that is not JSON goes as the plain string it is.
fn recipients(r: &crate::database::PushRow) -> serde_json::Value {
    let raw = r.recipients.as_deref().unwrap_or("").trim();
    if raw.is_empty() { return serde_json::Value::String(String::new()); }
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(v @ serde_json::Value::Array(_)) => v,
        _ => serde_json::Value::String(raw.to_string()),
    }
}

/// What the row is, and the ids an answer would need: the conversation
/// for Workspace, the channel for Discord, the buffer for a relay. The
/// indexer replies on its own connectors, and these say where.
fn attributes(r: &crate::database::PushRow) -> serde_json::Value {
    let mut a = serde_json::json!({ "source": r.plugin_type, "kastrup_id": r.id });
    let m = |k: &str| r.metadata.get(k).and_then(|v| v.as_str()).filter(|v| !v.is_empty());
    if !r.external_id.is_empty() { a["message_id"] = r.external_id.clone().into(); }
    if let Some(v) = m("conversation_id") { a["conversation_id"] = v.into(); }
    if let Some(v) = m("discord_channel_id") { a["channel_id"] = v.into(); }
    if let Some(v) = m("thread_key") { a["thread_key"] = v.into(); }
    if r.plugin_type == "weechat-relay" {
        if let Some(f) = r.folder.as_deref().filter(|f| !f.is_empty()) { a["buffer"] = f.into(); }
    }
    a
}

/// `2026-09-03T14:05:09Z` from a unix timestamp.
fn iso8601_utc(ts: i64) -> String {
    let days = ts.div_euclid(86400);
    let (y, m, d) = crate::days_to_ymd(days);
    let t = ts.rem_euclid(86400);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, t / 3600, (t % 3600) / 60, t % 60)
}

/// The rows as one JSON array, cut short before it would pass `max`
/// bytes. Returns the text and how many rows it carries, at least one.
fn batch(rows: &[crate::database::PushRow], max: usize) -> (String, usize) {
    let mut out = String::from("[");
    let mut n = 0;
    for r in rows {
        let one = record(r).to_string();
        if n > 0 && out.len() + one.len() + 2 > max { break; }
        if n > 0 { out.push(','); }
        out.push_str(&one);
        n += 1;
    }
    out.push(']');
    (out, n)
}

/// True when nothing answered: no connection, or no reply in time. Any
/// answer is false, a 401 or a 500 too, so a wrong key never hides
/// behind the folder.
fn unreachable(e: &ureq::Error) -> bool {
    match e.kind() {
        ureq::ErrorKind::ConnectionFailed => true,
        ureq::ErrorKind::Io => std::error::Error::source(e)
            .and_then(|s| s.downcast_ref::<std::io::Error>())
            .is_some_and(|io| io.kind() == std::io::ErrorKind::TimedOut),
        _ => false,
    }
}

/// Write one batch to the folder as `<unix ms>-<first row id>.json`, so
/// the names sort oldest first. It is written under a `.part` name and
/// then renamed, so a reader never meets half a file. Only the user may
/// read it.
fn file(folder: &std::path::Path, stamp: u128, first_id: i64, body: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let name = format!("{}-{}.json", stamp, first_id);
    let part = folder.join(format!("{}.part", name));
    let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&part)?;
    f.write_all(body.as_bytes())?;
    std::fs::rename(&part, folder.join(name))
}

/// Hand one batch over: by POST, or as a file when nothing answers and
/// there is a folder. Ok(true) says it went to the folder. `filed` has
/// the stamp of the last file of this pass: once it is set the POST is
/// not tried again, so a long catch-up waits for one timeout, not one
/// per batch.
fn deliver(cfg: &PushConfig, key: &str, body: &str, first_id: i64, filed: &mut Option<u128>) -> Result<bool, String> {
    if filed.is_none() {
        let url = format!("{}/api/push/{}", cfg.url.trim_end_matches('/'), cfg.connector);
        let resp = sources::http_agent()
            .post(&url)
            .set("X-Push-Key", key)
            .set("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(5))
            .send_string(body);
        match resp {
            Ok(_) => return Ok(false),
            Err(e) if cfg.folder.is_some() && unreachable(&e) => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    let folder = cfg.folder.as_deref().unwrap_or_default();
    // The clock in ms, or one past the last name when it has not moved.
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis();
    let stamp = now.max(filed.map_or(0, |s| s + 1));
    file(std::path::Path::new(folder), stamp, first_id, body).map_err(|e| format!("folder {}: {}", folder, e))?;
    *filed = Some(stamp);
    Ok(true)
}

/// Send what arrived since the watermark. Returns (rows sent, how many
/// of them went to the folder, ms), or None when there was nothing to
/// send. Never raises: a batch that fails logs one line and leaves the
/// watermark where it was.
pub fn push_new(db: &Arc<Database>, cfg: &PushConfig) -> Option<(usize, usize, u128)> {
    let key = std::fs::read_to_string(&cfg.key_file).ok()?.trim().to_string();
    if key.is_empty() { return None; }
    // First run: start from now. What came before was loaded by hand, and
    // the indexer dedups on external_id in any case.
    let mark: i64 = match db.get_setting(WATERMARK).and_then(|v| v.parse().ok()) {
        Some(m) => m,
        None => {
            let top = db.max_message_id();
            db.set_setting(WATERMARK, &top.to_string());
            return None;
        }
    };
    let t0 = std::time::Instant::now();
    let (mut sent, mut in_folder) = (0usize, 0usize);
    let mut mark = mark;
    let mut filed = None;
    loop {
        let rows = db.rows_after(mark, BATCH);
        if rows.is_empty() { break; }
        let (body, n) = batch(&rows, MAX_BYTES);
        match deliver(cfg, &key, &body, rows[0].id, &mut filed) {
            Ok(to_folder) => {
                sent += n;
                if to_folder { in_folder += n; }
                mark = rows[n - 1].id;
                db.set_setting(WATERMARK, &mark.to_string());
                if n == rows.len() && n < BATCH { break; }
            }
            Err(e) => {
                crate::log::info(&format!("push: {} rows failed: {}", n, e));
                break;
            }
        }
    }
    if sent == 0 { return None; }
    let ms = t0.elapsed().as_millis();
    crate::log::info(&format!("push: {} rows, {} to the folder, {} ms", sent, in_folder, ms));
    Some((sent, in_folder, ms))
}

use crate::sources;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn row(id: i64, body: &str) -> crate::database::PushRow {
        crate::database::PushRow {
            id, external_id: String::new(), metadata: serde_json::Value::Null, sender: "ada".into(), sender_name: None,
            recipients: None, subject: None, body: body.into(), timestamp: 0, thread_id: None, folder: None,
            source_name: "chat".into(), plugin_type: "discord".into(),
        }
    }

    /// An empty folder of the test's own.
    fn scratch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("kastrup-feeder-{}-{}", std::process::id(), name));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn names(d: &std::path::Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(d).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
        v.sort();
        v
    }

    fn cfg(url: String, folder: Option<&std::path::Path>) -> PushConfig {
        PushConfig { url, connector: "c".into(), key_file: String::new(), folder: folder.map(|f| f.to_string_lossy().into_owned()) }
    }

    /// An address where nothing listens.
    fn dead() -> String {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", l.local_addr().unwrap())
    }

    /// A server that reads each request to its end and answers with one
    /// status line.
    fn server(status: &'static str) -> String {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        std::thread::spawn(move || {
            for s in l.incoming() {
                let Ok(mut s) = s else { break };
                let (mut buf, mut chunk) = (Vec::new(), [0u8; 4096]);
                loop {
                    let n = s.read(&mut chunk).unwrap_or(0);
                    if n == 0 { break; }
                    buf.extend_from_slice(&chunk[..n]);
                    let Some(h) = buf.windows(4).position(|w| w == b"\r\n\r\n") else { continue };
                    let head = String::from_utf8_lossy(&buf[..h]).to_ascii_lowercase();
                    let len = head.lines().find_map(|l| l.strip_prefix("content-length:")).and_then(|v| v.trim().parse::<usize>().ok()).unwrap_or(0);
                    if buf.len() >= h + 4 + len { break; }
                }
                let _ = write!(s, "HTTP/1.1 {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", status);
            }
        });
        url
    }

    #[test]
    fn nothing_listening_sends_the_batch_to_the_folder() {
        use std::os::unix::fs::PermissionsExt;
        let d = scratch("dead");
        let c = cfg(dead(), Some(&d));
        let mut filed = None;
        assert_eq!(deliver(&c, "key", "[1]", 7, &mut filed), Ok(true));
        assert_eq!(deliver(&c, "key", "[2]", 207, &mut filed), Ok(true));
        let got = names(&d);
        assert_eq!(got.len(), 2, "{:?}", got);
        // Oldest first by name, each a finished .json that only the user reads.
        assert!(got[0].ends_with("-7.json") && got[1].ends_with("-207.json"), "{:?}", got);
        assert_eq!(std::fs::read_to_string(d.join(&got[0])).unwrap(), "[1]");
        assert_eq!(std::fs::read_to_string(d.join(&got[1])).unwrap(), "[2]");
        assert_eq!(std::fs::metadata(d.join(&got[0])).unwrap().permissions().mode() & 0o777, 0o600);
        // With no folder named it fails as before, and writes nothing.
        let mut filed = None;
        assert!(deliver(&cfg(dead(), None), "key", "[3]", 9, &mut filed).is_err());
        assert_eq!((names(&d).len(), filed), (2, None));
        // A folder that is not there fails too, and the batch counts as unsent.
        let gone = d.join("gone");
        assert!(deliver(&cfg(dead(), Some(&gone)), "key", "[4]", 9, &mut filed).is_err());
        assert_eq!(filed, None);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn an_answer_never_goes_to_the_folder() {
        let d = scratch("answer");
        for status in ["401 Unauthorized", "500 Internal Server Error"] {
            let mut filed = None;
            assert!(deliver(&cfg(server(status), Some(&d)), "key", "[1]", 7, &mut filed).is_err(), "{}", status);
            assert_eq!(filed, None);
        }
        let mut filed = None;
        assert_eq!(deliver(&cfg(server("200 OK"), Some(&d)), "key", "[1]", 7, &mut filed), Ok(false));
        assert!(names(&d).is_empty(), "{:?}", names(&d));
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn no_reply_in_time_counts_as_nothing_answering() {
        // A server that takes the connection and never says a word.
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        std::thread::spawn(move || { let _held: Vec<_> = l.incoming().collect(); });
        let e = ureq::post(&url).timeout(std::time::Duration::from_millis(300)).send_string("[]").unwrap_err();
        assert!(unreachable(&e), "{}", e);
        let e = ureq::post(&server("401 Unauthorized")).send_string("[]").unwrap_err();
        assert!(!unreachable(&e), "{}", e);
    }

    #[test]
    fn a_batch_stops_at_the_size_limit() {
        let rows: Vec<_> = (1..=5).map(|i| row(i, &"x".repeat(1000))).collect();
        // Under the limit it is the array the POST always carried.
        let (all, n) = batch(&rows, MAX_BYTES);
        assert_eq!(n, 5);
        assert_eq!(all, serde_json::Value::Array(rows.iter().map(record).collect()).to_string());
        // A limit that fits two rows gives two, as a whole array.
        let one = record(&rows[0]).to_string().len();
        let (some, n) = batch(&rows, one * 2 + 10);
        assert_eq!(n, 2);
        assert!(some.len() <= one * 2 + 10);
        assert_eq!(serde_json::from_str::<serde_json::Value>(&some).unwrap().as_array().unwrap().len(), 2);
        // A single row over the limit still goes, alone.
        assert_eq!(batch(&rows, 10).1, 1);
    }
}
