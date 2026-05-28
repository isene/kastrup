use std::sync::{Arc, Mutex, Condvar};
use std::sync::mpsc;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use crate::database::Database;
use crate::sources;

pub enum PollerEvent {
    NewMessages(usize),
}

/// Tri-state shared between the main poller thread and any
/// thread that wants to wake or stop it. The poller's wait
/// loop blocks on a condvar that observes this state:
///
///   * `Idle`  — normal sleeping state.
///   * `Wake`  — somebody (inotify, an external trigger) wants
///               the poller to scan NOW. Consumed by the poller
///               on the next iteration.
///   * `Stop`  — terminal: poller (and helpers) exit.
#[derive(PartialEq, Eq, Clone, Copy)]
enum WakeState { Idle, Wake, Stop }

pub struct Poller {
    wake: Arc<(Mutex<WakeState>, Condvar)>,
    thread: Option<std::thread::JoinHandle<()>>,
    inotify_thread: Option<std::thread::JoinHandle<()>>,
}

impl Poller {
    pub fn start(db: Arc<Database>, tx: mpsc::Sender<PollerEvent>) -> Self {
        let wake = Arc::new((Mutex::new(WakeState::Idle), Condvar::new()));
        let wake_clone = wake.clone();
        let db_for_poller = db.clone();

        let thread = std::thread::spawn(move || {
            poller_loop(db_for_poller, tx, wake_clone);
        });

        // Inotify watcher on maildir new/ dirs (Linux only — inotify
        // is a Linux kernel API). Runs in its own thread because the
        // underlying read() syscall blocks until the kernel fires an
        // event; we don't want to interleave it with the poller's
        // scan + sleep cycle. Best-effort: if inotify init fails
        // (rare) or there are no maildir sources, we skip and fall
        // back to pure-poll behaviour. On macOS/BSD the watcher is
        // never spawned and the 10 s poll cadence is the only driver.
        #[cfg(target_os = "linux")]
        let inotify_thread = {
            let wake = wake.clone();
            let db = db.clone();
            std::thread::Builder::new()
                .name("kastrup-inotify".to_string())
                .spawn(move || inotify_watcher(db, wake))
                .ok()
        };
        #[cfg(not(target_os = "linux"))]
        let inotify_thread = {
            let _ = &db; // db only needed by the linux watcher
            None
        };

        Self { wake, thread: Some(thread), inotify_thread }
    }

    pub fn stop(&mut self) {
        let (lock, cvar) = &*self.wake;
        *lock.lock().unwrap() = WakeState::Stop;
        cvar.notify_all();
        // Don't join: the poller may be mid-sync with network
        // timeouts, and the inotify thread is blocked on a syscall
        // the kernel will tear down when the process exits. Both
        // are daemon threads.
        self.thread.take();
        self.inotify_thread.take();
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.wake;
        *lock.lock().unwrap() = WakeState::Stop;
        cvar.notify_all();
    }
}

fn poller_loop(
    db: Arc<Database>,
    tx: mpsc::Sender<PollerEvent>,
    wake: Arc<(Mutex<WakeState>, Condvar)>,
) {
    // Cache known_ids per source (loaded once, updated incrementally).
    // This HashSet grows for the process lifetime but eviction is only
    // worth building if VmRSS actually creeps up — the periodic log
    // below lets us confirm whether that ever matters in practice.
    let mut known_cache: HashMap<i64, HashSet<String>> = HashMap::new();
    log_process_memory("poller startup", &known_cache);
    let mut next_mem_log = std::time::Instant::now()
        + std::time::Duration::from_secs(3600);

    // True when this iteration was woken by inotify (a maildir file
    // actually appeared) rather than by the 10 s safety-net timeout.
    // A forced iteration scans maildir NOW, bypassing both the
    // poll-interval loop gate and sync_maildir's mtime gate, so a
    // delivery that lands within the poll-interval window can't get
    // orphaned (the bug: inotify wake skipped by the 5 s gate, then
    // the next timeout poll advances last_sync past the file's dir
    // mtime → sync_maildir mtime-skips it forever until the next
    // delivery bumps the dir). First iteration is unforced (normal
    // startup gating / last_sync from DB).
    let mut forced = false;

    loop {
        if std::time::Instant::now() >= next_mem_log {
            log_process_memory("poller hourly", &known_cache);
            next_mem_log += std::time::Duration::from_secs(3600);
            // Reclaim up to 1024 freelist pages (~4 MB) so deletes
            // and expired-message purges don't grow the DB file
            // without bound. Only effective once the DB has been
            // VACUUMed with auto_vacuum=incremental at least
            // once; on an auto_vacuum=NONE DB this is a no-op.
            // Runs hourly so it stays well under the radar.
            let conn = db.conn.lock().unwrap();
            let _ = conn.execute_batch("PRAGMA incremental_vacuum(1024);");
            drop(conn);
        }
        let sources_list = db.get_sources(true);
        let now = crate::database::now_secs();

        for source in &sources_list {
            let interval = source.poll_interval;
            let last_sync = source.last_sync.unwrap_or(0);
            let is_maildir = source.plugin_type == "maildir";
            // inotify-forced wakes bypass the poll-interval gate for
            // maildir — reacting immediately to a delivery is the
            // whole point. Other sources (and timeout polls) keep the
            // normal gate.
            if !(forced && is_maildir) && now - last_sync < interval { continue; }

            // Get or initialize cached known_ids (only load from DB on first access)
            let known = known_cache.entry(source.id).or_insert_with(|| {
                db.get_known_external_ids(source.id)
            });

            // Sync: filesystem/network scan happens WITHOUT holding DB lock
            let messages = match source.plugin_type.as_str() {
                "maildir" => {
                    let path = source.config.get("maildir_path")
                        .or_else(|| source.config.get("path"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("~/Maildir");
                    let expanded = path.replace("~/",
                        &format!("{}/", std::env::var("HOME").unwrap_or_default()));
                    // Forced (inotify) scan passes last_sync=0 to defeat
                    // sync_maildir's per-dir mtime gate — we KNOW a file
                    // just landed, so scan every dir and let known_ids
                    // dedup down to the genuinely-new file(s). This full
                    // walk only fires on a real delivery event, so the
                    // tens-of-ms cost is paid only when there's something
                    // to find; idle 10 s timeout polls still use the
                    // cheap mtime gate.
                    let eff_last_sync = if forced { 0 } else { last_sync };
                    sources::maildir::sync_maildir(&expanded, known, eff_last_sync)
                }
                "rss" => {
                    let feeds = source.config.get("feeds")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    sources::rss::sync_rss(&feeds, known)
                }
                "weechat" => sources::weechat::sync_weechat(&source.config, known),
                "messenger" => sources::messenger::sync_messenger(&source.config, known),
                "instagram" => sources::instagram::sync_instagram(&source.config, known),
                "discord" => sources::discord::sync_discord(&source.config, known),
                "slack" => sources::slack::sync_slack(&source.config, known),
                // weechat-relay is driven by its own push
                // supervisor (see weechat_relay::spawn_supervisor)
                // — the poller path stays a no-op so we don't
                // double-fetch over the network.
                "weechat-relay" => Vec::new(),
                _ => Vec::new(),
            };

            let count = messages.len();
            if count > 0 {
                crate::log::info(&format!("Poller: {} new messages from source {}", count, source.name));
                // Add new external_ids to cache (exact + base without flags)
                for msg in &messages {
                    known.insert(msg.external_id.clone());
                    // Also cache the base (stripped of :2,FLAGS) for flag-change dedup
                    let base = msg.external_id.split(":2,").next().unwrap_or(&msg.external_id);
                    known.insert(base.to_string());
                }
                // Brief DB lock for batch insert only
                db.insert_messages_batch(source.id, &messages);
            }

            db.update_source_sync_time(source.id);

            if count > 0 {
                let _ = tx.send(PollerEvent::NewMessages(count));
            }
        }

        // Park until 10s elapse, inotify nudges Wake, or stop is
        // signaled. wait_timeout_while atomically drops the lock and
        // parks on the condvar — notify from inotify_watcher or
        // stop() wakes us instantly with no missed-wakeup race. The
        // 10 s upper bound is a safety net for the no-event case
        // (RSS / weechat / etc.) so polled sources still tick.
        let (lock, cvar) = &*wake;
        let guard = lock.lock().unwrap();
        let (mut guard, _) = cvar.wait_timeout_while(
            guard,
            std::time::Duration::from_secs(10),
            |state| *state == WakeState::Idle,
        ).unwrap();
        forced = match *guard {
            WakeState::Stop => return,
            WakeState::Wake => { *guard = WakeState::Idle; true }
            WakeState::Idle => false, // 10 s timeout — normal gated poll
        };
    }
}

/// Background thread that watches every `new/` subdirectory under
/// every maildir source's root and pokes the poller (via the shared
/// WakeState) the moment a file appears. Idle cost: zero (single
/// blocking `read()` on the inotify fd; the kernel suspends the
/// thread until an event fires). Per-event cost: one mutex acquire
/// + one condvar notify, microseconds.
///
/// Best-effort: if inotify init fails, or there are no maildir
/// sources, or the filesystem walk finds zero `new/` dirs, the
/// thread quietly exits and the poller's 10-second fallback tick
/// keeps things working.
#[cfg(target_os = "linux")]
fn inotify_watcher(db: Arc<Database>, wake: Arc<(Mutex<WakeState>, Condvar)>) {
    use inotify::{Inotify, WatchMask};

    let mut inotify = match Inotify::init() {
        Ok(i) => i,
        Err(e) => {
            crate::log::info(&format!("inotify init failed: {} (polling-only fallback)", e));
            return;
        }
    };

    let sources_list = db.get_sources(true);
    let home = std::env::var("HOME").unwrap_or_default();
    let mut watched = 0usize;
    for source in &sources_list {
        if source.plugin_type != "maildir" { continue; }
        let path = source.config.get("maildir_path")
            .or_else(|| source.config.get("path"))
            .and_then(|v| v.as_str())
            .unwrap_or("~/Maildir");
        let root = PathBuf::from(path.replace("~/", &format!("{}/", home)));
        // Top-level INBOX new/ (where gmail-idle drops mail) — the
        // common hot path; watch it even if the subfolder enumeration
        // below somehow fails.
        let top = root.join("new");
        if top.is_dir() {
            if inotify.watches()
                .add(&top, WatchMask::CREATE | WatchMask::MOVED_TO)
                .is_ok()
            {
                watched += 1;
            }
        }
        // Maildir++ subfolders: `~/Maildir/.<Folder>/new/`. Each
        // user-visible folder has its own new/. One readdir at the
        // root + one stat per `.<Folder>/new/` candidate; sub-MB
        // total on a 30 GB Maildir with ~hundreds of subfolders.
        if let Ok(entries) = std::fs::read_dir(&root) {
            for entry in entries.flatten() {
                let p = entry.path();
                let name = match p.file_name().and_then(|n| n.to_str()) {
                    Some(n) => n,
                    None => continue,
                };
                // Maildir++ subfolders start with `.` and are not `.`
                // or `..`. Avoid the metadata cousins (`courierimap*`,
                // `subscriptions`, etc.) since they don't have `new/`.
                if !name.starts_with('.') || name == "." || name == ".." {
                    continue;
                }
                if !p.is_dir() { continue; }
                let sub = p.join("new");
                if sub.is_dir() {
                    if inotify.watches()
                        .add(&sub, WatchMask::CREATE | WatchMask::MOVED_TO)
                        .is_ok()
                    {
                        watched += 1;
                    }
                }
            }
        }
    }

    if watched == 0 {
        crate::log::info("inotify: no maildir new/ dirs to watch (polling-only)");
        return;
    }
    crate::log::info(&format!("inotify: watching {} maildir new/ dir(s)", watched));

    // Event loop. Drain whatever the kernel hands us per wake, then
    // promote WakeState::Idle → Wake exactly once per batch. Multiple
    // events coalesce naturally because the poller resets Wake → Idle
    // only when it actually runs — the second event in a burst sees
    // Wake still pending and is a no-op.
    let mut buf = [0u8; 4096];
    loop {
        match inotify.read_events_blocking(&mut buf) {
            Ok(events) => {
                // Consume the iterator so the inotify queue clears;
                // event contents don't matter, just the fact of
                // arrival. (Without this consumer the kernel buffer
                // fills and subsequent CREATE events get dropped.)
                let _ = events.count();
                // Nudge the poller.
                let (lock, cvar) = &*wake;
                let mut g = lock.lock().unwrap();
                if *g == WakeState::Idle {
                    *g = WakeState::Wake;
                    cvar.notify_one();
                }
                if *g == WakeState::Stop { return; }
            }
            Err(e) => {
                crate::log::info(&format!("inotify read error: {} (exiting watcher)", e));
                return;
            }
        }
    }
}

/// Log process VmRSS and the total number of entries in the poller's
/// known_ids cache so we can judge whether the cache ever becomes large
/// enough to justify an eviction policy.
fn log_process_memory(tag: &str, known_cache: &HashMap<i64, HashSet<String>>) {
    let vm_rss_kb = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()))
        });
    let total_known: usize = known_cache.values().map(|s| s.len()).sum();
    let rss_str = match vm_rss_kb {
        Some(kb) => format!("{} KB", kb),
        None => "unknown".to_string(),
    };
    crate::log::info(&format!(
        "{}: VmRSS={}, known_cache={} entries across {} sources",
        tag, rss_str, total_known, known_cache.len()
    ));
}
