use std::sync::{Arc, Mutex, Condvar};
use std::sync::mpsc;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use crate::database::Database;
use crate::sources;

pub enum PollerEvent {
    NewMessages(usize),
}

/// Hard wall-clock deadline for a single network source's sync. A healthy
/// incremental sync finishes in a few seconds; this is the backstop for a
/// connection that wedges after suspend/resume — half-open, where the TLS
/// layer can swallow ureq's own read/write timeout — so one stuck source
/// can't stall every source behind it in the sequential poll loop.
const NETWORK_SYNC_DEADLINE: std::time::Duration = std::time::Duration::from_secs(45);

/// Run `f` on a worker thread, waiting at most `deadline` for its result.
/// Returns `None` on timeout; the orphaned worker keeps running and exits on
/// its own once its blocking I/O finally errors (its result is discarded —
/// `tx.send` just fails into the dropped receiver).
fn run_with_timeout<F>(deadline: std::time::Duration, f: F) -> Option<Vec<sources::MessageData>>
where
    F: FnOnce() -> Vec<sources::MessageData> + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(deadline).ok()
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
    pub fn start(db: Arc<Database>, tx: mpsc::Sender<PollerEvent>, push: Option<crate::feeder::PushConfig>) -> Self {
        let wake = Arc::new((Mutex::new(WakeState::Idle), Condvar::new()));
        let wake_clone = wake.clone();
        let db_for_poller = db.clone();

        let thread = std::thread::spawn(move || {
            poller_loop(db_for_poller, tx, wake_clone, push);
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

    /// Force an immediate scan (same Wake the inotify watcher uses). Used by
    /// the main loop's resume watchdog: after a suspend, the poller's parked
    /// condvar timeout is on CLOCK_MONOTONIC (which doesn't count suspend) and
    /// can leave maildir un-synced long past resume — this kicks it awake.
    pub fn wake(&self) {
        let (lock, cvar) = &*self.wake;
        if let Ok(mut g) = lock.lock() {
            if *g == WakeState::Idle {
                *g = WakeState::Wake;
                cvar.notify_one();
            }
        }
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
    push: Option<crate::feeder::PushConfig>,
) {
    // Cache known_ids per source (loaded once, updated incrementally).
    // This HashSet grows for the process lifetime but eviction is only
    // worth building if VmRSS actually creeps up — the periodic log
    // below lets us confirm whether that ever matters in practice.
    let mut known_cache: HashMap<i64, HashSet<String>> = HashMap::new();
    // When each source was last polled, and when that was last written
    // down. The poll-interval gate only needs the number in memory; a
    // row rewritten every tick is a database write every tick, and the
    // UI's unread recount watches the database for changes. So an idle
    // poller kept the UI recounting a multi-GB table every five
    // seconds, for nothing. Persist on real news, or every 5 minutes so
    // a restart does not rescan the world.
    // Seeded from the database, not empty: an error written before the
    // last restart is still on screen, and only a cycle that knows
    // about it will clear it.
    let mut failing: HashMap<i64, String> = db.failing_sources().into_iter()
        .map(|(id, _, err, _)| (id, err))
        .collect();
    let mut polled_at: HashMap<i64, i64> = HashMap::new();
    let mut persisted_at: HashMap<i64, i64> = HashMap::new();
    const PERSIST_EVERY: i64 = 300;
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
    // delivery bumps the dir).
    //
    // The first iteration is ALSO forced (last_sync=0, no mtime gate):
    // a mail that was skipped or dropped on an earlier run (mtime-gate
    // race, or a parse failure now fixed) sits in new/ with a dir mtime
    // ≤ the stored last_sync, so a gated startup scan would never re-read
    // it. One full scan at boot re-examines every dir so such backlog
    // gets ingested (known_ids dedups everything already in the DB).
    // Runs on the background poller thread, off the UI paint path, so it
    // adds no startup latency — the heavier walk is paid once per launch.
    let mut forced = true;

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
        let mut sources_list = db.get_sources(true);
        // Sync local maildir before network sources each cycle. Maildir is
        // fast and never blocks on the network, so it always gets its turn
        // even when a (now timeout-bounded) network source is slow this
        // cycle — defence-in-depth against the "network hang freezes mail
        // sync" class of bug, on top of the per-request HTTP timeouts.
        sources_list.sort_by_key(|s| s.plugin_type != "maildir");
        let now = crate::database::now_secs();
        check_ws_bridge(&db, &sources_list, &mut failing);
        // Did this cycle insert anything the outside indexer wants?
        let mut new_non_mail = false;

        for source in &sources_list {
            let interval = source.poll_interval;
            let last_sync = *polled_at.get(&source.id)
                .unwrap_or(&source.last_sync.unwrap_or(0));
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

            let _ = sources::take_sync_error();
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
                // weechat-relay is driven by its own push supervisor (see
                // weechat_relay::spawn_supervisor) — the poller path stays a
                // no-op so we don't double-fetch over the network.
                "weechat-relay" => Vec::new(),
                // Every network source runs under a hard wall-clock deadline on
                // a worker thread. A connection that wedges after resume goes
                // half-open and the TLS layer can swallow ureq's own read/write
                // timeout, so without this one stuck source stalls every source
                // behind it (Slack hangs → Gateway never polled). On deadline we
                // abandon the worker and move on; it exits on its own once the
                // socket finally errors, and `last_sync` is left untouched so
                // the next cycle retries. (gateway drains local JSON and won't
                // wedge, but the wrapper is harmless there.)
                plugin @ ("rss" | "weechat" | "messenger" | "instagram"
                          | "gateway" | "discord" | "slack") => {
                    let cfg = source.config.clone();
                    let known_snapshot = known.clone();
                    let plugin = plugin.to_string();
                    match run_with_timeout(NETWORK_SYNC_DEADLINE, move || {
                        match plugin.as_str() {
                            "rss" => {
                                let feeds = cfg.get("feeds")
                                    .and_then(|v| v.as_array())
                                    .cloned()
                                    .unwrap_or_default();
                                sources::rss::sync_rss(&feeds, &known_snapshot)
                            }
                            "weechat" => sources::weechat::sync_weechat(&cfg, &known_snapshot),
                            "messenger" => sources::messenger::sync_messenger(&cfg, &known_snapshot),
                            "instagram" => sources::instagram::sync_instagram(&cfg, &known_snapshot),
                            "gateway" => sources::gateway::sync_gateway(&cfg, &known_snapshot),
                            "discord" => sources::discord::sync_discord(&cfg, &known_snapshot),
                            "slack" => sources::slack::sync_slack(&cfg, &known_snapshot),
                            _ => Vec::new(),
                        }
                    }) {
                        Some(msgs) => msgs,
                        None => {
                            crate::log::info(&format!(
                                "Poller: {} sync exceeded {}s deadline — skipping this cycle (wedged connection?)",
                                source.name, NETWORK_SYNC_DEADLINE.as_secs()));
                            note_error(&db, &source.name, source.id, &mut failing,
                                &format!("no answer within {}s", NETWORK_SYNC_DEADLINE.as_secs()));
                            continue;
                        }
                    }
                }
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
                if !is_maildir { new_non_mail = true; }
            }

            // A failed sync is not a quiet one. Say why, and leave
            // last_sync where it was so the next cycle re-covers the
            // window this one missed.
            if let Some(err) = sources::take_sync_error() {
                note_error(&db, &source.name, source.id, &mut failing, &err);
                continue;
            }
            clear_error(&db, source.id, &mut failing);

            polled_at.insert(source.id, now);
            let written = *persisted_at.get(&source.id)
                .unwrap_or(&source.last_sync.unwrap_or(0));
            if count > 0 || now - written >= PERSIST_EVERY {
                db.update_source_sync_time(source.id);
                persisted_at.insert(source.id, now);
            }

            if count > 0 {
                let _ = tx.send(PollerEvent::NewMessages(count));
            }
        }

        // One POST per cycle, and only a cycle that inserted non-mail
        // rows. Idle cycles never reach this line.
        if new_non_mail {
            if let Some(cfg) = &push { crate::feeder::push_new(&db, cfg); }
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

/// Write a source's failure down, but only when it is news. A source
/// that has been failing for an hour should still say "failing since
/// 05:30", and a row rewritten every ten seconds is a database write
/// every ten seconds.
fn note_error(db: &Arc<Database>, name: &str, id: i64,
              failing: &mut HashMap<i64, String>, err: &str) {
    if failing.get(&id).map(|e| e.as_str()) == Some(err) { return; }
    crate::log::info(&format!("Poller: {} sync failed: {}", name, err));
    db.set_source_error(id, Some(err));
    failing.insert(id, err.to_string());
}

/// Clear a source's failure after a sync that worked.
fn clear_error(db: &Arc<Database>, id: i64, failing: &mut HashMap<i64, String>) {
    if failing.remove(&id).is_some() {
        db.set_source_error(id, None);
    }
}

/// Dualog Workspace never reaches the poller: an external
/// `ws-bridge-listen` writes its rows straight into the database. So
/// the one thing kastrup can check is the breadcrumb ws-bridge leaves
/// when its refresh token expires. One `stat()` per cycle, and it is
/// the difference between a silent morning and a line that says to run
/// `ws-bridge login`.
fn check_ws_bridge(db: &Arc<Database>, sources: &[crate::source::Source],
                   failing: &mut HashMap<i64, String>) {
    let Some(src) = sources.iter().find(|s| s.plugin_type == "workspace") else { return };
    let home = std::env::var("HOME").unwrap_or_default();
    let flag = PathBuf::from(&home).join(".ws-bridge").join("token-expired");
    if flag.exists() {
        note_error(db, &src.name, src.id, failing,
                   "login expired, run `ws-bridge login`");
    } else {
        clear_error(db, src.id, failing);
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
