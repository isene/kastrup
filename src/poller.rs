use std::sync::{Arc, Mutex, Condvar};
use std::sync::mpsc;
use std::collections::{HashMap, HashSet};
use crate::database::Database;
use crate::sources;

pub enum PollerEvent {
    NewMessages(usize),
}

pub struct Poller {
    // (stopped flag, wakeup condvar). The condvar lets stop() wake the
    // thread immediately instead of waiting for the next 1s sleep tick.
    stopped: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Poller {
    pub fn start(db: Arc<Database>, tx: mpsc::Sender<PollerEvent>) -> Self {
        let stopped = Arc::new((Mutex::new(false), Condvar::new()));
        let stopped_clone = stopped.clone();

        let thread = std::thread::spawn(move || {
            // Cache known_ids per source (loaded once, updated incrementally).
            // This HashSet grows for the process lifetime but eviction is only
            // worth building if VmRSS actually creeps up — the periodic log
            // below lets us confirm whether that ever matters in practice.
            let mut known_cache: HashMap<i64, HashSet<String>> = HashMap::new();
            log_process_memory("poller startup", &known_cache);
            let mut next_mem_log = std::time::Instant::now()
                + std::time::Duration::from_secs(3600);

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
                    if now - last_sync < interval { continue; }

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
                            sources::maildir::sync_maildir(&expanded, known, last_sync)
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

                // Park until 10s elapse or stop is signaled. wait_timeout_while
                // atomically drops the lock and parks on the condvar, so notify
                // from stop() wakes us instantly with no missed-wakeup race.
                let (lock, cvar) = &*stopped_clone;
                let guard = lock.lock().unwrap();
                let (guard, _) = cvar.wait_timeout_while(
                    guard,
                    std::time::Duration::from_secs(10),
                    |stopped| !*stopped,
                ).unwrap();
                if *guard { return; }
            }
        });

        Self { stopped, thread: Some(thread) }
    }

    pub fn stop(&mut self) {
        let (lock, cvar) = &*self.stopped;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
        // Don't join: the thread may be mid-sync with network timeouts.
        // It will exit on its own when the condvar wake re-checks the flag.
        self.thread.take();
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.stopped;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
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
