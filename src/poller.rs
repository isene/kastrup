use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::sync::mpsc;
use std::collections::{HashMap, HashSet};
use crate::database::Database;
use crate::sources;

pub enum PollerEvent {
    NewMessages(usize),
}

pub struct Poller {
    running: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Poller {
    pub fn start(db: Arc<Database>, tx: mpsc::Sender<PollerEvent>) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        let thread = std::thread::spawn(move || {
            // Cache known_ids per source (loaded once, updated incrementally)
            let mut known_cache: HashMap<i64, HashSet<String>> = HashMap::new();

            while running_clone.load(Ordering::Relaxed) {
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
                            sources::maildir::sync_maildir(&expanded, known)
                        }
                        "rss" => {
                            let feeds = source.config.get("feeds")
                                .and_then(|v| v.as_array())
                                .cloned()
                                .unwrap_or_default();
                            sources::rss::sync_rss(&feeds)
                        }
                        "weechat" => sources::weechat::sync_weechat(&source.config, known),
                        "messenger" => sources::messenger::sync_messenger(&source.config, known),
                        "instagram" => sources::instagram::sync_instagram(&source.config, known),
                        _ => Vec::new(),
                    };

                    let count = messages.len();
                    if count > 0 {
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

                // Sleep 10 seconds between poll cycles (check stop flag every 1s)
                for _ in 0..10 {
                    if !running_clone.load(Ordering::Relaxed) { break; }
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        });

        Self { running, thread: Some(thread) }
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        self.stop();
    }
}
