use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::sync::mpsc;
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
            while running_clone.load(Ordering::Relaxed) {
                // Get enabled sources
                let sources_list = db.get_sources(true);
                let now = crate::database::now_secs();

                for source in &sources_list {
                    let interval = source.poll_interval;
                    let last_sync = source.last_sync.unwrap_or(0);
                    if now - last_sync < interval { continue; }

                    // Poll this source
                    let new_count = match source.plugin_type.as_str() {
                        "maildir" => {
                            let path = source.config.get("path")
                                .and_then(|v| v.as_str())
                                .unwrap_or("~/Maildir");
                            let expanded = path.replace("~/",
                                &format!("{}/", std::env::var("HOME").unwrap_or_default()));

                            // Get known external_ids for this source
                            let known = db.get_known_external_ids(source.id);
                            let messages = sources::maildir::sync_maildir(&expanded, &known);
                            let count = messages.len();
                            db.insert_messages_batch(source.id, &messages);
                            count
                        }
                        "rss" => {
                            let feeds = source.config.get("feeds")
                                .and_then(|v| v.as_array())
                                .cloned()
                                .unwrap_or_default();
                            let messages = sources::rss::sync_rss(&feeds);
                            let count = messages.len();
                            db.insert_messages_batch(source.id, &messages);
                            count
                        }
                        "weechat" => {
                            let known = db.get_known_external_ids(source.id);
                            let messages = sources::weechat::sync_weechat(&source.config, &known);
                            let count = messages.len();
                            if count > 0 { db.insert_messages_batch(source.id, &messages); }
                            count
                        }
                        "messenger" => {
                            let known = db.get_known_external_ids(source.id);
                            let messages = sources::messenger::sync_messenger(&source.config, &known);
                            let count = messages.len();
                            if count > 0 { db.insert_messages_batch(source.id, &messages); }
                            count
                        }
                        "instagram" => {
                            let known = db.get_known_external_ids(source.id);
                            let messages = sources::instagram::sync_instagram(&source.config, &known);
                            let count = messages.len();
                            if count > 0 { db.insert_messages_batch(source.id, &messages); }
                            count
                        }
                        _ => 0,
                    };

                    db.update_source_sync_time(source.id);

                    if new_count > 0 {
                        let _ = tx.send(PollerEvent::NewMessages(new_count));
                    }
                }

                // Sleep 10 seconds between poll cycles (check stop flag every 100ms)
                for _ in 0..100 {
                    if !running_clone.load(Ordering::Relaxed) { break; }
                    std::thread::sleep(std::time::Duration::from_millis(100));
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
