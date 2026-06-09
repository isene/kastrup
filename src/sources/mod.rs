pub mod maildir;
pub mod rss;
pub mod weechat;
pub mod messenger;
pub mod instagram;
pub mod discord;
pub mod slack;
pub mod weechat_relay;
pub mod gateway;

/// Shared HTTP agent for polling network sources (Slack, Discord, …), with
/// hard connect/read/write timeouts. A bare `ureq::get(...).call()` uses a
/// default agent with NO read timeout, so a server that accepts the
/// connection but never replies blocks the calling thread forever in
/// recv(). Because every source is polled on the single poller thread, one
/// such hang freezes ALL mail sync (maildir included) and inotify wakes —
/// the root cause of the recurring "kastrup stale / phantom asmite unread"
/// bug. Built once and Arc-cloned per call (cheap; shares the pool).
pub fn http_agent() -> ureq::Agent {
    static HTTP_AGENT: std::sync::OnceLock<ureq::Agent> = std::sync::OnceLock::new();
    HTTP_AGENT
        .get_or_init(|| {
            ureq::AgentBuilder::new()
                .timeout_connect(std::time::Duration::from_secs(8))
                .timeout_read(std::time::Duration::from_secs(20))
                .timeout_write(std::time::Duration::from_secs(15))
                .build()
        })
        .clone()
}

/// Data for a single message from any source plugin.
/// Used by pollers to pass parsed messages to the database layer.
pub struct MessageData {
    pub external_id: String,
    pub sender: String,
    pub sender_name: Option<String>,
    pub recipients: String,
    pub cc: Option<String>,
    pub bcc: Option<String>,
    pub subject: Option<String>,
    pub content: String,
    pub html_content: Option<String>,
    pub timestamp: i64,
    pub labels: Vec<String>,
    pub attachments: Vec<serde_json::Value>,
    pub metadata: serde_json::Value,
    pub folder: Option<String>,
    pub thread_id: Option<String>,
}
