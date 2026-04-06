pub mod maildir;
pub mod rss;
pub mod weechat;
pub mod messenger;
pub mod instagram;

/// Data for a single message from any source plugin.
/// Used by pollers to pass parsed messages to the database layer.
pub struct MessageData {
    pub external_id: String,
    pub sender: String,
    pub sender_name: Option<String>,
    pub recipients: String,
    pub cc: Option<String>,
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
