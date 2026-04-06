/// A single message from any source (email, chat, RSS, etc.)
#[derive(Clone)]
pub struct Message {
    pub id: i64,
    pub source_id: i64,
    pub external_id: String,
    pub thread_id: Option<String>,
    pub parent_id: Option<i64>,
    pub sender: String,
    pub sender_name: Option<String>,
    pub recipients: String,
    pub cc: Option<String>,
    pub subject: Option<String>,
    pub content: String,
    pub html_content: Option<String>,
    pub timestamp: i64,
    pub received_at: i64,
    pub read: bool,
    pub starred: bool,
    pub archived: bool,
    pub labels: Vec<String>,
    pub attachments: Vec<serde_json::Value>,
    pub metadata: serde_json::Value,
    pub folder: Option<String>,
    pub replied: bool,
    // UI state (not from DB)
    pub source_type: String,
    pub is_header: bool,
    pub full_loaded: bool,
}

impl Message {
    /// Create a default header message (used as section separator in threaded view).
    pub fn default_header() -> Self {
        Self {
            id: 0, source_id: 0, external_id: String::new(),
            thread_id: None, parent_id: None,
            sender: String::new(), sender_name: None,
            recipients: String::new(), cc: None,
            subject: None, content: String::new(),
            html_content: None, timestamp: 0, received_at: 0,
            read: true, starred: false, archived: false,
            labels: Vec::new(), attachments: Vec::new(),
            metadata: serde_json::Value::Null, folder: None,
            replied: false, source_type: String::new(),
            is_header: true, full_loaded: true,
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            id: 0,
            source_id: 0,
            external_id: String::new(),
            thread_id: None,
            parent_id: None,
            sender: String::new(),
            sender_name: None,
            recipients: String::new(),
            cc: None,
            subject: None,
            content: String::new(),
            html_content: None,
            timestamp: 0,
            received_at: 0,
            read: false,
            starred: false,
            archived: false,
            labels: Vec::new(),
            attachments: Vec::new(),
            metadata: serde_json::Value::Null,
            folder: None,
            replied: false,
            source_type: String::new(),
            is_header: false,
            full_loaded: false,
        }
    }
}
