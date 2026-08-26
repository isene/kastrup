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
    pub bcc: Option<String>,
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
    /// Set on the display row that opens a mail thread: the thread's own
    /// top message, carrying the collapse key. Mutt's shape — no synthetic
    /// header line above a conversation, the first mail IS the row you fold.
    pub fold_key: Option<String>,
    /// How many messages the fold holds. 1 means a lone mail, which gets no
    /// arrow and no count.
    pub fold_count: usize,
    pub is_header: bool,
    pub full_loaded: bool,
    /// 0 for top-level messages, +1 per reply nesting level. Set by
    /// `rebuild_display` for messages in email/maildir thread sections;
    /// stays 0 otherwise. Render side uses this to indent replies.
    pub thread_depth: u8,
}

impl Message {
    /// What to call the sender: the name, or the address when there is
    /// no name worth showing.
    ///
    /// A name can be present and empty. Plenty of mail carries a bare
    /// `From: us@example.com`, which parses to an empty name rather
    /// than none, and taken at face value that renders a blank sender
    /// in the list and `From:  <us@example.com>` in the header. A name
    /// that merely repeats the address is the same thing said twice.
    pub fn display_name(&self) -> &str {
        match self.sender_name.as_deref().map(str::trim) {
            Some(n) if !n.is_empty() && n != self.sender => n,
            _ => &self.sender,
        }
    }

    /// Create a default header message (used as section separator in threaded view).
    pub fn default_header() -> Self {
        Self {
            id: 0, source_id: 0, external_id: String::new(),
            thread_id: None, parent_id: None,
            sender: String::new(), sender_name: None,
            recipients: String::new(), cc: None, bcc: None,
            subject: None, content: String::new(),
            html_content: None, timestamp: 0, received_at: 0,
            read: true, starred: false, archived: false,
            labels: Vec::new(), attachments: Vec::new(),
            metadata: serde_json::Value::Null, folder: None,
            replied: false, source_type: String::new(),
            fold_key: None, fold_count: 0,
            is_header: true, full_loaded: true,
            thread_depth: 0,
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
            bcc: None,
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
            fold_key: None,
            fold_count: 0,
            is_header: false,
            full_loaded: false,
            thread_depth: 0,
        }
    }
}
