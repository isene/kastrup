/// A configured communication source (email account, Discord server, RSS feed, etc.)
pub struct Source {
    pub id: i64,
    pub name: String,
    pub plugin_type: String,
    pub enabled: bool,
    pub config: serde_json::Value,
    pub capabilities: serde_json::Value,
    pub last_sync: Option<i64>,
    pub last_error: Option<String>,
    pub message_count: i64,
    pub poll_interval: i64,
    pub color: Option<String>,
}

impl Default for Source {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            plugin_type: String::new(),
            enabled: true,
            config: serde_json::Value::Null,
            capabilities: serde_json::Value::Null,
            last_sync: None,
            last_error: None,
            message_count: 0,
            poll_interval: 900,
            color: None,
        }
    }
}
