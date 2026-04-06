use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;

use rusqlite::{params, Connection};

use crate::message::Message;
use crate::source::Source;

/// Filter criteria for querying messages
#[derive(Default)]
pub struct Filters {
    pub source_id: Option<i64>,
    pub source_ids: Option<Vec<i64>>,
    pub is_read: Option<bool>,
    pub is_starred: Option<bool>,
    pub folder: Option<String>,
    pub sender_pattern: Option<String>,
    pub source_type: Option<String>,
    pub content_pattern: Option<String>,
}

/// A user-defined view from the database
pub struct View {
    pub id: i64,
    pub name: String,
    pub key_binding: Option<String>,
    pub filters: String,
    pub sort_order: String,
    pub is_remainder: bool,
    pub color: Option<i64>,
    pub icon: Option<String>,
}

/// Thread-safe wrapper around the SQLite database
pub struct Database {
    pub conn: Mutex<Connection>,
}

pub fn now_secs() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

impl Database {
    /// Open or create the Heathrow/Kastrup database.
    pub fn new() -> Result<Self, String> {
        let path = db_path();
        // Ensure directory exists
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let is_new = !path.exists();
        let conn = Connection::open(&path)
            .map_err(|e| format!("Failed to open database: {}", e))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .map_err(|e| format!("Failed to set pragmas: {}", e))?;
        if is_new {
            Self::create_schema(&conn)?;
        }
        Ok(Self { conn: Mutex::new(conn) })
    }

    /// Returns true if the database was just created (no messages)
    pub fn is_empty(&self) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get::<_, i64>(0))
            .unwrap_or(0) == 0
    }

    fn create_schema(conn: &Connection) -> Result<(), String> {
        conn.execute_batch(r#"
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                external_id TEXT NOT NULL,
                thread_id TEXT,
                parent_id INTEGER,
                sender TEXT NOT NULL,
                sender_name TEXT,
                recipients TEXT NOT NULL,
                cc TEXT,
                bcc TEXT,
                subject TEXT,
                content TEXT NOT NULL,
                html_content TEXT,
                timestamp INTEGER NOT NULL,
                received_at INTEGER NOT NULL,
                read INTEGER DEFAULT 0,
                starred INTEGER DEFAULT 0,
                archived INTEGER DEFAULT 0,
                labels TEXT,
                attachments TEXT,
                metadata TEXT,
                folder TEXT,
                replied INTEGER DEFAULT 0,
                UNIQUE(source_id, external_id),
                FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE,
                FOREIGN KEY(parent_id) REFERENCES messages(id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                plugin_type TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                config TEXT NOT NULL,
                capabilities TEXT NOT NULL,
                last_sync INTEGER,
                last_error TEXT,
                message_count INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                poll_interval INTEGER DEFAULT 900,
                color TEXT
            );
            CREATE TABLE IF NOT EXISTS views (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                key_binding TEXT UNIQUE,
                filters TEXT NOT NULL,
                sort_order TEXT DEFAULT 'timestamp DESC',
                is_remainder INTEGER DEFAULT 0,
                show_count INTEGER DEFAULT 1,
                color INTEGER,
                icon TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT, primary_email TEXT, identities TEXT,
                phone TEXT, avatar_url TEXT,
                tags TEXT, notes TEXT,
                message_count INTEGER DEFAULT 0,
                last_contact INTEGER
            );
            CREATE TABLE IF NOT EXISTS drafts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                reply_to_id INTEGER,
                recipients TEXT NOT NULL,
                cc TEXT, bcc TEXT, subject TEXT,
                content TEXT NOT NULL,
                attachments TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE SET NULL,
                FOREIGN KEY(reply_to_id) REFERENCES messages(id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS filters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                priority INTEGER DEFAULT 0,
                conditions TEXT NOT NULL,
                actions TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS postponed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                data TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_messages_source ON messages(source_id);
            CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id);
            CREATE INDEX IF NOT EXISTS idx_messages_read ON messages(read);
            CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender);
            CREATE INDEX IF NOT EXISTS idx_messages_read_timestamp ON messages(read, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_messages_folder ON messages(folder);
            CREATE INDEX IF NOT EXISTS idx_sources_enabled ON sources(enabled);
            CREATE INDEX IF NOT EXISTS idx_sources_plugin_type ON sources(plugin_type);
            CREATE INDEX IF NOT EXISTS idx_views_key_binding ON views(key_binding);
            CREATE INDEX IF NOT EXISTS idx_drafts_updated ON drafts(updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_filters_enabled ON filters(enabled);
            CREATE INDEX IF NOT EXISTS idx_filters_priority ON filters(priority DESC);
        "#).map_err(|e| format!("Failed to create schema: {}", e))?;

        // Insert default views
        let now = now_secs();
        let _ = conn.execute_batch(&format!(r#"
            INSERT OR IGNORE INTO views (name, key_binding, filters, created_at, updated_at)
                VALUES ('All', 'A', '{{"rules":[]}}', {now}, {now});
            INSERT OR IGNORE INTO views (name, key_binding, filters, created_at, updated_at)
                VALUES ('Unread', 'N', '{{"rules":[{{"field":"read","op":"=","value":false}}]}}', {now}, {now});
            INSERT OR IGNORE INTO views (name, key_binding, filters, created_at, updated_at)
                VALUES ('Starred', '*', '{{"rules":[{{"field":"starred","op":"=","value":true}}]}}', {now}, {now});
        "#));

        Ok(())
    }

    /// Get messages matching filters with limit and offset.
    /// Uses light mode (substr content to 200 chars) for list display.
    pub fn get_messages(&self, filters: &Filters, limit: usize, offset: usize) -> Vec<Message> {
        let conn = self.conn.lock().unwrap();
        let mut sql = String::from(
            "SELECT id, source_id, external_id, thread_id, parent_id, \
             sender, sender_name, recipients, cc, bcc, \
             subject, substr(content, 1, 200) as content, \
             timestamp, received_at, read, starred, archived, \
             labels, attachments, metadata, folder, replied \
             FROM messages WHERE 1=1"
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        // Exclude archived by default
        sql.push_str(" AND (archived = 0 OR archived IS NULL)");

        if let Some(sid) = filters.source_id {
            sql.push_str(" AND source_id = ?");
            param_values.push(Box::new(sid));
        }

        if let Some(ref ids) = filters.source_ids {
            if !ids.is_empty() {
                let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND source_id IN ({})", placeholders.join(",")));
                for id in ids {
                    param_values.push(Box::new(*id));
                }
            }
        }

        if let Some(is_read) = filters.is_read {
            sql.push_str(" AND read = ?");
            param_values.push(Box::new(if is_read { 1i64 } else { 0i64 }));
        }

        if let Some(is_starred) = filters.is_starred {
            sql.push_str(" AND starred = ?");
            param_values.push(Box::new(if is_starred { 1i64 } else { 0i64 }));
        }

        if let Some(ref folder) = filters.folder {
            sql.push_str(" AND folder = ?");
            param_values.push(Box::new(folder.clone()));
        }

        if let Some(ref pattern) = filters.sender_pattern {
            let parts: Vec<&str> = pattern.split('|').collect();
            let conditions: Vec<String> = parts.iter().map(|_|
                "(sender LIKE ? OR sender_name LIKE ?)".to_string()
            ).collect();
            sql.push_str(&format!(" AND ({})", conditions.join(" OR ")));
            for p in &parts {
                let like = format!("%{}%", p.trim());
                param_values.push(Box::new(like.clone()));
                param_values.push(Box::new(like));
            }
        }

        if let Some(ref stype) = filters.source_type {
            sql.push_str(
                " AND source_id IN (SELECT id FROM sources WHERE plugin_type = ?)"
            );
            param_values.push(Box::new(stype.clone()));
        }

        if let Some(ref pattern) = filters.content_pattern {
            sql.push_str(" AND (content LIKE ? OR subject LIKE ? OR sender LIKE ?)");
            let like = format!("%{}%", pattern);
            param_values.push(Box::new(like.clone()));
            param_values.push(Box::new(like.clone()));
            param_values.push(Box::new(like));
        }

        sql.push_str(" ORDER BY timestamp DESC LIMIT ? OFFSET ?");
        param_values.push(Box::new(limit as i64));
        param_values.push(Box::new(offset as i64));

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();

        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok(row_to_message(row))
        });

        match rows {
            Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Get a single message with full content
    pub fn get_message(&self, id: i64) -> Option<Message> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source_id, external_id, thread_id, parent_id, \
             sender, sender_name, recipients, cc, bcc, \
             subject, content, \
             timestamp, received_at, read, starred, archived, \
             labels, attachments, metadata, folder, replied, html_content \
             FROM messages WHERE id = ?"
        ).ok()?;
        stmt.query_row(params![id], |row| {
            let mut msg = row_to_message(row);
            msg.html_content = row.get::<_, Option<String>>(22).unwrap_or(None);
            msg.full_loaded = true;
            Ok(msg)
        }).ok()
    }

    /// Get only the full content and html_content for a message (light-to-full upgrade)
    pub fn get_message_content(&self, id: i64) -> Option<(String, Option<String>)> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT content, html_content FROM messages WHERE id = ?",
            params![id],
            |r| Ok((
                r.get::<_, String>(0).unwrap_or_default(),
                r.get::<_, Option<String>>(1).unwrap_or(None),
            ))
        ).ok()
    }

    /// Mark a message as read
    pub fn mark_as_read(&self, id: i64) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute("UPDATE messages SET read = 1 WHERE id = ?", params![id]);
    }

    /// Mark a message as unread
    pub fn mark_as_unread(&self, id: i64) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute("UPDATE messages SET read = 0 WHERE id = ?", params![id]);
    }

    /// Toggle read status, returning new state
    pub fn toggle_read(&self, id: i64) -> bool {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "UPDATE messages SET read = NOT read WHERE id = ?", params![id]
        );
        let new: i64 = conn.query_row(
            "SELECT read FROM messages WHERE id = ?", params![id], |r| r.get(0)
        ).unwrap_or(0);
        new != 0
    }

    /// Toggle star status, returning new state
    pub fn toggle_star(&self, id: i64) -> bool {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "UPDATE messages SET starred = NOT starred WHERE id = ?", params![id]
        );
        let new: i64 = conn.query_row(
            "SELECT starred FROM messages WHERE id = ?", params![id], |r| r.get(0)
        ).unwrap_or(0);
        new != 0
    }

    /// Mark all messages as read, optionally filtered
    pub fn mark_all_as_read(&self, view_filter: Option<&Filters>) {
        let conn = self.conn.lock().unwrap();
        match view_filter {
            Some(f) => {
                let mut sql = "UPDATE messages SET read = 1 WHERE read = 0".to_string();
                let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
                if let Some(sid) = f.source_id {
                    sql.push_str(" AND source_id = ?");
                    param_values.push(Box::new(sid));
                }
                if let Some(ref ids) = f.source_ids {
                    if !ids.is_empty() {
                        let ph: Vec<&str> = ids.iter().map(|_| "?").collect();
                        sql.push_str(&format!(" AND source_id IN ({})", ph.join(",")));
                        for id in ids { param_values.push(Box::new(*id)); }
                    }
                }
                if let Some(ref folder) = f.folder {
                    sql.push_str(" AND folder = ?");
                    param_values.push(Box::new(folder.clone()));
                }
                let refs: Vec<&dyn rusqlite::types::ToSql> =
                    param_values.iter().map(|b| b.as_ref()).collect();
                let _ = conn.execute(&sql, refs.as_slice());
            }
            None => {
                let _ = conn.execute("UPDATE messages SET read = 1 WHERE read = 0", []);
            }
        }
    }

    /// Delete messages by IDs
    pub fn delete_messages(&self, ids: &[i64]) {
        if ids.is_empty() { return; }
        let conn = self.conn.lock().unwrap();
        let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
        let sql = format!("DELETE FROM messages WHERE id IN ({})", placeholders.join(","));
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            ids.iter().map(|id| id as &dyn rusqlite::types::ToSql).collect();
        let _ = conn.execute(&sql, param_refs.as_slice());
    }

    /// Get all sources, optionally enabled only
    pub fn get_sources(&self, enabled_only: bool) -> Vec<Source> {
        let conn = self.conn.lock().unwrap();
        let sql = if enabled_only {
            "SELECT * FROM sources WHERE enabled = 1 ORDER BY id"
        } else {
            "SELECT * FROM sources ORDER BY id"
        };
        let mut stmt = match conn.prepare(sql) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt.query_map([], |row| {
            Ok(row_to_source(row))
        });
        match rows {
            Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Get source stats: source_id -> (total, unread)
    pub fn get_source_stats(&self) -> HashMap<i64, (i64, i64)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT source_id, COUNT(*) as cnt, \
             SUM(CASE WHEN read = 0 THEN 1 ELSE 0 END) as unread \
             FROM messages WHERE archived = 0 OR archived IS NULL \
             GROUP BY source_id"
        ) {
            Ok(s) => s,
            Err(_) => return HashMap::new(),
        };
        let rows = stmt.query_map([], |row| {
            let sid: i64 = row.get(0)?;
            let total: i64 = row.get(1)?;
            let unread: i64 = row.get(2)?;
            Ok((sid, (total, unread)))
        });
        match rows {
            Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
            Err(_) => HashMap::new(),
        }
    }

    /// Get source_id -> plugin_type map for all sources
    pub fn get_source_type_map(&self) -> HashMap<i64, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare("SELECT id, plugin_type FROM sources") {
            Ok(s) => s,
            Err(_) => return HashMap::new(),
        };
        let rows = stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            let ptype: String = row.get(1)?;
            Ok((id, ptype))
        });
        match rows {
            Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
            Err(_) => HashMap::new(),
        }
    }

    /// Update a message's folder, labels, and metadata
    pub fn update_message_folder(&self, id: i64, folder: &str, metadata: &serde_json::Value) {
        let conn = self.conn.lock().unwrap();
        let meta_str = serde_json::to_string(metadata).unwrap_or_default();
        let labels = serde_json::json!([folder]).to_string();
        let _ = conn.execute(
            "UPDATE messages SET folder = ?, labels = ?, metadata = ? WHERE id = ?",
            params![folder, labels, meta_str, id],
        );
    }

    /// Get overall stats: (total, unread, starred) in a single query
    pub fn get_stats(&self) -> (i64, i64, i64) {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT COUNT(*), SUM(CASE WHEN read=0 THEN 1 ELSE 0 END), \
             SUM(CASE WHEN starred=1 THEN 1 ELSE 0 END) FROM messages",
            [],
            |r| Ok((
                r.get::<_, i64>(0).unwrap_or(0),
                r.get::<_, Option<i64>>(1).unwrap_or(Some(0)).unwrap_or(0),
                r.get::<_, Option<i64>>(2).unwrap_or(Some(0)).unwrap_or(0),
            ))
        ).unwrap_or((0, 0, 0))
    }

    /// Get a setting value
    pub fn get_setting(&self, key: &str) -> Option<String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT value FROM settings WHERE key = ?", params![key], |r| r.get(0)
        ).ok()
    }

    /// Set a setting value
    pub fn set_setting(&self, key: &str, val: &str) {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let _ = conn.execute(
            "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            params![key, val, now],
        );
    }

    /// Toggle source enabled/disabled, returns new state
    pub fn toggle_source_enabled(&self, source_id: i64) -> bool {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "UPDATE sources SET enabled = NOT enabled WHERE id = ?",
            params![source_id],
        );
        let new: i64 = conn.query_row(
            "SELECT enabled FROM sources WHERE id = ?", params![source_id], |r| r.get(0)
        ).unwrap_or(0);
        new != 0
    }

    /// Get folder message counts: (total, unread)
    pub fn folder_message_count(&self, folder: &str) -> (i64, i64) {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT COUNT(*), SUM(CASE WHEN read = 0 THEN 1 ELSE 0 END) FROM messages WHERE folder = ?",
            params![folder],
            |row| Ok((
                row.get::<_, i64>(0).unwrap_or(0),
                row.get::<_, Option<i64>>(1).unwrap_or(Some(0)).unwrap_or(0),
            ))
        ).unwrap_or((0, 0))
    }

    /// Get favorite folders from settings
    pub fn get_favorite_folders(&self) -> Vec<String> {
        self.get_setting("favorite_folders")
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    /// Save favorite folders to settings
    pub fn save_favorite_folders(&self, folders: &[String]) {
        let json = serde_json::to_string(folders).unwrap_or_default();
        self.set_setting("favorite_folders", &json);
    }

    /// Get all views
    pub fn get_views(&self) -> Vec<View> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare("SELECT * FROM views ORDER BY id") {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt.query_map([], |row| {
            Ok(View {
                id: row.get(0)?,
                name: row.get(1)?,
                key_binding: row.get(2)?,
                filters: row.get(3)?,
                sort_order: row.get::<_, String>(4).unwrap_or_else(|_| "timestamp DESC".to_string()),
                is_remainder: row.get::<_, i64>(5).unwrap_or(0) != 0,
                color: row.get(7).ok(),
                icon: row.get(8).ok(),
            })
        });
        match rows {
            Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Get all known external_ids for a given source (used by poller to skip duplicates)
    pub fn get_known_external_ids(&self, source_id: i64) -> HashSet<String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT external_id FROM messages WHERE source_id = ?"
        ).unwrap();
        stmt.query_map(params![source_id], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    /// Insert a new message from a source plugin
    pub fn insert_message(&self, source_id: i64, msg: &crate::sources::MessageData) {
        let conn = self.conn.lock().unwrap();
        let now = now_secs();
        let labels_json = serde_json::to_string(&msg.labels).unwrap_or_default();
        let atts_json = serde_json::to_string(&msg.attachments).unwrap_or_default();
        let meta_json = serde_json::to_string(&msg.metadata).unwrap_or_default();
        let recipients_json = serde_json::json!([&msg.recipients]).to_string();
        let cc_json = msg.cc.as_ref().map(|c| serde_json::json!([c]).to_string());

        let _ = conn.execute(
            "INSERT OR IGNORE INTO messages (source_id, external_id, thread_id, \
             sender, sender_name, recipients, cc, subject, content, html_content, \
             timestamp, received_at, read, starred, labels, attachments, metadata, folder) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?)",
            params![
                source_id, msg.external_id, msg.thread_id,
                msg.sender, msg.sender_name, recipients_json, cc_json,
                msg.subject, msg.content, msg.html_content,
                msg.timestamp, now,
                labels_json, atts_json, meta_json, msg.folder,
            ],
        );
    }

    /// Insert multiple messages in a single transaction (batch mode).
    /// Uses small batches to avoid holding the lock for too long.
    pub fn insert_messages_batch(&self, source_id: i64, msgs: &[crate::sources::MessageData]) {
        if msgs.is_empty() { return; }
        // Insert in chunks of 20 to keep lock hold time short
        for chunk in msgs.chunks(20) {
            let conn = self.conn.lock().unwrap();
            let _ = conn.execute("BEGIN", []);
            for msg in chunk {
                let labels_json = serde_json::to_string(&msg.labels).unwrap_or_default();
                let atts_json = serde_json::to_string(&msg.attachments).unwrap_or_default();
                let meta_json = serde_json::to_string(&msg.metadata).unwrap_or_default();
                let recipients_json = serde_json::json!([&msg.recipients]).to_string();
                let cc_json = msg.cc.as_ref().map(|c| serde_json::json!([c]).to_string());
                let now = now_secs();
                let _ = conn.execute(
                    "INSERT OR IGNORE INTO messages (source_id, external_id, thread_id, \
                     sender, sender_name, recipients, cc, subject, content, html_content, \
                     timestamp, received_at, read, starred, labels, attachments, metadata, folder) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?)",
                    params![
                        source_id, msg.external_id, msg.thread_id,
                        msg.sender, msg.sender_name, recipients_json, cc_json,
                        msg.subject, msg.content, msg.html_content,
                        msg.timestamp, now,
                        labels_json, atts_json, meta_json, msg.folder,
                    ],
                );
            }
            let _ = conn.execute("COMMIT", []);
            // Drop lock between chunks so main thread can acquire it
        }
    }

    /// Add a new source
    pub fn add_source(&self, name: &str, plugin_type: &str, config: &str, capabilities: &str, poll_interval: i64) {
        let conn = self.conn.lock().unwrap();
        let now = now_secs();
        let _ = conn.execute(
            "INSERT INTO sources (name, plugin_type, config, capabilities, created_at, updated_at, poll_interval) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![name, plugin_type, config, capabilities, now, now, poll_interval],
        );
    }

    /// Update the last_sync timestamp for a source
    pub fn update_source_sync_time(&self, source_id: i64) {
        let conn = self.conn.lock().unwrap();
        let now = now_secs();
        let _ = conn.execute(
            "UPDATE sources SET last_sync = ? WHERE id = ?",
            params![now, source_id],
        );
    }
}

/// Convert a rusqlite row to a Message struct
fn row_to_message(row: &rusqlite::Row) -> Message {
    let labels_str: String = row.get::<_, String>(17).unwrap_or_default();
    let labels: Vec<String> = serde_json::from_str(&labels_str).unwrap_or_default();

    let attachments_str: String = row.get::<_, String>(18).unwrap_or_default();
    let has_attachments = !attachments_str.is_empty() && attachments_str != "[]" && attachments_str != "null";
    let attachments: Vec<serde_json::Value> = if has_attachments {
        serde_json::from_str(&attachments_str).unwrap_or_default()
    } else {
        Vec::new()
    };

    let metadata_str: String = row.get::<_, String>(19).unwrap_or_default();
    let metadata: serde_json::Value =
        serde_json::from_str(&metadata_str).unwrap_or(serde_json::Value::Null);

    Message {
        id: row.get(0).unwrap_or(0),
        source_id: row.get(1).unwrap_or(0),
        external_id: row.get(2).unwrap_or_default(),
        thread_id: row.get(3).ok(),
        parent_id: row.get(4).ok(),
        sender: row.get(5).unwrap_or_default(),
        sender_name: row.get(6).ok(),
        recipients: row.get(7).unwrap_or_default(),
        cc: row.get(8).ok(),
        subject: row.get(10).ok(),
        content: row.get(11).unwrap_or_default(),
        html_content: None,
        timestamp: row.get(12).unwrap_or(0),
        received_at: row.get(13).unwrap_or(0),
        read: row.get::<_, i64>(14).unwrap_or(0) != 0,
        starred: row.get::<_, i64>(15).unwrap_or(0) != 0,
        archived: row.get::<_, i64>(16).unwrap_or(0) != 0,
        labels,
        attachments,
        metadata,
        folder: row.get(20).ok(),
        replied: row.get::<_, i64>(21).unwrap_or(0) != 0,
        source_type: String::new(),
        is_header: false,
        full_loaded: false,
    }
}

/// Convert a rusqlite row to a Source struct
fn row_to_source(row: &rusqlite::Row) -> Source {
    let config_str: String = row.get::<_, String>(4).unwrap_or_default();
    let config: serde_json::Value =
        serde_json::from_str(&config_str).unwrap_or(serde_json::Value::Null);
    let caps_str: String = row.get::<_, String>(5).unwrap_or_default();
    let capabilities: serde_json::Value =
        serde_json::from_str(&caps_str).unwrap_or(serde_json::Value::Null);

    Source {
        id: row.get(0).unwrap_or(0),
        name: row.get(1).unwrap_or_default(),
        plugin_type: row.get(2).unwrap_or_default(),
        enabled: row.get::<_, i64>(3).unwrap_or(0) != 0,
        config,
        capabilities,
        last_sync: row.get(6).ok(),
        last_error: row.get(7).ok(),
        message_count: row.get(8).unwrap_or(0),
        poll_interval: row.get::<_, i64>(10).unwrap_or(900),
        color: row.get(11).ok(),
    }
}

/// Path to ~/.heathrow/heathrow.db
fn db_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".heathrow").join("heathrow.db")
}
