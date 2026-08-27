use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;

use rusqlite::{params, Connection};

use crate::message::Message;
use crate::source::Source;

/// Filter criteria for querying messages.
///
/// A single Filters describes one AND-group: all set fields must
/// match. To express OR across heterogeneous criteria — e.g. a
/// "Project Foo" view that wants `(folder = X) OR (folder LIKE %foo%)
/// OR (sender LIKE %foo%)` — set `branches` to a list of sub-Filters.
/// When `branches` is `Some`, the rest of THIS Filters is ignored and
/// the query is built as `WHERE (b1) OR (b2) OR …`. A single-branch
/// list is equivalent to the un-branched Filters.
#[derive(Default, Clone)]
pub struct Filters {
    /// One message by its row id — what `kastrup 7957849` looks up. Set
    /// alone it names exactly one message, and it is the one filter that
    /// reaches an archived one: asking for a message by number is asking
    /// for that message, not for whichever of them is still in the inbox.
    pub message_id: Option<i64>,
    pub source_id: Option<i64>,
    pub source_ids: Option<Vec<i64>>,
    pub is_read: Option<bool>,
    pub is_starred: Option<bool>,
    pub folder: Option<String>,
    /// `folder LIKE %pattern%` (with `%` wildcards added around the
    /// value). Pipe-separated for OR-of-LIKE matching — same shape
    /// as `sender_pattern`. Set when a view rule has
    /// `field=folder, op=like`.
    pub folder_pattern: Option<String>,
    pub sender_pattern: Option<String>,
    pub source_type: Option<String>,
    pub content_pattern: Option<String>,
    /// Match gateway/phone messages by their `metadata.platform` value
    /// (e.g. whatsapp, sms, or a relay "Add app" slug). Lets a view scope
    /// to one platform within the shared gateway source. `=` exact match.
    pub platform: Option<String>,
    /// Whole conversations: `subject` ending in any of these, so a thread's
    /// `Re:` and `Sv:` replies come along with the message that matched.
    /// Search sets it after widening its hits.
    pub subjects: Option<Vec<String>>,
    /// Optional OR-of-Filters. When present, takes precedence over
    /// the other fields of this struct — each branch is rendered as
    /// its own AND-group and combined with `OR`.
    pub branches: Option<Vec<Filters>>,
}

/// A user-defined view from the database. DB-model struct: fields mirror
/// the `views` table columns; not all are read at every call site.
#[allow(dead_code)]
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
    /// A small pool of independent connections used for *reads*. A
    /// connection is checked out only for the duration of one query, so a
    /// slow read (a cold-page `pread64` in D-state) holds nothing but its
    /// own connection and can never block the UI's reads or the writer.
    /// This is the fix for the "single SQLite mutex held across a slow read
    /// freezes the whole UI" stall: more connections, not finer locking.
    read_pool: Mutex<Vec<Connection>>,
    /// Set by every path that changes a message's read state, cleared by
    /// the read-state export. One atomic load per idle tick is what keeps
    /// that export off a timer: nothing changed, nothing runs.
    read_dirty: std::sync::atomic::AtomicBool,
}

/// A read connection checked out of `Database::read_pool`. Derefs to the
/// underlying `Connection`, and returns it to the pool on drop. The `Shared`
/// variant is the rare fallback to the writer connection.
enum ReadConn<'a> {
    Pooled { db: &'a Database, conn: Option<Connection> },
    Shared(std::sync::MutexGuard<'a, Connection>),
}

impl std::ops::Deref for ReadConn<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        match self {
            ReadConn::Pooled { conn, .. } => conn.as_ref().unwrap(),
            ReadConn::Shared(g) => g,
        }
    }
}

impl Drop for ReadConn<'_> {
    fn drop(&mut self) {
        if let ReadConn::Pooled { db, conn } = self {
            if let Some(c) = conn.take() {
                db.read_pool.lock().unwrap().push(c);
            }
        }
    }
}

/// Build a single AND-group's WHERE fragment from a Filters (ignoring
/// `branches`) plus its parameter list. Returns `(sql, params)` where
/// `sql` is concatenated to a `WHERE ` prefix by the caller and the
/// params line up positionally with the `?` placeholders.
///
/// Empty fragment (no field set) means "match everything" — caller
/// handles that as a short-circuit.
fn build_branch_where(filters: &Filters) -> (String, Vec<Box<dyn rusqlite::types::ToSql>>) {
    let mut parts: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if let Some(id) = filters.message_id {
        parts.push("id = ?".into());
        params.push(Box::new(id));
    }
    if let Some(sid) = filters.source_id {
        parts.push("source_id = ?".into());
        params.push(Box::new(sid));
    }
    if let Some(ref ids) = filters.source_ids {
        if !ids.is_empty() {
            let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
            parts.push(format!("source_id IN ({})", placeholders.join(",")));
            for id in ids { params.push(Box::new(*id)); }
        }
    }
    if let Some(is_read) = filters.is_read {
        parts.push("read = ?".into());
        params.push(Box::new(if is_read { 1i64 } else { 0i64 }));
    }
    if let Some(is_starred) = filters.is_starred {
        parts.push("starred = ?".into());
        params.push(Box::new(if is_starred { 1i64 } else { 0i64 }));
    }
    if let Some(ref folder) = filters.folder {
        parts.push("folder = ?".into());
        params.push(Box::new(folder.clone()));
    }
    if let Some(ref pattern) = filters.folder_pattern {
        let parts_p: Vec<&str> = pattern.split('|').collect();
        let conditions: Vec<String> = parts_p.iter()
            .map(|_| "folder LIKE ?".to_string()).collect();
        parts.push(format!("({})", conditions.join(" OR ")));
        for p in &parts_p {
            params.push(Box::new(format!("%{}%", p.trim())));
        }
    }
    if let Some(ref pattern) = filters.sender_pattern {
        let parts_p: Vec<&str> = pattern.split('|').collect();
        let conditions: Vec<String> = parts_p.iter().map(|_|
            "(sender LIKE ? OR sender_name LIKE ?)".to_string()
        ).collect();
        parts.push(format!("({})", conditions.join(" OR ")));
        for p in &parts_p {
            let like = format!("%{}%", p.trim());
            params.push(Box::new(like.clone()));
            params.push(Box::new(like));
        }
    }
    if let Some(ref stype) = filters.source_type {
        parts.push("source_id IN (SELECT id FROM sources WHERE plugin_type = ?)".into());
        params.push(Box::new(stype.clone()));
    }
    if let Some(ref plat) = filters.platform {
        parts.push("json_extract(metadata, '$.platform') = ?".into());
        params.push(Box::new(plat.clone()));
    }
    if let Some(ref subs) = filters.subjects {
        if !subs.is_empty() {
            let ors: Vec<&str> = subs.iter().map(|_| "subject LIKE ?").collect();
            parts.push(format!("({})", ors.join(" OR ")));
            // Anchored at the end, so "Dualog Insight" catches "RE: Dualog
            // Insight" and "Sv: Re: Dualog Insight" without catching a
            // subject that merely mentions it in the middle.
            for sub in subs { params.push(Box::new(format!("%{}", sub))); }
        }
    }
    if let Some(ref pattern) = filters.content_pattern {
        // content_text is the decoded body. Matching `content` matched
        // base64: it invents hits where the letters happen to fall inside an
        // encoded blob, and misses real ones. 366 of 269,434 rows predate the
        // column, so those fall back to raw.
        parts.push(
            "(COALESCE(content_text, content) LIKE ? OR subject LIKE ? OR sender LIKE ?)".into());
        let like = format!("%{}%", pattern);
        params.push(Box::new(like.clone()));
        params.push(Box::new(like.clone()));
        params.push(Box::new(like));
    }
    (parts.join(" AND "), params)
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
        // WAL + tuning aimed at "background daemon, never freeze the desktop":
        //   - synchronous=NORMAL is safe under WAL (no corruption risk,
        //     only loses the last commit on a power cut). FULL was
        //     issuing an extra fsync per commit that landed in the
        //     foreground IO queue.
        //   - wal_autocheckpoint=200 keeps each WAL flush small
        //     (~800 KB), so a checkpoint never becomes a multi-second
        //     IO stall. The default 1000 batches more, costs less
        //     total IO but spikes harder — wrong tradeoff for a
        //     laptop where the user sees the spike.
        //   - journal_size_limit caps the WAL at 64 MB so it can't
        //     grow into a giant during a long write burst.
        //   - busy_timeout=5000 unchanged: writers wait politely.
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;\n\
             PRAGMA synchronous=NORMAL;\n\
             PRAGMA wal_autocheckpoint=200;\n\
             PRAGMA journal_size_limit=67108864;\n\
             PRAGMA busy_timeout=5000;"
        ).map_err(|e| format!("Failed to set pragmas: {}", e))?;
        if is_new {
            Self::create_schema(&conn)?;
        } else {
            // create_schema only runs for a brand-new file, so tables added
            // in later versions never reach an existing database. Anything
            // additive belongs here too.
            Self::ensure_added_tables(&conn);
        }
        Ok(Self {
            conn: Mutex::new(conn),
            read_pool: Mutex::new(Vec::new()),
            read_dirty: std::sync::atomic::AtomicBool::new(true),
        })
    }

    /// Open an independent connection to the same DB file. WAL mode lets
    /// it read concurrently without taking the shared `conn` Mutex, so a
    /// slow background scan (the startup stuck-maildir reconcile, which on
    /// a cold 2.4 GB DB took ~90 s) can never block the UI's message-load
    /// on the main connection. busy_timeout so it waits politely on the
    /// rare checkpoint.
    pub fn open_aux_connection(&self) -> Result<Connection, String> {
        let conn = Connection::open(db_path())
            .map_err(|e| format!("aux connection open: {}", e))?;
        // query_only rejects accidental writes on a read connection; WAL is
        // already on at the file level so reads see the latest snapshot.
        let _ = conn.execute_batch("PRAGMA busy_timeout=5000; PRAGMA query_only=ON;");
        Ok(conn)
    }

    /// Check out an independent read connection from the pool (opening one
    /// on demand). Returned to the pool when the guard drops. Read methods
    /// use this instead of `self.conn.lock()` so a slow read holds only its
    /// own connection, never the shared write mutex the UI is waiting on.
    fn read(&self) -> ReadConn<'_> {
        let pooled = self.read_pool.lock().unwrap().pop();
        match pooled.or_else(|| self.open_aux_connection().ok()) {
            Some(conn) => ReadConn::Pooled { db: self, conn: Some(conn) },
            // Fallback (read connection couldn't be opened): use the shared
            // one. Rare; keeps reads correct even if the pool can't grow.
            None => ReadConn::Shared(self.conn.lock().unwrap()),
        }
    }

    /// Returns true if the database was just created (no messages)
    pub fn is_empty(&self) -> bool {
        let conn = self.read();
        conn.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get::<_, i64>(0))
            .unwrap_or(0) == 0
    }

    /// Tables introduced after a database was first created. Idempotent,
    /// and cheap enough to run on every open: SQLite parses two statements
    /// and finds both objects already there.
    fn ensure_added_tables(conn: &Connection) {
        let _ = conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS scheduled (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                data TEXT NOT NULL,
                send_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                last_error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_scheduled_due ON scheduled(send_at);"
        );
        // The decoded body, so nothing outside kastrup has to reassemble
        // MIME to read a message. `content` keeps the raw parts, which is
        // what attachment extraction needs; this is the text a reader
        // wants — and what `content LIKE '%…%'` has to search to find
        // anything at all, since most bodies arrive base64'd. Error
        // ignored: the column is already there on the second run.
        let _ = conn.execute("ALTER TABLE messages ADD COLUMN content_text TEXT", []);
        // Added after create_schema stopped running for this database. One
        // full scan the first time, then every unread recount is covered.
        let t = std::time::Instant::now();
        let _ = conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_messages_source_read ON messages(source_id, read);\n\
             CREATE INDEX IF NOT EXISTS idx_messages_parent ON messages(parent_id);\n\
             CREATE INDEX IF NOT EXISTS idx_drafts_reply_to ON drafts(reply_to_id);"
        );
        let ms = t.elapsed().as_millis();
        if ms >= 500 {
            crate::log::info(&format!("built idx_messages_source_read in {} ms", ms));
        }
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
                content_text TEXT,
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
            -- Outgoing messages held until their time comes. `kind` is the
            -- DraftKind tag so a scheduled Workspace post goes out the
            -- Workspace path, not the SMTP one.
            CREATE TABLE IF NOT EXISTS scheduled (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                data TEXT NOT NULL,
                send_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                last_error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_scheduled_due ON scheduled(send_at);
            CREATE INDEX IF NOT EXISTS idx_messages_source ON messages(source_id);
            CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id);
            CREATE INDEX IF NOT EXISTS idx_messages_read ON messages(read);
            CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender);
            CREATE INDEX IF NOT EXISTS idx_messages_read_timestamp ON messages(read, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_messages_folder ON messages(folder);
            CREATE INDEX IF NOT EXISTS idx_messages_folder_timestamp ON messages(folder, timestamp DESC);
            -- Covers all_folder_counts: (folder, COUNT(*), SUM(read=0)) GROUP BY
            -- folder. Without `read` in the index the grouping does a per-row
            -- table lookup over the whole DB (folio_wait UI freeze on the F /
            -- folder browser). With it the query is covering: ~30s -> <0.05s.
            CREATE INDEX IF NOT EXISTS idx_messages_folder_read ON messages(folder, read);
            -- Covers unread_count_by_source, which the 5s idle tick runs on
            -- the UI thread. Without `read` in the index the group-by does a
            -- table lookup per unread row across the whole DB.
            CREATE INDEX IF NOT EXISTS idx_messages_source_read ON messages(source_id, read);
            -- Foreign keys are ON, so deleting a message makes SQLite look for
            -- rows pointing at it through these two ON DELETE SET NULL links.
            -- Unindexed, each delete scanned all of messages and all of drafts.
            CREATE INDEX IF NOT EXISTS idx_messages_parent ON messages(parent_id);
            CREATE INDEX IF NOT EXISTS idx_drafts_reply_to ON drafts(reply_to_id);
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

        // One-time cleanup: dedup maildir messages sharing the same file
        // path (caused by pre-fix filing that didn't update external_id).
        let migration_key = "maildir_dedup_v1";
        let already_run: bool = conn
            .query_row(
                "SELECT 1 FROM settings WHERE key = ?",
                params![migration_key],
                |_| Ok(true),
            )
            .unwrap_or(false);
        if !already_run {
            Self::dedup_maildir(&conn);
            let _ = conn.execute(
                "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
                params![migration_key, "done", now],
            );
        }

        Ok(())
    }

    /// Remove duplicate maildir rows and fix stale external_ids left behind
    /// by the pre-fix filing bug. Keeps the row with the lowest id per file.
    fn dedup_maildir(conn: &rusqlite::Connection) {
        use std::collections::HashMap;
        let mut stmt = match conn.prepare(
            "SELECT id, external_id, folder, json_extract(metadata, '$.maildir_file') \
             FROM messages WHERE external_id LIKE 'maildir_%'"
        ) { Ok(s) => s, Err(_) => return };
        let rows: Vec<(i64, String, Option<String>, Option<String>)> = stmt
            .query_map([], |r| Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
            )))
            .and_then(|it| it.collect::<Result<Vec<_>, _>>())
            .unwrap_or_default();
        drop(stmt);

        // Group by the maildir BASENAME (stable across folder moves
        // and flag changes), not the full filesystem path. When a
        // message is saved/archived the parent dir changes but the
        // basename (and its `:2,FLAGS` suffix is also stripped here)
        // stays the same — so two rows that share a basename are
        // the same physical message and one must go.
        //
        // We also accept rows whose `maildir_file` is missing by
        // falling back to the external_id's basename component.
        let mut groups: HashMap<String, Vec<(i64, String, Option<String>)>> = HashMap::new();
        for (id, ext, folder, file) in &rows {
            let key = file.as_deref()
                .and_then(|f| std::path::Path::new(f).file_name())
                .and_then(|f| f.to_str())
                .map(|s| s.split(":2,").next().unwrap_or(s).to_string())
                .or_else(|| {
                    // Fallback: pull basename out of the external_id.
                    let no_flags = ext.split(":2,").next().unwrap_or(ext);
                    // Skip past "maildir_<folder>_" to the digit-run.
                    let bytes = no_flags.as_bytes();
                    let mut i = 0;
                    while i < bytes.len() {
                        if bytes[i].is_ascii_digit() {
                            let start = i;
                            while i < bytes.len() && bytes[i].is_ascii_digit() { i += 1; }
                            let run = i - start;
                            if (9..=12).contains(&run) && i < bytes.len() && bytes[i] == b'.' {
                                return Some(no_flags[start..].to_string());
                            }
                        } else { i += 1; }
                    }
                    None
                });
            if let Some(k) = key {
                groups.entry(k)
                    .or_default()
                    .push((*id, ext.clone(), folder.clone()));
            }
        }

        let mut to_delete: Vec<i64> = Vec::new();
        let mut to_fix: Vec<(i64, String)> = Vec::new();
        for (path, mut entries) in groups {
            entries.sort_by_key(|e| e.0);
            let keeper = &entries[0];
            for dup in &entries[1..] {
                to_delete.push(dup.0);
            }
            // Fix keeper's external_id if it doesn't match current folder
            if let Some(folder) = &keeper.2 {
                let filename = std::path::Path::new(&path)
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or("");
                if !filename.is_empty() {
                    let expected = format!("maildir_{}_{}", folder, filename);
                    if keeper.1 != expected {
                        to_fix.push((keeper.0, expected));
                    }
                }
            }
        }

        let deleted = to_delete.len();
        for id in &to_delete {
            let _ = conn.execute("DELETE FROM messages WHERE id = ?", params![id]);
        }
        for (id, ext) in &to_fix {
            // Ignore errors from UNIQUE conflicts (e.g. another row already has this id)
            let _ = conn.execute(
                "UPDATE messages SET external_id = ? WHERE id = ?",
                params![ext, id],
            );
        }
        if deleted > 0 {
            eprintln!(
                "Kastrup: removed {} duplicate maildir entries, fixed {} external_ids",
                deleted, to_fix.len()
            );
        }
    }

    /// Get messages matching filters with limit and offset.
    /// Uses light mode (substr content to 200 chars) for list display.
    pub fn get_messages(&self, filters: &Filters, limit: usize, offset: usize) -> Vec<Message> {
        let conn = self.read();
        let mut sql = String::from(
            "SELECT id, source_id, external_id, thread_id, parent_id, \
             sender, sender_name, recipients, cc, bcc, \
             subject, substr(content, 1, 200) as content, \
             timestamp, received_at, read, starred, archived, \
             labels, attachments, metadata, folder, replied \
             FROM messages WHERE 1=1"
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        // Exclude archived by default — but not when the caller named
        // one message by id; see Filters::message_id.
        if filters.message_id.is_none() {
            sql.push_str(" AND (archived = 0 OR archived IS NULL)");
        }

        // OR-of-AND-groups: when `branches` is set, render each branch's
        // single-AND WHERE fragment in parens, joined by `OR`. The
        // unwrapped top-level Filters fields are ignored in this case —
        // the rule parser places everything into `branches` instead.
        if let Some(branches) = &filters.branches {
            if branches.is_empty() {
                return Vec::new();
            }
            let mut parts: Vec<String> = Vec::with_capacity(branches.len());
            for b in branches {
                let (frag, params) = build_branch_where(b);
                if frag.is_empty() {
                    // Empty branch matches everything — short-circuit
                    // the whole OR.
                    parts.clear();
                    break;
                }
                parts.push(format!("({})", frag));
                for p in params { param_values.push(p); }
            }
            if !parts.is_empty() {
                sql.push_str(&format!(" AND ({})", parts.join(" OR ")));
            }
            // A branched view still has to answer a search. The view's own
            // rules live in `branches`, but `/` sets content_pattern on the
            // top-level struct, and dropping it turned a search in Dualog or
            // PassionFruits into a plain re-list of the view — 500 rows and a
            // match count that meant nothing. AND it onto the branch group.
            let (frag, params) = build_branch_where(filters);
            if !frag.is_empty() {
                sql.push_str(" AND ");
                sql.push_str(&frag);
                for p in params { param_values.push(p); }
            }
        } else {
            let (frag, params) = build_branch_where(filters);
            if !frag.is_empty() {
                sql.push_str(" AND ");
                sql.push_str(&frag);
                for p in params { param_values.push(p); }
            }
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

    /// Does this view's *actual* filter match at least one non-archived
    /// unread message? Mirrors `get_messages`' WHERE exactly (every rule
    /// dimension via `build_branch_where`, plus the archived exclusion), so
    /// an inactive-view badge lights only when the view would really show
    /// unread. The folder/source caches can't do this: they ignore the
    /// per-branch rules (platform, sender, …) and archived state, so a
    /// `folder=Virham AND platform=discord` branch lit on any unread Virham
    /// row. Index-backed EXISTS (`idx_messages_folder_read`), LIMIT 1.
    /// True iff the view WOULD SHOW an unread message: probes the same
    /// window the view loads (newest `limit` matching messages, like
    /// get_messages), not the whole history. Querying all of history
    /// lit badges for views whose visible window was fully read but
    /// whose deep past held forever-unread rows (old RSS items etc.).
    pub fn view_has_unread(&self, filters: &Filters, limit: i64) -> bool {
        let conn = self.read();
        let mut sql = String::from(
            "SELECT read FROM messages WHERE (archived = 0 OR archived IS NULL)"
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(branches) = &filters.branches {
            if branches.is_empty() {
                return false;
            }
            let mut parts: Vec<String> = Vec::with_capacity(branches.len());
            for b in branches {
                let (frag, params) = build_branch_where(b);
                if frag.is_empty() {
                    // Empty branch matches everything — short-circuit.
                    parts.clear();
                    break;
                }
                parts.push(format!("({})", frag));
                for p in params { param_values.push(p); }
            }
            if !parts.is_empty() {
                sql.push_str(&format!(" AND ({})", parts.join(" OR ")));
            }
            // A branched view still has to answer a search. The view's own
            // rules live in `branches`, but `/` sets content_pattern on the
            // top-level struct, and dropping it turned a search in Dualog or
            // PassionFruits into a plain re-list of the view — 500 rows and a
            // match count that meant nothing. AND it onto the branch group.
            let (frag, params) = build_branch_where(filters);
            if !frag.is_empty() {
                sql.push_str(" AND ");
                sql.push_str(&frag);
                for p in params { param_values.push(p); }
            }
        } else {
            let (frag, params) = build_branch_where(filters);
            if !frag.is_empty() {
                sql.push_str(" AND ");
                sql.push_str(&frag);
                for p in params { param_values.push(p); }
            }
        }
        sql.push_str(" ORDER BY timestamp DESC LIMIT ?");
        param_values.push(Box::new(limit));
        let sql = format!("SELECT 1 FROM ({}) WHERE read = 0 LIMIT 1", sql);
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();
        let found = match conn.prepare(&sql) {
            Ok(mut stmt) => stmt.exists(param_refs.as_slice()).unwrap_or(false),
            Err(_) => false,
        };
        found
    }

    /// Get a single message with full content
    pub fn get_message(&self, id: i64) -> Option<Message> {
        let conn = self.read();
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

    /// Just the metadata JSON for one message. The purge path needs the
    /// `maildir_file` key and nothing else; `get_message` would drag the
    /// full body + html_content along, which on a cold page cache is a
    /// multi-second D-state read on whichever thread asked (the UI, here).
    pub fn get_message_metadata(&self, id: i64) -> Option<serde_json::Value> {
        let conn = self.read();
        let json: String = conn.query_row(
            "SELECT metadata FROM messages WHERE id = ?", params![id],
            |row| row.get(0),
        ).ok()?;
        serde_json::from_str(&json).ok()
    }

    /// Get only the full content and html_content for a message (light-to-full upgrade)
    pub fn get_message_content(&self, id: i64) -> Option<(String, Option<String>)> {
        let conn = self.read();
        Self::get_message_content_conn(&conn, id)
    }

    /// Read a message body on a caller-supplied connection. The async reader
    /// thread passes its own `open_aux_connection()` handle so a cold or large
    /// body read (D-state `folio_wait_bit_common`) stalls only that thread — it
    /// never holds the shared `conn` mutex, so it can't freeze the writer or
    /// the render thread. (kfreeze showed a 27 s UI freeze from exactly that
    /// lock contention.)
    pub fn get_message_content_conn(conn: &Connection, id: i64) -> Option<(String, Option<String>)> {
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
    /// Note that read state moved, so the export knows to run. Cheap
    /// enough to call from every path that touches `read`.
    pub fn touch_read_state(&self) {
        self.read_dirty.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// True once per batch of changes. Clears as it reports.
    pub fn take_read_dirty(&self) -> bool {
        self.read_dirty.swap(false, std::sync::atomic::Ordering::Relaxed)
    }

    /// Recent mail, for the read state shared with the phone: row id,
    /// RFC822 Message-ID, read. Bounded by `since` and index-backed
    /// (`idx_messages_timestamp`), so it stays a few hundred rows on a
    /// database of millions.
    pub fn recent_mail_read_state(&self, since: i64) -> Vec<(i64, String, bool)> {
        let conn = self.read();
        let mut stmt = match conn.prepare(
            "SELECT id, json_extract(metadata, '$.message_id'), read \
             FROM messages WHERE timestamp > ? AND metadata LIKE '%\"message_id\"%' \
             ORDER BY timestamp DESC"
        ) { Ok(s) => s, Err(_) => return Vec::new() };
        let rows = stmt.query_map(params![since], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, i64>(2)?))
        });
        match rows {
            Ok(it) => it.filter_map(|r| r.ok())
                .filter_map(|(id, mid, read)| mid.map(|m| (id, m, read != 0)))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Message-IDs of mail deleted here recently. Deleting is the
    /// strongest "I am done with this" there is, so the phone should not
    /// keep showing it as new — these go out as read.
    pub fn recent_deleted_message_ids(&self, since: i64) -> Vec<String> {
        let conn = self.read();
        let mut stmt = match conn.prepare(
            "SELECT message_id FROM deleted_external_ids \
             WHERE deleted_at > ? AND message_id IS NOT NULL"
        ) { Ok(s) => s, Err(_) => return Vec::new() };   // no column yet: nothing to say
        let rows = stmt.query_map(params![since], |r| r.get::<_, String>(0));
        match rows {
            Ok(it) => it.filter_map(|r| r.ok()).filter(|m| !m.is_empty()).collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn mark_as_read(&self, id: i64) {
        self.touch_read_state();
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute("UPDATE messages SET read = 1 WHERE id = ?", params![id]);
    }

    /// Mark a message as unread
    pub fn mark_as_unread(&self, id: i64) {
        self.touch_read_state();
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute("UPDATE messages SET read = 0 WHERE id = ?", params![id]);
    }

    /// Toggle read status, returning new state
    pub fn toggle_read(&self, id: i64) -> bool {
        self.touch_read_state();
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

    /// Unread maildir rows only, scoped to the given source_ids so
    /// the planner uses `idx_messages_source` instead of a full-table
    /// scan. Combined with `read = 0`, the result set is exactly the
    /// files that still need an `S` flag appended — typically zero
    /// or a handful, even on a 250k-message DB. Used by
    /// `mark_all_read` to avoid the metadata-LIKE scan that was
    /// freezing the UI for seconds.
    pub fn collect_unread_maildir_targets(
        &self,
        view_filter: Option<&Filters>,
        maildir_source_ids: &[i64],
    ) -> Vec<(serde_json::Value, i64)> {
        if maildir_source_ids.is_empty() { return Vec::new(); }
        let conn = self.read();
        let ph: Vec<&str> = maildir_source_ids.iter().map(|_| "?").collect();
        let mut sql = format!(
            "SELECT id, metadata FROM messages \
             WHERE source_id IN ({}) AND (read = 0 OR read IS NULL) \
               AND metadata IS NOT NULL",
            ph.join(",")
        );
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        for id in maildir_source_ids { param_values.push(Box::new(*id)); }
        if let Some(f) = view_filter {
            if let Some(sid) = f.source_id {
                sql.push_str(" AND source_id = ?");
                param_values.push(Box::new(sid));
            }
            if let Some(ref ids) = f.source_ids {
                if !ids.is_empty() {
                    let ph2: Vec<&str> = ids.iter().map(|_| "?").collect();
                    sql.push_str(&format!(" AND source_id IN ({})", ph2.join(",")));
                    for id in ids { param_values.push(Box::new(*id)); }
                }
            }
            if let Some(ref folder) = f.folder {
                sql.push_str(" AND folder = ?");
                param_values.push(Box::new(folder.clone()));
            }
        }
        let refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();
        let mut out = Vec::new();
        let Ok(mut stmt) = conn.prepare(&sql) else { return out; };
        let rows = stmt.query_map(refs.as_slice(), |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?))
        });
        if let Ok(rows) = rows {
            for row in rows.flatten() {
                let (id, meta_opt) = row;
                let Some(meta_str) = meta_opt else { continue };
                let Ok(meta) = serde_json::from_str::<serde_json::Value>(&meta_str) else { continue };
                out.push((meta, id));
            }
        }
        out
    }

    pub fn mark_all_as_read(&self, view_filter: Option<&Filters>) {
        self.touch_read_state();
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

    /// Collect (metadata, id) for unread maildir rows in the given id
    /// set. Mirror of `collect_unread_maildir_targets` but scoped by
    /// an explicit `id IN (...)` instead of a `Filters` predicate —
    /// used when the caller already knows exactly which messages to
    /// touch (e.g. the visible view).
    pub fn collect_unread_maildir_targets_by_ids(
        &self,
        ids: &[i64],
    ) -> Vec<(serde_json::Value, i64)> {
        if ids.is_empty() { return Vec::new(); }
        let conn = self.read();
        let ph: Vec<&str> = ids.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT id, metadata FROM messages \
             WHERE id IN ({}) AND (read = 0 OR read IS NULL) \
               AND metadata IS NOT NULL",
            ph.join(",")
        );
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            ids.iter().map(|id| id as &dyn rusqlite::types::ToSql).collect();
        let mut out = Vec::new();
        let Ok(mut stmt) = conn.prepare(&sql) else { return out; };
        let rows = stmt.query_map(param_refs.as_slice(), |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?))
        });
        if let Ok(rows) = rows {
            for row in rows.flatten() {
                let (id, meta_opt) = row;
                let Some(meta_str) = meta_opt else { continue };
                let Ok(meta) = serde_json::from_str::<serde_json::Value>(&meta_str) else { continue };
                out.push((meta, id));
            }
        }
        out
    }

    /// Flip `read = 1` on the explicit id set. No-op if empty.
    pub fn mark_as_read_by_ids(&self, ids: &[i64]) {
        if ids.is_empty() { return; }
        self.touch_read_state();
        let conn = self.conn.lock().unwrap();
        let ph: Vec<&str> = ids.iter().map(|_| "?").collect();
        let sql = format!(
            "UPDATE messages SET read = 1 WHERE read = 0 AND id IN ({})",
            ph.join(",")
        );
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            ids.iter().map(|id| id as &dyn rusqlite::types::ToSql).collect();
        let _ = conn.execute(&sql, param_refs.as_slice());
    }

    /// Delete messages by IDs
    pub fn delete_messages(&self, ids: &[i64]) {
        if ids.is_empty() { return; }
        let conn = self.conn.lock().unwrap();
        // Save external_ids to prevent re-insertion by poller
        let _ = conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS deleted_external_ids (external_id TEXT PRIMARY KEY, source_id INTEGER, deleted_at INTEGER)"
        );
        // Carry the RFC822 Message-ID across with the tombstone. It is the
        // only identity the phone shares, and once the row is gone there is
        // nowhere else to read it from — so a message deleted here can be
        // published as read rather than sitting unread on the phone for
        // ever. Errors ignored: the column already exists on second run.
        let _ = conn.execute("ALTER TABLE deleted_external_ids ADD COLUMN message_id TEXT", []);
        let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
        let select_sql = format!(
            "INSERT OR IGNORE INTO deleted_external_ids (external_id, source_id, deleted_at, message_id) \
             SELECT external_id, source_id, {}, json_extract(metadata, '$.message_id') \
             FROM messages WHERE id IN ({})",
            now_secs(), placeholders.join(",")
        );
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            ids.iter().map(|id| id as &dyn rusqlite::types::ToSql).collect();
        let _ = conn.execute(&select_sql, param_refs.as_slice());
        let del_sql = format!("DELETE FROM messages WHERE id IN ({})", placeholders.join(","));
        let _ = conn.execute(&del_sql, param_refs.as_slice());
    }

    /// Get all sources, optionally enabled only
    pub fn get_sources(&self, enabled_only: bool) -> Vec<Source> {
        let conn = self.read();
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
        let conn = self.read();
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
        let conn = self.read();
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
        // Also update external_id for maildir sources so the poller doesn't
        // re-ingest the file at its new path as a new message.
        // Format: maildir_{folder}_{filename} - strip using OLD folder, prepend NEW.
        let current: Option<(String, Option<String>)> = conn.query_row(
            "SELECT external_id, folder FROM messages WHERE id = ?",
            params![id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ).ok();
        if let Some((ext_id, old_folder)) = current {
            if let (Some(old), Some(rest)) = (old_folder.as_ref(), ext_id.strip_prefix("maildir_")) {
                let prefix = format!("{}_", old);
                if let Some(filename) = rest.strip_prefix(&prefix) {
                    let new_ext_id = format!("maildir_{}_{}", folder, filename);
                    let _ = conn.execute(
                        "UPDATE messages SET external_id = ? WHERE id = ?",
                        params![new_ext_id, id],
                    );
                }
            }
        }
        let _ = conn.execute(
            "UPDATE messages SET folder = ?, labels = ?, metadata = ? WHERE id = ?",
            params![folder, labels, meta_str, id],
        );
    }


    /// Folder → count of unread messages. Used by the top-bar
    /// view-strip badges to flag views (other than the current one)
    /// where new messages have arrived. One query, no per-view loop
    /// on the call site.
    pub fn unread_count_by_folder(&self) -> std::collections::HashMap<String, i64> {
        let conn = self.read();
        let mut out = std::collections::HashMap::new();
        let mut stmt = match conn.prepare(
            "SELECT folder, COUNT(*) FROM messages INDEXED BY idx_messages_folder_read \
             WHERE read = 0 \
               AND folder IS NOT NULL \
             GROUP BY folder"
        ) {
            Ok(s) => s,
            Err(_) => return out,
        };
        let rows = stmt.query_map([], |r| {
            let folder: String = r.get(0)?;
            let count: i64    = r.get(1)?;
            Ok((folder, count))
        });
        if let Ok(rows) = rows {
            for row in rows.flatten() {
                out.insert(row.0, row.1);
            }
        }
        out
    }

    /// Per-source unread counts: source_id -> unread. Lean companion to
    /// `unread_count_by_folder` for the inactive-view badges — source-
    /// scoped views (e.g. Messenger = source_id 5) carry no folder
    /// filter, so the folder cache alone can't tell whether THAT source
    /// has unread. Same `read = 0` / no-archived-filter semantics as the
    /// folder query so the two caches agree.
    pub fn unread_count_by_source(&self) -> std::collections::HashMap<i64, i64> {
        let conn = self.read();
        let mut out = std::collections::HashMap::new();
        let mut stmt = match conn.prepare(
            "SELECT source_id, COUNT(*) FROM messages INDEXED BY idx_messages_source_read \
             WHERE read = 0 \
             GROUP BY source_id"
        ) {
            Ok(s) => s,
            Err(_) => return out,
        };
        let rows = stmt.query_map([], |r| {
            let sid: i64   = r.get(0)?;
            let count: i64 = r.get(1)?;
            Ok((sid, count))
        });
        if let Ok(rows) = rows {
            for row in rows.flatten() {
                out.insert(row.0, row.1);
            }
        }
        out
    }

    /// Get a setting value
    pub fn get_setting(&self, key: &str) -> Option<String> {
        let conn = self.read();
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
        let conn = self.read();
        conn.query_row(
            "SELECT COUNT(*), SUM(CASE WHEN read = 0 THEN 1 ELSE 0 END) FROM messages WHERE folder = ?",
            params![folder],
            |row| Ok((
                row.get::<_, i64>(0).unwrap_or(0),
                row.get::<_, Option<i64>>(1).unwrap_or(Some(0)).unwrap_or(0),
            ))
        ).unwrap_or((0, 0))
    }

    /// Get total+unread counts for all folders in a single grouped query.
    /// Much faster than calling folder_message_count per folder when browsing.
    pub fn all_folder_counts(&self) -> HashMap<String, (i64, i64)> {
        let conn = self.read();
        let mut out = HashMap::new();
        if let Ok(mut stmt) = conn.prepare(
            "SELECT folder, COUNT(*), SUM(CASE WHEN read = 0 THEN 1 ELSE 0 END) \
             FROM messages WHERE folder IS NOT NULL GROUP BY folder"
        ) {
            let iter = stmt.query_map([], |r| Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1).unwrap_or(0),
                r.get::<_, Option<i64>>(2).unwrap_or(Some(0)).unwrap_or(0),
            )));
            if let Ok(rows) = iter {
                for row in rows.flatten() {
                    out.insert(row.0, (row.1, row.2));
                }
            }
        }
        out
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
        let conn = self.read();
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
        let conn = self.read();
        let mut ids: HashSet<String> = HashSet::new();
        // Current messages
        if let Ok(mut stmt) = conn.prepare("SELECT external_id FROM messages WHERE source_id = ?") {
            if let Ok(rows) = stmt.query_map(params![source_id], |row| row.get::<_, String>(0)) {
                for r in rows.flatten() { ids.insert(r); }
            }
        }
        // Deleted messages (prevent re-insertion)
        if let Ok(mut stmt) = conn.prepare("SELECT external_id FROM deleted_external_ids WHERE source_id = ?") {
            if let Ok(rows) = stmt.query_map(params![source_id], |row| row.get::<_, String>(0)) {
                for r in rows.flatten() { ids.insert(r); }
            }
        }
        ids
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
        let bcc_json = msg.bcc.as_ref().map(|c| serde_json::json!([c]).to_string());

        let _ = conn.execute(
            "INSERT OR IGNORE INTO messages (source_id, external_id, thread_id, \
             sender, sender_name, recipients, cc, bcc, subject, content, html_content, \
             timestamp, received_at, read, starred, labels, attachments, metadata, folder) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
            params![
                source_id, msg.external_id, msg.thread_id,
                msg.sender, msg.sender_name, recipients_json, cc_json, bcc_json,
                msg.subject, msg.content, msg.html_content,
                msg.timestamp, now,
                // Sent mail is mail the user wrote — arrives already read.
                if is_sent_folder(msg.folder.as_deref()) { 1i64 } else { 0i64 },
                labels_json, atts_json, meta_json, msg.folder,
            ],
        );
        // Only for a row that was actually new. The insert is OR IGNORE
        // and the pollers re-offer what they have already delivered, so
        // decoding before the insert would decode the same message on
        // every poll for nothing.
        if conn.changes() > 0 {
            let id = conn.last_insert_rowid();
            let text = decoded_body(&msg.content, msg.html_content.as_deref());
            let _ = conn.execute(
                "UPDATE messages SET content_text = ? WHERE id = ?",
                params![text, id],
            );
            link_sent_reply(&conn, id, msg);
        }
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
                let bcc_json = msg.bcc.as_ref().map(|c| serde_json::json!([c]).to_string());
                let now = now_secs();
                let _ = conn.execute(
                    "INSERT OR IGNORE INTO messages (source_id, external_id, thread_id, \
                     sender, sender_name, recipients, cc, bcc, subject, content, html_content, \
                     timestamp, received_at, read, starred, labels, attachments, metadata, folder) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
                    params![
                        source_id, msg.external_id, msg.thread_id,
                        msg.sender, msg.sender_name, recipients_json, cc_json, bcc_json,
                        msg.subject, msg.content, msg.html_content,
                        msg.timestamp, now,
                        // Sent mail is mail the user wrote — arrives already read.
                        if is_sent_folder(msg.folder.as_deref()) { 1i64 } else { 0i64 },
                        labels_json, atts_json, meta_json, msg.folder,
                    ],
                );
                // See insert_message: only what the insert actually added.
                if conn.changes() > 0 {
                    let id = conn.last_insert_rowid();
                    let text = decoded_body(&msg.content, msg.html_content.as_deref());
                    let _ = conn.execute(
                        "UPDATE messages SET content_text = ? WHERE id = ?",
                        params![text, id],
                    );
                    link_sent_reply(&conn, id, msg);
                }
            }
            let _ = conn.execute("COMMIT", []);
            // Drop lock between chunks so main thread can acquire it
        }
    }

    /// Fill `content_text` for rows that predate the column.
    ///
    /// Batched, because the decode is the expensive half and a single
    /// transaction over a quarter of a million messages would hold the
    /// write lock for minutes. Returns how many it filled; call until it
    /// returns zero.
    pub fn backfill_content_text(&self, batch: usize) -> usize {
        let rows: Vec<(i64, String, Option<String>)> = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = match conn.prepare(
                "SELECT id, content, html_content FROM messages \
                 WHERE content_text IS NULL LIMIT ?"
            ) { Ok(s) => s, Err(_) => return 0 };
            let mapped = stmt.query_map(params![batch as i64], |r| {
                Ok((r.get(0)?, r.get::<_, String>(1).unwrap_or_default(), r.get(2).ok()))
            });
            match mapped {
                Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
                Err(_) => return 0,
            }
        };
        if rows.is_empty() { return 0; }
        // Decode outside the lock: the reader thread should not wait on a
        // megabyte of base64.
        let decoded: Vec<(i64, String)> = rows.into_iter()
            .map(|(id, content, html)| (id, decoded_body(&content, html.as_deref())))
            .collect();
        let n = decoded.len();
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute("BEGIN", []);
        for (id, text) in decoded {
            let _ = conn.execute(
                "UPDATE messages SET content_text = ? WHERE id = ?",
                params![text, id],
            );
        }
        let _ = conn.execute("COMMIT", []);
        n
    }

    /// How many rows still have no decoded body.
    pub fn content_text_missing(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT count(*) FROM messages WHERE content_text IS NULL", [], |r| r.get(0))
            .unwrap_or(0)
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

/// The body as a person reads it: MIME walked, transfer encoding and
/// charset decoded, HTML rendered when there is no plain part.
///
/// One decode at write time in place of one in every reader, which is
/// the cheap direction — and the only one that does not depend on each
/// reader remembering the recipe.
fn decoded_body(content: &str, html: Option<&str>) -> String {
    mail::body_text(content, html)
}

/// True for a "Sent" mailbox in the common maildir / IMAP naming schemes
/// (`Sent`, `Sent.2026-06`, `sent-mail`, `INBOX.Sent`, `[Gmail]/Sent Mail`,
/// Outlook `Sent Items`). Mail in these folders was written by the user, so
/// it lands already-read. Deliberately strict so folders like `consent` or
/// `Presents` don't match.
/// How far back a subject match may reach when no header names the parent.
const REPLY_MATCH_WINDOW: i64 = 7 * 24 * 3600;

/// Drop every leading reply / forward prefix so two subjects compare equal.
/// Covers the English and Norwegian ones the user's mail actually carries.
pub fn normalise_subject(s: &str) -> String {
    let mut cur = s.trim();
    loop {
        let lower = cur.to_ascii_lowercase();
        let hit = ["re:", "sv:", "svar:", "fwd:", "fw:", "vs:", "aw:"]
            .iter()
            .find(|p| lower.starts_with(**p));
        match hit {
            Some(p) => cur = cur[p.len()..].trim_start(),
            None => return cur.to_string(),
        }
    }
}

/// The message a reply answers.
///
/// `In-Reply-To` and `References` settle it when they are there: both hold
/// the parent's Message-Id, which maildir rows keep in `thread_id` (indexed,
/// so this is one seek). Failing that, take the newest mail with the same
/// subject, from someone this reply is addressed to, inside the window.
///
/// `exclude_id` is the reply itself, which must never match.
pub fn find_reply_target(
    conn: &Connection,
    in_reply_to: Option<&str>,
    references: Option<&str>,
    subject: &str,
    recipients: &str,
    ts: i64,
    exclude_id: i64,
) -> Option<i64> {
    // Nearest ancestor first: In-Reply-To, then References right to left.
    let mut ids: Vec<String> = Vec::new();
    if let Some(v) = in_reply_to {
        ids.push(v.trim().trim_matches(|c| c == '<' || c == '>').to_string());
    }
    if let Some(v) = references {
        let mut r: Vec<String> = v
            .split_whitespace()
            .map(|t| t.trim_matches(|c| c == '<' || c == '>').to_string())
            .collect();
        r.reverse();
        ids.extend(r);
    }
    for mid in ids {
        if mid.is_empty() { continue; }
        let hit: Option<i64> = conn
            .query_row(
                "SELECT id FROM messages WHERE thread_id = ? AND id != ? \
                 ORDER BY timestamp DESC LIMIT 1",
                params![mid, exclude_id],
                |r| r.get(0),
            )
            .ok();
        if hit.is_some() { return hit; }
    }

    // Subject fallback. Bounded by timestamp, which is indexed, so this
    // reads a week of rows rather than the whole table.
    let want = normalise_subject(subject);
    if want.is_empty() { return None; }
    let to_lower = recipients.to_ascii_lowercase();
    let mut stmt = conn
        .prepare(
            "SELECT id, sender, subject, folder FROM messages \
             WHERE timestamp BETWEEN ? AND ? AND id != ? AND subject IS NOT NULL \
             ORDER BY timestamp DESC",
        )
        .ok()?;
    let rows = stmt
        .query_map(params![ts - REPLY_MATCH_WINDOW, ts, exclude_id], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })
        .ok()?;
    for (id, sender, subj, folder) in rows.flatten() {
        // Our own sent copies are not what a reply answers.
        if is_sent_folder(folder.as_deref()) { continue; }
        if normalise_subject(&subj) != want { continue; }
        let addr = sender.to_ascii_lowercase();
        if addr.is_empty() || !to_lower.contains(&addr) { continue; }
        return Some(id);
    }
    None
}

/// Give a freshly imported sent mail its arrow: mark what it answers as
/// replied, and hang the sent copy under it. Only runs for sent folders,
/// and only for a row the insert actually added.
fn link_sent_reply(conn: &Connection, id: i64, msg: &crate::sources::MessageData) {
    if !is_sent_folder(msg.folder.as_deref()) { return; }
    let irt = msg.metadata.get("in_reply_to").and_then(|v| v.as_str());
    let refs = msg.metadata.get("references").and_then(|v| v.as_str());
    let subject = msg.subject.as_deref().unwrap_or("");
    let recipients = format!(
        "{} {}",
        msg.recipients,
        msg.cc.clone().unwrap_or_default()
    );
    let Some(orig) = find_reply_target(
        conn, irt, refs, subject, &recipients, msg.timestamp, id,
    ) else { return };
    let _ = conn.execute("UPDATE messages SET replied = 1 WHERE id = ?", params![orig]);
    let _ = conn.execute("UPDATE messages SET parent_id = ? WHERE id = ?", params![orig, id]);
}

fn is_sent_folder(folder: Option<&str>) -> bool {
    let Some(f) = folder else { return false };
    let l = f.to_lowercase();
    l == "sent"
        || l.starts_with("sent.")   // Sent.2026-06 archive months
        || l.starts_with("sent/")
        || l.starts_with("sent-")   // sent-mail, sent-items
        || l.starts_with("sent ")   // "Sent Mail", "Sent Items"
        || l.ends_with(".sent")     // INBOX.Sent
        || l.ends_with("/sent")     // [Gmail]/Sent
        || l.contains("sent mail")
        || l.contains("sent items")
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
        fold_key: None,
        fold_count: 0,
        sender: row.get(5).unwrap_or_default(),
        sender_name: row.get(6).ok(),
        recipients: row.get(7).unwrap_or_default(),
        cc: row.get(8).ok(),
        bcc: row.get(9).ok(),
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
        thread_depth: 0,
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
        poll_interval: row.get::<_, i64>(11).unwrap_or(900),
        color: row.get(12).ok(),
    }
}

/// Path to ~/.kastrup/kastrup.db
fn db_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".kastrup").join("kastrup.db")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::MessageData;

    fn msg(ext: &str, sender: &str, to: &str, subject: &str, folder: &str,
           ts: i64, mid: &str, irt: Option<&str>) -> MessageData {
        let mut meta = serde_json::json!({ "message_id": mid });
        if let Some(v) = irt { meta["in_reply_to"] = serde_json::json!(v); }
        MessageData {
            external_id: ext.into(),
            sender: sender.into(),
            sender_name: None,
            recipients: to.into(),
            cc: None,
            bcc: None,
            subject: Some(subject.into()),
            content: "body".into(),
            html_content: None,
            timestamp: ts,
            labels: vec![],
            attachments: vec![],
            metadata: meta,
            folder: Some(folder.into()),
            thread_id: Some(mid.into()),
        }
    }

    fn id_of(db: &Database, ext: &str) -> i64 {
        let conn = db.conn.lock().unwrap();
        conn.query_row("SELECT id FROM messages WHERE external_id = ?",
                       params![ext], |r| r.get(0)).unwrap()
    }

    fn link_of(db: &Database, ext: &str) -> (i64, Option<i64>) {
        let conn = db.conn.lock().unwrap();
        conn.query_row("SELECT replied, parent_id FROM messages WHERE external_id = ?",
                       params![ext], |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
    }

    #[test]
    fn sent_replies_find_their_original() {
        assert_eq!(normalise_subject("RE: Sv: Dualog Insight"), "Dualog Insight");
        assert_eq!(normalise_subject("Fwd:  Re: hi"), "hi");
        assert_eq!(normalise_subject("Dualog Insight"), "Dualog Insight");
        assert_eq!(normalise_subject("Regarding the report"), "Regarding the report");

        let tmp = std::env::temp_dir().join("kastrup-link-test");
        std::fs::remove_dir_all(&tmp).ok();
        std::fs::create_dir_all(tmp.join(".kastrup")).unwrap();
        std::env::set_var("HOME", &tmp);
        let db = Database::new().unwrap();
        let t = 1_787_000_000i64;
        // Foreign keys are enforced on this connection, so messages need a
        // source row to hang off.
        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "INSERT OR IGNORE INTO sources (id, name, plugin_type, config, capabilities, \
                 created_at, updated_at) VALUES (1,'test','maildir','{}','{}',0,0)", []).unwrap();
        }

        // 1. Header match: In-Reply-To names the original outright.
        db.insert_message(1, &msg("in1", "bernd@ess.biz", "geir@dualog.com",
            "Dualog Insight", "AA.Customers.Dualog", t, "MID-1", None));
        db.insert_message(1, &msg("out1", "geir@dualog.com", "bernd@ess.biz",
            "RE: Dualog Insight", "Sent.2026-08", t + 600, "MID-2", Some("MID-1")));
        assert_eq!(link_of(&db, "in1").0, 1, "original marked replied");
        assert_eq!(link_of(&db, "out1").1, Some(id_of(&db, "in1")), "sent copy linked");

        // 2. No header at all: same subject, going to the original sender.
        db.insert_message(1, &msg("in2", "alice@x.com", "geir@isene.com",
            "Project X", "Geir", t, "MID-3", None));
        db.insert_message(1, &msg("out2", "geir@isene.com", "alice@x.com",
            "Re: Project X", "Sent.2026-08", t + 900, "MID-4", None));
        assert_eq!(link_of(&db, "in2").0, 1, "subject fallback marked replied");
        assert_eq!(link_of(&db, "out2").1, Some(id_of(&db, "in2")));

        // 3. Same subject, but written to somebody else — no link.
        db.insert_message(1, &msg("in3", "carol@x.com", "geir@isene.com",
            "Budget", "Geir", t, "MID-5", None));
        db.insert_message(1, &msg("out3", "geir@isene.com", "dave@x.com",
            "Re: Budget", "Sent.2026-08", t + 900, "MID-6", None));
        assert_eq!(link_of(&db, "in3").0, 0, "wrong recipient must not link");
        assert_eq!(link_of(&db, "out3").1, None);

        // 4. Same subject and person, but months apart — outside the window.
        db.insert_message(1, &msg("in4", "erik@x.com", "geir@isene.com",
            "Old thread", "Geir", t, "MID-7", None));
        db.insert_message(1, &msg("out4", "geir@isene.com", "erik@x.com",
            "Re: Old thread", "Sent.2026-08", t + 40 * 86400, "MID-8", None));
        assert_eq!(link_of(&db, "in4").0, 0, "stale match must not link");

        // 5. An incoming mail is never linked, however it looks.
        db.insert_message(1, &msg("in5", "frank@x.com", "geir@isene.com",
            "Re: Project X", "Geir", t + 1200, "MID-9", Some("MID-3")));
        assert_eq!(link_of(&db, "in5").1, None, "only sent mail links");

        std::fs::remove_dir_all(&tmp).ok();
        println!("linking ok");
    }

    #[test]
    fn the_real_pair_resolves() {
        // The message DI could not link: 7966396 (Sent) answers 7966391.
        let home = "/home/geir";
        let conn = Connection::open_with_flags(
            format!("{}/.kastrup/kastrup.db", home),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        ).unwrap();
        let (subject, to, cc, ts): (String, String, Option<String>, i64) = conn.query_row(
            "SELECT subject, recipients, cc, timestamp FROM messages WHERE id = 7966396",
            [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        ).unwrap();
        let recipients = format!("{} {}", to, cc.unwrap_or_default());
        let hit = find_reply_target(&conn, None, None, &subject, &recipients, ts, 7966396);
        println!("subject {:?} resolved to {:?}", subject, hit);
        assert_eq!(hit, Some(7966391));
    }
}
