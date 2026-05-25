//! AI-assisted message triage. Ctrl+t on a message shells out to
//! `~/.kastrup/triage.sh` (which calls `claude --print`), receives a
//! JSON array of action objects, and lets the user commit any subset
//! to:
//!   - tock calendar (drops an ICS in ~/.tock/incoming/)
//!   - hyperlist todo file (~/.tasks/todo.hl, appends under category)
//!
//! The prompt + wrapper script live as user-editable files in
//! ~/.kastrup/ so the user can tune them without rebuilding kastrup.
//! On first Ctrl+t use, if either file is missing we drop the
//! embedded defaults below into place.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const DEFAULT_PROMPT: &str = include_str!("triage_prompt_default.txt");
const DEFAULT_WRAPPER: &str = include_str!("triage_sh_default.sh");

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Action {
    Calendar {
        title: String,
        when: String,            // ISO8601 string
        #[serde(default)]
        duration_min: u32,
        #[serde(default)]
        calendar: Option<String>,
    },
    Todo {
        category: String,
        text: String,
    },
    Clarify {
        question: String,
    },
}

impl Action {
    pub fn short_label(&self) -> String {
        match self {
            Action::Calendar { title, when, calendar, .. } => format!(
                "calendar: {} ({}{})",
                title,
                when,
                calendar.as_deref().map(|c| format!(", cal={}", c)).unwrap_or_default()
            ),
            Action::Todo { category, text } => format!("todo: [{}] {}", category, text),
            Action::Clarify { question } => format!("clarify: {}", question),
        }
    }
}

/// Ensure the prompt + wrapper exist in ~/.kastrup/. First-run install.
/// Idempotent and harmless if the user has customised either file —
/// only writes when MISSING.
pub fn ensure_files_installed() -> Result<(PathBuf, PathBuf), String> {
    let home = std::env::var("HOME").map_err(|_| "no HOME".to_string())?;
    let dir = PathBuf::from(home).join(".kastrup");
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir ~/.kastrup: {}", e))?;
    let prompt = dir.join("triage-prompt.txt");
    let wrap = dir.join("triage.sh");
    if !prompt.exists() {
        std::fs::write(&prompt, DEFAULT_PROMPT)
            .map_err(|e| format!("write prompt: {}", e))?;
    }
    if !wrap.exists() {
        std::fs::write(&wrap, DEFAULT_WRAPPER)
            .map_err(|e| format!("write wrapper: {}", e))?;
        // chmod +x
        use std::os::unix::fs::PermissionsExt;
        let mut perm = std::fs::metadata(&wrap)
            .map_err(|e| format!("stat wrapper: {}", e))?
            .permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&wrap, perm)
            .map_err(|e| format!("chmod wrapper: {}", e))?;
    }
    Ok((prompt, wrap))
}

/// Read tock's CLOUD calendar names from ~/.tock/tock.db. Local
/// calendars are filtered out — triage events must always land on a
/// cloud-synced calendar so they show up on the user's phone too.
/// Returns an empty Vec if tock isn't installed or the DB is
/// unreadable; in that case Claude just won't be given a calendar
/// choice and the caller's default kicks in.
pub fn read_calendars() -> Vec<String> {
    let home = std::env::var("HOME").unwrap_or_default();
    let db = PathBuf::from(home).join(".tock/tock.db");
    if !db.exists() { return Vec::new(); }
    let Ok(conn) = rusqlite::Connection::open_with_flags(
        &db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ) else { return Vec::new(); };
    let mut stmt = match conn.prepare(
        "SELECT name FROM calendars \
         WHERE source_type != 'local' AND enabled = 1 \
         ORDER BY id"
    ) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let rows = stmt.query_map([], |r| r.get::<_, String>(0));
    let Ok(rows) = rows else { return Vec::new(); };
    rows.filter_map(|r| r.ok()).collect()
}

/// Read the existing top-level categories from a hyperlist file.
/// First-indent-level lines are the categories. Returns an empty list
/// if the file doesn't exist yet — Claude will get to invent the
/// initial set of categories.
pub fn read_categories(todo_hl: &Path) -> Vec<String> {
    let Ok(content) = std::fs::read_to_string(todo_hl) else {
        return Vec::new();
    };
    let mut out: Vec<String> = Vec::new();
    for line in content.lines() {
        // First-level lines: exactly one leading tab, then the name.
        // Two or more tabs = child items. Zero tabs = blank/comment.
        if let Some(rest) = line.strip_prefix('\t') {
            if !rest.starts_with('\t') && !rest.trim().is_empty() {
                let name = rest.trim();
                if !out.contains(&name.to_string()) {
                    out.push(name.to_string());
                }
            }
        }
    }
    out
}

/// Spawn the triage wrapper with the message context JSON on stdin
/// and parse its JSON array stdout into a Vec<Action>.
pub fn run_triage(context_json: &str) -> Result<Vec<Action>, String> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let (_prompt, wrap) = ensure_files_installed()?;
    let mut child = Command::new(&wrap)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn {}: {}", wrap.display(), e))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(context_json.as_bytes())
            .map_err(|e| format!("write stdin: {}", e))?;
    }
    let out = child.wait_with_output()
        .map_err(|e| format!("wait: {}", e))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        return Err(format!("triage wrapper failed: {}", err.trim()));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str::<Vec<Action>>(&stdout)
        .map_err(|e| format!("parse JSON: {} — body: {}", e, stdout))
}

/// Append a todo item to a hyperlist file under the given category.
/// Creates the category section if missing. Writes atomically via
/// temp-file + rename so scribe's external-change reload sees one
/// clean mtime bump.
pub fn append_todo(todo_hl: &Path, category: &str, text: &str) -> Result<(), String> {
    let existing = std::fs::read_to_string(todo_hl).unwrap_or_default();
    let new_content = insert_todo(&existing, category, text);
    if let Some(parent) = todo_hl.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let tmp = todo_hl.with_extension("hl.tmp");
    std::fs::write(&tmp, &new_content)
        .map_err(|e| format!("write tmp: {}", e))?;
    std::fs::rename(&tmp, todo_hl)
        .map_err(|e| format!("rename: {}", e))?;
    Ok(())
}

/// Pure function — given current todo.hl content + a new item, return
/// the updated content. Exposed for testability.
fn insert_todo(existing: &str, category: &str, text: &str) -> String {
    let mut lines: Vec<String> = existing.lines().map(String::from).collect();

    // Find the category line: exactly one leading tab + the category name.
    let cat_marker = format!("\t{}", category);
    let cat_idx = lines.iter().position(|l| l.trim_end() == cat_marker);

    let new_item = format!("\t\t{}", text);

    match cat_idx {
        Some(i) => {
            // Find the end of this category's items (next first-level
            // line or end of file).
            let mut insert_at = lines.len();
            for (j, line) in lines.iter().enumerate().skip(i + 1) {
                let leading_tabs = line.chars().take_while(|c| *c == '\t').count();
                let trimmed = line.trim();
                if leading_tabs <= 1 && !trimmed.is_empty() {
                    insert_at = j;
                    break;
                }
            }
            lines.insert(insert_at, new_item);
        }
        None => {
            // Append a new category section. Add a blank separator if
            // the file is non-empty and doesn't already end with one.
            if !lines.is_empty()
                && !lines.last().map(|l| l.trim().is_empty()).unwrap_or(true)
            {
                lines.push(String::new());
            }
            lines.push(format!("\t{}", category));
            lines.push(new_item);
        }
    }

    let mut out = lines.join("\n");
    if !out.ends_with('\n') { out.push('\n'); }
    out
}

/// One past triage decision, persisted to ~/.kastrup/triage.log.
/// The file holds up to MAX_LOG_ENTRIES separated by a `===…` line.
pub struct LogEntry<'a> {
    pub msg_id: i64,
    pub folder: &'a str,
    pub sender: &'a str,
    pub subject: &'a str,
    pub hint: Option<&'a str>,
    /// (action, status) — status is "committed" / "skipped" / "failed: <err>"
    pub results: &'a [(Action, String)],
}

const MAX_LOG_ENTRIES: usize = 20;

/// Append a triage decision to ~/.kastrup/triage.log, then trim the
/// file to the most recent MAX_LOG_ENTRIES entries. Idempotent and
/// fail-soft — if the log can't be written, returns Err without
/// affecting the rest of the triage flow.
pub fn append_log(entry: LogEntry) -> Result<(), String> {
    let home = std::env::var("HOME").map_err(|_| "no HOME".to_string())?;
    let path = PathBuf::from(home).join(".kastrup/triage.log");
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let now = chrono_local_iso();
    let mut block = String::new();
    block.push_str(&format!(
        "=== {} — message #{} — folder: {} ===\n",
        now, entry.msg_id, entry.folder
    ));
    block.push_str(&format!("from:    {}\n", entry.sender));
    block.push_str(&format!("subject: {}\n", entry.subject));
    block.push_str(&format!("hint:    {}\n",
        entry.hint.unwrap_or("(none)")));
    block.push_str("actions:\n");
    for (a, status) in entry.results {
        block.push_str(&format!("  [{}] {}\n", status, a.short_label()));
    }
    block.push('\n');

    // Append, then rewrite trimmed to MAX_LOG_ENTRIES.
    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    let mut combined = existing;
    combined.push_str(&block);

    // Split into entry blocks on the "=== " marker.
    let entries: Vec<String> = combined
        .split("\n=== ")
        .enumerate()
        .map(|(i, s)| if i == 0 { s.to_string() } else { format!("=== {}", s) })
        .filter(|s| !s.trim().is_empty())
        .collect();
    let trimmed = if entries.len() > MAX_LOG_ENTRIES {
        entries[entries.len() - MAX_LOG_ENTRIES..].join("\n")
    } else {
        entries.join("\n")
    };
    let final_str = if trimmed.ends_with('\n') { trimmed } else { format!("{}\n", trimmed) };

    std::fs::write(&path, final_str)
        .map_err(|e| format!("write triage.log: {}", e))?;
    Ok(())
}

/// Best-effort local-time ISO8601 without pulling chrono. Uses
/// libc::localtime_r so format matches scribe/kastrup's existing
/// time displays.
fn chrono_local_iso() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) as i64;
    unsafe {
        let mut t: libc::time_t = secs as libc::time_t;
        let mut tm: libc::tm = std::mem::zeroed();
        libc::localtime_r(&mut t as *mut _, &mut tm);
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            tm.tm_year + 1900,
            tm.tm_mon + 1,
            tm.tm_mday,
            tm.tm_hour,
            tm.tm_min,
            tm.tm_sec,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_into_existing_category() {
        let before = "\tPersonal\n\t\tBuy milk\n\tWork\n\t\tFinish report\n";
        let after = insert_todo(before, "Personal", "Call the dentist");
        assert!(after.contains("\t\tBuy milk\n\t\tCall the dentist\n\tWork"));
    }

    #[test]
    fn append_creates_new_category() {
        let before = "\tPersonal\n\t\tBuy milk\n";
        let after = insert_todo(before, "Side Project", "Review the design draft");
        assert!(after.contains("\tSide Project\n\t\tReview the design draft"));
    }

    #[test]
    fn append_into_empty_file() {
        let after = insert_todo("", "Personal", "First todo");
        assert_eq!(after, "\tPersonal\n\t\tFirst todo\n");
    }
}
