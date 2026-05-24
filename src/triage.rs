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

/// Read tock's calendar name list from ~/.tock/tock.db. Returns an
/// empty Vec if tock isn't installed or the DB is unreadable — the
/// triage prompt still works, Claude just can't suggest a specific
/// calendar.
pub fn read_calendars() -> Vec<String> {
    let home = std::env::var("HOME").unwrap_or_default();
    let db = PathBuf::from(home).join(".tock/tock.db");
    if !db.exists() { return Vec::new(); }
    let Ok(conn) = rusqlite::Connection::open_with_flags(
        &db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ) else { return Vec::new(); };
    let mut stmt = match conn.prepare("SELECT name FROM calendars ORDER BY id") {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_into_existing_category() {
        let before = "\tPersonal\n\t\tPick up keycard\n\tDualog\n\t\tBook PIP meeting\n";
        let after = insert_todo(before, "Personal", "Email NAV about barnetrygd");
        assert!(after.contains("\t\tPick up keycard\n\t\tEmail NAV about barnetrygd\n\tDualog"));
    }

    #[test]
    fn append_creates_new_category() {
        let before = "\tPersonal\n\t\tPick up keycard\n";
        let after = insert_todo(before, "Passion Fruits", "Verify design with Siv");
        assert!(after.contains("\tPassion Fruits\n\t\tVerify design with Siv"));
    }

    #[test]
    fn append_into_empty_file() {
        let after = insert_todo("", "Personal", "First todo");
        assert_eq!(after, "\tPersonal\n\t\tFirst todo\n");
    }
}
