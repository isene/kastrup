//! Asmite count-file writer. Mirrors `gmail-idle`'s `notify` module so
//! both writers produce byte-identical output: one line per mailbox in
//! `~/.gmail.conf`'s `$mailboxes` order, formatted `{label}{count}\n`,
//! written to `$mailfile` and a duplicate `$mailfile2` (the strip
//! display reads `$mailfile2` to avoid mid-write tear).
//!
//! Counts come from the SQLite DB (`messages WHERE read = 0 AND
//! folder = X`), not a filesystem scan, so we don't race with kastrup's
//! own `sync_maildir_seen_flag_bg` writer-thread.
//!
//! The legacy file is Ruby; we extract just `$mailfile` and `$mailboxes`
//! with a tiny scanner. Unrecognised lines are ignored — gmail-idle's
//! more thorough parser remains the source of truth for full config
//! semantics.
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct MailfileConfig {
    /// Absolute path of `$mailfile`; the writer also writes `path + "2"`.
    pub path: String,
    /// `[(label, folder), …]` in `$mailboxes` order. Label is the
    /// short prefix the asmite displays (e.g. `"G:"`); folder is the
    /// Maildir folder name (no leading dot).
    pub mailboxes: Vec<(String, String)>,
}

impl MailfileConfig {
    pub fn load(home: &Path) -> Option<Self> {
        let conf_path = home.join(".gmail.conf");
        let text = fs::read_to_string(&conf_path).ok()?;
        let path = scan_scalar(&text, "mailfile")?;
        let mailboxes = scan_mailboxes(&text)?;
        if mailboxes.is_empty() { return None; }
        Some(Self { path, mailboxes })
    }
}

/// Match `$NAME = "value"` (single or double quotes). Returns the
/// unquoted string for the first occurrence; None if absent.
fn scan_scalar(text: &str, name: &str) -> Option<String> {
    let needle = format!("${}", name);
    for raw in text.lines() {
        let line = raw.trim();
        if line.starts_with('#') { continue; }
        if !line.starts_with(&needle) { continue; }
        // Make sure it's a word boundary, not a prefix match.
        let after = line[needle.len()..].chars().next()?;
        if after != ' ' && after != '\t' && after != '=' { continue; }
        let eq = line.find('=')?;
        let value = line[eq+1..].trim();
        if value.starts_with('"') {
            return Some(value.trim_matches('"').to_string());
        }
        if value.starts_with('\'') {
            return Some(value.trim_matches('\'').to_string());
        }
    }
    None
}

/// Find `$mailboxes = [ ["label","folder"], … ]` (multi-line). Returns
/// the (label, folder) pairs. None if the variable isn't present.
fn scan_mailboxes(text: &str) -> Option<Vec<(String, String)>> {
    // Locate the start of the array literal — `$mailboxes` then `=`.
    let start_marker = text.find("$mailboxes")?;
    let after_name = &text[start_marker + "$mailboxes".len()..];
    let eq = after_name.find('=')?;
    let after_eq = &after_name[eq+1..];
    let bracket = after_eq.find('[')?;
    let body_start = eq + 1 + bracket + 1; // relative to after_name

    // Walk forward tracking bracket depth until we close the outer
    // array. We're starting one inside (`[` already consumed).
    let mut depth: i32 = 1;
    let mut end = body_start;
    let bytes = after_name.as_bytes();
    let mut in_str = false;
    let mut quote = b'"';
    while end < bytes.len() && depth > 0 {
        let b = bytes[end];
        if in_str {
            if b == quote { in_str = false; }
        } else {
            match b {
                b'"' | b'\'' => { in_str = true; quote = b; }
                b'[' => depth += 1,
                b']' => depth -= 1,
                b'#' => {
                    // Inline comment: skip to end of line.
                    while end < bytes.len() && bytes[end] != b'\n' { end += 1; }
                    continue;
                }
                _ => {}
            }
        }
        end += 1;
    }
    if depth != 0 { return None; }
    let body = &after_name[body_start..end-1]; // drop closing `]`

    // Now walk the inner pairs as `["label", "folder"]`.
    let mut out = Vec::new();
    let mut depth: i32 = 0;
    let mut current: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut in_str = false;
    let mut quote = '"';
    for ch in body.chars() {
        match ch {
            '"' | '\'' if !in_str => { in_str = true; quote = ch; }
            c if in_str && c == quote => {
                in_str = false;
                current.push(buf.clone());
                buf.clear();
            }
            c if in_str => buf.push(c),
            '[' => { depth += 1; current.clear(); buf.clear(); }
            ']' => {
                if depth == 1 && current.len() >= 2 {
                    out.push((current[0].clone(), current[1].clone()));
                }
                current.clear();
                depth -= 1;
            }
            _ => {}
        }
    }
    Some(out)
}

/// Write `$mailfile` and `$mailfile2` from the DB unread counts.
/// Caller passes the folder→unread map (typically `db.all_folder_counts()`)
/// so we don't take a DB lock per mailbox.
pub fn write_count_file(
    cfg: &MailfileConfig,
    counts: &std::collections::HashMap<String, (i64, i64)>,
) {
    let mut out = String::new();
    for (label, folder) in &cfg.mailboxes {
        let unread = counts.get(folder).map(|(_, u)| *u).unwrap_or(0);
        out.push_str(label);
        out.push_str(&unread.to_string());
        out.push('\n');
    }
    let path = PathBuf::from(&cfg.path);
    if let Err(e) = fs::write(&path, &out) {
        eprintln!("[kastrup] write {}: {}", cfg.path, e);
        return;
    }
    let path2 = format!("{}2", cfg.path);
    if let Err(e) = fs::write(&path2, &out) {
        eprintln!("[kastrup] write {}: {}", path2, e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_scalar_basic() {
        let t = r#"
$mailfile   = "/tmp/example.mail"
$safedir    = "/tmp/example.safe"
"#;
        assert_eq!(scan_scalar(t, "mailfile"), Some("/tmp/example.mail".into()));
        assert_eq!(scan_scalar(t, "safedir"),  Some("/tmp/example.safe".into()));
        assert_eq!(scan_scalar(t, "missing"),  None);
    }

    #[test]
    fn scan_mailboxes_multiline() {
        let t = r#"
$mailboxes = [
    [ "P:", "Personal" ],
    [ "W:", "Work"     ],
    [ "L:", "Lists"    ]
]
"#;
        let mb = scan_mailboxes(t).expect("parsed");
        assert_eq!(mb.len(), 3);
        assert_eq!(mb[0], ("P:".into(), "Personal".into()));
        assert_eq!(mb[1], ("W:".into(), "Work".into()));
        assert_eq!(mb[2], ("L:".into(), "Lists".into()));
    }

    #[test]
    fn count_file_format() {
        let cfg = MailfileConfig {
            path: "/tmp/_kastrup_test_mail".into(),
            mailboxes: vec![
                ("P:".into(), "Personal".into()),
                ("W:".into(), "Work".into()),
            ],
        };
        let mut counts = std::collections::HashMap::new();
        counts.insert("Personal".to_string(), (10i64, 3i64));
        counts.insert("Work".to_string(),     (50i64, 7i64));
        write_count_file(&cfg, &counts);
        let body = std::fs::read_to_string(&cfg.path).unwrap();
        assert_eq!(body, "P:3\nW:7\n");
        let body2 = std::fs::read_to_string(format!("{}2", cfg.path)).unwrap();
        assert_eq!(body2, "P:3\nW:7\n");
        let _ = std::fs::remove_file(&cfg.path);
        let _ = std::fs::remove_file(format!("{}2", cfg.path));
    }
}
