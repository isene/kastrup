//! Read state, agreed with the phone.
//!
//! The rule is asymmetric, and it falls out of what each side *writes*
//! rather than from anything here: this laptop publishes every read, a
//! phone publishes only the reads its user asked for. Both then merge
//! the same way — newest timestamp per Message-ID wins.
//!
//!   read here                   → read on the phone
//!   merely opened on the phone  → nothing
//!   marked READ on the phone    → read here
//!   deleted on the phone        → deleted here
//!
//! A phone's deletions come in its own `mail-delete-<device>.txt`, one
//! Message-ID per line. Applying one twice does nothing, so the phone
//! keeps a line for its fetch window and nothing here keeps track.
//!
//! Storage is one file per device in a folder Syncthing carries, so
//! there is never a second writer for it to leave a `.sync-conflict-`
//! copy of. The merge itself is [`mail::read_state`], shared with the
//! phone so neither end can invent its own answer.
//!
//! Cost when nothing happens: one `stat()` of the folder plus one per
//! other device's file, per idle tick. The database is not touched
//! unless a mark moved on one side or the other — [`Database::
//! take_read_dirty`] on this side, an mtime on the other.
//!
//! The laptop's own `read` column is the truth here; our published file
//! is the memory of *when* each of those states was last set, which is
//! what the merge needs and the schema does not store.

use crate::database::{now_secs, Database};
use mail::read_state::{self, Mark, Marks};
use std::path::{Path, PathBuf};

pub struct ReadSync {
    dir: PathBuf,
    mine: PathBuf,
    /// Our published file, in memory.
    marks: Marks,
    /// Other devices' files and the mtime we last folded in.
    others: Vec<(PathBuf, i64)>,
    /// The phones' delete lists, watched the same way.
    drops: Vec<(PathBuf, i64)>,
    /// Rows a phone deleted, found by the last sync, for the caller to
    /// delete with their files.
    gone: Vec<i64>,
    dir_mtime: i64,
    days: i64,
    /// Our views file for the phone, and what we last wrote to it.
    views_file: PathBuf,
    views_last: String,
}

fn mtime(p: &Path) -> i64 {
    std::fs::metadata(p)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl ReadSync {
    /// The shared folder itself.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// `None` when no folder is configured or it does not exist — the
    /// whole feature then costs nothing at all.
    pub fn new(dir: &str, days: i64) -> Option<Self> {
        if dir.trim().is_empty() { return None; }
        let dir = PathBuf::from(shellexpand_home(dir));
        if !dir.is_dir() { return None; }
        let host = hostname();
        let mine = dir.join(format!("mail-read-{}.json", host));
        let marks = std::fs::read_to_string(&mine)
            .map(|s| read_state::parse(&s))
            .unwrap_or_default();
        let views_file = dir.join(format!("mail-views-{}.json", host));
        let views_last = std::fs::read_to_string(&views_file).unwrap_or_default();
        let mut rs = Self {
            dir, mine, marks, others: Vec::new(), drops: Vec::new(), gone: Vec::new(), dir_mtime: 0,
            days: days.max(1), views_file, views_last,
        };
        rs.rescan();
        Some(rs)
    }

    /// The other devices' files, as they are right now. Zeroed mtimes so
    /// the first pass folds them in.
    fn rescan(&mut self) {
        self.dir_mtime = mtime(&self.dir);
        let mut found = Vec::new();
        let mut drops = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p == self.mine { continue; }
                let name = match p.file_name().and_then(|n| n.to_str()) {
                    Some(n) => n, None => continue,
                };
                if name.starts_with("mail-delete-") && name.ends_with(".txt") {
                    let seen = self.drops.iter().find(|(q, _)| *q == p).map(|(_, t)| *t);
                    drops.push((p, seen.unwrap_or(0)));
                    continue;
                }
                if !name.starts_with("mail-read-") || !name.ends_with(".json") { continue; }
                let seen = self.others.iter().find(|(q, _)| *q == p).map(|(_, t)| *t);
                found.push((p, seen.unwrap_or(0)));
            }
        }
        self.others = found;
        self.drops = drops;
    }

    /// Has another device said anything since we last looked? Three
    /// `stat()`s in the common case, and no read, parse or query.
    pub fn others_changed(&mut self) -> bool {
        let dm = mtime(&self.dir);
        if dm != self.dir_mtime { self.rescan(); }
        let mut changed = false;
        for (p, seen) in self.others.iter_mut().chain(self.drops.iter_mut()) {
            let m = mtime(p);
            if m != *seen { *seen = m; changed = true; }
        }
        changed
    }

    /// Fold the phone's marks into the database, then publish ours.
    /// Returns how many messages the phone moved here.
    pub fn sync(&mut self, db: &Database) -> usize {
        let since = now_secs() - self.days * 86_400;
        let rows = db.recent_mail_read_state(since);
        let deleted = db.recent_deleted_message_ids(since);
        self.gone = phone_deletes(
            &rows,
            &self.drops.iter().filter_map(|(p, _)| std::fs::read_to_string(p).ok()).collect::<Vec<_>>(),
        );
        if rows.is_empty() && deleted.is_empty() { return 0; }

        let theirs = read_state::merge_all(
            self.others.iter()
                .filter_map(|(p, _)| std::fs::read_to_string(p).ok())
                .collect::<Vec<_>>()
                .iter()
                .map(|s| s.as_str())
        );

        let plan = plan(&rows, &deleted, &theirs, &mut self.marks, now_secs());
        for (id, read) in &plan.apply {
            if *read { db.mark_as_read(*id) } else { db.mark_as_unread(*id) }
        }
        if plan.changed {
            let _ = std::fs::write(&self.mine, read_state::serialize(&self.marks));
        }
        plan.apply.len()
    }
}

impl ReadSync {
    /// Rows a phone deleted, once. The caller deletes them as `d` would,
    /// files and all.
    pub fn take_gone(&mut self) -> Vec<i64> {
        std::mem::take(&mut self.gone)
    }

    /// The oldest timestamp the phone still holds mail for.
    pub fn since(&self) -> i64 {
        now_secs() - self.days * 86_400
    }

    /// Tell the phone the views and which recent mail each takes, in
    /// `mail-views-<host>.json`. Writes only when the file would change,
    /// so a new mail outside every view costs no Syncthing transfer.
    pub fn publish_views(&mut self, views: &[PhoneView], sources: &[(i64, String)]) {
        let text = views_json(views, sources);
        if text == self.views_last { return; }
        if std::fs::write(&self.views_file, &text).is_ok() {
            self.views_last = text;
        }
    }
}

/// A view as the phone's gluon gets it.
pub struct PhoneView {
    pub key: String,
    pub name: String,
    /// The top bar's 256-colour number, as a string; empty for none.
    pub colour: String,
    /// The view's rules as kastrup keeps them. The phone reads them for
    /// what it fetches itself (chats, feeds), which the laptop's list
    /// below cannot name: the two sides give a relayed chat different ids.
    pub filters: serde_json::Value,
    /// Message-IDs of the recent mail the view takes here.
    pub mail: Vec<String>,
}

/// The views file. Keys sorted, so the same views give the same bytes and
/// [`ReadSync::publish_views`] can skip the write.
///
///   {"views":   [{"key":"1","name":"Geir","colour":"130","filters":{…}}],
///    "sources": {"4":"rss","11":"gateway"},
///    "mail":    {"<message-id>":["1","5"]}}
pub fn views_json(views: &[PhoneView], sources: &[(i64, String)]) -> String {
    use std::collections::BTreeMap;
    let mut mail: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for v in views {
        for mid in &v.mail {
            let keys = mail.entry(mid.as_str()).or_default();
            if !keys.contains(&v.key.as_str()) { keys.push(v.key.as_str()); }
        }
    }
    let list: Vec<serde_json::Value> = views.iter().map(|v| serde_json::json!({
        "key": v.key, "name": v.name, "colour": v.colour, "filters": v.filters,
    })).collect();
    let sources: BTreeMap<String, &str> =
        sources.iter().map(|(id, kind)| (id.to_string(), kind.as_str())).collect();
    serde_json::json!({ "views": list, "sources": sources, "mail": mail }).to_string()
}

/// The rows named in the phones' delete lists. A line is a Message-ID,
/// then anything after a space or tab (the phone's timestamp).
pub fn phone_deletes(rows: &[(i64, String, bool)], lists: &[String]) -> Vec<i64> {
    let wanted: std::collections::HashSet<&str> = lists.iter()
        .flat_map(|s| s.lines())
        .filter_map(|l| l.split_whitespace().next())
        .collect();
    if wanted.is_empty() { return Vec::new(); }
    rows.iter().filter(|(_, mid, _)| wanted.contains(mid.as_str())).map(|(id, _, _)| *id).collect()
}

pub struct Plan {
    /// Rows the other devices moved: (message row id, new read state).
    pub apply: Vec<(i64, bool)>,
    /// Whether our published file needs rewriting.
    pub changed: bool,
}

/// The whole decision, with no I/O in it.
///
/// `marks` is our published file and is updated in place. It is both
/// what we tell everyone else and our memory of *when* each state was
/// last set — the database has the states but not the times, and the
/// merge is decided on times.
///
/// `deleted` are Message-IDs of mail deleted here. They travel one way
/// only — published as read, never importable. A deletion is final on
/// this side, and letting a phone mark argue with it would flip the
/// state back and forth on every pass.
pub fn plan(
    rows: &[(i64, String, bool)],
    deleted: &[String],
    theirs: &Marks,
    marks: &mut Marks,
    now: i64,
) -> Plan {
    let mut apply = Vec::new();

    // Import: a mark newer than ours wins, in either direction. Their
    // timestamp is adopted even when we already agree, so agreeing does
    // not keep arriving as news on every pass.
    for (id, mid, db_read) in rows {
        let m = match theirs.get(mid) { Some(m) => *m, None => continue };
        if m.ts <= marks.get(mid).map(|x| x.ts).unwrap_or(i64::MIN) { continue; }
        if m.read != *db_read { apply.push((*id, m.read)); }
        marks.insert(mid.clone(), m);
    }

    // Export: our read column, stamped with the time it moved. Note
    // `effective` — a row we are about to update has to be compared
    // against what it is becoming, not what the query found. Comparing
    // against the stale value restamps the phone's own mark with `now`
    // and bounces it straight back.
    let pending: std::collections::HashMap<i64, bool> = apply.iter().copied().collect();
    let mut changed = !apply.is_empty();
    for (id, mid, db_read) in rows {
        let effective = *pending.get(id).unwrap_or(db_read);
        match marks.get(mid) {
            Some(m) if m.read == effective => {}
            _ => { marks.insert(mid.clone(), Mark { read: effective, ts: now }); changed = true; }
        }
    }

    // Deleted here means dealt with. One direction only, and it wins:
    // whatever anyone else thinks, this is not new mail.
    for mid in deleted {
        match marks.get(mid) {
            Some(m) if m.read => {}
            _ => { marks.insert(mid.clone(), Mark { read: true, ts: now }); changed = true; }
        }
    }

    // Nothing outside the window can still be argued about, so it is
    // dead weight. This is what keeps the file from growing for ever.
    let live: std::collections::HashSet<&str> =
        rows.iter().map(|(_, mid, _)| mid.as_str())
            .chain(deleted.iter().map(|m| m.as_str()))
            .collect();
    let before = marks.len();
    marks.retain(|k, _| live.contains(k.as_str()));
    if marks.len() != before { changed = true; }

    Plan { apply, changed }
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "laptop".to_string())
}

fn shellexpand_home(p: &str) -> String {
    match p.strip_prefix("~/") {
        Some(rest) => format!("{}/{}", crate::home_dir().display(), rest),
        None => p.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(v: &[(i64, &str, bool)]) -> Vec<(i64, String, bool)> {
        v.iter().map(|(i, m, r)| (*i, m.to_string(), *r)).collect()
    }
    fn marks(v: &[(&str, bool, i64)]) -> Marks {
        v.iter().map(|(m, r, t)| (m.to_string(), Mark { read: *r, ts: *t })).collect()
    }

    #[test]
    fn a_read_here_gets_published() {
        let mut mine = Marks::new();
        let p = plan(&rows(&[(1, "a@x", true)]), &[], &Marks::new(), &mut mine, 500);
        assert!(p.apply.is_empty(), "nobody else asked for anything");
        assert!(p.changed);
        assert_eq!(mine.get("a@x").map(|m| (m.read, m.ts)), Some((true, 500)));
    }

    #[test]
    fn a_phone_mark_reaches_the_database() {
        let mut mine = marks(&[("a@x", false, 100)]);
        let theirs = marks(&[("a@x", true, 200)]);
        let p = plan(&rows(&[(7, "a@x", false)]), &[], &theirs, &mut mine, 500);
        assert_eq!(p.apply, vec![(7, true)]);
        // Their timestamp is adopted, not restamped with `now`.
        assert_eq!(mine.get("a@x").map(|m| m.ts), Some(200));
    }

    #[test]
    fn a_state_we_already_publish_keeps_its_timestamp() {
        // The bug this guards: restamping every pass would make the
        // laptop win every argument for ever, and no phone mark could
        // ever land.
        let mut mine = marks(&[("a@x", true, 100)]);
        let p = plan(&rows(&[(1, "a@x", true)]), &[], &Marks::new(), &mut mine, 900);
        assert!(!p.changed);
        assert_eq!(mine.get("a@x").map(|m| m.ts), Some(100));
    }

    #[test]
    fn an_older_phone_mark_loses() {
        let mut mine = marks(&[("a@x", true, 300)]);
        let theirs = marks(&[("a@x", false, 100)]);
        let p = plan(&rows(&[(1, "a@x", true)]), &[], &theirs, &mut mine, 500);
        assert!(p.apply.is_empty());
        assert_eq!(mine.get("a@x").map(|m| (m.read, m.ts)), Some((true, 300)));
    }

    #[test]
    fn agreeing_settles_instead_of_repeating() {
        // Phone says read, we already are. Nothing to apply, but their
        // timestamp is taken so the next pass has nothing to do either.
        let mut mine = marks(&[("a@x", true, 100)]);
        let theirs = marks(&[("a@x", true, 200)]);
        let first = plan(&rows(&[(1, "a@x", true)]), &[], &theirs, &mut mine, 500);
        assert!(first.apply.is_empty());
        assert_eq!(mine.get("a@x").map(|m| m.ts), Some(200));
        let second = plan(&rows(&[(1, "a@x", true)]), &[], &theirs, &mut mine, 600);
        assert!(!second.changed, "settled");
    }

    #[test]
    fn what_falls_out_of_the_window_is_forgotten() {
        let mut mine = marks(&[("old@x", true, 1), ("a@x", true, 2)]);
        let p = plan(&rows(&[(1, "a@x", true)]), &[], &Marks::new(), &mut mine, 500);
        assert!(p.changed);
        assert_eq!(mine.len(), 1);
        assert!(mine.contains_key("a@x"));
    }

    #[test]
    fn only_other_devices_files_are_watched() {
        let dir = std::env::temp_dir().join(format!("kastrup-rs-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let host = hostname();
        std::fs::write(dir.join(format!("mail-read-{}.json", host)), "{}").unwrap();
        std::fs::write(dir.join("mail-read-phone.json"), r#"{"a@x":{"read":true,"ts":9}}"#).unwrap();
        std::fs::write(dir.join("ratings-phone.json"), "{}").unwrap();

        let mut rs = ReadSync::new(dir.to_str().unwrap(), 30).expect("folder exists");
        assert_eq!(rs.others.len(), 1, "our own file and unrelated files are not others");
        assert!(rs.others_changed(), "first look folds in what is already there");
        assert!(!rs.others_changed(), "and then goes quiet");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_phone_delete_list_names_rows_and_is_watched() {
        let r = rows(&[(1, "a@x", true), (2, "b@x", false), (3, "c@x", false)]);
        let lists = vec!["b@x\t1700000000\n\nmissing@x\t1\n".to_string(), "c@x\n".to_string()];
        assert_eq!(phone_deletes(&r, &lists), vec![2, 3]);
        assert!(phone_deletes(&r, &[]).is_empty());

        let dir = std::env::temp_dir().join(format!("kastrup-rd-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(dir.join("mail-delete-phone.txt"), "b@x\n").unwrap();
        let mut rs = ReadSync::new(dir.to_str().unwrap(), 30).expect("folder exists");
        assert_eq!(rs.drops.len(), 1);
        assert!(rs.others_changed(), "a new delete list is news");
        assert!(!rs.others_changed());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn no_folder_means_no_feature() {
        assert!(ReadSync::new("", 30).is_none());
        assert!(ReadSync::new("/nonexistent/kastrup/sync", 30).is_none());
    }

    #[test]
    fn deleting_here_shows_read_on_the_phone() {
        let mut mine = Marks::new();
        let p = plan(&[], &["gone@x".to_string()], &Marks::new(), &mut mine, 500);
        assert!(p.apply.is_empty(), "there is no row left to update");
        assert!(p.changed);
        assert_eq!(mine.get("gone@x").map(|m| m.read), Some(true));
    }

    #[test]
    fn a_deletion_cannot_be_argued_with() {
        // The phone marks a deleted message unread. Without the one-way
        // rule this flips on every pass: import adopts false, export
        // sees the deletion and writes true, forever.
        let mut mine = marks(&[("gone@x", true, 100)]);
        let theirs = marks(&[("gone@x", false, 900)]);
        let del = vec!["gone@x".to_string()];
        let first = plan(&[], &del, &theirs, &mut mine, 500);
        assert!(first.apply.is_empty());
        assert_eq!(mine.get("gone@x").map(|m| m.read), Some(true));
        let second = plan(&[], &del, &theirs, &mut mine, 600);
        assert!(!second.changed, "settled, not oscillating");
    }

    #[test]
    fn unread_again_travels_too() {
        let mut mine = marks(&[("a@x", true, 100)]);
        let theirs = marks(&[("a@x", false, 200)]);
        let p = plan(&rows(&[(3, "a@x", true)]), &[], &theirs, &mut mine, 500);
        assert_eq!(p.apply, vec![(3, false)]);
    }

    fn view(key: &str, mail: &[&str]) -> PhoneView {
        PhoneView {
            key: key.into(), name: format!("View {key}"), colour: "130".into(),
            filters: serde_json::json!({"rules": [{"field": "platform", "op": "=", "value": "sms"}]}),
            mail: mail.iter().map(|m| m.to_string()).collect(),
        }
    }

    #[test]
    fn the_views_file_names_each_mail_once_with_all_its_views() {
        let text = views_json(&[view("1", &["a@x", "b@x"]), view("3", &["a@x"])], &[(11, "gateway".into())]);
        let v: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(v["mail"]["a@x"], serde_json::json!(["1", "3"]));
        assert_eq!(v["mail"]["b@x"], serde_json::json!(["1"]));
        assert_eq!(v["views"][1]["key"], "3");
        assert_eq!(v["views"][0]["filters"]["rules"][0]["value"], "sms");
        assert_eq!(v["sources"]["11"], "gateway");
    }

    #[test]
    fn the_same_views_give_the_same_bytes() {
        // publish_views skips the write on equal text, so the text must
        // not depend on the order mail was found in.
        let a = views_json(&[view("1", &["b@x", "a@x"])], &[]);
        let b = views_json(&[view("1", &["a@x", "b@x"])], &[]);
        assert_eq!(a, b);
    }
}
