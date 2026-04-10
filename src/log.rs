use std::fs::OpenOptions;
use std::io::Write;
use std::sync::OnceLock;
use std::sync::Mutex;
use std::path::PathBuf;

static LOG_FILE: OnceLock<Mutex<PathBuf>> = OnceLock::new();

fn log_path() -> &'static Mutex<PathBuf> {
    LOG_FILE.get_or_init(|| {
        let home = std::env::var("HOME").unwrap_or_default();
        Mutex::new(PathBuf::from(format!("{}/.kastrup/kastrup.log", home)))
    })
}

fn timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Simple UTC timestamp
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86400;
    let y = 1970 + (days * 4 + 2) / 1461;
    let doy = days - (365 * (y - 1970) + (y - 1969) / 4);
    let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut mo = 0u64;
    let mut d = doy;
    for &md in &month_days {
        if d < md { break; }
        d -= md;
        mo += 1;
    }
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        y, mo + 1, d + 1, h, m, s)
}

pub fn log(level: &str, msg: &str) {
    let path = log_path().lock().unwrap();
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&*path) {
        let _ = writeln!(f, "[{}] {} {}", timestamp(), level, msg);
    }
}

pub fn info(msg: &str) { log("INFO", msg); }
pub fn warn(msg: &str) { log("WARN", msg); }
pub fn error(msg: &str) { log("ERROR", msg); }
