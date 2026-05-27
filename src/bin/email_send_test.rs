//! Standalone test driver for the native SMTP send path. Sends a
//! tiny test message from the user's default Gmail account to
//! themselves so the end-to-end XOAUTH2 + rustls + SMTP plumbing
//! can be verified without bringing up the full kastrup TUI.
//!
//! Usage:
//!   cargo run --release --bin email_send_test [from] [to]
//!
//! Defaults to from=geir@isene.com, to=same. Writes the result to
//! stdout and exits 0 on success, 1 on failure (with the error
//! message on stderr).

#[path = "../email_send.rs"]
mod email_send;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let from = args.first().cloned().unwrap_or_else(|| "geir@isene.com".to_string());
    let to = args.get(1).cloned().unwrap_or_else(|| from.clone());

    let rfc822 = format!(
        "From: {}\r\n\
         To: {}\r\n\
         Subject: kastrup native-SMTP self-test\r\n\
         MIME-Version: 1.0\r\n\
         Content-Type: text/plain; charset=UTF-8\r\n\
         \r\n\
         Hello from kastrup's native Rust SMTP path.\r\n\
         If you see this, XOAUTH2 + rustls + the IPv4-only resolve worked.\r\n\
         \r\n\
         Sent at: {}\r\n",
        from, to,
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs()).unwrap_or(0),
    );

    let safedir = email_send::default_safedir();
    eprintln!("[+] from: {}", from);
    eprintln!("[+] to:   {}", to);
    eprintln!("[+] safedir: {}", safedir.display());
    eprintln!("[+] sending {} bytes...", rfc822.len());

    let t = std::time::Instant::now();
    match email_send::send_email_gmail(&safedir, &from, &[to.clone()], rfc822.as_bytes()) {
        Ok(()) => {
            eprintln!("[+] SENT in {} ms", t.elapsed().as_millis());
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("[!] FAILED in {} ms: {}", t.elapsed().as_millis(), e);
            std::process::exit(1);
        }
    }
}
