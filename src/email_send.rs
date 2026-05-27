//! Native-Rust SMTP send for Gmail via XOAUTH2.
//!
//! Replaces the shell-out to `~/bin/gmail_smtp` (Ruby + Python
//! `oauth2.py`). Two reasons to drop the subprocess path:
//!
//! 1. **No Ruby/Python in the kastrup pipeline.** Fits the Fe₂O₃
//!    "everything native Rust" goal and removes interpreter cold-
//!    start cost per send (~2-3 s combined Ruby + Python launch +
//!    libs).
//! 2. **Control over DNS resolution.** Gmail's IPv6 endpoints
//!    (smtp.gmail.com over AAAA) have been unreachable from this
//!    network repeatedly while IPv4 works fine. Ruby's `Net::SMTP`
//!    tries AAAA first and burns its 30 s `open_timeout` before
//!    falling back, producing user-visible 30 s freezes (well, used
//!    to — kastrup's send is on a worker thread now). This module
//!    resolves IPv4 only and connects to that directly, sub-second.
//!
//! Lookup convention matches the Ruby script's so existing OAuth
//! files keep working:
//!
//!   {safedir}/{from_email}.json  — Google client_secret (`web`)
//!   {safedir}/{from_email}.txt   — refresh_token (single line)
//!
//! Falls back to `{safedir}/{default}.json` if the per-email file
//! doesn't exist, same as the script.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD as B64};

const SMTP_HOST: &str = "smtp.gmail.com";
const SMTP_PORT: u16 = 465;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const IO_TIMEOUT: Duration = Duration::from_secs(60);
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const TOKEN_REFRESH_TIMEOUT: Duration = Duration::from_secs(15);

/// Default OAuth secret directory — matches the legacy
/// `~/bin/gmail_smtp` Ruby script's `$safedir`. Override-able when
/// kastrup grows a config field for it.
pub fn default_safedir() -> PathBuf {
    PathBuf::from("/home/.safe/mail")
}

/// Default "From" email used when the requested account's OAuth
/// files don't exist. Mirrors the script's `$default` fallback.
pub const DEFAULT_FROM: &str = "geir@isene.com";

/// High-level: send a complete RFC822 message from a Gmail OAuth
/// account. Returns `Ok(())` on a successful 250 to end-of-data.
///
/// `from_email` MUST be the bare address (no `Name <addr>` form);
/// caller is responsible for parsing the From header.
pub fn send_email_gmail(
    safedir: &Path,
    from_email: &str,
    recipients: &[String],
    eml_body: &[u8],
) -> Result<(), String> {
    if recipients.is_empty() {
        return Err("no recipients".to_string());
    }
    let (client_id, client_secret, refresh_token) =
        load_oauth_creds(safedir, from_email)?;
    let access_token = refresh_oauth_token(&client_id, &client_secret, &refresh_token)?;
    send_via_xoauth2(from_email, recipients, eml_body, &access_token)
}

/// Read `{safedir}/{from}.json` (Google client_secret `web` block)
/// and `{safedir}/{from}.txt` (refresh_token). Falls back to
/// `{safedir}/{DEFAULT_FROM}.json` when the per-email file is absent
/// — same lookup as the Ruby script so existing setups keep working.
fn load_oauth_creds(safedir: &Path, from: &str) -> Result<(String, String, String), String> {
    let mut json_path = safedir.join(format!("{}.json", from));
    if !json_path.exists() {
        json_path = safedir.join(format!("{}.json", DEFAULT_FROM));
    }
    let txt_path = json_path.with_extension("txt");

    let json_str = std::fs::read_to_string(&json_path)
        .map_err(|e| format!("read {}: {}", json_path.display(), e))?;
    let refresh_token = std::fs::read_to_string(&txt_path)
        .map_err(|e| format!("read {}: {}", txt_path.display(), e))?
        .trim().to_string();

    let creds: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| format!("parse {}: {}", json_path.display(), e))?;
    let web = creds.get("web")
        .ok_or_else(|| format!("{}: missing 'web' block", json_path.display()))?;
    let client_id = web.get("client_id").and_then(|v| v.as_str())
        .ok_or_else(|| format!("{}: missing client_id", json_path.display()))?
        .to_string();
    let client_secret = web.get("client_secret").and_then(|v| v.as_str())
        .ok_or_else(|| format!("{}: missing client_secret", json_path.display()))?
        .to_string();
    Ok((client_id, client_secret, refresh_token))
}

/// Exchange a refresh token for a fresh access token. Uses the
/// existing `ureq` dep (already pulled in for HTTP). IPv6 for
/// `oauth2.googleapis.com` has been fine on this network; if that
/// changes we'll need to swap to a manual IPv4-only HTTPS POST.
pub fn refresh_oauth_token(
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<String, String> {
    let resp = ureq::post(TOKEN_URL)
        .timeout(TOKEN_REFRESH_TIMEOUT)
        .send_form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("refresh_token", refresh_token),
            ("grant_type", "refresh_token"),
        ])
        .map_err(|e| format!("oauth POST: {}", e))?;
    // ureq 2 without the `json` feature → read body as string, parse
    // manually with serde_json (already pulled in).
    let body_str = resp.into_string()
        .map_err(|e| format!("oauth read body: {}", e))?;
    let body: serde_json::Value = serde_json::from_str(&body_str)
        .map_err(|e| format!("oauth parse: {} (body={})", e, body_str))?;
    body.get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("oauth: no access_token (body={})", body_str))
}

/// Manual SMTP+TLS exchange. Forces IPv4. Implements just enough
/// of RFC 5321 to authenticate via XOAUTH2 and ship a single
/// message: EHLO, AUTH XOAUTH2, MAIL FROM, one RCPT TO per
/// recipient, DATA, dot-stuffed body, QUIT.
fn send_via_xoauth2(
    from_email: &str,
    recipients: &[String],
    eml_body: &[u8],
    access_token: &str,
) -> Result<(), String> {
    // IPv4-only resolve — see module docstring for why.
    let addrs: Vec<SocketAddr> = (SMTP_HOST, SMTP_PORT)
        .to_socket_addrs()
        .map_err(|e| format!("resolve {}: {}", SMTP_HOST, e))?
        .filter(|a| a.is_ipv4())
        .collect();
    let addr = addrs.first()
        .ok_or_else(|| format!("no IPv4 address for {}", SMTP_HOST))?;

    let tcp = TcpStream::connect_timeout(addr, CONNECT_TIMEOUT)
        .map_err(|e| format!("tcp connect {}: {}", addr, e))?;
    tcp.set_read_timeout(Some(IO_TIMEOUT)).ok();
    tcp.set_write_timeout(Some(IO_TIMEOUT)).ok();
    tcp.set_nodelay(true).ok();

    let connector = native_tls::TlsConnector::new()
        .map_err(|e| format!("tls connector: {}", e))?;
    let tls = connector.connect(SMTP_HOST, tcp)
        .map_err(|e| format!("tls handshake: {}", e))?;

    let mut io = SmtpIo::new(tls);

    // Greeting (220)
    io.expect("220", "greeting")?;

    // EHLO. Hostname doesn't matter to Gmail's relay.
    io.write_line("EHLO kastrup.localhost")?;
    io.expect("250", "EHLO")?;

    // XOAUTH2 SASL: base64("user={email}\x01auth=Bearer {token}\x01\x01").
    let auth = format!("user={}\x01auth=Bearer {}\x01\x01", from_email, access_token);
    let auth_b64 = B64.encode(auth.as_bytes());
    io.write_line(&format!("AUTH XOAUTH2 {}", auth_b64))?;
    io.expect("235", "AUTH XOAUTH2")?;

    io.write_line(&format!("MAIL FROM:<{}>", from_email))?;
    io.expect("250", "MAIL FROM")?;

    for rcpt in recipients {
        io.write_line(&format!("RCPT TO:<{}>", rcpt))?;
        io.expect("250", "RCPT TO")?;
    }

    io.write_line("DATA")?;
    io.expect("354", "DATA")?;
    io.write_body(eml_body)?;
    io.expect("250", "end-of-data")?;

    // QUIT is best-effort; if the server already accepted the
    // message a failure here doesn't affect delivery.
    let _ = io.write_line("QUIT");
    let _ = io.expect("221", "QUIT");
    let _ = io.into_inner().shutdown();
    Ok(())
}

/// Owns the TLS stream and a BufReader on a clone of it, so we can
/// alternate command-write and response-read without fighting the
/// borrow checker on a single `&mut TlsStream`.
struct SmtpIo {
    writer: native_tls::TlsStream<TcpStream>,
    // Reader holds a separate BufReader created from the same
    // stream via a re-wrap; we can't try_clone the TlsStream itself.
    // Instead, route all reads through `&mut self.writer` and own
    // an internal byte buffer for line accumulation.
    buf: Vec<u8>,
}
impl SmtpIo {
    fn new(tls: native_tls::TlsStream<TcpStream>) -> Self {
        Self { writer: tls, buf: Vec::with_capacity(512) }
    }
    fn into_inner(self) -> native_tls::TlsStream<TcpStream> { self.writer }

    fn write_line(&mut self, line: &str) -> Result<(), String> {
        self.writer.write_all(line.as_bytes())
            .map_err(|e| format!("smtp write: {}", e))?;
        self.writer.write_all(b"\r\n")
            .map_err(|e| format!("smtp write CRLF: {}", e))?;
        Ok(())
    }

    /// Send the .eml body with CRLF line endings and dot-stuffing
    /// (RFC 5321 §4.5.2). Terminates with the canonical `\r\n.\r\n`.
    fn write_body(&mut self, eml: &[u8]) -> Result<(), String> {
        // Iterate over LF-separated lines, stripping any trailing CR
        // so we get exactly one CRLF terminator per line.
        let s = std::str::from_utf8(eml).unwrap_or("");
        for line in s.split('\n') {
            let line = line.trim_end_matches('\r');
            if line.starts_with('.') {
                self.writer.write_all(b".")
                    .map_err(|e| format!("smtp dot-stuff: {}", e))?;
            }
            self.writer.write_all(line.as_bytes())
                .map_err(|e| format!("smtp body: {}", e))?;
            self.writer.write_all(b"\r\n")
                .map_err(|e| format!("smtp body CRLF: {}", e))?;
        }
        self.writer.write_all(b".\r\n")
            .map_err(|e| format!("smtp data terminator: {}", e))?;
        Ok(())
    }

    /// Read one SMTP response (possibly multi-line, "250-…\r\n"
    /// continuations followed by a final "250 …\r\n") and return it.
    fn read_response(&mut self) -> Result<String, String> {
        self.buf.clear();
        let mut byte = [0u8; 1];
        loop {
            self.writer.read_exact(&mut byte)
                .map_err(|e| format!("smtp read: {}", e))?;
            self.buf.push(byte[0]);
            // Each line ends with CRLF. Check whether we just
            // finished a line whose 4th char is space (terminal) or
            // dash (continuation).
            let n = self.buf.len();
            if n >= 2 && self.buf[n-2] == b'\r' && self.buf[n-1] == b'\n' {
                // Find the start of the current line by walking back
                // past the previous CRLF (or to the start of buf).
                let line_start = self.buf[..n-2].iter()
                    .rposition(|&b| b == b'\n')
                    .map(|p| p + 1)
                    .unwrap_or(0);
                let line = &self.buf[line_start..n-2];
                if line.len() >= 4 && line[3] == b' ' {
                    break;
                }
            }
        }
        String::from_utf8(self.buf.clone())
            .map_err(|e| format!("smtp resp utf8: {}", e))
    }

    fn expect(&mut self, prefix: &str, stage: &str) -> Result<String, String> {
        let resp = self.read_response()?;
        if resp.starts_with(prefix) {
            Ok(resp)
        } else {
            // Trim to first line for the error — usually all we need.
            let first = resp.lines().next().unwrap_or("").trim();
            Err(format!("{}: expected {}, got: {}", stage, prefix, first))
        }
    }
}

