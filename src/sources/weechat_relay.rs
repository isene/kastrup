//! Weechat relay-protocol client (M1: handshake + buffer list).
//!
//! Replaces the legacy log-tailing `weechat.rs` source over the
//! next few milestones. M1 only proves the wire is alive: connect,
//! handshake, init, fetch the buffer list, print it. No background
//! thread, no DB write, no UI integration yet — those land in M2+.
//!
//! Wire format reference: <https://weechat.org/doc/weechat/relay/>
//!
//! Each protocol message looks like:
//!   `[len:u32 BE] [compression:u8] [id-field] [object...]`
//! where the `id-field` is itself a `str` (4-byte len + bytes), so
//! every message starts with at least 5 bytes of header (length +
//! compression flag) followed by an empty-or-string ID. Objects are
//! tagged with a 3-byte ASCII type code.
//!
//! Object types we handle in M1: `chr`, `int`, `lon`, `str`, `buf`,
//! `ptr`, `tim`, `htb`, `hda`. The rest (`inf`, `inl`, `arr`) we
//! decode-and-discard so an unexpected reply doesn't desync the
//! stream — they appear in later milestones.
//!
//! Auth: secrets come from `~/.kastrup/.env`
//!   WEECHAT_RELAY_HOST · WEECHAT_RELAY_PORT · WEECHAT_RELAY_PASSWORD

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration    = Duration::from_secs(20);

// Keepalive params for the M5 long-lived push connection. With these
// the kernel detects a dead peer in ~30 + 4×15 = 90s. Default kernel
// values (7200/75/9 ≈ 2h) would leave kastrup waiting hours after a
// laptop suspend/resume — way past useful.
const PUSH_KEEPALIVE_IDLE:     Duration = Duration::from_secs(30);
const PUSH_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const PUSH_KEEPALIVE_RETRIES:  u32      = 4;

// ---------------------------------------------------------------------------
// Protocol object model
// ---------------------------------------------------------------------------

/// One typed value off the wire. We carry enough variants to fully
/// represent every shape M1 reads back; later milestones add nothing
/// new here, just more `Hdata` consumers.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Object {
    Char(u8),
    Int(i32),
    Long(i64),
    Str(Option<String>),       // None for null, Some("") for empty
    Buf(Option<Vec<u8>>),
    Ptr(String),               // hex pointer like "0x7f12..." or "0" (null)
    Time(i64),
    Hashtable(Vec<(Object, Object)>),
    Hdata(Hdata),
    Array(Vec<Object>),
}

/// hda — a list of items each carrying the same set of named fields.
/// `path` is a slash-separated list of hdata type names (e.g.
/// `"buffer"` or `"buffer/own_lines/last_line/data"`); the per-item
/// `ptrs` mirror that path. `keys` defines the field name + type for
/// each entry in `items`.
#[derive(Debug, Clone)]
pub struct Hdata {
    pub path: String,
    pub keys: Vec<(String, String)>,   // (name, 3-char type)
    pub items: Vec<HdataItem>,
}

#[derive(Debug, Clone)]
pub struct HdataItem {
    pub ptrs: Vec<String>,
    pub fields: std::collections::BTreeMap<String, Object>,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Cursor over a single message body (after length + compression
/// flag have been peeled off). Cheap; everything is index-based.
struct Cursor<'a> { buf: &'a [u8], pos: usize }

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self { Self { buf, pos: 0 } }
    fn remaining(&self) -> usize { self.buf.len() - self.pos }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.remaining() < n {
            return Err(format!("short read: want {} have {}", n, self.remaining()));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, String> { Ok(self.take(1)?[0]) }
    fn i32(&mut self) -> Result<i32, String> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u32(&mut self) -> Result<u32, String> { Ok(self.i32()? as u32) }

    /// Weechat str: 4-byte length, then bytes. Length −1 = null,
    /// 0 = empty string.
    fn str(&mut self) -> Result<Option<String>, String> {
        let len = self.i32()?;
        if len < 0 { return Ok(None); }
        let bytes = self.take(len as usize)?;
        Ok(Some(String::from_utf8_lossy(bytes).into_owned()))
    }

    /// Weechat buf: same as str but the payload is opaque bytes.
    fn buf(&mut self) -> Result<Option<Vec<u8>>, String> {
        let len = self.i32()?;
        if len < 0 { return Ok(None); }
        Ok(Some(self.take(len as usize)?.to_vec()))
    }

    /// Weechat ptr: 1-byte length, then ASCII hex digits. "0" is
    /// the null pointer. We prepend "0x" so the value is convenient
    /// to use later as the address parameter in subsequent commands.
    fn ptr(&mut self) -> Result<String, String> {
        let len = self.u8()? as usize;
        let bytes = self.take(len)?;
        let hex = std::str::from_utf8(bytes).map_err(|e| format!("ptr utf8: {}", e))?;
        if hex == "0" { Ok("0".to_string()) }
        else { Ok(format!("0x{}", hex)) }
    }

    /// Weechat lon: 1-byte length, then ASCII decimal digits.
    fn lon(&mut self) -> Result<i64, String> {
        let len = self.u8()? as usize;
        let bytes = self.take(len)?;
        let s = std::str::from_utf8(bytes).map_err(|e| format!("lon utf8: {}", e))?;
        s.parse::<i64>().map_err(|e| format!("lon parse: {}", e))
    }

    /// Read a 3-byte type tag.
    fn type_tag(&mut self) -> Result<[u8; 3], String> {
        let b = self.take(3)?;
        Ok([b[0], b[1], b[2]])
    }
}

fn parse_object(c: &mut Cursor) -> Result<Object, String> {
    let tag = c.type_tag()?;
    parse_object_typed(c, &tag)
}

/// Decode an object of a known type — used inside htb/hda where the
/// type tag is given once for the whole list rather than per item.
fn parse_object_typed(c: &mut Cursor, tag: &[u8; 3]) -> Result<Object, String> {
    match tag {
        b"chr" => Ok(Object::Char(c.u8()?)),
        b"int" => Ok(Object::Int(c.i32()?)),
        b"lon" => Ok(Object::Long(c.lon()?)),
        b"str" => Ok(Object::Str(c.str()?)),
        b"buf" => Ok(Object::Buf(c.buf()?)),
        b"ptr" => Ok(Object::Ptr(c.ptr()?)),
        b"tim" => Ok(Object::Time(c.lon()?)),
        b"htb" => {
            let kt = c.type_tag()?;
            let vt = c.type_tag()?;
            let count = c.u32()? as usize;
            let mut pairs = Vec::with_capacity(count);
            for _ in 0..count {
                let k = parse_object_typed(c, &kt)?;
                let v = parse_object_typed(c, &vt)?;
                pairs.push((k, v));
            }
            Ok(Object::Hashtable(pairs))
        }
        b"hda" => parse_hdata(c).map(Object::Hdata),
        b"arr" => {
            let inner = c.type_tag()?;
            let count = c.u32()? as usize;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_object_typed(c, &inner)?);
            }
            Ok(Object::Array(items))
        }
        // inf / inl are rare and only appear in milestones past M1.
        // Return a friendly error so we notice if weechat ever
        // surprises us with one.
        other => Err(format!(
            "unsupported object type {:?}",
            std::str::from_utf8(other).unwrap_or("?")
        )),
    }
}

fn parse_hdata(c: &mut Cursor) -> Result<Hdata, String> {
    let path = c.str()?.unwrap_or_default();
    let keys_csv = c.str()?.unwrap_or_default();
    let count = c.u32()? as usize;
    let path_depth = path.matches('/').count() + 1;
    let keys: Vec<(String, String)> = keys_csv.split(',')
        .filter(|s| !s.is_empty())
        .filter_map(|kv| {
            let mut it = kv.splitn(2, ':');
            Some((it.next()?.to_string(), it.next()?.to_string()))
        })
        .collect();
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let mut ptrs = Vec::with_capacity(path_depth);
        for _ in 0..path_depth {
            ptrs.push(c.ptr()?);
        }
        let mut fields = std::collections::BTreeMap::new();
        for (name, ty) in &keys {
            let mut tag = [0u8; 3];
            let tb = ty.as_bytes();
            if tb.len() < 3 {
                return Err(format!("bad type tag for {}: {:?}", name, ty));
            }
            tag.copy_from_slice(&tb[..3]);
            fields.insert(name.clone(), parse_object_typed(c, &tag)?);
        }
        items.push(HdataItem { ptrs, fields });
    }
    Ok(Hdata { path, keys, items })
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

#[derive(Debug)]
#[allow(dead_code)]
pub struct ServerHandshake {
    pub password_hash_algo: String,
    pub password_hash_iterations: u32,
    pub nonce: String,
    pub totp: bool,
    pub compression: String,
}

pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    /// Open a plain TCP connection to a weechat relay (no TLS in
    /// M1). Keepalives + read timeout are set up so a network glitch
    /// doesn't hang the caller forever.
    pub fn connect(host: &str, port: u16) -> Result<Self, String> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect_timeout(
            &addr.to_socket_addrs_first()?,
            CONNECT_TIMEOUT,
        ).map_err(|e| format!("connect {}: {}", addr, e))?;
        stream.set_read_timeout(Some(READ_TIMEOUT))
            .map_err(|e| format!("read timeout: {}", e))?;
        stream.set_write_timeout(Some(READ_TIMEOUT))
            .map_err(|e| format!("write timeout: {}", e))?;
        stream.set_nodelay(true).ok();
        Ok(Self { stream })
    }

    /// Open a TCP connection sized for the M5 push loop: kernel TCP
    /// keepalives (30s idle / 15s interval / 4 retries) so a silent
    /// drop surfaces in ~90s, and a long read timeout so a healthy
    /// idle channel doesn't accidentally tear down. Used by
    /// `run_supervised`.
    pub fn connect_for_push(host: &str, port: u16) -> Result<Self, String> {
        use socket2::{Domain, SockAddr, Socket, TcpKeepalive, Type};
        let sock_addr = format!("{}:{}", host, port).to_socket_addrs_first()?;
        let socket = Socket::new(Domain::for_address(sock_addr), Type::STREAM, None)
            .map_err(|e| format!("socket: {}", e))?;
        socket.set_nonblocking(false).ok();
        socket.connect_timeout(&SockAddr::from(sock_addr), CONNECT_TIMEOUT)
            .map_err(|e| format!("connect {}: {}", sock_addr, e))?;
        socket.set_tcp_keepalive(
            &TcpKeepalive::new()
                .with_time(PUSH_KEEPALIVE_IDLE)
                .with_interval(PUSH_KEEPALIVE_INTERVAL)
                .with_retries(PUSH_KEEPALIVE_RETRIES),
        ).map_err(|e| format!("keepalive: {}", e))?;
        // NO read timeout: chat lulls of 6+ minutes are normal and a
        // userspace timer would just churn reconnects every few minutes.
        // Kernel keepalives are the disconnect detector — ~90s after a
        // real drop the next read() returns ECONNRESET and the
        // supervisor reconnects.
        socket.set_read_timeout(None)
            .map_err(|e| format!("read timeout: {}", e))?;
        socket.set_write_timeout(Some(Duration::from_secs(60)))
            .map_err(|e| format!("write timeout: {}", e))?;
        socket.set_nodelay(true).ok();
        Ok(Self { stream: TcpStream::from(socket) })
    }

    /// Send a plain-text relay command. Weechat reads commands as
    /// LF-terminated UTF-8 — the binary protocol only kicks in for
    /// server → client traffic.
    fn send_cmd(&mut self, cmd: &str) -> Result<(), String> {
        self.stream.write_all(cmd.as_bytes()).map_err(|e| format!("write: {}", e))?;
        if !cmd.ends_with('\n') {
            self.stream.write_all(b"\n").map_err(|e| format!("write: {}", e))?;
        }
        Ok(())
    }

    /// Read exactly `n` bytes or error.
    fn read_exact(&mut self, n: usize) -> Result<Vec<u8>, String> {
        let mut buf = vec![0u8; n];
        self.stream.read_exact(&mut buf).map_err(|e| format!("read: {}", e))?;
        Ok(buf)
    }

    /// Pull one server message off the wire. Returns `(id, objects)`.
    /// `id` is whatever string the caller put in parens before its
    /// command (empty for unsolicited push events).
    pub fn read_message(&mut self) -> Result<(String, Vec<Object>), String> {
        let header = self.read_exact(4)?;
        let total = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if total < 5 {
            return Err(format!("impossibly short message: total={}", total));
        }
        let body = self.read_exact(total - 4)?;
        let compression = body[0];
        let payload = if compression == 0 {
            body[1..].to_vec()
        } else {
            // M1 always negotiates compression=off, so seeing this
            // flag set is a server-side surprise. Bail loud rather
            // than silently mis-parse.
            return Err("server enabled compression but we asked for off".to_string());
        };
        let mut c = Cursor::new(&payload);
        let id = c.str()?.unwrap_or_default();
        let mut objects = Vec::new();
        while c.remaining() > 0 {
            objects.push(parse_object(&mut c)?);
        }
        Ok((id, objects))
    }

    /// Initial handshake: ask the server to use plain auth + no
    /// compression. M1 keeps both simple; later milestones can
    /// upgrade to pbkdf2+sha256 + zlib once the wire works.
    pub fn handshake(&mut self) -> Result<ServerHandshake, String> {
        self.send_cmd("(hs) handshake password_hash_algo=plain,compression=off")?;
        let (_id, objs) = self.read_message()?;
        let pairs = match objs.into_iter().next() {
            Some(Object::Hashtable(p)) => p,
            other => return Err(format!("expected htb, got {:?}", other)),
        };
        let mut algo = String::from("plain");
        let mut iters = 0u32;
        let mut nonce = String::new();
        let mut totp = false;
        let mut compr = String::from("off");
        for (k, v) in pairs {
            let key = match k {
                Object::Str(Some(s)) => s,
                _ => continue,
            };
            let val = match v {
                Object::Str(Some(s)) => s,
                _ => continue,
            };
            match key.as_str() {
                "password_hash_algo"       => algo = val,
                "password_hash_iterations" => iters = val.parse().unwrap_or(0),
                "nonce"                    => nonce = val,
                "totp"                     => totp = val == "on",
                "compression"              => compr = val,
                _ => {}
            }
        }
        Ok(ServerHandshake {
            password_hash_algo: algo, password_hash_iterations: iters,
            nonce, totp, compression: compr,
        })
    }

    /// Authenticate. Plain-text password for now; pbkdf2/sha256
    /// arrives in M5 alongside the supervised reconnect.
    pub fn init_plain(&mut self, password: &str) -> Result<(), String> {
        let cmd = format!("init password={},compression=off", password);
        self.send_cmd(&cmd)?;
        // `init` is silent on success; the next request gets the
        // first observable response.
        Ok(())
    }

    /// Fetch every buffer with the keys we want for the M3 tree
    /// (number / full_name / short_name / title / type).
    pub fn list_buffers(&mut self) -> Result<Hdata, String> {
        self.send_cmd("(buffers) hdata buffer:gui_buffers(*) number,full_name,short_name,title,type")?;
        let (_id, objs) = self.read_message()?;
        match objs.into_iter().next() {
            Some(Object::Hdata(h)) => Ok(h),
            other => Err(format!("expected hda, got {:?}", other)),
        }
    }

    /// Fetch the last `n` lines of a buffer. `buffer_ptr` is the
    /// "0x…" hex string from `list_buffers()` (the `ptrs[0]` of each
    /// HdataItem). Returned hdata has `path` like `"lines/line/data"`
    /// and one item per line, oldest first.
    ///
    /// `last_line(-N)/data` is weechat's compact "go back N lines"
    /// syntax: start at the most recent line, walk `prev_line` N-1
    /// times, then drill into the `data` struct that carries the
    /// renderable fields (date / prefix / message / tags / etc.).
    pub fn last_lines(&mut self, buffer_ptr: &str, n: u32) -> Result<Hdata, String> {
        let cmd = format!(
            "(lines) hdata buffer:{}/own_lines/last_line(-{})/data \
             date,date_printed,prefix,message,displayed,highlight,tags_array",
            buffer_ptr, n);
        self.send_cmd(&cmd)?;
        let (_id, objs) = self.read_message()?;
        match objs.into_iter().next() {
            Some(Object::Hdata(h)) => Ok(h),
            other => Err(format!("expected hda, got {:?}", other)),
        }
    }

    /// Subscribe to live events for one buffer. After this returns
    /// the server will start pushing `_buffer_line_added` /
    /// `_buffer_closing` / `_nicklist_diff` etc. messages whenever
    /// state changes — read them with `read_message()`.
    ///
    /// Pass `"*"` to sync all buffers at once. The "buffers,upgrade"
    /// signal filter limits the noise to events that actually change
    /// what the user sees.
    pub fn sync(&mut self, buffer_ptr: &str) -> Result<(), String> {
        let cmd = format!("sync {} buffers,upgrade", buffer_ptr);
        self.send_cmd(&cmd)
    }

    /// Stop receiving push events for a buffer (or `"*"` for all).
    /// Not used in M1/M2 yet but handy for the supervisor in M5.
    #[allow(dead_code)]
    pub fn desync(&mut self, buffer_ptr: &str) -> Result<(), String> {
        self.send_cmd(&format!("desync {}", buffer_ptr))
    }

    /// Fetch nick lists for every buffer in one shot. Response arrives
    /// as a single hdata message with id `"nicks"`. M6: feeds the
    /// supervisor's shared `nick_lists` map so future @-completion has
    /// data to work with.
    pub fn nicklist_all(&mut self) -> Result<(), String> {
        self.send_cmd("(nicks) nicklist")
    }

    /// Type `text` into a buffer addressed by `full_name` (the same
    /// dotted name kastrup uses as the folder, e.g.
    /// `python.slack.<workspace>.#general` or `irc.<net>.#channel`).
    ///
    /// `input` is weechat's "as if you typed it at the prompt"
    /// command — newlines in `text` become separate posts; that's
    /// usually what the user wants for multi-line markdown blocks
    /// in Slack, and matches the behaviour of weechat-android.
    ///
    /// The relay's input command targets a buffer by name OR by
    /// pointer. Names are friendlier because they survive weechat
    /// restarts (pointers don't).
    pub fn input_by_name(&mut self, full_name: &str, text: &str) -> Result<(), String> {
        if text.is_empty() {
            return Err("empty body".to_string());
        }
        let cmd = format!("input {} {}\n", full_name, text);
        self.send_cmd(&cmd)
    }
}

// ---------------------------------------------------------------------------
// M3 — sync_weechat_relay() as a kastrup source plugin
// ---------------------------------------------------------------------------

use std::collections::HashSet;
use crate::sources::MessageData;

/// Poll-based sync: connect, fetch last N lines from every "real"
/// buffer (Slack channels + DMs, IRC channels, Matrix rooms, …),
/// disconnect. Each buffer becomes a `folder` in the kastrup DB so
/// the user can pick it from the view list like any maildir folder.
///
/// M5 swaps this implementation for a long-lived background
/// connection. M3 is intentionally simple: per-tick fetch, no
/// shared state.
pub fn sync_weechat_relay(_config: &serde_json::Value, known_ids: &HashSet<String>) -> Vec<MessageData> {
    let secrets = load_relay_secrets();
    let (Some(host), Some(port), Some(pass)) =
        (secrets.host, secrets.port, secrets.password) else { return Vec::new(); };

    let mut conn = match Connection::connect(&host, port) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    if conn.handshake().is_err() { return Vec::new(); }
    if conn.init_plain(&pass).is_err() { return Vec::new(); }
    let buffers = match conn.list_buffers() {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };

    // Per-buffer line budget: last 30 lines on the first fetch
    // (when known_ids is small), 10 thereafter. That's the
    // difference between "first launch of the day shows useful
    // backlog" and "subsequent ticks only pick up new traffic".
    let backlog = if known_ids.len() < 50 { 30 } else { 10 };

    let mut out: Vec<MessageData> = Vec::new();
    for buf in &buffers.items {
        let full_name = match buf.fields.get("full_name") {
            Some(Object::Str(Some(s))) => s.clone(),
            _ => continue,
        };
        if is_uninteresting_buffer(&full_name) { continue; }
        let buf_ptr = match buf.ptrs.first() {
            Some(p) if p != "0" => p.clone(),
            _ => continue,
        };
        let short_name = match buf.fields.get("short_name") {
            Some(Object::Str(Some(s))) if !s.is_empty() => s.clone(),
            _ => full_name.split('.').next_back().unwrap_or(&full_name).to_string(),
        };
        let lines = match conn.last_lines(&buf_ptr, backlog) {
            Ok(h) => h,
            Err(_) => continue,
        };
        let (platform, label) = classify_buffer(&full_name);
        for line in &lines.items {
            let date = match line.fields.get("date") {
                Some(Object::Time(t)) if *t > 0 => *t,
                _ => continue,
            };
            let prefix_raw = match line.fields.get("prefix") {
                Some(Object::Str(Some(s))) => s.as_str(), _ => "",
            };
            let message_raw = match line.fields.get("message") {
                Some(Object::Str(Some(s))) => s.as_str(), _ => "",
            };
            // `displayed=0` means weechat's filter plugin is hiding
            // the line from the user's terminal (typically smart-
            // filter for joins/parts on busy IRC channels). Mirror
            // that decision into kastrup so the inbox tracks what
            // the user actually sees on the weechat side.
            let displayed = matches!(line.fields.get("displayed"), Some(Object::Char(1)));
            if !displayed { continue; }
            // Defensive: also reject explicit `-->` / `<--` etc.
            // prefixes in case the filter plugin isn't loaded for
            // a given buffer.
            if is_system_prefix(prefix_raw) { continue; }

            let nick = strip_codes(prefix_raw);
            let nick = nick.trim().to_string();
            if nick.is_empty() && message_raw.is_empty() { continue; }
            let message = strip_codes(message_raw);

            // Dedup by stable (buffer, date, nick, message-head) hash
            // — line pointers reset on weechat restart, so a content-
            // based external_id keeps the DB consistent across
            // server bounces.
            let hash_input = format!("{}\t{}\t{}\t{}",
                full_name, date, nick,
                &message.get(..message.len().min(80)).unwrap_or(""));
            let ext_id = format!("weechat-relay_{}", md5_hex(&hash_input));
            if known_ids.contains(&ext_id) { continue; }

            let subject = {
                let line0 = message.lines().next().unwrap_or(&message).trim();
                let s: String = line0.chars().take(80).collect();
                if line0.chars().count() > 80 { format!("{}…", s) } else { s }
            };

            out.push(MessageData {
                external_id: ext_id,
                sender: nick.clone(),
                sender_name: Some(if nick.is_empty() { "system".into() } else { nick.clone() }),
                recipients: short_name.clone(),
                cc: None,
                bcc: None,
                subject: Some(subject),
                content: message,
                html_content: None,
                timestamp: date,
                labels: vec![label.to_string()],
                attachments: Vec::new(),
                metadata: serde_json::json!({
                    "buffer":   full_name,
                    "platform": platform,
                    "source_type": "weechat-relay",
                }),
                folder: Some(full_name.clone()),
                thread_id: None,
            });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// M5 — persistent push connection with supervisor + backoff
// ---------------------------------------------------------------------------

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Per-buffer nick set, keyed by buffer `full_name`. Built up from
/// the initial `nicklist` response and kept current by
/// `_nicklist_diff` events. Shared between the supervisor thread
/// (writer) and the main App thread (future @-completion reader).
pub type NickLists = Arc<Mutex<HashMap<String, BTreeSet<String>>>>;

/// One subscribed weechat buffer the relay is tracking. Mirrored
/// into App state by the supervisor so the Folders view can render
/// every subscribed channel as a section, including ones with no
/// messages yet.
#[derive(Clone)]
pub struct SubscribedBuffer {
    pub full_name: String,
    pub short_name: String,
}

/// Snapshot of the supervisor's current buffer registry. Held under
/// a mutex; readers (rebuild_display) take a quick clone so they
/// don't block the supervisor thread.
pub type SubscribedBuffers = Arc<Mutex<Vec<SubscribedBuffer>>>;

/// Per-buffer metadata cached on the supervisor thread. The `ptr` ←
/// `full_name` mapping is what lets us route `_buffer_line_added`
/// events (which only carry the buffer pointer) into the correct DB
/// folder.
struct BufferMeta {
    full_name: String,
    short_name: String,
    interesting: bool,   // false → drop events without DB work
}

/// Build a MessageData from a single line hdata item. Returns None if
/// the line is filtered (system prefix, undisplayed, empty) or if
/// it's already in `known_ids` so we don't double-insert on a
/// reconnect backfill.
fn line_to_message(
    line: &HdataItem,
    meta: &BufferMeta,
    known_ids: &mut std::collections::HashSet<String>,
) -> Option<MessageData> {
    let date = match line.fields.get("date") {
        Some(Object::Time(t)) if *t > 0 => *t,
        _ => return None,
    };
    let prefix_raw = match line.fields.get("prefix") {
        Some(Object::Str(Some(s))) => s.as_str(), _ => "",
    };
    let message_raw = match line.fields.get("message") {
        Some(Object::Str(Some(s))) => s.as_str(), _ => "",
    };
    let displayed = matches!(line.fields.get("displayed"), Some(Object::Char(1)));
    if !displayed { return None; }
    // Drop join/part/quit/nick-change lines via weechat's own tag
    // metadata — more reliable than glyph-matching the prefix
    // column. We keep `is_system_prefix` as a fallback for plugins
    // that don't tag these (rare).
    if tags_contain_any(line.fields.get("tags_array"),
        &["irc_join", "irc_part", "irc_quit", "irc_nick",
          "irc_mode", "irc_topic", "irc_kick", "irc_invite",
          "slack_join", "slack_leave"])
    {
        return None;
    }
    if is_system_prefix(prefix_raw) { return None; }

    // Prefer the stripped prefix — wee-slack normally puts the human-
    // readable display nick there for channel messages (and IRC /
    // Matrix always do). Only fall back to the canonical nick from
    // `tags_array` when the prefix collapses to a continuation marker
    // (`` `-> ``), which wee-slack uses for multi-line bot posts and
    // thread replies under the same author. `nick_*` tag values get
    // a `_<color>` prefix from wee-slack's colour palette — strip that
    // so `_16alice` shows as `alice`.
    let nick_from_prefix = strip_codes(prefix_raw).trim().to_string();
    let nick = if !nick_from_prefix.is_empty() && !is_continuation_marker(&nick_from_prefix) {
        nick_from_prefix
    } else {
        nick_from_tags(line.fields.get("tags_array"))
            .map(|n| strip_weeslack_color_prefix(&n))
            .unwrap_or_default()
    };
    if nick.is_empty() && message_raw.is_empty() { return None; }
    let mut message = strip_codes(message_raw);

    // `/me` actions: weechat tags these `irc_action` / `slack_action`.
    // The relay prefix column already shows `* nick`, but the
    // continuation-marker branch may have lost the nick. Normalise so
    // the message body itself reads `* nick text` and the sender
    // column stays informative.
    let tags = line.fields.get("tags_array");
    if tags_has(tags, "irc_action") || tags_has(tags, "slack_action") {
        message = format!("* {} {}", nick, message);
    }

    // Dedup hash: (folder, date, message-head). Deliberately NO nick —
    // a future tweak to nick-recovery shouldn't re-insert old lines as
    // duplicates. Two distinct authors posting different messages in
    // the same channel in the same second is rare enough that the
    // 80-char message head disambiguates them.
    //
    // ALSO check the legacy hash (with nick) — DB rows inserted before
    // the formula change use that. Without this back-compat check, the
    // first reconnect after upgrade would re-insert every cached line
    // as a fresh row under the new hash.
    let msg_head = message.get(..message.len().min(80)).unwrap_or("");
    let new_hash = format!("weechat-relay_{}",
        md5_hex(&format!("{}\t{}\t{}", meta.full_name, date, msg_head)));
    let legacy_hash = format!("weechat-relay_{}",
        md5_hex(&format!("{}\t{}\t{}\t{}", meta.full_name, date, nick, msg_head)));
    if known_ids.contains(&legacy_hash) || known_ids.contains(&new_hash) {
        return None;
    }
    let ext_id = new_hash;
    known_ids.insert(ext_id.clone());

    let subject = {
        let line0 = message.lines().next().unwrap_or(&message).trim();
        let s: String = line0.chars().take(80).collect();
        if line0.chars().count() > 80 { format!("{}…", s) } else { s }
    };
    let (platform, label) = classify_buffer(&meta.full_name);

    // `highlight=1` means weechat flagged the line as a mention/ping
    // for the user. Captured in metadata so the renderer can light
    // up the section header / top bar with a `!` badge.
    let highlight = matches!(line.fields.get("highlight"), Some(Object::Char(1)));

    Some(MessageData {
        external_id: ext_id,
        sender: nick.clone(),
        sender_name: Some(if nick.is_empty() { "system".into() } else { nick.clone() }),
        recipients: meta.short_name.clone(),
        cc: None,
        bcc: None,
        subject: Some(subject),
        content: message,
        html_content: None,
        timestamp: date,
        labels: vec![label.to_string()],
        attachments: Vec::new(),
        metadata: serde_json::json!({
            "buffer":    meta.full_name,
            "platform":  platform,
            "source_type": "weechat-relay",
            "highlight": highlight,
        }),
        folder: Some(meta.full_name.clone()),
        thread_id: None,
    })
}

/// Replace the shared subscribed-buffers list with everything in
/// `buffers` that's not on the uninteresting skip-list. Cheap clone
/// of full_name + short_name; the App reads this on every Folders
/// rebuild so we want it minimal.
fn publish_subscribed(subscribed: &SubscribedBuffers, buffers: &BTreeMap<String, BufferMeta>) {
    let snapshot: Vec<SubscribedBuffer> = buffers.values()
        .filter(|m| m.interesting)
        .map(|m| SubscribedBuffer {
            full_name: m.full_name.clone(),
            short_name: m.short_name.clone(),
        })
        .collect();
    *subscribed.lock().unwrap() = snapshot;
}

/// Apply a single nicklist hdata item to the shared map. `is_diff`
/// switches on the `_diff` char field: `+` add, `-` remove, `*`
/// update (no nick-set change). For non-diff (initial fetch) we just
/// add. Returns the ptr→full_name resolution so the caller can skip
/// items whose buffer isn't in our registry.
fn apply_nicklist_item(
    lists: &NickLists,
    buffers: &BTreeMap<String, BufferMeta>,
    item: &HdataItem,
    is_diff: bool,
) {
    // ptrs[0] is the buffer pointer, ptrs[1] is the nicklist_item pointer.
    let buf_ptr = match item.ptrs.first() {
        Some(p) if p != "0" => p,
        _ => return,
    };
    let Some(meta) = buffers.get(buf_ptr) else { return };
    let name = match item.fields.get("name") {
        Some(Object::Str(Some(s))) if !s.is_empty() => s.clone(),
        _ => return,
    };
    let group = matches!(item.fields.get("group"), Some(Object::Char(1)));
    // `group=1` items are nicklist headers (Ops / Voices / Members),
    // not actual nicks. Skip them.
    if group { return; }
    let visible = matches!(item.fields.get("visible"), Some(Object::Char(1)));
    if !visible { return; }
    let mut map = lists.lock().unwrap();
    let entry = map.entry(meta.full_name.clone()).or_default();
    if is_diff {
        let diff = match item.fields.get("_diff") {
            Some(Object::Char(c)) => *c as char,
            _ => '+',
        };
        match diff {
            '-' => { entry.remove(&name); }
            // '+' add, '*' update — either way ensure presence.
            _   => { entry.insert(name); }
        }
    } else {
        entry.insert(name);
    }
}

/// One supervised session. Connects, handshakes, lists buffers,
/// backfills recent history, subscribes to push events, and reads
/// forever. Returns `Err(...)` on any I/O failure so the supervisor
/// can retry with backoff.
fn run_persistent(
    db: &Arc<crate::database::Database>,
    source_id: i64,
    messages_dirty: &Arc<AtomicBool>,
    nick_lists: &NickLists,
    subscribed: &SubscribedBuffers,
) -> Result<(), String> {
    let secrets = load_relay_secrets();
    let host = secrets.host.ok_or("WEECHAT_RELAY_HOST missing")?;
    let port = secrets.port.ok_or("WEECHAT_RELAY_PORT missing")?;
    let pass = secrets.password.ok_or("WEECHAT_RELAY_PASSWORD missing")?;

    crate::log::info(&format!("weechat-relay: connecting to {}:{}", host, port));
    let mut conn = Connection::connect_for_push(&host, port)?;
    conn.handshake()?;
    conn.init_plain(&pass)?;

    // Buffer registry: pointer → metadata. Populated from the initial
    // list_buffers() and kept in sync via _buffer_opened / _closing
    // events the relay pushes when channels come and go.
    let mut buffers: BTreeMap<String, BufferMeta> = BTreeMap::new();
    let buf_list = conn.list_buffers()?;
    for it in &buf_list.items {
        let full_name = match it.fields.get("full_name") {
            Some(Object::Str(Some(s))) => s.clone(),
            _ => continue,
        };
        let ptr = match it.ptrs.first() {
            Some(p) if p != "0" => p.clone(),
            _ => continue,
        };
        let short_name = match it.fields.get("short_name") {
            Some(Object::Str(Some(s))) if !s.is_empty() => s.clone(),
            _ => full_name.split('.').next_back().unwrap_or(&full_name).to_string(),
        };
        let interesting = !is_uninteresting_buffer(&full_name);
        buffers.insert(ptr, BufferMeta { full_name, short_name, interesting });
    }
    crate::log::info(&format!("weechat-relay: {} buffers ({} interesting)",
        buffers.len(),
        buffers.values().filter(|m| m.interesting).count()));

    // Publish a snapshot of the interesting buffers to the shared
    // `subscribed` list so the Folders view can render every
    // subscribed channel even when it has no messages yet.
    publish_subscribed(subscribed, &buffers);

    // Backfill: pull the last ~15 lines per interesting buffer to
    // close any gap left by the previous session. Dedup via the DB's
    // known-ids set so we don't re-insert lines that already exist.
    let mut known_ids = db.get_known_external_ids(source_id);
    let mut backfill_inserts = 0usize;
    let interesting_ptrs: Vec<String> = buffers.iter()
        .filter(|(_, m)| m.interesting)
        .map(|(p, _)| p.clone())
        .collect();
    for ptr in &interesting_ptrs {
        let meta = match buffers.get(ptr) { Some(m) => m, None => continue };
        let lines = match conn.last_lines(ptr, 15) {
            Ok(h) => h,
            Err(e) => {
                crate::log::info(&format!("weechat-relay: backfill {}: {}", meta.full_name, e));
                continue;
            }
        };
        for line in &lines.items {
            if let Some(msg) = line_to_message(line, meta, &mut known_ids) {
                db.insert_message(source_id, &msg);
                backfill_inserts += 1;
            }
        }
    }
    if backfill_inserts > 0 {
        crate::log::info(&format!("weechat-relay: backfill inserted {} messages", backfill_inserts));
        messages_dirty.store(true, Ordering::Relaxed);
    }
    db.update_source_sync_time(source_id);

    // Seed nick lists. One round-trip pulls every visible nick across
    // every buffer; `_nicklist_diff` push events keep it current after
    // that. Wipe the shared map first so a reconnect doesn't leave
    // stale nicks for closed buffers.
    {
        let mut map = nick_lists.lock().unwrap();
        map.clear();
    }
    if conn.nicklist_all().is_ok() {
        if let Ok((id, objs)) = conn.read_message() {
            if id == "nicks" {
                for obj in objs {
                    if let Object::Hdata(h) = obj {
                        for it in &h.items {
                            apply_nicklist_item(nick_lists, &buffers, it, false);
                        }
                    }
                }
                let total: usize = nick_lists.lock().unwrap().values().map(|s| s.len()).sum();
                crate::log::info(&format!("weechat-relay: nicklist seeded ({} nicks)", total));
            }
        }
    }

    // Subscribe to all live buffers. From here on the server pushes
    // `_buffer_line_added`, `_buffer_opened`, `_buffer_closing`,
    // `_nicklist_diff`, etc. read_message() blocks until something
    // arrives — kernel-parked thread, zero CPU when idle.
    conn.sync("*")?;
    crate::log::info("weechat-relay: sync * — listening");

    loop {
        let (id, objs) = conn.read_message()?;
        match id.as_str() {
            "_buffer_line_added" => {
                let mut pending: Vec<MessageData> = Vec::new();
                for obj in objs {
                    let Object::Hdata(h) = obj else { continue };
                    for line in &h.items {
                        let buf_ptr = match line.fields.get("buffer") {
                            Some(Object::Ptr(p)) => p.clone(),
                            _ => continue,
                        };
                        let Some(meta) = buffers.get(&buf_ptr) else { continue };
                        if !meta.interesting { continue; }
                        if let Some(msg) = line_to_message(line, meta, &mut known_ids) {
                            pending.push(msg);
                        }
                    }
                }
                if !pending.is_empty() {
                    db.insert_messages_batch(source_id, &pending);
                    messages_dirty.store(true, Ordering::Relaxed);
                }
            }
            "_buffer_opened" => {
                // Update registry so subsequent lines for this buffer
                // route correctly. Same field shape as list_buffers().
                for obj in objs {
                    let Object::Hdata(h) = obj else { continue };
                    for it in &h.items {
                        let full_name = match it.fields.get("full_name") {
                            Some(Object::Str(Some(s))) => s.clone(),
                            _ => continue,
                        };
                        let ptr = match it.ptrs.first() {
                            Some(p) if p != "0" => p.clone(),
                            _ => continue,
                        };
                        let short_name = match it.fields.get("short_name") {
                            Some(Object::Str(Some(s))) if !s.is_empty() => s.clone(),
                            _ => full_name.split('.').next_back().unwrap_or(&full_name).to_string(),
                        };
                        let interesting = !is_uninteresting_buffer(&full_name);
                        buffers.insert(ptr, BufferMeta { full_name, short_name, interesting });
                    }
                }
                publish_subscribed(subscribed, &buffers);
            }
            "_buffer_closing" => {
                for obj in objs {
                    let Object::Hdata(h) = obj else { continue };
                    for it in &h.items {
                        if let Some(p) = it.ptrs.first() {
                            buffers.remove(p);
                        }
                    }
                }
                publish_subscribed(subscribed, &buffers);
            }
            "_nicklist_diff" => {
                for obj in objs {
                    if let Object::Hdata(h) = obj {
                        for it in &h.items {
                            apply_nicklist_item(nick_lists, &buffers, it, true);
                        }
                    }
                }
            }
            // _buffer_renamed, _buffer_moved, etc. — ignore for now.
            _ => {}
        }
    }
}

/// Public entry: spawn the supervised push thread. Owns its own
/// reconnect loop with exponential backoff capped at 5 min. A
/// successful drain (we ran for > 60 s) resets the backoff so
/// transient blips don't permanently slow recovery.
pub fn spawn_supervisor(
    db: Arc<crate::database::Database>,
    source_id: i64,
    messages_dirty: Arc<AtomicBool>,
    nick_lists: NickLists,
    subscribed: SubscribedBuffers,
) {
    std::thread::Builder::new()
        .name("weechat-relay-supervisor".to_string())
        .spawn(move || {
            let initial = Duration::from_secs(1);
            let cap     = Duration::from_secs(5 * 60);
            let mut backoff = initial;
            loop {
                let started = std::time::Instant::now();
                let err = run_persistent(
                    &db, source_id, &messages_dirty, &nick_lists, &subscribed,
                ).err();
                let ran_for = started.elapsed();
                if ran_for > Duration::from_secs(60) {
                    backoff = initial;
                }
                if let Some(e) = err {
                    crate::log::info(&format!(
                        "weechat-relay: session ended ({}); reconnecting in {:?}",
                        e, backoff));
                } else {
                    crate::log::info(&format!(
                        "weechat-relay: session ended cleanly; reconnecting in {:?}",
                        backoff));
                }
                std::thread::sleep(backoff);
                backoff = std::cmp::min(backoff * 2, cap);
            }
        })
        .expect("spawn weechat-relay supervisor");
}

/// Heuristic skip-list for relay-internal buffers — `core.weechat`,
/// `relay.relay.list`, the script-manager, etc. The user can still
/// see those in weechat itself; mirroring them into kastrup just
/// clutters the folder list.
fn is_uninteresting_buffer(full_name: &str) -> bool {
    if full_name == "core.weechat"
        || full_name == "relay.relay.list"
        || full_name.starts_with("perl.")
        || full_name.starts_with("python.script.")
        || full_name.starts_with("script.")
        || full_name.starts_with("fset.")
        || full_name.starts_with("irc.server.")
        || full_name == "irc.bitlbee.&bitlbee"
    {
        return true;
    }
    // `python.slack.<workspace>` with no further `.<channel>` suffix
    // is wee-slack's per-workspace root buffer — a placeholder for
    // the workspace itself, not a chat channel. Same idea for
    // `matrix.<server>` (matrix plugin's server-root buffer).
    for transport in ["python.slack.", "matrix."] {
        if let Some(rest) = full_name.strip_prefix(transport) {
            if !rest.contains('.') { return true; }
        }
    }
    false
}

/// Classify a buffer's `full_name` into a (platform, label) pair.
/// `platform` lands in metadata for filtering; `label` is the
/// per-message tag (mirrors what the legacy log-tail source emits).
fn classify_buffer(full_name: &str) -> (&'static str, &'static str) {
    if full_name.starts_with("python.slack.")  { ("slack",   "Slack")   }
    else if full_name.starts_with("matrix.")    { ("matrix",  "Matrix")  }
    else if full_name.starts_with("irc.discord-bridge.") { ("discord", "DiscordBridge") }
    else if full_name.starts_with("irc.")       { ("irc",     "IRC")     }
    else if full_name.starts_with("python.whatsapp.") { ("whatsapp", "WhatsApp") }
    else                                        { ("other",   "Weechat") }
}

/// True if `tags_array` contains any of `needles`. Used to filter
/// out structural lines (joins/parts/topics) before they hit the DB.
fn tags_contain_any(field: Option<&Object>, needles: &[&str]) -> bool {
    let Some(Object::Array(items)) = field else { return false };
    for it in items {
        let Object::Str(Some(s)) = it else { continue };
        if needles.iter().any(|n| s == n) { return true; }
    }
    false
}

/// True if `tags_array` contains any tag in `needles`. For use when
/// we care about specific tags by name (action types, etc.). Same
/// signature as `tags_contain_any` but kept separate so the call
/// sites read clearly.
fn tags_has(field: Option<&Object>, tag: &str) -> bool {
    let Some(Object::Array(items)) = field else { return false };
    items.iter().any(|it| matches!(it, Object::Str(Some(s)) if s == tag))
}

/// Extract the canonical author nick from a `tags_array` field. Weechat
/// (and wee-slack specifically) tags every line with a `nick_XXX` tag
/// when the author is known — even when the prefix column is collapsed
/// to a thread-continuation marker like `` `-> ``. Returns the first
/// `nick_*` value found, or `None` if no such tag is present.
fn nick_from_tags(field: Option<&Object>) -> Option<String> {
    let Some(Object::Array(items)) = field else { return None };
    for it in items {
        let Object::Str(Some(s)) = it else { continue };
        if let Some(rest) = s.strip_prefix("nick_") {
            if !rest.is_empty() { return Some(rest.to_string()); }
        }
    }
    None
}

/// Wee-slack prefixes `nick_*` tag values with `_<NN>` where NN is the
/// 2-digit palette colour for that user (e.g. `nick__16alice` →
/// `alice`). Real Slack usernames never start with `_<digits>`, so
/// stripping is safe. If the input doesn't match the pattern, return
/// it unchanged.
fn strip_weeslack_color_prefix(nick: &str) -> String {
    let bytes = nick.as_bytes();
    if bytes.len() < 3 || bytes[0] != b'_' { return nick.to_string(); }
    let mut i = 1;
    while i < bytes.len() && bytes[i].is_ascii_digit() { i += 1; }
    if i == 1 || i >= bytes.len() { return nick.to_string(); }
    nick[i..].to_string()
}

/// True for wee-slack / wee-matrix style "this is a continuation of the
/// previous author's line" prefix markers. We swallow these as nicks so
/// the From column shows the real author (recovered from tags) or no
/// author at all, instead of the marker glyph.
fn is_continuation_marker(s: &str) -> bool {
    let t = s.trim();
    matches!(t, "`->" | "->" | "↳" | "→" | "» " | "»" | "·" | "...")
        || t.starts_with("`->")
}

/// IRC-style prefix markers that the relay sends for join / part /
/// quit / nick-change / mode / topic. We filter these so kastrup's
/// folder count reflects real chat traffic, not connection churn.
fn is_system_prefix(prefix: &str) -> bool {
    let p = strip_codes(prefix);
    let p = p.trim();
    matches!(p, "-->" | "<--" | "--" | "*" | "**" | "→" | "←" | "↔" | "ℹ" | "")
}

/// Cheap MD5 implementation — used only for content-hash external_ids.
/// `md5` crate would be cleaner but adds 25kB to the binary for one
/// call site, and we already have the legacy `weechat::md5_hex` for
/// the same job. Re-export it here to keep the source self-contained.
fn md5_hex(input: &str) -> String {
    crate::sources::weechat::md5_hex_public(input)
}

// Tiny extension so connect_timeout takes a single SocketAddr cleanly.
trait ToSocketAddrsFirst {
    fn to_socket_addrs_first(&self) -> Result<std::net::SocketAddr, String>;
}
impl ToSocketAddrsFirst for String {
    fn to_socket_addrs_first(&self) -> Result<std::net::SocketAddr, String> {
        use std::net::ToSocketAddrs;
        self.to_socket_addrs()
            .map_err(|e| format!("resolve {}: {}", self, e))?
            .next()
            .ok_or_else(|| format!("no addrs for {}", self))
    }
}

// ---------------------------------------------------------------------------
// M1 entry point — exposed as `kastrup --weechat-probe`
// ---------------------------------------------------------------------------

/// M2 probe: connect, fetch last 20 lines of `buffer_full_name`,
/// subscribe, then loop forever printing any new line events.
/// Run from main via `kastrup --weechat-tail <buffer>`. Ctrl-C to
/// exit.
pub fn tail(buffer_full_name: &str) -> Result<(), String> {
    let secrets = load_relay_secrets();
    let host = secrets.host.ok_or("WEECHAT_RELAY_HOST not set in ~/.kastrup/.env")?;
    let port = secrets.port.ok_or("WEECHAT_RELAY_PORT not set in ~/.kastrup/.env")?;
    let pass = secrets.password.ok_or("WEECHAT_RELAY_PASSWORD not set in ~/.kastrup/.env")?;

    eprintln!("→ connecting to {}:{}", host, port);
    let mut c = Connection::connect(&host, port)?;
    let _hs = c.handshake()?;
    c.init_plain(&pass)?;

    // Find the buffer by full_name. Match exact first; fall back
    // to case-insensitive contains so the user can type
    // "#general" or "general-oslo" without full prefixing.
    let buffers = c.list_buffers()?;
    let target = buffers.items.iter().find(|it| {
        matches!(it.fields.get("full_name"),
            Some(Object::Str(Some(s))) if s == buffer_full_name)
    }).or_else(|| {
        let needle = buffer_full_name.to_lowercase();
        buffers.items.iter().find(|it| {
            match it.fields.get("full_name") {
                Some(Object::Str(Some(s))) => s.to_lowercase().contains(&needle),
                _ => false,
            }
        })
    });
    let target = target.ok_or_else(|| format!(
        "no buffer matches '{}'. List: {}",
        buffer_full_name,
        buffers.items.iter().take(5).filter_map(|it|
            match it.fields.get("full_name") {
                Some(Object::Str(Some(s))) => Some(s.as_str()), _ => None
            }).collect::<Vec<_>>().join(", ")
    ))?;
    let ptr = target.ptrs.first().cloned().unwrap_or_default();
    let title = match target.fields.get("full_name") {
        Some(Object::Str(Some(s))) => s.clone(),
        _ => "?".to_string(),
    };
    eprintln!("← target {} @ {}", title, ptr);

    // History.
    let lines = c.last_lines(&ptr, 20)?;
    eprintln!("← last {} lines:", lines.items.len());
    for it in &lines.items {
        println!("  {}", format_line(it));
    }

    // Live tail.
    c.sync(&ptr)?;
    eprintln!("→ sync {} — listening (Ctrl-C to exit)", ptr);
    loop {
        let (id, objs) = c.read_message()?;
        if id == "_buffer_line_added" {
            // Each event carries one hda of new line(s).
            for obj in objs {
                if let Object::Hdata(h) = obj {
                    for it in &h.items {
                        // Filter: only print lines for OUR buffer.
                        // Sync events arrive for any buffer matching
                        // the sync target, but `sync 0xPTR` should be
                        // already filtered server-side. Belt-and-
                        // braces filter using the `buffer` field.
                        let buf_field = it.fields.get("buffer");
                        if let Some(Object::Ptr(p)) = buf_field {
                            if p != &ptr { continue; }
                        }
                        println!("  {}", format_line(it));
                    }
                }
            }
        } else if id.starts_with("_buffer_") || id == "_nicklist_diff" {
            // Other lifecycle events — fine to ignore in M2.
            eprintln!("· {} ({} objs)", id, objs.len());
        } else {
            eprintln!("· unhandled id={}", id);
        }
    }
}

/// Render one line hdata item as a single-line string for the tail
/// probe. Strips weechat colour codes; later milestones reuse the
/// existing `weechat::strip_weechat_colors` helper.
fn format_line(it: &HdataItem) -> String {
    let prefix = match it.fields.get("prefix") {
        Some(Object::Str(Some(s))) => strip_codes(s),
        _ => String::new(),
    };
    let message = match it.fields.get("message") {
        Some(Object::Str(Some(s))) => strip_codes(s),
        _ => String::new(),
    };
    let ts = match it.fields.get("date") {
        Some(Object::Time(t)) => format_clock(*t),
        _ => String::from("--:--"),
    };
    let hl = match it.fields.get("highlight") {
        Some(Object::Char(1)) => " *",
        _ => "  ",
    };
    format!("{}{} {:<14}  {}", ts, hl, prefix, message)
}

/// hh:mm in UTC. The TUI integration in M3 will switch to local time;
/// for the probe UTC is unambiguous.
fn format_clock(unix: i64) -> String {
    let secs_per_day = 86400i64;
    let secs_today = unix.rem_euclid(secs_per_day);
    let h = secs_today / 3600;
    let m = (secs_today % 3600) / 60;
    format!("{:02}:{:02}", h, m)
}

/// Strip both weechat-internal colour codes AND raw IRC mIRC codes
/// from a line. Grammar (matches the legacy log-tail source):
///   `\x19`          — colour intro
///     then ONE type char (F, B, *, _, /, |, etc.)
///     then optional `~` or `@`
///     then ASCII digits (any count)
///   `\x1A` / `\x1B` — set / remove attribute (one payload byte)
///   `\x1C`          — reset (no payload)
///   `\x02 \x0F \x16 \x1D \x1F` — IRC formatting toggles (no payload)
///   `\x03`          — IRC mIRC colour (1-2 fg digits, optional
///                     `,` + 1-2 bg digits)
///
/// Earlier `taken < 8` cap was eating the first letter of the
/// nick when the colour spec was `F@00xxx` (8 bytes of meta) and
/// the next byte was the nick's initial letter.
fn strip_codes(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\u{19}' => {
                if let Some(&next) = chars.peek() {
                    if next == '\u{1c}' {
                        chars.next();
                    } else {
                        chars.next();  // type char
                        if let Some(&d) = chars.peek() {
                            if d == '~' || d == '@' { chars.next(); }
                        }
                        while let Some(&d) = chars.peek() {
                            if d.is_ascii_digit() { chars.next(); } else { break; }
                        }
                    }
                }
            }
            '\u{1a}' | '\u{1b}' => { chars.next(); }
            '\u{1c}' => { /* reset, no payload */ }
            '\u{02}' | '\u{0f}' | '\u{16}' | '\u{1d}' | '\u{1f}' => {}
            '\u{03}' => {
                // IRC mIRC colour. Eat 1-2 fg digits, optional
                // ,bg-digits.
                let mut fg = 0;
                while fg < 2 {
                    match chars.peek() {
                        Some(c) if c.is_ascii_digit() => { chars.next(); fg += 1; }
                        _ => break,
                    }
                }
                if chars.peek() == Some(&',') {
                    // Only swallow the comma if a digit follows —
                    // a literal "Colour, 12" shouldn't lose its
                    // comma + space.
                    let mut lookahead = chars.clone();
                    lookahead.next();
                    if matches!(lookahead.peek(), Some(c) if c.is_ascii_digit()) {
                        chars.next();
                        let mut bg = 0;
                        while bg < 2 {
                            match chars.peek() {
                                Some(c) if c.is_ascii_digit() => { chars.next(); bg += 1; }
                                _ => break,
                            }
                        }
                    }
                }
            }
            _ => out.push(ch),
        }
    }
    out
}

/// Diagnostic probe: print prefix + tags_array for the last `n` lines
/// of a buffer, so we can see what the relay actually sends when a
/// nick comes out wrong.
pub fn dump_tags(buffer_full_name: &str, n: u32) -> Result<(), String> {
    let secrets = load_relay_secrets();
    let host = secrets.host.ok_or("WEECHAT_RELAY_HOST missing")?;
    let port = secrets.port.ok_or("WEECHAT_RELAY_PORT missing")?;
    let pass = secrets.password.ok_or("WEECHAT_RELAY_PASSWORD missing")?;
    let mut c = Connection::connect(&host, port)?;
    c.handshake()?;
    c.init_plain(&pass)?;
    let bufs = c.list_buffers()?;
    let target = bufs.items.iter().find(|it|
        matches!(it.fields.get("full_name"),
            Some(Object::Str(Some(s))) if s == buffer_full_name)
    ).ok_or_else(|| format!("no buffer matches '{}'", buffer_full_name))?;
    let ptr = target.ptrs.first().cloned().unwrap_or_default();
    let lines = c.last_lines(&ptr, n)?;
    for it in &lines.items {
        let pre = match it.fields.get("prefix") {
            Some(Object::Str(Some(s))) => s.clone(), _ => String::new() };
        let msg = match it.fields.get("message") {
            Some(Object::Str(Some(s))) => s.clone(), _ => String::new() };
        println!("PREFIX raw={:?}  stripped={:?}", pre, strip_codes(&pre));
        let mhead: String = msg.chars().take(60).collect();
        println!("MSG    {:?}", mhead);
        match it.fields.get("tags_array") {
            Some(Object::Array(items)) => {
                let tags: Vec<String> = items.iter().filter_map(|o| match o {
                    Object::Str(Some(s)) => Some(s.clone()), _ => None
                }).collect();
                println!("TAGS   {:?}", tags);
            }
            other => println!("TAGS   (other: {:?})", other),
        }
        println!("---");
    }
    Ok(())
}

/// One-shot probe used by main.rs's `--weechat-probe` flag. Prints
/// the buffer list to stdout in a human-readable form so we can
/// verify the wire end-to-end before plugging into the TUI.
pub fn probe() -> Result<(), String> {
    let secrets = load_relay_secrets();
    let host = secrets.host.ok_or("WEECHAT_RELAY_HOST not set in ~/.kastrup/.env")?;
    let port = secrets.port.ok_or("WEECHAT_RELAY_PORT not set in ~/.kastrup/.env")?;
    let pass = secrets.password.ok_or("WEECHAT_RELAY_PASSWORD not set in ~/.kastrup/.env")?;

    eprintln!("→ connecting to {}:{}", host, port);
    let mut c = Connection::connect(&host, port)?;
    let hs = c.handshake()?;
    eprintln!("← handshake: algo={} compression={} totp={}",
        hs.password_hash_algo, hs.compression, hs.totp);
    c.init_plain(&pass)?;
    eprintln!("→ init password=*** sent");
    let buffers = c.list_buffers()?;
    eprintln!("← {} buffers:", buffers.items.len());
    for it in &buffers.items {
        let s = |k: &str| match it.fields.get(k) {
            Some(Object::Str(Some(s))) => s.as_str(),
            _ => "",
        };
        let n = match it.fields.get("number") {
            Some(Object::Int(n)) => *n,
            _ => 0,
        };
        println!("  #{:<4} {:<40} {}", n, s("full_name"), s("title"));
    }
    Ok(())
}

#[derive(Default)]
pub struct RelaySecrets {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub password: Option<String>,
}

impl RelaySecrets {
    /// True when the env file has all three keys; used by
    /// `main::ensure_weechat_relay_source` so the source only
    /// auto-registers when there's something to connect to.
    pub fn has_all(&self) -> bool {
        self.host.is_some() && self.port.is_some() && self.password.is_some()
    }
}

/// Public alias of `load_relay_secrets` for main.rs's source-bootstrap
/// path. Keeps the actual parser private to this module.
pub fn load_secrets_for_main() -> RelaySecrets { load_relay_secrets() }

fn load_relay_secrets() -> RelaySecrets {
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from).unwrap_or_default();
    let path = home.join(".kastrup").join(".env");
    let mut s = RelaySecrets::default();
    let Ok(text) = std::fs::read_to_string(path) else { return s };
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        let Some(eq) = line.find('=') else { continue };
        let key = line[..eq].trim();
        let mut val = line[eq + 1..].trim().to_string();
        if (val.starts_with('"') && val.ends_with('"'))
            || (val.starts_with('\'') && val.ends_with('\''))
        {
            val = val[1..val.len() - 1].to_string();
        }
        match key {
            "WEECHAT_RELAY_HOST"     => s.host = Some(val),
            "WEECHAT_RELAY_PORT"     => s.port = val.parse().ok(),
            "WEECHAT_RELAY_PASSWORD" => s.password = Some(val),
            _ => {}
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parser smoke test: a hand-crafted minimal message containing
    /// an empty-id string + a single int object. Catches the most
    /// likely break (header maths) before any network call.
    #[test]
    fn parses_minimal_id_plus_int() {
        let mut payload: Vec<u8> = Vec::new();
        // empty id string: len=0
        payload.extend_from_slice(&0i32.to_be_bytes());
        // object: 'int' tag + 4-byte int
        payload.extend_from_slice(b"int");
        payload.extend_from_slice(&42i32.to_be_bytes());
        let mut c = Cursor::new(&payload);
        let id = c.str().unwrap().unwrap_or_default();
        assert_eq!(id, "");
        let obj = parse_object(&mut c).unwrap();
        match obj {
            Object::Int(v) => assert_eq!(v, 42),
            other => panic!("expected Int, got {:?}", other),
        }
    }

    #[test]
    fn parses_str_null_and_empty() {
        // null
        let bytes = (-1i32).to_be_bytes();
        let mut c = Cursor::new(&bytes);
        assert!(matches!(c.str().unwrap(), None));
        // empty
        let bytes = 0i32.to_be_bytes();
        let mut c = Cursor::new(&bytes);
        let v = c.str().unwrap();
        assert_eq!(v.as_deref(), Some(""));
    }

    #[test]
    fn parses_ptr_null_and_hex() {
        // null pointer: length=1, value="0"
        let bytes: &[u8] = &[1, b'0'];
        let mut c = Cursor::new(bytes);
        assert_eq!(c.ptr().unwrap(), "0");
        // real pointer: length=6, value="abc123" → "0xabc123"
        let bytes: &[u8] = &[6, b'a', b'b', b'c', b'1', b'2', b'3'];
        let mut c = Cursor::new(bytes);
        assert_eq!(c.ptr().unwrap(), "0xabc123");
    }
}
