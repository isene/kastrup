//! Chat send paths for the `m` draft picker (slack + discord).
//!
//! `send_slack` and `send_discord` are blocking HTTPS calls against
//! the respective vendor APIs. Tokens come from `~/.kastrup/.env`;
//! if a slack token isn't present there, we fall back to parsing
//! `~/.weechat/plugins.conf` so a working weechat install Just Works
//! without duplicating credentials.

use std::collections::HashMap;
use std::path::PathBuf;

/// Secrets loaded from `~/.kastrup/.env`. Missing keys are not an
/// error — we surface a friendly "set X in ~/.kastrup/.env" message
/// at send time so the user sees what's missing.
#[derive(Default, Clone)]
pub struct Secrets {
    pub slack_token: Option<String>,
    /// When `slack_token` is an `xoxc-…` browser/client token, this
    /// holds the matching `xoxd-…` session cookie value. Together they
    /// auth like the Slack web client — no app attribution badge. None
    /// when only a legacy `xoxp-…` user token is available.
    pub slack_cookie: Option<String>,
    pub discord_bot_token: Option<String>,
    /// Lowercase webhook name → URL.
    pub discord_webhooks: HashMap<String, String>,
}

fn env_file_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".kastrup").join(".env")
}

fn weechat_plugins_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".weechat").join("plugins.conf")
}

/// Best-effort parse of a `KEY=value` style .env file. Quotes are
/// stripped; comment lines (`#`) and blanks are skipped.
pub fn load_secrets() -> Secrets {
    let mut s = Secrets::default();
    if let Ok(text) = std::fs::read_to_string(env_file_path()) {
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') { continue; }
            let Some(eq) = line.find('=') else { continue };
            let key = line[..eq].trim();
            let mut val = line[eq + 1..].trim().to_string();
            // strip optional surrounding quotes
            if (val.starts_with('"') && val.ends_with('"'))
                || (val.starts_with('\'') && val.ends_with('\''))
            {
                val = val[1..val.len() - 1].to_string();
            }
            match key {
                "SLACK_API_TOKEN"  => s.slack_token = Some(val),
                "SLACK_API_COOKIE" => s.slack_cookie = Some(val),
                "DISCORD_BOT_TOKEN" => s.discord_bot_token = Some(val),
                k if k.starts_with("DISCORD_WEBHOOK_") => {
                    let name = k.trim_start_matches("DISCORD_WEBHOOK_")
                        .to_ascii_lowercase();
                    s.discord_webhooks.insert(name, val);
                }
                _ => {}
            }
        }
    }
    // Fallback chain when .env didn't set SLACK_API_TOKEN: first try
    // the browser-cookie pair (xoxc-/xoxd-) that wee-slack itself
    // uses, since it posts without an app-attribution badge — exactly
    // the "as me, not as a bot" behaviour the user sees in weechat.
    // If that's missing (older wee-slack auth), fall back to the
    // legacy `xoxp-…` user token, which posts as the user too but
    // shows "via wee-slack" alongside every message.
    if s.slack_token.is_none() {
        if let Some((bearer, cookie)) = slack_cookie_pair_from_weechat() {
            s.slack_token = Some(bearer);
            s.slack_cookie = Some(cookie);
        } else if let Some(tok) = slack_token_from_weechat() {
            s.slack_token = Some(tok);
        }
    }
    s
}

/// Pull the `xoxc-…:xoxd-…` browser/cookie pair from a weechat
/// plugins.conf line. Returns `(bearer_token, cookie_value)` where
/// `cookie_value` is the bare xoxd- (caller wraps as `d=...` in the
/// Cookie header). Modern wee-slack stores this as the first entry
/// in the comma-separated `slack_api_token` list.
fn slack_cookie_pair_from_weechat() -> Option<(String, String)> {
    let text = std::fs::read_to_string(weechat_plugins_path()).ok()?;
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with("python.slack.slack_api_token") { continue; }
        let eq = line.find('=')?;
        let mut val = line[eq + 1..].trim().to_string();
        if val.starts_with('"') && val.ends_with('"') {
            val = val[1..val.len() - 1].to_string();
        }
        for part in val.split(',') {
            let p = part.trim();
            if let Some((bearer, cookie)) = p.split_once(':') {
                if bearer.starts_with("xoxc-") && cookie.starts_with("xoxd-") {
                    return Some((bearer.to_string(), cookie.to_string()));
                }
            }
        }
    }
    None
}

/// Pull the legacy `xoxp-…` user token from a weechat plugins.conf
/// line like:
///   python.slack.slack_api_token = "xoxc-…:xoxd-…,xoxp-…,xoxp-…"
/// Picks the *first* xoxp- in the comma list.
fn slack_token_from_weechat() -> Option<String> {
    let text = std::fs::read_to_string(weechat_plugins_path()).ok()?;
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with("python.slack.slack_api_token") { continue; }
        let eq = line.find('=')?;
        let mut val = line[eq + 1..].trim().to_string();
        if val.starts_with('"') && val.ends_with('"') {
            val = val[1..val.len() - 1].to_string();
        }
        for part in val.split(',') {
            let p = part.trim();
            if p.starts_with("xoxp-") {
                return Some(p.to_string());
            }
        }
    }
    None
}

// --- Slack -----------------------------------------------------------------

/// Resolve a draft `Channel:` value into a Slack ID suitable for
/// `chat.postMessage`. Accepts:
///   - `C…` / `G…` / `D…`  → passed through
///   - `#name`               → resolved via `conversations.list`
///   - `@name` / `U…`        → opened as a DM via `conversations.open`
///   - `mpdm:a,b,c`          → opened as a multi-party DM (each handle
///                             resolved, then `conversations.open` with
///                             the comma-separated user IDs)
pub fn slack_resolve_channel(token: &str, cookie: Option<&str>, raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw.is_empty() { return Err("empty channel".into()); }
    if let Some(rest) = raw.strip_prefix("mpdm:") {
        let mut user_ids: Vec<String> = Vec::new();
        for handle in rest.split(',') {
            let handle = handle.trim();
            if handle.is_empty() { continue; }
            let id = if handle.starts_with('U') && handle.len() >= 9
                && handle[1..].chars().all(|c| c.is_ascii_alphanumeric())
            {
                handle.to_string()
            } else {
                slack_lookup_user_by_handle(token, cookie, handle.trim_start_matches('@'))?
            };
            user_ids.push(id);
        }
        if user_ids.is_empty() {
            return Err("mpdm: no users in target".into());
        }
        return slack_open_dm(token, cookie, &user_ids.join(","));
    }
    if let Some(c0) = raw.chars().next() {
        if (c0 == 'C' || c0 == 'G' || c0 == 'D') && raw.len() >= 9
            && raw[1..].chars().all(|c| c.is_ascii_alphanumeric())
        {
            return Ok(raw.to_string());
        }
        if c0 == 'U' && raw.len() >= 9
            && raw[1..].chars().all(|c| c.is_ascii_alphanumeric())
        {
            return slack_open_dm(token, cookie, raw);
        }
    }
    if let Some(name) = raw.strip_prefix('#') {
        return slack_lookup_channel_by_name(token, cookie, name);
    }
    if let Some(handle) = raw.strip_prefix('@') {
        let uid = slack_lookup_user_by_handle(token, cookie, handle)?;
        return slack_open_dm(token, cookie, &uid);
    }
    slack_lookup_channel_by_name(token, cookie, raw)
}

/// Attach Authorization (Bearer) + optional Cookie (d=...) to a
/// ureq request. Used for every Slack API call so the cookie pair
/// (when present) carries through.
fn apply_slack_auth(mut req: ureq::Request, token: &str, cookie: Option<&str>) -> ureq::Request {
    req = req.set("Authorization", &format!("Bearer {}", token));
    if let Some(c) = cookie {
        req = req.set("Cookie", &format!("d={}", c));
    }
    req
}

fn slack_lookup_channel_by_name(token: &str, cookie: Option<&str>, name: &str) -> Result<String, String> {
    let target = name.trim_start_matches('#').to_ascii_lowercase();
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let mut url = String::from(
            "https://slack.com/api/conversations.list?limit=1000&types=public_channel,private_channel"
        );
        if let Some(c) = &cursor {
            url.push_str("&cursor=");
            url.push_str(c);
        }
        let resp = apply_slack_auth(ureq::get(&url), token, cookie)
            .call()
            .map_err(|e| format!("conversations.list: {}", e))?
            .into_string()
            .map_err(|e| e.to_string())?;
        let v: serde_json::Value = serde_json::from_str(&resp).map_err(|e| e.to_string())?;
        if !v["ok"].as_bool().unwrap_or(false) {
            return Err(format!(
                "slack: {}",
                v["error"].as_str().unwrap_or("conversations.list failed")
            ));
        }
        if let Some(chans) = v["channels"].as_array() {
            for ch in chans {
                let cname = ch["name"].as_str().unwrap_or("").to_ascii_lowercase();
                if cname == target {
                    return Ok(ch["id"].as_str().unwrap_or("").to_string());
                }
            }
        }
        cursor = v["response_metadata"]["next_cursor"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if cursor.is_none() { break; }
    }
    Err(format!("channel not found: #{}", target))
}

fn slack_lookup_user_by_handle(token: &str, cookie: Option<&str>, handle: &str) -> Result<String, String> {
    let target = handle.trim_start_matches('@').to_ascii_lowercase();
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let mut url = String::from("https://slack.com/api/users.list?limit=1000");
        if let Some(c) = &cursor {
            url.push_str("&cursor=");
            url.push_str(c);
        }
        let resp = apply_slack_auth(ureq::get(&url), token, cookie)
            .call()
            .map_err(|e| format!("users.list: {}", e))?
            .into_string()
            .map_err(|e| e.to_string())?;
        let v: serde_json::Value = serde_json::from_str(&resp).map_err(|e| e.to_string())?;
        if !v["ok"].as_bool().unwrap_or(false) {
            return Err(format!("slack: {}", v["error"].as_str().unwrap_or("users.list failed")));
        }
        if let Some(users) = v["members"].as_array() {
            for u in users {
                let uname = u["name"].as_str().unwrap_or("").to_ascii_lowercase();
                let dname = u["profile"]["display_name"].as_str().unwrap_or("").to_ascii_lowercase();
                if uname == target || dname == target {
                    return Ok(u["id"].as_str().unwrap_or("").to_string());
                }
            }
        }
        cursor = v["response_metadata"]["next_cursor"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if cursor.is_none() { break; }
    }
    Err(format!("user not found: @{}", target))
}

fn slack_open_dm(token: &str, cookie: Option<&str>, user_id: &str) -> Result<String, String> {
    let body = serde_json::json!({ "users": user_id });
    let resp = apply_slack_auth(
        ureq::post("https://slack.com/api/conversations.open"),
        token, cookie,
    )
        .set("Content-Type", "application/json; charset=utf-8")
        .send_string(&body.to_string())
        .map_err(|e| format!("conversations.open: {}", e))?
        .into_string()
        .map_err(|e| e.to_string())?;
    let v: serde_json::Value = serde_json::from_str(&resp).map_err(|e| e.to_string())?;
    if !v["ok"].as_bool().unwrap_or(false) {
        return Err(format!("slack: {}", v["error"].as_str().unwrap_or("conversations.open failed")));
    }
    Ok(v["channel"]["id"].as_str().unwrap_or("").to_string())
}

/// Post a message to a slack channel/DM. `channel` should already be
/// an ID — use `slack_resolve_channel` first if the user typed a name.
/// When `cookie` is `Some`, auth is the wee-slack browser/client
/// flow (`xoxc-…` bearer + `d=xoxd-…` cookie) and Slack shows the
/// message exactly like a web-typed one — no "via wee-slack"
/// attribution. With `cookie = None`, falls back to plain Bearer
/// (legacy xoxp- user token, which DOES carry the app badge).
pub fn send_slack(token: &str, cookie: Option<&str>, channel: &str, text: &str) -> Result<(), String> {
    let body = serde_json::json!({
        "channel": channel,
        "text": text,
    });
    let resp = apply_slack_auth(
        ureq::post("https://slack.com/api/chat.postMessage"),
        token, cookie,
    )
        .set("Content-Type", "application/json; charset=utf-8")
        .send_string(&body.to_string())
        .map_err(|e| format!("chat.postMessage: {}", e))?
        .into_string()
        .map_err(|e| e.to_string())?;
    let v: serde_json::Value = serde_json::from_str(&resp).map_err(|e| e.to_string())?;
    if !v["ok"].as_bool().unwrap_or(false) {
        return Err(format!("slack: {}", v["error"].as_str().unwrap_or("chat.postMessage failed")));
    }
    Ok(())
}

/// Post a `/me`-style action message (third-person narration) to a
/// Slack channel via `chat.meMessage`. Caller has already stripped
/// the `/me ` prefix from `text`. The xoxc/xoxd browser flow is
/// supported here too so the message lands with no app attribution.
pub fn send_slack_me(token: &str, cookie: Option<&str>, channel: &str, text: &str) -> Result<(), String> {
    let body = serde_json::json!({
        "channel": channel,
        "text": text,
    });
    let resp = apply_slack_auth(
        ureq::post("https://slack.com/api/chat.meMessage"),
        token, cookie,
    )
        .set("Content-Type", "application/json; charset=utf-8")
        .send_string(&body.to_string())
        .map_err(|e| format!("chat.meMessage: {}", e))?
        .into_string()
        .map_err(|e| e.to_string())?;
    let v: serde_json::Value = serde_json::from_str(&resp).map_err(|e| e.to_string())?;
    if !v["ok"].as_bool().unwrap_or(false) {
        return Err(format!("slack: {}", v["error"].as_str().unwrap_or("chat.meMessage failed")));
    }
    Ok(())
}

/// Upload a single file to a Slack channel via the legacy
/// `files.upload` endpoint. Multipart form constructed by hand to
/// avoid pulling in a multipart crate for one call site. `channel`
/// is the resolved channel ID (`C…` / `G…` / `D…`), not a name —
/// the caller resolves first via `slack_resolve_channel`.
///
/// `comment` is an optional message that appears alongside the
/// file in the channel; pass an empty string to skip.
pub fn slack_upload_file(
    token: &str, cookie: Option<&str>,
    channel: &str, path: &std::path::Path, comment: &str,
) -> Result<(), String> {
    let bytes = std::fs::read(path)
        .map_err(|e| format!("read {}: {}", path.display(), e))?;
    let filename = path.file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("attachment")
        .to_string();

    let boundary = format!("----kastrup-{:x}", crate::database::now_secs());
    let mut body: Vec<u8> = Vec::with_capacity(bytes.len() + 1024);

    let mut add_field = |name: &str, value: &str| {
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        body.extend_from_slice(format!("Content-Disposition: form-data; name=\"{}\"\r\n\r\n", name).as_bytes());
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    };
    add_field("channels", channel);
    add_field("filename", &filename);
    if !comment.is_empty() {
        add_field("initial_comment", comment);
    }
    // file part
    body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{}\"\r\n",
            filename).as_bytes());
    body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
    body.extend_from_slice(&bytes);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());

    let resp = apply_slack_auth(
        ureq::post("https://slack.com/api/files.upload"),
        token, cookie,
    )
        .set("Content-Type", &format!("multipart/form-data; boundary={}", boundary))
        .send_bytes(&body)
        .map_err(|e| format!("files.upload: {}", e))?
        .into_string()
        .map_err(|e| e.to_string())?;
    let v: serde_json::Value = serde_json::from_str(&resp).map_err(|e| e.to_string())?;
    if !v["ok"].as_bool().unwrap_or(false) {
        return Err(format!("slack: {}", v["error"].as_str().unwrap_or("files.upload failed")));
    }
    Ok(())
}

// --- Discord ---------------------------------------------------------------

/// Route a discord draft. `target` is the post-prefix value, e.g.
///   `channel:1234`   → bot API send to channel
///   `webhook:tekst`  → use stored webhook URL
///   `dm:1234`        → create DM with user, send
///   bare numeric     → treat as channel id (bot)
pub fn send_discord(
    secrets: &Secrets,
    target: &str,
    text: &str,
) -> Result<String, String> {
    let target = target.trim();
    if let Some(name) = target.strip_prefix("webhook:") {
        let key = name.trim().to_ascii_lowercase();
        if let Some(url) = secrets.discord_webhooks.get(&key) {
            discord_post_webhook(url, text)?;
            return Ok(format!("webhook:{}", key));
        }
        // No webhook URL for that name — the bot can post to any channel
        // kastrup already mirrors, so resolve the name and send that way.
        let cid = discord_channel_for_webhook(secrets, &key)?;
        let token = secrets.discord_bot_token.as_ref()
            .ok_or_else(|| "DISCORD_BOT_TOKEN not set".to_string())?;
        discord_post_bot_channel(token, &cid, text)?;
        return Ok(format!("channel:{}", cid));
    }
    if let Some(cid) = target.strip_prefix("channel:") {
        let token = secrets.discord_bot_token.as_ref()
            .ok_or_else(|| "DISCORD_BOT_TOKEN not set".to_string())?;
        discord_post_bot_channel(token, cid.trim(), text)?;
        return Ok(format!("channel:{}", cid.trim()));
    }
    if let Some(uid) = target.strip_prefix("dm:") {
        let token = secrets.discord_bot_token.as_ref()
            .ok_or_else(|| "DISCORD_BOT_TOKEN not set".to_string())?;
        let cid = discord_create_dm(token, uid.trim())?;
        discord_post_bot_channel(token, &cid, text)?;
        // Remember this peer so their replies to the bot get polled into View 3.
        crate::sources::discord::remember_dm_peer(uid.trim());
        return Ok(format!("dm:{}", uid.trim()));
    }
    // Bare numeric → treat as channel id
    if target.chars().all(|c| c.is_ascii_digit()) {
        let token = secrets.discord_bot_token.as_ref()
            .ok_or_else(|| "DISCORD_BOT_TOKEN not set".to_string())?;
        discord_post_bot_channel(token, target, text)?;
        return Ok(format!("channel:{}", target));
    }
    Err(format!("unrecognised discord target: {}", target))
}

/// Channel id to use for a `webhook:<key>` draft that has no URL in
/// `.env`. On failure the error names both halves of the miss, so the
/// user isn't left guessing which file to edit.
pub fn discord_channel_for_webhook(secrets: &Secrets, key: &str) -> Result<String, String> {
    if let Some(cid) = crate::sources::discord::channel_id_for_name(key) {
        return Ok(cid);
    }
    let webhooks = if secrets.discord_webhooks.is_empty() {
        "no DISCORD_WEBHOOK_* line is set in ~/.kastrup/.env".to_string()
    } else {
        let mut names: Vec<&str> = secrets.discord_webhooks
            .keys().map(|s| s.as_str()).collect();
        names.sort();
        format!("webhooks set: {}", names.join(", "))
    };
    Err(format!(
        "discord '{}': {}, and no channel by that name in ~/.kastrup/discord_channels \
         — address the draft as Channel: channel:<id> instead",
        key, webhooks))
}

fn discord_post_webhook(url: &str, text: &str) -> Result<(), String> {
    let body = serde_json::json!({ "content": text });
    let resp = ureq::post(url)
        .set("Content-Type", "application/json")
        .send_string(&body.to_string());
    match resp {
        Ok(_) => Ok(()),
        Err(ureq::Error::Status(code, r)) => {
            let msg = r.into_string().unwrap_or_default();
            Err(format!("discord webhook {}: {}", code, msg))
        }
        Err(e) => Err(format!("discord webhook: {}", e)),
    }
}

fn discord_post_bot_channel(token: &str, channel_id: &str, text: &str) -> Result<(), String> {
    let url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
    let body = serde_json::json!({ "content": text });
    let auth = if token.starts_with("Bot ") || token.starts_with("Bearer ") {
        token.to_string()
    } else {
        format!("Bot {}", token)
    };
    let resp = ureq::post(&url)
        .set("Authorization", &auth)
        .set("Content-Type", "application/json")
        .set("User-Agent", "kastrup (https://github.com/isene/kastrup, 0.1)")
        .send_string(&body.to_string());
    match resp {
        Ok(_) => Ok(()),
        Err(ureq::Error::Status(code, r)) => {
            let msg = r.into_string().unwrap_or_default();
            Err(format!("discord bot {}: {}", code, msg))
        }
        Err(e) => Err(format!("discord bot: {}", e)),
    }
}

/// Build the multipart body Discord expects for an attachment-bearing
/// message: a `payload_json` part with the JSON content + per-file
/// `files[i]` parts. Returns `(boundary, body_bytes)`. Empty `text`
/// is allowed (Discord accepts files with no caption).
fn build_discord_multipart(text: &str, files: &[(&std::path::Path, Vec<u8>)])
    -> Result<(String, Vec<u8>), String>
{
    let boundary = format!("----kastrup-d-{:x}", crate::database::now_secs());
    let mut body: Vec<u8> = Vec::with_capacity(
        files.iter().map(|(_, b)| b.len()).sum::<usize>() + 1024
    );

    // payload_json — Discord requires this exact field name.
    body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"payload_json\"\r\n");
    body.extend_from_slice(b"Content-Type: application/json\r\n\r\n");
    let payload = serde_json::json!({ "content": text });
    body.extend_from_slice(payload.to_string().as_bytes());
    body.extend_from_slice(b"\r\n");

    for (i, (path, bytes)) in files.iter().enumerate() {
        let filename = path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("attachment");
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"files[{}]\"; filename=\"{}\"\r\n",
                i, filename).as_bytes());
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());
    Ok((boundary, body))
}

/// Upload one or more files to a Discord channel as a single message
/// (with optional caption `text`). Discord accepts up to 10 files per
/// message — we don't cap here; the API will reject oversize requests
/// itself.
pub fn discord_upload_files_to_channel(
    token: &str, channel_id: &str, text: &str, paths: &[std::path::PathBuf],
) -> Result<(), String> {
    let mut buffers: Vec<(&std::path::Path, Vec<u8>)> = Vec::with_capacity(paths.len());
    let mut owned_bufs: Vec<Vec<u8>> = Vec::with_capacity(paths.len());
    for p in paths {
        let b = std::fs::read(p).map_err(|e| format!("read {}: {}", p.display(), e))?;
        owned_bufs.push(b);
    }
    for (i, p) in paths.iter().enumerate() {
        buffers.push((p.as_path(), owned_bufs[i].clone()));
    }
    let (boundary, body) = build_discord_multipart(text, &buffers)?;
    let auth = if token.starts_with("Bot ") || token.starts_with("Bearer ") {
        token.to_string()
    } else {
        format!("Bot {}", token)
    };
    let url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
    let resp = ureq::post(&url)
        .set("Authorization", &auth)
        .set("Content-Type", &format!("multipart/form-data; boundary={}", boundary))
        .set("User-Agent", "kastrup (https://github.com/isene/kastrup, 0.1)")
        .send_bytes(&body);
    match resp {
        Ok(_) => Ok(()),
        Err(ureq::Error::Status(code, r)) => {
            Err(format!("discord upload {}: {}", code, r.into_string().unwrap_or_default()))
        }
        Err(e) => Err(format!("discord upload: {}", e)),
    }
}

/// Webhook variant: same payload format, but the destination is a
/// webhook URL and there's no bot Authorization header.
pub fn discord_upload_files_to_webhook(
    webhook_url: &str, text: &str, paths: &[std::path::PathBuf],
) -> Result<(), String> {
    let mut buffers: Vec<(&std::path::Path, Vec<u8>)> = Vec::with_capacity(paths.len());
    let mut owned_bufs: Vec<Vec<u8>> = Vec::with_capacity(paths.len());
    for p in paths {
        let b = std::fs::read(p).map_err(|e| format!("read {}: {}", p.display(), e))?;
        owned_bufs.push(b);
    }
    for (i, p) in paths.iter().enumerate() {
        buffers.push((p.as_path(), owned_bufs[i].clone()));
    }
    let (boundary, body) = build_discord_multipart(text, &buffers)?;
    let resp = ureq::post(webhook_url)
        .set("Content-Type", &format!("multipart/form-data; boundary={}", boundary))
        .send_bytes(&body);
    match resp {
        Ok(_) => Ok(()),
        Err(ureq::Error::Status(code, r)) => {
            Err(format!("discord webhook upload {}: {}", code, r.into_string().unwrap_or_default()))
        }
        Err(e) => Err(format!("discord webhook upload: {}", e)),
    }
}

pub fn discord_create_dm_pub(token: &str, user_id: &str) -> Result<String, String> {
    discord_create_dm(token, user_id)
}

fn discord_create_dm(token: &str, user_id: &str) -> Result<String, String> {
    let auth = if token.starts_with("Bot ") || token.starts_with("Bearer ") {
        token.to_string()
    } else {
        format!("Bot {}", token)
    };
    let body = serde_json::json!({ "recipient_id": user_id });
    let resp = ureq::post("https://discord.com/api/v10/users/@me/channels")
        .set("Authorization", &auth)
        .set("Content-Type", "application/json")
        .set("User-Agent", "kastrup (https://github.com/isene/kastrup, 0.1)")
        .send_string(&body.to_string());
    let text = match resp {
        Ok(r) => r.into_string().map_err(|e| e.to_string())?,
        Err(ureq::Error::Status(code, r)) => {
            return Err(format!(
                "discord create_dm {}: {}",
                code,
                r.into_string().unwrap_or_default()
            ));
        }
        Err(e) => return Err(format!("discord create_dm: {}", e)),
    };
    let v: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    Ok(v["id"].as_str().unwrap_or("").to_string())
}
