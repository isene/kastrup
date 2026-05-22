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
                "SLACK_API_TOKEN" => s.slack_token = Some(val),
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
    // Fallback: extract xoxp from weechat config if .env didn't set it.
    if s.slack_token.is_none() {
        if let Some(tok) = slack_token_from_weechat() {
            s.slack_token = Some(tok);
        }
    }
    s
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
pub fn slack_resolve_channel(token: &str, raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw.is_empty() { return Err("empty channel".into()); }
    if let Some(rest) = raw.strip_prefix("mpdm:") {
        let mut user_ids: Vec<String> = Vec::new();
        for handle in rest.split(',') {
            let handle = handle.trim();
            if handle.is_empty() { continue; }
            // Allow `U…` IDs verbatim, otherwise look up by handle.
            let id = if handle.starts_with('U') && handle.len() >= 9
                && handle[1..].chars().all(|c| c.is_ascii_alphanumeric())
            {
                handle.to_string()
            } else {
                slack_lookup_user_by_handle(token, handle.trim_start_matches('@'))?
            };
            user_ids.push(id);
        }
        if user_ids.is_empty() {
            return Err("mpdm: no users in target".into());
        }
        return slack_open_dm(token, &user_ids.join(","));
    }
    // Already an ID?
    if let Some(c0) = raw.chars().next() {
        if (c0 == 'C' || c0 == 'G' || c0 == 'D') && raw.len() >= 9
            && raw[1..].chars().all(|c| c.is_ascii_alphanumeric())
        {
            return Ok(raw.to_string());
        }
        if c0 == 'U' && raw.len() >= 9
            && raw[1..].chars().all(|c| c.is_ascii_alphanumeric())
        {
            return slack_open_dm(token, raw);
        }
    }
    if let Some(name) = raw.strip_prefix('#') {
        return slack_lookup_channel_by_name(token, name);
    }
    if let Some(handle) = raw.strip_prefix('@') {
        let uid = slack_lookup_user_by_handle(token, handle)?;
        return slack_open_dm(token, &uid);
    }
    // Bare alphanumeric: assume it's a channel name without the `#`.
    slack_lookup_channel_by_name(token, raw)
}

fn slack_lookup_channel_by_name(token: &str, name: &str) -> Result<String, String> {
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
        let resp = ureq::get(&url)
            .set("Authorization", &format!("Bearer {}", token))
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

fn slack_lookup_user_by_handle(token: &str, handle: &str) -> Result<String, String> {
    let target = handle.trim_start_matches('@').to_ascii_lowercase();
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let mut url = String::from("https://slack.com/api/users.list?limit=1000");
        if let Some(c) = &cursor {
            url.push_str("&cursor=");
            url.push_str(c);
        }
        let resp = ureq::get(&url)
            .set("Authorization", &format!("Bearer {}", token))
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

fn slack_open_dm(token: &str, user_id: &str) -> Result<String, String> {
    let body = serde_json::json!({ "users": user_id });
    let resp = ureq::post("https://slack.com/api/conversations.open")
        .set("Authorization", &format!("Bearer {}", token))
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
pub fn send_slack(token: &str, channel: &str, text: &str) -> Result<(), String> {
    let body = serde_json::json!({
        "channel": channel,
        "text": text,
    });
    let resp = ureq::post("https://slack.com/api/chat.postMessage")
        .set("Authorization", &format!("Bearer {}", token))
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
        let url = secrets.discord_webhooks.get(&key)
            .ok_or_else(|| format!("no DISCORD_WEBHOOK_{} in ~/.kastrup/.env", key.to_ascii_uppercase()))?;
        discord_post_webhook(url, text)?;
        return Ok(format!("webhook:{}", key));
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
