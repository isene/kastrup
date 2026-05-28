# Handover: wire the phone `relay` gateway into kastrup (replaces Marionette)

Written by the nomad (mobile) CC session for the kastrup (Rust) CC session.
Goal: finish wiring the phone notification gateway into kastrup and retire the
Firefox/Marionette scrape of Instagram + Messenger.

## TL;DR

A new Android app, **`relay`** (`com.isene.relay`, in
`/home/geir/Main/G/GIT-isene/nomad/apps/relay/`), captures incoming messages
on the phone and sends replies, ferrying them to/from kastrup over a Syncthing
folder. It **replaces the laptop Marionette** for Instagram + Messenger and
**adds WhatsApp + SMS**.

Note: Marionette FF has been disabled in `~/.tilerc` since 2026-05-05, and there
are no `*_fetch.py` scripts in `~/.kastrup/plugins/`, so the existing
`messenger`/`instagram` sources have been silent no-ops. This gateway restores
that lost functionality and extends it.

The inbound half is **done and validated on-device** (all four platforms
captured correctly). The outbound (reply) half and the source config are the
remaining work, plus cleaning Marionette out of startup.

## How the gateway works (the contract)

Syncthing folder id **`kastrup-gw`**, laptop path **`~/.kastrup/gateway/`**
(this device "juba" ↔ phone), already created and shared. Subdirs:

- `inbound/` — phone → laptop. The relay writes one JSON per captured message:
  ```json
  {"platform":"messenger","thread_key":"Alice","sender":"Alice",
   "text":"hi","timestamp":1779983415,"group":false}
  ```
- `outbox/` — laptop → phone. kastrup writes a reply request; relay fires it:
  ```json
  {"platform":"messenger","thread_key":"Alice","text":"on my way"}
  ```
- `sent/` — relay writes an ack per request: `{"request":"<file>","ok":true,"ts":...}`.

`platform` ∈ `instagram | messenger | whatsapp | telegram | signal | sms`.
`thread_key` is the conversation display name (chat apps) or the phone number
(SMS). It is BOTH the reply target AND kastrup's `thread_id` for these messages
— the only stable id both sides observe.

**Constraints (by design, accepted):**
- Chat replies (instagram/messenger/whatsapp/…) only work while the thread has
  a *live notification* on the phone (someone who recently messaged). No new
  conversations, no replying to dormant threads, no history backfill.
- **SMS is the exception**: native `SmsManager`, so it sends to ANY number, no
  active-notification needed.

## Already done in kastrup (committed locally, NOT pushed)

- `src/sources/gateway.rs` (commits **d1ff4d2** + **307db1c**):
  - `sync_gateway(config, known_ids) -> Vec<MessageData>` — drains
    `<gateway_dir>/inbound/*.json` into `MessageData`. Per-platform labels
    (Messenger/Instagram/WhatsApp/Telegram/Signal/SMS). `external_id =
    gw_{platform}_{thread_key}_{timestamp}` (dedups re-delivery). `thread_id` =
    `metadata.thread_key`; `metadata` carries `{platform, thread_key, group,
    source:"gateway"}`. Drain-on-read (like tock `incoming/`).
  - `queue_reply(config, platform, thread_key, text) -> Result<(),String>` —
    writes an `outbox/` reply request. **Ready, not yet called.**
  - `gateway_dir` config defaults to `~/.kastrup/gateway` if unset.
- `src/sources/mod.rs` — `pub mod gateway;`
- `src/poller.rs` — `"gateway" => sources::gateway::sync_gateway(...)` arm.

## TODO for this session

### 1. Add the gateway source + disable the Marionette ones

In `~/.kastrup/kastrup.db` (current rows: `5|Messenger|messenger|1`,
`6|Instagram|instagram|1`, both `["read"]`):

```sql
-- capabilities include "send" so the reply path is offered for these messages
INSERT INTO sources (name, plugin_type, config, capabilities, created_at, updated_at, poll_interval)
VALUES ('Gateway (phone)', 'gateway', '{"gateway_dir":"~/.kastrup/gateway"}', '["read","send"]', strftime('%s','now'), strftime('%s','now'), 30);

UPDATE sources SET enabled = 0 WHERE plugin_type IN ('messenger','instagram');
```

(Or do it via `Database::add_source` then a disable call — match how the app
manages sources. Pick a poll_interval that gives low reply/receive latency;
inbound files land via Syncthing whenever the phone captures.)

### 2. Wire outbound reply (the real work)

kastrup has no IG/Messenger send path today (`DraftKind` = Email/Slack/Discord/
Weechat; `chat_send.rs` is Slack/Discord only). Add a gateway reply path:

- When the user replies to a message whose `metadata.source == "gateway"`,
  route the send to `sources::gateway::queue_reply(&source.config, platform,
  thread_key, text)` instead of email/slack/discord. `platform` and
  `thread_key` come straight from the message's `metadata`.
- Likely shape: a `DraftKind::Gateway` (or per-message reply that reads the
  metadata), pre-filling the target from the message being replied to. The
  reply UI should make clear it's a chat reply (active-thread only) vs SMS
  (any number).
- Optional polish: after writing to `outbox/`, watch `sent/` for the matching
  `<request>.ack` and surface ok/failure (e.g. "sent" vs "no active thread").

### 3. Optional: instant inbound pickup

Currently inbound is drained on the poller tick. For near-instant receive, add
the gateway `inbound/` dir to kastrup's inotify watch (same mechanism as the
maildir forced-scan) and trigger a forced poll on a new file. Event-driven, so
no extra idle cost — fits the battery-first rule.

## Remove Marionette

- `~/.tilerc`: the Marionette FF `exec` is already commented (line ~18). Clean
  up the now-dead lines: the commented `exec firefox --marionette ...`, the
  `stash-on-map FirefoxMarionette` rule (~line 46), and the marionette mention
  in the `Mod4+o unstash` comment (~line 113). **Keep** the plain `exec firefox`
  (line ~16) — that's the main browser, unrelated.
- `src/sources/messenger.rs` + `src/sources/instagram.rs`: now superseded.
  Safe to leave in place (sources disabled) or delete for cleanliness — your
  call. If deleting, drop their `pub mod` lines in `mod.rs` and the
  `"messenger"`/`"instagram"` arms in `poller.rs`.
- No `*_fetch.py` scripts exist to remove. If a marionette helper lingers
  elsewhere (old `~/.kastrup/plugins/`), remove it.

## Validation status (phone side, done)

relay **v0.2.1** installed; Instagram, Messenger, WhatsApp (notification-based)
and SMS (native) all captured correctly into `inbound/`, emoji intact, senders/
numbers parsed right, duplicate re-posts deduped. The capture → Syncthing →
`~/.kastrup/gateway/inbound/` path is confirmed working end-to-end.
