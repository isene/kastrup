# Roadmap

Snapshot as of v0.1.110. Items grouped by realistic scope. Tractable items get knocked off as time allows; "shelf" items need their own focused session each.

## Recently shipped

- **v0.1.103** — M5: long-lived push connection to the weechat relay (kernel-keepalive driven, exp backoff supervisor). Slack reply via Web API. Folders view polish (channels themed by source, dim/colored split, `[N/M]` counter, default-collapsed in Folders mode). Manual section ordering via `Ctrl+Up/Down`, reset via `Ctrl+Home`. Mark-section-read on `a`, mark-view-read on `A`.
- **v0.1.104** — M6.1/M6.2/M6.3: joins/parts filtered via `tags_array`, `/me` action rendering, highlight badges in section headers and top bar, live nick lists per channel (data collected, consumer pending).
- **v0.1.105** — Folders view shows every subscribed buffer (even empty ones). Hide channel per view via `Ctrl+K`, restore via `Ctrl+U`.
- **v0.1.106** — Drop workspace-root pseudo-buffer (`<workspace>` with no channel suffix) from the subscribed list.
- **v0.1.107** — Slack send via the same xoxc/xoxd browser-cookie auth wee-slack uses internally. No "via app" attribution badge.
- **v0.1.108** — Architecture doc at `docs/architecture.html`. Personally-identifying examples and test fixtures replaced with generic names.
- **v0.1.109** — Messenger/Instagram fetch-script paths are now config-driven via `fetch_script` in the source config, defaulting to `~/.kastrup/plugins/<name>.py`.
- **v0.1.110** — Bumped `rustls-webpki` to 0.103.13 (patches three GHSA advisories). Channel-mismatch hint shown in the status line at compose time. Desktop notification (`notify-send`) on live highlight. Dead `sync_weechat_relay` polling fallback removed. README links to the architecture doc and documents the `fetch_script` key.

## Tractable next (small/medium, ~half-day each)

### @-mention and #channel completion in compose

Status: **data ready, consumer missing.** The supervisor maintains a shared `nick_lists: Arc<Mutex<HashMap<buffer, BTreeSet<nick>>>>` and a `subscribed_buffers: Arc<Mutex<Vec<SubscribedBuffer>>>`. Kastrup compose hands off to an external editor (`scribe`/`vim`/`$EDITOR`), so completion can't be a simple inline Tab handler. Two viable approaches:

- **Pre-compose lookup**: an "insert reference" key (e.g. `Ctrl+@` to fuzzy-pick a nick, `Ctrl+#` for a channel) that drops the chosen handle into a yank buffer the user pastes in the editor.
- **Editor-side hook**: scribe gains an IPC reader for kastrup's nick/channel snapshot. Larger change, touches multiple repos.

The first option is roughly a day of work; the second is a multi-day cross-repo effort.

### Cross-folder chat search

Current `S` (search) works against the current view's message scope. Chat-specific UX would search across all weechat-relay folders, group hits by channel, and offer a "jump to channel + scroll to date" action. Needs UI design for the result list.

### View-strip badge for inactive views

The top bar shows `!K` for unread highlights in the **current** view only. The user can't see at a glance that F2 has a mention waiting while F1 is focused. Implementation: one cached query (`SELECT folder, count(*) FROM messages WHERE read=0 AND json_extract(metadata, '$.highlight')=1 GROUP BY folder`) on a ~5s tick. Map each view's filter to a subset of those folders. Render F-key labels with badges.

## Shelf items (multi-day, each its own session)

### glow image speedup

Three phases, queued from earlier sessions. All in the [glow](https://github.com/isene/glow) repo, not kastrup.

- **Phase 1**: disk-persisted PNG cache so kastrup launches don't re-convert every image. Cache keyed by content hash.
- **Phase 2**: replace external converters (`magick` / `montage`) with the Rust `image` crate. Cuts per-image fork cost and dependency.
- **Phase 3**: idle preconvert in a background thread so the next image is ready before the user navigates to it.

### scroll → Servo embed

Hybrid plan: keep the existing boa text renderer for plain HTML, layer in a Servo-backed surface for SPAs. Started 2026-05-06 in a parallel session; status is independent of kastrup.

### Reactions on chat messages

Slack and Discord both support emoji reactions; the relay exposes them as message metadata. Kastrup currently throws this away. Work: extend MessageData to carry reaction summaries, render a compact line under the message, add a key to add/remove a reaction from the current message. Per-platform API surface.

### Edit / delete sent messages

`r` to edit, `Ctrl+D` to delete one's own message in Slack/Discord. Slack API: `chat.update` and `chat.delete`. Discord API: PATCH/DELETE `/channels/.../messages/<id>`. Requires UI for confirming the action and routing back through the cookie/bot auth paths.

### File and image send & download in messages

Across Slack, Discord, Messenger, Instagram. Two halves:

- **Download**: when a message has an attachment URL, fetch (using the right auth per platform) into `~/.kastrup/attachments/<id>/<file>`, render inline previews where possible (images via glow, others as `[attachment: file.pdf]` with `o` to open).
- **Upload**: an `f` key in compose that prompts for a file path, uploads to the target platform's file API, and references the result in the message body.

Per-platform notes:
- Slack: `files.upload` for upload; downloads are CDN URLs requiring the Bearer token.
- Discord: multipart `application/octet-stream` POST to a channel + file.
- Messenger / Instagram: requires the Marionette-driven Python fallback because there's no documented public API. Probably script-only.

Realistic scope: each platform is a half-day to a day. Whole feature is roughly a week.

## Definitely-out-of-scope-here

- **Sound on highlight** — system-level concern; configure `notify-send` daemon to route notifications through `paplay`.
- **Read receipts / typing indicators** — Slack-only, requires Socket Mode subscription. Cost outweighs benefit for a TUI inbox.
- **Threaded conversations rendered as nested replies** — Slack thread parent/child support. Currently messages from a thread share the parent's channel folder but lose their parent-child relationship. Would require thread_ts capture and a tree-render mode.
