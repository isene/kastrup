# Roadmap

Snapshot as of v0.1.115. Items grouped by realistic scope. Tractable items get knocked off as time allows; "shelf" items need their own focused session each.

## Recently shipped

- **v0.1.103** — M5: long-lived push connection to the weechat relay (kernel-keepalive supervisor, exp backoff). Slack reply via Web API. Folders view polish (source-themed dim/color split, `[N/M]` counter, default-collapsed). Manual section ordering via `Ctrl+Up/Down`, reset via `Ctrl+Home`. Mark-section-read on `a`, mark-view-read on `A`.
- **v0.1.104** — M6.1/M6.2/M6.3: joins/parts filtered via `tags_array`, `/me` action rendering, highlight badges in section headers and top bar, live nick lists per channel.
- **v0.1.105** — Folders view shows every subscribed buffer (even empty ones). Hide channel per view via `Ctrl+K`, restore via `Ctrl+U`.
- **v0.1.106** — Drop wee-slack workspace-root pseudo-buffer from the subscribed list.
- **v0.1.107** — Slack send via the same xoxc/xoxd browser-cookie auth wee-slack uses internally. No "via app" attribution badge.
- **v0.1.108** — Architecture doc at `docs/architecture.html`. Test fixtures and example comments replaced with generic names.
- **v0.1.109** — Messenger/Instagram fetch-script paths config-driven via `fetch_script`, defaulting to `~/.kastrup/plugins/<name>.py`.
- **v0.1.110** — `rustls-webpki` 0.103.13 (CVE patches). Channel-mismatch hint at compose time. `notify-send` on live highlight. Dead `sync_weechat_relay` polling fallback removed.
- **v0.1.111** — M7.1–M7.5: `/` honors current view scope (cross-folder chat search inside Slack/IRC view). Inactive-view badges (`1 5 F2`). `Ctrl+N` / `Ctrl+G` nick/channel pickers → system clipboard. `.slack` drafts gain `Attach:` headers; `files.upload` on send. `O` downloads + opens the first Slack file attachment.
- **v0.1.112** — `v` / `V` harmonised across sources (Slack file URLs now flow through the same attachment pipeline as email). OR-rules engine: views support `branches: [...]` for cross-source filters. Tighter top-bar badges (key-only, no count, excludes A/N/*).
- **v0.1.113** — Tolerate lying charset declarations (sender says iso-8859-1, body is actually UTF-8).
- **v0.1.114** — Killed a destructive second QP decode pass that mojibaked Norwegian bodies via accidental hex-digit pattern matching.
- **v0.1.115** — Final cleanup of doc-comment examples that still leaked workspace/identity strings.

## Tractable next (small/medium, ~half-day each)

### `O` action on Discord / Messenger / Instagram attachments

`O` (download attachment) currently knows how to fetch Slack file URLs with the Bearer + cookie auth. Other chat sources have their own attachment shapes (Discord CDN URLs, Marionette-scraped Instagram payloads) and need their own handlers. The `enrich_attachments_from_chat_urls` hook is the right place to extend.

### File send to Discord

The pattern is the same as Slack file send (`.discord` drafts gain `Attach:` headers, post via multipart upload to the channel). Implementation: roughly the Slack code mirrored against Discord's `POST /channels/<id>/messages` with `multipart/form-data`. ~150 lines.

### Compose-side editor IPC for true @-completion

The current `Ctrl+N` / `Ctrl+G` pickers copy to clipboard for paste. A nicer flow: scribe (or whatever `$EDITOR` is) asks kastrup for a live nick/channel snapshot over a Unix socket when the user types `@<tab>` or `#<tab>`. Editor-side work, but the kastrup side is just a small reader server that exposes `nick_lists` / `subscribed_buffers`.

### `/me` action send

Currently kastrup can RENDER incoming `/me` actions but can't SEND one. A `Channel:` line plus a body starting with `/me` should route to the relay's `input` command with the action prefix preserved, and to Slack via `chat.meMessage`.

## Shelf items (multi-day, each its own session)

### glow image speedup

Three phases, all in the [glow](https://github.com/isene/glow) repo, not kastrup.

- **Phase 1**: disk-persisted PNG cache so kastrup launches don't re-convert every image. Cache keyed by content hash.
- **Phase 2**: replace external converters (`magick` / `montage`) with the Rust `image` crate. Cuts per-image fork cost and dependency.
- **Phase 3**: idle preconvert in a background thread so the next image is ready before the user navigates to it.

### scroll → Servo embed

Hybrid plan: keep the existing boa text renderer for plain HTML, layer in a Servo-backed surface for SPAs. Started 2026-05-06 in a parallel session; status is independent of kastrup.

### Reactions on chat messages

Slack and Discord both support emoji reactions; the relay exposes them as message metadata. Kastrup currently throws this away. Work: extend `MessageData` to carry reaction summaries, render a compact line under the message, add a key to add/remove a reaction from the current message. Per-platform API surface.

### Edit / delete sent messages

A key to edit, another to delete one's own message in Slack/Discord. Slack: `chat.update` / `chat.delete`. Discord: PATCH/DELETE `/channels/.../messages/<id>`. Needs confirmation UI and routing back through the cookie/bot auth paths.

### File and image upload to Messenger / Instagram

The Marionette-driven Python plugins are the only path for these — no public API. Either extend the plugin protocol with an `upload` template, or skip until the user actually needs it.

### Threaded conversations as nested replies

Slack thread parent/child support. Currently messages in a thread share the parent's folder but lose the parent→child relationship. Would require `thread_ts` capture at insert time and a tree-render mode in Folders/Threaded view.

## Out of scope (intentionally)

- **Sound on highlight** — system-level concern; configure the `notify-send` daemon (e.g. `dunst`) to route notifications through `paplay`.
- **Read receipts / typing indicators** — Slack-only, requires Socket Mode subscription. Cost outweighs benefit for a TUI inbox.
- **Custom theme editor** — themes live in `~/.kastrup/config.yml`; that's already as flexible as it needs to be.
- **Multi-window / split-pane chat** — pane layout is fixed at 4 panes by design.
