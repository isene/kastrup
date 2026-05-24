# Roadmap

Snapshot as of v0.1.126. Items grouped by realistic scope. Tractable items get knocked off as time allows; "shelf" items need their own focused session each.

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
- **v0.1.116** — M8.1: nested email threading (DFS-walk over `In-Reply-To` / `References` headers; indent rail in left pane).
- **v0.1.117** — Pulled in glow v0.1.14: first inline image V on any new attachment drops from ~250-500 ms (magick subprocess) to ~10-50 ms (in-process `image` crate decode + Triangle resize + cell-aligned pad). Repeat shows already instant via Phase 1 disk PNG cache (glow v0.1.13).
- **v0.1.118** — M8.2 (v/V across all chat sources): Discord and Instagram attachment objects now carry `kastrup_remote: true` and `source_type` so the unified fetch path downloads from their CDNs without leaking Slack auth headers. Discord file send: `.discord` drafts gain `Attach:` headers and post via a single `multipart/form-data` to `POST /channels/<id>/messages` (bot, webhook, or DM target). `/me` send: bodies starting with `/me ` route to Slack `chat.meMessage`, Discord `_italic_` markdown, and weechat's native `input` handler (IRC `ACTION` / Slack action). Editor completion socket at `~/.kastrup/completion.sock` — scribe/vim can query `NICKS [substr]`, `NICKS_IN <folder> [substr]`, `CHANNELS [substr]` over a Unix-domain socket; one-shot request/response, no polling.
- **v0.1.119** — Pressing a function key (or any other view key) with no matching view in the DB used to fall through to `Filters::default()` and silently show the entire database; now shows "No view bound to <key>" and leaves the current view in place.
- **v0.1.120** — Stripped wee-slack's `_<NN>` colour-palette prefix from prefix-derived nicks too (previously only stripped from tag-derived nicks), fixing the user appearing as `_16<nick>` in some live push-event echoes.
- **v0.1.121-v0.1.124** — `s`-save guards: refuse non-mail messages (chat messages no longer get their DB folder rewritten to a maildir path), cursor lookup is mode-aware in folders / threaded view (was reading the wrong message at the same numeric index), folder browse via B/F now defaults to flat view, and `rebuild_display` after save so the moved message disappears without a restart.
- **v0.1.125-v0.1.126** — M9: **AI-assisted triage** (`z` key). Claude reads the current message, optional free-text hint, and emits a JSON action plan (calendar events → ICS in `~/.tock/incoming/` for tock; todos → `~/.tasks/todo.hl` appended atomically under the right category so a scribe buffer reloads cleanly). Multi-pick preview before commit. `:triage` shows the last 20 decisions from `~/.kastrup/triage.log`. Defaults install on first `z` use; both prompt and wrapper are user-editable in `~/.kastrup/`.

## Tractable next (small/medium, ~half-day each)

_None at the moment — all previously-tractable items shipped in v0.1.118._

## Shelf items (multi-day, each its own session)

### scroll → Servo embed

Hybrid plan: keep the existing boa text renderer for plain HTML, layer in a Servo-backed surface for SPAs. Started 2026-05-06 in a parallel session; status is independent of kastrup.

### Reactions on chat messages

Slack and Discord both support emoji reactions; the relay exposes them as message metadata. Kastrup currently throws this away. Work: extend `MessageData` to carry reaction summaries, render a compact line under the message, add a key to add/remove a reaction from the current message. Per-platform API surface.

### Edit / delete sent messages

A key to edit, another to delete one's own message in Slack/Discord. Slack: `chat.update` / `chat.delete`. Discord: PATCH/DELETE `/channels/.../messages/<id>`. Needs confirmation UI and routing back through the cookie/bot auth paths.

### File and image upload to Messenger / Instagram

The Marionette-driven Python plugins are the only path for these — no public API. Either extend the plugin protocol with an `upload` template, or skip until the user actually needs it.

### Slack thread parent/child

Currently Slack messages in a thread share the parent's folder but lose the parent→child relationship (the wee-slack relay's `tags_array` doesn't carry `slack_thread_ts`). Would need either a Slack-API-side enrichment pass or upstream wee-slack changes before the M8.1-style nested view extends to chat.

## Done (recent, not re-listed above)

- **glow image speedup** — Phases 1 (disk PNG cache) + 2 (Rust `image` crate) shipped 2026-05-22. Phase 3 (idle preconvert thread) **shelved per battery-first directive**: would fire image decode + disk-write on every cursor move for messages the user may never V open, against "hot paths must be cold when idle." The remaining ~30 ms latency on the first show of any new image is below user-perception threshold.

## Out of scope (intentionally)

- **Sound on highlight** — system-level concern; configure the `notify-send` daemon (e.g. `dunst`) to route notifications through `paplay`.
- **Read receipts / typing indicators** — Slack-only, requires Socket Mode subscription. Cost outweighs benefit for a TUI inbox.
- **Custom theme editor** — themes live in `~/.kastrup/config.yml`; that's already as flexible as it needs to be.
- **Multi-window / split-pane chat** — pane layout is fixed at 4 panes by design.
