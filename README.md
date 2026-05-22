# kastrup

<img src="img/kastrup.svg" align="right" width="150">

**The fast unified messaging hub. Written in Rust.**

![Rust](https://img.shields.io/badge/language-Rust-f74c00) ![License](https://img.shields.io/badge/license-Unlicense-green) ![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-blue) ![Stay Amazing](https://img.shields.io/badge/Stay-Amazing-important)

Unified terminal messaging client. All your email, chat, and feeds in one TUI. Built on [Crust](https://github.com/isene/crust). Feature clone of [Heathrow](https://github.com/isene/Heathrow) rewritten in Rust for speed and single-binary distribution.

## Screenshot

![Screenshot](img/screenshot.png)

*Unified messaging: threaded RSS feeds (shown), mail, chat, and Workspace in one inbox.*

## Features

- **Multi-source messaging**: Maildir email, RSS/Atom feeds, WeeChat/IRC, Messenger, Instagram
- **4-pane TUI**: source/message list, message content, info bar, and status bar
- **Threading**: flat, threaded, and folder-grouped message views
- **Background sync**: automatic polling with configurable intervals per source
- **Compose/Reply/Forward**: full email composition with editor integration
- **Inline images**: Kitty protocol image display (V key)
- **Folder browser**: hierarchical Maildir folder navigation (B key)
- **Search**: substring and notmuch full-text search
- **AI assistant**: draft, summarize, translate, ask (I key)
- **Address book**: contact storage and lookup (@ key)
- **Labels and tagging**: multi-select tagging, label management
- **Customizable themes**: full 256-color theme editor with presets
- **Per-view settings**: independent sort, thread mode, and section order per view
- **Shared database**: uses the same SQLite DB as Heathrow (~/.heathrow/heathrow.db)

## Install

Download the prebuilt binary from [Releases](https://github.com/isene/kastrup/releases), or build from source:

```bash
cargo build --release
cp target/release/kastrup ~/.local/bin/
```

## Key Bindings

### Navigation
| Key | Action |
|-----|--------|
| j/k, Down/Up | Move cursor |
| h, Left | Collapse current thread (threaded view) |
| Space | Toggle thread collapse |
| Home / End | First / last message |
| PgDn / PgUp | Page down / up |
| Enter | Open message (or expand section in threaded view) |
| n / p | Next / previous unread |
| J | Jump to date |
| G | Cycle view mode (flat / threaded / folder) |

### Views
| Key | Action |
|-----|--------|
| A | All messages |
| N | New (unread) |
| Ctrl-S | Sources management |
| 0-9 | Custom views |
| F1-F12 | Extended custom views |
| F | Favorites browser |
| L | Load more messages |
| Ctrl-R | Refresh current view |
| Ctrl-F | Edit filter |
| K | Kill (close) view |

### Message Operations
| Key | Action |
|-----|--------|
| R | Toggle read / unread |
| M | Mark all as read |
| `*` / `-` | Toggle star |
| t / T | Tag message / tag all |
| Ctrl-T | Tag by regex |
| d | Mark for deletion |
| `<` | Purge deleted |
| u / U | Mark unseen |
| Shift-Space | Mark browsed as read |

### Compose & Reply
| Key | Action |
|-----|--------|
| r | Reply |
| e | Reply in editor |
| g | Reply-all |
| f | Forward (then `i` inline / `a` attach) |
| m | Compose new |
| E | Edit draft |

### Attachments & External
| Key | Action |
|-----|--------|
| v | View / save attachments |
| V | Toggle inline image |
| D | Download images to disk |
| x | Open in external app |
| X | Open HTML in browser |

### Search & AI
| Key | Action |
|-----|--------|
| / | Search messages (notmuch + DB substring fallback) |
| **S** | **`:search`** — natural-language query → claude translates to a `Filters` JSON spec → applied to the message list |
| @ | Address book |
| l | Label message |
| s | File / save message |
| `+` | Add to favorites |
| I | AI assistant menu (draft / summarize / translate / ask) |
| **c** | **`:claude PROMPT`** — pipe message + custom prompt to `claude -p`, response shown in the right pane |
| **C** | **`:chat`** — suspend kastrup, open interactive `claude` with the current message snapshot as context |
| **`:`** | **Colon prompt** — type any verb explicitly: `:claude PROMPT`, `:search QUERY`, `:chat`, `:q`/`:quit` |
| Esc | Clear sticky search, return to current view |
| Z | Open in Tock (calendar events) |

### UI
| Key | Action |
|-----|--------|
| o | Cycle sort order |
| i | Invert sort |
| w / W | Cycle pane width forward / back |
| H | Set top-bar (view) colour |
| B | Folder browser |
| Ctrl-B | Cycle border style |
| P | Preferences |
| y / Y | Copy message ID / copy right pane content |
| Ctrl-L | Force redraw |
| ? | Help (press again for extended help) |
| q | Quit |

## Configuration

Config file: `~/.heathrow/config.yml`

On first run, Kastrup creates the database and guides you through initial setup (default email, editor, SMTP command).

### External fetch scripts (messenger / instagram)

Both `messenger` and `instagram` source types shell out to a Python script that drives a Marionette-controlled Firefox. The default location is `~/.kastrup/plugins/<name>.py`. Override per-source with:

```yaml
fetch_script: ~/path/to/messenger_fetch.py
```

`~/` and `$HOME` are expanded.

### Views

Views are user-defined filter recipes stored in the `views` table. Each row's `filters` is a JSON blob with a `rules` array of `{field, op, value}` triples:

```json
{"rules": [{"field":"folder","op":"like","value":"python.slack.|irc.libera."}]}
```

To express OR across heterogeneous criteria — e.g. one view for "everything related to project Foo, across email AND Slack AND IRC" — use `branches` instead of (or together with) `rules`. Each branch is an independent rule set; results are unioned:

```json
{
  "name": "Foo",
  "branches": [
    {"rules": [{"field":"folder","op":"=","value":"AA.Customers.Foo"}]},
    {"rules": [{"field":"folder","op":"like","value":"python.slack.workspace.#foo"}]},
    {"rules": [{"field":"sender","op":"like","value":"foo"}]}
  ],
  "top_bg": "24"
}
```

There's nothing source-specific about which view-key gets a "chat" badge or layout — any view (numbered or F-key) can mix mail folders, chat channels, and sender / content patterns however the user wants.

Supported rule fields: `read`, `starred`, `folder`, `source_id`, `source_type`, `sender`. Supported `op` values: `=` (exact) and `like` (SQL `LIKE %value%`, pipe-separated value gives OR-of-LIKE within the field).

## Architecture

A detailed walk-through of the receive flows, send flows, auth matrix, threads and shared state, and the dedup scheme lives in [`docs/architecture.html`](docs/architecture.html). Open it locally for the rendered diagrams.

## Dependencies

Runtime: SQLite (bundled). Optional: `notmuch` (search), `montage` (multi-image compositing), `curl` (RSS feeds).

## Part of the Rust Terminal Suite

See the [Fe₂O₃ suite overview](https://github.com/isene/fe2o3) and the [landing page](https://isene.org/fe2o3/) for the full list of projects.

| Tool | Clones | Description |
|------|--------|-------------|
| [rush](https://github.com/isene/rush) | [rsh](https://github.com/isene/rsh) | Shell |
| [crust](https://github.com/isene/crust) | [rcurses](https://github.com/isene/rcurses) | TUI library |
| [kastrup](https://github.com/isene/kastrup) | [Heathrow](https://github.com/isene/heathrow) | Messaging hub |
| [pointer](https://github.com/isene/pointer) | [RTFM](https://github.com/isene/RTFM) | File manager |
| [scroll](https://github.com/isene/scroll) | [brrowser](https://github.com/isene/brrowser) | Web browser |
| [crush](https://github.com/isene/crush) | - | Rush config helper |

## License

[Unlicense](https://unlicense.org/) - public domain.

## Credits

Created by Geir Isene (https://isene.org) with extensive pair-programming with Claude Code.
