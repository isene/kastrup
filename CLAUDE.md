# Kastrup

Rust feature clone of [Heathrow](https://github.com/isene/heathrow), a unified terminal messaging hub.

Multi-source email and messaging client with thread view, tagging, search, and AI integration. Built on Crust.

## Build

```bash
PATH="/usr/bin:$PATH" cargo build --release
```

Note: `PATH` prefix needed to avoid `~/bin/cc` (Claude Code sessions) shadowing the C compiler.
