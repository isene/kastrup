#!/bin/bash
# kastrup triage wrapper.
#
# stdin:  JSON describing the message (subject, sender, folder, body, ...)
# stdout: JSON array of action objects (see triage-prompt.txt)
# exit:   0 on success (stdout is valid JSON array)
#         1 on transport / parse failure (stderr explains)
#
# Called by kastrup's Ctrl+t handler. Keep this thin — prompt logic lives
# in ~/.kastrup/triage-prompt.txt; this script just glues stdin → claude
# → JSON validation → stdout.

set -uo pipefail

PROMPT_FILE="$HOME/.kastrup/triage-prompt.txt"
if [ ! -f "$PROMPT_FILE" ]; then
  echo "missing prompt file: $PROMPT_FILE" >&2
  exit 1
fi

# Slurp the message context JSON
input=$(cat)
if [ -z "$input" ]; then
  echo "empty stdin" >&2
  exit 1
fi

# Quick sanity: input must parse as JSON
if ! echo "$input" | jq empty 2>/dev/null; then
  echo "stdin is not valid JSON" >&2
  exit 1
fi

# Shell out to claude. Use the user's normal OAuth/subscription auth
# (NOT --bare, which strictly requires ANTHROPIC_API_KEY).
# --no-session-persistence avoids saving a session file per triage.
# --output-format json wraps the response in an envelope ({result, ...});
# we extract .result and re-validate it as our action array.
envelope=$(
  claude --print --output-format json --no-session-persistence \
    --system-prompt "$(cat "$PROMPT_FILE")" \
    "$input" 2>&1
)
status=$?
if [ $status -ne 0 ]; then
  echo "claude failed (exit $status): $envelope" >&2
  exit 1
fi

# Extract the model's text response from the JSON envelope.
result=$(echo "$envelope" | jq -r '.result // empty' 2>/dev/null)
if [ -z "$result" ]; then
  echo "claude returned no result. envelope: $envelope" >&2
  exit 1
fi

# Strip any accidental code-fence wrapping the model emitted.
result=$(echo "$result" \
  | sed -E 's/^```(json)?[[:space:]]*//; s/```[[:space:]]*$//' \
  | sed '/^$/d')

# Final validation: must be a JSON array.
if ! echo "$result" | jq -e 'type == "array"' >/dev/null 2>&1; then
  echo "claude response is not a JSON array: $result" >&2
  exit 1
fi

# Emit the array to stdout
echo "$result"
