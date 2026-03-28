#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_ROOT="${1:-$HOME/.codex/chat_backup}"
TARGET_ROOT="${TARGET_ROOT/#\~/$HOME}"
TARGET_ROOT="$(mkdir -p "$TARGET_ROOT" && cd "$TARGET_ROOT" && pwd)"
LINK_WORKSPACE="${2:-}"

SAFE_NAME="$(basename "$TARGET_ROOT" | tr -cs '[:alnum:]' '-' | sed 's/^-*//;s/-*$//')"
LABEL="com.codex.chatlog.sync.${SAFE_NAME}"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
RUNNER_DIR="$HOME/.codex/bin"
RUNNER="$RUNNER_DIR/sync_codex_chat_logs.py"

mkdir -p "$HOME/Library/LaunchAgents" "$TARGET_ROOT/chat_logs" "$RUNNER_DIR"
cp "$SCRIPT_DIR/sync_codex_chat_logs.py" "$RUNNER"
chmod +x "$RUNNER"

cat >"$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/env</string>
    <string>python3</string>
    <string>$RUNNER</string>
    <string>--workspace</string>
    <string>$TARGET_ROOT</string>
  </array>
  <key>StartInterval</key>
  <integer>60</integer>
  <key>RunAtLoad</key>
  <true/>
  <key>WorkingDirectory</key>
  <string>$TARGET_ROOT</string>
  <key>StandardOutPath</key>
  <string>$TARGET_ROOT/chat_logs/sync.log</string>
  <key>StandardErrorPath</key>
  <string>$TARGET_ROOT/chat_logs/sync.err.log</string>
</dict>
</plist>
EOF

UID_NUM="$(id -u)"
launchctl bootout "gui/$UID_NUM" "$PLIST" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$UID_NUM" "$PLIST"
launchctl enable "gui/$UID_NUM/$LABEL" >/dev/null 2>&1 || true
launchctl kickstart -k "gui/$UID_NUM/$LABEL"

if [[ -n "$LINK_WORKSPACE" ]]; then
  LINK_WORKSPACE="${LINK_WORKSPACE/#\~/$HOME}"
  LINK_WORKSPACE="$(mkdir -p "$LINK_WORKSPACE/chat_logs" && cd "$LINK_WORKSPACE" && pwd)"
  ln -sfn "$TARGET_ROOT/chat_logs/sessions" "$LINK_WORKSPACE/chat_logs/auto_sessions"
fi

echo "Installed and started: $LABEL"
echo "Plist: $PLIST"
echo "ArchiveRoot: $TARGET_ROOT"
if [[ -n "$LINK_WORKSPACE" ]]; then
  echo "Symlink: $LINK_WORKSPACE/chat_logs/auto_sessions -> $TARGET_ROOT/chat_logs/sessions"
fi
