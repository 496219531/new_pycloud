#!/usr/bin/env bash
set -euo pipefail

TARGET_ROOT="${1:-$HOME/.codex/chat_backup}"
TARGET_ROOT="${TARGET_ROOT/#\~/$HOME}"
TARGET_ROOT="$(mkdir -p "$TARGET_ROOT" && cd "$TARGET_ROOT" && pwd)"
LINK_WORKSPACE="${2:-}"

SAFE_NAME="$(basename "$TARGET_ROOT" | tr -cs '[:alnum:]' '-' | sed 's/^-*//;s/-*$//')"
LABEL="com.codex.chatlog.sync.${SAFE_NAME}"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

UID_NUM="$(id -u)"
launchctl bootout "gui/$UID_NUM" "$PLIST" >/dev/null 2>&1 || true
rm -f "$PLIST"

if [[ -n "$LINK_WORKSPACE" ]]; then
  LINK_WORKSPACE="${LINK_WORKSPACE/#\~/$HOME}"
  rm -f "$LINK_WORKSPACE/chat_logs/auto_sessions"
fi

echo "Uninstalled: $LABEL"
