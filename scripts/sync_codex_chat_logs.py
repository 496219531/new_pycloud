#!/usr/bin/env python3
"""Sync Codex local session jsonl files into readable markdown chat logs."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass
class Message:
    timestamp: str
    role: str
    phase: str
    text: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--codex-home",
        default=os.environ.get("CODEX_HOME", str(Path.home() / ".codex")),
        help="Codex home directory that contains sessions/.",
    )
    parser.add_argument(
        "--workspace",
        default=os.getcwd(),
        help="Workspace root where chat_logs/ will be written.",
    )
    parser.add_argument(
        "--out-dir",
        default="chat_logs/sessions",
        help="Output directory relative to workspace.",
    )
    return parser.parse_args()


def iter_session_jsonl(codex_home: Path) -> Iterable[Path]:
    sessions_root = codex_home / "sessions"
    if not sessions_root.exists():
        return []
    return sorted(sessions_root.glob("*/*/*/rollout-*.jsonl"))


def extract_text(content_items: list[dict]) -> str:
    parts: list[str] = []
    for item in content_items:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            parts.append(text.strip())
    return "\n\n".join(parts).strip()


def parse_messages(jsonl_path: Path) -> list[Message]:
    messages: list[Message] = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if obj.get("type") != "response_item":
                continue
            payload = obj.get("payload") or {}
            if payload.get("type") != "message":
                continue

            role = payload.get("role", "")
            if role not in {"user", "assistant"}:
                continue

            text = extract_text(payload.get("content") or [])
            if not text:
                continue

            messages.append(
                Message(
                    timestamp=obj.get("timestamp", ""),
                    role=role,
                    phase=payload.get("phase", ""),
                    text=text,
                )
            )
    return messages


def render_markdown(source: Path, messages: list[Message]) -> str:
    lines = [
        "# Codex Chat Session",
        "",
        f"- Source: `{source}`",
        f"- Messages: `{len(messages)}`",
        "",
    ]

    for msg in messages:
        role = "User" if msg.role == "user" else "Assistant"
        header = f"## {role}"
        meta = []
        if msg.timestamp:
            meta.append(msg.timestamp)
        if msg.phase:
            meta.append(msg.phase)
        if meta:
            header += f" ({' | '.join(meta)})"
        lines.append(header)
        lines.append(msg.text)
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_if_changed(path: Path, content: str) -> bool:
    if path.exists():
        old = path.read_text(encoding="utf-8")
        if old == content:
            return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def main() -> int:
    args = parse_args()
    codex_home = Path(args.codex_home).expanduser().resolve()
    workspace = Path(args.workspace).expanduser().resolve()
    out_root = (workspace / args.out_dir).resolve()
    session_root = codex_home / "sessions"

    scanned = 0
    written = 0
    for src in iter_session_jsonl(codex_home):
        scanned += 1
        messages = parse_messages(src)
        if not messages:
            continue
        rel = src.relative_to(session_root).with_suffix(".md")
        dst = out_root / rel
        md = render_markdown(src, messages)
        if write_if_changed(dst, md):
            written += 1

    print(f"Scanned={scanned} Written={written} OutDir={out_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
