from __future__ import annotations

"""Local digest cache for file-backed object uploads."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
from typing import Dict, Optional


_CACHE_LOCK = threading.Lock()
_INDEX_CACHE_PATH: Optional[Path] = None
_INDEX_CACHE_MTIME_NS: int = -1
_INDEX_CACHE: Optional[Dict[str, Dict[str, str]]] = None


def _cache_root_dir() -> Path:
    home = str(os.environ.get("PYCLOUD_HOME", "") or "").strip()
    if home:
        return (Path(home).expanduser().resolve() / "object_digest_cache").resolve()
    return (Path.home() / ".pycloud_parallel" / "object_digest_cache").resolve()


def _cache_path() -> Path:
    return _cache_root_dir() / "index.json"


def _stable_path(path: Path) -> str:
    return str(path.expanduser().resolve())


def _file_cache_key(path: Path, *, format: str) -> str:
    stat = path.stat()
    return json.dumps(
        {
            "realpath": _stable_path(path),
            "size": int(stat.st_size),
            "mtime_ns": int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            "format": str(format or "").strip().lower(),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _load_index() -> Dict[str, Dict[str, str]]:
    global _INDEX_CACHE, _INDEX_CACHE_MTIME_NS, _INDEX_CACHE_PATH
    cache_path = _cache_path()
    try:
        stat = cache_path.stat()
    except FileNotFoundError:
        _INDEX_CACHE_PATH = cache_path
        _INDEX_CACHE_MTIME_NS = -1
        _INDEX_CACHE = {}
        return {}
    except OSError:
        _INDEX_CACHE_PATH = cache_path
        _INDEX_CACHE_MTIME_NS = -1
        _INDEX_CACHE = {}
        return {}
    mtime_ns = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))
    if _INDEX_CACHE is not None and _INDEX_CACHE_PATH == cache_path and _INDEX_CACHE_MTIME_NS == mtime_ns:
        return {key: dict(value) for key, value in _INDEX_CACHE.items()}
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8") or "{}")
    except Exception:
        _INDEX_CACHE_PATH = cache_path
        _INDEX_CACHE_MTIME_NS = mtime_ns
        _INDEX_CACHE = {}
        return {}
    if not isinstance(payload, dict):
        _INDEX_CACHE_PATH = cache_path
        _INDEX_CACHE_MTIME_NS = mtime_ns
        _INDEX_CACHE = {}
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            out[str(key)] = {
                "object_id": str(value.get("object_id", "") or "").strip(),
                "updated_at": str(value.get("updated_at", "") or "").strip(),
            }
    _INDEX_CACHE_PATH = cache_path
    _INDEX_CACHE_MTIME_NS = mtime_ns
    _INDEX_CACHE = {key: dict(value) for key, value in out.items()}
    return out


def _write_index(index: Dict[str, Dict[str, str]]) -> None:
    global _INDEX_CACHE, _INDEX_CACHE_MTIME_NS, _INDEX_CACHE_PATH
    cache_path = _cache_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cache_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(index, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(str(tmp_path), str(cache_path))
    try:
        stat = cache_path.stat()
        mtime_ns = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))
    except OSError:
        mtime_ns = -1
    _INDEX_CACHE_PATH = cache_path
    _INDEX_CACHE_MTIME_NS = mtime_ns
    _INDEX_CACHE = {key: dict(value) for key, value in index.items()}


def lookup_file_digest(path: Path, *, format: str) -> Optional[str]:
    candidate = Path(path).expanduser()
    if not candidate.exists() or not candidate.is_file():
        return None
    key = _file_cache_key(candidate, format=format)
    with _CACHE_LOCK:
        entry = _load_index().get(key)
    if not entry:
        return None
    object_id = str(entry.get("object_id", "") or "").strip()
    return object_id or None


def store_file_digest(path: Path, *, format: str, object_id: str) -> None:
    candidate = Path(path).expanduser()
    if not candidate.exists() or not candidate.is_file():
        return
    normalized_format = str(format or "").strip().lower()
    realpath = _stable_path(candidate)
    key = _file_cache_key(candidate, format=normalized_format)
    updated_at = datetime.now(timezone.utc).isoformat()
    with _CACHE_LOCK:
        index = _load_index()
        stale_keys = []
        for existing_key in index.keys():
            try:
                payload = json.loads(existing_key)
            except Exception:
                continue
            if str(payload.get("realpath", "") or "") == realpath and str(payload.get("format", "") or "") == normalized_format:
                stale_keys.append(existing_key)
        for existing_key in stale_keys:
            index.pop(existing_key, None)
        index[key] = {"object_id": str(object_id or "").strip(), "updated_at": updated_at}
        _write_index(index)


def invalidate_file_digest(path: Path, *, format: str) -> None:
    candidate = Path(path).expanduser()
    realpath = _stable_path(candidate)
    normalized_format = str(format or "").strip().lower()
    with _CACHE_LOCK:
        index = _load_index()
        stale_keys = []
        for existing_key in index.keys():
            try:
                payload = json.loads(existing_key)
            except Exception:
                stale_keys.append(existing_key)
                continue
            if str(payload.get("realpath", "") or "") == realpath and str(payload.get("format", "") or "") == normalized_format:
                stale_keys.append(existing_key)
        for existing_key in stale_keys:
            index.pop(existing_key, None)
        _write_index(index)


__all__ = [
    "invalidate_file_digest",
    "lookup_file_digest",
    "store_file_digest",
]
