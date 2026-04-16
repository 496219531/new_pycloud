from __future__ import annotations

"""Object metadata and pin/release helpers for NodeControl storage."""

import contextlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pycloud_parallel.controlplane.node.filesystem import (
    _atomic_write_json,
    _object_meta_path,
    _objects_meta_dir,
    _segment_path_from_relpath,
)
from pycloud_parallel.controlplane.state_time import utc_now
from pycloud_parallel.data.ref import normalize_object_format, normalize_object_id


def _write_object_meta(
    object_dir: Path,
    *,
    object_id: str,
    fmt: str,
    size_bytes: int,
    created_at: datetime,
    last_at: Optional[datetime] = None,
    storage_backend: str = "file",
    segment_relpath: str = "",
    segment_offset: int = 0,
    segment_length: int = 0,
    pinned_ref_ids: Sequence[str] = (),
) -> None:
    meta_dir = _objects_meta_dir(object_dir)
    meta_dir.mkdir(parents=True, exist_ok=True)
    meta_path = _object_meta_path(object_dir, object_id=object_id)
    timestamp = (last_at or created_at).astimezone(timezone.utc).isoformat()
    payload = {
        "object_id": normalize_object_id(object_id),
        "format": normalize_object_format(fmt, default="bin"),
        "size_bytes": max(0, int(size_bytes or 0)),
        "created_at": created_at.astimezone(timezone.utc).isoformat(),
        "last_at": timestamp,
        "storage_backend": str(storage_backend or "file").strip() or "file",
        "pinned_ref_ids": [str(item).strip() for item in pinned_ref_ids if str(item).strip()],
    }
    if payload["storage_backend"] == "segment":
        payload["segment_relpath"] = str(segment_relpath or "").strip()
        payload["segment_offset"] = max(0, int(segment_offset or 0))
        payload["segment_length"] = max(0, int(segment_length or 0))
    _atomic_write_json(meta_path, payload)


def _load_object_meta(object_dir: Path, *, object_id: str) -> Dict[str, Any]:
    meta_path = _object_meta_path(object_dir, object_id=object_id)
    if not meta_path.exists():
        return {}
    return json.loads(meta_path.read_text(encoding="utf-8") or "{}")


def _touch_object_last_at(object_dir: Path, *, object_id: str, fallback_path: Optional[Path] = None) -> None:
    object_root = Path(object_dir)
    meta = _load_object_meta(object_root, object_id=object_id)
    now = utc_now()
    if meta:
        meta["object_id"] = normalize_object_id(object_id)
        meta["format"] = normalize_object_format(str(meta.get("format", "") or "bin"), default="bin")
        meta["size_bytes"] = max(0, int(meta.get("size_bytes", 0) or 0))
        created_at_raw = str(meta.get("created_at", "") or "").strip()
        if not created_at_raw:
            meta["created_at"] = now.astimezone(timezone.utc).isoformat()
        meta["last_at"] = now.astimezone(timezone.utc).isoformat()
        meta["pinned_ref_ids"] = [
            str(item).strip()
            for item in list(meta.get("pinned_ref_ids") or ())
            if str(item).strip()
        ]
        _atomic_write_json(_object_meta_path(object_root, object_id=object_id), meta)
        return
    candidate = Path(fallback_path) if fallback_path is not None else None
    if candidate is None or not candidate.exists():
        return
    _write_object_meta(
        object_root,
        object_id=object_id,
        fmt=normalize_object_format("", source_name=candidate.name, default="bin"),
        size_bytes=candidate.stat().st_size,
        created_at=datetime.fromtimestamp(candidate.stat().st_ctime, tz=timezone.utc),
        last_at=now,
        storage_backend="file",
        pinned_ref_ids=(),
    )


def touch_object_last_at(object_dir: Path, *, object_id: str, fallback_path: Optional[Path] = None) -> None:
    _touch_object_last_at(object_dir, object_id=object_id, fallback_path=fallback_path)


def _normalize_pinned_ref_ids(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in values or ():
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _object_meta_pinned_ref_ids(meta: Dict[str, Any]) -> List[str]:
    return _normalize_pinned_ref_ids(list(meta.get("pinned_ref_ids") or ()))


def _pin_object_meta(object_dir: Path, *, object_id: str, ref_id: str, fallback_path: Optional[Path] = None) -> bool:
    normalized_ref_id = str(ref_id or "").strip()
    if not normalized_ref_id:
        raise ValueError("ref_id is required")
    object_root = Path(object_dir)
    meta = _load_object_meta(object_root, object_id=object_id)
    now = utc_now()
    if not meta:
        candidate = Path(fallback_path) if fallback_path is not None else None
        if candidate is None or not candidate.exists():
            return False
        _write_object_meta(
            object_root,
            object_id=object_id,
            fmt=normalize_object_format("", source_name=candidate.name, default="bin"),
            size_bytes=candidate.stat().st_size,
            created_at=datetime.fromtimestamp(candidate.stat().st_ctime, tz=timezone.utc),
            last_at=now,
            storage_backend="file",
            pinned_ref_ids=(normalized_ref_id,),
        )
        return True

    pinned = _object_meta_pinned_ref_ids(meta)
    if normalized_ref_id not in pinned:
        pinned.append(normalized_ref_id)
    meta["pinned_ref_ids"] = pinned
    meta["last_at"] = now.astimezone(timezone.utc).isoformat()
    _atomic_write_json(_object_meta_path(object_root, object_id=object_id), meta)
    return True


def _release_object_meta_pin(object_dir: Path, *, object_id: str, ref_id: str) -> Tuple[bool, bool]:
    normalized_ref_id = str(ref_id or "").strip()
    if not normalized_ref_id:
        raise ValueError("ref_id is required")
    object_root = Path(object_dir)
    meta = _load_object_meta(object_root, object_id=object_id)
    if not meta:
        return False, False
    pinned = _object_meta_pinned_ref_ids(meta)
    if normalized_ref_id in pinned:
        pinned = [item for item in pinned if item != normalized_ref_id]
    meta["pinned_ref_ids"] = pinned
    meta["last_at"] = utc_now().astimezone(timezone.utc).isoformat()
    _atomic_write_json(_object_meta_path(object_root, object_id=object_id), meta)
    return True, bool(pinned)


def _segment_has_live_refs(object_dir: Path, *, segment_relpath: str) -> bool:
    normalized_relpath = str(segment_relpath or "").strip()
    if not normalized_relpath:
        return False
    meta_dir = _objects_meta_dir(object_dir)
    if not meta_dir.exists():
        return False
    for meta_path in meta_dir.glob("*.json"):
        meta = _load_object_meta(object_dir, object_id=f"sha256:{meta_path.stem}")
        if not meta:
            continue
        if str(meta.get("storage_backend", "file") or "file").strip() != "segment":
            continue
        if str(meta.get("segment_relpath", "") or "").strip() == normalized_relpath:
            return True
    return False


def _cleanup_orphan_segment_file(object_dir: Path, *, segment_relpath: str) -> None:
    normalized_relpath = str(segment_relpath or "").strip()
    if not normalized_relpath:
        return
    if _segment_has_live_refs(object_dir, segment_relpath=normalized_relpath):
        return
    segment_path = _segment_path_from_relpath(object_dir, normalized_relpath)
    with contextlib.suppress(FileNotFoundError):
        segment_path.unlink()


__all__ = [
    "_cleanup_orphan_segment_file",
    "_load_object_meta",
    "_normalize_pinned_ref_ids",
    "_object_meta_pinned_ref_ids",
    "_pin_object_meta",
    "_release_object_meta_pin",
    "_segment_has_live_refs",
    "_touch_object_last_at",
    "_write_object_meta",
    "touch_object_last_at",
]
