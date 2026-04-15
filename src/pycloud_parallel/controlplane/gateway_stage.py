from __future__ import annotations

"""Temporary stage-file management for gateway upload-call requests."""

import contextlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import shutil
import threading
import time
import uuid
from typing import Dict, List, Optional

from pycloud_parallel.controlplane.config import (
    GATEWAY_STAGE_GC_INTERVAL_SEC,
    GATEWAY_STAGE_TTL_SEC,
)


_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dt_text(value: Optional[datetime]) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _sanitize_part(value: str, *, fallback: str) -> str:
    normalized = _SAFE_NAME_RE.sub("_", str(value or "").strip())
    normalized = normalized.strip("._")
    return normalized or fallback


def default_gateway_stage_dir() -> Path:
    custom = str(os.environ.get("PYCLOUD_GATEWAY_STAGE_DIR", "") or "").strip()
    if custom:
        return Path(custom).expanduser().resolve()
    home = str(os.environ.get("PYCLOUD_HOME", "") or "").strip()
    if home:
        return (Path(home).expanduser().resolve() / "gateway_stage").resolve()
    return (Path.cwd() / "gateway_stage").resolve()


@dataclass
class GatewayStageFile:
    slot: str
    original_name: str
    content_type: str
    path: Path
    size_bytes: int = 0
    field_name: str = ""

    def as_dict(self) -> Dict[str, object]:
        return {
            "slot": self.slot,
            "field_name": self.field_name,
            "original_name": self.original_name,
            "content_type": self.content_type,
            "path": str(self.path),
            "size_bytes": int(self.size_bytes or 0),
        }


@dataclass
class GatewayStageRequest:
    request_id: str
    service_name: str
    method: str
    request_dir: Path
    files_dir: Path
    meta_path: Path
    created_at: datetime = field(default_factory=_utc_now)
    expires_at: Optional[datetime] = None
    status: str = "created"
    uploaded_files: Dict[str, GatewayStageFile] = field(default_factory=dict)
    resolved_refs: Dict[str, Dict[str, object]] = field(default_factory=dict)
    target_route: Dict[str, object] = field(default_factory=dict)


class GatewayStageManager:
    def __init__(
        self,
        *,
        root_dir: str = "",
        failure_ttl_sec: int = GATEWAY_STAGE_TTL_SEC,
        gc_interval_sec: int = GATEWAY_STAGE_GC_INTERVAL_SEC,
    ) -> None:
        self.root_dir = Path(root_dir).expanduser().resolve() if str(root_dir or "").strip() else default_gateway_stage_dir()
        self.failure_ttl_sec = max(60, int(failure_ttl_sec or GATEWAY_STAGE_TTL_SEC))
        self.gc_interval_sec = max(5, int(gc_interval_sec or GATEWAY_STAGE_GC_INTERVAL_SEC))
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = False

    @property
    def requests_dir(self) -> Path:
        return self.root_dir / "requests"

    def start(self) -> None:
        self.requests_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            if self._thread is not None:
                return
            self._stop = False
            self._thread = threading.Thread(target=self._gc_loop, name="gateway-stage-gc", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._stop = True
            thread = self._thread
            self._thread = None
        if thread is not None:
            thread.join(timeout=2.0)

    def create_request(self, *, service_name: str, method: str) -> GatewayStageRequest:
        request_id = uuid.uuid4().hex
        request_dir = self.requests_dir / request_id
        files_dir = request_dir / "files"
        files_dir.mkdir(parents=True, exist_ok=True)
        request = GatewayStageRequest(
            request_id=request_id,
            service_name=str(service_name or "").strip(),
            method=str(method or "").strip(),
            request_dir=request_dir,
            files_dir=files_dir,
            meta_path=request_dir / "meta.json",
        )
        self._write_meta(request)
        return request

    def allocate_file_path(self, request: GatewayStageRequest, *, slot: str, original_name: str) -> Path:
        safe_slot = _sanitize_part(slot, fallback="file")
        safe_name = _sanitize_part(Path(str(original_name or "")).name, fallback="upload.bin")
        return request.files_dir / f"{safe_slot}__{safe_name}"

    def record_file(self, request: GatewayStageRequest, file: GatewayStageFile) -> None:
        request.uploaded_files[file.slot] = file
        self._write_meta(request)

    def record_route(self, request: GatewayStageRequest, *, route: Dict[str, object]) -> None:
        request.target_route = dict(route or {})
        self._write_meta(request)

    def record_resolved_refs(self, request: GatewayStageRequest, *, refs_by_slot: Dict[str, Dict[str, object]]) -> None:
        request.resolved_refs = {str(slot): dict(value or {}) for slot, value in dict(refs_by_slot or {}).items()}
        self._write_meta(request)

    def mark_status(
        self,
        request: GatewayStageRequest,
        *,
        status: str,
        expires_at: Optional[datetime] = None,
    ) -> None:
        request.status = str(status or "").strip() or request.status
        request.expires_at = expires_at
        self._write_meta(request)

    def cleanup(self, request: GatewayStageRequest) -> None:
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(request.request_dir)

    def preserve_failure(self, request: GatewayStageRequest, *, status: str = "failed") -> None:
        expires_at = _utc_now() + timedelta(seconds=self.failure_ttl_sec)
        self.mark_status(request, status=status, expires_at=expires_at)

    def _write_meta(self, request: GatewayStageRequest) -> None:
        payload = {
            "request_id": request.request_id,
            "service_name": request.service_name,
            "method": request.method,
            "created_at": _dt_text(request.created_at),
            "expires_at": _dt_text(request.expires_at),
            "status": request.status,
            "uploaded_files": [item.as_dict() for item in request.uploaded_files.values()],
            "resolved_refs": dict(request.resolved_refs),
            "target_route": dict(request.target_route),
        }
        request.request_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = request.meta_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(str(tmp_path), str(request.meta_path))

    def _gc_loop(self) -> None:
        while True:
            with self._lock:
                if self._stop:
                    return
            self.run_gc_once()
            time.sleep(float(self.gc_interval_sec))

    def run_gc_once(self) -> None:
        now = _utc_now()
        requests_dir = self.requests_dir
        if not requests_dir.exists():
            return
        for request_dir in requests_dir.iterdir():
            if not request_dir.is_dir():
                continue
            meta_path = request_dir / "meta.json"
            expires_at = None
            try:
                if meta_path.exists():
                    payload = json.loads(meta_path.read_text(encoding="utf-8") or "{}")
                    raw_expires_at = str(payload.get("expires_at", "") or "").strip()
                    if raw_expires_at:
                        expires_at = datetime.fromisoformat(raw_expires_at)
                        if expires_at.tzinfo is None:
                            expires_at = expires_at.replace(tzinfo=timezone.utc)
                if expires_at is None:
                    expires_at = datetime.fromtimestamp(request_dir.stat().st_mtime, tz=timezone.utc) + timedelta(
                        seconds=self.failure_ttl_sec
                    )
                if expires_at <= now:
                    shutil.rmtree(request_dir, ignore_errors=True)
            except Exception:
                shutil.rmtree(request_dir, ignore_errors=True)
