from __future__ import annotations

"""HTTP object API for NodeControl DataRef blobs."""

import contextlib
import json
import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, Optional, Tuple
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.http_client import target_to_base_url
from pycloud_parallel.controlplane.node.object_meta import touch_object_last_at
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.controlplane.services import _expected_object_id
from pycloud_parallel.data.ref import (
    DataRef,
    normalize_object_format,
    object_id_from_sha256_hex,
)
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


MAX_OBJECT_HTTP_BODY_BYTES = 512 * 1024 * 1024


def _split_host_port(bind: str) -> Tuple[str, int]:
    if ":" not in bind:
        raise ValueError("bind must be host:port")
    host, port = bind.rsplit(":", 1)
    return host.strip(), int(port)


def _json_bytes(data: Dict[str, object]) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def _dt_text(value) -> str:
    try:
        return value.isoformat()
    except Exception:
        return ""


def _object_ref(*, object_id: str, format: str, size_bytes: int, control_addr: str = "") -> DataRef:
    return DataRef(
        ref_id=str(object_id or "").strip(),
        storage_id=str(object_id or "").strip(),
        logical_type="",
        format=str(format or "").strip(),
        size_bytes=int(size_bytes or 0),
        materialize_as="path",
        locator_kind="node_control" if str(control_addr or "").strip() else "node_local",
        locator_token=str(control_addr or "").strip(),
        control_addr=str(control_addr or "").strip(),
    )


class NodeObjectHttpApp:
    def __init__(self, state: NodeControlState, *, max_body_bytes: int = MAX_OBJECT_HTTP_BODY_BYTES) -> None:
        self.state = state
        self.max_body_bytes = max(1, int(max_body_bytes or MAX_OBJECT_HTTP_BODY_BYTES))

    def handle_get(self, path: str) -> Tuple[int, Dict[str, str], bytes]:
        parsed = urlparse(path)
        parts = [unquote(x) for x in parsed.path.split("/") if x]
        if len(parts) == 3 and parts[0] == "objects" and parts[2] == "meta":
            return self._handle_meta(parts[1])
        if len(parts) == 3 and parts[0] == "objects" and parts[2] == "download":
            return self._handle_download(parts[1])
        return 404, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": "not found"})

    def handle_post(self, path: str, headers, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        parsed = urlparse(path)
        parts = [unquote(x) for x in parsed.path.split("/") if x]
        if parts == ["objects", "upload"]:
            return self._handle_upload(headers, body)
        if len(parts) == 3 and parts[0] == "objects" and parts[2] == "pin":
            return self._handle_pin(parts[1], body)
        if len(parts) == 3 and parts[0] == "objects" and parts[2] == "release":
            return self._handle_release(parts[1], body)
        return 404, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": "not found"})

    def _handle_meta(self, object_id: str) -> Tuple[int, Dict[str, str], bytes]:
        object_id = str(object_id or "").strip()
        if not object_id:
            return 400, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": "object_id is required"})
        try:
            artifact = self.state.get_object_artifact(object_id)
        except ValueError as exc:
            return 400, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": str(exc)})
        except KeyError:
            return 200, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": True, "exists": False, "object_id": object_id})
        return 200, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes(
            {
                "ok": True,
                "exists": True,
                "object_id": artifact.object_id,
                "format": artifact.format,
                "size_bytes": int(artifact.size_bytes or 0),
                "created_at": _dt_text(artifact.created_at),
            }
        )

    def _handle_upload(self, headers, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        object_format = normalize_object_format(str(headers.get("X-Pycloud-Object-Format", "") or ""), default="bin")
        integrity_mode = str(headers.get("X-Pycloud-Integrity-Mode", "") or "").strip().lower()
        object_id = str(headers.get("X-Pycloud-Object-Id", "") or "").strip()
        if not integrity_mode:
            integrity_mode = "client_declared" if object_id else "server_authoritative"
        meta = pb2.UploadObjectMeta(object_id=object_id, format=object_format, integrity_mode=integrity_mode)

        self.state.object_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = ""
        try:
            tmp = tempfile.NamedTemporaryFile(mode="wb", prefix="pycloud-object-http-", suffix=".bin", delete=False, dir=str(self.state.object_dir))
            tmp_path = tmp.name
            with tmp:
                tmp.write(body)
            import hashlib

            digest = hashlib.sha256(body).hexdigest()
            expected_object_id = _expected_object_id(meta, digest)
            artifact, cached = self.state.data_store.put_uploaded_file(
                object_id=expected_object_id,
                format=object_format,
                uploaded_path=tmp_path,
                actual_sha256=digest,
                size_bytes=len(body),
            )
        except ValueError as exc:
            return 400, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": str(exc)})
        finally:
            if tmp_path:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(tmp_path)
        return 200, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes(
            {
                "ok": True,
                "object_id": artifact.object_id,
                "format": artifact.format,
                "cached": bool(cached),
                "size_bytes": int(artifact.size_bytes or 0),
                "created_at": _dt_text(artifact.created_at),
            }
        )

    def _handle_download(self, object_id: str) -> Tuple[int, Dict[str, str], bytes]:
        try:
            artifact = self.state.get_object_artifact(str(object_id or "").strip())
        except KeyError:
            return 404, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": "object not found"})
        except ValueError as exc:
            return 400, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": str(exc)})
        try:
            if getattr(artifact, "storage_backend", "file") == "segment":
                touch_object_last_at(self.state.object_dir, object_id=artifact.object_id, fallback_path=Path(artifact.segment_path))
                with open(artifact.segment_path, "rb") as fp:
                    fp.seek(max(0, int(getattr(artifact, "segment_offset", 0) or 0)))
                    blob = fp.read(max(0, int(getattr(artifact, "segment_length", artifact.size_bytes) or artifact.size_bytes)))
            else:
                touch_object_last_at(self.state.object_dir, object_id=artifact.object_id, fallback_path=Path(artifact.path))
                blob = Path(artifact.path).read_bytes()
        except FileNotFoundError:
            return 404, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": "object file missing"})
        return 200, {
            "Content-Type": "application/octet-stream",
            "X-Pycloud-Object-Id": artifact.object_id,
            "X-Pycloud-Object-Format": artifact.format,
            "X-Pycloud-Object-Size-Bytes": str(int(artifact.size_bytes or len(blob))),
        }, blob

    def _json_body(self, body: bytes) -> Dict[str, object]:
        if not body:
            return {}
        parsed = json.loads(body.decode("utf-8") or "{}")
        if not isinstance(parsed, dict):
            raise ValueError("json body must be object")
        return parsed

    def _handle_pin(self, object_id: str, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        try:
            payload = self._json_body(body)
            ref_id = str(payload.get("ref_id", "") or "").strip()
            pinned = self.state.pin_object(str(object_id or "").strip(), ref_id=ref_id)
        except ValueError as exc:
            return 400, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "pinned": False, "error": str(exc)})
        if not pinned:
            return 404, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "pinned": False, "error": "object not found"})
        return 200, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": True, "pinned": True})

    def _handle_release(self, object_id: str, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        try:
            payload = self._json_body(body)
            ref_id = str(payload.get("ref_id", "") or "").strip()
            released = self.state.release_object(str(object_id or "").strip(), ref_id=ref_id)
        except ValueError as exc:
            return 400, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "released": False, "error": str(exc)})
        if not released:
            return 404, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "released": False, "error": "object not found"})
        return 200, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": True, "released": True})


class NodeObjectHttpServer:
    def __init__(self, *, bind: str, state: NodeControlState, max_body_bytes: int = MAX_OBJECT_HTTP_BODY_BYTES) -> None:
        self.bind = bind
        self.app = NodeObjectHttpApp(state, max_body_bytes=max_body_bytes)
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self.base_url = ""

    def start(self) -> None:
        host, port = _split_host_port(self.bind)
        app = self.app

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                self._send(*app.handle_get(self.path))

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0") or 0)
                if length > app.max_body_bytes:
                    self._send(413, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": "payload too large"}))
                    return
                self._send(*app.handle_post(self.path, self.headers, self.rfile.read(max(0, length))))

            def log_message(self, _format, *args):  # noqa: A002
                return

            def _send(self, status_code: int, headers: Dict[str, str], raw: bytes) -> None:
                try:
                    self.send_response(int(status_code))
                    for key, value in dict(headers or {}).items():
                        self.send_header(str(key), str(value))
                    self.send_header("Content-Length", str(len(raw or b"")))
                    self.end_headers()
                    if raw:
                        self.wfile.write(raw)
                except (BrokenPipeError, ConnectionResetError):
                    return

        self._server = ThreadingHTTPServer((host, int(port)), _Handler)
        actual_port = self._server.server_address[1]
        public_host = "127.0.0.1" if host in {"", "0.0.0.0", "::"} else host
        self.base_url = f"http://{public_host}:{actual_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, name="node-object-http", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


class HttpNodeObjectClient:
    def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
        self.base_url = target_to_base_url(target)
        self.target = self.base_url
        self.control_addr = self.base_url
        self.timeout_sec = max(0.1, float(timeout_sec))

    def close(self) -> None:
        return None

    def __enter__(self) -> "HttpNodeObjectClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def upload_object_from_bytes(
        self,
        *,
        blob: bytes,
        format: str = "",
        chunk_size: int = 0,
        trusted_precheck: Optional[bool] = None,
        transfer_mode: str = "",
    ) -> DataRef:
        import hashlib

        effective_format = normalize_object_format(format, default="bin")
        payload = bytes(blob)
        digest = hashlib.sha256(payload).hexdigest()
        object_id = object_id_from_sha256_hex(digest)
        mode = str(transfer_mode or "").strip().lower()
        if not mode or mode == "auto":
            mode = "known_digest_precheck"
        if mode == "known_digest_precheck" and trusted_precheck is not False:
            existing = self._object_ref_if_exists(object_id=object_id, fallback_format=effective_format, fallback_size=len(payload))
            if existing is not None:
                return existing
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Pycloud-Object-Format": effective_format,
            "X-Pycloud-Integrity-Mode": "client_declared" if mode == "known_digest_precheck" else "server_authoritative",
        }
        if mode == "known_digest_precheck":
            headers["X-Pycloud-Object-Id"] = object_id
        data = self._request_json("POST", "/objects/upload", data=payload, headers=headers)
        return _object_ref(
            object_id=str(data.get("object_id", "") or object_id),
            format=str(data.get("format", "") or effective_format),
            size_bytes=int(data.get("size_bytes", len(payload)) or len(payload)),
            control_addr=self.base_url,
        )

    def upload_object_from_file(
        self,
        *,
        file_path: str,
        format: str = "",
        chunk_size: int = 0,
        trusted_precheck: Optional[bool] = None,
        transfer_mode: str = "",
    ) -> DataRef:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"file_path not found: {file_path}")
        effective_format = normalize_object_format(format, source_name=path.name)
        blob = path.read_bytes()
        return self.upload_object_from_bytes(
            blob=blob,
            format=effective_format,
            chunk_size=chunk_size,
            trusted_precheck=trusted_precheck,
            transfer_mode=transfer_mode,
        )

    def get_object_meta(self, *, object_id: str):
        data = self._request_json("GET", f"/objects/{quote(str(object_id or '').strip(), safe='')}/meta")
        return SimpleNamespace(
            ok=bool(data.get("ok", False)),
            exists=bool(data.get("exists", False)),
            object_id=str(data.get("object_id", "") or object_id),
            format=str(data.get("format", "") or ""),
            size_bytes=int(data.get("size_bytes", 0) or 0),
            created_at=str(data.get("created_at", "") or ""),
        )

    def has_object(self, *, object_id: str) -> bool:
        return bool(self.get_object_meta(object_id=object_id).exists)

    def _object_ref_if_exists(self, *, object_id: str, fallback_format: str, fallback_size: int) -> Optional[DataRef]:
        try:
            meta = self.get_object_meta(object_id=object_id)
        except Exception:
            return None
        if not bool(meta.exists):
            return None
        return _object_ref(
            object_id=str(meta.object_id or object_id),
            format=str(meta.format or fallback_format or "bin"),
            size_bytes=int(meta.size_bytes or fallback_size or 0),
            control_addr=self.base_url,
        )

    def download_object_bytes(self, *, object_id: str) -> bytes:
        url = f"{self.base_url}/objects/{quote(str(object_id or '').strip(), safe='')}/download"
        req = Request(url, method="GET")
        try:
            with urlopen(req, timeout=self.timeout_sec) as resp:
                return resp.read()
        except HTTPError as exc:
            raise RuntimeError(self._error_message(exc)) from exc

    def download_object_to_file(self, *, object_id: str, target_path: str) -> Path:
        path = Path(target_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.download_object_bytes(object_id=object_id))
        return path

    def pin_object(self, *, object_id: str, ref_id: str) -> bool:
        data = self._request_json("POST", f"/objects/{quote(str(object_id or '').strip(), safe='')}/pin", payload={"ref_id": str(ref_id or "")})
        return bool(data.get("pinned", False))

    def release_object(self, *, object_id: str) -> bool:
        return self.release_object_ref(object_id=object_id)

    def release_object_ref(self, *, object_id: str, ref_id: str = "") -> bool:
        data = self._request_json("POST", f"/objects/{quote(str(object_id or '').strip(), safe='')}/release", payload={"ref_id": str(ref_id or "")})
        return bool(data.get("released", False))

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: Optional[Dict[str, object]] = None,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, object]:
        request_headers = dict(headers or {})
        raw = data
        if payload is not None:
            raw = _json_bytes(payload)
            request_headers["Content-Type"] = "application/json; charset=utf-8"
        req = Request(f"{self.base_url}{path}", method=method.upper(), data=raw, headers=request_headers)
        try:
            with urlopen(req, timeout=self.timeout_sec) as resp:
                parsed = json.loads((resp.read() or b"{}").decode("utf-8") or "{}")
        except HTTPError as exc:
            raise RuntimeError(self._error_message(exc)) from exc
        if not bool(parsed.get("ok", False)):
            raise RuntimeError(str(parsed.get("error", "request failed")))
        return parsed

    def _error_message(self, exc: HTTPError) -> str:
        try:
            parsed = json.loads((exc.read() or b"{}").decode("utf-8") or "{}")
            return str(parsed.get("error", exc.reason))
        except Exception:
            return str(exc.reason)


def make_node_object_client(target: str, *, timeout_sec: float = 10.0):
    text = str(target or "").strip()
    if text.startswith(("http://", "https://")):
        return HttpNodeObjectClient(text, timeout_sec=timeout_sec)
    from pycloud_parallel.controlplane.node_control_client import NodeControlClient

    return NodeControlClient(text, timeout_sec=timeout_sec)


__all__ = [
    "HttpNodeObjectClient",
    "NodeObjectHttpApp",
    "NodeObjectHttpServer",
    "make_node_object_client",
]
