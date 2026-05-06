from __future__ import annotations

"""Multipart upload parsing and payload rewrite helpers for gateway upload-call."""

import contextlib
from copy import deepcopy
from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any, BinaryIO, Dict, Optional, Sequence, Tuple
import uuid

from .client_transport import _decode_http_request_body_with_mode
from pycloud_parallel.controlplane.config import get_gateway_dataref_relay
from pycloud_parallel.data.ref import DataRef, maybe_data_ref, with_data_ref_locator
from pycloud_parallel.controlplane.data_registry import DataRegistryClient, resolve_data_ref
from pycloud_parallel.controlplane.gateway_stage import GatewayStageFile, GatewayStageManager, GatewayStageRequest
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.data.ref import normalize_object_format


_FILE_FIELD_RE = re.compile(r"^files?\[(?P<slot>[^\]]+)\]$")
_FILE_SENTINEL_PREFIX = "__file__:"
_PATH_TOKEN_RE = re.compile(r"([^[.\]]+)|\[(\d+)\]")


class GatewayUploadError(ValueError):
    pass


@dataclass
class ParsedGatewayUploadCall:
    request: GatewayStageRequest
    payload: Dict[str, object]
    serialization_mode: str
    file_map: Dict[str, str]
    files: Dict[str, GatewayStageFile]
    used_slots: Tuple[str, ...] = ()


class _LimitedStream:
    def __init__(self, stream: BinaryIO, *, remaining: int) -> None:
        self._stream = stream
        self._remaining = max(0, int(remaining or 0))

    def read(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        if size is None or int(size) < 0 or int(size) > self._remaining:
            size = self._remaining
        chunk = self._stream.read(size)
        if not chunk:
            self._remaining = 0
            return b""
        self._remaining = max(0, self._remaining - len(chunk))
        return chunk


def is_gateway_upload_call_path(path: str) -> bool:
    parts = [item for item in str(path or "").split("?")[0].split("/") if item]
    return len(parts) == 4 and parts[0] == "svc" and parts[2] == "upload-call"


def _parse_boundary(headers) -> bytes:
    if hasattr(headers, "get_boundary"):
        boundary = headers.get_boundary()
        if boundary:
            return str(boundary).encode("utf-8")
    raw = str(headers.get("Content-Type", "") or "").strip()
    match = re.search(r'boundary=(?:"([^"]+)"|([^;]+))', raw, flags=re.IGNORECASE)
    if not match:
        raise GatewayUploadError("multipart boundary is required")
    boundary = match.group(1) or match.group(2) or ""
    boundary = str(boundary).strip()
    if not boundary:
        raise GatewayUploadError("multipart boundary is required")
    return boundary.encode("utf-8")


def _readline(buffer: bytearray, stream: BinaryIO) -> bytes:
    while True:
        idx = buffer.find(b"\n")
        if idx >= 0:
            line = bytes(buffer[: idx + 1])
            del buffer[: idx + 1]
            return line
        chunk = stream.read(64 * 1024)
        if not chunk:
            line = bytes(buffer)
            buffer.clear()
            return line
        buffer.extend(chunk)


def _read_headers(buffer: bytearray, stream: BinaryIO) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    while True:
        line = _readline(buffer, stream)
        if not line:
            raise GatewayUploadError("unexpected end of multipart body while reading headers")
        if line in {b"\r\n", b"\n"}:
            return headers
        text = line.decode("utf-8", errors="replace").strip()
        if ":" not in text:
            raise GatewayUploadError("invalid multipart header line")
        name, value = text.split(":", 1)
        headers[str(name or "").strip().lower()] = str(value or "").strip()


def _parse_content_disposition(value: str) -> Tuple[str, Dict[str, str]]:
    parts = [item.strip() for item in str(value or "").split(";") if item.strip()]
    if not parts:
        raise GatewayUploadError("missing multipart content-disposition")
    disposition = parts[0].lower()
    params: Dict[str, str] = {}
    for item in parts[1:]:
        if "=" not in item:
            continue
        key, raw_value = item.split("=", 1)
        params[str(key or "").strip().lower()] = str(raw_value or "").strip().strip('"')
    return disposition, params


def _stream_part_body(
    *,
    buffer: bytearray,
    stream: BinaryIO,
    boundary: bytes,
    sink,
    max_bytes: int,
) -> Tuple[int, bool]:
    delimiter = b"\r\n--" + boundary
    keep_bytes = len(delimiter) + 8
    written = 0
    while True:
        idx = buffer.find(delimiter)
        if idx >= 0:
            if idx:
                chunk = bytes(buffer[:idx])
                sink(chunk)
                written += len(chunk)
                if written > max_bytes:
                    raise GatewayUploadError("uploaded file exceeds per-file limit")
            del buffer[: idx + len(delimiter)]
            while len(buffer) < 2:
                extra = stream.read(2 - len(buffer))
                if not extra:
                    raise GatewayUploadError("unexpected end of multipart body after boundary")
                buffer.extend(extra)
            final = False
            if buffer.startswith(b"--"):
                final = True
                del buffer[:2]
            while len(buffer) < 2:
                extra = stream.read(2 - len(buffer))
                if not extra:
                    raise GatewayUploadError("unexpected end of multipart body after boundary terminator")
                buffer.extend(extra)
            if not buffer.startswith(b"\r\n"):
                raise GatewayUploadError("invalid multipart boundary terminator")
            del buffer[:2]
            return written, final

        chunk = stream.read(64 * 1024)
        if not chunk:
            raise GatewayUploadError("unexpected end of multipart body")
        buffer.extend(chunk)
        if len(buffer) > keep_bytes:
            flush = bytes(buffer[:-keep_bytes])
            sink(flush)
            written += len(flush)
            del buffer[:-keep_bytes]
            if written > max_bytes:
                raise GatewayUploadError("uploaded file exceeds per-file limit")


def _normalize_slot(field_name: str) -> str:
    normalized_name = str(field_name or "").strip()
    match = _FILE_FIELD_RE.match(normalized_name)
    if match:
        return str(match.group("slot") or "").strip()
    return normalized_name


def parse_gateway_upload_call(
    *,
    headers,
    stream: BinaryIO,
    content_length: int,
    service_name: str,
    method: str,
    stage_manager: GatewayStageManager,
    max_total_bytes: int,
    max_file_bytes: int,
) -> ParsedGatewayUploadCall:
    total_limit = max(1, int(max_total_bytes or 1))
    if int(content_length or 0) > total_limit:
        raise GatewayUploadError("upload payload exceeds total size limit")
    boundary = _parse_boundary(headers)
    limited_stream = _LimitedStream(stream, remaining=max(0, int(content_length or 0)))
    request = stage_manager.create_request(service_name=service_name, method=method)
    buffer = bytearray()
    try:
        first = _readline(buffer, limited_stream)
        while first in {b"\r\n", b"\n"}:
            first = _readline(buffer, limited_stream)
        normalized_first = first.rstrip(b"\r\n")
        if normalized_first != b"--" + boundary:
            raise GatewayUploadError("invalid multipart preamble")

        payload: Optional[Dict[str, object]] = None
        serialization_mode = ""
        file_map: Dict[str, str] = {}
        files: Dict[str, GatewayStageFile] = {}
        field_count = 0
        while True:
            field_count += 1
            if field_count > 64:
                raise GatewayUploadError("too many multipart fields")
            part_headers = _read_headers(buffer, limited_stream)
            disposition, params = _parse_content_disposition(part_headers.get("content-disposition", ""))
            if disposition != "form-data":
                raise GatewayUploadError("unsupported multipart disposition")
            field_name = str(params.get("name", "") or "").strip()
            if not field_name:
                raise GatewayUploadError("multipart field name is required")
            filename = str(params.get("filename", "") or "").strip()
            content_type = str(part_headers.get("content-type", "") or "application/octet-stream").strip()

            if filename:
                slot = _normalize_slot(field_name)
                if not slot:
                    raise GatewayUploadError("uploaded file slot is required")
                if slot in files:
                    raise GatewayUploadError(f"duplicate uploaded file slot: {slot}")
                output_path = stage_manager.allocate_file_path(request, slot=slot, original_name=filename)
                written = 0
                with output_path.open("wb") as fh:
                    written, final = _stream_part_body(
                        buffer=buffer,
                        stream=limited_stream,
                        boundary=boundary,
                        sink=fh.write,
                        max_bytes=max_file_bytes,
                    )
                stage_file = GatewayStageFile(
                    slot=slot,
                    field_name=field_name,
                    original_name=Path(filename).name or f"{slot}.bin",
                    content_type=content_type,
                    path=output_path,
                    size_bytes=written,
                )
                stage_manager.record_file(request, stage_file)
                files[slot] = stage_file
            else:
                collected = bytearray()
                _, final = _stream_part_body(
                    buffer=buffer,
                    stream=limited_stream,
                    boundary=boundary,
                    sink=collected.extend,
                    max_bytes=max(64 * 1024, total_limit),
                )
                text = bytes(collected).decode("utf-8")
                if field_name == "payload":
                    decoded, serialization_mode = _decode_http_request_body_with_mode(
                        text.encode("utf-8"),
                        context="gateway_public",
                    )
                    if not isinstance(decoded, dict):
                        raise GatewayUploadError("upload-call payload must decode to object")
                    payload = decoded
                elif field_name == "file_map":
                    raw_file_map = json.loads(text or "{}")
                    if not isinstance(raw_file_map, dict):
                        raise GatewayUploadError("file_map must be json object")
                    file_map = {
                        str(path or "").strip(): str(slot or "").strip()
                        for path, slot in raw_file_map.items()
                        if str(path or "").strip() and str(slot or "").strip()
                    }
            if final:
                break

        if payload is None:
            raise GatewayUploadError("multipart payload field is required")
        if not files:
            raise GatewayUploadError("at least one uploaded file is required")
        return ParsedGatewayUploadCall(
            request=request,
            payload=payload,
            serialization_mode=serialization_mode,
            file_map=file_map,
            files=files,
        )
    except Exception:
        stage_manager.preserve_failure(request, status="parse_failed")
        raise


def upload_staged_files_to_route(
    *,
    request: GatewayStageRequest,
    route,
    files: Dict[str, GatewayStageFile],
    timeout_sec: float,
) -> Dict[str, object]:
    refs: Dict[str, object] = {}
    control_addr = str(getattr(route, "control_addr", "") or "").strip()
    if not control_addr:
        raise GatewayUploadError("route control_addr is required for upload-call")
    with NodeControlClient(control_addr, timeout_sec=max(0.1, float(timeout_sec))) as client:
        try:
            for slot, stage_file in files.items():
                object_ref = client.upload_object_from_file(
                    file_path=str(stage_file.path),
                    format=normalize_object_format("", source_name=stage_file.original_name, default="bin"),
                )
                base_ref = object_ref if isinstance(object_ref, DataRef) else DataRef(
                    ref_id=str(object_ref.object_id or ""),
                    storage_id=str(object_ref.object_id or ""),
                    logical_type="",
                    format=str(object_ref.format or "bin"),
                    size_bytes=int(object_ref.size_bytes or 0),
                    materialize_as=str(getattr(object_ref, "materialize_as", "") or "path"),
                    locator_kind="node_local",
                    locator_token="",
                    consume_on_read=bool(getattr(object_ref, "consume_on_read", False)),
                )
                ref_id = f"gateway-upload:{request.request_id}:{str(getattr(route, 'service_id', '') or '').strip() or uuid.uuid4().hex}:{slot}"
                if not client.pin_object(object_id=object_ref.object_id, ref_id=ref_id):
                    raise GatewayUploadError(f"failed to pin uploaded object for slot={slot}")
                refs[slot] = DataRef(
                    ref_id=ref_id,
                    storage_id=object_ref.object_id,
                    logical_type=base_ref.logical_type,
                    format=object_ref.format,
                    size_bytes=object_ref.size_bytes,
                    materialize_as=base_ref.materialize_as,
                    locator_kind="node_control",
                    locator_token=control_addr,
                    node_id=str(getattr(route, "node_id", "") or "").strip(),
                    node_instance_id=str(getattr(route, "node_instance_id", "") or "").strip(),
                    control_addr=control_addr,
                )
        except Exception:
            _release_uploaded_refs_via_client(client, refs_by_slot=refs)
            raise
    return refs


def _release_uploaded_refs_via_client(client: NodeControlClient, *, refs_by_slot: Dict[str, object]) -> None:
    seen: set[tuple[str, str]] = set()
    for ref in refs_by_slot.values():
        data_ref = maybe_data_ref(ref)
        if data_ref is None:
            continue
        key = (data_ref.object_id, str(data_ref.ref_id or "").strip())
        if key in seen:
            continue
        seen.add(key)
        with contextlib.suppress(Exception):
            client.release_object_ref(object_id=data_ref.object_id, ref_id=str(data_ref.ref_id or "").strip())


def release_uploaded_refs_on_route(
    *,
    route,
    refs_by_slot: Dict[str, object],
    timeout_sec: float,
) -> None:
    if not refs_by_slot:
        return
    control_addr = str(getattr(route, "control_addr", "") or "").strip()
    if not control_addr:
        return
    with NodeControlClient(control_addr, timeout_sec=max(0.1, float(timeout_sec))) as client:
        _release_uploaded_refs_via_client(client, refs_by_slot=refs_by_slot)


def _set_path_value(payload: object, path: str, value: object) -> None:
    tokens = []
    for match in _PATH_TOKEN_RE.finditer(str(path or "").strip()):
        if match.group(1) is not None:
            tokens.append(match.group(1))
        elif match.group(2) is not None:
            tokens.append(int(match.group(2)))
    if not tokens:
        raise GatewayUploadError(f"invalid file_map path: {path!r}")
    current = payload
    for token in tokens[:-1]:
        if isinstance(token, int):
            if not isinstance(current, list) or token < 0 or token >= len(current):
                raise GatewayUploadError(f"file_map path not found: {path!r}")
            current = current[token]
        else:
            if not isinstance(current, dict) or token not in current:
                raise GatewayUploadError(f"file_map path not found: {path!r}")
            current = current[token]
    last = tokens[-1]
    if isinstance(last, int):
        if not isinstance(current, list) or last < 0 or last >= len(current):
            raise GatewayUploadError(f"file_map path not found: {path!r}")
        current[last] = value
    else:
        if not isinstance(current, dict):
            raise GatewayUploadError(f"file_map path not found: {path!r}")
        current[last] = value


def rewrite_payload_with_uploaded_refs(
    *,
    payload: Dict[str, object],
    refs_by_slot: Dict[str, object],
    file_map: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, object], Sequence[str]]:
    used_slots = set()

    def _rewrite(value: object) -> object:
        if isinstance(value, dict):
            kind = str(value.get("kind", "") or "").strip().lower()
            slot = str(value.get("slot", "") or "").strip()
            if kind == "uploaded_file" and slot:
                if slot not in refs_by_slot:
                    raise GatewayUploadError(f"uploaded file slot not found: {slot}")
                used_slots.add(slot)
                return refs_by_slot[slot]
            return {key: _rewrite(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_rewrite(item) for item in value]
        if isinstance(value, tuple):
            return tuple(_rewrite(item) for item in value)
        if isinstance(value, str) and value.startswith(_FILE_SENTINEL_PREFIX):
            slot = value[len(_FILE_SENTINEL_PREFIX) :].strip()
            if slot not in refs_by_slot:
                raise GatewayUploadError(f"uploaded file slot not found: {slot}")
            used_slots.add(slot)
            return refs_by_slot[slot]
        return value

    rewritten = _rewrite(dict(payload or {}))
    mapping = dict(file_map or {})
    for path, slot in mapping.items():
        normalized_slot = str(slot or "").strip()
        if normalized_slot.startswith(_FILE_SENTINEL_PREFIX):
            normalized_slot = normalized_slot[len(_FILE_SENTINEL_PREFIX) :].strip()
        if normalized_slot not in refs_by_slot:
            raise GatewayUploadError(f"uploaded file slot not found: {normalized_slot}")
        _set_path_value(rewritten, path, refs_by_slot[normalized_slot])
        used_slots.add(normalized_slot)
    if not used_slots:
        raise GatewayUploadError("upload-call payload must reference uploaded files via placeholder or file_map")
    return rewritten, sorted(used_slots)


def collect_used_upload_slots(
    *,
    payload: Dict[str, object],
    file_slots: Sequence[str],
    file_map: Optional[Dict[str, str]] = None,
) -> Sequence[str]:
    placeholder_refs = {
        str(slot or "").strip(): {"__gateway_upload_slot__": str(slot or "").strip()}
        for slot in file_slots
        if str(slot or "").strip()
    }
    _rewritten, used_slots = rewrite_payload_with_uploaded_refs(
        payload=dict(payload or {}),
        refs_by_slot=placeholder_refs,
        file_map=file_map,
    )
    return used_slots


def relay_data_ref_v1(
    *,
    route,
    data_ref: DataRef | object,
    registry_target: str,
    timeout_sec: float,
) -> DataRef:
    normalized_ref = maybe_data_ref(data_ref)
    if normalized_ref is None:
        raise GatewayUploadError("relay_data_ref_v1 requires a DataRef-compatible value")
    route_control_addr = str(getattr(route, "control_addr", "") or "").strip()
    if route_control_addr and str(normalized_ref.control_addr or "").strip() == route_control_addr:
        return normalized_ref
    resolved = resolve_data_ref(normalized_ref, target=registry_target, timeout_sec=max(0.1, float(timeout_sec)))
    source_addr = str(resolved.control_addr or "").strip()
    if not source_addr:
        raise GatewayUploadError(f"could not resolve data ref for relay: {normalized_ref.ref_id}")
    if get_gateway_dataref_relay() == "lazy":
        relayed = with_data_ref_locator(
            normalized_ref,
            locator_kind="node_control",
            locator_token=source_addr,
            node_id=str(resolved.node_id or normalized_ref.node_id or ""),
            node_instance_id=str(resolved.node_instance_id or normalized_ref.node_instance_id or ""),
            control_addr=source_addr,
        )
        if str(registry_target or "").strip():
            with contextlib.suppress(Exception):
                DataRegistryClient(registry_target, timeout_sec=max(0.1, float(timeout_sec))).register(
                    relayed,
                    node_id=str(relayed.node_id or ""),
                    node_instance_id=str(relayed.node_instance_id or ""),
                    control_addr=str(relayed.control_addr or ""),
                    locator_kind="node_control",
                    locator_token=str(relayed.control_addr or ""),
                    replicas=resolved.replicas or (
                        {
                            "control_addr": str(relayed.control_addr or ""),
                            "node_id": str(relayed.node_id or ""),
                            "node_instance_id": str(relayed.node_instance_id or ""),
                        },
                    ),
                )
        return relayed
    if not route_control_addr:
        raise GatewayUploadError("route control_addr is required for DataRef relay")
    with NodeControlClient(source_addr, timeout_sec=max(0.1, float(timeout_sec))) as source_client:
        blob = source_client.download_object_bytes(object_id=normalized_ref.object_id)
    with NodeControlClient(route_control_addr, timeout_sec=max(0.1, float(timeout_sec))) as target_client:
        uploaded = target_client.upload_object_from_bytes(blob=blob, format=normalized_ref.format)
    return DataRef(
        ref_id=str(uploaded.ref_id or uploaded.object_id),
        storage_id=str(uploaded.storage_id or uploaded.object_id),
        logical_type=str(normalized_ref.logical_type or ""),
        format=str(uploaded.format or normalized_ref.format or "bin"),
        size_bytes=int(uploaded.size_bytes or normalized_ref.size_bytes or 0),
        materialize_as=str(normalized_ref.materialize_as or "auto"),
        locator_kind="node_control",
        locator_token=route_control_addr,
        node_id=str(getattr(route, "node_id", "") or ""),
        node_instance_id=str(getattr(route, "node_instance_id", "") or ""),
        control_addr=route_control_addr,
        consume_on_read=bool(normalized_ref.consume_on_read),
    )


def ensure_data_ref_on_route(
    *,
    route,
    value: object,
    registry_target: str,
    timeout_sec: float,
) -> object:
    normalized_ref = maybe_data_ref(value)
    if normalized_ref is None:
        return value
    route_control_addr = str(getattr(route, "control_addr", "") or "").strip()
    if route_control_addr and str(normalized_ref.control_addr or "").strip() == route_control_addr:
        return normalized_ref
    return relay_data_ref_v1(
        route=route,
        data_ref=normalized_ref,
        registry_target=registry_target,
        timeout_sec=timeout_sec,
    )


def relay_payload_data_refs_v1(
    *,
    route,
    payload: Dict[str, object],
    registry_target: str,
    timeout_sec: float,
) -> Dict[str, object]:
    def _rewrite(value: object) -> object:
        normalized_ref = maybe_data_ref(value)
        if normalized_ref is not None:
            return ensure_data_ref_on_route(
                route=route,
                value=normalized_ref,
                registry_target=registry_target,
                timeout_sec=timeout_sec,
            )
        if isinstance(value, dict):
            return {str(key): _rewrite(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_rewrite(item) for item in value]
        if isinstance(value, tuple):
            return tuple(_rewrite(item) for item in value)
        return value

    return dict(_rewrite(dict(payload or {})))


__all__ = [
    "GatewayUploadError",
    "ParsedGatewayUploadCall",
    "collect_used_upload_slots",
    "ensure_data_ref_on_route",
    "is_gateway_upload_call_path",
    "parse_gateway_upload_call",
    "relay_data_ref_v1",
    "relay_payload_data_refs_v1",
    "release_uploaded_refs_on_route",
    "rewrite_payload_with_uploaded_refs",
    "upload_staged_files_to_route",
]
