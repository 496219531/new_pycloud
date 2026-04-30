from __future__ import annotations

"""NodeControl gRPC client extracted from controlplane client."""

import contextlib
import hashlib
from datetime import timedelta
from pathlib import Path
import tempfile
import time
from typing import Any, Dict, Iterator, List, Optional, Sequence, Union

import grpc
from google.protobuf import timestamp_pb2

from pycloud_parallel.controlplane.artifact import (
    ArtifactDeps,
    _coerce_artifact_deps,
    _default_entry_module_for_package,
    _normalize_dependency_policy_mode,
    _resolve_package_format,
)
from .client_transport import _materialize_downloaded_result
from pycloud_parallel.controlplane.config import (
    FILE_HASH_CHUNK_SIZE_BYTES,
    OBJECT_CHUNK_SIZE_BYTES,
    get_object_transfer_mode,
    resolve_object_transfer_mode,
    grpc_channel_options,
)
from pycloud_parallel.data.ref import DataRef, maybe_data_ref
from pycloud_parallel.controlplane.effective_policy import EffectivePolicy, should_use_transport_payload_bytes
from pycloud_parallel.controlplane.object_digest_cache import invalidate_file_digest, lookup_file_digest, store_file_digest
from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient, ServiceSessionClient
from pycloud_parallel.data.ref import normalize_object_format, object_id_from_sha256_hex
from pycloud_parallel.controlplane.serialization import (
    encode_transport_payload_bytes,
    detect_transport_mode,
    dict_to_struct,
    log_payload_flow,
    serialize_inline_payload,
    struct_to_python,
    summarize_payload_flow_value,
)
from pycloud_parallel.controlplane.payload_transport import decode_result_from_transport
from pycloud_parallel.controlplane.serialization_mode import resolve_effective_serialization_mode
from pycloud_parallel.execution.support import _prepare_managed_globals_batches_for_upload
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc

def _utc_now():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)


def _now_timestamp() -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(_utc_now())
    return ts


def _err_msg(resp_error: pb2.Error, default_msg: str) -> str:
    if resp_error and resp_error.message:
        return resp_error.message
    return default_msg


def _set_prepared_globals_payload(
    request_kwargs: Dict[str, object],
    prepared_values: Dict[str, object],
    *,
    mode: str,
    effective_policy: Optional[EffectivePolicy],
    context: str,
) -> None:
    if should_use_transport_payload_bytes(mode=mode, effective_policy=effective_policy):
        request_kwargs["transport_values"] = encode_transport_payload_bytes(
            prepared_values,
            mode=mode,
            context=context,
            limit_bytes=(
                int(effective_policy.inline_payload_hard_limit_bytes)
                if effective_policy is not None
                else 0
            ),
        )
    else:
        request_kwargs["values"] = dict_to_struct(prepared_values, mode=mode)


def _set_encoded_globals_payload(
    request_kwargs: Dict[str, object],
    *,
    values: Optional[Any] = None,
    transport_values: Optional[pb2.TransportPayload] = None,
) -> None:
    if transport_values is not None and str(getattr(transport_values, "codec", "") or "").strip():
        request_kwargs["transport_values"] = pb2.TransportPayload(
            codec=str(transport_values.codec or ""),
            version=int(transport_values.version or 0),
            payload=bytes(transport_values.payload or b""),
        )
    elif values is not None:
        request_kwargs["values"] = values
    else:
        request_kwargs["values"] = dict_to_struct({})


def _sha256_file(path: Path, *, chunk_size: int = FILE_HASH_CHUNK_SIZE_BYTES) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(max(1, int(chunk_size)))
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _iter_file_chunks(path: Path, *, chunk_size: int = OBJECT_CHUNK_SIZE_BYTES):
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(max(1, int(chunk_size)))
            if not chunk:
                break
            yield chunk


def _build_export_spec(
    *,
    export_mode: str,
    export_methods: Optional[Sequence[str]],
) -> pb2.ModuleExportSpec:
    return pb2.ModuleExportSpec(
        mode=str(export_mode or "").strip(),
        methods=[x.strip() for x in (export_methods or []) if str(x).strip()],
        decorator="pycloud_export",
    )


def _normalize_object_transfer_mode(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    if normalized not in {"auto", "known_digest_precheck", "single_pass_authoritative"}:
        raise ValueError(f"unsupported object transfer mode: {value!r}")
    return normalized


def _resolve_upload_object_transfer_mode(
    *,
    transfer_mode: str,
    source_kind: str,
    local_digest_known: bool,
) -> str:
    normalized = _normalize_object_transfer_mode(transfer_mode)
    if normalized:
        if normalized == "auto":
            return resolve_object_transfer_mode(source_kind=source_kind, local_digest_known=local_digest_known)
        return normalized
    return resolve_object_transfer_mode(source_kind=source_kind, local_digest_known=local_digest_known)


def _serialize_upload_object_requests_from_bytes(
    *,
    blob: bytes,
    object_id: str,
    format: str,
    integrity_mode: str,
    chunk_size: int,
) -> Iterator[pb2.UploadObjectRequest]:
    yield pb2.UploadObjectRequest(
        meta=pb2.UploadObjectMeta(
            object_id=str(object_id or "").strip(),
            format=str(format or "").strip(),
            integrity_mode=str(integrity_mode or "").strip(),
        )
    )
    for i in range(0, len(blob), max(1, int(chunk_size))):
        yield pb2.UploadObjectRequest(chunk=blob[i : i + chunk_size])


def _serialize_upload_object_requests_from_file(
    *,
    file_path: Path,
    object_id: str,
    format: str,
    integrity_mode: str,
    chunk_size: int,
) -> Iterator[pb2.UploadObjectRequest]:
    yield pb2.UploadObjectRequest(
        meta=pb2.UploadObjectMeta(
            object_id=str(object_id or "").strip(),
            format=str(format or "").strip(),
            integrity_mode=str(integrity_mode or "").strip(),
        )
    )
    yield from (pb2.UploadObjectRequest(chunk=chunk) for chunk in _iter_file_chunks(file_path, chunk_size=chunk_size))


def _build_uploaded_object_ref(
    *,
    object_id: str,
    format: str,
    size_bytes: int,
) -> DataRef:
    resolved_object_id = str(object_id or "").strip()
    return DataRef(
        ref_id=resolved_object_id,
        storage_id=resolved_object_id,
        logical_type="",
        format=str(format or "").strip(),
        size_bytes=int(size_bytes or 0),
        materialize_as="path",
        locator_kind="node_local",
        locator_token="",
    )


class NodeControlClient:
    """Low-level gRPC transport client for the NodeControl service."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
        self.target = target
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.channel = grpc.insecure_channel(target, options=grpc_channel_options())
        self.stub = pb2_grpc.NodeControlServiceStub(self.channel)

    def close(self) -> None:
        self.channel.close()

    def __enter__(self) -> "NodeControlClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def upload_object_from_file(
        self,
        *,
        file_path: str,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        trusted_precheck: Optional[bool] = None,
        transfer_mode: str = "",
    ) -> DataRef:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"file_path not found: {file_path}")
        if not path.is_file():
            raise ValueError(f"file_path must be a file: {file_path}")
        effective_format = normalize_object_format(format, source_name=path.name)
        cached_object_id = lookup_file_digest(path, format=effective_format)
        effective_mode = _resolve_upload_object_transfer_mode(
            transfer_mode=("known_digest_precheck" if trusted_precheck is False and not str(transfer_mode or "").strip() else transfer_mode),
            source_kind="file",
            local_digest_known=bool(cached_object_id),
        )
        return self._upload_object_impl(
            source_kind="file",
            file_path=path,
            blob=None,
            format=effective_format,
            chunk_size=chunk_size,
            transfer_mode=effective_mode,
            cached_object_id=str(cached_object_id or "").strip(),
            precheck_enabled=trusted_precheck is not False,
        )

    def upload_object_from_bytes(
        self,
        *,
        blob: bytes,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        trusted_precheck: Optional[bool] = None,
        transfer_mode: str = "",
    ) -> DataRef:
        effective_format = normalize_object_format(format, default="bin")
        effective_mode = _resolve_upload_object_transfer_mode(
            transfer_mode=transfer_mode or get_object_transfer_mode(),
            source_kind="memory",
            local_digest_known=False,
        )
        return self._upload_object_impl(
            source_kind="memory",
            file_path=None,
            blob=bytes(blob),
            format=effective_format,
            chunk_size=chunk_size,
            transfer_mode=effective_mode,
            cached_object_id="",
            precheck_enabled=trusted_precheck is not False,
        )

    def _upload_object_impl(
        self,
        *,
        source_kind: str,
        file_path: Optional[Path],
        blob: Optional[bytes],
        format: str,
        chunk_size: int,
        transfer_mode: str,
        cached_object_id: str,
        precheck_enabled: bool,
    ) -> DataRef:
        normalized_source_kind = str(source_kind or "").strip().lower()
        effective_format = (
            normalize_object_format(format, source_name=file_path.name)
            if normalized_source_kind == "file" and file_path is not None
            else normalize_object_format(format, default="bin")
        )
        normalized_mode = _normalize_object_transfer_mode(transfer_mode)
        if normalized_mode not in {"known_digest_precheck", "single_pass_authoritative"}:
            raise ValueError(f"unsupported {normalized_source_kind} object transfer mode: {transfer_mode!r}")

        upload_blob = bytes(blob or b"")
        upload_path = file_path
        size_bytes = upload_path.stat().st_size if upload_path is not None else len(upload_blob)
        object_id = ""
        if normalized_mode == "known_digest_precheck":
            object_id = str(cached_object_id or "").strip()
            if not object_id:
                digest = (
                    _sha256_file(upload_path)
                    if upload_path is not None
                    else hashlib.sha256(upload_blob).hexdigest()
                )
                object_id = object_id_from_sha256_hex(digest)
                if upload_path is not None:
                    store_file_digest(upload_path, format=effective_format, object_id=object_id)
            if precheck_enabled:
                existing = self._object_ref_if_exists(
                    object_id=object_id,
                    fallback_format=effective_format,
                    fallback_size=size_bytes,
                )
                if existing is not None:
                    return existing

        request_stream = (
            _serialize_upload_object_requests_from_file(
                file_path=upload_path,
                object_id=object_id if normalized_mode == "known_digest_precheck" else "",
                format=effective_format,
                integrity_mode=("client_declared" if normalized_mode == "known_digest_precheck" else "server_authoritative"),
                chunk_size=chunk_size,
            )
            if upload_path is not None
            else _serialize_upload_object_requests_from_bytes(
                blob=upload_blob,
                object_id=object_id if normalized_mode == "known_digest_precheck" else "",
                format=effective_format,
                integrity_mode=("client_declared" if normalized_mode == "known_digest_precheck" else "server_authoritative"),
                chunk_size=chunk_size,
            )
        )
        try:
            resp = self.stub.UploadObject(request_stream, timeout=self.timeout_sec)
        except Exception:
            if upload_path is not None and cached_object_id:
                invalidate_file_digest(upload_path, format=effective_format)
            raise
        if not resp.ok:
            if upload_path is not None and cached_object_id:
                invalidate_file_digest(upload_path, format=effective_format)
            raise RuntimeError(_err_msg(resp.error, "upload object failed"))

        final_object_id = str(resp.object_id or object_id).strip()
        ref = _build_uploaded_object_ref(
            object_id=final_object_id,
            format=str(resp.format or effective_format),
            size_bytes=int(resp.size_bytes or size_bytes),
        )
        if upload_path is not None:
            store_file_digest(upload_path, format=effective_format, object_id=ref.object_id)
        return ref

    def _upload_object_from_local_file_precheck(
        self,
        *,
        file_path: Path,
        format: str,
        chunk_size: int,
        cached_object_id: str,
        precheck_enabled: bool,
    ) -> DataRef:
        return self._upload_object_impl(
            source_kind="file",
            file_path=file_path,
            blob=None,
            format=format,
            chunk_size=chunk_size,
            transfer_mode="known_digest_precheck",
            cached_object_id=str(cached_object_id or "").strip(),
            precheck_enabled=precheck_enabled,
        )

    def _upload_object_from_local_file_single_pass(
        self,
        *,
        file_path: Path,
        format: str,
        chunk_size: int,
    ) -> DataRef:
        return self._upload_object_impl(
            source_kind="file",
            file_path=file_path,
            blob=None,
            format=format,
            chunk_size=chunk_size,
            transfer_mode="single_pass_authoritative",
            cached_object_id="",
            precheck_enabled=False,
        )

    def _upload_object_from_bytes_precheck(
        self,
        *,
        blob: bytes,
        format: str,
        chunk_size: int,
        precheck_enabled: bool,
    ) -> DataRef:
        return self._upload_object_impl(
            source_kind="memory",
            file_path=None,
            blob=bytes(blob),
            format=format,
            chunk_size=chunk_size,
            transfer_mode="known_digest_precheck",
            cached_object_id="",
            precheck_enabled=precheck_enabled,
        )

    def _upload_object_from_bytes_single_pass(
        self,
        *,
        blob: bytes,
        format: str,
        chunk_size: int,
    ) -> DataRef:
        return self._upload_object_impl(
            source_kind="memory",
            file_path=None,
            blob=bytes(blob),
            format=format,
            chunk_size=chunk_size,
            transfer_mode="single_pass_authoritative",
            cached_object_id="",
            precheck_enabled=False,
        )

    def get_object_meta(self, *, object_id: str) -> pb2.GetObjectMetaResponse:
        resp = self.stub.GetObjectMeta(
            pb2.GetObjectMetaRequest(object_id=str(object_id or "").strip()),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "get object meta failed"))
        return resp

    def has_object(self, *, object_id: str) -> bool:
        return bool(self.get_object_meta(object_id=object_id).exists)

    def _object_ref_if_exists(
        self,
        *,
        object_id: str,
        fallback_format: str,
        fallback_size: int,
    ) -> Optional[DataRef]:
        try:
            meta = self.get_object_meta(object_id=object_id)
        except Exception:
            return None
        if not bool(meta.exists):
            return None
        return DataRef(
            ref_id=str(meta.object_id or object_id),
            storage_id=str(meta.object_id or object_id),
            logical_type="",
            format=str(meta.format or fallback_format or "bin"),
            size_bytes=int(meta.size_bytes or fallback_size or 0),
            materialize_as="path",
            locator_kind="node_local",
            locator_token="",
        )

    def download_object_to_file(
        self,
        *,
        object_id: str,
        target_path: str,
    ) -> Path:
        path = Path(target_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as fh:
            stream = self.stub.DownloadObject(
                pb2.DownloadObjectRequest(object_id=str(object_id or "").strip()),
                timeout=self.timeout_sec,
            )
            try:
                for chunk in stream:
                    if chunk.chunk:
                        fh.write(chunk.chunk)
            finally:
                with contextlib.suppress(Exception):
                    stream.cancel()
        return path

    def download_object_bytes(self, *, object_id: str) -> bytes:
        out = bytearray()
        stream = self.stub.DownloadObject(
            pb2.DownloadObjectRequest(object_id=str(object_id or "").strip()),
            timeout=self.timeout_sec,
        )
        try:
            for chunk in stream:
                if chunk.chunk:
                    out.extend(chunk.chunk)
        finally:
            with contextlib.suppress(Exception):
                stream.cancel()
        return bytes(out)

    def release_object(self, *, object_id: str) -> bool:
        return self.release_object_ref(object_id=object_id)

    def pin_object(self, *, object_id: str, ref_id: str) -> bool:
        resp = self.stub.PinObject(
            pb2.PinObjectRequest(
                object_id=str(object_id or "").strip(),
                ref_id=str(ref_id or "").strip(),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok and not bool(resp.pinned):
            return False
        return bool(resp.pinned)

    def release_object_ref(self, *, object_id: str, ref_id: str = "") -> bool:
        resp = self.stub.ReleaseObject(
            pb2.ReleaseObjectRequest(
                object_id=str(object_id or "").strip(),
                ref_id=str(ref_id or "").strip(),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok and not bool(resp.released):
            return False
        return bool(resp.released)

    def download_result_to_file(self, result_ref: DataRef | object, *, target_path: str) -> Path:
        data_ref = maybe_data_ref(result_ref)
        if data_ref is None:
            raise TypeError("result_ref must be a DataRef-compatible value")
        path = self.download_object_to_file(object_id=data_ref.object_id, target_path=target_path)
        self._release_data_ref_if_consumed(data_ref)
        return path

    def fetch_result_ref_data(self, result_ref: DataRef | object, *, target_path: str = ""):
        data_ref = maybe_data_ref(result_ref)
        if data_ref is None:
            raise TypeError("result_ref must be a DataRef-compatible value")
        total_started_at = time.perf_counter()
        log_payload_flow(
            "result_ref_fetch",
            format=data_ref.format,
            materialize_as=data_ref.materialize_as,
            target_path=(target_path or "<temp>"),
            summary=summarize_payload_flow_value(data_ref),
        )
        if target_path:
            download_started_at = time.perf_counter()
            path = self.download_result_to_file(data_ref, target_path=target_path)
            log_payload_flow(
                "result_ref_fetch_done",
                client_result_ref_download_ms=(time.perf_counter() - download_started_at) * 1000.0,
                client_result_materialize_ms=0.0,
                client_result_total_ms=(time.perf_counter() - total_started_at) * 1000.0,
                target_path=str(path),
            )
            return path
        suffix = Path(f"result{('.' + data_ref.format) if data_ref.format else ''}")
        tmp = tempfile.NamedTemporaryFile(prefix="pycloud-result-", suffix=suffix.suffix, delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        try:
            download_started_at = time.perf_counter()
            self.download_result_to_file(data_ref, target_path=str(tmp_path))
            download_ms = (time.perf_counter() - download_started_at) * 1000.0
            materialize_started_at = time.perf_counter()
            result = _materialize_downloaded_result(tmp_path, result_ref=data_ref)
            materialize_ms = (time.perf_counter() - materialize_started_at) * 1000.0
            log_payload_flow(
                "result_ref_fetch_done",
                client_result_ref_download_ms=download_ms,
                client_result_materialize_ms=materialize_ms,
                client_result_total_ms=(time.perf_counter() - total_started_at) * 1000.0,
                target_path=str(tmp_path),
            )
            return result
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    def fetch_result_data(self, task_result: pb2.TaskResult, *, target_path: str = ""):
        if task_result.HasField("transport_result") and str(task_result.transport_result.codec or "").strip():
            from pycloud_parallel.controlplane.serialization import decode_transport_payload_bytes

            data = decode_transport_payload_bytes(
                str(task_result.transport_result.codec or ""),
                int(task_result.transport_result.version or 0),
                task_result.transport_result.payload,
                context="taskpool_session",
            )
        else:
            raw = struct_to_python(task_result.result)
            data = decode_result_from_transport(
                raw,
                mode=detect_transport_mode(raw, default="legacy_v1"),
                context="taskpool_session",
            )
        if maybe_data_ref(data) is None:
            return data
        return self.fetch_result_ref_data(data, target_path=target_path)

    def fetch_service_result_data(self, call_response: pb2.CallServiceResponse, *, target_path: str = ""):
        if call_response.HasField("transport_data") and str(call_response.transport_data.codec or "").strip():
            from pycloud_parallel.controlplane.serialization import decode_transport_payload_bytes

            data = decode_transport_payload_bytes(
                str(call_response.transport_data.codec or ""),
                int(call_response.transport_data.version or 0),
                call_response.transport_data.payload,
                context="service_owner",
            )
        else:
            raw = struct_to_python(call_response.data)
            data = decode_result_from_transport(
                raw,
                mode=detect_transport_mode(raw, default="legacy_v1"),
                context="service_owner",
            )
        if maybe_data_ref(data) is None:
            return data
        return self.fetch_result_ref_data(data, target_path=target_path)

    def _release_data_ref_if_consumed(self, ref: DataRef) -> None:
        if not bool(getattr(ref, "consume_on_read", False)):
            return
        with contextlib.suppress(Exception):
            self.release_object_ref(object_id=ref.object_id, ref_id=str(ref.ref_id or ""))

    def get_metrics(self) -> pb2.GetMetricsResponse:
        resp = self.stub.GetMetrics(
            pb2.GetMetricsRequest(),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "get metrics failed"))
        return resp

    def update_runtime_globals(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str,
        code_token: str,
        values: Dict[str, object],
    ) -> pb2.UpdateRuntimeGlobalsResponse:
        batches, _ = _prepare_managed_globals_batches_for_upload([self], values)
        last_resp: Optional[pb2.UpdateRuntimeGlobalsResponse] = None
        updated_names: List[str] = []
        for prepared_values in batches:
            last_resp = self.update_runtime_globals_prepared(
                client_id=client_id,
                code_version=code_version,
                runtime_key=runtime_key,
                code_token=code_token,
                prepared_values=prepared_values,
            )
            updated_names.extend(str(name) for name in last_resp.updated_names)
        if last_resp is None:
            return pb2.UpdateRuntimeGlobalsResponse(ok=True, code_version=code_version, runtime_key=runtime_key or code_version)
        return pb2.UpdateRuntimeGlobalsResponse(
            ok=last_resp.ok,
            code_version=last_resp.code_version,
            runtime_key=last_resp.runtime_key,
            globals_digest=last_resp.globals_digest,
            updated_names=sorted(set(updated_names)),
            error=last_resp.error,
        )

    def update_runtime_globals_prepared(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str,
        code_token: str,
        prepared_values: Dict[str, object],
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> pb2.UpdateRuntimeGlobalsResponse:
        effective_serialization_mode = resolve_effective_serialization_mode(
            request_mode=serialization_mode,
            context="taskpool_session",
        )
        request_kwargs = {
            "client_id": str(client_id or "").strip(),
            "code_version": str(code_version or "").strip(),
            "runtime_key": str(runtime_key or "").strip(),
            "code_token": str(code_token or "").strip(),
        }
        _set_prepared_globals_payload(
            request_kwargs,
            prepared_values,
            mode=effective_serialization_mode,
            effective_policy=effective_policy,
            context="taskpool_session",
        )
        resp = self.stub.UpdateRuntimeGlobals(
            pb2.UpdateRuntimeGlobalsRequest(**request_kwargs),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "update runtime globals failed"))
        return resp

    def update_runtime_globals_encoded(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str,
        code_token: str,
        prepared_keys: Sequence[str],
        values: Optional[Any] = None,
        transport_values: Optional[pb2.TransportPayload] = None,
    ) -> pb2.UpdateRuntimeGlobalsResponse:
        request_kwargs = {
            "client_id": str(client_id or "").strip(),
            "code_version": str(code_version or "").strip(),
            "runtime_key": str(runtime_key or "").strip(),
            "code_token": str(code_token or "").strip(),
        }
        _set_encoded_globals_payload(
            request_kwargs,
            values=values,
            transport_values=transport_values,
        )
        resp = self.stub.UpdateRuntimeGlobals(
            pb2.UpdateRuntimeGlobalsRequest(**request_kwargs),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "update runtime globals failed"))
        return resp

    def create_service_from_bytes(
        self,
        *,
        owner_client_id: str,
        service_name: str,
        blob: bytes,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        deps: Optional[ArtifactDeps] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        policy_id: str = "",
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> ServiceSessionClient:
        if not owner_client_id:
            raise ValueError("owner_client_id is required")
        digest = hashlib.sha256(blob).hexdigest()
        effective_format = _resolve_package_format(package_format, default="py")
        effective_module = _default_entry_module_for_package(
            package_format=effective_format,
            entry_module=entry_module,
            fallback_stem="service_artifact",
        )
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
        )
        resolved_deps = _coerce_artifact_deps(deps)
        normalized_dependency_policy_mode = _normalize_dependency_policy_mode(
            resolved_deps.mode,
            dependency_allowlist=resolved_deps.dependency_allowlist,
        )

        def _iter() -> Iterator[pb2.CreateServiceRequest]:
            yield pb2.CreateServiceRequest(
                meta=pb2.CreateServiceMeta(
                    owner_client_id=owner_client_id,
                    service_name=service_name,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=effective_module,
                    entry_callable=entry_callable or "run",
                    worker_count=max(1, int(worker_count)),
                    heartbeat_timeout_sec=max(1, int(heartbeat_timeout_sec)),
                    idle_ttl_sec=max(0, int(idle_ttl_sec)),
                    expose_http=bool(expose_http),
                    package_format=effective_format,
                    export_spec=export_spec,
                    dependency_allowlist=list(resolved_deps.dependency_allowlist),
                    managed_global_names=[str(name) for name in (managed_global_names or ()) if str(name).strip()],
                    dependency_policy_mode=normalized_dependency_policy_mode,
                    policy_id=str(policy_id or "").strip().lower() or "default_safe",
                )
            )
            for i in range(0, len(blob), max(1, int(chunk_size))):
                yield pb2.CreateServiceRequest(chunk=blob[i : i + chunk_size])

        resp = self.stub.CreateService(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "create service failed"))
        return ServiceSessionClient(
            _client=self,
            owner_client_id=owner_client_id,
            service_id=resp.service_id,
            service_token=resp.service_token,
            code_version=resp.code_version,
            http_base_url=resp.http_base_url,
            heartbeat_timeout_sec=resp.heartbeat_timeout_sec,
            worker_count=resp.worker_count,
            status=resp.status,
            service_name=str(service_name or ""),
            idle_ttl_sec=max(0, int(idle_ttl_sec or 0)),
            created_at=_utc_now(),
            last_heartbeat_at=_utc_now(),
            lease_expire_at=_utc_now() + timedelta(seconds=max(1, int(resp.heartbeat_timeout_sec or 0))),
        )

    def create_task_pool_from_bytes(
        self,
        *,
        owner_client_id: str,
        pool_name: str = "",
        blob: bytes,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        deps: Optional[ArtifactDeps] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 1,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> NativeTaskPoolClient:
        if not owner_client_id:
            raise ValueError("owner_client_id is required")
        digest = hashlib.sha256(blob).hexdigest()
        effective_format = _resolve_package_format(package_format, default="py")
        effective_module = _default_entry_module_for_package(
            package_format=effective_format,
            entry_module=entry_module,
            fallback_stem="task_pool_artifact",
        )
        log_payload_flow(
            "taskpool_create_grpc",
            owner_client_id=owner_client_id,
            pool_name=(pool_name or ""),
            runtime=runtime,
            entry_module=effective_module,
            entry_callable=(entry_callable or "run"),
            worker_count=max(1, int(worker_count)),
            blob_size=len(blob),
        )
        resolved_deps = _coerce_artifact_deps(deps)
        normalized_dependency_policy_mode = _normalize_dependency_policy_mode(
            resolved_deps.mode,
            dependency_allowlist=resolved_deps.dependency_allowlist,
        )

        def _iter() -> Iterator[pb2.CreateTaskPoolRequest]:
            yield pb2.CreateTaskPoolRequest(
                meta=pb2.CreateTaskPoolMeta(
                    owner_client_id=owner_client_id,
                    pool_name=pool_name,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=effective_module,
                    entry_callable=entry_callable or "run",
                    worker_count=max(1, int(worker_count)),
                    heartbeat_timeout_sec=max(1, int(heartbeat_timeout_sec)),
                    idle_ttl_sec=max(0, int(idle_ttl_sec)),
                    package_format=effective_format,
                    dependency_allowlist=list(resolved_deps.dependency_allowlist),
                    managed_global_names=[str(name) for name in (managed_global_names or ()) if str(name).strip()],
                    dependency_policy_mode=normalized_dependency_policy_mode,
                )
            )
            for i in range(0, len(blob), max(1, int(chunk_size))):
                yield pb2.CreateTaskPoolRequest(chunk=blob[i : i + chunk_size])

        resp = self.stub.CreateTaskPool(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "create task pool failed"))
        log_payload_flow(
            "taskpool_create_grpc_result",
            pool_id=resp.pool_id,
            code_version=resp.code_version,
            worker_count=resp.worker_count,
        )
        return NativeTaskPoolClient(
            _client=self,
            owner_client_id=owner_client_id,
            pool_id=resp.pool_id,
            pool_token=resp.pool_token,
            code_version=resp.code_version,
            worker_count=resp.worker_count,
            heartbeat_timeout_sec=resp.heartbeat_timeout_sec,
            pool_name=str(pool_name or ""),
            idle_ttl_sec=max(0, int(idle_ttl_sec or 0)),
            created_at=_utc_now(),
            last_heartbeat_at=_utc_now(),
            lease_expire_at=_utc_now() + timedelta(seconds=max(1, int(resp.heartbeat_timeout_sec or 0))),
        )

    def submit_pool_tasks(
        self,
        *,
        pool_id: str,
        pool_token: str,
        tasks: Sequence[pb2.TaskSubmitItem],
        job_id: str = "",
    ) -> pb2.SubmitTasksResponse:
        log_payload_flow(
            "taskpool_submit_grpc",
            pool_id=str(pool_id or "").strip(),
            task_count=len(tasks),
            job_id=str(job_id or "").strip(),
        )
        resp = self.stub.SubmitPoolTasks(
            pb2.SubmitPoolTasksRequest(
                pool_id=str(pool_id or "").strip(),
                pool_token=str(pool_token or "").strip(),
                tasks=list(tasks),
                job_id=str(job_id or "").strip(),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "submit pool tasks failed"))
        log_payload_flow(
            "taskpool_submit_grpc_result",
            pool_id=str(pool_id or "").strip(),
            accepted=len(resp.accepted),
            rejected=len(resp.rejected),
        )
        return resp

    def pull_pool_results(
        self,
        *,
        pool_id: str,
        pool_token: str,
        limit: int = 100,
        wait_ms: int = 0,
        cursor: str = "",
    ) -> pb2.PullResultsResponse:
        log_payload_flow(
            "taskpool_pull_results_grpc",
            pool_id=str(pool_id or "").strip(),
            limit=max(1, int(limit or 100)),
            wait_ms=max(0, int(wait_ms or 0)),
            cursor=str(cursor or "").strip(),
        )
        resp = self.stub.PullPoolResults(
            pb2.PullPoolResultsRequest(
                pool_id=str(pool_id or "").strip(),
                pool_token=str(pool_token or "").strip(),
                limit=max(1, int(limit or 100)),
                wait_ms=max(0, int(wait_ms or 0)),
                cursor=str(cursor or "").strip(),
            ),
            timeout=max(self.timeout_sec, max(0.1, float(wait_ms) / 1000.0) + 1.0),
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "pull pool results failed"))
        log_payload_flow(
            "taskpool_pull_results_grpc_result",
            pool_id=str(pool_id or "").strip(),
            result_count=len(resp.results),
            next_cursor=resp.next_cursor,
        )
        return resp

    def close_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
        reason: str = "",
    ) -> pb2.CloseTaskPoolResponse:
        resp = self.stub.CloseTaskPool(
            pb2.CloseTaskPoolRequest(
                owner_client_id=str(owner_client_id or "").strip(),
                pool_id=str(pool_id or "").strip(),
                pool_token=str(pool_token or "").strip(),
                reason=str(reason or ""),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok or not resp.accepted:
            raise RuntimeError(_err_msg(resp.error, "close task pool failed"))
        return resp

    def heartbeat_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
        seq: int = 0,
    ) -> pb2.HeartbeatTaskPoolResponse:
        resp = self.stub.HeartbeatTaskPool(
            pb2.HeartbeatTaskPoolRequest(
                owner_client_id=str(owner_client_id or "").strip(),
                pool_id=str(pool_id or "").strip(),
                seq=int(seq),
                timestamp=_now_timestamp(),
                pool_token=str(pool_token or "").strip(),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok or not resp.accepted:
            raise RuntimeError(_err_msg(resp.error, "heartbeat task pool failed"))
        return resp

    def cancel_pool_job(
        self,
        *,
        pool_id: str,
        pool_token: str,
        job_id: str,
        reason: str = "",
    ) -> pb2.CancelJobResponse:
        resp = self.stub.CancelPoolJob(
            pb2.CancelPoolJobRequest(
                pool_id=str(pool_id or "").strip(),
                pool_token=str(pool_token or "").strip(),
                job_id=str(job_id or "").strip(),
                reason=str(reason or ""),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "cancel pool job failed"))
        return resp

    def get_task_pool_status(
        self,
        *,
        pool_id: str,
        pool_token: str,
    ) -> pb2.TaskPoolStatusInfo:
        resp = self.stub.GetTaskPoolStatus(
            pb2.GetTaskPoolStatusRequest(
                pool_id=str(pool_id or "").strip(),
                pool_token=str(pool_token or "").strip(),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "get task pool status failed"))
        return resp.pool

    def list_service_methods(self, *, service_id: str, include_docs: bool = False) -> Sequence[pb2.ServiceMethodInfo]:
        resp = self.stub.ListServiceMethods(
            pb2.ListServiceMethodsRequest(
                service_id=service_id,
                include_docs=bool(include_docs),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "list service methods failed"))
        return list(resp.methods)

    def call_service(
        self,
        *,
        service_id: str,
        method: str,
        payload: Dict[str, object],
        timeout_sec: float = 60.0,
        service_token: str = "",
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> pb2.CallServiceResponse:
        effective_serialization_mode = resolve_effective_serialization_mode(
            request_mode=serialization_mode,
            context="service_owner",
        )
        request_kwargs = {
            "service_id": service_id,
            "method": method,
            "timeout_sec": max(0.1, float(timeout_sec)),
            "service_token": service_token or "",
        }
        if should_use_transport_payload_bytes(
            mode=effective_serialization_mode,
            effective_policy=effective_policy,
        ):
            request_kwargs["transport_payload"] = encode_transport_payload_bytes(
                payload or {},
                mode=effective_serialization_mode,
                context="service_owner",
            )
        else:
            _, payload_struct, _ = serialize_inline_payload(
                payload or {},
                context="service call payload",
                mode=effective_serialization_mode,
            )
            request_kwargs["payload"] = payload_struct
        resp = self.stub.CallService(
            pb2.CallServiceRequest(**request_kwargs),
            timeout=max(self.timeout_sec, max(0.1, float(timeout_sec)) + 1.0),
        )
        if not resp.ok:
            reason = resp.task_error.message if resp.task_error and resp.task_error.message else _err_msg(resp.error, "call service failed")
            raise RuntimeError(reason)
        return resp

    def update_service_globals(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        values: Dict[str, object],
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> pb2.UpdateServiceGlobalsResponse:
        effective_serialization_mode = resolve_effective_serialization_mode(
            request_mode=serialization_mode,
            context="object_upload",
        )
        batches, _ = _prepare_managed_globals_batches_for_upload(
            [self],
            values,
            serialization_mode=effective_serialization_mode,
            effective_policy=effective_policy,
            context="service_owner",
        )
        last_resp: Optional[pb2.UpdateServiceGlobalsResponse] = None
        updated_names: List[str] = []
        for prepared_values in batches:
            last_resp = self.update_service_globals_prepared(
                owner_client_id=owner_client_id,
                service_id=service_id,
                service_token=service_token,
                prepared_values=prepared_values,
                serialization_mode=effective_serialization_mode,
                effective_policy=effective_policy,
            )
            updated_names.extend(str(name) for name in last_resp.updated_names)
        if last_resp is None:
            return pb2.UpdateServiceGlobalsResponse(ok=True, service_id=service_id)
        return pb2.UpdateServiceGlobalsResponse(
            ok=last_resp.ok,
            service_id=last_resp.service_id,
            globals_digest=last_resp.globals_digest,
            updated_names=sorted(set(updated_names)),
            error=last_resp.error,
        )

    def update_service_globals_prepared(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        prepared_values: Dict[str, object],
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> pb2.UpdateServiceGlobalsResponse:
        effective_serialization_mode = resolve_effective_serialization_mode(
            request_mode=serialization_mode,
            context="object_upload",
        )
        request_kwargs = {
            "owner_client_id": str(owner_client_id or "").strip(),
            "service_id": str(service_id or "").strip(),
            "service_token": str(service_token or "").strip(),
        }
        _set_prepared_globals_payload(
            request_kwargs,
            prepared_values,
            mode=effective_serialization_mode,
            effective_policy=effective_policy,
            context="service_owner",
        )
        resp = self.stub.UpdateServiceGlobals(
            pb2.UpdateServiceGlobalsRequest(**request_kwargs),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "update service globals failed"))
        return resp

    def update_service_globals_encoded(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        prepared_keys: Sequence[str],
        values: Optional[Any] = None,
        transport_values: Optional[pb2.TransportPayload] = None,
    ) -> pb2.UpdateServiceGlobalsResponse:
        request_kwargs = {
            "owner_client_id": str(owner_client_id or "").strip(),
            "service_id": str(service_id or "").strip(),
            "service_token": str(service_token or "").strip(),
        }
        _set_encoded_globals_payload(
            request_kwargs,
            values=values,
            transport_values=transport_values,
        )
        resp = self.stub.UpdateServiceGlobals(
            pb2.UpdateServiceGlobalsRequest(**request_kwargs),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "update service globals failed"))
        return resp

    def heartbeat_service(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        seq: int = 0,
    ) -> pb2.HeartbeatServiceResponse:
        resp = self.stub.HeartbeatService(
            pb2.HeartbeatServiceRequest(
                owner_client_id=owner_client_id,
                service_id=service_id,
                seq=seq,
                timestamp=_now_timestamp(),
                service_token=service_token,
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok or not resp.accepted:
            raise RuntimeError(_err_msg(resp.error, "heartbeat service failed"))
        return resp

    def end_service(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        reason: str = "",
    ) -> pb2.EndServiceResponse:
        resp = self.stub.EndService(
            pb2.EndServiceRequest(
                owner_client_id=owner_client_id,
                service_id=service_id,
                reason=reason,
                service_token=service_token,
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok or not resp.accepted:
            raise RuntimeError(_err_msg(resp.error, "end service failed"))
        return resp

    def get_service_status(self, *, service_id: str) -> pb2.ServiceStatusInfo:
        resp = self.stub.GetServiceStatus(
            pb2.GetServiceStatusRequest(service_id=service_id),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "get service status failed"))
        return resp.service

__all__ = ["NodeControlClient"]
