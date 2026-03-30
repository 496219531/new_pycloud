from __future__ import annotations

"""Client helpers for InfoCenter/NodeControl service-session workflow."""

import asyncio
import hashlib
import json
import os
import re
import socket
import tarfile
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import grpc
from google.protobuf import json_format
from google.protobuf import timestamp_pb2

from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


def pycloud_export(fn):
    fn.__pycloud_export__ = True
    return fn


def _get_local_ip() -> str:
    """获取本机 IP 地址。

    Returns:
        str: 本机 IP 地址，如果获取失败返回 "localhost"
    """
    try:
        # 创建一个 UDP socket，不实际发送数据
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # 连接到一个外部地址（不实际发送数据）
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            return local_ip
    except Exception:
        return "localhost"


def _now_timestamp() -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(datetime.now(timezone.utc))
    return ts


def _err_msg(resp_error: pb2.Error, default_msg: str) -> str:
    if resp_error and resp_error.message:
        return resp_error.message
    return default_msg


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(max(1, int(chunk_size)))
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _iter_file_chunks(path: Path, *, chunk_size: int = 256 * 1024) -> Iterator[bytes]:
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(max(1, int(chunk_size)))
            if not chunk:
                break
            yield chunk


def _package_format_from_filename(filename: str) -> str:
    lower = str(filename or "").lower()
    if lower.endswith(".tar.gz") or lower.endswith(".tgz"):
        return "tar.gz"
    if lower.endswith(".zip"):
        return "zip"
    if lower.endswith(".whl"):
        return "whl"
    if lower.endswith(".py"):
        return "py"
    return "bin"


def _build_export_spec(
    *,
    export_mode: str,
    export_methods: Optional[Sequence[str]],
    export_decorator: str,
) -> pb2.ModuleExportSpec:
    return pb2.ModuleExportSpec(
        mode=str(export_mode or "").strip(),
        methods=[x.strip() for x in (export_methods or []) if str(x).strip()],
        decorator=str(export_decorator or "").strip(),
    )


def _package_directory_to_targz(dir_path: Path) -> Path:
    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-pkg-", suffix=".tar.gz")
    os.close(fd)
    out = Path(tmp_name)
    with tarfile.open(out, "w:gz") as tf:
        for item in sorted(dir_path.rglob("*")):
            if item.name == "__pycache__":
                continue
            rel = item.relative_to(dir_path)
            tf.add(item, arcname=str(rel))
    return out


def _package_paths_to_targz(*, root_dir: Path, paths: Sequence[str]) -> Path:
    normalized: List[Path] = []
    root = root_dir.resolve()
    for item in paths:
        p = (root / item).resolve()
        if not p.exists():
            raise FileNotFoundError(f"path not found: {item}")
        if p != root and root not in p.parents:
            raise ValueError(f"path escapes root_dir: {item}")
        normalized.append(p)

    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-paths-", suffix=".tar.gz")
    os.close(fd)
    out = Path(tmp_name)
    with tarfile.open(out, "w:gz") as tf:
        for p in normalized:
            rel = p.relative_to(root)
            tf.add(p, arcname=str(rel))
    return out


_SERVICE_SESSION_SCHEMA_VERSION = 1


def _artifact_code_version(blob: bytes) -> str:
    return f"sha256:{hashlib.sha256(blob).hexdigest()}"


def _default_service_session_cache_dir() -> Path:
    custom = str(os.environ.get("PYCLOUD_SERVICE_SESSION_DIR", "")).strip()
    if custom:
        return Path(custom).expanduser()
    return Path.home() / ".pycloud_parallel" / "service_sessions"


def _sanitize_session_cache_part(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    return text.strip("._") or "default"


def _service_session_cache_file(
    *,
    owner_client_id: str,
    service_name: str,
    cache_dir: str = "",
) -> Path:
    base_dir = Path(cache_dir).expanduser() if str(cache_dir).strip() else _default_service_session_cache_dir()
    return (
        base_dir
        / _sanitize_session_cache_part(owner_client_id)
        / f"{_sanitize_session_cache_part(service_name)}.json"
    )


def _ensure_private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o700)
    except OSError:
        pass


def _write_private_json(path: Path, payload: Dict[str, object]) -> None:
    _ensure_private_dir(path.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.stem}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=True, indent=2, sort_keys=True)
            fp.write("\n")
        try:
            os.chmod(tmp_name, 0o600)
        except OSError:
            pass
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.unlink(tmp_name)
            except OSError:
                pass


def _load_service_session_cache(
    *,
    owner_client_id: str,
    service_name: str,
    cache_dir: str = "",
) -> Optional[Dict[str, object]]:
    path = _service_session_cache_file(
        owner_client_id=owner_client_id,
        service_name=service_name,
        cache_dir=cache_dir,
    )
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("schema_version", 0) or 0) != _SERVICE_SESSION_SCHEMA_VERSION:
        return None
    if payload.get("owner_client_id") != owner_client_id or payload.get("service_name") != service_name:
        return None
    nodes = payload.get("nodes")
    if not isinstance(nodes, dict):
        return None
    return payload


@dataclass(frozen=True)
class InfoCenterNode:
    node_id: str
    control_addr: str
    healthy: bool
    capacity: int
    queue_capacity: int
    queued: int
    inflight: int
    credit: int
    tags: Tuple[str, ...] = ()


@dataclass(frozen=True)
class InfoCenterServiceRoute:
    service_name: str
    service_id: str
    status: int
    node_id: str
    control_addr: str
    node_healthy: bool
    worker_count: int
    alive_workers: int
    in_flight: int
    lease_expire_at: datetime
    http_base_url: str


@dataclass
class NodeCircuitState:
    state: str = "closed"  # closed | open | half_open
    consecutive_failures: int = 0
    open_until_monotonic: float = 0.0
    open_count: int = 0
    probe_in_flight: bool = False
    last_error: str = ""


class InfoCenterClient:
    """Thin gRPC client wrapper for InfoCenter service."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
        self.target = target
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.channel = grpc.insecure_channel(target)
        self.stub = pb2_grpc.InfoCenterServiceStub(self.channel)

    def close(self) -> None:
        self.channel.close()

    def __enter__(self) -> "InfoCenterClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def register_node(
        self,
        *,
        node_id: str,
        control_addr: str,
        capacity: int = 32,
        queue_capacity: int = 4000,
        tags: Optional[Sequence[str]] = None,
        version: str = "",
        metadata: Optional[Dict[str, str]] = None,
        services: Optional[Sequence[pb2.ServiceRouteReport]] = None,
    ) -> pb2.RegisterNodeResponse:
        resp = self.stub.RegisterNode(
            pb2.RegisterNodeRequest(
                node_id=node_id,
                control_addr=control_addr,
                capacity=max(1, int(capacity)),
                queue_capacity=max(1, int(queue_capacity)),
                tags=list(tags or []),
                version=version,
                metadata=dict(metadata or {}),
                services=list(services or []),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "register node failed"))
        return resp

    def list_nodes(
        self,
        *,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        limit: int = 100,
    ) -> Sequence[InfoCenterNode]:
        resp = self.stub.ListNodes(
            pb2.ListNodesRequest(
                healthy_only=bool(healthy_only),
                tags=list(tags or []),
                limit=max(1, int(limit)),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "list nodes failed"))
        out = []
        for item in resp.nodes:
            out.append(
                InfoCenterNode(
                    node_id=item.node_id,
                    control_addr=item.control_addr,
                    healthy=bool(item.healthy),
                    capacity=int(item.capacity),
                    queue_capacity=int(item.queue_capacity),
                    queued=int(item.queued),
                    inflight=int(item.inflight),
                    credit=int(item.credit),
                    tags=tuple(item.tags),
                )
            )
        return out

    def list_service_routes(
        self,
        *,
        service_name: str = "",
        healthy_only: bool = True,
        limit: int = 500,
    ) -> Sequence[InfoCenterServiceRoute]:
        resp = self.stub.ListServiceRoutes(
            pb2.ListServiceRoutesRequest(
                service_name=service_name,
                healthy_only=bool(healthy_only),
                limit=max(1, int(limit)),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "list service routes failed"))
        out = []
        for item in resp.routes:
            dt = item.lease_expire_at.ToDatetime()
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            out.append(
                InfoCenterServiceRoute(
                    service_name=item.service_name,
                    service_id=item.service_id,
                    status=int(item.status),
                    node_id=item.node_id,
                    control_addr=item.control_addr,
                    node_healthy=bool(item.node_healthy),
                    worker_count=int(item.worker_count),
                    alive_workers=int(item.alive_workers),
                    in_flight=int(item.in_flight),
                    lease_expire_at=dt.astimezone(timezone.utc),
                    http_base_url=item.http_base_url,
                )
            )
        return out


@dataclass
class ServiceSessionClient:
    """Client-side service session handle with optional keepalive loop."""

    _client: "NodeControlClient" = field(repr=False)
    owner_client_id: str
    service_id: str
    service_token: str
    http_base_url: str
    heartbeat_timeout_sec: int
    worker_count: int
    status: int
    _hb_stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _hb_thread: Optional[threading.Thread] = field(default=None, repr=False)
    _hb_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _hb_seq: int = field(default=0, repr=False)
    _hb_interval_sec: float = field(default=0.0, repr=False)

    def start_keepalive(self, interval_sec: Optional[float] = None) -> None:
        with self._hb_lock:
            if self._hb_thread is not None and self._hb_thread.is_alive():
                return
            self._hb_stop.clear()
            default_interval = max(1.0, float(self.heartbeat_timeout_sec) / 2.0)
            self._hb_interval_sec = max(0.5, float(interval_sec if interval_sec is not None else default_interval))
            self._hb_thread = threading.Thread(
                target=self._keepalive_loop,
                name=f"svc-hb-{self.service_id[:8]}",
                daemon=True,
            )
            self._hb_thread.start()

    def stop_keepalive(self) -> None:
        with self._hb_lock:
            self._hb_stop.set()
            thread = self._hb_thread
        if thread is not None:
            thread.join(timeout=2.0)
        with self._hb_lock:
            self._hb_thread = None

    def heartbeat(self) -> pb2.HeartbeatServiceResponse:
        self._hb_seq += 1
        resp = self._client.stub.HeartbeatService(
            pb2.HeartbeatServiceRequest(
                owner_client_id=self.owner_client_id,
                service_id=self.service_id,
                seq=self._hb_seq,
                timestamp=_now_timestamp(),
                service_token=self.service_token,
            ),
            timeout=self._client.timeout_sec,
        )
        if not resp.ok or not resp.accepted:
            raise RuntimeError(_err_msg(resp.error, "heartbeat rejected"))
        self.status = resp.status
        if resp.next_heartbeat_in_sec > 0:
            self._hb_interval_sec = float(resp.next_heartbeat_in_sec)
        return resp

    def end(self, reason: str = "client requested end") -> pb2.EndServiceResponse:
        self.stop_keepalive()
        resp = self._client.stub.EndService(
            pb2.EndServiceRequest(
                owner_client_id=self.owner_client_id,
                service_id=self.service_id,
                reason=reason,
                service_token=self.service_token,
            ),
            timeout=self._client.timeout_sec,
        )
        if not resp.ok or not resp.accepted:
            raise RuntimeError(_err_msg(resp.error, "end service rejected"))
        self.status = resp.status
        return resp

    def get_status(self) -> pb2.ServiceStatusInfo:
        resp = self._client.stub.GetServiceStatus(
            pb2.GetServiceStatusRequest(service_id=self.service_id),
            timeout=self._client.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "get service status failed"))
        self.status = resp.service.status
        return resp.service

    def list_methods(self, *, include_docs: bool = False) -> Sequence[pb2.ServiceMethodInfo]:
        return self._client.list_service_methods(service_id=self.service_id, include_docs=include_docs)

    def call(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        token: Optional[str] = None,
        via: str = "http",
    ) -> Dict[str, object]:
        if via == "grpc":
            resp = self._client.call_service(
                service_id=self.service_id,
                method=method,
                payload=payload,
                timeout_sec=timeout_sec,
                service_token=(self.service_token if token is None else (token or "")),
            )
            return {
                "ok": True,
                "method": method,
                "data": json_format.MessageToDict(resp.data, preserving_proto_field_name=True),
            }

        if not self.http_base_url:
            raise RuntimeError("service has no http_base_url; expose_http may be false")
        if not method:
            raise ValueError("method is required")

        params = urlencode({"timeout_sec": f"{max(0.1, float(timeout_sec)):.3f}"})
        url = f"{self.http_base_url}/call/{quote(method, safe='')}?{params}"
        headers = {"Content-Type": "application/json"}
        auth_token = self.service_token if token is None else token
        if auth_token:
            headers["X-Service-Token"] = auth_token
        req = Request(
            url=url,
            method="POST",
            headers=headers,
            data=json.dumps(payload or {}).encode("utf-8"),
        )
        try:
            with urlopen(req, timeout=max(2.0, float(timeout_sec) + 1.0)) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            raw = exc.read().decode("utf-8") if hasattr(exc, "read") else ""
            msg = raw or str(exc)
            raise RuntimeError(f"call failed: {msg}") from exc
        if not isinstance(body, dict):
            raise RuntimeError("call failed: invalid response body")
        if not body.get("ok", False):
            raise RuntimeError(f"call failed: {body.get('error', 'unknown error')}")
        return body

    def _keepalive_loop(self) -> None:
        while not self._hb_stop.wait(max(0.5, self._hb_interval_sec)):
            try:
                self.heartbeat()
            except Exception:
                # Keep trying. If heartbeat stays broken for too long, server will reclaim service.
                self._hb_interval_sec = max(1.0, self._hb_interval_sec)


class NodeControlClient:
    """Thin gRPC client wrapper for NodeControl service."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
        self.target = target
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.channel = grpc.insecure_channel(target)
        self.stub = pb2_grpc.NodeControlServiceStub(self.channel)

    def close(self) -> None:
        self.channel.close()

    def __enter__(self) -> "NodeControlClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def create_service_from_file(
        self,
        *,
        owner_client_id: str,
        artifact_path: str,
        service_name: str = "",
        filename: str = "",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = 256 * 1024,
    ) -> ServiceSessionClient:
        path = Path(artifact_path)
        if not path.exists():
            raise FileNotFoundError(f"artifact_path not found: {artifact_path}")

        tmp_pkg: Optional[Path] = None
        upload_file = path
        effective_filename = filename or path.name
        inferred_format = package_format
        if path.is_dir():
            tmp_pkg = _package_directory_to_targz(path)
            upload_file = tmp_pkg
            effective_filename = filename or f"{path.name}.tar.gz"
            inferred_format = package_format or "tar.gz"

        try:
            return self._create_service_from_local_file(
                owner_client_id=owner_client_id,
                service_name=service_name,
                file_path=upload_file,
                filename=effective_filename,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format=inferred_format,
                export_mode=export_mode,
                export_methods=export_methods,
                export_decorator=export_decorator,
                worker_count=worker_count,
                heartbeat_timeout_sec=heartbeat_timeout_sec,
                idle_ttl_sec=idle_ttl_sec,
                expose_http=expose_http,
                chunk_size=chunk_size,
            )
        finally:
            if tmp_pkg is not None:
                tmp_pkg.unlink(missing_ok=True)

    def create_service_from_paths(
        self,
        *,
        owner_client_id: str,
        root_dir: str,
        paths: Sequence[str],
        service_name: str = "",
        filename: str = "service_bundle.tar.gz",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = 256 * 1024,
    ) -> ServiceSessionClient:
        tar_path = _package_paths_to_targz(root_dir=Path(root_dir), paths=paths)
        try:
            return self._create_service_from_local_file(
                owner_client_id=owner_client_id,
                service_name=service_name,
                file_path=tar_path,
                filename=filename or "service_bundle.tar.gz",
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format="tar.gz",
                export_mode=export_mode,
                export_methods=export_methods,
                export_decorator=export_decorator,
                worker_count=worker_count,
                heartbeat_timeout_sec=heartbeat_timeout_sec,
                idle_ttl_sec=idle_ttl_sec,
                expose_http=expose_http,
                chunk_size=chunk_size,
            )
        finally:
            tar_path.unlink(missing_ok=True)

    def _create_service_from_local_file(
        self,
        *,
        owner_client_id: str,
        service_name: str,
        file_path: Path,
        filename: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str,
        export_mode: str,
        export_methods: Optional[Sequence[str]],
        export_decorator: str,
        worker_count: int,
        heartbeat_timeout_sec: int,
        idle_ttl_sec: int,
        expose_http: bool,
        chunk_size: int,
    ) -> ServiceSessionClient:
        effective_filename = filename or file_path.name
        effective_module = entry_module
        if not effective_module and effective_filename.endswith(".py"):
            effective_module = Path(effective_filename).stem
        effective_format = package_format or _package_format_from_filename(effective_filename)
        digest = _sha256_file(file_path)
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
        )

        def _iter() -> Iterator[pb2.CreateServiceRequest]:
            yield pb2.CreateServiceRequest(
                meta=pb2.CreateServiceMeta(
                    owner_client_id=owner_client_id,
                    service_name=service_name,
                    filename=effective_filename,
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
                )
            )
            yield from (pb2.CreateServiceRequest(chunk=chunk) for chunk in _iter_file_chunks(file_path, chunk_size=chunk_size))

        resp = self.stub.CreateService(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "create service failed"))
        return ServiceSessionClient(
            owner_client_id=owner_client_id,
            _client=self,
            service_id=resp.service_id,
            service_token=resp.service_token,
            http_base_url=resp.http_base_url,
            heartbeat_timeout_sec=resp.heartbeat_timeout_sec,
            worker_count=resp.worker_count,
            status=resp.status,
        )

    def create_service_from_bytes(
        self,
        *,
        owner_client_id: str,
        service_name: str,
        filename: str,
        blob: bytes,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = 256 * 1024,
    ) -> ServiceSessionClient:
        if not owner_client_id:
            raise ValueError("owner_client_id is required")
        if not filename:
            raise ValueError("filename is required")

        digest = hashlib.sha256(blob).hexdigest()
        effective_format = package_format or _package_format_from_filename(filename)
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
        )

        def _iter() -> Iterator[pb2.CreateServiceRequest]:
            yield pb2.CreateServiceRequest(
                meta=pb2.CreateServiceMeta(
                    owner_client_id=owner_client_id,
                    service_name=service_name,
                    filename=filename,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=entry_module,
                    entry_callable=entry_callable or "run",
                    worker_count=max(1, int(worker_count)),
                    heartbeat_timeout_sec=max(1, int(heartbeat_timeout_sec)),
                    idle_ttl_sec=max(0, int(idle_ttl_sec)),
                    expose_http=bool(expose_http),
                    package_format=effective_format,
                    export_spec=export_spec,
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
            http_base_url=resp.http_base_url,
            heartbeat_timeout_sec=resp.heartbeat_timeout_sec,
            worker_count=resp.worker_count,
            status=resp.status,
        )

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
    ) -> pb2.CallServiceResponse:
        resp = self.stub.CallService(
            pb2.CallServiceRequest(
                service_id=service_id,
                method=method,
                payload=payload or {},
                timeout_sec=max(0.1, float(timeout_sec)),
                service_token=service_token or "",
            ),
            timeout=max(self.timeout_sec, max(0.1, float(timeout_sec)) + 1.0),
        )
        if not resp.ok:
            reason = resp.task_error.message if resp.task_error and resp.task_error.message else _err_msg(resp.error, "call service failed")
            raise RuntimeError(reason)
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


@dataclass
class MultiNodeServiceGroup:
    """A deployed service group spread across multiple NodeControl nodes."""

    owner_client_id: str
    service_name: str
    sessions: Dict[str, ServiceSessionClient]
    nodes: Dict[str, InfoCenterNode]
    failures: Dict[str, str] = field(default_factory=dict)
    breaker_enabled: bool = True
    breaker_failure_threshold: int = 3
    breaker_cooldown_sec: float = 15.0
    breaker_max_cooldown_sec: float = 120.0
    _clients: Dict[str, NodeControlClient] = field(default_factory=dict, repr=False)
    _session_cache_file: Optional[Path] = field(default=None, repr=False)
    _artifact_code_version: str = field(default="", repr=False)
    _route_index: int = field(default=0, repr=False)
    _route_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _breaker_states: Dict[str, NodeCircuitState] = field(default_factory=dict, repr=False)

    @classmethod
    def deploy_from_infocenter(
        cls,
        *,
        infocenter_target: str,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        artifact_path: str = "",
        artifact_paths: Optional[Sequence[str]] = None,
        blob: Optional[bytes] = None,
        filename: str = "",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = 256 * 1024,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = False,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 15.0,
        breaker_max_cooldown_sec: float = 120.0,
    ) -> "MultiNodeServiceGroup":
        """从 InfoCenter 发现节点并部署服务。

        Args:
            infocenter_target: InfoCenter 地址
            owner_client_id: 所有者客户端 ID
            service_name: 服务名称
            artifact_path: 单个文件路径（优先级低于 blob，优先级高于 artifact_paths）
            artifact_paths: 文件/文件夹路径列表，会自动打包成 zip
            blob: 直接提供代码内容（优先级最高）
            runtime: 运行时版本
            entry_module: 入口模块名
            entry_callable: 入口函数名
            package_format: 包格式 ("py", "zip", "tar.gz")
            export_mode: 导出模式 ("decorator", "explicit", "all", "single")
            export_methods: 显式导出的方法列表
            export_decorator: 装饰器名称
            worker_count: 工作进程数
            heartbeat_timeout_sec: 心跳超时
            idle_ttl_sec: 空闲 TTL
            expose_http: 是否暴露 HTTP
            chunk_size: 上传分片大小
            healthy_only: 是否只使用健康节点
            tags: 节点标签过滤
            node_limit: 节点数量限制
            allow_partial: 是否允许部分失败
            min_success_nodes: 最小成功节点数
            timeout_sec: 超时时间
            ensure_unique_service_name: 是否确保服务名唯一
            reuse_existing_same_code: 同 owner + 同代码时是否直接复用已存在服务
            replace_existing_if_code_changed: 同 owner + 同服务名但代码变化时是否替换
            session_cache_dir: 本地 service session token 缓存目录
            breaker_enabled: 是否启用熔断器
            breaker_failure_threshold: 熔断失败阈值
            breaker_cooldown_sec: 熔断冷却时间
            breaker_max_cooldown_sec: 熔断最大冷却时间

        Returns:
            MultiNodeServiceGroup: 部署的服务组
        """
        # 生成默认的 owner_client_id 和 service_name
        local_ip = _get_local_ip()

        # 如果 owner_client_id 为空，使用本机 IP
        effective_owner_client_id = owner_client_id
        if not effective_owner_client_id:
            effective_owner_client_id = f"client-{local_ip}"

        # 先确定 entry_module（用于生成 service_name）
        effective_entry_module = entry_module
        if not effective_entry_module:
            if filename:
                # 优先使用 filename
                if filename.endswith(".py"):
                    effective_entry_module = Path(filename).stem
            else:
                # 尝试从 artifact_path 推断
                if artifact_path:
                    path = Path(artifact_path)
                    if path.suffix == ".py":
                        effective_entry_module = path.stem
                # 尝试从 artifact_paths 推断
                elif artifact_paths and len(artifact_paths) > 0:
                    first_path = Path(artifact_paths[0])
                    if first_path.suffix == ".py":
                        effective_entry_module = first_path.stem

        # 如果 service_name 为空，使用 entry_module + 本机 IP + 时间戳（精确到秒）
        # 添加时间戳确保唯一性，避免服务名冲突
        effective_service_name = service_name
        if not effective_service_name:
            # 生成时间戳（精确到秒）
            timestamp = time.strftime("%Y%m%d%H%M%S")  # 格式: 20250330120000

            if effective_entry_module:
                effective_service_name = f"{effective_entry_module}-{local_ip}-{timestamp}"
            else:
                effective_service_name = f"service-{local_ip}-{timestamp}"

        # 现在才进行校验
        if not effective_owner_client_id:
            raise ValueError("owner_client_id is required")
        if not effective_service_name:
            raise ValueError("service_name is required")

        effective_blob = blob
        effective_filename = filename
        effective_package_format = package_format

        # 优先级: blob > artifact_path > artifact_paths
        if effective_blob is None and artifact_paths:
            # artifact_paths 列表：打包成 zip
            import zipfile
            import tempfile

            paths = [str(p) for p in artifact_paths if p]
            if not paths:
                raise ValueError("artifact_paths list is empty")

            # 创建临时 zip 文件
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
                tmp_zip_path = tmp_zip.name

            with zipfile.ZipFile(tmp_zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for path_str in paths:
                    p = Path(path_str)
                    if not p.exists():
                        raise FileNotFoundError(f"Path not found: {p}")
                    if p.is_file():
                        # 单个文件：直接添加，保持原名
                        zf.write(p, p.name)
                    elif p.is_dir():
                        # 收集所有需要写入 zip 的目录（用于自动补全 __init__.py）
                        dirs_to_check = {p}  # 顶级目录本身
                        for child in p.rglob('*'):
                            if child.is_dir():
                                dirs_to_check.add(child)

                        # 为缺少 __init__.py 的目录写入空文件
                        for d in sorted(dirs_to_check, key=lambda x: str(x)):
                            if not (d / "__init__.py").exists():
                                init_arcname = p.name / d.relative_to(p) / "__init__.py"
                                zf.writestr(str(init_arcname), "")

                        # 文件夹：将文件夹本身作为 zip 内的一层目录，写入所有文件
                        for file_path in p.rglob('*'):
                            if file_path.is_file():
                                arcname = p.name / file_path.relative_to(p)
                                zf.write(file_path, str(arcname))
            if not effective_filename:
                effective_filename=tmp_zip_path
            effective_blob = Path(tmp_zip_path).read_bytes()
            effective_package_format = "zip"

            # 清理临时文件
            try:
                os.unlink(tmp_zip_path)
            except Exception:
                pass

        if effective_blob is None:
            if not artifact_path:
                raise ValueError("artifact_path or artifact_paths or blob must be provided")
            path = Path(artifact_path)
            if not path.exists():
                raise FileNotFoundError(f"artifact_path not found: {path}")
            effective_blob = path.read_bytes()
            if not effective_filename:
                effective_filename = path.name
                # 再次尝试从文件名推断 entry_module
                if not effective_entry_module and effective_filename.endswith(".py"):
                    effective_entry_module = Path(effective_filename).stem

        if effective_blob is None:
            raise ValueError("artifact content is empty")

        effective_code_version = _artifact_code_version(effective_blob)
        session_cache_file = _service_session_cache_file(
            owner_client_id=effective_owner_client_id,
            service_name=effective_service_name,
            cache_dir=session_cache_dir,
        )

        with InfoCenterClient(infocenter_target, timeout_sec=timeout_sec) as infocenter:
            existing_routes: Sequence[InfoCenterServiceRoute] = ()
            if ensure_unique_service_name:
                existing_routes = infocenter.list_service_routes(
                    service_name=effective_service_name,
                    healthy_only=True,
                    limit=max(100, node_limit * 10),
                )
            discovered_nodes = infocenter.list_nodes(
                healthy_only=healthy_only,
                tags=tags,
                limit=node_limit,
            )

        if not discovered_nodes:
            raise RuntimeError("no available nodes from InfoCenter")

        discovered_node_map = {node.node_id: node for node in discovered_nodes}

        if ensure_unique_service_name:
            active_routes = cls._select_active_routes(existing_routes)
            if active_routes:
                existing_infos = cls._inspect_existing_routes(active_routes=active_routes, timeout_sec=timeout_sec)
                existing_owners = {info.owner_client_id for _, info in existing_infos}
                existing_versions = {info.code_version for _, info in existing_infos}
                if len(existing_owners) != 1 or len(existing_versions) != 1:
                    raise RuntimeError(
                        f"service_name already exists but active routes are inconsistent: {effective_service_name}"
                    )

                existing_owner = next(iter(existing_owners))
                existing_code_version = next(iter(existing_versions))
                if existing_owner != effective_owner_client_id:
                    raise RuntimeError(
                        f"service_name already exists and belongs to another owner: "
                        f"service_name={effective_service_name}; owner={existing_owner}"
                    )

                cached_session = _load_service_session_cache(
                    owner_client_id=effective_owner_client_id,
                    service_name=effective_service_name,
                    cache_dir=session_cache_dir,
                )

                if existing_code_version == effective_code_version:
                    if not reuse_existing_same_code:
                        raise RuntimeError(
                            f"service_name already exists with same code_version: {effective_service_name}; "
                            "set reuse_existing_same_code=True to reuse"
                        )
                    if cached_session is None or cached_session.get("artifact_code_version") != effective_code_version:
                        raise RuntimeError(
                            f"service_name already exists with same code_version but no reusable local token cache was found: "
                            f"{effective_service_name}"
                        )
                    return cls._reuse_existing_group(
                        owner_client_id=effective_owner_client_id,
                        service_name=effective_service_name,
                        artifact_code_version=effective_code_version,
                        cache_payload=cached_session,
                        active_routes=existing_infos,
                        discovered_node_map=discovered_node_map,
                        timeout_sec=timeout_sec,
                        breaker_enabled=breaker_enabled,
                        breaker_failure_threshold=breaker_failure_threshold,
                        breaker_cooldown_sec=breaker_cooldown_sec,
                        breaker_max_cooldown_sec=breaker_max_cooldown_sec,
                        session_cache_file=session_cache_file,
                    )

                if not replace_existing_if_code_changed:
                    raise RuntimeError(
                        f"service_name already exists with different code_version: {effective_service_name}; "
                        f"existing={existing_code_version}; incoming={effective_code_version}; "
                        "set replace_existing_if_code_changed=True to replace"
                    )
                if cached_session is None:
                    raise RuntimeError(
                        f"service_name already exists with different code_version but no local token cache was found for replacement: "
                        f"{effective_service_name}"
                    )

                cls._end_existing_group(
                    owner_client_id=effective_owner_client_id,
                    cache_payload=cached_session,
                    active_routes=existing_infos,
                    timeout_sec=timeout_sec,
                    reason=f"replace service due to code change: {effective_code_version}",
                )
                try:
                    session_cache_file.unlink()
                except FileNotFoundError:
                    pass

        sessions: Dict[str, ServiceSessionClient] = {}
        clients: Dict[str, NodeControlClient] = {}
        nodes: Dict[str, InfoCenterNode] = {}
        failures: Dict[str, str] = {}

        for node in discovered_nodes:
            client = NodeControlClient(node.control_addr, timeout_sec=timeout_sec)
            try:
                session = client.create_service_from_bytes(
                    owner_client_id=effective_owner_client_id,
                    service_name=effective_service_name,
                    filename=effective_filename,
                    blob=effective_blob,
                    runtime=runtime,
                    entry_module=effective_entry_module,
                    entry_callable=entry_callable,
                    package_format=effective_package_format,
                    export_mode=export_mode,
                    export_methods=export_methods,
                    export_decorator=export_decorator,
                    worker_count=worker_count,
                    heartbeat_timeout_sec=heartbeat_timeout_sec,
                    idle_ttl_sec=idle_ttl_sec,
                    expose_http=expose_http,
                    chunk_size=chunk_size,
                )
            except Exception as exc:
                failures[node.node_id] = repr(exc)
                client.close()
                if not allow_partial:
                    cls._cleanup_created_services(sessions=sessions, clients=clients, reason="rollback deploy")
                    raise RuntimeError(f"deploy failed on node={node.node_id}: {exc}") from exc
                continue

            sessions[node.node_id] = session
            clients[node.node_id] = client
            nodes[node.node_id] = node

        if len(sessions) < max(1, int(min_success_nodes)):
            cls._cleanup_created_services(sessions=sessions, clients=clients, reason="insufficient success nodes")
            raise RuntimeError(
                f"deploy success nodes={len(sessions)} < min_success_nodes={max(1, int(min_success_nodes))}; "
                f"failures={failures}"
            )

        group = cls(
            owner_client_id=effective_owner_client_id,
            service_name=effective_service_name,
            sessions=sessions,
            nodes=nodes,
            failures=failures,
            breaker_enabled=bool(breaker_enabled),
            breaker_failure_threshold=max(1, int(breaker_failure_threshold)),
            breaker_cooldown_sec=max(0.1, float(breaker_cooldown_sec)),
            breaker_max_cooldown_sec=max(0.1, float(breaker_max_cooldown_sec)),
            _clients=clients,
            _session_cache_file=session_cache_file,
            _artifact_code_version=effective_code_version,
        )
        group._persist_session_cache()
        return group

    @staticmethod
    def _select_active_routes(routes: Sequence[InfoCenterServiceRoute]) -> List[InfoCenterServiceRoute]:
        return [
            route
            for route in routes
            if route.status in (
                pb2.SERVICE_STATUS_STARTING,
                pb2.SERVICE_STATUS_RUNNING,
                pb2.SERVICE_STATUS_DRAINING,
            )
        ]

    @classmethod
    def _inspect_existing_routes(
        cls,
        *,
        active_routes: Sequence[InfoCenterServiceRoute],
        timeout_sec: float,
    ) -> List[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]]:
        out: List[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]] = []
        failures: Dict[str, str] = {}
        for route in active_routes:
            client = NodeControlClient(route.control_addr, timeout_sec=timeout_sec)
            try:
                info = client.get_service_status(service_id=route.service_id)
                out.append((route, info))
            except Exception as exc:
                failures[route.node_id] = repr(exc)
            finally:
                client.close()
        if failures:
            raise RuntimeError(f"failed to inspect existing active service routes: {failures}")
        return out

    @classmethod
    def _reuse_existing_group(
        cls,
        *,
        owner_client_id: str,
        service_name: str,
        artifact_code_version: str,
        cache_payload: Dict[str, object],
        active_routes: Sequence[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]],
        discovered_node_map: Dict[str, InfoCenterNode],
        timeout_sec: float,
        breaker_enabled: bool,
        breaker_failure_threshold: int,
        breaker_cooldown_sec: float,
        breaker_max_cooldown_sec: float,
        session_cache_file: Path,
    ) -> "MultiNodeServiceGroup":
        cache_nodes = cache_payload.get("nodes")
        if not isinstance(cache_nodes, dict):
            raise RuntimeError("invalid local service session cache: nodes missing")

        sessions: Dict[str, ServiceSessionClient] = {}
        clients: Dict[str, NodeControlClient] = {}
        nodes: Dict[str, InfoCenterNode] = {}

        try:
            for route, info in active_routes:
                node = discovered_node_map.get(route.node_id)
                if node is None:
                    raise RuntimeError(
                        f"existing service route is outside current discovery scope: node_id={route.node_id}"
                    )

                cached_node = cache_nodes.get(route.node_id)
                if not isinstance(cached_node, dict):
                    raise RuntimeError(
                        f"local service session cache missing node entry for reuse: node_id={route.node_id}"
                    )

                cached_service_id = str(cached_node.get("service_id", "")).strip()
                cached_token = str(cached_node.get("service_token", "")).strip()
                if cached_service_id != route.service_id:
                    raise RuntimeError(
                        f"local service session cache is stale for node={route.node_id}: "
                        f"cached_service_id={cached_service_id} route_service_id={route.service_id}"
                    )
                if not cached_token:
                    raise RuntimeError(f"local service session cache missing token for node={route.node_id}")

                client = NodeControlClient(route.control_addr, timeout_sec=timeout_sec)
                try:
                    hb = client.heartbeat_service(
                        owner_client_id=owner_client_id,
                        service_id=route.service_id,
                        service_token=cached_token,
                        seq=0,
                    )
                except Exception:
                    client.close()
                    raise

                sessions[route.node_id] = ServiceSessionClient(
                    _client=client,
                    owner_client_id=owner_client_id,
                    service_id=route.service_id,
                    service_token=cached_token,
                    http_base_url=str(cached_node.get("http_base_url", "") or info.http_base_url or route.http_base_url),
                    heartbeat_timeout_sec=max(
                        1,
                        int(
                            cached_node.get("heartbeat_timeout_sec", 0)
                            or (max(1, int(hb.next_heartbeat_in_sec or 0)) * 2)
                            or 30
                        ),
                    ),
                    worker_count=max(1, int(cached_node.get("worker_count", 0) or info.worker_count or route.worker_count or 1)),
                    status=hb.status or info.status,
                )
                clients[route.node_id] = client
                nodes[route.node_id] = node
        except Exception:
            for client in clients.values():
                try:
                    client.close()
                except Exception:
                    pass
            raise

        group = cls(
            owner_client_id=owner_client_id,
            service_name=service_name,
            sessions=sessions,
            nodes=nodes,
            failures={},
            breaker_enabled=bool(breaker_enabled),
            breaker_failure_threshold=max(1, int(breaker_failure_threshold)),
            breaker_cooldown_sec=max(0.1, float(breaker_cooldown_sec)),
            breaker_max_cooldown_sec=max(0.1, float(breaker_max_cooldown_sec)),
            _clients=clients,
            _session_cache_file=session_cache_file,
            _artifact_code_version=artifact_code_version,
        )
        group._persist_session_cache()
        return group

    @classmethod
    def _end_existing_group(
        cls,
        *,
        owner_client_id: str,
        cache_payload: Dict[str, object],
        active_routes: Sequence[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]],
        timeout_sec: float,
        reason: str,
    ) -> None:
        cache_nodes = cache_payload.get("nodes")
        if not isinstance(cache_nodes, dict):
            raise RuntimeError("invalid local service session cache: nodes missing")

        failures: Dict[str, str] = {}
        for route, _info in active_routes:
            cached_node = cache_nodes.get(route.node_id)
            if not isinstance(cached_node, dict):
                failures[route.node_id] = "missing cached node entry"
                continue
            cached_service_id = str(cached_node.get("service_id", "")).strip()
            cached_token = str(cached_node.get("service_token", "")).strip()
            if cached_service_id != route.service_id or not cached_token:
                failures[route.node_id] = "stale or missing cached token"
                continue

            client = NodeControlClient(route.control_addr, timeout_sec=timeout_sec)
            try:
                client.end_service(
                    owner_client_id=owner_client_id,
                    service_id=route.service_id,
                    service_token=cached_token,
                    reason=reason,
                )
            except Exception as exc:
                failures[route.node_id] = repr(exc)
            finally:
                client.close()

        if failures:
            raise RuntimeError(f"failed to end existing active service before replace: {failures}")

    @staticmethod
    def _cleanup_created_services(
        *,
        sessions: Dict[str, ServiceSessionClient],
        clients: Dict[str, NodeControlClient],
        reason: str,
    ) -> None:
        for session in sessions.values():
            try:
                session.end(reason)
            except Exception:
                pass
        for client in clients.values():
            try:
                client.close()
            except Exception:
                pass

    def _persist_session_cache(self) -> None:
        if self._session_cache_file is None or not self.sessions:
            return
        payload: Dict[str, object] = {
            "schema_version": _SERVICE_SESSION_SCHEMA_VERSION,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "owner_client_id": self.owner_client_id,
            "service_name": self.service_name,
            "artifact_code_version": self._artifact_code_version,
            "nodes": {},
        }
        nodes_payload: Dict[str, object] = {}
        for node_id, session in sorted(self.sessions.items()):
            node = self.nodes.get(node_id)
            control_addr = ""
            if node is not None:
                control_addr = node.control_addr
            elif node_id in self._clients:
                control_addr = self._clients[node_id].target
            nodes_payload[node_id] = {
                "control_addr": control_addr,
                "service_id": session.service_id,
                "service_token": session.service_token,
                "http_base_url": session.http_base_url,
                "heartbeat_timeout_sec": int(session.heartbeat_timeout_sec),
                "worker_count": int(session.worker_count),
            }
        payload["nodes"] = nodes_payload
        _write_private_json(self._session_cache_file, payload)

    def _clear_session_cache(self) -> None:
        if self._session_cache_file is None:
            return
        try:
            self._session_cache_file.unlink()
        except FileNotFoundError:
            pass

    def __post_init__(self) -> None:
        if self.breaker_max_cooldown_sec < self.breaker_cooldown_sec:
            self.breaker_max_cooldown_sec = self.breaker_cooldown_sec
        for node_id in self.sessions:
            self._breaker_states.setdefault(node_id, NodeCircuitState())

    def _breaker_state_locked(self, node_id: str) -> NodeCircuitState:
        state = self._breaker_states.get(node_id)
        if state is None:
            state = NodeCircuitState()
            self._breaker_states[node_id] = state
        return state

    def _breaker_cooldown_locked(self, state: NodeCircuitState) -> float:
        exp = max(0, state.open_count - 1)
        cooldown = self.breaker_cooldown_sec * (2.0**exp)
        return min(self.breaker_max_cooldown_sec, cooldown)

    def _breaker_mark_success(self, node_id: str) -> None:
        if not self.breaker_enabled:
            return
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            state.state = "closed"
            state.consecutive_failures = 0
            state.open_until_monotonic = 0.0
            state.open_count = 0
            state.probe_in_flight = False
            state.last_error = ""

    def _breaker_mark_failure(self, node_id: str, exc: Exception) -> None:
        if not self.breaker_enabled:
            return
        now = time.monotonic()
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            state.last_error = repr(exc)
            if state.state == "half_open":
                state.consecutive_failures = max(state.consecutive_failures, self.breaker_failure_threshold)
            elif state.state == "closed":
                state.consecutive_failures += 1
            state.probe_in_flight = False

            if state.consecutive_failures < self.breaker_failure_threshold:
                return

            state.state = "open"
            state.open_count += 1
            state.open_until_monotonic = now + self._breaker_cooldown_locked(state)

    def _breaker_candidate_state(self, node_id: str) -> Tuple[str, bool]:
        if not self.breaker_enabled:
            return "closed", True
        now = time.monotonic()
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            if state.state == "open":
                if now >= state.open_until_monotonic:
                    state.state = "half_open"
                    state.probe_in_flight = False
                else:
                    return state.state, False
            if state.state == "half_open" and state.probe_in_flight:
                return state.state, False
            return state.state, True

    def _breaker_before_invoke(self, node_id: str) -> bool:
        if not self.breaker_enabled:
            return True
        now = time.monotonic()
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            if state.state == "open":
                if now < state.open_until_monotonic:
                    return False
                state.state = "half_open"
                state.probe_in_flight = False
            if state.state == "half_open":
                if state.probe_in_flight:
                    return False
                state.probe_in_flight = True
            return True

    def breaker_snapshot(self) -> Dict[str, Dict[str, object]]:
        now = time.monotonic()
        out: Dict[str, Dict[str, object]] = {}
        with self._route_lock:
            for node_id, state in self._breaker_states.items():
                remain = max(0.0, state.open_until_monotonic - now) if state.state == "open" else 0.0
                out[node_id] = {
                    "state": state.state,
                    "consecutive_failures": state.consecutive_failures,
                    "open_count": state.open_count,
                    "cooldown_remaining_sec": round(remain, 3),
                    "probe_in_flight": state.probe_in_flight,
                    "last_error": state.last_error,
                }
        return out

    def __enter__(self) -> "MultiNodeServiceGroup":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(end_services=False)

    def node_ids(self) -> Sequence[str]:
        return list(self.sessions.keys())

    def start_keepalive(self, interval_sec: Optional[float] = None) -> None:
        for session in self.sessions.values():
            session.start_keepalive(interval_sec=interval_sec)

    def stop_keepalive(self) -> None:
        for session in self.sessions.values():
            session.stop_keepalive()

    def status_map(self) -> Dict[str, pb2.ServiceStatusInfo]:
        out: Dict[str, pb2.ServiceStatusInfo] = {}
        for node_id, session in self.sessions.items():
            out[node_id] = session.get_status()
        return out

    def call_on_node(
        self,
        node_id: str,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        via: str = "http",
    ) -> Dict[str, object]:
        session = self.sessions.get(node_id)
        if session is None:
            raise KeyError(f"unknown node_id: {node_id}")
        return session.call(method, payload, timeout_sec=timeout_sec, via=via)

    def _select_node(self, *, strategy: str, refresh_status: bool, exclude: Optional[Set[str]] = None) -> str:
        excluded = exclude or set()
        all_candidates = [nid for nid in sorted(self.sessions.keys()) if nid not in excluded]
        candidates = []
        state_rank: Dict[str, int] = {}
        for node_id in all_candidates:
            breaker_state, allowed = self._breaker_candidate_state(node_id)
            if not allowed:
                continue
            # Prefer closed nodes over half-open probe nodes.
            state_rank[node_id] = 0 if breaker_state == "closed" else 1
            candidates.append(node_id)
        if not candidates:
            raise RuntimeError("no available service node (all candidates may be open-circuit)")

        if strategy == "round_robin":
            idx = self._route_index % len(candidates)
            self._route_index += 1
            return candidates[idx]

        if strategy != "least_inflight":
            raise ValueError("strategy must be one of: least_inflight, round_robin")

        best_node_id = ""
        best_key: Optional[Tuple[int, int, int, str]] = None
        for node_id in candidates:
            session = self.sessions[node_id]
            info: Optional[pb2.ServiceStatusInfo] = None
            if refresh_status:
                try:
                    info = session.get_status()
                except Exception:
                    continue
                if info.status != pb2.SERVICE_STATUS_RUNNING:
                    continue
            in_flight = int(info.in_flight if info is not None else 0)
            alive_workers = int(info.alive_workers if info is not None else session.worker_count)
            key = (state_rank.get(node_id, 0), in_flight, -alive_workers, node_id)
            if best_key is None or key < best_key:
                best_key = key
                best_node_id = node_id

        if best_node_id:
            return best_node_id

        idx = self._route_index % len(candidates)
        self._route_index += 1
        return candidates[idx]

    def call_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
        max_attempts: int = 0,
        via: str = "http",
    ) -> Tuple[str, Dict[str, object]]:
        if not self.sessions:
            raise RuntimeError("no active service sessions")

        tries = max(1, int(max_attempts or len(self.sessions)))
        excluded: Set[str] = set()
        last_error: Optional[Exception] = None

        for _ in range(tries):
            node_id = self._select_node(strategy=strategy, refresh_status=refresh_status, exclude=excluded)
            excluded.add(node_id)
            if not self._breaker_before_invoke(node_id):
                continue
            try:
                resp = self.sessions[node_id].call(method, payload, timeout_sec=timeout_sec, via=via)
                self._breaker_mark_success(node_id)
                return node_id, resp
            except Exception as exc:
                last_error = exc
                self._breaker_mark_failure(node_id, exc)

        raise RuntimeError(f"call failed on all candidate nodes: {last_error}")

    async def acall_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
        max_attempts: int = 0,
        via: str = "http",
    ) -> Tuple[str, Dict[str, object]]:
        """异步版本的 call_balanced。

        使用 asyncio 在线程池中执行同步 HTTP 调用，不阻塞事件循环。

        Args:
            method: 服务方法名
            payload: 调用参数
            timeout_sec: 超时时间
            strategy: 节点选择策略（"least_inflight" 或 "round_robin"）
            refresh_status: 是否在选择节点前刷新状态
            max_attempts: 最大尝试次数
            via: 调用方式（"http" 或 "grpc"）

        Returns:
            Tuple[str, Dict[str, object]]: (节点 ID, 响应结果)

        Raises:
            RuntimeError: 所有节点都调用失败时
        """
        if not self.sessions:
            raise RuntimeError("no active service sessions")

        tries = max(1, int(max_attempts or len(self.sessions)))
        excluded: Set[str] = set()
        last_error: Optional[Exception] = None

        loop = asyncio.get_running_loop()
        for _ in range(tries):
            node_id = self._select_node(strategy=strategy, refresh_status=refresh_status, exclude=excluded)
            excluded.add(node_id)
            if not self._breaker_before_invoke(node_id):
                continue
            try:
                # 在线程池中执行同步调用，不阻塞事件循环
                resp = await loop.run_in_executor(
                    None,
                    lambda: self.sessions[node_id].call(method, payload, timeout_sec=timeout_sec, via=via),
                )
                self._breaker_mark_success(node_id)
                return node_id, resp
            except Exception as exc:
                last_error = exc
                self._breaker_mark_failure(node_id, exc)

        raise RuntimeError(f"call failed on all candidate nodes: {last_error}")

    async def acall_all(
        self,
        method: str,
        payloads: Union[List[Dict[str, object]], Dict[str, object]],
        *,
        timeout_sec: float = 60.0,
        via: str = "http",
        max_concurrency: int = 100,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        """并发调用所有节点。

        将 payload 同时发送到所有可用节点，返回所有结果。

        Args:
            method: 服务方法名
            payloads: 可以是单个 payload（发送给所有节点）或 payload 列表（与节点一一对应）
            timeout_sec: 单次调用超时时间
            via: 调用方式（"http" 或 "grpc"）
            max_concurrency: 最大并发数

        Returns:
            List[Tuple[节点ID, 响应, 异常]]：所有节点的结果列表
        """
        if not self.sessions:
            raise RuntimeError("no active service sessions")

        nodes = list(self.sessions.keys())
        # 如果是单个 payload，复制给所有节点
        if isinstance(payloads, dict):
            payloads = [dict(payloads) for _ in nodes]
        elif isinstance(payloads, list):
            if len(payloads) != len(nodes):
                raise ValueError(f"payload list length ({len(payloads)}) must match node count ({len(nodes)})")

        loop = asyncio.get_running_loop()
        semaphore = asyncio.Semaphore(max_concurrency)

        async def _call_single(node_id: str, payload: Dict[str, object]) -> Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]:
            async with semaphore:
                if not self._breaker_before_invoke(node_id):
                    return node_id, None, RuntimeError("circuit breaker open")
                try:
                    resp = await loop.run_in_executor(
                        None,
                        lambda: self.sessions[node_id].call(method, payload, timeout_sec=timeout_sec, via=via),
                    )
                    self._breaker_mark_success(node_id)
                    return node_id, resp, None
                except Exception as exc:
                    self._breaker_mark_failure(node_id, exc)
                    return node_id, None, exc

        tasks = [_call_single(node_id, payload) for node_id, payload in zip(nodes, payloads)]
        return await asyncio.gather(*tasks)

    def end(self, reason: str = "group end") -> Dict[str, Optional[pb2.EndServiceResponse]]:
        self.stop_keepalive()
        out: Dict[str, Optional[pb2.EndServiceResponse]] = {}
        for node_id, session in self.sessions.items():
            try:
                out[node_id] = session.end(reason)
            except Exception:
                out[node_id] = None
        if out and all(
            resp is not None and resp.ok and resp.accepted and resp.status == pb2.SERVICE_STATUS_STOPPED
            for resp in out.values()
        ):
            self._clear_session_cache()
        return out

    def close(self, *, end_services: bool = False, reason: str = "group close") -> None:
        self.stop_keepalive()
        if end_services:
            self.end(reason=reason)
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                pass
        self._clients.clear()


class _CallProxy:
    """服务方法调用代理。

    支持多种调用方式：
    - await proxy(x=1, y=2)  # 异步调用
    - proxy.sync(x=1, y=2)    # 同步调用
    - await proxy.broadcast(x=1)  # 广播到所有节点
    """

    def __init__(
        self,
        method: str,
        group: "MultiNodeServiceGroup",
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
        via: str = "http",
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status
        self._via = via

    def __repr__(self) -> str:
        return f"<CallProxy method={self._method!r}>"

    @property
    def method(self) -> str:
        """返回方法名。"""
        return self._method

    async def __call__(self, **kwargs) -> Dict[str, object]:
        """异步调用服务方法。

        Args:
            **kwargs: 方法参数

        Returns:
            Dict[str, object]: 服务的返回值

        Example:
            >>> result = await group.square(x=7)
            >>> result = await group.fibonacci(n=10)
        """
        _, resp = await self._group.acall_balanced(
            self._method,
            kwargs,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            via=self._via,
        )
        return resp.get("data", resp)

    def __await__(self):
        """支持 await proxy() 语法。"""
        return self().__await__()

    @property
    def sync(self) -> "_SyncCallProxy":
        """返回同步调用代理。

        Example:
            >>> result = group.square.sync(x=7)
        """
        return _SyncCallProxy(
            method=self._method,
            group=self._group,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            via=self._via,
        )

    @property
    def broadcast(self) -> "_BroadcastProxy":
        """返回广播调用代理。

        Example:
            >>> results = await group.square.broadcast(x=7)
            >>> # results = [(node_id, result, error), ...]
        """
        return _BroadcastProxy(
            method=self._method,
            group=self._group,
            timeout_sec=self._timeout_sec,
            via=self._via,
        )

    def with_options(
        self,
        *,
        timeout_sec: Optional[float] = None,
        strategy: Optional[str] = None,
        refresh_status: Optional[bool] = None,
        via: Optional[str] = None,
    ) -> "_CallProxy":
        """返回一个新的代理，使用指定的选项。

        Example:
            >>> proxy = group.square.with_options(timeout_sec=30)
            >>> result = await proxy(x=7)
        """
        return _CallProxy(
            method=self._method,
            group=self._group,
            timeout_sec=timeout_sec if timeout_sec is not None else self._timeout_sec,
            strategy=strategy if strategy is not None else self._strategy,
            refresh_status=refresh_status if refresh_status is not None else self._refresh_status,
            via=via if via is not None else self._via,
        )


class _SyncCallProxy:
    """同步调用代理。"""

    def __init__(
        self,
        method: str,
        group: "MultiNodeServiceGroup",
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
        via: str = "http",
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status
        self._via = via

    def __repr__(self) -> str:
        return f"<SyncCallProxy method={self._method!r}>"

    def __call__(self, **kwargs) -> Dict[str, object]:
        """同步调用服务方法。

        Args:
            **kwargs: 方法参数

        Returns:
            Dict[str, object]: 服务的返回值

        Example:
            >>> result = group.square.sync(x=7)
        """
        _, resp = self._group.call_balanced(
            self._method,
            kwargs,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            via=self._via,
        )
        return resp.get("data", resp)


class _BroadcastProxy:
    """广播调用代理，调用所有节点。"""

    def __init__(
        self,
        method: str,
        group: "MultiNodeServiceGroup",
        *,
        timeout_sec: float = 60.0,
        via: str = "http",
        max_concurrency: int = 100,
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._via = via
        self._max_concurrency = max_concurrency

    def __repr__(self) -> str:
        return f"<BroadcastProxy method={self._method!r}>"

    async def __call__(
        self,
        **kwargs,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        """异步广播调用所有节点。

        Args:
            **kwargs: 方法参数

        Returns:
            List[Tuple[节点ID, 结果, 异常]]: 所有节点的结果

        Example:
            >>> results = await group.square.broadcast(x=7)
            >>> for node_id, result, error in results:
            ...     if error:
            ...         print(f"{node_id}: FAILED - {error}")
            ...     else:
            ...         print(f"{node_id}: {result}")
        """
        return await self._group.acall_all(
            self._method,
            kwargs,
            timeout_sec=self._timeout_sec,
            via=self._via,
            max_concurrency=self._max_concurrency,
        )

    def __await__(self):
        return self().__await__()


class ModuleLikeServiceGroup(MultiNodeServiceGroup):
    """模块化的服务组，像使用 Python 模块一样调用远程服务。

    支持多种调用方式：
    - await group.square(x=7)        # 异步调用
    - group.square.sync(x=7)         # 同步调用
    - await group.square.broadcast() # 广播到所有节点
    - group.list_methods()           # 列出所有可用方法

    Example:
        >>> group = ModuleLikeServiceGroup.deploy_from_infocenter(...)
        >>>
        >>> # 异步调用
        >>> result = await group.square(x=7)
        >>>
        >>> # 批量调用
        >>> results = await asyncio.gather(
        ...     group.square(x=i) for i in range(100)
        ... )
        >>>
        >>> # 同步调用
        >>> result = group.square.sync(x=7)
        >>>
        >>> # 广播调用
        >>> results = await group.square.broadcast(x=7)
    """

    # 缓存已发现的方法列表（使用普通属性，不是 dataclass field）
    _discovered_methods: Optional[List[str]] = None

    def __getattr__(self, name: str):
        """动态代理服务方法。

        Args:
            name: 方法名

        Returns:
            _CallProxy: 方法调用代理

        Raises:
            AttributeError: 如果方法不存在且已成功获取到非空方法列表
        """
        # 避免无限递归和处理特殊属性
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        # 如果还没有尝试过发现方法，先尝试发现
        if self._discovered_methods is None:
            self._ensure_methods_discovered()

        # 验证方法是否存在（空列表也应该验证）
        if self._discovered_methods is not None and name not in self._discovered_methods:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{name}'. "
                f"Available methods: {self._discovered_methods}"
            )

        return _CallProxy(
            method=name,
            group=self,
            timeout_sec=60.0,
            strategy="least_inflight",
            refresh_status=True,
            via="http",
        )

    def _ensure_methods_discovered(self) -> None:
        """确保方法列表已发现。"""
        if self._discovered_methods is not None:
            return

        # 尝试从 session 获取方法
        if self.sessions:
            first_session = next(iter(self.sessions.values()))
            try:
                methods = first_session.list_methods(include_docs=True)
                # ServiceMethodInfo 的字段名是 method
                self._discovered_methods = [m.method for m in methods]
                return
            except Exception:
                pass

        # 无法获取，设置为空列表
        self._discovered_methods = []

    def list_methods(self) -> List[str]:
        """列出所有可用的服务方法。

        Returns:
            List[str]: 方法名列表
        """
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    @property
    def methods(self) -> List[str]:
        """返回所有方法名的列表。

        Example:
            >>> print(group.methods)
            ['square', 'fibonacci', ...]
        """
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    async def call(self, method: str, **kwargs) -> Dict[str, object]:
        """通用异步调用接口。

        Args:
            method: 方法名
            **kwargs: 方法参数

        Returns:
            Dict[str, object]: 服务的返回值
        """
        _, resp = await self.acall_balanced(method, kwargs)
        return resp.get("data", resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        """通用同步调用接口。

        Args:
            method: 方法名
            **kwargs: 方法参数

        Returns:
            Dict[str, object]: 服务的返回值
        """
        _, resp = self.call_balanced(method, kwargs)
        return resp.get("data", resp)

    async def call_all(
        self,
        method: str,
        **kwargs,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        """异步调用所有节点。

        Args:
            method: 方法名
            **kwargs: 方法参数

        Returns:
            List[Tuple[节点ID, 结果, 异常]]: 所有节点的结果
        """
        return await self.acall_all(method, kwargs)

    def __repr__(self) -> str:
        node_ids = list(self.sessions.keys()) if self.sessions else []
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<ModuleLikeServiceGroup "
            f"service={self.service_name!r} "
            f"nodes={len(node_ids)} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )
