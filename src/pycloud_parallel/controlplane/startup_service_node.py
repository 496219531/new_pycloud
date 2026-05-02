from __future__ import annotations

"""Restricted startup service node built on the normal NodeControl runtime."""

import asyncio
import contextlib
import json
from types import SimpleNamespace
from typing import Any
from urllib.parse import unquote

from pycloud_parallel.controlplane.client_transport import _restore_stream_transport_carrier
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.controlplane.node_runtime_base import NodeRuntimeBase
from pycloud_parallel.execution.call_proxy import _CallProxy
from pycloud_parallel.controlplane.serialization import decode_inline_transport_carrier, is_inline_transport_carrier
from pycloud_parallel.execution.support import _extract_result_ref, _resolve_high_level_service_data


class StartupServiceNode(NodeControlState):
    """NodeControlState variant that only allows startup-time service mounts."""

    accept_service_deploy = False

    def __init__(
        self,
        *,
        enable_internal_executor: bool = False,
        enable_service_session: bool = False,
        accept_service_deploy: bool = False,
        **kwargs: Any,
    ) -> None:
        del accept_service_deploy
        super().__init__(
            enable_internal_executor=enable_internal_executor,
            enable_service_session=enable_service_session,
            **kwargs,
        )
        self.accept_service_deploy = False
        self._local_service_id = ""
        self._local_service_token = ""
        self._local_service_name = ""
        self._local_owner_client_id = ""
        self._local_code_version = ""
        self._local_policy_id = ""
        self._local_service_methods: list[str] = []
        self._local_ipc_server = None

    def mount_prepared_service(self, **kwargs: Any):
        session = super().create_service(**kwargs)
        session.node_managed = True
        self._local_service_id = str(session.service_id or "")
        self._local_service_token = str(session.service_token or "")
        self._local_service_name = str(session.service_name or "")
        self._local_owner_client_id = str(session.owner_client_id or "")
        self._local_code_version = str(session.code_version or "")
        self._local_policy_id = str(session.policy_id or "")
        self._local_service_methods = sorted(str(name) for name in getattr(session, "methods", {}).keys())
        return session

    def mount_python_module_service(self, **kwargs: Any):
        mount = super().mount_python_module_service(**kwargs)
        self._local_service_id = str(mount.service_id or "")
        self._local_service_token = ""
        self._local_service_name = str(mount.service_name or "")
        self._local_owner_client_id = ""
        self._local_code_version = ""
        self._local_policy_id = str(mount.policy_id or "")
        status, body = mount.methods_handler(False)
        if int(status) == 200:
            self._local_service_methods = sorted(
                str(item.get("method", "") or "")
                for item in list(body.get("methods") or [])
                if isinstance(item, dict) and str(item.get("method", "") or "").strip()
            )
        return mount

    def start_local_ipc(self) -> None:
        if not self._local_service_name:
            raise RuntimeError("startup service is not mounted")
        if self._local_ipc_server is not None:
            return
        from pycloud_parallel.controlplane.local_ipc import start_local_service_ipc

        self._local_ipc_server = start_local_service_ipc(
            node=self,
            service_name=self._local_service_name,
        )

    @property
    def service_id(self) -> str:
        return self._local_service_id

    @property
    def service_name(self) -> str:
        return self._local_service_name

    @property
    def service_token(self) -> str:
        return self._local_service_token

    @property
    def owner_client_id(self) -> str:
        return self._local_owner_client_id

    @property
    def code_version(self) -> str:
        return self._local_code_version

    @property
    def policy_id(self) -> str:
        return self._local_policy_id

    @property
    def methods(self) -> list[str]:
        return list(self._local_service_methods)

    def _local_node_key(self) -> str:
        return str(self.node_instance_id or self.node_id or "local").strip()

    @property
    def sessions(self) -> dict[str, Any]:
        if not self._local_service_id:
            return {}
        session = getattr(self, "_services", {}).get(self._local_service_id)
        return {self._local_node_key(): session} if session is not None else {}

    @property
    def nodes(self) -> dict[str, Any]:
        key = self._local_node_key()
        return {
            key: SimpleNamespace(
                node_id=str(self.node_id or key),
                node_instance_id=str(self.node_instance_id or key),
                control_addr="local" if self._local_ipc_server is not None else "",
                healthy=True,
            )
        }

    @property
    def failures(self) -> dict[str, str]:
        return {}

    def node_ids(self) -> list[str]:
        return [str(self.node_id or self._local_node_key())]

    def node_instance_ids(self) -> list[str]:
        return [self._local_node_key()]

    def route_summary(self) -> list[dict[str, object]]:
        if not self._local_service_id:
            return []
        return [
            {
                "node_instance_id": self._local_node_key(),
                "node_id": str(self.node_id or ""),
                "control_addr": "local" if self._local_ipc_server is not None else "",
                "service_name": self._local_service_name,
                "service_id": self._local_service_id,
                "http_base_url": str(self.service_http_base_url or ""),
            }
        ]

    def routes(self) -> list[dict[str, object]]:
        return self.route_summary()

    def list_methods(self, *, include_docs: bool = False) -> list[Any]:
        if include_docs:
            return [
                {"method": name, "qualified_name": name, "doc": ""}
                for name in self._local_service_methods
            ]
        return self.methods

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        if not self._local_service_id:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        if self._local_service_methods and name not in self._local_service_methods:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{name}'. Available methods: {self._local_service_methods}"
            )
        return _CallProxy(method=name, group=self, timeout_sec=60.0, refresh_status=False)

    def call_balanced(
        self,
        method: str,
        payload: dict,
        *,
        timeout_sec: float = 60.0,
        strategy: str = "",
        refresh_status: bool = False,
        **kwargs: Any,
    ):
        serialization_mode = str(kwargs.pop("serialization_mode", "") or "")
        del strategy, refresh_status, kwargs
        if not self._local_service_id:
            raise RuntimeError("startup service is not mounted")
        status, body = self.call_service(
            service_id=self._local_service_id,
            method=method,
            payload=dict(payload or {}),
            service_token=self._local_service_token,
            timeout_sec=timeout_sec,
            serialization_mode=serialization_mode,
        )
        if int(status) >= 400 or not bool(body.get("ok", False)):
            raise RuntimeError(str(body.get("error") or body.get("error_type") or "startup service call failed"))
        return str(self.node_instance_id or self.node_id or "startup"), body

    async def acall_balanced(self, method: str, payload: dict, **kwargs: Any):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: self.call_balanced(method, payload, **kwargs))

    async def acall_all(
        self,
        method: str,
        payloads: Any,
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ):
        del max_concurrency
        if isinstance(payloads, list):
            if len(payloads) != 1:
                raise ValueError("local startup broadcast accepts exactly one payload")
            payload = dict(payloads[0] or {})
        else:
            payload = dict(payloads or {})
        node_id = str(self.node_instance_id or self.node_id or "startup")
        try:
            called_node_id, response = await self.acall_balanced(method, payload, timeout_sec=timeout_sec)
            return [(called_node_id or node_id, response, None)]
        except Exception as exc:
            return [(node_id, None, exc)]

    def stream_call(
        self,
        method: str,
        payload: dict,
        *,
        timeout_sec: float = 60.0,
        strategy: str = "",
        refresh_status: bool = False,
        serialization_mode: str = "",
    ):
        del strategy, refresh_status
        if not self._local_service_id:
            raise RuntimeError("startup service is not mounted")
        handled = self._invoke_service_stream_http(
            service_id=self._local_service_id,
            method=method,
            payload=dict(payload or {}),
            service_token=self._local_service_token,
            timeout_sec=timeout_sec,
            serialization_mode=serialization_mode,
            use_transport_result=True,
        )
        if isinstance(handled, tuple):
            status, body = handled
            raise RuntimeError(str(body.get("error") or f"startup service stream failed status={status}"))

        node_id = str(self.node_instance_id or self.node_id or "startup")
        for raw_line in handled.body_iter:
            line = bytes(raw_line or b"").strip()
            if not line:
                continue
            event = _restore_stream_transport_carrier(json.loads(line.decode("utf-8")))
            event_name = str(event.get("event", "") or "")
            if event_name == "item":
                item_data = event.get("data")
                if is_inline_transport_carrier(item_data):
                    item_data = decode_inline_transport_carrier(item_data, context="service_result")
                yield _resolve_high_level_service_data(self, node_id=node_id, response={"data": item_data})
                continue
            if event_name == "done":
                if bool(event.get("ok", False)):
                    return
                raise RuntimeError(str(event.get("error", "startup service stream failed")))

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        if target_path:
            raise ValueError("local startup fetch_result_data does not support target_path")
        ref = _extract_result_ref(response_or_data)  # type: ignore[arg-type]
        if ref is None:
            if isinstance(response_or_data, dict) and "data" in response_or_data:
                return response_or_data["data"]
            return response_or_data
        return self.data_store.resolve_data_ref(ref)

    def service_report_payloads(self, *, include_stopped: bool = False):
        return [
            *NodeRuntimeBase.startup_service_report_payloads(self),
            *NodeControlState.service_report_payloads(self, include_stopped=include_stopped),
        ]

    def update_globals(
        self,
        values: dict[str, Any],
        *,
        service_id: str = "",
        service_name: str = "",
    ) -> str:
        if self._matching_startup_services(service_id=service_id, service_name=service_name):
            return NodeRuntimeBase.update_startup_service_globals(
                self,
                values,
                service_id=service_id,
                service_name=service_name,
            )
        normalized_service_id = str(service_id or "").strip()
        normalized_service_name = str(service_name or "").strip()
        with self._lock:
            has_runtime_service = any(
                (not normalized_service_id or session.service_id == normalized_service_id)
                and (not normalized_service_name or session.service_name == normalized_service_name)
                for session in self._services.values()
            )
        if (normalized_service_id or normalized_service_name) and not has_runtime_service:
            return NodeRuntimeBase.update_startup_service_globals(
                self,
                values,
                service_id=service_id,
                service_name=service_name,
            )
        return NodeControlState.update_globals(
            self,
            values,
            service_id=service_id,
            service_name=service_name,
        )

    def apply_managed_globals(
        self,
        values: dict[str, Any],
        *,
        service_id: str = "",
        service_name: str = "",
    ) -> str:
        return self.update_globals(values, service_id=service_id, service_name=service_name)

    def _extra_get_mounted_startup_service(self, service_id: str, path_parts: list[str], query: dict[str, list[str]]):
        handled = NodeRuntimeBase._extra_get_mounted_startup_service(self, service_id, path_parts, query)
        if handled is not None:
            return handled
        if len(path_parts) == 2 and path_parts[0] == "objects":
            object_id = unquote(str(path_parts[1] or ""))
            artifact = self.get_object_artifact(object_id)
            if getattr(artifact, "storage_backend", "file") == "segment":
                with open(artifact.segment_path, "rb") as fp:
                    fp.seek(max(0, int(getattr(artifact, "segment_offset", 0) or 0)))
                    body = fp.read(max(0, int(getattr(artifact, "segment_length", artifact.size_bytes) or artifact.size_bytes)))
            else:
                with open(artifact.path, "rb") as fp:
                    body = fp.read()
            return 200, body, "application/octet-stream"
        return None

    def close(self) -> None:
        if self._local_ipc_server is not None:
            with contextlib.suppress(Exception):
                self._local_ipc_server.close()
            self._local_ipc_server = None
        NodeControlState.close(self)
        NodeRuntimeBase.close(self)

    def create_service(self, *args: Any, **kwargs: Any):
        del args, kwargs
        raise RuntimeError("this node only supports startup-mounted services; dynamic service deployment is disabled")


__all__ = ["StartupServiceNode"]
