from __future__ import annotations

"""Restricted startup service node built on the normal NodeControl runtime."""

from typing import Any
from urllib.parse import unquote

from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.controlplane.node_runtime_base import NodeRuntimeBase


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

    def mount_prepared_service(self, **kwargs: Any):
        session = super().create_service(**kwargs)
        session.node_managed = True
        return session

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
        NodeControlState.close(self)
        NodeRuntimeBase.close(self)

    def create_service(self, *args: Any, **kwargs: Any):
        del args, kwargs
        raise RuntimeError("this node only supports startup-mounted services; dynamic service deployment is disabled")


__all__ = ["StartupServiceNode"]
