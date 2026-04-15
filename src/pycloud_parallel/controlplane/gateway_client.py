from __future__ import annotations

"""Gateway HTTP caller facade extracted from the legacy client module."""

import contextlib
from pathlib import Path
from typing import Dict, List, Optional, Sequence
from urllib.parse import quote, urlencode

from pycloud_parallel.controlplane import client as client_mod
from pycloud_parallel.controlplane.data_ref import maybe_data_ref, with_data_ref_locator
from pycloud_parallel.controlplane.data_registry import DataRegistryClient, resolve_data_ref


class GatewayServiceClient:
    """Thin HTTP + JSON client wrapper for ControlPlane Gateway service calls."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0, service_token: str = "") -> None:
        self.target = target
        self.base_url = client_mod._target_to_base_url(target)
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.service_token = str(service_token or "").strip()

    def close(self) -> None:
        return None

    def __enter__(self) -> "GatewayServiceClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def call(
        self,
        *,
        service_name: str,
        method: str,
        payload: Optional[Dict[str, object]] = None,
        timeout_sec: float = 60.0,
        service_token: Optional[str] = None,
    ) -> Dict[str, object]:
        name = str(service_name or "").strip()
        method_name = str(method or "").strip()
        if not name:
            raise ValueError("service_name is required")
        if not method_name:
            raise ValueError("method is required")
        token = self.service_token if service_token is None else str(service_token or "").strip()
        headers: Dict[str, str] = {}
        if token:
            headers["X-Service-Token"] = token
        params = urlencode({"timeout_sec": f"{max(0.1, float(timeout_sec)):.3f}"})
        routes = []
        try:
            status = self.get_status(service_name=name)
            routes = list(status.get("routes", [])) if isinstance(status, dict) else []
        except Exception:
            routes = []
        clients: List[object] = []
        prepared_payload = payload or {}
        try:
            for item in routes:
                if not isinstance(item, dict):
                    continue
                control_addr = str(item.get("control_addr", "") or "").strip()
                if not control_addr:
                    continue
                clients.append(client_mod.NodeControlClient(control_addr, timeout_sec=self.timeout_sec))
            if clients:
                prepared_payload = client_mod._prepare_remote_call_payload(
                    clients,
                    payload,
                    object_threshold_bytes=client_mod.INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
                )
            serialized_payload = client_mod._serialize_http_call_payload(prepared_payload, context="service call payload")
        finally:
            for client in clients:
                with contextlib.suppress(Exception):
                    client.close()
        response = client_mod._http_json_request(
            base_url=self.base_url,
            path=f"/svc/{quote(name, safe='')}/call/{quote(method_name, safe='')}?{params}",
            method="POST",
            timeout_sec=max(self.timeout_sec, max(0.1, float(timeout_sec)) + 1.0),
            payload=serialized_payload,
            headers=headers,
        )
        return self._attach_controlplane_locator(response)

    def list_methods(self, *, service_name: str, include_docs: bool = False) -> Sequence[Dict[str, object]]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        params = urlencode({"include_docs": "true" if include_docs else "false"})
        resp = client_mod._http_json_request(
            base_url=self.base_url,
            path=f"/svc/{quote(name, safe='')}/methods?{params}",
            method="GET",
            timeout_sec=self.timeout_sec,
        )
        methods = resp.get("methods", [])
        if not isinstance(methods, list):
            raise RuntimeError("invalid methods response")
        return [item for item in methods if isinstance(item, dict)]

    def get_status(self, *, service_name: str) -> Dict[str, object]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        return client_mod._http_json_request(
            base_url=self.base_url,
            path=f"/svc/{quote(name, safe='')}/status",
            method="GET",
            timeout_sec=self.timeout_sec,
        )

    def download_result_to_file(self, response_or_data: object, *, target_path: str) -> Path:
        ref = client_mod._extract_result_ref(response_or_data)
        if ref is None:
            raise ValueError("service result is inline data; no download needed")
        self._touch_data_ref(ref)
        resolved = resolve_data_ref(ref, target=self.target, timeout_sec=self.timeout_sec)
        try:
            with client_mod.NodeControlClient(resolved.control_addr, timeout_sec=self.timeout_sec) as client:
                return client.download_result_to_file(ref, target_path=target_path)
        finally:
            self._release_data_ref_if_consumed(ref)

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        ref = client_mod._extract_result_ref(response_or_data)
        if ref is None:
            if isinstance(response_or_data, dict) and "data" in response_or_data:
                return response_or_data["data"]
            return response_or_data
        self._touch_data_ref(ref)
        resolved = resolve_data_ref(ref, target=self.target, timeout_sec=self.timeout_sec)
        try:
            with client_mod.NodeControlClient(resolved.control_addr, timeout_sec=self.timeout_sec) as client:
                return client.fetch_result_ref_data(ref, target_path=target_path)
        finally:
            self._release_data_ref_if_consumed(ref)

    def _attach_controlplane_locator(self, response: Dict[str, object]) -> Dict[str, object]:
        if not isinstance(response, dict) or "data" not in response:
            return response
        updated = with_data_ref_locator(
            response.get("data"),
            locator_kind="controlplane",
            locator_token=self.target,
        )
        if updated is response.get("data"):
            return response
        body = dict(response)
        body["data"] = updated
        return body

    def _touch_data_ref(self, ref: object) -> None:
        data_ref = maybe_data_ref(ref)
        if data_ref is None or str(data_ref.locator_kind or "").strip().lower() != "controlplane":
            return
        target = str(data_ref.locator_token or self.target or "").strip()
        if not target:
            return
        try:
            DataRegistryClient(target, timeout_sec=self.timeout_sec).touch(data_ref.ref_id)
        except Exception:
            pass

    def _release_data_ref_if_consumed(self, ref: object) -> None:
        data_ref = maybe_data_ref(ref)
        if data_ref is None or not bool(data_ref.consume_on_read):
            return
        target = str(data_ref.locator_token or self.target or "").strip()
        if not target:
            return
        try:
            DataRegistryClient(target, timeout_sec=self.timeout_sec).release(data_ref.ref_id)
        except Exception:
            pass


__all__ = ["GatewayServiceClient"]
