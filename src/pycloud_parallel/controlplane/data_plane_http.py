from __future__ import annotations

"""Minimal result data-plane download facade for registered DataRefs."""

import json
from typing import Dict, Iterator, Tuple, Union
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.data_registry import resolve_data_ref
from pycloud_parallel.controlplane.config import OBJECT_CHUNK_SIZE_BYTES
from pycloud_parallel.controlplane.http_client import target_to_base_url
from pycloud_parallel.controlplane.http_gateway import StreamingHttpResponse
from pycloud_parallel.data.ref import DataRef


def _json_bytes(data: Dict[str, object]) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def _error(status_code: int, message: str) -> Tuple[int, Dict[str, str], bytes]:
    return status_code, {"Content-Type": "application/json; charset=utf-8"}, _json_bytes({"ok": False, "error": str(message)})


def _object_id_for_ref(ref: DataRef) -> str:
    object_id = str(ref.object_id or ref.storage_id or ref.ref_id or "").strip()
    if not object_id:
        raise ValueError("DataRef has no object_id/storage_id/ref_id")
    return object_id


def _open_node_object_download(*, control_addr: str, object_id: str, timeout_sec: float):
    base_url = target_to_base_url(control_addr)
    url = f"{base_url}/objects/{quote(str(object_id or '').strip(), safe='')}/download"
    req = Request(url, method="GET")
    return urlopen(req, timeout=max(0.1, float(timeout_sec)))


class DataPlaneHttpApp:
    def __init__(self, *, target: str, timeout_sec: float = 30.0) -> None:
        self.target = str(target or "").strip()
        self.timeout_sec = max(0.1, float(timeout_sec))

    def handle_get(self, path: str) -> Union[Tuple[int, Dict[str, str], bytes], StreamingHttpResponse, None]:
        parsed = urlparse(path)
        parts = [unquote(x) for x in parsed.path.split("/") if x]
        if len(parts) == 4 and parts[0] == "data" and parts[1] == "refs" and parts[3] == "download":
            return self._download_ref(parts[2])
        return None

    def _download_ref(self, ref_id: str) -> Union[Tuple[int, Dict[str, str], bytes], StreamingHttpResponse]:
        normalized_ref_id = str(ref_id or "").strip()
        if not normalized_ref_id:
            return _error(400, "ref_id is required")
        request_ref = DataRef(ref_id=normalized_ref_id, storage_id=normalized_ref_id, locator_kind="controlplane", locator_token=self.target)
        try:
            resolved = resolve_data_ref(request_ref, target=self.target, timeout_sec=self.timeout_sec)
        except KeyError:
            return _error(404, "data ref not found")
        except Exception as exc:
            return _error(400, str(exc))
        control_addr = str(resolved.control_addr or "").strip()
        if not control_addr:
            return _error(404, "data ref has no healthy node replica")
        try:
            object_id = _object_id_for_ref(resolved.ref)
        except ValueError as exc:
            return _error(400, str(exc))

        try:
            resp = _open_node_object_download(control_addr=control_addr, object_id=object_id, timeout_sec=self.timeout_sec)
            headers = resp.headers
            content_length = int(str(headers.get("Content-Length", "0") or "0"))
            object_format = str(headers.get("X-Pycloud-Object-Format", "") or resolved.ref.format or "")
            size_bytes = str(headers.get("X-Pycloud-Object-Size-Bytes", "") or resolved.ref.size_bytes or content_length or "")
        except HTTPError as exc:
            if exc.code == 404:
                return _error(404, "object not found")
            return _error(exc.code, str(exc.reason or "object download failed"))
        except URLError as exc:
            return _error(502, str(exc.reason or exc))
        except Exception as exc:
            return _error(502, str(exc))

        def _iter_download() -> Iterator[bytes]:
            try:
                while True:
                    chunk = resp.read(max(1, int(OBJECT_CHUNK_SIZE_BYTES)))
                    if not chunk:
                        break
                    yield chunk
            finally:
                resp.close()

        return StreamingHttpResponse(
            status_code=200,
            body_iter=_iter_download(),
            content_type="application/octet-stream",
            content_length=content_length,
            extra_headers={
                "X-Pycloud-Ref-Id": normalized_ref_id,
                "X-Pycloud-Object-Id": object_id,
                "X-Pycloud-Object-Format": object_format,
                "X-Pycloud-Object-Size-Bytes": str(size_bytes),
            },
        )


__all__ = ["DataPlaneHttpApp"]
