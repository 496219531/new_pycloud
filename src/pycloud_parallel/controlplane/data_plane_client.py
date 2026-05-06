from __future__ import annotations

"""Thin client for the result data-plane download endpoint."""

import json
import tempfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.client_transport import _materialize_downloaded_result
from pycloud_parallel.controlplane.config import OBJECT_CHUNK_SIZE_BYTES
from pycloud_parallel.controlplane.http_client import _friendly_http_connect_error, target_to_base_url
from pycloud_parallel.data.ref import DataRef, maybe_data_ref


class DataPlaneClient:
    def __init__(self, target: str, *, timeout_sec: float = 30.0) -> None:
        self.target = str(target or "").strip()
        self.base_url = target_to_base_url(self.target)
        self.timeout_sec = max(0.1, float(timeout_sec))

    def download_ref_to_file(self, result_ref: DataRef | object, *, target_path: str) -> Path:
        data_ref = maybe_data_ref(result_ref)
        if data_ref is None:
            raise TypeError("result_ref must be a DataRef-compatible value")
        ref_id = str(data_ref.ref_id or "").strip()
        if not ref_id:
            raise ValueError("DataRef ref_id is required for data-plane download")
        path = Path(target_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        url = f"{self.base_url}/data/refs/{quote(ref_id, safe='')}/download"
        req = Request(url, method="GET")
        try:
            with urlopen(req, timeout=self.timeout_sec) as resp:
                with open(path, "wb") as fp:
                    while True:
                        chunk = resp.read(max(1, int(OBJECT_CHUNK_SIZE_BYTES)))
                        if not chunk:
                            break
                        fp.write(chunk)
        except HTTPError as exc:
            try:
                body = json.loads((exc.read() or b"{}").decode("utf-8") or "{}")
            except Exception:
                body = {"error": exc.reason}
            raise RuntimeError(str(body.get("error", exc.reason))) from exc
        except URLError as exc:
            raise _friendly_http_connect_error(url=url, exc=exc) from exc
        return path

    def fetch_ref_data(self, result_ref: DataRef | object, *, target_path: str = ""):
        data_ref = maybe_data_ref(result_ref)
        if data_ref is None:
            raise TypeError("result_ref must be a DataRef-compatible value")
        if target_path:
            return self.download_ref_to_file(data_ref, target_path=target_path)
        suffix = Path(f"result{('.' + data_ref.format) if data_ref.format else ''}")
        tmp = tempfile.NamedTemporaryFile(prefix="pycloud-result-", suffix=suffix.suffix, delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        try:
            self.download_ref_to_file(data_ref, target_path=str(tmp_path))
            return _materialize_downloaded_result(tmp_path, result_ref=data_ref)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise


__all__ = ["DataPlaneClient"]
