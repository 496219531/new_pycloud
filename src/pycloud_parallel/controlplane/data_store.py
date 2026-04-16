from __future__ import annotations

"""Internal data-store facade for uploaded objects and spilled results."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from pycloud_parallel.controlplane.data_ref import DataRef, coerce_data_ref


@dataclass(frozen=True)
class StoredDataArtifact:
    object_id: str
    format: str
    size_bytes: int
    materialize_as: str
    storage_backend: str = "file"
    segment_relpath: str = ""
    segment_offset: int = 0
    segment_length: int = 0


@dataclass
class DataStore:
    object_dir: str
    node_id: str = ""
    control_addr: str = ""
    put_uploaded_file_impl: Optional[Callable[..., Any]] = None
    store_path_impl: Optional[Callable[[Path], StoredDataArtifact]] = None
    store_dataframe_impl: Optional[Callable[[Any], StoredDataArtifact]] = None
    store_series_impl: Optional[Callable[[Any], StoredDataArtifact]] = None
    store_ndarray_impl: Optional[Callable[[Any], StoredDataArtifact]] = None
    register_stored_result_impl: Optional[Callable[[StoredDataArtifact], StoredDataArtifact]] = None
    resolve_data_ref_impl: Optional[Callable[[DataRef], Any]] = None

    def put_uploaded_file(
        self,
        *,
        object_id: str,
        format: str = "",
        uploaded_path: str,
        actual_sha256: str,
        size_bytes: int,
    ):
        if self.put_uploaded_file_impl is None:
            raise RuntimeError("DataStore.put_uploaded_file is not configured")
        return self.put_uploaded_file_impl(
            object_id=object_id,
            format=format,
            uploaded_path=uploaded_path,
            actual_sha256=actual_sha256,
            size_bytes=size_bytes,
        )

    def register_stored_result(self, result: StoredDataArtifact) -> StoredDataArtifact:
        if self.register_stored_result_impl is None:
            return result
        return self.register_stored_result_impl(result)

    def store_path(self, path: Path) -> StoredDataArtifact:
        if self.store_path_impl is None:
            raise RuntimeError("DataStore.store_path is not configured")
        return self.store_path_impl(path)

    def store_dataframe(self, frame: Any) -> StoredDataArtifact:
        if self.store_dataframe_impl is None:
            raise RuntimeError("DataStore.store_dataframe is not configured")
        return self.store_dataframe_impl(frame)

    def store_series(self, series: Any) -> StoredDataArtifact:
        if self.store_series_impl is None:
            raise RuntimeError("DataStore.store_series is not configured")
        return self.store_series_impl(series)

    def store_ndarray(self, array: Any) -> StoredDataArtifact:
        if self.store_ndarray_impl is None:
            raise RuntimeError("DataStore.store_ndarray is not configured")
        return self.store_ndarray_impl(array)

    def resolve_data_ref(self, ref: DataRef | object) -> Any:
        if self.resolve_data_ref_impl is None:
            raise RuntimeError("DataStore.resolve_data_ref is not configured")
        data_ref = ref if isinstance(ref, DataRef) else coerce_data_ref(ref)
        return self.resolve_data_ref_impl(data_ref)

    def data_ref_from_stored_artifact(self, result: StoredDataArtifact) -> DataRef:
        normalized_control_addr = str(self.control_addr or "").strip()
        return DataRef(
            ref_id=str(result.object_id or ""),
            storage_id=str(result.object_id or ""),
            logical_type="",
            format=str(result.format or "bin"),
            size_bytes=int(result.size_bytes or 0),
            materialize_as=str(result.materialize_as or "path"),
            locator_kind="node_control" if normalized_control_addr else "node_local",
            locator_token=normalized_control_addr,
            node_id=str(self.node_id or ""),
            control_addr=normalized_control_addr,
        )

    def result_ref_from_stored_artifact(self, result: StoredDataArtifact) -> DataRef:
        normalized_control_addr = str(self.control_addr or "").strip()
        return DataRef(
            ref_id=str(result.object_id or ""),
            storage_id=str(result.object_id or ""),
            logical_type="",
            node_id=str(self.node_id or ""),
            format=str(result.format or "bin"),
            size_bytes=int(result.size_bytes or 0),
            materialize_as=str(result.materialize_as or "path"),
            locator_kind="node_control" if normalized_control_addr else "node_local",
            locator_token=normalized_control_addr,
            control_addr=normalized_control_addr,
        )


__all__ = [
    "DataStore",
    "StoredDataArtifact",
]
