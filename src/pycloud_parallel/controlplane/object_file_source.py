from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Callable

from pycloud_parallel.controlplane.serialization import (
    dataframe_bundle_parquet_frame,
    serialize_dataframe_bundle,
    serialize_series_bundle,
)
from pycloud_parallel.data.ref import normalize_object_format


def write_dataframe_bundle_file(path: Path, frame: Any) -> None:
    import zipfile

    fd, parquet_name = tempfile.mkstemp(prefix="pycloud-object-file-", suffix=".parquet", dir=str(path.parent))
    os.close(fd)
    parquet_path = Path(parquet_name)
    try:
        dataframe_bundle_parquet_frame(frame).to_parquet(parquet_path, index=False)
        meta = serialize_dataframe_bundle(frame)
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(parquet_path, arcname="data.parquet")
            zf.writestr("meta.json", json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    finally:
        parquet_path.unlink(missing_ok=True)


def write_series_bundle_file(path: Path, series: Any) -> None:
    import zipfile

    fd, parquet_name = tempfile.mkstemp(prefix="pycloud-object-file-", suffix=".parquet", dir=str(path.parent))
    os.close(fd)
    parquet_path = Path(parquet_name)
    try:
        series.to_frame("__pycloud_series_value__").to_parquet(parquet_path, index=False)
        meta = serialize_series_bundle(series)
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(parquet_path, arcname="data.parquet")
            zf.writestr("meta.json", json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    finally:
        parquet_path.unlink(missing_ok=True)


def write_ndarray_file(path: Path, array: Any) -> None:
    import numpy as np

    with path.open("wb") as fp:
        np.save(fp, array, allow_pickle=False)


def write_temp_object_file(*, suffix: str, write_file: Callable[[Path], None], dir: str = "") -> Path:
    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-object-file-", suffix=suffix, dir=(dir or None))
    os.close(fd)
    path = Path(tmp_name)
    try:
        write_file(path)
        return path
    except Exception:
        path.unlink(missing_ok=True)
        raise


def dataframe_bundle_temp_file(frame: Any, *, dir: str = "") -> Path:
    return write_temp_object_file(
        suffix=".dfbundle",
        dir=dir,
        write_file=lambda path: write_dataframe_bundle_file(path, frame),
    )


def series_bundle_temp_file(series: Any, *, dir: str = "") -> Path:
    return write_temp_object_file(
        suffix=".seriesbundle",
        dir=dir,
        write_file=lambda path: write_series_bundle_file(path, series),
    )


def ndarray_temp_file(array: Any, *, format: str = "", dir: str = "") -> tuple[Path, str]:
    normalized_format = normalize_object_format(format or "npy", default="npy")
    path = write_temp_object_file(
        suffix=f".{normalized_format}",
        dir=dir,
        write_file=lambda target: write_ndarray_file(target, array),
    )
    return path, normalized_format
