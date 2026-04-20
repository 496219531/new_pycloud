from __future__ import annotations

"""Additive object-transfer strategy helpers for multi-mode experiments."""

from pathlib import Path
from typing import Any

from pycloud_parallel.controlplane.config import (
    OBJECT_CHUNK_SIZE_BYTES,
    resolve_object_transfer_mode as _resolve_transfer_mode,
)
from pycloud_parallel.controlplane.node_control_client import NodeControlClient


def resolve_object_transfer_mode(*, source_kind: str, local_digest_known: bool) -> str:
    return str(
        _resolve_transfer_mode(
            source_kind=str(source_kind or "").strip().lower(),
            local_digest_known=bool(local_digest_known),
        )
    )


def upload_file_single_pass_authoritative(
    client: NodeControlClient,
    *,
    file_path: str,
    format: str = "",
    chunk_size: int = 0,
) -> Any:
    return client.upload_object_from_file(
        file_path=file_path,
        format=format,
        chunk_size=max(1, int(chunk_size or OBJECT_CHUNK_SIZE_BYTES)),
        transfer_mode="single_pass_authoritative",
    )


def upload_known_digest_precheck(
    client: NodeControlClient,
    *,
    file_path: str,
    format: str = "",
    chunk_size: int = 0,
    trusted_precheck: bool | None = None,
) -> Any:
    return client.upload_object_from_file(
        file_path=file_path,
        format=format,
        chunk_size=max(1, int(chunk_size or OBJECT_CHUNK_SIZE_BYTES)),
        trusted_precheck=trusted_precheck,
        transfer_mode="known_digest_precheck",
    )


def upload_memory_object_precheck(
    client: NodeControlClient,
    *,
    blob: bytes,
    format: str = "",
    chunk_size: int = 0,
    trusted_precheck: bool | None = None,
) -> Any:
    return client.upload_object_from_bytes(
        blob=blob,
        format=format,
        chunk_size=max(1, int(chunk_size or OBJECT_CHUNK_SIZE_BYTES)),
        trusted_precheck=trusted_precheck,
        transfer_mode="known_digest_precheck",
    )


__all__ = [
    "resolve_object_transfer_mode",
    "upload_file_single_pass_authoritative",
    "upload_known_digest_precheck",
    "upload_memory_object_precheck",
]
