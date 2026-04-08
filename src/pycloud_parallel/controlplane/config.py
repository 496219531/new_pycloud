from __future__ import annotations

"""Centralized runtime limits for controlplane client/server paths."""

import os


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return int(default)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"environment variable {name} must be int, got {raw!r}") from exc
    return value


def env_int(name: str, default: int) -> int:
    return _env_int(name, default)


INLINE_PAYLOAD_SOFT_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES", 256 * 1024)
INLINE_PAYLOAD_HARD_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES", 1024 * 1024)
INLINE_PAYLOAD_REQUEST_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_PAYLOAD_REQUEST_LIMIT_BYTES", 4 * 1024 * 1024)
INLINE_RESULT_SOFT_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_RESULT_SOFT_LIMIT_BYTES", 256 * 1024)
INLINE_RESULT_HARD_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", 1024 * 1024)

OBJECT_CHUNK_SIZE_BYTES = _env_int("PYCLOUD_OBJECT_CHUNK_SIZE_BYTES", 256 * 1024)
FILE_HASH_CHUNK_SIZE_BYTES = _env_int("PYCLOUD_FILE_HASH_CHUNK_SIZE_BYTES", 1024 * 1024)

GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES = _env_int("PYCLOUD_GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES", 4 * 1024 * 1024)
GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES = _env_int("PYCLOUD_GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES", 4 * 1024 * 1024)

NODE_WORKER_CAPACITY = _env_int("PYCLOUD_NODE_WORKER_CAPACITY", 32)
NODE_QUEUE_CAPACITY = _env_int("PYCLOUD_NODE_QUEUE_CAPACITY", 4000)
NODE_MAX_WORKERS = _env_int("PYCLOUD_NODE_MAX_WORKERS", 64)
SERVICE_DEFAULT_WORKERS = _env_int("PYCLOUD_SERVICE_DEFAULT_WORKERS", 10)
SERVICE_HEARTBEAT_TIMEOUT_SEC = _env_int("PYCLOUD_SERVICE_HEARTBEAT_TIMEOUT_SEC", 30)


def grpc_channel_options() -> list[tuple[str, int]]:
    return [
        ("grpc.max_send_message_length", int(GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES)),
        ("grpc.max_receive_message_length", int(GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES)),
    ]
