from __future__ import annotations

"""Centralized runtime limits for controlplane client/server paths."""

from dataclasses import dataclass
import logging
import os
from typing import Literal


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return int(default)
    try:
        value = int(raw)
    except ValueError:
        logging.warning("invalid int env %s=%r; using default %s", name, raw, default)
        return int(default)
    return value


def env_int(name: str, default: int) -> int:
    return _env_int(name, default)


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    logging.warning("invalid bool env %s=%r; using default %s", name, raw, default)
    return bool(default)


def _env_choice(name: str, default: str, choices: set[str]) -> str:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return str(default or "").strip().lower()
    if raw in choices:
        return raw
    logging.warning("invalid choice env %s=%r; using default %s", name, raw, default)
    return str(default or "").strip().lower()


INLINE_PAYLOAD_SOFT_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES", 512 * 1024)
INLINE_PAYLOAD_HARD_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES", 2 * 1024 * 1024)
INLINE_PAYLOAD_REQUEST_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_PAYLOAD_REQUEST_LIMIT_BYTES", 8 * 1024 * 1024)
JOB_PAYLOAD_MAX_BYTES = _env_int("PYCLOUD_JOB_PAYLOAD_MAX_BYTES", 64 * 1024)
JOB_STAGING_REPLICA_COUNT = _env_int("PYCLOUD_JOB_STAGING_REPLICA_COUNT", 2)
JOB_STAGED_REF_TTL_SEC = _env_int("PYCLOUD_JOB_STAGED_REF_TTL_SEC", 24 * 60 * 60)
GATEWAY_STAGE_TTL_SEC = _env_int("PYCLOUD_GATEWAY_STAGE_TTL_SEC", 30 * 60)
GATEWAY_STAGE_GC_INTERVAL_SEC = _env_int("PYCLOUD_GATEWAY_STAGE_GC_INTERVAL_SEC", 60)
GATEWAY_MAX_UPLOAD_FILE_BYTES = _env_int("PYCLOUD_GATEWAY_MAX_UPLOAD_FILE_BYTES", 512 * 1024 * 1024)
GATEWAY_MAX_UPLOAD_TOTAL_BYTES = _env_int("PYCLOUD_GATEWAY_MAX_UPLOAD_TOTAL_BYTES", 1024 * 1024 * 1024)
INLINE_RESULT_SOFT_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_RESULT_SOFT_LIMIT_BYTES", 1024 * 1024)
INLINE_RESULT_HARD_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", 4 * 1024 * 1024)

OBJECT_CHUNK_SIZE_BYTES = _env_int("PYCLOUD_OBJECT_CHUNK_SIZE_BYTES", 256 * 1024)
FILE_HASH_CHUNK_SIZE_BYTES = _env_int("PYCLOUD_FILE_HASH_CHUNK_SIZE_BYTES", 1024 * 1024)
OBJECT_SEGMENT_MAX_BYTES = _env_int("PYCLOUD_OBJECT_SEGMENT_MAX_BYTES", 8 * 1024 * 1024)
OBJECT_SEGMENT_TARGET_BYTES = _env_int("PYCLOUD_OBJECT_SEGMENT_TARGET_BYTES", 64 * 1024 * 1024)
OBJECT_UPLOAD_TRUSTED_PRECHECK = _env_bool("PYCLOUD_OBJECT_UPLOAD_TRUSTED_PRECHECK", True)
TRUST_MODE = _env_choice("PYCLOUD_TRUST_MODE", "trusted", {"trusted", "balanced", "strict"})
OBJECT_TRANSFER_MODE = _env_choice(
    "PYCLOUD_OBJECT_TRANSFER_MODE",
    "auto",
    {"auto", "known_digest_precheck", "single_pass_authoritative"},
)

GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES = _env_int("PYCLOUD_GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES", 16 * 1024 * 1024)
GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES = _env_int("PYCLOUD_GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES", 16 * 1024 * 1024)

NODE_WORKER_CAPACITY = _env_int("PYCLOUD_NODE_WORKER_CAPACITY", 32)
NODE_QUEUE_CAPACITY = _env_int("PYCLOUD_NODE_QUEUE_CAPACITY", 4000)
NODE_MAX_WORKERS = _env_int("PYCLOUD_NODE_MAX_WORKERS", 64)
SERVICE_DEFAULT_WORKERS = _env_int("PYCLOUD_SERVICE_DEFAULT_WORKERS", 10)
SERVICE_HEARTBEAT_TIMEOUT_SEC = _env_int("PYCLOUD_SERVICE_HEARTBEAT_TIMEOUT_SEC", 30)


PayloadMode = Literal["http_call", "job_submit", "task_submit", "managed_globals", "result"]
TrustMode = Literal["trusted", "balanced", "strict"]
ObjectTransferMode = Literal["auto", "known_digest_precheck", "single_pass_authoritative"]


@dataclass(frozen=True)
class PayloadLimits:
    inline_payload_soft_limit_bytes: int
    inline_payload_hard_limit_bytes: int
    inline_payload_request_limit_bytes: int
    inline_result_soft_limit_bytes: int
    inline_result_hard_limit_bytes: int
    object_chunk_size_bytes: int
    file_hash_chunk_size_bytes: int


@dataclass(frozen=True)
class PayloadPolicy:
    mode: PayloadMode
    limits: PayloadLimits
    objectify_strings_as_files: bool = False
    objectify_pathlikes: bool = False
    objectify_bytes: bool = False
    recurse_containers: bool = True
    consume_on_read: bool = False
    preserve_args_kwargs_container: bool = False
    managed_global_field_names: tuple[str, ...] = ()

    @property
    def inline_payload_soft_limit_bytes(self) -> int:
        return int(self.limits.inline_payload_soft_limit_bytes)

    @property
    def inline_payload_hard_limit_bytes(self) -> int:
        return int(self.limits.inline_payload_hard_limit_bytes)

    @property
    def inline_payload_request_limit_bytes(self) -> int:
        return int(self.limits.inline_payload_request_limit_bytes)

    @property
    def inline_result_soft_limit_bytes(self) -> int:
        return int(self.limits.inline_result_soft_limit_bytes)

    @property
    def inline_result_hard_limit_bytes(self) -> int:
        return int(self.limits.inline_result_hard_limit_bytes)

    @property
    def object_chunk_size_bytes(self) -> int:
        return int(self.limits.object_chunk_size_bytes)

    @property
    def file_hash_chunk_size_bytes(self) -> int:
        return int(self.limits.file_hash_chunk_size_bytes)


def get_runtime_limits() -> PayloadLimits:
    return PayloadLimits(
        inline_payload_soft_limit_bytes=int(INLINE_PAYLOAD_SOFT_LIMIT_BYTES),
        inline_payload_hard_limit_bytes=int(INLINE_PAYLOAD_HARD_LIMIT_BYTES),
        inline_payload_request_limit_bytes=int(INLINE_PAYLOAD_REQUEST_LIMIT_BYTES),
        inline_result_soft_limit_bytes=int(INLINE_RESULT_SOFT_LIMIT_BYTES),
        inline_result_hard_limit_bytes=int(INLINE_RESULT_HARD_LIMIT_BYTES),
        object_chunk_size_bytes=int(OBJECT_CHUNK_SIZE_BYTES),
        file_hash_chunk_size_bytes=int(FILE_HASH_CHUNK_SIZE_BYTES),
    )


def get_trust_mode() -> TrustMode:
    return str(TRUST_MODE or "trusted").strip().lower()  # type: ignore[return-value]


def get_object_transfer_mode() -> ObjectTransferMode:
    return str(OBJECT_TRANSFER_MODE or "auto").strip().lower()  # type: ignore[return-value]


def resolve_object_transfer_mode(*, source_kind: str, local_digest_known: bool) -> ObjectTransferMode:
    mode = get_object_transfer_mode()
    if mode == "auto":
        normalized_source_kind = str(source_kind or "").strip().lower()
        if normalized_source_kind == "memory":
            mode = "known_digest_precheck"
        elif normalized_source_kind == "file":
            mode = "known_digest_precheck" if bool(local_digest_known) else "single_pass_authoritative"
        else:
            raise ValueError(f"unsupported object transfer source_kind: {source_kind!r}")
    if get_trust_mode() != "trusted" and mode == "single_pass_authoritative":
        return "known_digest_precheck"
    return mode  # type: ignore[return-value]


def get_payload_policy(mode: PayloadMode) -> PayloadPolicy:
    limits = get_runtime_limits()
    if mode == "http_call":
        return PayloadPolicy(
            mode=mode,
            limits=limits,
            recurse_containers=True,
            consume_on_read=True,
            preserve_args_kwargs_container=True,
        )
    if mode == "job_submit":
        return PayloadPolicy(
            mode=mode,
            limits=limits,
            recurse_containers=True,
            consume_on_read=True,
            preserve_args_kwargs_container=True,
            managed_global_field_names=("update_globals",),
        )
    if mode == "task_submit":
        return PayloadPolicy(
            mode=mode,
            limits=limits,
            recurse_containers=False,
            consume_on_read=True,
        )
    if mode == "managed_globals":
        return PayloadPolicy(
            mode=mode,
            limits=limits,
            objectify_strings_as_files=True,
            objectify_pathlikes=True,
            objectify_bytes=True,
            recurse_containers=False,
            consume_on_read=False,
        )
    if mode == "result":
        return PayloadPolicy(
            mode=mode,
            limits=limits,
        )
    raise ValueError(f"unsupported payload policy mode: {mode!r}")


def reload_config() -> None:
    """Reload environment-backed limits for tests or dynamic config."""
    globals().update(
        INLINE_PAYLOAD_SOFT_LIMIT_BYTES=_env_int("PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES", 512 * 1024),
        INLINE_PAYLOAD_HARD_LIMIT_BYTES=_env_int("PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES", 2 * 1024 * 1024),
        INLINE_PAYLOAD_REQUEST_LIMIT_BYTES=_env_int("PYCLOUD_INLINE_PAYLOAD_REQUEST_LIMIT_BYTES", 8 * 1024 * 1024),
        JOB_PAYLOAD_MAX_BYTES=_env_int("PYCLOUD_JOB_PAYLOAD_MAX_BYTES", 64 * 1024),
        JOB_STAGING_REPLICA_COUNT=_env_int("PYCLOUD_JOB_STAGING_REPLICA_COUNT", 2),
        JOB_STAGED_REF_TTL_SEC=_env_int("PYCLOUD_JOB_STAGED_REF_TTL_SEC", 24 * 60 * 60),
        GATEWAY_STAGE_TTL_SEC=_env_int("PYCLOUD_GATEWAY_STAGE_TTL_SEC", 30 * 60),
        GATEWAY_STAGE_GC_INTERVAL_SEC=_env_int("PYCLOUD_GATEWAY_STAGE_GC_INTERVAL_SEC", 60),
        GATEWAY_MAX_UPLOAD_FILE_BYTES=_env_int("PYCLOUD_GATEWAY_MAX_UPLOAD_FILE_BYTES", 512 * 1024 * 1024),
        GATEWAY_MAX_UPLOAD_TOTAL_BYTES=_env_int("PYCLOUD_GATEWAY_MAX_UPLOAD_TOTAL_BYTES", 1024 * 1024 * 1024),
        INLINE_RESULT_SOFT_LIMIT_BYTES=_env_int("PYCLOUD_INLINE_RESULT_SOFT_LIMIT_BYTES", 1024 * 1024),
        INLINE_RESULT_HARD_LIMIT_BYTES=_env_int("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES", 4 * 1024 * 1024),
        OBJECT_CHUNK_SIZE_BYTES=_env_int("PYCLOUD_OBJECT_CHUNK_SIZE_BYTES", 256 * 1024),
        FILE_HASH_CHUNK_SIZE_BYTES=_env_int("PYCLOUD_FILE_HASH_CHUNK_SIZE_BYTES", 1024 * 1024),
        OBJECT_SEGMENT_MAX_BYTES=_env_int("PYCLOUD_OBJECT_SEGMENT_MAX_BYTES", 8 * 1024 * 1024),
        OBJECT_SEGMENT_TARGET_BYTES=_env_int("PYCLOUD_OBJECT_SEGMENT_TARGET_BYTES", 64 * 1024 * 1024),
        OBJECT_UPLOAD_TRUSTED_PRECHECK=_env_bool("PYCLOUD_OBJECT_UPLOAD_TRUSTED_PRECHECK", True),
        TRUST_MODE=_env_choice("PYCLOUD_TRUST_MODE", "trusted", {"trusted", "balanced", "strict"}),
        OBJECT_TRANSFER_MODE=_env_choice(
            "PYCLOUD_OBJECT_TRANSFER_MODE",
            "auto",
            {"auto", "known_digest_precheck", "single_pass_authoritative"},
        ),
        GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES=_env_int("PYCLOUD_GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES", 16 * 1024 * 1024),
        GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES=_env_int("PYCLOUD_GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES", 16 * 1024 * 1024),
        NODE_WORKER_CAPACITY=_env_int("PYCLOUD_NODE_WORKER_CAPACITY", 32),
        NODE_QUEUE_CAPACITY=_env_int("PYCLOUD_NODE_QUEUE_CAPACITY", 4000),
        NODE_MAX_WORKERS=_env_int("PYCLOUD_NODE_MAX_WORKERS", 64),
        SERVICE_DEFAULT_WORKERS=_env_int("PYCLOUD_SERVICE_DEFAULT_WORKERS", 10),
        SERVICE_HEARTBEAT_TIMEOUT_SEC=_env_int("PYCLOUD_SERVICE_HEARTBEAT_TIMEOUT_SEC", 30),
    )


def grpc_channel_options() -> list[tuple[str, int]]:
    return [
        ("grpc.max_send_message_length", int(GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES)),
        ("grpc.max_receive_message_length", int(GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES)),
    ]
