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


PYCLOUD_SYSTEM_MODE = "PYCLOUD_SYSTEM_MODE"
PYCLOUD_TRUST_MODE = "PYCLOUD_TRUST_MODE"
PYCLOUD_OBJECT_TRANSFER_MODE = "PYCLOUD_OBJECT_TRANSFER_MODE"
PYCLOUD_SERIALIZATION_MODE = "PYCLOUD_SERIALIZATION_MODE"
PYCLOUD_DEPENDENCY_POLICY_MODE = "PYCLOUD_DEPENDENCY_POLICY_MODE"
PYCLOUD_EXECUTOR_BACKEND = "PYCLOUD_EXECUTOR_BACKEND"
PYCLOUD_DATAREF_RESOLUTION = "PYCLOUD_DATAREF_RESOLUTION"
PYCLOUD_DATAREF_UPLOAD_STRATEGY = "PYCLOUD_DATAREF_UPLOAD_STRATEGY"
PYCLOUD_GATEWAY_DATAREF_RELAY = "PYCLOUD_GATEWAY_DATAREF_RELAY"
PYCLOUD_JOBQUEUE_RESOLVE_REFS = "PYCLOUD_JOBQUEUE_RESOLVE_REFS"
PYCLOUD_INLINE_TRANSPORT_CHECKSUM = "PYCLOUD_INLINE_TRANSPORT_CHECKSUM"


INLINE_PAYLOAD_SOFT_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES", 512 * 1024)
INLINE_PAYLOAD_HARD_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES", 2 * 1024 * 1024)
INLINE_PAYLOAD_REQUEST_LIMIT_BYTES = _env_int("PYCLOUD_INLINE_PAYLOAD_REQUEST_LIMIT_BYTES", 8 * 1024 * 1024)
LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES = _env_int("PYCLOUD_LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES", 64 * 1024 * 1024)
LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES = _env_int("PYCLOUD_LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES", 256 * 1024 * 1024)
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
INLINE_TRANSPORT_CHECKSUM = _env_bool(PYCLOUD_INLINE_TRANSPORT_CHECKSUM, False)
SYSTEM_MODE = _env_choice(PYCLOUD_SYSTEM_MODE, "trusted_default", {"trusted_default"})
TRUST_MODE = _env_choice(PYCLOUD_TRUST_MODE, "trusted", {"trusted", "balanced", "strict"})
OBJECT_TRANSFER_MODE = _env_choice(
    PYCLOUD_OBJECT_TRANSFER_MODE,
    "auto",
    {"auto", "known_digest_precheck", "single_pass_authoritative"},
)
SERIALIZATION_MODE = _env_choice(
    PYCLOUD_SERIALIZATION_MODE,
    "legacy_v1",
    {"legacy_v1", "structured_v1", "pickle_stable_v1"},
)
DEPENDENCY_POLICY_MODE = _env_choice(
    PYCLOUD_DEPENDENCY_POLICY_MODE,
    "prebuilt",
    {"prebuilt", "node_preinstalled", "allow_install"},
)
EXECUTOR_BACKEND = _env_choice(
    PYCLOUD_EXECUTOR_BACKEND,
    "subprocess_host",
    {"subprocess_host"},
)
DATAREF_RESOLUTION = _env_choice(
    PYCLOUD_DATAREF_RESOLUTION,
    "remote_fetch",
    {"local_only", "remote_fetch"},
)
DATAREF_UPLOAD_STRATEGY = _env_choice(
    PYCLOUD_DATAREF_UPLOAD_STRATEGY,
    "upload_once",
    {"fanout", "upload_once"},
)
GATEWAY_DATAREF_RELAY = _env_choice(
    PYCLOUD_GATEWAY_DATAREF_RELAY,
    "eager",
    {"eager", "lazy"},
)
JOBQUEUE_RESOLVE_REFS = _env_choice(
    PYCLOUD_JOBQUEUE_RESOLVE_REFS,
    "defer_to_worker",
    {"eager", "defer_to_worker"},
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
SystemMode = Literal["trusted_default"]
SerializationMode = Literal["legacy_v1", "structured_v1", "pickle_stable_v1"]
DependencyPolicyMode = Literal["prebuilt", "node_preinstalled", "allow_install"]
ExecutorBackendMode = Literal["subprocess_host"]
DataRefResolutionMode = Literal["local_only", "remote_fetch"]
DataRefUploadStrategy = Literal["fanout", "upload_once"]
GatewayDataRefRelayMode = Literal["eager", "lazy"]
JobQueueResolveRefsMode = Literal["eager", "defer_to_worker"]


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


def get_system_mode() -> SystemMode:
    return str(SYSTEM_MODE or "trusted_default").strip().lower()  # type: ignore[return-value]


def get_object_transfer_mode() -> ObjectTransferMode:
    return str(OBJECT_TRANSFER_MODE or "auto").strip().lower()  # type: ignore[return-value]


def get_serialization_mode() -> SerializationMode:
    return _env_choice(
        PYCLOUD_SERIALIZATION_MODE,
        "legacy_v1",
        {"legacy_v1", "structured_v1", "pickle_stable_v1"},
    )  # type: ignore[return-value]


def get_dependency_policy_mode() -> DependencyPolicyMode:
    return str(DEPENDENCY_POLICY_MODE or "prebuilt").strip().lower()  # type: ignore[return-value]


def get_dataref_resolution() -> DataRefResolutionMode:
    return str(DATAREF_RESOLUTION or "remote_fetch").strip().lower()  # type: ignore[return-value]


def get_dataref_upload_strategy() -> DataRefUploadStrategy:
    return str(DATAREF_UPLOAD_STRATEGY or "upload_once").strip().lower()  # type: ignore[return-value]


def get_gateway_dataref_relay() -> GatewayDataRefRelayMode:
    return str(GATEWAY_DATAREF_RELAY or "eager").strip().lower()  # type: ignore[return-value]


def get_jobqueue_resolve_refs() -> JobQueueResolveRefsMode:
    return str(JOBQUEUE_RESOLVE_REFS or "defer_to_worker").strip().lower()  # type: ignore[return-value]


def get_inline_transport_checksum() -> bool:
    return bool(INLINE_TRANSPORT_CHECKSUM)


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


def get_local_service_payload_policy() -> PayloadPolicy:
    limits = get_runtime_limits()
    local_hard = max(1, int(LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES))
    local_soft = min(max(1, int(LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES)), local_hard)
    return PayloadPolicy(
        mode="http_call",
        limits=PayloadLimits(
            inline_payload_soft_limit_bytes=local_soft,
            inline_payload_hard_limit_bytes=local_hard,
            inline_payload_request_limit_bytes=local_hard,
            inline_result_soft_limit_bytes=limits.inline_result_soft_limit_bytes,
            inline_result_hard_limit_bytes=limits.inline_result_hard_limit_bytes,
            object_chunk_size_bytes=limits.object_chunk_size_bytes,
            file_hash_chunk_size_bytes=limits.file_hash_chunk_size_bytes,
        ),
        recurse_containers=True,
        consume_on_read=True,
        preserve_args_kwargs_container=True,
    )


def reload_config() -> None:
    """Reload environment-backed limits for tests or dynamic config."""
    globals().update(
        INLINE_PAYLOAD_SOFT_LIMIT_BYTES=_env_int("PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES", 512 * 1024),
        INLINE_PAYLOAD_HARD_LIMIT_BYTES=_env_int("PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES", 2 * 1024 * 1024),
        INLINE_PAYLOAD_REQUEST_LIMIT_BYTES=_env_int("PYCLOUD_INLINE_PAYLOAD_REQUEST_LIMIT_BYTES", 8 * 1024 * 1024),
        LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=_env_int("PYCLOUD_LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES", 64 * 1024 * 1024),
        LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES=_env_int("PYCLOUD_LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES", 256 * 1024 * 1024),
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
        INLINE_TRANSPORT_CHECKSUM=_env_bool(PYCLOUD_INLINE_TRANSPORT_CHECKSUM, False),
        SYSTEM_MODE=_env_choice(PYCLOUD_SYSTEM_MODE, "trusted_default", {"trusted_default"}),
        TRUST_MODE=_env_choice(PYCLOUD_TRUST_MODE, "trusted", {"trusted", "balanced", "strict"}),
        OBJECT_TRANSFER_MODE=_env_choice(
            PYCLOUD_OBJECT_TRANSFER_MODE,
            "auto",
            {"auto", "known_digest_precheck", "single_pass_authoritative"},
        ),
        SERIALIZATION_MODE=_env_choice(
            PYCLOUD_SERIALIZATION_MODE,
            "legacy_v1",
            {"legacy_v1", "structured_v1", "pickle_stable_v1"},
        ),
        DEPENDENCY_POLICY_MODE=_env_choice(
            PYCLOUD_DEPENDENCY_POLICY_MODE,
            "prebuilt",
            {"prebuilt", "node_preinstalled", "allow_install"},
        ),
        DATAREF_RESOLUTION=_env_choice(
            PYCLOUD_DATAREF_RESOLUTION,
            "remote_fetch",
            {"local_only", "remote_fetch"},
        ),
        DATAREF_UPLOAD_STRATEGY=_env_choice(
            PYCLOUD_DATAREF_UPLOAD_STRATEGY,
            "upload_once",
            {"fanout", "upload_once"},
        ),
        GATEWAY_DATAREF_RELAY=_env_choice(
            PYCLOUD_GATEWAY_DATAREF_RELAY,
            "eager",
            {"eager", "lazy"},
        ),
        JOBQUEUE_RESOLVE_REFS=_env_choice(
            PYCLOUD_JOBQUEUE_RESOLVE_REFS,
            "defer_to_worker",
            {"eager", "defer_to_worker"},
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


__all__ = [
    "DEPENDENCY_POLICY_MODE",
    "DATAREF_RESOLUTION",
    "DATAREF_UPLOAD_STRATEGY",
    "DataRefResolutionMode",
    "DataRefUploadStrategy",
    "DependencyPolicyMode",
    "FILE_HASH_CHUNK_SIZE_BYTES",
    "GATEWAY_DATAREF_RELAY",
    "GATEWAY_MAX_UPLOAD_FILE_BYTES",
    "GATEWAY_MAX_UPLOAD_TOTAL_BYTES",
    "GATEWAY_STAGE_GC_INTERVAL_SEC",
    "GATEWAY_STAGE_TTL_SEC",
    "GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES",
    "GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES",
    "GatewayDataRefRelayMode",
    "INLINE_PAYLOAD_HARD_LIMIT_BYTES",
    "INLINE_PAYLOAD_REQUEST_LIMIT_BYTES",
    "INLINE_PAYLOAD_SOFT_LIMIT_BYTES",
    "INLINE_RESULT_HARD_LIMIT_BYTES",
    "INLINE_RESULT_SOFT_LIMIT_BYTES",
    "INLINE_TRANSPORT_CHECKSUM",
    "JOB_PAYLOAD_MAX_BYTES",
    "JOB_STAGED_REF_TTL_SEC",
    "JOB_STAGING_REPLICA_COUNT",
    "JOBQUEUE_RESOLVE_REFS",
    "JobQueueResolveRefsMode",
    "LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES",
    "LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES",
    "NODE_MAX_WORKERS",
    "NODE_QUEUE_CAPACITY",
    "NODE_WORKER_CAPACITY",
    "OBJECT_CHUNK_SIZE_BYTES",
    "OBJECT_SEGMENT_MAX_BYTES",
    "OBJECT_SEGMENT_TARGET_BYTES",
    "OBJECT_TRANSFER_MODE",
    "OBJECT_UPLOAD_TRUSTED_PRECHECK",
    "ObjectTransferMode",
    "PayloadLimits",
    "PayloadMode",
    "PayloadPolicy",
    "PYCLOUD_DEPENDENCY_POLICY_MODE",
    "PYCLOUD_DATAREF_RESOLUTION",
    "PYCLOUD_DATAREF_UPLOAD_STRATEGY",
    "PYCLOUD_GATEWAY_DATAREF_RELAY",
    "PYCLOUD_JOBQUEUE_RESOLVE_REFS",
    "PYCLOUD_INLINE_TRANSPORT_CHECKSUM",
    "PYCLOUD_OBJECT_TRANSFER_MODE",
    "PYCLOUD_SERIALIZATION_MODE",
    "PYCLOUD_SYSTEM_MODE",
    "PYCLOUD_TRUST_MODE",
    "SERIALIZATION_MODE",
    "SERVICE_DEFAULT_WORKERS",
    "SERVICE_HEARTBEAT_TIMEOUT_SEC",
    "SYSTEM_MODE",
    "SerializationMode",
    "SystemMode",
    "TRUST_MODE",
    "TrustMode",
    "env_int",
    "get_dataref_resolution",
    "get_dataref_upload_strategy",
    "get_dependency_policy_mode",
    "get_gateway_dataref_relay",
    "get_inline_transport_checksum",
    "get_jobqueue_resolve_refs",
    "get_local_service_payload_policy",
    "get_object_transfer_mode",
    "get_payload_policy",
    "get_runtime_limits",
    "get_serialization_mode",
    "get_system_mode",
    "get_trust_mode",
    "grpc_channel_options",
    "reload_config",
    "resolve_object_transfer_mode",
]
