from __future__ import annotations

"""Centralized runtime limits for controlplane client/server paths."""

from dataclasses import dataclass, replace
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


def _env_int_any(names: tuple[str, ...], default: int) -> int:
    for name in names:
        raw = str(os.environ.get(name, "") or "").strip()
        if raw:
            return _env_int(name, default)
    return int(default)


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


@dataclass(frozen=True)
class EnvIntSetting:
    names: tuple[str, ...]
    default: int


@dataclass(frozen=True)
class EnvBoolSetting:
    name: str
    default: bool


@dataclass(frozen=True)
class EnvChoiceSetting:
    name: str
    default: str
    choices: frozenset[str]


_INT_SETTINGS: dict[str, EnvIntSetting] = {
    "INLINE_PAYLOAD_SOFT_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES",), 512 * 1024),
    "INLINE_PAYLOAD_HARD_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_INLINE_PAYLOAD_HARD_LIMIT_BYTES",), 2 * 1024 * 1024),
    "INLINE_PAYLOAD_ESTIMATE_THRESHOLD_BYTES": EnvIntSetting(("PYCLOUD_INLINE_PAYLOAD_ESTIMATE_THRESHOLD_BYTES",), 512 * 1024),
    "INLINE_PAYLOAD_REQUEST_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_INLINE_PAYLOAD_REQUEST_LIMIT_BYTES",), 8 * 1024 * 1024),
    "LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES",), 64 * 1024 * 1024),
    "LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES",), 256 * 1024 * 1024),
    "DEFAULT_SAFE_INLINE_PAYLOAD_SOFT_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_DEFAULT_SAFE_INLINE_PAYLOAD_SOFT_LIMIT_BYTES",), 2 * 1024 * 1024),
    "DEFAULT_SAFE_INLINE_PAYLOAD_HARD_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_DEFAULT_SAFE_INLINE_PAYLOAD_HARD_LIMIT_BYTES",), 8 * 1024 * 1024),
    "DEFAULT_SAFE_INLINE_RESULT_HARD_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_DEFAULT_SAFE_INLINE_RESULT_HARD_LIMIT_BYTES",), 4 * 1024 * 1024),
    "TRUSTED_INTERNAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_TRUSTED_INTERNAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES",), 10 * 1024 * 1024),
    "TRUSTED_INTERNAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_TRUSTED_INTERNAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES",), 50 * 1024 * 1024),
    "TRUSTED_INTERNAL_INLINE_RESULT_HARD_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_TRUSTED_INTERNAL_INLINE_RESULT_HARD_LIMIT_BYTES",), 1000 * 1024 * 1024),
    "JOB_PAYLOAD_MAX_BYTES": EnvIntSetting(("PYCLOUD_JOB_PAYLOAD_MAX_BYTES",), 64 * 1024),
    "JOB_STAGING_REPLICA_COUNT": EnvIntSetting(("PYCLOUD_JOB_STAGING_REPLICA_COUNT",), 2),
    "JOB_STAGED_REF_TTL_SEC": EnvIntSetting(("PYCLOUD_JOB_STAGED_REF_TTL_SEC",), 24 * 60 * 60),
    "GATEWAY_STAGE_TTL_SEC": EnvIntSetting(("PYCLOUD_GATEWAY_STAGE_TTL_SEC",), 30 * 60),
    "GATEWAY_STAGE_GC_INTERVAL_SEC": EnvIntSetting(("PYCLOUD_GATEWAY_STAGE_GC_INTERVAL_SEC",), 60),
    "GATEWAY_MAX_UPLOAD_FILE_BYTES": EnvIntSetting(("PYCLOUD_GATEWAY_MAX_UPLOAD_FILE_BYTES",), 512 * 1024 * 1024),
    "GATEWAY_MAX_UPLOAD_TOTAL_BYTES": EnvIntSetting(("PYCLOUD_GATEWAY_MAX_UPLOAD_TOTAL_BYTES",), 1024 * 1024 * 1024),
    "INLINE_RESULT_SOFT_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_INLINE_RESULT_SOFT_LIMIT_BYTES",), 1024 * 1024),
    "INLINE_RESULT_HARD_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_INLINE_RESULT_HARD_LIMIT_BYTES",), 4 * 1024 * 1024),
    "INLINE_RESULT_ESTIMATE_THRESHOLD_BYTES": EnvIntSetting(("PYCLOUD_INLINE_RESULT_ESTIMATE_THRESHOLD_BYTES",), 1024 * 1024),
    "OBJECT_CHUNK_SIZE_BYTES": EnvIntSetting(("PYCLOUD_OBJECT_CHUNK_SIZE_BYTES",), 256 * 1024),
    "FILE_HASH_CHUNK_SIZE_BYTES": EnvIntSetting(("PYCLOUD_FILE_HASH_CHUNK_SIZE_BYTES",), 1024 * 1024),
    "OBJECT_SIZE_HARD_LIMIT_BYTES": EnvIntSetting(("PYCLOUD_OBJECT_SIZE_HARD_LIMIT_BYTES",), 1024 * 1024 * 1024),
    "BYTES_MATERIALIZE_THRESHOLD_BYTES": EnvIntSetting(("PYCLOUD_BYTES_MATERIALIZE_THRESHOLD_BYTES",), 16 * 1024 * 1024),
    "OBJECT_SEGMENT_MAX_BYTES": EnvIntSetting(("PYCLOUD_OBJECT_SEGMENT_MAX_BYTES",), 8 * 1024 * 1024),
    "OBJECT_SEGMENT_TARGET_BYTES": EnvIntSetting(("PYCLOUD_OBJECT_SEGMENT_TARGET_BYTES",), 64 * 1024 * 1024),
    "CONTROL_HTTP_MAX_SEND_BYTES": EnvIntSetting(("PYCLOUD_CONTROL_HTTP_MAX_SEND_BYTES", "PYCLOUD_CONTROL_MAX_SEND_MESSAGE_LENGTH_BYTES"), 16 * 1024 * 1024),
    "CONTROL_HTTP_MAX_RECEIVE_BYTES": EnvIntSetting(("PYCLOUD_CONTROL_HTTP_MAX_RECEIVE_BYTES", "PYCLOUD_CONTROL_MAX_RECEIVE_MESSAGE_LENGTH_BYTES"), 16 * 1024 * 1024),
    "SERVICE_HTTP_BODY_MAX_BYTES": EnvIntSetting(("PYCLOUD_SERVICE_HTTP_BODY_MAX_BYTES", "PYCLOUD_HTTP_SERVICE_BODY_MAX_BYTES"), 64 * 1024 * 1024),
    "GATEWAY_HTTP_BODY_MAX_BYTES": EnvIntSetting(("PYCLOUD_GATEWAY_HTTP_BODY_MAX_BYTES", "PYCLOUD_HTTP_GATEWAY_BODY_MAX_BYTES"), 64 * 1024 * 1024),
    "INFOCENTER_HTTP_BODY_MAX_BYTES": EnvIntSetting(("PYCLOUD_INFOCENTER_HTTP_BODY_MAX_BYTES", "PYCLOUD_HTTP_INFOCENTER_BODY_MAX_BYTES"), 64 * 1024 * 1024),
    "NODE_CONTROL_HTTP_BODY_MAX_BYTES": EnvIntSetting(("PYCLOUD_NODE_CONTROL_HTTP_BODY_MAX_BYTES", "PYCLOUD_NODECONTROL_HTTP_BODY_MAX_BYTES", "PYCLOUD_HTTP_NODECONTROL_BODY_MAX_BYTES"), 256 * 1024 * 1024),
    "OBJECT_HTTP_BODY_MAX_BYTES": EnvIntSetting(("PYCLOUD_OBJECT_HTTP_BODY_MAX_BYTES", "PYCLOUD_HTTP_OBJECT_BODY_MAX_BYTES"), 512 * 1024 * 1024),
    "NODE_WORKER_CAPACITY": EnvIntSetting(("PYCLOUD_NODE_WORKER_CAPACITY",), 32),
    "NODE_QUEUE_CAPACITY": EnvIntSetting(("PYCLOUD_NODE_QUEUE_CAPACITY",), 4000),
    "NODE_MAX_WORKERS": EnvIntSetting(("PYCLOUD_NODE_MAX_WORKERS",), 64),
    "SERVICE_DEFAULT_WORKERS": EnvIntSetting(("PYCLOUD_SERVICE_DEFAULT_WORKERS",), 10),
    "SERVICE_HEARTBEAT_TIMEOUT_SEC": EnvIntSetting(("PYCLOUD_SERVICE_HEARTBEAT_TIMEOUT_SEC",), 30),
}

_BOOL_SETTINGS: dict[str, EnvBoolSetting] = {
    "OBJECT_UPLOAD_TRUSTED_PRECHECK": EnvBoolSetting("PYCLOUD_OBJECT_UPLOAD_TRUSTED_PRECHECK", True),
    "INLINE_TRANSPORT_CHECKSUM": EnvBoolSetting(PYCLOUD_INLINE_TRANSPORT_CHECKSUM, False),
}

_CHOICE_SETTINGS: dict[str, EnvChoiceSetting] = {
    "SYSTEM_MODE": EnvChoiceSetting(PYCLOUD_SYSTEM_MODE, "trusted_default", frozenset({"trusted_default"})),
    "TRUST_MODE": EnvChoiceSetting(PYCLOUD_TRUST_MODE, "trusted", frozenset({"trusted", "balanced", "strict"})),
    "OBJECT_TRANSFER_MODE": EnvChoiceSetting(PYCLOUD_OBJECT_TRANSFER_MODE, "auto", frozenset({"auto", "known_digest_precheck", "single_pass_authoritative"})),
    "SERIALIZATION_MODE": EnvChoiceSetting(PYCLOUD_SERIALIZATION_MODE, "legacy_v1", frozenset({"legacy_v1", "structured_v1", "pickle_stable_v1"})),
    "DEPENDENCY_POLICY_MODE": EnvChoiceSetting(PYCLOUD_DEPENDENCY_POLICY_MODE, "prebuilt", frozenset({"prebuilt", "node_preinstalled", "allow_install"})),
    "EXECUTOR_BACKEND": EnvChoiceSetting(PYCLOUD_EXECUTOR_BACKEND, "subprocess_host", frozenset({"subprocess_host"})),
    "DATAREF_RESOLUTION": EnvChoiceSetting(PYCLOUD_DATAREF_RESOLUTION, "remote_fetch", frozenset({"local_only", "remote_fetch"})),
    "DATAREF_UPLOAD_STRATEGY": EnvChoiceSetting(PYCLOUD_DATAREF_UPLOAD_STRATEGY, "upload_once", frozenset({"fanout", "upload_once"})),
    "GATEWAY_DATAREF_RELAY": EnvChoiceSetting(PYCLOUD_GATEWAY_DATAREF_RELAY, "eager", frozenset({"eager", "lazy"})),
    "JOBQUEUE_RESOLVE_REFS": EnvChoiceSetting(PYCLOUD_JOBQUEUE_RESOLVE_REFS, "defer_to_worker", frozenset({"eager", "defer_to_worker"})),
}


def load_config_from_env() -> dict[str, object]:
    values: dict[str, object] = {}
    for key, setting in _INT_SETTINGS.items():
        values[key] = _env_int_any(setting.names, setting.default)
    for key, setting in _BOOL_SETTINGS.items():
        values[key] = _env_bool(setting.name, setting.default)
    for key, setting in _CHOICE_SETTINGS.items():
        values[key] = _env_choice(setting.name, setting.default, set(setting.choices))
    return values


globals().update(load_config_from_env())


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
    inline_payload_estimate_threshold_bytes: int = 0
    inline_result_estimate_threshold_bytes: int = 0


@dataclass(frozen=True)
class PolicyThresholdLimits:
    inline_payload_soft_limit_bytes: int
    inline_payload_hard_limit_bytes: int
    inline_result_hard_limit_bytes: int


@dataclass(frozen=True)
class PolicyThresholds:
    default_safe: PolicyThresholdLimits
    trusted_internal: PolicyThresholdLimits


@dataclass(frozen=True)
class TransportBounds:
    control_http_max_send_bytes: int
    control_http_max_receive_bytes: int
    service_http_body_max_bytes: int
    gateway_http_body_max_bytes: int
    infocenter_http_body_max_bytes: int
    node_control_http_body_max_bytes: int
    object_http_body_max_bytes: int


@dataclass(frozen=True)
class ObjectStoreBounds:
    object_chunk_size_bytes: int
    file_hash_chunk_size_bytes: int
    object_size_hard_limit_bytes: int
    bytes_materialize_threshold_bytes: int
    object_segment_max_bytes: int
    object_segment_target_bytes: int
    gateway_max_upload_file_bytes: int
    gateway_max_upload_total_bytes: int
    object_upload_trusted_precheck: bool


@dataclass(frozen=True)
class JobStagingBounds:
    job_payload_max_bytes: int
    job_staging_replica_count: int
    job_staged_ref_ttl_sec: int
    gateway_stage_ttl_sec: int
    gateway_stage_gc_interval_sec: int


@dataclass(frozen=True)
class CapacityDefaults:
    node_worker_capacity: int
    node_queue_capacity: int
    node_max_workers: int
    service_default_workers: int
    service_heartbeat_timeout_sec: int


@dataclass(frozen=True)
class ConfigLimitAuthority:
    runtime_payload: PayloadLimits
    local_inline_payload: tuple[int, int]
    policy_thresholds: PolicyThresholds
    transport_bounds: TransportBounds
    object_store_bounds: ObjectStoreBounds
    job_staging_bounds: JobStagingBounds
    capacity_defaults: CapacityDefaults


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
    def inline_payload_estimate_threshold_bytes(self) -> int:
        threshold = int(getattr(self.limits, "inline_payload_estimate_threshold_bytes", 0) or 0)
        if threshold <= 0:
            threshold = int(self.inline_payload_soft_limit_bytes)
        return max(1, min(threshold, int(self.inline_payload_hard_limit_bytes)))

    @property
    def inline_result_soft_limit_bytes(self) -> int:
        return int(self.limits.inline_result_soft_limit_bytes)

    @property
    def inline_result_hard_limit_bytes(self) -> int:
        return int(self.limits.inline_result_hard_limit_bytes)

    @property
    def inline_result_estimate_threshold_bytes(self) -> int:
        threshold = int(getattr(self.limits, "inline_result_estimate_threshold_bytes", 0) or 0)
        if threshold <= 0:
            threshold = int(self.inline_result_soft_limit_bytes)
        return max(1, min(threshold, int(self.inline_result_hard_limit_bytes)))

    @property
    def object_chunk_size_bytes(self) -> int:
        return int(self.limits.object_chunk_size_bytes)

    @property
    def file_hash_chunk_size_bytes(self) -> int:
        return int(self.limits.file_hash_chunk_size_bytes)


def get_runtime_limits() -> PayloadLimits:
    payload_hard = max(1, int(INLINE_PAYLOAD_HARD_LIMIT_BYTES))
    result_hard = max(1, int(INLINE_RESULT_HARD_LIMIT_BYTES))
    return PayloadLimits(
        inline_payload_soft_limit_bytes=min(max(1, int(INLINE_PAYLOAD_SOFT_LIMIT_BYTES)), payload_hard),
        inline_payload_hard_limit_bytes=payload_hard,
        inline_payload_request_limit_bytes=max(1, int(INLINE_PAYLOAD_REQUEST_LIMIT_BYTES)),
        inline_result_soft_limit_bytes=min(max(1, int(INLINE_RESULT_SOFT_LIMIT_BYTES)), result_hard),
        inline_result_hard_limit_bytes=result_hard,
        object_chunk_size_bytes=int(OBJECT_CHUNK_SIZE_BYTES),
        file_hash_chunk_size_bytes=int(FILE_HASH_CHUNK_SIZE_BYTES),
        inline_payload_estimate_threshold_bytes=min(
            max(1, int(INLINE_PAYLOAD_ESTIMATE_THRESHOLD_BYTES)),
            payload_hard,
        ),
        inline_result_estimate_threshold_bytes=min(
            max(1, int(INLINE_RESULT_ESTIMATE_THRESHOLD_BYTES)),
            result_hard,
        ),
    )


def get_payload_estimate_threshold_bytes(value: int = 0) -> int:
    limits = get_runtime_limits()
    return max(
        1,
        min(
            int(value or limits.inline_payload_estimate_threshold_bytes),
            int(limits.inline_payload_hard_limit_bytes),
        ),
    )


def get_result_estimate_threshold_bytes(value: int = 0) -> int:
    limits = get_runtime_limits()
    return max(
        1,
        min(
            int(value or limits.inline_result_estimate_threshold_bytes),
            int(limits.inline_result_hard_limit_bytes),
        ),
    )


def get_config_limit_authority() -> ConfigLimitAuthority:
    return ConfigLimitAuthority(
        runtime_payload=get_runtime_limits(),
        local_inline_payload=get_local_inline_limits(),
        policy_thresholds=PolicyThresholds(
            default_safe=PolicyThresholdLimits(
                inline_payload_soft_limit_bytes=int(DEFAULT_SAFE_INLINE_PAYLOAD_SOFT_LIMIT_BYTES),
                inline_payload_hard_limit_bytes=int(DEFAULT_SAFE_INLINE_PAYLOAD_HARD_LIMIT_BYTES),
                inline_result_hard_limit_bytes=int(DEFAULT_SAFE_INLINE_RESULT_HARD_LIMIT_BYTES),
            ),
            trusted_internal=PolicyThresholdLimits(
                inline_payload_soft_limit_bytes=int(TRUSTED_INTERNAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES),
                inline_payload_hard_limit_bytes=int(TRUSTED_INTERNAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES),
                inline_result_hard_limit_bytes=int(TRUSTED_INTERNAL_INLINE_RESULT_HARD_LIMIT_BYTES),
            ),
        ),
        transport_bounds=TransportBounds(
            control_http_max_send_bytes=int(CONTROL_HTTP_MAX_SEND_BYTES),
            control_http_max_receive_bytes=int(CONTROL_HTTP_MAX_RECEIVE_BYTES),
            service_http_body_max_bytes=int(SERVICE_HTTP_BODY_MAX_BYTES),
            gateway_http_body_max_bytes=int(GATEWAY_HTTP_BODY_MAX_BYTES),
            infocenter_http_body_max_bytes=int(INFOCENTER_HTTP_BODY_MAX_BYTES),
            node_control_http_body_max_bytes=int(NODE_CONTROL_HTTP_BODY_MAX_BYTES),
            object_http_body_max_bytes=int(OBJECT_HTTP_BODY_MAX_BYTES),
        ),
        object_store_bounds=ObjectStoreBounds(
            object_chunk_size_bytes=int(OBJECT_CHUNK_SIZE_BYTES),
            file_hash_chunk_size_bytes=int(FILE_HASH_CHUNK_SIZE_BYTES),
            object_size_hard_limit_bytes=int(OBJECT_SIZE_HARD_LIMIT_BYTES),
            bytes_materialize_threshold_bytes=int(BYTES_MATERIALIZE_THRESHOLD_BYTES),
            object_segment_max_bytes=int(OBJECT_SEGMENT_MAX_BYTES),
            object_segment_target_bytes=int(OBJECT_SEGMENT_TARGET_BYTES),
            gateway_max_upload_file_bytes=int(GATEWAY_MAX_UPLOAD_FILE_BYTES),
            gateway_max_upload_total_bytes=int(GATEWAY_MAX_UPLOAD_TOTAL_BYTES),
            object_upload_trusted_precheck=bool(OBJECT_UPLOAD_TRUSTED_PRECHECK),
        ),
        job_staging_bounds=JobStagingBounds(
            job_payload_max_bytes=int(JOB_PAYLOAD_MAX_BYTES),
            job_staging_replica_count=int(JOB_STAGING_REPLICA_COUNT),
            job_staged_ref_ttl_sec=int(JOB_STAGED_REF_TTL_SEC),
            gateway_stage_ttl_sec=int(GATEWAY_STAGE_TTL_SEC),
            gateway_stage_gc_interval_sec=int(GATEWAY_STAGE_GC_INTERVAL_SEC),
        ),
        capacity_defaults=CapacityDefaults(
            node_worker_capacity=int(NODE_WORKER_CAPACITY),
            node_queue_capacity=int(NODE_QUEUE_CAPACITY),
            node_max_workers=int(NODE_MAX_WORKERS),
            service_default_workers=int(SERVICE_DEFAULT_WORKERS),
            service_heartbeat_timeout_sec=int(SERVICE_HEARTBEAT_TIMEOUT_SEC),
        ),
    )


def get_policy_limit_defaults(policy_id: str) -> tuple[int, int, int]:
    normalized = str(policy_id or "").strip().lower()
    if normalized == "default_safe":
        return (
            int(DEFAULT_SAFE_INLINE_PAYLOAD_SOFT_LIMIT_BYTES),
            int(DEFAULT_SAFE_INLINE_PAYLOAD_HARD_LIMIT_BYTES),
            int(DEFAULT_SAFE_INLINE_RESULT_HARD_LIMIT_BYTES),
        )
    if normalized in {"trusted_internal", "pickle_internal_heavy"}:
        return (
            int(TRUSTED_INTERNAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES),
            int(TRUSTED_INTERNAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES),
            int(TRUSTED_INTERNAL_INLINE_RESULT_HARD_LIMIT_BYTES),
        )
    return (
        int(INLINE_PAYLOAD_SOFT_LIMIT_BYTES),
        int(INLINE_PAYLOAD_HARD_LIMIT_BYTES),
        int(INLINE_RESULT_HARD_LIMIT_BYTES),
    )


def get_binding_payload_thresholds(
    binding_id: str,
    *,
    requested_mode: str = "",
    context: str = "",
) -> tuple[int, int, int]:
    from pycloud_parallel.controlplane.effective_policy import resolve_effective_policy
    from pycloud_parallel.controlplane.policy_profile import (
        get_default_mode_for_binding,
        get_default_policy_id_for_binding,
        get_policy_profile,
    )

    normalized_binding = str(binding_id or "").strip().lower()
    if not normalized_binding:
        raise ValueError("binding_id is required")
    effective = resolve_effective_policy(
        get_policy_profile(get_default_policy_id_for_binding(normalized_binding)),
        requested_mode=str(requested_mode or "").strip() or get_default_mode_for_binding(normalized_binding),
        context=context,
    )
    return (
        int(effective.inline_payload_soft_limit_bytes or 1),
        int(effective.inline_payload_hard_limit_bytes or 1),
        int(effective.inline_result_hard_limit_bytes or 1),
    )


def get_local_inline_limits() -> tuple[int, int]:
    local_hard = max(1, int(LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES))
    local_soft = min(max(1, int(LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES)), local_hard)
    return local_soft, local_hard


def get_job_blob_inline_threshold_bytes() -> int:
    return max(256 * 1024, int(INLINE_PAYLOAD_HARD_LIMIT_BYTES / 1.5))


def get_job_staging_replica_count(value: int = 0) -> int:
    return max(1, int(value or JOB_STAGING_REPLICA_COUNT))


def get_job_staged_ref_ttl_sec(value: int = 0) -> int:
    return max(1, int(value or JOB_STAGED_REF_TTL_SEC))


def merge_object_threshold_with_policy_soft_limit(*, object_threshold_bytes: int, policy_soft_limit_bytes: int) -> int:
    threshold = max(1, int(object_threshold_bytes or 1))
    soft_limit = max(1, int(policy_soft_limit_bytes or 1))
    return min(threshold, soft_limit)


def policy_with_soft_limit(policy: PayloadPolicy, object_threshold_bytes: int) -> PayloadPolicy:
    threshold = max(1, int(object_threshold_bytes or 1))
    if (
        int(threshold) == int(policy.inline_payload_soft_limit_bytes)
        and int(policy.inline_payload_estimate_threshold_bytes) <= int(threshold)
    ):
        return policy
    return replace(
        policy,
        limits=replace(
            policy.limits,
            inline_payload_soft_limit_bytes=threshold,
            inline_payload_estimate_threshold_bytes=min(
                threshold,
                int(policy.inline_payload_estimate_threshold_bytes),
            ),
        ),
    )


def resolve_payload_policy(
    mode: PayloadMode,
    *,
    effective_policy: object = None,
    object_threshold_bytes: int = 0,
) -> PayloadPolicy:
    policy = get_payload_policy(mode)
    if effective_policy is not None:
        policy = replace(policy, limits=merge_payload_limits_with_effective_policy(policy.limits, effective_policy))
    if int(object_threshold_bytes or 0) <= 0:
        return policy
    threshold = merge_object_threshold_with_policy_soft_limit(
        object_threshold_bytes=object_threshold_bytes,
        policy_soft_limit_bytes=policy.inline_payload_soft_limit_bytes,
    )
    return policy_with_soft_limit(policy, threshold)


def get_node_control_http_body_limit_bytes(node_control_body_bytes: int = 0) -> int:
    control_limit = int(node_control_body_bytes or NODE_CONTROL_HTTP_BODY_MAX_BYTES)
    return max(1, control_limit, int(OBJECT_HTTP_BODY_MAX_BYTES))


def get_http_object_body_limit_bytes(object_body_bytes: int = 0) -> int:
    return max(1, int(object_body_bytes or OBJECT_HTTP_BODY_MAX_BYTES))


def get_transport_bounds() -> TransportBounds:
    return get_config_limit_authority().transport_bounds


def get_object_store_bounds() -> ObjectStoreBounds:
    return get_config_limit_authority().object_store_bounds


def get_object_size_hard_limit_bytes(value: int = 0) -> int:
    return max(1, int(value or get_object_store_bounds().object_size_hard_limit_bytes))


def get_bytes_materialize_threshold_bytes(value: int = 0) -> int:
    bounds = get_object_store_bounds()
    return max(
        1,
        min(
            int(value or bounds.bytes_materialize_threshold_bytes),
            int(bounds.object_size_hard_limit_bytes),
        ),
    )


def validate_bytes_materialize_size(size_bytes: int, *, context: str = "object") -> None:
    size = max(0, int(size_bytes or 0))
    limit = get_bytes_materialize_threshold_bytes()
    if size > limit:
        raise ValueError(
            f"{context} is too large for in-memory bytes materialize: "
            f"size_bytes={size} limit_bytes={limit}; use file/path download instead"
        )


def validate_object_size_bytes(size_bytes: int, *, context: str = "object") -> None:
    size = max(0, int(size_bytes or 0))
    limit = get_object_size_hard_limit_bytes()
    if size > limit:
        raise ValueError(f"{context} exceeds object size hard limit: size_bytes={size} limit_bytes={limit}")


def get_service_http_body_limit_bytes(value: int = 0) -> int:
    return max(1, int(value or get_transport_bounds().service_http_body_max_bytes))


def get_gateway_http_body_limit_bytes(value: int = 0) -> int:
    return max(1, int(value or get_transport_bounds().gateway_http_body_max_bytes))


def get_infocenter_http_body_limit_bytes(value: int = 0) -> int:
    return max(1, int(value or get_transport_bounds().infocenter_http_body_max_bytes))


def get_gateway_upload_limits(*, max_file_bytes: int = 0, max_total_bytes: int = 0) -> tuple[int, int]:
    bounds = get_object_store_bounds()
    file_limit = max(1, int(max_file_bytes or bounds.gateway_max_upload_file_bytes))
    total_limit = max(file_limit, int(max_total_bytes or bounds.gateway_max_upload_total_bytes))
    return file_limit, total_limit


def get_managed_globals_control_limit_bytes(*, policy_hard_limit_bytes: int, control_send_bytes: int = 0) -> int:
    return max(
        1,
        min(
            max(1, int(policy_hard_limit_bytes or 1)),
            max(1, int(control_send_bytes or CONTROL_HTTP_MAX_SEND_BYTES)),
        ),
    )


def normalize_policy_limit_values(*, soft: int, hard: int, result_hard: int) -> tuple[int, int, int]:
    hard_value = max(1, int(hard or 1))
    return (
        min(max(1, int(soft or 1)), hard_value),
        hard_value,
        max(1, int(result_hard or 1)),
    )


def effective_limits_from_profile(profile: object) -> tuple[int, int, int]:
    return normalize_policy_limit_values(
        soft=int(getattr(profile, "inline_payload_soft_limit_bytes", 1) or 1),
        hard=int(getattr(profile, "inline_payload_hard_limit_bytes", 1) or 1),
        result_hard=int(getattr(profile, "inline_result_hard_limit_bytes", 1) or 1),
    )


def merge_payload_limits_with_effective_policy(base_limits: PayloadLimits, effective_policy: object) -> PayloadLimits:
    soft, hard, result_hard = normalize_policy_limit_values(
        soft=min(
            int(base_limits.inline_payload_soft_limit_bytes),
            int(getattr(effective_policy, "inline_payload_soft_limit_bytes", 1) or 1),
        ),
        hard=min(
            int(base_limits.inline_payload_hard_limit_bytes),
            int(getattr(effective_policy, "inline_payload_hard_limit_bytes", 1) or 1),
        ),
        result_hard=min(
            int(base_limits.inline_result_hard_limit_bytes),
            int(getattr(effective_policy, "inline_result_hard_limit_bytes", 1) or 1),
        ),
    )
    return PayloadLimits(
        inline_payload_soft_limit_bytes=soft,
        inline_payload_hard_limit_bytes=hard,
        inline_payload_request_limit_bytes=min(int(base_limits.inline_payload_request_limit_bytes), hard),
        inline_result_soft_limit_bytes=min(int(base_limits.inline_result_soft_limit_bytes), result_hard),
        inline_result_hard_limit_bytes=result_hard,
        object_chunk_size_bytes=int(base_limits.object_chunk_size_bytes),
        file_hash_chunk_size_bytes=int(base_limits.file_hash_chunk_size_bytes),
        inline_payload_estimate_threshold_bytes=min(
            int(getattr(base_limits, "inline_payload_estimate_threshold_bytes", 0) or soft),
            soft,
            hard,
        ),
        inline_result_estimate_threshold_bytes=min(
            int(getattr(base_limits, "inline_result_estimate_threshold_bytes", 0) or result_hard),
            result_hard,
        ),
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
    local_soft, local_hard = get_local_inline_limits()
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
            inline_payload_estimate_threshold_bytes=local_soft,
            inline_result_estimate_threshold_bytes=limits.inline_result_estimate_threshold_bytes,
        ),
        recurse_containers=True,
        consume_on_read=True,
        preserve_args_kwargs_container=True,
    )


def reload_config() -> None:
    """Reload environment-backed limits for tests or dynamic config."""
    globals().update(load_config_from_env())


def control_http_limits() -> list[tuple[str, int]]:
    return [
        ("http.max_send_bytes", int(CONTROL_HTTP_MAX_SEND_BYTES)),
        ("http.max_receive_bytes", int(CONTROL_HTTP_MAX_RECEIVE_BYTES)),
    ]


STABLE_CONFIG_API_EXPORTS = [
    "CapacityDefaults",
    "ConfigLimitAuthority",
    "ObjectStoreBounds",
    "PayloadLimits",
    "PayloadMode",
    "PayloadPolicy",
    "PolicyThresholdLimits",
    "PolicyThresholds",
    "TransportBounds",
    "control_http_limits",
    "effective_limits_from_profile",
    "env_int",
    "get_config_limit_authority",
    "get_bytes_materialize_threshold_bytes",
    "get_dataref_resolution",
    "get_dataref_upload_strategy",
    "get_dependency_policy_mode",
    "get_gateway_dataref_relay",
    "get_gateway_http_body_limit_bytes",
    "get_gateway_upload_limits",
    "get_http_object_body_limit_bytes",
    "get_infocenter_http_body_limit_bytes",
    "get_inline_transport_checksum",
    "get_job_blob_inline_threshold_bytes",
    "get_job_staged_ref_ttl_sec",
    "get_job_staging_replica_count",
    "get_jobqueue_resolve_refs",
    "get_local_inline_limits",
    "get_local_service_payload_policy",
    "get_managed_globals_control_limit_bytes",
    "get_node_control_http_body_limit_bytes",
    "get_object_store_bounds",
    "get_object_size_hard_limit_bytes",
    "get_object_transfer_mode",
    "get_payload_policy",
    "get_result_estimate_threshold_bytes",
    "get_policy_limit_defaults",
    "get_payload_estimate_threshold_bytes",
    "get_runtime_limits",
    "get_serialization_mode",
    "get_service_http_body_limit_bytes",
    "get_system_mode",
    "get_transport_bounds",
    "get_trust_mode",
    "load_config_from_env",
    "merge_object_threshold_with_policy_soft_limit",
    "merge_payload_limits_with_effective_policy",
    "normalize_policy_limit_values",
    "policy_with_soft_limit",
    "reload_config",
    "resolve_object_transfer_mode",
    "resolve_payload_policy",
    "validate_object_size_bytes",
    "validate_bytes_materialize_size",
]


COMPATIBILITY_CONFIG_EXPORTS = [
    "DEPENDENCY_POLICY_MODE",
    "BYTES_MATERIALIZE_THRESHOLD_BYTES",
    "DEFAULT_SAFE_INLINE_PAYLOAD_HARD_LIMIT_BYTES",
    "DEFAULT_SAFE_INLINE_PAYLOAD_SOFT_LIMIT_BYTES",
    "DEFAULT_SAFE_INLINE_RESULT_HARD_LIMIT_BYTES",
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
    "CONTROL_HTTP_MAX_RECEIVE_BYTES",
    "CONTROL_HTTP_MAX_SEND_BYTES",
    "GatewayDataRefRelayMode",
    "GATEWAY_HTTP_BODY_MAX_BYTES",
    "INFOCENTER_HTTP_BODY_MAX_BYTES",
    "NODE_CONTROL_HTTP_BODY_MAX_BYTES",
    "OBJECT_HTTP_BODY_MAX_BYTES",
    "SERVICE_HTTP_BODY_MAX_BYTES",
    "INLINE_PAYLOAD_HARD_LIMIT_BYTES",
    "INLINE_PAYLOAD_ESTIMATE_THRESHOLD_BYTES",
    "INLINE_PAYLOAD_REQUEST_LIMIT_BYTES",
    "INLINE_PAYLOAD_SOFT_LIMIT_BYTES",
    "INLINE_RESULT_HARD_LIMIT_BYTES",
    "INLINE_RESULT_ESTIMATE_THRESHOLD_BYTES",
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
    "OBJECT_SIZE_HARD_LIMIT_BYTES",
    "OBJECT_SEGMENT_MAX_BYTES",
    "OBJECT_SEGMENT_TARGET_BYTES",
    "OBJECT_TRANSFER_MODE",
    "OBJECT_UPLOAD_TRUSTED_PRECHECK",
    "ObjectTransferMode",
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
    "TRUSTED_INTERNAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES",
    "TRUSTED_INTERNAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES",
    "TRUSTED_INTERNAL_INLINE_RESULT_HARD_LIMIT_BYTES",
    "TrustMode",
]


# New code should prefer STABLE_CONFIG_API_EXPORTS. The compatibility exports
# remain importable for external users and older internal call sites.
__all__ = sorted(set(STABLE_CONFIG_API_EXPORTS + COMPATIBILITY_CONFIG_EXPORTS))
