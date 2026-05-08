from __future__ import annotations

import pytest
from unittest.mock import patch

from pycloud_parallel.controlplane.client_transport import _encode_http_transport_body
from pycloud_parallel.controlplane.effective_policy import EffectivePolicy, resolve_effective_policy
from pycloud_parallel.controlplane.policy_profile import PolicyProfile, get_policy_profile
from pycloud_parallel.data.ref import DataRef
from pycloud_parallel.execution.support import _prepare_task_payload_for_submit


def _effective_policy(
    *,
    resolved_mode: str = "structured_v1",
    allowed_modes: tuple[str, ...] = ("legacy_v1", "structured_v1"),
    threshold_bytes: int = 64,
    hard_limit_bytes: int = 128,
    result_limit_bytes: int = 128,
) -> EffectivePolicy:
    return EffectivePolicy(
        policy_id="trusted_internal",
        version=1,
        resolved_mode=resolved_mode,
        allowed_modes=allowed_modes,
        inline_payload_threshold_bytes=threshold_bytes,
        inline_payload_hard_limit_bytes=hard_limit_bytes,
        inline_result_threshold_bytes=result_limit_bytes,
        inline_result_hard_limit_bytes=result_limit_bytes,
        use_raw_bytes_payload=True,
        use_http_raw_bytes_body=True,
        allow_pickle_stable="pickle_stable_v1" in allowed_modes,
    )


def _fake_data_ref() -> DataRef:
    return DataRef(
        ref_id="sha256:" + ("a" * 64),
        storage_id="sha256:" + ("a" * 64),
        logical_type="json",
        format="structured_v1",
        size_bytes=256,
        materialize_as="json",
        locator_kind="controlplane",
        locator_token="127.0.0.1:50051",
    )


def test_effective_policy_depends_only_on_profile_and_context():
    profile = get_policy_profile("trusted_internal")

    effective = resolve_effective_policy(profile, context="taskpool_session")

    assert effective.allowed_modes == ("legacy_v1", "structured_v1", "pickle_stable_v1")
    assert effective.resolved_mode == "pickle_stable_v1"
    assert effective.inline_payload_hard_limit_bytes == profile.inline_payload_hard_limit_bytes
    assert effective.use_raw_bytes_payload is True
    assert effective.use_http_raw_bytes_body is False


def test_taskpool_owner_enables_http_raw_bytes_body_for_internal_policy():
    profile = get_policy_profile("trusted_internal")

    effective = resolve_effective_policy(profile, context="taskpool_owner")

    assert effective.use_raw_bytes_payload is True
    assert effective.use_http_raw_bytes_body is True


def test_effective_policy_rejects_gateway_pickle_when_profile_disallows_it():
    profile = get_policy_profile("pickle_internal_heavy")

    with pytest.raises(ValueError, match="requested_mode"):
        resolve_effective_policy(
            profile,
            requested_mode="pickle_stable_v1",
            context="gateway_public",
        )


def test_effective_policy_respects_requested_mode_without_capability_intersection():
    profile = PolicyProfile(
        policy_id="custom",
        version=1,
        allowed_modes=("pickle_stable_v1", "structured_v1", "legacy_v1"),
        default_mode="pickle_stable_v1",
        inline_payload_threshold_bytes=16,
        inline_payload_hard_limit_bytes=32,
        inline_result_threshold_bytes=48,
        inline_result_hard_limit_bytes=48,
        use_raw_bytes_payload=True,
        use_http_raw_bytes_body=True,
        allow_pickle_stable=True,
        force_dataref_above_threshold=True,
    )

    effective = resolve_effective_policy(
        profile,
        requested_mode="structured_v1",
        context="service_connect",
    )

    assert effective.resolved_mode == "structured_v1"
    assert effective.use_raw_bytes_payload is True
    assert effective.use_http_raw_bytes_body is True


def test_effective_policy_keeps_legacy_off_bytes_lane():
    profile = PolicyProfile(
        policy_id="custom",
        version=1,
        allowed_modes=("legacy_v1", "structured_v1", "pickle_stable_v1"),
        default_mode="legacy_v1",
        inline_payload_threshold_bytes=16,
        inline_payload_hard_limit_bytes=32,
        inline_result_threshold_bytes=48,
        inline_result_hard_limit_bytes=48,
        use_raw_bytes_payload=True,
        use_http_raw_bytes_body=True,
        allow_pickle_stable=True,
        force_dataref_above_threshold=True,
    )

    effective = resolve_effective_policy(profile, context="service_connect")

    assert effective.resolved_mode == "legacy_v1"
    assert effective.use_raw_bytes_payload is False
    assert effective.use_http_raw_bytes_body is False


def test_task_submit_payload_preparation_clamps_to_effective_policy_threshold():
    effective_policy = _effective_policy(threshold_bytes=32, hard_limit_bytes=256)

    with patch(
        "pycloud_parallel.execution.support._put_data_via_clients",
        return_value=_fake_data_ref(),
    ) as mocked_put:
        prepared = _prepare_task_payload_for_submit(
            object(),
            {"value": "x" * 128},
            object_threshold_bytes=1024,
            serialization_mode="structured_v1",
            effective_policy=effective_policy,
        )

    assert isinstance(prepared["value"], DataRef)
    assert mocked_put.called


def test_http_raw_bytes_body_obeys_effective_policy_hard_limit():
    effective_policy = _effective_policy(
        resolved_mode="pickle_stable_v1",
        allowed_modes=("pickle_stable_v1", "structured_v1"),
        threshold_bytes=64,
        hard_limit_bytes=48,
    )

    with pytest.raises(ValueError, match="inline limit"):
        _encode_http_transport_body(
            {"value": "x" * 256},
            context="service_internal",
            mode="pickle_stable_v1",
            effective_policy=effective_policy,
        )
