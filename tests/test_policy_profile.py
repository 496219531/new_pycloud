from __future__ import annotations

import pytest

from pycloud_parallel.controlplane.policy_profile import (
    PolicyProfile,
    builtin_policy_bindings,
    builtin_policy_profiles,
    get_default_mode_for_binding,
    get_default_policy_id_for_binding,
    get_policy_profile,
)


def test_builtin_policy_profiles_expose_expected_defaults():
    profiles = builtin_policy_profiles()

    assert {"default_safe", "trusted_internal", "pickle_internal_heavy"}.issubset(profiles)
    assert get_policy_profile().policy_id == "default_safe"
    assert get_policy_profile("trusted_internal").default_mode == "pickle_stable_v1"


def test_builtin_policy_bindings_expose_expected_type_defaults():
    bindings = builtin_policy_bindings()

    assert {
        "gateway_public",
        "service_internal",
        "taskpool_default",
        "taskpool_heavy_dataframe_numpy",
        "jobqueue_controlplane_transport",
    }.issubset(bindings)
    assert get_default_policy_id_for_binding("gateway_public") == "default_safe"
    assert get_default_mode_for_binding("gateway_public") == "legacy_v1"
    assert get_default_policy_id_for_binding("service_internal") == "trusted_internal"
    assert get_default_mode_for_binding("service_internal") == "pickle_stable_v1"
    assert get_default_policy_id_for_binding("taskpool_default") == "trusted_internal"
    assert get_default_mode_for_binding("taskpool_default") == "pickle_stable_v1"
    assert get_default_policy_id_for_binding("taskpool_heavy_dataframe_numpy") == "pickle_internal_heavy"
    assert get_default_mode_for_binding("taskpool_heavy_dataframe_numpy") == "pickle_stable_v1"
    assert get_default_policy_id_for_binding("jobqueue_controlplane_transport") == "default_safe"
    assert get_default_mode_for_binding("jobqueue_controlplane_transport") == "structured_v1"


def test_policy_profile_normalizes_and_rejects_invalid_default_mode():
    with pytest.raises(ValueError, match="default_mode"):
        PolicyProfile(
            policy_id="bad",
            version=1,
            allowed_modes=("legacy_v1",),
            default_mode="structured_v1",
            inline_payload_soft_limit_bytes=1,
            inline_payload_hard_limit_bytes=2,
            inline_result_hard_limit_bytes=3,
            use_raw_bytes_payload=False,
            use_http_raw_bytes_body=False,
            allow_pickle_stable=False,
            force_dataref_above_soft_limit=True,
        )
