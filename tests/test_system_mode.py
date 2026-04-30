from __future__ import annotations

from pycloud_parallel.controlplane import config
from pycloud_parallel.controlplane.system_mode import ResolvedSystemMode, resolve_system_mode


def test_resolve_system_mode_defaults_to_trusted_default(monkeypatch):
    monkeypatch.delenv("PYCLOUD_SYSTEM_MODE", raising=False)
    monkeypatch.delenv("PYCLOUD_TRUST_MODE", raising=False)
    monkeypatch.delenv("PYCLOUD_OBJECT_TRANSFER_MODE", raising=False)
    monkeypatch.delenv("PYCLOUD_SERIALIZATION_MODE", raising=False)
    monkeypatch.delenv("PYCLOUD_DEPENDENCY_POLICY_MODE", raising=False)
    config.reload_config()

    resolved = resolve_system_mode()

    assert isinstance(resolved, ResolvedSystemMode)
    assert resolved.system_mode == "trusted_default"
    assert resolved.trust_mode == "trusted"
    assert resolved.object_transfer_mode == "auto"
    assert resolved.serialization_mode == "legacy_v1"
    assert resolved.dependency_policy_mode == "prebuilt"


def test_resolve_system_mode_applies_env_and_explicit_overrides(monkeypatch):
    monkeypatch.setenv("PYCLOUD_SYSTEM_MODE", "trusted_default")
    monkeypatch.setenv("PYCLOUD_TRUST_MODE", "balanced")
    monkeypatch.setenv("PYCLOUD_OBJECT_TRANSFER_MODE", "known_digest_precheck")
    monkeypatch.setenv("PYCLOUD_SERIALIZATION_MODE", "structured_v1")
    monkeypatch.setenv("PYCLOUD_DEPENDENCY_POLICY_MODE", "node_preinstalled")
    config.reload_config()

    resolved = resolve_system_mode(serialization_mode="pickle_stable_v1")

    assert resolved.system_mode == "trusted_default"
    assert resolved.trust_mode == "balanced"
    assert resolved.object_transfer_mode == "known_digest_precheck"
    assert resolved.serialization_mode == "pickle_stable_v1"
    assert resolved.dependency_policy_mode == "node_preinstalled"


def test_resolve_system_mode_rejects_unknown_mode():
    try:
        resolve_system_mode("unknown_mode")
    except ValueError as exc:
        assert "unsupported system_mode" in str(exc)
    else:
        raise AssertionError("expected ValueError")
