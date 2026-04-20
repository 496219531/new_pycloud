from __future__ import annotations

import pytest

from pycloud_parallel.controlplane import config
from pycloud_parallel.controlplane.serialization import detect_transport_mode
from pycloud_parallel.controlplane.serialization_mode import (
    resolve_effective_serialization_mode,
    validate_mode_for_context,
)


def test_resolve_effective_serialization_mode_priority(monkeypatch):
    try:
        monkeypatch.delenv("PYCLOUD_SERIALIZATION_MODE", raising=False)
        config.reload_config()

        assert (
            resolve_effective_serialization_mode(
                request_mode="pickle_stable_v1",
                session_mode="structured_v1",
                default_mode="legacy_v1",
            )
            == "pickle_stable_v1"
        )
        assert (
            resolve_effective_serialization_mode(
                session_mode="structured_v1",
                default_mode="legacy_v1",
            )
            == "structured_v1"
        )
        assert resolve_effective_serialization_mode(default_mode="structured_v1") == "structured_v1"

        monkeypatch.setenv("PYCLOUD_SERIALIZATION_MODE", "structured_v1")
        config.reload_config()
        assert resolve_effective_serialization_mode() == "structured_v1"
    finally:
        monkeypatch.delenv("PYCLOUD_SERIALIZATION_MODE", raising=False)
        config.reload_config()


def test_detect_transport_mode_without_envelope_defaults_to_legacy(monkeypatch):
    try:
        monkeypatch.setenv("PYCLOUD_SERIALIZATION_MODE", "structured_v1")
        config.reload_config()

        assert detect_transport_mode({"value": 1}) == "legacy_v1"
        assert detect_transport_mode({"value": 1}, default="pickle_stable_v1") == "pickle_stable_v1"
    finally:
        monkeypatch.delenv("PYCLOUD_SERIALIZATION_MODE", raising=False)
        config.reload_config()


def test_validate_mode_for_context_rejects_untrusted_gateway_pickle(monkeypatch):
    try:
        monkeypatch.setenv("PYCLOUD_TRUST_MODE", "balanced")
        config.reload_config()

        with pytest.raises(ValueError, match="pickle_stable_v1"):
            validate_mode_for_context("pickle_stable_v1", context="gateway_public")

        assert validate_mode_for_context("pickle_stable_v1", context="taskpool_session") == "pickle_stable_v1"
    finally:
        monkeypatch.delenv("PYCLOUD_TRUST_MODE", raising=False)
        config.reload_config()
