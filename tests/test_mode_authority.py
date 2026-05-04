from __future__ import annotations

import pytest
from types import SimpleNamespace
from unittest.mock import patch

from pycloud_parallel.controlplane import config
from pycloud_parallel.api import JobQueue
from pycloud_parallel.controlplane.serialization_mode import resolve_received_transport_mode
from pycloud_parallel.data.ref import DataRef
from pycloud_parallel.execution.service_session import Service
from pycloud_parallel.execution.task_pool import TaskPool


def _fake_data_ref() -> DataRef:
    return DataRef(
        ref_id="sha256:" + ("a" * 64),
        storage_id="sha256:" + ("a" * 64),
        logical_type="json",
        format="structured_v1",
        size_bytes=12,
        materialize_as="json",
        locator_kind="controlplane",
        locator_token="127.0.0.1:50051",
    )


def test_job_queue_tracks_session_serialization_mode_and_propagates_submit():
    queue = JobQueue.connect(
        "127.0.0.1:50051",
        client_id="jobq-client",
        task_serialization_mode="structured_v1",
    )
    try:
        with (
            patch(
                "pycloud_parallel.execution.queue._stage_job_submit_payload_for_transport",
                side_effect=lambda **kwargs: dict(kwargs["payload"]),
            ),
            patch(
                "pycloud_parallel.execution.queue._prepare_job_submit_payload_for_call",
                side_effect=lambda **kwargs: dict(kwargs["payload"]),
            ),
            patch.object(queue, "_call_job_orchestrator", return_value={"job": {"job_id": "job-1"}}) as mocked_call,
        ):
            queue.submit_job({"entry_module": "job_demo", "subtasks": [{"value": 1}]})

        assert queue.serialization_mode == "structured_v1"
        assert "serialization_mode=structured_v1" in repr(queue)
        assert mocked_call.call_args.kwargs["serialization_mode"] == "structured_v1"
    finally:
        queue.close()


def test_job_queue_transport_mode_stays_structured_even_when_constructor_requests_pickle():
    queue = JobQueue.connect(
        "127.0.0.1:50051",
        client_id="jobq-client",
        task_serialization_mode="pickle_stable_v1",
    )
    try:
        assert queue.serialization_mode == "structured_v1"
        assert queue.effective_policy is not None
        assert queue.effective_policy.policy_id == "default_safe"
        assert queue.effective_policy.resolved_mode == "structured_v1"
    finally:
        queue.close()


def test_task_pool_put_data_inherits_session_serialization_mode():
    fake_pool = SimpleNamespace(
        _client=SimpleNamespace(close=lambda: None),
        close=lambda reason="": None,
    )
    session = TaskPool(
        pools={"node-1": fake_pool},
        nodes={},
        task_method="run",
        serialization_mode="structured_v1",
    )
    try:
        with patch(
            "pycloud_parallel.execution.task_pool._put_data_via_clients",
            return_value=_fake_data_ref(),
        ) as mocked_put:
            session.put_json({"value": 1})

        assert session.serialization_mode == "structured_v1"
        assert "serialization_mode=structured_v1" in repr(session)
        assert mocked_put.call_args.kwargs["serialization_mode"] == "structured_v1"
    finally:
        session.close()


def test_task_pool_defaults_to_trusted_internal_binding():
    fake_pool = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-1": fake_pool},
        nodes={},
        task_method="run",
    )
    try:
        assert session.effective_policy is not None
        assert session.effective_policy.policy_id == "trusted_internal"
        assert session.serialization_mode == "pickle_stable_v1"
    finally:
        session.close()


def test_service_put_data_inherits_session_serialization_mode():
    service = Service(
        owner_client_id="owner-demo",
        service_name="service-demo",
        sessions={},
        nodes={},
        _clients={"node-1": object()},
        serialization_mode="structured_v1",
    )
    with patch(
        "pycloud_parallel.execution.service_session._put_data_via_clients",
        return_value=_fake_data_ref(),
    ) as mocked_put:
        service.put_json({"value": 1})

    assert service.serialization_mode == "structured_v1"
    assert "serialization_mode=structured_v1" in repr(service)
    assert mocked_put.call_args.kwargs["serialization_mode"] == "structured_v1"


def test_service_defaults_to_trusted_internal_binding():
    service = Service(
        owner_client_id="owner-demo",
        service_name="service-demo",
        sessions={},
        nodes={},
        _clients={},
    )
    assert service.effective_policy is not None
    assert service.effective_policy.policy_id == "trusted_internal"
    assert service.serialization_mode == "pickle_stable_v1"


def test_gateway_public_rejects_pickle_even_when_trusted(monkeypatch):
    try:
        monkeypatch.setenv("PYCLOUD_TRUST_MODE", "trusted")
        config.reload_config()
        with pytest.raises(ValueError, match="gateway_public"):
            resolve_received_transport_mode(
                declared_mode="pickle_stable_v1",
                context="gateway_public",
            )
    finally:
        monkeypatch.delenv("PYCLOUD_TRUST_MODE", raising=False)
        config.reload_config()
