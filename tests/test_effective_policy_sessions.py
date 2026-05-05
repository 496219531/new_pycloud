from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from pycloud_parallel.api import JobQueue
from pycloud_parallel.controlplane.infocenter_client import InfoCenterServiceRoute
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def _route(*, policy_id: str = "trusted_internal") -> InfoCenterServiceRoute:
    return InfoCenterServiceRoute(
        service_name="job-orchestrator",
        service_id="job-orch-1",
        status=pb2.SERVICE_STATUS_RUNNING,
        node_instance_id="job-orch-1-inst",
        node_id="job-orch-1",
        control_addr="",
        node_healthy=True,
        worker_count=1,
        alive_workers=1,
        in_flight=0,
        lease_expire_at=datetime.now(timezone.utc),
        http_base_url="http://127.0.0.1:18080/svc/job-orch-1",
        policy_id=policy_id,
    )


def test_jobqueue_transport_policy_stays_fixed_even_when_orchestrator_route_policy_differs(monkeypatch):
    route = _route(
        policy_id="pickle_internal_heavy",
    )

    def _fake_list_service_routes(self, *, service_name="", healthy_only=True, limit=32):
        del self, healthy_only, limit
        assert service_name == "job-orchestrator"
        return [route]

    captured = {}

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.infocenter_client.InfoCenterClient.list_service_routes",
        _fake_list_service_routes,
    )

    client = JobQueue(
        "127.0.0.1:50051",
        client_id="client-refresh",
        auth_token="token-refresh",
    )
    try:
        def _capture_prepared_payload(**kwargs):
            captured["prepare_policy"] = kwargs["effective_policy"]
            return dict(kwargs["payload"])

        with (
            patch(
                "pycloud_parallel.execution.queue._stage_job_submit_payload_for_transport",
                side_effect=lambda **kwargs: dict(kwargs["payload"]),
            ),
            patch(
                "pycloud_parallel.execution.queue._prepare_job_submit_payload_for_call",
                side_effect=_capture_prepared_payload,
            ),
            patch.object(
                client,
                "_call_job_orchestrator",
                side_effect=lambda **kwargs: (
                    captured.setdefault("call_policy", kwargs.get("effective_policy")),
                    {"job": {"job_id": "job-1", "status": "WAITING"}},
                )[1],
            ),
        ):
            resp = client.submit_job({"entry_module": "job_demo", "runtime": "py3"})
    finally:
        client.close()

    assert resp["job"]["job_id"] == "job-1"
    assert captured["prepare_policy"].resolved_mode == "structured_v1"
    assert captured["prepare_policy"].policy_id == "default_safe"
    assert captured["prepare_policy"].use_raw_bytes_payload is False
    assert captured["call_policy"].resolved_mode == "structured_v1"
    assert captured["call_policy"].policy_id == "default_safe"
    assert client.effective_policy.resolved_mode == "structured_v1"
    assert client.effective_policy.policy_id == "default_safe"
