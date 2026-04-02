"""ID generation tests for task mode clients."""

from __future__ import annotations

import re
import threading
from unittest.mock import MagicMock, patch

from pycloud_parallel.controlplane.client import TaskBatchClient, _get_local_ip
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _mock_empty_infocenter(mock_infocenter) -> None:
    mock_infocenter_instance = MagicMock()
    mock_infocenter.return_value.__enter__.return_value = mock_infocenter_instance
    mock_infocenter_instance.select_task_nodes.return_value = []


def test_get_local_ip() -> None:
    ip = _get_local_ip()
    assert ip
    assert ip == "localhost" or "." in ip or ":" in ip


def test_auto_client_id_and_job_id_readable_and_include_ip() -> None:
    local_ip = _get_local_ip()
    with patch("pycloud_parallel.controlplane.client.InfoCenterClient") as mock_infocenter:
        _mock_empty_infocenter(mock_infocenter)
        batch = TaskBatchClient.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            code_version="sha256:test",
        )

    assert batch.client_id.startswith("client-")
    assert batch.job_id.startswith("job-")
    assert local_ip in batch.client_id
    assert local_ip in batch.job_id

    client_parts = batch.client_id.split("-")
    job_parts = batch.job_id.split("-")
    assert len(client_parts) == 6
    assert len(job_parts) == 6
    assert re.fullmatch(r"\d{13}", client_parts[2])
    assert re.fullmatch(r"\d+", client_parts[3])
    assert re.fullmatch(r"\d{4}", client_parts[4])
    assert re.fullmatch(r"[0-9a-f]{6}", client_parts[5])
    assert re.fullmatch(r"\d{13}", job_parts[2])
    assert re.fullmatch(r"\d+", job_parts[3])
    assert re.fullmatch(r"\d{4}", job_parts[4])
    assert re.fullmatch(r"[0-9a-f]{6}", job_parts[5])


def test_auto_job_id_uniqueness_under_concurrency() -> None:
    rounds_per_thread = 100
    thread_count = 8
    all_ids = set()
    lock = threading.Lock()

    def _worker() -> None:
        local_ids = [TaskBatchClient._build_auto_id(prefix="job") for _ in range(rounds_per_thread)]
        with lock:
            for item in local_ids:
                assert item not in all_ids
                all_ids.add(item)

    threads = [threading.Thread(target=_worker) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(all_ids) == rounds_per_thread * thread_count


def test_default_client_id_reused_and_job_id_still_unique() -> None:
    with patch("pycloud_parallel.controlplane.client.InfoCenterClient") as mock_infocenter:
        _mock_empty_infocenter(mock_infocenter)
        batch1 = TaskBatchClient.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            code_version="sha256:test",
        )
        batch2 = TaskBatchClient.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            code_version="sha256:test",
        )

    assert batch1.client_id == batch2.client_id
    assert batch1.job_id != batch2.job_id


def test_manual_client_id_and_job_id_keep_compatibility() -> None:
    with patch("pycloud_parallel.controlplane.client.InfoCenterClient") as mock_infocenter:
        _mock_empty_infocenter(mock_infocenter)
        batch = TaskBatchClient.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            client_id="my-custom-client",
            job_id="my-custom-job",
            code_version="sha256:test",
        )
    assert batch.client_id == "my-custom-client"
    assert batch.job_id == "my-custom-job"


def test_task_id_is_based_on_job_id_and_increments() -> None:
    batch = TaskBatchClient(
        _clients={},
        _streams={},
        client_id="client-demo",
        job_id="job-default",
        nodes={},
        code_version="sha256:test",
    )

    captured_task_ids = []

    def _fake_submit(tasks, **_kwargs):
        captured_task_ids.extend(item.task_id for item in tasks)
        accepted = [pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks]
        return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=[], node_credit=0)

    with patch.object(batch, "submit_tasks", side_effect=_fake_submit):
        batch.submit_payloads([{"x": 1}], job_id="job-fixed")
        batch.submit_payloads([{"x": 2}], job_id="job-fixed")
        batch.submit_payloads([{"x": 3}], job_id="job-fixed")

    assert captured_task_ids == [
        "job-fixed-task-0001",
        "job-fixed-task-0002",
        "job-fixed-task-0003",
    ]


def test_task_id_increment_is_thread_safe() -> None:
    batch = TaskBatchClient(
        _clients={},
        _streams={},
        client_id="client-demo",
        job_id="job-default",
        nodes={},
        code_version="sha256:test",
    )
    captured_task_ids = []
    captured_lock = threading.Lock()
    thread_count = 60

    def _fake_submit(tasks, **_kwargs):
        with captured_lock:
            captured_task_ids.extend(item.task_id for item in tasks)
        accepted = [pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks]
        return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=[], node_credit=0)

    def _worker(i: int) -> None:
        batch.submit_payloads([{"idx": i}], job_id="job-concurrent")

    with patch.object(batch, "submit_tasks", side_effect=_fake_submit):
        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert len(captured_task_ids) == thread_count
    assert len(set(captured_task_ids)) == thread_count
    suffixes = sorted(int(task_id.rsplit("-", 1)[1]) for task_id in captured_task_ids)
    assert suffixes == list(range(1, thread_count + 1))
