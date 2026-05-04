from __future__ import annotations

import inspect
from dataclasses import fields

from pycloud_parallel import JobQueue, Service, TaskPool
from pycloud_parallel.execution.service_session import Service as _ServiceSession
from pycloud_parallel.execution.task_pool import _TaskPoolSessionBase


def test_public_api_signatures_do_not_expose_policy_id():
    assert "policy_id" not in inspect.signature(Service.connect).parameters
    assert "policy_id" not in inspect.signature(Service.deploy).parameters
    assert "policy_id" not in inspect.signature(TaskPool.open).parameters
    assert "nodecontrol_transport" not in inspect.signature(Service.deploy).parameters
    assert "policy_id" not in inspect.signature(JobQueue.__init__).parameters
    assert "policy_id" not in inspect.signature(JobQueue.connect).parameters


def test_low_level_controlplane_entries_still_accept_policy_id():
    assert "policy_id" in inspect.signature(Service._deploy_from_infocenter).parameters
    assert "policy_id" in inspect.signature(TaskPool._from_infocenter).parameters


def test_runtime_sessions_do_not_expose_policy_id_as_public_field():
    assert "policy_id" not in {item.name for item in fields(_ServiceSession)}
    assert not hasattr(_TaskPoolSessionBase, "policy_id")
