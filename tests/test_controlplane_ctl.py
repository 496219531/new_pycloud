from __future__ import annotations

from pycloud_parallel.controlplane import ctl


def test_default_node_worker_capacity_is_positive(monkeypatch):
    monkeypatch.setattr(ctl.os, "cpu_count", lambda: 8)
    assert ctl._default_node_worker_capacity() == 4


def test_default_node_worker_capacity_handles_single_cpu(monkeypatch):
    monkeypatch.setattr(ctl.os, "cpu_count", lambda: 1)
    assert ctl._default_node_worker_capacity() == 1


def test_ctl_parser_accepts_start_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["start"])
    assert args.command == "start"
    assert args.controlplane_port == 50051
    assert args.node_worker_capacity == 0
