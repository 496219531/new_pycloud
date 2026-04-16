from __future__ import annotations

from types import SimpleNamespace


def test_prepare_task_payload_for_submit_uses_task_submit_policy(monkeypatch) -> None:
    from pycloud_parallel.execution.support import _prepare_task_payload_for_submit

    captured = {}

    def _fake_prepare(payload, *, put_data, estimate_inline_size, policy):
        del put_data, estimate_inline_size
        captured["payload"] = dict(payload or {})
        captured["mode"] = policy.mode
        captured["consume_on_read"] = policy.consume_on_read
        return dict(payload or {})

    monkeypatch.setattr(
        "pycloud_parallel.execution.support.prepare_outbound_payload",
        _fake_prepare,
    )

    client = SimpleNamespace(target="127.0.0.1:50061")
    prepared = _prepare_task_payload_for_submit(client, {"value": 7})

    assert prepared == {"value": 7}
    assert captured["payload"] == {"value": 7}
    assert captured["mode"] == "task_submit"
    assert captured["consume_on_read"] is True
