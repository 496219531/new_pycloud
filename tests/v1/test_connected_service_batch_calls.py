from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

import pytest

from pycloud_parallel.api import Service


def test_connected_service_map_preserves_input_order_for_discovery():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="discovery",
        validate_on_init=False,
    )
    try:
        client._discovered_methods = ["square"]
        with patch.object(
            type(client),
            "call_balanced",
            side_effect=lambda method, payload, **kwargs: (
                "node-1",
                {"ok": True, "data": {"value": payload["x"] * payload["x"]}},
            ),
        ):
            results = client.square.map([1, 2, 3], arg_name="x")
        assert results == [{"value": 1}, {"value": 4}, {"value": 9}]
    finally:
        client.close()


def test_connected_service_map_returns_none_on_failure():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="discovery",
        validate_on_init=False,
    )

    def _call(method, payload, **kwargs):
        x = int(payload["x"])
        if x == 2:
            raise RuntimeError("boom-2")
        return ("node-1", {"ok": True, "data": {"value": x * 10}})

    try:
        client._discovered_methods = ["square"]
        with patch.object(type(client), "call_balanced", side_effect=_call):
            results = client.square.map([1, 2, 3], arg_name="x")
        assert results == [{"value": 10}, None, {"value": 30}]
    finally:
        client.close()


def test_connected_service_amap_preserves_input_order_for_gateway():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="gateway",
        validate_on_init=False,
    )

    async def _call(method, payload, **kwargs):
        x = int(payload["x"])
        await asyncio.sleep({1: 0.05, 2: 0.01, 3: 0.03}[x])
        return ("gateway", {"ok": True, "data": {"value": x + 10}})

    async def _collect():
        return await client.square.amap([1, 2, 3], arg_name="x")

    try:
        client._discovered_methods = ["square"]
        with patch.object(type(client), "acall_balanced", side_effect=_call):
            results = asyncio.run(_collect())
        assert results == [{"value": 11}, {"value": 12}, {"value": 13}]
    finally:
        client.close()


def test_connected_service_unordered_sync_yields_index_and_result_or_none():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="discovery",
        validate_on_init=False,
    )

    def _call(method, payload, **kwargs):
        x = int(payload["x"])
        time.sleep({1: 0.06, 2: 0.01, 3: 0.03}[x])
        if x == 2:
            raise RuntimeError("boom-2")
        return ("node-1", {"ok": True, "data": {"value": x * 10}})

    try:
        client._discovered_methods = ["square"]
        with patch.object(type(client), "call_balanced", side_effect=_call):
            items = list(client.square.unordered([{"x": 1}, {"x": 2}, {"x": 3}], max_in_flight=3))
        assert len(items) == 3
        assert [idx for idx, _result in items] != [0, 1, 2]
        assert sorted(items) == [
            (0, {"value": 10}),
            (1, None),
            (2, {"value": 30}),
        ]
    finally:
        client.close()


def test_connected_service_aunordered_supports_gateway_transport():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="gateway",
        validate_on_init=False,
    )

    async def _call(method, payload, **kwargs):
        x = int(payload["x"])
        await asyncio.sleep({1: 0.05, 2: 0.01, 3: 0.03}[x])
        return ("gateway", {"ok": True, "data": {"value": x + 100}})

    async def _collect():
        items = []
        async for item in client.square.aunordered([{"x": 1}, {"x": 2}, {"x": 3}], max_in_flight=3):
            items.append(item)
        return items

    try:
        client._discovered_methods = ["square"]
        with patch.object(type(client), "acall_balanced", side_effect=_call):
            items = asyncio.run(_collect())
        assert len(items) == 3
        assert [idx for idx, _result in items] != [0, 1, 2]
        assert sorted(items) == [
            (0, {"value": 101}),
            (1, {"value": 102}),
            (2, {"value": 103}),
        ]
    finally:
        client.close()


def test_connected_service_iter_items_exposes_full_execution_items():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="discovery",
        validate_on_init=False,
    )

    def _call(method, payload, **kwargs):
        x = int(payload["x"])
        if x == 2:
            raise RuntimeError("boom-2")
        return ("node-1", {"ok": True, "data": {"value": x}})

    try:
        client._discovered_methods = ["square"]
        with patch.object(type(client), "call_balanced", side_effect=_call):
            items = list(client.square.iter_items([{"x": 1}, {"x": 2}]))
        assert len(items) == 2
        ordered = sorted(items, key=lambda item: item.index)
        assert ordered[0].ok is True
        assert ordered[0].result == {"value": 1}
        assert ordered[0].key == 0
        assert ordered[1].ok is False
        assert ordered[1].result is None
        assert ordered[1].error_type == "RuntimeError"
        assert ordered[1].error_message == "boom-2"
    finally:
        client.close()


def test_connected_service_collect_items_returns_input_order():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="gateway",
        validate_on_init=False,
    )

    async def _call(method, payload, **kwargs):
        x = int(payload["x"])
        await asyncio.sleep({1: 0.05, 2: 0.01, 3: 0.03}[x])
        return ("gateway", {"ok": True, "data": {"value": x}})

    try:
        client._discovered_methods = ["square"]
        with patch.object(type(client), "acall_balanced", side_effect=_call):
            items = asyncio.run(client.square.acollect_items([{"x": 1}, {"x": 2}, {"x": 3}]))
        assert [item.index for item in items] == [0, 1, 2]
        assert [item.result for item in items] == [{"value": 1}, {"value": 2}, {"value": 3}]
    finally:
        client.close()


def test_connected_service_map_requires_arg_name_for_non_dict_values():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="discovery",
        validate_on_init=False,
    )
    try:
        client._discovered_methods = ["square"]
        with pytest.raises(TypeError, match="arg_name"):
            client.square.map([1, 2, 3], arg_name="")
    finally:
        client.close()


def test_connected_service_unordered_requires_mapping_payloads():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="discovery",
        validate_on_init=False,
    )
    try:
        client._discovered_methods = ["square"]
        with pytest.raises(TypeError, match="mapping payloads"):
            list(client.square.unordered([1, 2, 3]))
    finally:
        client.close()


def test_connected_service_unordered_merges_shared_kwargs():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="discovery",
        validate_on_init=False,
    )

    def _call(method, payload, **kwargs):
        return ("node-1", {"ok": True, "data": {"value": payload["x"] + payload["bias"]}})

    try:
        client._discovered_methods = ["square"]
        with patch.object(type(client), "call_balanced", side_effect=_call):
            items = list(client.square.unordered([{"x": 1}, {"x": 3}], bias=2))
        assert sorted(items) == [(0, {"value": 3}), (1, {"value": 5})]
    finally:
        client.close()


def test_connected_service_unordered_is_not_async_iterable():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="discovery",
        validate_on_init=False,
    )
    try:
        client._discovered_methods = ["square"]
        stream = client.square.unordered([{"x": 1}])
        assert hasattr(stream, "__iter__")
        assert not hasattr(stream, "__aiter__")
    finally:
        client.close()
