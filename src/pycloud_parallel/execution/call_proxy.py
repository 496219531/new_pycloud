from __future__ import annotations

"""Module-style service call proxies for authoritative execution/service clients."""

from collections.abc import Mapping, Sequence
from typing import Dict, Iterable, List, Optional, Tuple

from pycloud_parallel.execution.base import ExecutionItem
from pycloud_parallel.execution.support import (
    _resolve_high_level_service_data,
    _resolve_high_level_service_results,
)


def _normalize_batch_call_payloads(
    values: Iterable[object],
    *,
    arg_name: str = "value",
    shared_kwargs: Optional[Mapping[str, object]] = None,
) -> List[Dict[str, object]]:
    if isinstance(values, Mapping):
        raise TypeError("batch call inputs must be a sequence, not a single mapping payload")

    normalized_arg_name = "value" if arg_name is None else str(arg_name).strip()
    shared = dict(shared_kwargs or {})
    payloads: List[Dict[str, object]] = []
    for item in values:
        if isinstance(item, Mapping):
            payloads.append({**dict(item), **shared})
            continue
        if not normalized_arg_name:
            raise TypeError(
                "map()/amap() non-dict batch inputs require arg_name"
            )
        payloads.append({normalized_arg_name: item, **shared})
    return payloads


def _normalize_unordered_call_payloads(
    values: Iterable[object],
    *,
    shared_kwargs: Optional[Mapping[str, object]] = None,
) -> List[Dict[str, object]]:
    if isinstance(values, Mapping):
        raise TypeError(
            "unordered()/aunordered()/iter_items() inputs must be a sequence, not a single mapping payload"
        )

    shared = dict(shared_kwargs or {})
    payloads: List[Dict[str, object]] = []
    for item in values:
        if not isinstance(item, Mapping):
            raise TypeError(
                "unordered()/aunordered()/iter_items() inputs must be mapping payloads; "
                "use map(..., arg_name=...) for scalar batches"
            )
        payloads.append({**dict(item), **shared})
    return payloads


class _UnorderedCallProxyStream:
    def __init__(
        self,
        *,
        method: str,
        group,
        payloads: List[Dict[str, object]],
        timeout_sec: float,
        strategy: str,
        refresh_status: bool,
        max_in_flight: int,
    ) -> None:
        self._method = method
        self._group = group
        self._payloads = list(payloads)
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status
        self._max_in_flight = max_in_flight

    def __repr__(self) -> str:
        return f"<UnorderedCallProxyStream method={self._method!r} count={len(self._payloads)}>"

    def __iter__(self):
        yield from self._group.unordered_calls(
            self._method,
            self._payloads,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=self._max_in_flight,
        )


class _AUnorderedCallProxyStream:
    def __init__(
        self,
        *,
        method: str,
        group,
        payloads: List[Dict[str, object]],
        timeout_sec: float,
        strategy: str,
        refresh_status: bool,
        max_in_flight: int,
    ) -> None:
        self._method = method
        self._group = group
        self._payloads = list(payloads)
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status
        self._max_in_flight = max_in_flight

    def __repr__(self) -> str:
        return f"<AUnorderedCallProxyStream method={self._method!r} count={len(self._payloads)}>"

    def __aiter__(self):
        return self._group.aunordered_calls(
            self._method,
            self._payloads,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=self._max_in_flight,
        )


class _IterItemsProxyStream:
    def __init__(
        self,
        *,
        method: str,
        group,
        payloads: List[Dict[str, object]],
        timeout_sec: float,
        strategy: str,
        refresh_status: bool,
        max_in_flight: int,
    ) -> None:
        self._method = method
        self._group = group
        self._payloads = list(payloads)
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status
        self._max_in_flight = max_in_flight

    def __iter__(self):
        yield from self._group.iter_item_calls(
            self._method,
            self._payloads,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=self._max_in_flight,
        )


class _AIterItemsProxyStream:
    def __init__(
        self,
        *,
        method: str,
        group,
        payloads: List[Dict[str, object]],
        timeout_sec: float,
        strategy: str,
        refresh_status: bool,
        max_in_flight: int,
    ) -> None:
        self._method = method
        self._group = group
        self._payloads = list(payloads)
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status
        self._max_in_flight = max_in_flight

    def __aiter__(self):
        return self._group.aiter_item_calls(
            self._method,
            self._payloads,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=self._max_in_flight,
        )


class _CallProxy:
    def __init__(
        self,
        method: str,
        group,
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status

    def __repr__(self) -> str:
        return f"<CallProxy method={self._method!r}>"

    @property
    def method(self) -> str:
        return self._method

    async def __call__(self, *args, **kwargs) -> Dict[str, object]:
        payload = {}
        if args:
            payload["args"] = list(args)
        if args and kwargs:
            payload["kwargs"] = kwargs
        final_payload = payload if args else kwargs
        node_id, resp = await self._group.acall_balanced(
            self._method,
            final_payload,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )
        return _resolve_high_level_service_data(self._group, node_id=node_id, response=resp)

    def __await__(self):
        return self().__await__()

    @property
    def sync(self) -> "_SyncCallProxy":
        return _SyncCallProxy(
            method=self._method,
            group=self._group,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )

    @property
    def broadcast(self) -> "_BroadcastProxy":
        return _BroadcastProxy(
            method=self._method,
            group=self._group,
            timeout_sec=self._timeout_sec,
        )

    def with_options(
        self,
        *,
        timeout_sec: Optional[float] = None,
        strategy: Optional[str] = None,
        refresh_status: Optional[bool] = None,
    ) -> "_CallProxy":
        return _CallProxy(
            method=self._method,
            group=self._group,
            timeout_sec=timeout_sec if timeout_sec is not None else self._timeout_sec,
            strategy=strategy if strategy is not None else self._strategy,
            refresh_status=refresh_status if refresh_status is not None else self._refresh_status,
        )

    def map(
        self,
        values: Sequence[object],
        *,
        arg_name: str = "value",
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> List[Optional[object]]:
        payloads = _normalize_batch_call_payloads(values, arg_name=arg_name, shared_kwargs=shared_kwargs)
        return self._group.map_calls(
            self._method,
            payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=32,
        )

    async def amap(
        self,
        values: Sequence[object],
        *,
        arg_name: str = "value",
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> List[Optional[object]]:
        payloads = _normalize_batch_call_payloads(values, arg_name=arg_name, shared_kwargs=shared_kwargs)
        return await self._group.amap_calls(
            self._method,
            payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=32,
        )

    def unordered(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int = 32,
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> _UnorderedCallProxyStream:
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        return _UnorderedCallProxyStream(
            method=self._method,
            group=self._group,
            payloads=normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
        )

    def aunordered(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int = 32,
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> _AUnorderedCallProxyStream:
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        return _AUnorderedCallProxyStream(
            method=self._method,
            group=self._group,
            payloads=normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
        )

    def iter_items(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int = 32,
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> _IterItemsProxyStream:
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        return _IterItemsProxyStream(
            method=self._method,
            group=self._group,
            payloads=normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
        )

    def aiter_items(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int = 32,
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> _AIterItemsProxyStream:
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        return _AIterItemsProxyStream(
            method=self._method,
            group=self._group,
            payloads=normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
        )

    def collect_items(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int = 32,
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> List[ExecutionItem]:
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        return self._group.collect_item_calls(
            self._method,
            normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
        )

    async def acollect_items(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int = 32,
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> List[ExecutionItem]:
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        return await self._group.acollect_item_calls(
            self._method,
            normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
        )


class _SyncCallProxy:
    def __init__(
        self,
        method: str,
        group,
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status

    def __repr__(self) -> str:
        return f"<SyncCallProxy method={self._method!r}>"

    def __call__(self, *args, **kwargs) -> Dict[str, object]:
        payload = {}
        if args:
            payload["args"] = list(args)
        if args and kwargs:
            payload["kwargs"] = kwargs
        final_payload = payload if args else kwargs
        node_id, resp = self._group.call_balanced(
            self._method,
            final_payload,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )
        return _resolve_high_level_service_data(self._group, node_id=node_id, response=resp)


class _BroadcastProxy:
    def __init__(
        self,
        method: str,
        group,
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._max_concurrency = max_concurrency

    def __repr__(self) -> str:
        return f"<BroadcastProxy method={self._method!r}>"

    async def __call__(self, **kwargs) -> List[Tuple[Optional[str], Optional[object], Optional[Exception]]]:
        results = await self._group.acall_all(
            self._method,
            kwargs,
            timeout_sec=self._timeout_sec,
            max_concurrency=self._max_concurrency,
        )
        return _resolve_high_level_service_results(self._group, results=results)

    def __await__(self):
        return self().__await__()


__all__ = ["_CallProxy", "_SyncCallProxy", "_BroadcastProxy"]
