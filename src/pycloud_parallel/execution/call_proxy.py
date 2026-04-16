from __future__ import annotations

"""Module-style service call proxies for authoritative execution/service clients."""

from typing import Dict, List, Optional, Tuple

from pycloud_parallel.execution.support import (
    _resolve_high_level_service_data,
    _resolve_high_level_service_results,
    _serialize_arrow_compatible,
)


class _CallProxy:
    def __init__(
        self,
        method: str,
        group,
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
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
        serialized_payload = _serialize_arrow_compatible(final_payload)
        node_id, resp = await self._group.acall_balanced(
            self._method,
            serialized_payload,
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


class _SyncCallProxy:
    def __init__(
        self,
        method: str,
        group,
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
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
        serialized_payload = _serialize_arrow_compatible(final_payload)
        node_id, resp = self._group.call_balanced(
            self._method,
            serialized_payload,
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
