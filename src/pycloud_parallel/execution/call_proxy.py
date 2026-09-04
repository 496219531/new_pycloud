from __future__ import annotations

"""Module-style service call proxies for authoritative execution/service clients."""

from collections.abc import Mapping, Sequence
import logging
from typing import Dict, Iterable, List, Optional, Tuple

from pycloud_parallel.execution.base import ExecutionItem
from pycloud_parallel.execution.progress import ProgressOption, is_progress_option
from pycloud_parallel.execution.support import (
    _resolve_high_level_service_data,
    _resolve_high_level_service_results,
)


logger = logging.getLogger(__name__)


def _is_scalar_batch_input(value: object) -> bool:
    if isinstance(value, (str, bytes, bytearray)):
        return True
    try:
        iter(value)  # type: ignore[arg-type]
    except TypeError:
        return True
    return False


def _normalize_batch_call_payloads(
    values: Iterable[object],
    *,
    arg_name: str = "value",
    shared_kwargs: Optional[Mapping[str, object]] = None,
) -> List[Dict[str, object]]:
    if isinstance(values, Mapping):
        logger.warning("batch call received a single mapping payload; treating it as one-item batch")
        values = [values]
    elif _is_scalar_batch_input(values):
        logger.warning("batch call received a scalar input; treating it as one-item batch")
        values = [values]

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
        logger.warning("unordered call received a single mapping payload; treating it as one-item batch")
        values = [values]
    elif _is_scalar_batch_input(values):
        logger.warning("unordered call received a scalar input; treating it as one-item batch")
        values = [values]

    shared = dict(shared_kwargs or {})
    payloads: List[Dict[str, object]] = []
    for item in values:
        if not isinstance(item, Mapping):
            logger.warning("unordered call received scalar payload; wrapping it as {'value': item}")
            payloads.append({"value": item, **shared})
            continue
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
        return_items: bool = False,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> None:
        self._method = method
        self._group = group
        self._payloads = list(payloads)
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status
        self._max_in_flight = max_in_flight
        self._return_items = bool(return_items)
        self._progress = progress
        self._progress_interval_sec = progress_interval_sec

    def __repr__(self) -> str:
        return f"<UnorderedCallProxyStream method={self._method!r} count={len(self._payloads)}>"

    def __iter__(self):
        progress_kwargs = {}
        if self._progress:
            progress_kwargs["progress"] = self._progress
            progress_kwargs["progress_interval_sec"] = self._progress_interval_sec
        yield from self._group.unordered_calls(
            self._method,
            self._payloads,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=self._max_in_flight,
            return_items=self._return_items,
            **progress_kwargs,
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
        return_items: bool = False,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> None:
        self._method = method
        self._group = group
        self._payloads = list(payloads)
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status
        self._max_in_flight = max_in_flight
        self._return_items = bool(return_items)
        self._progress = progress
        self._progress_interval_sec = progress_interval_sec

    def __repr__(self) -> str:
        return f"<AUnorderedCallProxyStream method={self._method!r} count={len(self._payloads)}>"

    def __aiter__(self):
        progress_kwargs = {}
        if self._progress:
            progress_kwargs["progress"] = self._progress
            progress_kwargs["progress_interval_sec"] = self._progress_interval_sec
        return self._group.aunordered_calls(
            self._method,
            self._payloads,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=self._max_in_flight,
            return_items=self._return_items,
            **progress_kwargs,
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
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> None:
        self._method = method
        self._group = group
        self._payloads = list(payloads)
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status
        self._max_in_flight = max_in_flight
        self._progress = progress
        self._progress_interval_sec = progress_interval_sec

    def __iter__(self):
        progress_kwargs = {}
        if self._progress:
            progress_kwargs["progress"] = self._progress
            progress_kwargs["progress_interval_sec"] = self._progress_interval_sec
        yield from self._group.iter_item_calls(
            self._method,
            self._payloads,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=self._max_in_flight,
            **progress_kwargs,
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
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> None:
        self._method = method
        self._group = group
        self._payloads = list(payloads)
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status
        self._max_in_flight = max_in_flight
        self._progress = progress
        self._progress_interval_sec = progress_interval_sec

    def __aiter__(self):
        progress_kwargs = {}
        if self._progress:
            progress_kwargs["progress"] = self._progress
            progress_kwargs["progress_interval_sec"] = self._progress_interval_sec
        return self._group.aiter_item_calls(
            self._method,
            self._payloads,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=self._max_in_flight,
            **progress_kwargs,
        )


class _CallProxy:
    def __init__(
        self,
        method: str,
        group,
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
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

    def stream(self, *args, **kwargs):
        payload = {}
        if args:
            payload["args"] = list(args)
        if args and kwargs:
            payload["kwargs"] = kwargs
        final_payload = payload if args else kwargs
        stream_call = getattr(self._group, "stream_call", None)
        if not callable(stream_call):
            raise AttributeError(f"{type(self._group).__name__} does not support stream_call()")
        return stream_call(
            self._method,
            final_payload,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )

    def map(
        self,
        values: Sequence[object],
        *,
        arg_name: str = "value",
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> List[Optional[object]]:
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        payloads = _normalize_batch_call_payloads(values, arg_name=arg_name, shared_kwargs=shared_kwargs)
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        return self._group.map_calls(
            self._method,
            payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=None,
            **progress_kwargs,
        )

    def map_values(
        self,
        values: Sequence[object],
        *,
        arg_name: str = "value",
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> List[Optional[object]]:
        """Explicit value-mapping alias for ``map(...)``.

        This sends each local value as ``{arg_name: value}`` to the remote
        service method; it does not accept a local Python callable like the
        built-in ``map``.
        """
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        return self.map(
            values,
            arg_name=arg_name,
            timeout_sec=timeout_sec,
            **progress_kwargs,
            **shared_kwargs,
        )

    async def amap(
        self,
        values: Sequence[object],
        *,
        arg_name: str = "value",
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> List[Optional[object]]:
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        payloads = _normalize_batch_call_payloads(values, arg_name=arg_name, shared_kwargs=shared_kwargs)
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        return await self._group.amap_calls(
            self._method,
            payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=None,
            **progress_kwargs,
        )

    async def amap_values(
        self,
        values: Sequence[object],
        *,
        arg_name: str = "value",
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> List[Optional[object]]:
        """Explicit async value-mapping alias for ``amap(...)``."""
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        return await self.amap(
            values,
            arg_name=arg_name,
            timeout_sec=timeout_sec,
            **progress_kwargs,
            **shared_kwargs,
        )

    def unordered(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int | None = None,
        timeout_sec: float = 30.0,
        return_items: bool = False,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> _UnorderedCallProxyStream:
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        return _UnorderedCallProxyStream(
            method=self._method,
            group=self._group,
            payloads=normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
            return_items=return_items,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )

    def aunordered(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int | None = None,
        timeout_sec: float = 30.0,
        return_items: bool = False,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> _AUnorderedCallProxyStream:
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        return _AUnorderedCallProxyStream(
            method=self._method,
            group=self._group,
            payloads=normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
            return_items=return_items,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )

    def iter_items(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int | None = None,
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> _IterItemsProxyStream:
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        return _IterItemsProxyStream(
            method=self._method,
            group=self._group,
            payloads=normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )

    def aiter_items(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int | None = None,
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> _AIterItemsProxyStream:
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        return _AIterItemsProxyStream(
            method=self._method,
            group=self._group,
            payloads=normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )

    def collect_items(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int | None = None,
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> List[ExecutionItem]:
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        return self._group.collect_item_calls(
            self._method,
            normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
            **progress_kwargs,
        )

    async def acollect_items(
        self,
        payloads: Sequence[Mapping[str, object]],
        *,
        max_in_flight: int | None = None,
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> List[ExecutionItem]:
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        normalized_payloads = _normalize_unordered_call_payloads(payloads, shared_kwargs=shared_kwargs)
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        return await self._group.acollect_item_calls(
            self._method,
            normalized_payloads,
            timeout_sec=timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
            max_in_flight=max_in_flight,
            **progress_kwargs,
        )


class _SyncCallProxy:
    def __init__(
        self,
        method: str,
        group,
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
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

    def stream(self, *args, **kwargs):
        payload = {}
        if args:
            payload["args"] = list(args)
        if args and kwargs:
            payload["kwargs"] = kwargs
        final_payload = payload if args else kwargs
        stream_call = getattr(self._group, "stream_call", None)
        if not callable(stream_call):
            raise AttributeError(f"{type(self._group).__name__} does not support stream_call()")
        return stream_call(
            self._method,
            final_payload,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )


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
