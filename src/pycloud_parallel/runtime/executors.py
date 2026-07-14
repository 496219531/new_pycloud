from __future__ import annotations

"""Executor compatibility helpers."""

from typing import Any


def _shutdown_executor(executor: Any, *, wait: bool = False, cancel_futures: bool = True) -> None:
    """Shut down an executor while remaining compatible with Python 3.8."""
    if executor is None:
        return
    try:
        executor.shutdown(wait=wait, cancel_futures=cancel_futures)
    except TypeError as exc:
        if "cancel_futures" not in str(exc):
            raise
        executor.shutdown(wait=wait)
