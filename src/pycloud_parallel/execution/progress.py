from __future__ import annotations

from dataclasses import dataclass
import sys
import time
from typing import Callable, Optional, TextIO, Union


@dataclass(frozen=True)
class ProgressEvent:
    label: str
    phase: str
    total: int
    completed: int
    succeeded: int
    failed: int
    inflight: int
    submitted: int
    elapsed_sec: float
    rate: float
    eta_sec: Optional[float]
    last_error: str = ""


ProgressCallback = Callable[[ProgressEvent], object]
ProgressOption = Union[bool, ProgressCallback]


def is_progress_option(value: object) -> bool:
    return isinstance(value, bool) or callable(value)


class ProgressReporter:
    def __init__(
        self,
        progress: ProgressOption = False,
        *,
        label: str,
        total: int = 0,
        interval_sec: float = 2.0,
        stream: Optional[TextIO] = None,
    ) -> None:
        self._progress = progress
        self._label = str(label or "pycloud")
        self._total = max(0, int(total or 0))
        self._interval_sec = max(0.1, float(interval_sec or 2.0))
        self._stream = stream if stream is not None else sys.stderr
        self._started_at = time.monotonic()
        self._last_emit_at = 0.0
        self._last_completed = -1
        self._last_had_newline = True

    @property
    def enabled(self) -> bool:
        return bool(self._progress) if is_progress_option(self._progress) else False

    def emit(
        self,
        *,
        phase: str,
        completed: int = 0,
        succeeded: int = 0,
        failed: int = 0,
        inflight: int = 0,
        submitted: int = 0,
        last_error: str = "",
        force: bool = False,
    ) -> None:
        if not self.enabled:
            return
        now = time.monotonic()
        completed_value = max(0, int(completed or 0))
        if not force and completed_value == self._last_completed and now - self._last_emit_at < self._interval_sec:
            return
        if not force and now - self._last_emit_at < self._interval_sec and completed_value < self._total:
            return
        self._last_emit_at = now
        self._last_completed = completed_value
        elapsed = max(0.0, now - self._started_at)
        rate = completed_value / elapsed if elapsed > 0 and completed_value > 0 else 0.0
        remaining = max(0, self._total - completed_value) if self._total else 0
        eta = (remaining / rate) if rate > 0 and remaining > 0 else None
        event = ProgressEvent(
            label=self._label,
            phase=str(phase or ""),
            total=self._total,
            completed=completed_value,
            succeeded=max(0, int(succeeded or 0)),
            failed=max(0, int(failed or 0)),
            inflight=max(0, int(inflight or 0)),
            submitted=max(0, int(submitted or 0)),
            elapsed_sec=elapsed,
            rate=rate,
            eta_sec=eta,
            last_error=str(last_error or ""),
        )
        if callable(self._progress) and not isinstance(self._progress, bool):
            self._progress(event)
            return
        self._write_event(event, final=str(phase or "").lower() in {"done", "completed", "failed"})

    def done(self, *, completed: int, succeeded: int, failed: int, submitted: int = 0, last_error: str = "") -> None:
        self.emit(
            phase="done",
            completed=completed,
            succeeded=succeeded,
            failed=failed,
            inflight=0,
            submitted=submitted,
            last_error=last_error,
            force=True,
        )

    def _write_event(self, event: ProgressEvent, *, final: bool) -> None:
        stream = self._stream
        total_text = str(event.total) if event.total else "?"
        eta_text = f", eta {event.eta_sec:.0f}s" if event.eta_sec is not None else ""
        error_text = f", last_error={event.last_error[:80]}" if event.last_error else ""
        text = (
            f"[pycloud] {event.label} {event.completed}/{total_text} done, "
            f"ok={event.succeeded}, failed={event.failed}, inflight={event.inflight}, "
            f"{event.rate:.1f}/s{eta_text}, elapsed {event.elapsed_sec:.1f}s{error_text}"
        )
        is_tty = bool(getattr(stream, "isatty", lambda: False)())
        if is_tty and not final:
            stream.write("\r" + text)
            stream.flush()
            self._last_had_newline = False
            return
        if is_tty and final and not self._last_had_newline:
            stream.write("\r" + text + "\n")
        else:
            stream.write(text + "\n")
        stream.flush()
        self._last_had_newline = True


__all__ = ["ProgressCallback", "ProgressEvent", "ProgressOption", "ProgressReporter", "is_progress_option"]
