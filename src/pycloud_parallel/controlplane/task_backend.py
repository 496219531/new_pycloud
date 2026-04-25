from __future__ import annotations

"""Task-session helper objects."""

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class _TaskPoolCallProxy:
    session: Any
    method_name: str

    def _build_payload(self, *args, **kwargs) -> Dict[str, object]:
        payload: Dict[str, object] = {}
        if args:
            payload["args"] = list(args)
            if kwargs:
                payload["kwargs"] = kwargs
        elif kwargs:
            payload.update(kwargs)
        return payload

    def submit(self, *args, **kwargs) -> str:
        payload = self._build_payload(*args, **kwargs)
        resp = self.session.submit_payloads([payload], task_method=self.method_name)
        if len(resp.accepted) != 1:
            raise RuntimeError(
                f"expected exactly one accepted task for method={self.method_name}, "
                f"got accepted={len(resp.accepted)} rejected={len(resp.rejected)}"
            )
        return str(resp.accepted[0].task_id)

    def __call__(self, *args, **kwargs) -> str:
        return self.submit(*args, **kwargs)

    def sync(self, *args, **kwargs):
        enter_exclusive = getattr(self.session, "_enter_exclusive_mode", None)
        exit_exclusive = getattr(self.session, "_exit_exclusive_mode", None)
        entered_exclusive = False
        if callable(enter_exclusive) and callable(exit_exclusive):
            enter_exclusive("run.sync", require_clean=True)
            entered_exclusive = True
        try:
            task_id = self.submit(*args, **kwargs)
            items = self.session._collect_data_for_task_ids({task_id}, timeout_sec=30.0)  # noqa: SLF001
            results = [data for _, data in items]
            if len(results) == 1:
                return results[0]
            return results
        finally:
            if entered_exclusive:
                exit_exclusive("run.sync")
