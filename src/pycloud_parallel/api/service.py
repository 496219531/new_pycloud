from __future__ import annotations

"""Future public Service API facade."""

from pycloud_parallel.execution.service_session import Service

__all__ = [
    "Service",
]


def __dir__() -> list[str]:
    return list(__all__)


try:
    del annotations
except NameError:
    pass
