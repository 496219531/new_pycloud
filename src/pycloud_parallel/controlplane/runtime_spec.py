from __future__ import annotations

"""Helpers for lightweight Python runtime constraint parsing and matching."""

import re
from dataclasses import dataclass
from typing import Optional, Tuple


_RUNTIME_RE = re.compile(r"^(?P<op>>=|<=|>|<)?\s*(?:py)?(?P<major>\d+)(?:\.(?P<minor>\d+))?$", re.IGNORECASE)


@dataclass(frozen=True)
class PythonRuntimeSpec:
    op: str
    major: int
    minor: Optional[int] = None

    def normalized(self) -> str:
        version = f"py{self.major}" if self.minor is None else f"py{self.major}.{self.minor}"
        return f"{self.op}{version}" if self.op else version

    def comparable(self) -> Tuple[int, int]:
        return self.major, 0 if self.minor is None else self.minor


def parse_python_runtime_spec(text: str) -> Optional[PythonRuntimeSpec]:
    raw = str(text or "").strip()
    if not raw:
        return None
    match = _RUNTIME_RE.fullmatch(raw)
    if match is None:
        raise ValueError(
            "invalid runtime spec; expected py3 / py3.11 / >=py3.11 / <=3.11"
        )
    op = str(match.group("op") or "").strip()
    major = int(match.group("major"))
    minor_text = match.group("minor")
    minor = int(minor_text) if minor_text is not None else None
    return PythonRuntimeSpec(op=op, major=major, minor=minor)


def normalize_python_runtime_spec(text: str) -> str:
    spec = parse_python_runtime_spec(text)
    return "" if spec is None else spec.normalized()


def matches_python_runtime(node_python_version: str, runtime_spec: str) -> bool:
    constraint = parse_python_runtime_spec(runtime_spec)
    if constraint is None:
        return True
    node = parse_python_runtime_spec(node_python_version)
    if node is None:
        return False
    if node.op:
        raise ValueError(f"node python_version must not contain comparator: {node_python_version}")

    if not constraint.op:
        if constraint.minor is None:
            return node.major == constraint.major
        return node.major == constraint.major and node.minor == constraint.minor

    node_comp = node.comparable()
    expected_comp = constraint.comparable()
    if constraint.op == ">=":
        return node_comp >= expected_comp
    if constraint.op == "<=":
        return node_comp <= expected_comp
    if constraint.op == ">":
        return node_comp > expected_comp
    if constraint.op == "<":
        return node_comp < expected_comp
    raise ValueError(f"unsupported runtime operator: {constraint.op}")
