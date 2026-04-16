from __future__ import annotations

"""Code-version digest helpers shared across control-plane modules."""

import hashlib
import json
from typing import Any, Sequence

from pycloud_parallel.controlplane.artifact import _normalize_dependency_policy_mode


def _stable_json_bytes(data: Any) -> bytes:
    return json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _sha256_text(data: Any) -> str:
    return f"sha256:{hashlib.sha256(_stable_json_bytes(data)).hexdigest()}"


def _code_version_from_digest(
    digest: str,
    *,
    runtime: str,
    entry_module: str,
    entry_callable: str,
    package_format: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    dependency_policy_mode: str = "",
    dependency_allowlist: Sequence[str],
) -> str:
    normalized_digest = str(digest or "").strip().lower()
    if not normalized_digest:
        raise ValueError("invalid code digest")
    normalized_dependency_policy_mode = _normalize_dependency_policy_mode(
        dependency_policy_mode,
        dependency_allowlist=dependency_allowlist,
    )
    variant_payload = {
        "runtime": str(runtime or "").strip(),
        "entry_module": str(entry_module or "").strip(),
        "entry_callable": str(entry_callable or "").strip(),
        "package_format": str(package_format or "").strip(),
        "export_mode": str(export_mode or "").strip(),
        "export_methods": [str(name) for name in export_methods],
        "export_decorator": str(export_decorator or "").strip(),
        "dependency_policy_mode": normalized_dependency_policy_mode,
        "dependency_allowlist": [str(name) for name in dependency_allowlist],
    }
    variant_digest = hashlib.sha256(_stable_json_bytes(variant_payload)).hexdigest()[:16]
    return f"sha256:{normalized_digest}.{variant_digest}"


__all__ = ["_stable_json_bytes", "_sha256_text", "_code_version_from_digest"]
