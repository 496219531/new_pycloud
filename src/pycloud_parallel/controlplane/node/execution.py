from __future__ import annotations

"""Execution-loading helpers for NodeControl domain."""

import contextlib
import hashlib
import importlib
import importlib.util
import inspect
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

from pycloud_parallel.controlplane.artifact import _normalize_dependency_policy_mode
from pycloud_parallel.controlplane.config import get_payload_policy
from pycloud_parallel.controlplane.data_ref import maybe_data_ref
from pycloud_parallel.controlplane.node.filesystem import (
    _managed_globals_manifest_path,
    _managed_globals_value_path,
)
from pycloud_parallel.controlplane.node.results import (
    LargeResultError,
    ObjectResolutionError,
    _normalize_user_return,
    _resolve_object_refs_in_payload,
)
from pycloud_parallel.controlplane.runtime_spec import matches_python_runtime, normalize_python_runtime_spec
from pycloud_parallel.controlplane.serialization import (
    convert_dict_to_arrow,
    is_arrow_compatible,
    log_payload_flow,
    serialize_arrow_compatible,
    summarize_payload_flow_value,
)
from pycloud_parallel.runtime.compat import runtime_mismatch_message_for_current_node
from pycloud_parallel.runtime.errors import RuntimeMismatchError

_DEFAULT_EXPORT_DECORATOR = "pycloud_export"
service_timing_logger = logging.getLogger("pycloud_parallel.service_timing")

_ROUTER_CACHE_LOCK = threading.Lock()
_ROUTER_CACHE: Dict[str, Tuple[Any, Dict[str, Any], Dict[str, Tuple[str, str]]]] = {}
_MANAGED_GLOBALS_CACHE_LOCK = threading.Lock()
_MANAGED_GLOBALS_CACHE: Dict[str, str] = {}
_MANAGED_GLOBALS_APPLY_LOCKS_LOCK = threading.Lock()
_MANAGED_GLOBALS_APPLY_LOCKS: Dict[str, threading.Lock] = {}


@dataclass(frozen=True)
class ExecuteSpec:
    artifact_path: str
    entry_module: str
    package_format: str
    dependency_path: str
    dependency_policy_mode: str
    object_dir: str
    work_dir: str
    export_mode: str
    export_methods: Tuple[str, ...]
    export_decorator: str
    method_name: str
    entry_callable: str
    payload: Dict[str, Any]
    payload_mode: str = "task_submit"
    managed_globals_scope_dir: str = ""
    managed_globals_digest: str = ""
    warmup_only: bool = False

    def to_payload(self) -> Dict[str, Any]:
        return {
            "artifact_path": self.artifact_path,
            "entry_module": self.entry_module,
            "package_format": self.package_format,
            "dependency_path": self.dependency_path,
            "dependency_policy_mode": self.dependency_policy_mode,
            "object_dir": self.object_dir,
            "work_dir": self.work_dir,
            "export_mode": self.export_mode,
            "export_methods": list(self.export_methods),
            "export_decorator": self.export_decorator,
            "method_name": self.method_name,
            "entry_callable": self.entry_callable,
            "payload": dict(self.payload or {}),
            "payload_mode": self.payload_mode,
            "managed_globals_scope_dir": self.managed_globals_scope_dir,
            "managed_globals_digest": self.managed_globals_digest,
            "warmup_only": bool(self.warmup_only),
        }


def _build_execute_spec_model(
    artifact: Any,
    *,
    object_dir: Path,
    work_dir: Optional[Path] = None,
    method_name: str,
    payload: dict,
    payload_mode: str = "task_submit",
    managed_globals_scope_dir: str = "",
    managed_globals_digest: str = "",
    warmup_only: bool = False,
) -> ExecuteSpec:
    return ExecuteSpec(
        artifact_path=str(artifact.path),
        entry_module=str(artifact.entry_module),
        package_format=str(artifact.package_format),
        dependency_path=str(artifact.dependency_path),
        dependency_policy_mode=str(artifact.dependency_policy_mode),
        object_dir=str(object_dir),
        work_dir=str(work_dir or ""),
        export_mode=str(artifact.export_mode),
        export_methods=tuple(str(item) for item in artifact.export_methods),
        export_decorator=str(artifact.export_decorator),
        method_name=str(method_name),
        entry_callable=str(artifact.entry_callable),
        payload=dict(payload or {}),
        payload_mode=str(payload_mode or "task_submit"),
        managed_globals_scope_dir=str(managed_globals_scope_dir or ""),
        managed_globals_digest=str(managed_globals_digest or ""),
        warmup_only=bool(warmup_only),
    )


def _build_execute_spec(
    artifact: Any,
    *,
    object_dir: Path,
    work_dir: Optional[Path] = None,
    method_name: str,
    payload: dict,
    payload_mode: str = "task_submit",
    managed_globals_scope_dir: str = "",
    managed_globals_digest: str = "",
    warmup_only: bool = False,
) -> Dict[str, Any]:
    return _build_execute_spec_model(
        artifact,
        object_dir=object_dir,
        work_dir=work_dir,
        method_name=method_name,
        payload=payload,
        payload_mode=payload_mode,
        managed_globals_scope_dir=managed_globals_scope_dir,
        managed_globals_digest=managed_globals_digest,
        warmup_only=warmup_only,
    ).to_payload()


def _artifact_module_name(artifact_path: str) -> str:
    return f"_pycloud_user_{hashlib.sha1(artifact_path.encode('utf-8')).hexdigest()}"


def _normalize_package_format(package_format: str, artifact_path: str = "") -> str:
    raw = str(package_format or "").strip().lower().replace("_", "").replace(".", "")
    if raw in ("py", "python"):
        return "py"
    if raw in ("targz", "tgz", "tar"):
        return "tar.gz"
    if raw == "zip":
        return "zip"
    if raw == "whl":
        return "whl"

    lower_name = str(artifact_path or "").strip().lower()
    if lower_name.endswith(".tar.gz") or lower_name.endswith(".tgz"):
        return "tar.gz"
    if lower_name.endswith(".zip"):
        return "zip"
    if lower_name.endswith(".whl"):
        return "whl"
    if lower_name.endswith(".py"):
        return "py"
    return "bin"


def _package_suffix(package_format: str) -> str:
    normalized = _normalize_package_format(package_format)
    if normalized == "tar.gz":
        return ".tar.gz"
    if normalized == "zip":
        return ".zip"
    if normalized == "whl":
        return ".whl"
    if normalized == "py":
        return ".py"
    return ".bin"


def _normalize_export_spec(
    *,
    mode: str,
    methods: Sequence[str],
    decorator: str,
    entry_callable: str,
) -> Tuple[str, Tuple[str, ...], str]:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in ("decorator", "explicit", "all", "single"):
        normalized_mode = ""

    normalized_methods = tuple(sorted({x.strip() for x in methods if str(x).strip()}))
    normalized_decorator = _DEFAULT_EXPORT_DECORATOR
    fallback_callable = str(entry_callable or "").strip() or "run"

    if not normalized_mode:
        if normalized_methods:
            normalized_mode = "explicit"
        elif fallback_callable:
            normalized_mode = "single"
        else:
            normalized_mode = "decorator"

    if normalized_mode == "single":
        normalized_methods = (fallback_callable,)
    return normalized_mode, normalized_methods, normalized_decorator


def _validate_python_runtime_or_raise(*, node_python_version: str, runtime: str) -> str:
    normalized_runtime = normalize_python_runtime_spec(runtime)
    if not normalized_runtime:
        return ""
    if not matches_python_runtime(node_python_version, normalized_runtime):
        raise RuntimeMismatchError(
            runtime_mismatch_message_for_current_node(
                requested_runtime=normalized_runtime,
                node_python_version=node_python_version,
            )
        )
    return normalized_runtime


def _is_user_artifact_error(exc: BaseException) -> bool:
    user_error_types = (
        SyntaxError,
        ImportError,
        ModuleNotFoundError,
        AttributeError,
        NameError,
        TypeError,
        ValueError,
    )
    runtime_error_markers = (
        "cannot load python module",
        "entry_module is required",
        "not found",
        "not callable",
        "no exported methods found",
        "duplicate exported method",
        "exported method cannot start with",
    )

    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, user_error_types):
            return True
        if isinstance(current, RuntimeError):
            message = str(current)
            if any(marker in message for marker in runtime_error_markers):
                return True
        current = current.__cause__ or current.__context__
    return False


def _missing_import_name(exc: BaseException) -> str:
    current: Optional[BaseException] = exc
    while current is not None:
        if isinstance(current, ModuleNotFoundError):
            name = str(getattr(current, "name", "") or "").strip()
            if name:
                return name
        current = current.__cause__ or current.__context__
    return ""


def _dependency_policy_missing_import_hint(
    *,
    dependency_policy_mode: str,
    missing_import: str,
    install_failed: bool = False,
) -> str:
    normalized_mode = _normalize_dependency_policy_mode(dependency_policy_mode)
    if not missing_import:
        if install_failed and normalized_mode == "allow_install":
            return (
                " artifact dependency policy is `allow_install`; dependency installation failed. "
                "Pin versions and verify node network/package index availability."
            )
        return ""
    if normalized_mode == "node_preinstalled":
        return (
            f" artifact dependency policy is `node_preinstalled`; node environment is missing `{missing_import}`. "
            "Preinstall it on the node, or switch to a prebuilt artifact."
        )
    if normalized_mode == "allow_install":
        if install_failed:
            return (
                f" artifact dependency policy is `allow_install`; dependency install failed for `{missing_import}`. "
                "Pin the version and verify node network/package index availability."
            )
        return (
            f" artifact dependency policy is `allow_install`; dependency `{missing_import}` is still unavailable "
            "after preparation. Pin the version and verify node network/package index availability."
        )
    return (
        f" artifact dependency policy is `prebuilt`; missing dependency `{missing_import}`. "
        "Rebuild the artifact with bundled dependencies, or switch to `ArtifactDeps.allow_install([...])`."
    )


def _describe_artifact_error(
    exc: BaseException,
    *,
    entry_module: str,
    entry_callable: str,
    package_format: str,
    dependency_policy_mode: str = "",
    install_failed: bool = False,
) -> str:
    if isinstance(exc, SyntaxError):
        line = int(exc.lineno or 0)
        filename = str(exc.filename or entry_module or "<artifact>")
        if line > 0:
            detail = f"SyntaxError at {filename}:{line}: {exc.msg}"
        else:
            detail = f"SyntaxError at {filename}: {exc.msg}"
    else:
        message = str(exc) or repr(exc)
        detail = f"{exc.__class__.__name__}: {message}"
    normalized_module = str(entry_module or "").strip() or "<auto>"
    normalized_callable = str(entry_callable or "").strip() or "run"
    normalized_format = _normalize_package_format(package_format, package_format or "artifact.py")
    missing_import = _missing_import_name(exc)
    repair_hint = _dependency_policy_missing_import_hint(
        dependency_policy_mode=dependency_policy_mode,
        missing_import=missing_import,
        install_failed=install_failed,
    )
    return (
        "artifact validation failed while loading "
        f"(entry_module={normalized_module}, entry_callable={normalized_callable}, package_format={normalized_format}): "
        f"{detail}{repair_hint}"
    )


def _describe_user_execution_error(exc: BaseException, *, dependency_policy_mode: str = "") -> str:
    message = str(exc) or repr(exc)
    detail = f"{exc.__class__.__name__}: {message}"
    missing_import = _missing_import_name(exc)
    repair_hint = _dependency_policy_missing_import_hint(
        dependency_policy_mode=dependency_policy_mode,
        missing_import=missing_import,
        install_failed=False,
    )
    return f"user code execution failed: {detail}{repair_hint}"


def _normalize_dependency_allowlist(requirements: Sequence[str]) -> Tuple[str, ...]:
    normalized = []
    seen: set[str] = set()
    for item in requirements or ():
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return tuple(normalized)


@contextmanager
def _temporary_import_paths(*paths: str):
    inserted = []
    for raw in paths:
        path = str(raw or "").strip()
        if not path:
            continue
        sys.path.insert(0, path)
        inserted.append(path)
    try:
        yield
    finally:
        for path in reversed(inserted):
            try:
                sys.path.remove(path)
            except ValueError:
                pass


@contextmanager
def _temporary_working_dir(path: str):
    target = str(path or "").strip()
    if not target:
        yield
        return
    target_path = Path(target)
    target_path.mkdir(parents=True, exist_ok=True)
    previous = Path.cwd()
    os.chdir(target_path)
    try:
        yield
    finally:
        os.chdir(previous)


def _install_dependency_allowlist(requirements: Sequence[str], *, target_dir: Path) -> None:
    normalized = _normalize_dependency_allowlist(requirements)
    if not normalized:
        return
    target_dir = Path(target_dir)
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_dir = target_dir.with_name(f"{target_dir.name}.tmp-{uuid.uuid4().hex}")
    shutil.rmtree(staging_dir, ignore_errors=True)
    staging_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--upgrade",
        "--no-input",
        "--disable-pip-version-check",
        "--target",
        str(staging_dir),
        *normalized,
    ]
    completed = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        shutil.rmtree(staging_dir, ignore_errors=True)
        stderr = str(completed.stderr or "").strip()
        stdout = str(completed.stdout or "").strip()
        detail = stderr or stdout or f"pip exited with code {completed.returncode}"
        raise RuntimeError(f"dependency install failed for {list(normalized)}: {detail}")
    backup_dir = target_dir.with_name(f"{target_dir.name}.bak-{uuid.uuid4().hex}")
    try:
        if target_dir.exists():
            os.replace(str(target_dir), str(backup_dir))
        os.replace(str(staging_dir), str(target_dir))
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        if backup_dir.exists() and not target_dir.exists():
            os.replace(str(backup_dir), str(target_dir))
        raise
    finally:
        shutil.rmtree(backup_dir, ignore_errors=True)


def _purge_module_tree(module_name: str) -> None:
    if not module_name:
        return
    to_delete = [k for k in list(sys.modules.keys()) if k == module_name or k.startswith(f"{module_name}.")]
    for key in to_delete:
        sys.modules.pop(key, None)


def _load_user_module(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
    dependency_path: str = "",
):
    path = Path(artifact_path)
    format_name = _normalize_package_format(package_format, path.name)

    if format_name == "py" and path.is_file() and path.suffix.lower() == ".py":
        module_name = _artifact_module_name(artifact_path)
        loaded = sys.modules.get(module_name)
        if loaded is not None:
            return loaded
        spec = importlib.util.spec_from_file_location(module_name, artifact_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load python module from {artifact_path}")
        module = importlib.util.module_from_spec(spec)
        with _temporary_import_paths(dependency_path):
            spec.loader.exec_module(module)
            sys.modules[module_name] = module
        return module

    if not entry_module:
        raise RuntimeError("entry_module is required for package artifacts")

    importlib.invalidate_caches()
    root_module = entry_module.split(".", 1)[0].strip()
    if root_module:
        _purge_module_tree(root_module)
    _purge_module_tree(entry_module)
    with _temporary_import_paths(dependency_path, artifact_path):
        return importlib.import_module(entry_module)


def _purge_loaded_artifact_modules(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
    dependency_path: str = "",
) -> None:
    format_name = _normalize_package_format(package_format, Path(artifact_path).name)
    if format_name == "py":
        _purge_module_tree(_artifact_module_name(artifact_path))
    else:
        root_module = str(entry_module or "").split(".", 1)[0].strip()
        if root_module:
            _purge_module_tree(root_module)
        _purge_module_tree(str(entry_module or "").strip())

    prefixes = [str(Path(artifact_path).resolve())]
    if dependency_path:
        prefixes.append(str(Path(dependency_path).resolve()))

    for name, module in list(sys.modules.items()):
        module_file = getattr(module, "__file__", None)
        if module_file:
            resolved_file = str(Path(module_file).resolve())
            if any(resolved_file.startswith(prefix) for prefix in prefixes):
                sys.modules.pop(name, None)
                continue
        module_paths = getattr(module, "__path__", None)
        if module_paths:
            resolved_paths = [str(Path(p).resolve()) for p in module_paths]
            if any(any(path.startswith(prefix) for prefix in prefixes) for path in resolved_paths):
                sys.modules.pop(name, None)


def _build_callable_router(
    module,
    *,
    mode: str,
    methods: Sequence[str],
    decorator: str,
    entry_callable: str,
) -> Tuple[Dict[str, Any], Dict[str, Tuple[str, str]]]:
    marker = _DEFAULT_EXPORT_DECORATOR
    marker_candidates = {
        marker,
        f"__{marker}__",
        _DEFAULT_EXPORT_DECORATOR,
        f"__{_DEFAULT_EXPORT_DECORATOR}__",
    }
    exported_declared = set()
    declared = getattr(module, "__pycloud_exports__", None)
    if isinstance(declared, (list, tuple, set)):
        exported_declared = {str(x).strip() for x in declared if str(x).strip()}

    all_callables: Dict[str, Any] = {}
    for name in dir(module):
        if name.startswith("_"):
            continue
        value = getattr(module, name, None)
        if callable(value):
            all_callables[name] = value

    router: Dict[str, Any] = {}
    method_info: Dict[str, Tuple[str, str]] = {}

    def _register(method_name: str, fn: Any) -> None:
        normalized_method = str(method_name or "").strip()
        if not normalized_method:
            return
        if normalized_method.startswith("_"):
            raise RuntimeError(f"exported method cannot start with _: {normalized_method}")
        if normalized_method in router:
            raise RuntimeError(f"duplicate exported method: {normalized_method}")
        router[normalized_method] = fn
        method_info[normalized_method] = (str(getattr(fn, "__qualname__", normalized_method)), inspect.getdoc(fn) or "")

    if mode == "all":
        for name, fn in all_callables.items():
            _register(name, fn)
    elif mode == "explicit":
        for name in methods:
            fn = getattr(module, name, None)
            if fn is None or not callable(fn):
                raise RuntimeError(f"explicit exported method `{name}` not found or not callable")
            _register(name, fn)
    elif mode == "single":
        only = (list(methods)[:1] or [str(entry_callable or "run").strip() or "run"])[0]
        fn = getattr(module, only, None)
        if fn is None or not callable(fn):
            raise RuntimeError(f"callable `{only}` not found in uploaded artifact")
        _register(only, fn)
    else:
        for name, fn in all_callables.items():
            if name in exported_declared:
                exported_name = str(getattr(fn, "__pycloud_export_name__", "") or name).strip()
                _register(exported_name, fn)
                continue
            if any(bool(getattr(fn, attr, False)) for attr in marker_candidates):
                exported_name = str(getattr(fn, "__pycloud_export_name__", "") or name).strip()
                _register(exported_name, fn)
        if not router:
            legacy_name = str(entry_callable or "").strip()
            if legacy_name:
                legacy_fn = getattr(module, legacy_name, None)
                if legacy_fn is not None and callable(legacy_fn):
                    _register(legacy_name, legacy_fn)

    if not router:
        raise RuntimeError("no exported methods found; use decorator/explicit export rules")
    return router, method_info


def _load_callable_router(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
    dependency_path: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    entry_callable: str,
) -> Tuple[Any, Dict[str, Any], Dict[str, Tuple[str, str]]]:
    mode, methods, decorator = _normalize_export_spec(
        mode=export_mode,
        methods=export_methods,
        decorator=export_decorator,
        entry_callable=entry_callable,
    )
    key = "|".join(
        (
            artifact_path,
            entry_module,
            package_format,
            dependency_path,
            mode,
            ",".join(methods),
            decorator,
            entry_callable or "",
        )
    )
    with _ROUTER_CACHE_LOCK:
        cached = _ROUTER_CACHE.get(key)
        if cached is not None:
            return cached

    module = _load_user_module(
        artifact_path,
        entry_module=entry_module,
        package_format=package_format,
        dependency_path=dependency_path,
    )
    loaded = _build_callable_router(
        module,
        mode=mode,
        methods=methods,
        decorator=decorator,
        entry_callable=entry_callable,
    )
    loaded = (module, loaded[0], loaded[1])
    with _ROUTER_CACHE_LOCK:
        _ROUTER_CACHE[key] = loaded
    return loaded


def _discover_callable_methods(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
    dependency_path: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    entry_callable: str,
) -> Tuple[Any, Dict[str, Tuple[str, str]]]:
    mode, methods, decorator = _normalize_export_spec(
        mode=export_mode,
        methods=export_methods,
        decorator=export_decorator,
        entry_callable=entry_callable,
    )
    module = _load_user_module(
        artifact_path,
        entry_module=entry_module,
        package_format=package_format,
        dependency_path=dependency_path,
    )
    try:
        _router, method_info = _build_callable_router(
            module,
            mode=mode,
            methods=methods,
            decorator=decorator,
            entry_callable=entry_callable,
        )
        return module, method_info
    except Exception:
        _purge_loaded_artifact_modules(
            artifact_path,
            entry_module=entry_module,
            package_format=package_format,
            dependency_path=dependency_path,
        )
        raise


def _discover_callable_methods_or_raise_user_error(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
    dependency_path: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    entry_callable: str,
) -> Tuple[Any, Dict[str, Tuple[str, str]]]:
    try:
        return _discover_callable_methods(
            artifact_path,
            entry_module=entry_module,
            package_format=package_format,
            dependency_path=dependency_path,
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
            entry_callable=entry_callable,
        )
    except Exception as exc:
        if _is_user_artifact_error(exc):
            raise ValueError(
                _describe_artifact_error(
                    exc,
                    entry_module=entry_module,
                    entry_callable=entry_callable,
                    package_format=package_format,
                )
            ) from exc
        raise


def _resolve_apply_managed_globals_hook(module: Any) -> Optional[Any]:
    candidate = getattr(module, "apply_managed_globals", None)
    if candidate is None:
        return None
    if not callable(candidate):
        raise ValueError("apply_managed_globals must be callable when defined")
    return candidate


def _validate_arrow_compatible(obj: Any) -> None:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return
    if isinstance(obj, (list, tuple)):
        for item in obj:
            _validate_arrow_compatible(item)
        return
    if isinstance(obj, dict):
        for key, value in obj.items():
            _validate_arrow_compatible(key)
            _validate_arrow_compatible(value)
        return
    if is_arrow_compatible(obj):
        return
    raise TypeError(
        f"Type {type(obj).__name__} is not supported in PyCloud. "
        f"Supported types: basic types (str, int, float, bool, None), "
        f"list, tuple, dict, pd.DataFrame, pd.Series, np.ndarray (basic dtypes only). "
        f"For complex objects, please convert to JSON or use external storage."
    )


def _invoke_user_callable(fn, payload: dict):
    try:
        signature = inspect.signature(fn)
        params = list(signature.parameters.values())
    except Exception:
        params = []

    if not params:
        return fn()

    if isinstance(payload, dict) and ("args" in payload or "kwargs" in payload):
        other_keys = set(payload.keys()) - {"args", "kwargs"}
        if not other_keys:
            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})
            _validate_arrow_compatible(args)
            _validate_arrow_compatible(kwargs)
            args = convert_dict_to_arrow(args)
            kwargs = convert_dict_to_arrow(kwargs)
            if not isinstance(args, list):
                args = list(args) if args else []
            if not isinstance(kwargs, dict):
                kwargs = {}
            log_payload_flow(
                "user_invoke",
                mode="args_kwargs",
                args_summary=summarize_payload_flow_value(args),
                kwargs_summary=summarize_payload_flow_value(kwargs),
            )
            return fn(*args, **kwargs)

    if isinstance(payload, dict):
        deserialized = convert_dict_to_arrow(payload)
        log_payload_flow(
            "user_invoke",
            mode="http_kwargs",
            kwargs_summary=summarize_payload_flow_value(deserialized),
        )
        return fn(**deserialized)

    log_payload_flow(
        "user_invoke",
        mode="direct_payload",
        payload_summary=summarize_payload_flow_value(payload),
    )
    return fn(payload)


def _apply_managed_globals_to_router(
    module: Any,
    router: Dict[str, Any],
    *,
    scope_dir: str,
    globals_digest: str,
    object_dir: str,
    entry_module: str,
    method_name: str,
    session_kind: str,
) -> None:
    normalized_scope_dir = str(scope_dir or "").strip()
    normalized_digest = str(globals_digest or "").strip()
    if not normalized_scope_dir or not normalized_digest:
        return

    with _MANAGED_GLOBALS_APPLY_LOCKS_LOCK:
        apply_lock = _MANAGED_GLOBALS_APPLY_LOCKS.get(normalized_scope_dir)
        if apply_lock is None:
            apply_lock = threading.Lock()
            _MANAGED_GLOBALS_APPLY_LOCKS[normalized_scope_dir] = apply_lock

    with apply_lock:
        with _MANAGED_GLOBALS_CACHE_LOCK:
            if _MANAGED_GLOBALS_CACHE.get(normalized_scope_dir) == normalized_digest:
                return

        manifest_path = _managed_globals_manifest_path(Path(normalized_scope_dir), normalized_digest)
        if not manifest_path.exists():
            raise RuntimeError(f"managed globals manifest missing: {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8") or "{}")
        values_meta = dict(manifest.get("values") or {})

        resolved_values: Dict[str, Any] = {}
        for name, item in values_meta.items():
            if not isinstance(item, dict):
                continue
            value_digest = str(item.get("sha256", "") or "").strip()
            if not value_digest:
                continue
            value_path = _managed_globals_value_path(Path(normalized_scope_dir), value_digest=value_digest)
            if not value_path.exists():
                raise RuntimeError(f"managed globals value missing: {value_path}")
            serialized_value = json.loads(value_path.read_text(encoding="utf-8") or "null")
            resolved_value = convert_dict_to_arrow(serialized_value)
            resolved_values[name] = _resolve_object_refs_in_payload(resolved_value, object_dir=object_dir)

        apply_hook = _resolve_apply_managed_globals_hook(module)
        fallback_assign_values: Optional[Dict[str, Any]] = None
        if apply_hook is None:
            fallback_assign_values = dict(resolved_values)
        else:
            context = {
                "entry_module": str(entry_module or "").strip(),
                "session_kind": str(session_kind or "").strip(),
                "method_name": str(method_name or "").strip(),
                "globals_digest": normalized_digest,
            }
            hook_result = apply_hook(dict(resolved_values), **context)
            if hook_result is None:
                fallback_assign_values = None
            elif isinstance(hook_result, dict):
                fallback_assign_values = dict(hook_result)
            else:
                raise RuntimeError("apply_managed_globals must return None or dict")

        if fallback_assign_values:
            if apply_hook is not None:
                module_globals = getattr(module, "__dict__", None)
                if not isinstance(module_globals, dict):
                    raise RuntimeError("entry module globals are unavailable for apply_managed_globals fallback assign")
                for name, value in fallback_assign_values.items():
                    normalized_name = str(name or "").strip()
                    if not normalized_name:
                        continue
                    module_globals[normalized_name] = value
            else:
                seen_globals_ids = set()
                for fn in router.values():
                    globals_dict = getattr(fn, "__globals__", None)
                    if not isinstance(globals_dict, dict):
                        continue
                    globals_id = id(globals_dict)
                    if globals_id in seen_globals_ids:
                        continue
                    seen_globals_ids.add(globals_id)
                    for name, value in fallback_assign_values.items():
                        normalized_name = str(name or "").strip()
                        if not normalized_name:
                            continue
                        globals_dict[normalized_name] = value

        with _MANAGED_GLOBALS_CACHE_LOCK:
            _MANAGED_GLOBALS_CACHE[normalized_scope_dir] = normalized_digest


def _execute_payload_in_subprocess(
    artifact_path: str,
    entry_module: str,
    package_format: str,
    dependency_path: str,
    dependency_policy_mode: str,
    object_dir: str,
    work_dir: str,
    managed_globals_scope_dir: str,
    managed_globals_digest: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    method_name: str,
    entry_callable: str,
    payload: dict,
    warmup_only: bool = False,
    payload_mode: str = "task_submit",
) -> Tuple[str, Optional[dict], str, str, Dict[str, float]]:
    decode_start = time.perf_counter()
    decode_end = decode_start
    invoke_start = decode_start
    invoke_end = decode_start
    encode_start = decode_start
    encode_end = decode_start

    def _timings() -> Dict[str, float]:
        return {
            "decode_ms": round(max(0.0, decode_end - decode_start) * 1000.0, 3),
            "invoke_ms": round(max(0.0, invoke_end - invoke_start) * 1000.0, 3),
            "encode_ms": round(max(0.0, encode_end - encode_start) * 1000.0, 3),
        }

    try:
        with _temporary_working_dir(work_dir):
            try:
                module, router, _method_info = _load_callable_router(
                    artifact_path,
                    entry_module=entry_module,
                    package_format=package_format,
                    dependency_path=dependency_path,
                    export_mode=export_mode,
                    export_methods=export_methods,
                    export_decorator=export_decorator,
                    entry_callable=entry_callable,
                )
            except Exception as exc:
                decode_end = time.perf_counter()
                if _is_user_artifact_error(exc):
                    return (
                        "FAILED_USER",
                        None,
                        "ArtifactLoadError",
                        _describe_artifact_error(
                            exc,
                            entry_module=entry_module,
                            entry_callable=entry_callable,
                            package_format=package_format,
                            dependency_policy_mode=dependency_policy_mode,
                        ),
                        _timings(),
                    )
                return ("FAILED_INFRA", None, exc.__class__.__name__, repr(exc), _timings())
            try:
                with _temporary_import_paths(dependency_path):
                    method = str(method_name or "").strip() or str(entry_callable or "run").strip() or "run"
                    fn = router.get(method)
                    if fn is None:
                        raise RuntimeError(f"method `{method}` not exported")
                    _apply_managed_globals_to_router(
                        module,
                        router,
                        scope_dir=managed_globals_scope_dir,
                        globals_digest=managed_globals_digest,
                        object_dir=object_dir,
                        entry_module=entry_module,
                        method_name=method,
                        session_kind=("service" if str(payload_mode or "task_submit") == "http_call" else "task_pool"),
                    )
                    from pycloud_parallel.controlplane.payload_transport import normalize_inbound_payload

                    resolved_payload = normalize_inbound_payload(
                        payload,
                        object_dir=object_dir,
                        policy=get_payload_policy(str(payload_mode or "task_submit")),
                        resolve_object_refs=lambda value: _resolve_object_refs_in_payload(value, object_dir=object_dir),
                    )
                    decode_end = time.perf_counter()
                    if bool(warmup_only):
                        invoke_start = decode_end
                        invoke_end = decode_end
                        encode_start = decode_end
                        encode_end = decode_end
                        return ("SUCCEEDED", {"warmed": True, "worker_pid": os.getpid()}, "", "", _timings())
                    invoke_start = decode_end
                    ret = _invoke_user_callable(fn, resolved_payload)
                    invoke_end = time.perf_counter()
                    encode_start = invoke_end
                status_text, result, error_type, error_message = _normalize_user_return(ret, object_dir=object_dir)
                encode_end = time.perf_counter()
                return (status_text, result, error_type, error_message, _timings())
            except LargeResultError as exc:
                if decode_end <= decode_start:
                    decode_end = time.perf_counter()
                if invoke_end <= invoke_start and decode_end > decode_start:
                    invoke_end = time.perf_counter()
                    encode_start = invoke_end
                encode_end = time.perf_counter()
                return ("FAILED_USER", None, exc.__class__.__name__, str(exc), _timings())
            except ObjectResolutionError as exc:
                if decode_end <= decode_start:
                    decode_end = time.perf_counter()
                return ("FAILED_INFRA", None, exc.__class__.__name__, str(exc), _timings())
            except Exception as exc:
                now = time.perf_counter()
                if decode_end <= decode_start:
                    decode_end = now
                elif invoke_end <= invoke_start:
                    invoke_end = now
                else:
                    encode_end = now
                if isinstance(exc, (ImportError, ModuleNotFoundError)):
                    return (
                        "FAILED_USER",
                        None,
                        exc.__class__.__name__,
                        _describe_user_execution_error(exc, dependency_policy_mode=dependency_policy_mode),
                        _timings(),
                    )
                return ("FAILED_USER", None, exc.__class__.__name__, repr(exc), _timings())
    except Exception as exc:
        decode_end = time.perf_counter()
        return ("FAILED_INFRA", None, exc.__class__.__name__, repr(exc), _timings())


__all__ = [
    "ExecuteSpec",
    "_artifact_module_name",
    "_build_callable_router",
    "_build_execute_spec",
    "_build_execute_spec_model",
    "_describe_artifact_error",
    "_describe_user_execution_error",
    "_discover_callable_methods",
    "_discover_callable_methods_or_raise_user_error",
    "_execute_payload_in_subprocess",
    "_install_dependency_allowlist",
    "_invoke_user_callable",
    "_is_user_artifact_error",
    "_load_callable_router",
    "_load_user_module",
    "_missing_import_name",
    "_normalize_dependency_allowlist",
    "_normalize_export_spec",
    "_normalize_package_format",
    "_package_suffix",
    "_purge_loaded_artifact_modules",
    "_apply_managed_globals_to_router",
    "_resolve_apply_managed_globals_hook",
    "_temporary_import_paths",
    "_temporary_working_dir",
    "_validate_arrow_compatible",
    "_validate_python_runtime_or_raise",
    "service_timing_logger",
]
