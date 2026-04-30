from __future__ import annotations

"""Public artifact declaration and client-side artifact preparation helpers."""

import hashlib
import inspect
import os
from dataclasses import dataclass, replace
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, Callable, Iterable, Optional, Sequence, Tuple, Union

from pycloud_parallel.controlplane.dependency import DependencyPackager

_DEFAULT_EXPORT_DECORATOR = "pycloud_export"
_VALID_DEP_MODES = {"prebuilt", "node_preinstalled", "allow_install"}
_VALID_EXPORT_MODES = {"decorator", "explicit", "all", "single"}
_VALID_SOURCE_KINDS = {"module", "function", "path", "paths", "bytes"}
_SOURCE_UNSET = object()


def _packaging_include_tests_default() -> bool:
    return True


def _packaging_kwargs(*, synthesize_missing_package_inits: Optional[bool] = None) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "include_tests": _packaging_include_tests_default(),
    }
    if synthesize_missing_package_inits is not None:
        kwargs["synthesize_missing_package_inits"] = bool(synthesize_missing_package_inits)
    return kwargs


def _normalize_names(values: Sequence[str]) -> Tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values or ():
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return tuple(normalized)


def _normalize_dependency_policy_mode(mode: str, *, dependency_allowlist: Sequence[str] = ()) -> str:
    normalized_mode = str(mode or "").strip().lower()
    normalized_allowlist = _normalize_names(dependency_allowlist)
    if normalized_mode not in _VALID_DEP_MODES:
        return "allow_install" if normalized_allowlist else "prebuilt"
    if normalized_mode != "allow_install" and normalized_allowlist:
        raise ValueError(
            f"dependency_allowlist is only valid with dependency policy 'allow_install', got {normalized_mode!r}"
        )
    return normalized_mode


def _dependency_policy_allows_install(mode: str) -> bool:
    return _normalize_dependency_policy_mode(mode) == "allow_install"


def _package_format_from_filename(filename: str) -> str:
    lower = str(filename or "").lower()
    if lower.endswith(".tar.gz") or lower.endswith(".tgz"):
        return "tar.gz"
    if lower.endswith(".zip"):
        return "zip"
    if lower.endswith(".whl"):
        return "whl"
    if lower.endswith(".py"):
        return "py"
    return "bin"


def _resolve_package_format(package_format: str, filename: str = "", *, default: str = "bin") -> str:
    explicit = str(package_format or "").strip().lower()
    if explicit:
        return explicit
    inferred = _package_format_from_filename(filename)
    if inferred != "bin":
        return inferred
    fallback = str(default or "bin").strip().lower()
    return fallback or "bin"


def _normalize_entry_module_arg(entry_module: Any) -> str:
    if inspect.ismodule(entry_module):
        return str(getattr(entry_module, "__name__", "") or "").strip()
    return str(entry_module or "").strip()


def _normalize_entry_callable_arg(entry_callable: Any) -> str:
    if not isinstance(entry_callable, str) and callable(entry_callable):
        return str(getattr(entry_callable, "__name__", "") or "").strip()
    return str(entry_callable or "").strip()


def _infer_entry_module_from_source_file(source_file: str) -> str:
    path = Path(str(source_file or "")).resolve()
    if not path.exists() or path.suffix != ".py":
        return ""
    parts = [path.stem]
    parent = path.parent
    while (parent / "__init__.py").exists():
        parts.append(parent.name)
        parent = parent.parent
    return ".".join(reversed(parts))


def _default_entry_module_for_func(func: Callable) -> str:
    module_name = str(getattr(func, "__module__", "") or "").strip()
    try:
        source_file = inspect.getsourcefile(func) or inspect.getfile(func)
    except Exception:
        source_file = ""
    inferred = _infer_entry_module_from_source_file(str(source_file or ""))
    if module_name and module_name != "__main__" and not module_name.startswith("_pycloud_user_"):
        return module_name
    return inferred or module_name or "user_function"


def _default_entry_module_for_module(module: Any) -> str:
    module_name = str(getattr(module, "__name__", "") or "").strip()
    if module_name and module_name != "__main__":
        return module_name
    module_file = str(getattr(module, "__file__", "") or "").strip()
    inferred = _infer_entry_module_from_source_file(module_file)
    return inferred or module_name or "user_module"


def _default_entry_module_for_package(
    *,
    package_format: str,
    entry_module: Any = "",
    fallback_stem: str = "artifact",
) -> str:
    normalized_module = _normalize_entry_module_arg(entry_module).strip()
    if normalized_module:
        return normalized_module
    if _resolve_package_format(package_format, default="py") != "py":
        return ""
    return Path(
        _default_artifact_filename(
            package_format=package_format,
            entry_module="",
            fallback_stem=fallback_stem,
        )
    ).stem


def _default_artifact_filename(
    *,
    package_format: str,
    entry_module: Any = "",
    fallback_stem: str = "artifact",
) -> str:
    stem = _normalize_entry_module_arg(entry_module).split(".")[-1].strip()
    if not stem:
        stem = str(fallback_stem or "artifact").strip() or "artifact"
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._") or "artifact"

    normalized_format = _resolve_package_format(package_format, default="py")
    if normalized_format == "tar.gz":
        suffix = ".tar.gz"
    elif normalized_format == "zip":
        suffix = ".zip"
    elif normalized_format == "whl":
        suffix = ".whl"
    elif normalized_format == "py":
        suffix = ".py"
    else:
        suffix = ".bin"
    return f"{stem}{suffix}"


def _infer_entry_module_from_artifact_path(
    artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
) -> str:
    if not artifact_path:
        return ""
    if isinstance(artifact_path, (list, tuple)):
        first_path = next((Path(str(p)) for p in artifact_path if str(p)), None)
        if first_path is not None and first_path.suffix == ".py":
            return first_path.stem
        return ""
    path = Path(artifact_path)
    if path.suffix == ".py":
        return path.stem
    return ""


def _normalize_dependency_policy(deps: "ArtifactDeps | None") -> "ArtifactDeps":
    return deps if deps is not None else ArtifactDeps.prebuilt()


def _coerce_source_input(source: Any) -> Tuple[str, Any]:
    if isinstance(source, Artifact):
        return "artifact", source
    if inspect.ismodule(source):
        return "module", source
    if isinstance(source, (bytes, bytearray, memoryview)):
        return "bytes", bytes(source)
    if isinstance(source, (str, os.PathLike)):
        return "path", str(source)
    if isinstance(source, (list, tuple)):
        normalized_paths = tuple(str(item) for item in source if str(item or "").strip())
        if not normalized_paths:
            raise ValueError("source path sequence must not be empty")
        return "paths", normalized_paths
    if callable(source):
        return "function", source
    raise TypeError(
        "source must be a callable, module, bytes, path-like value, path sequence, or Artifact instance"
    )


def _coerce_artifact_deps(
    deps: "ArtifactDeps | None",
    *,
    dependency_policy_mode: str = "",
    dependency_allowlist: Sequence[str] = (),
) -> "ArtifactDeps":
    normalized_allowlist = _normalize_names(dependency_allowlist)
    requested_mode = _normalize_dependency_policy_mode(
        dependency_policy_mode,
        dependency_allowlist=normalized_allowlist,
    )
    if deps is None:
        if requested_mode == "allow_install":
            return ArtifactDeps.allow_install(normalized_allowlist)
        if requested_mode == "node_preinstalled":
            return ArtifactDeps.node_preinstalled()
        return ArtifactDeps.prebuilt()
    if not isinstance(deps, ArtifactDeps):
        raise TypeError("deps must be an ArtifactDeps instance")
    if dependency_policy_mode:
        if requested_mode != deps.mode:
            raise ValueError(
                f"deps.mode={deps.mode!r} conflicts with dependency_policy_mode={dependency_policy_mode!r}"
            )
    if normalized_allowlist and deps.dependency_allowlist != normalized_allowlist:
        raise ValueError(
            "deps.requirements conflicts with dependency_allowlist"
        )
    return deps


def _normalize_export_policy(
    exports: "ArtifactExports | None",
    *,
    consumer_kind: str,
    entry_callable: str,
) -> "ArtifactExports":
    if exports is not None:
        return exports.normalized(entry_callable=entry_callable)
    normalized_callable = str(entry_callable or "").strip() or "run"
    if str(consumer_kind or "").strip() == "task":
        return ArtifactExports.single(normalized_callable)
    return ArtifactExports.use_decorator()


@dataclass(frozen=True)
class ArtifactDeps:
    mode: str
    requirements: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        normalized_requirements = _normalize_names(self.requirements)
        normalized_mode = _normalize_dependency_policy_mode(self.mode, dependency_allowlist=normalized_requirements)
        object.__setattr__(self, "mode", normalized_mode)
        object.__setattr__(self, "requirements", normalized_requirements)

    @classmethod
    def prebuilt(cls) -> "ArtifactDeps":
        return cls(mode="prebuilt")

    @classmethod
    def node_preinstalled(cls) -> "ArtifactDeps":
        return cls(mode="node_preinstalled")

    @classmethod
    def allow_install(cls, requirements: Sequence[str]) -> "ArtifactDeps":
        return cls(mode="allow_install", requirements=tuple(requirements or ()))

    @property
    def dependency_allowlist(self) -> Tuple[str, ...]:
        if self.mode != "allow_install":
            return ()
        return self.requirements


@dataclass(frozen=True)
class ArtifactExports:
    mode: str
    methods: Tuple[str, ...] = ()
    decorator: str = _DEFAULT_EXPORT_DECORATOR

    def __post_init__(self) -> None:
        normalized_mode = str(self.mode or "").strip().lower()
        if normalized_mode not in _VALID_EXPORT_MODES:
            raise ValueError(f"unsupported artifact export mode: {self.mode!r}")
        object.__setattr__(self, "mode", normalized_mode)
        object.__setattr__(self, "methods", _normalize_names(self.methods))
        object.__setattr__(self, "decorator", str(self.decorator or _DEFAULT_EXPORT_DECORATOR).strip() or _DEFAULT_EXPORT_DECORATOR)

    @classmethod
    def use_decorator(cls, decorator: str = _DEFAULT_EXPORT_DECORATOR) -> "ArtifactExports":
        return cls(mode="decorator", decorator=decorator)

    @classmethod
    def single(cls, method: str) -> "ArtifactExports":
        normalized_method = str(method or "").strip() or "run"
        return cls(mode="single", methods=(normalized_method,))

    @classmethod
    def explicit(cls, methods: Sequence[str]) -> "ArtifactExports":
        return cls(mode="explicit", methods=tuple(methods or ()))

    @classmethod
    def export_all(cls) -> "ArtifactExports":
        return cls(mode="all")

    def normalized(self, *, entry_callable: str) -> "ArtifactExports":
        normalized_callable = str(entry_callable or "").strip() or "run"
        if self.mode == "single":
            return replace(self, methods=(normalized_callable,))
        return self


@dataclass(frozen=True)
class _ArtifactPathsSource:
    root_dir: str = ""
    paths: Tuple[str, ...] = ()
    mode: str = "paths"

    def __post_init__(self) -> None:
        normalized_mode = str(self.mode or "paths").strip().lower()
        if normalized_mode not in {"paths", "roots"}:
            raise ValueError(f"unsupported artifact paths source mode: {self.mode!r}")
        normalized_paths = tuple(str(path or "").strip() for path in self.paths if str(path or "").strip())
        if not normalized_paths:
            raise ValueError("artifact paths source is empty")
        object.__setattr__(self, "root_dir", str(self.root_dir or "").strip())
        object.__setattr__(self, "paths", normalized_paths)
        object.__setattr__(self, "mode", normalized_mode)


@dataclass(frozen=True)
class Artifact:
    source_kind: str
    source_value: object
    runtime: str = "py3"
    entry_module: str = ""
    entry_callable: str = "run"
    package_format: str = ""
    exports: Optional[ArtifactExports] = None
    deps: Optional[ArtifactDeps] = None
    managed_global_names: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        normalized_kind = str(self.source_kind or "").strip().lower()
        if normalized_kind not in _VALID_SOURCE_KINDS:
            raise ValueError(f"unsupported artifact source kind: {self.source_kind!r}")
        object.__setattr__(self, "source_kind", normalized_kind)
        object.__setattr__(self, "runtime", str(self.runtime or "py3").strip() or "py3")
        object.__setattr__(self, "entry_module", _normalize_entry_module_arg(self.entry_module))
        object.__setattr__(self, "entry_callable", _normalize_entry_callable_arg(self.entry_callable) or "run")
        object.__setattr__(self, "package_format", str(self.package_format or "").strip())
        object.__setattr__(self, "managed_global_names", _normalize_names(self.managed_global_names))

    @classmethod
    def from_module(cls, module: Any, **kwargs) -> "Artifact":
        return cls(source_kind="module", source_value=module, **kwargs)

    @classmethod
    def from_function(cls, func: Callable, **kwargs) -> "Artifact":
        normalized = dict(kwargs)
        if not str(normalized.get("entry_callable", "") or "").strip():
            normalized["entry_callable"] = str(getattr(func, "__name__", "") or "").strip() or "run"
        return cls(source_kind="function", source_value=func, **normalized)

    @classmethod
    def from_paths(
        cls,
        root_or_paths: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]],
        paths: Optional[Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]]] = None,
        **kwargs,
    ) -> "Artifact":
        def _normalize_paths_input(value: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]]) -> Tuple[str, ...]:
            if isinstance(value, (str, os.PathLike)):
                normalized = (str(value),)
            elif isinstance(value, (list, tuple)):
                normalized = tuple(str(path) for path in value if str(path or "").strip())
            else:
                raise TypeError("artifact paths must be a path or a sequence of paths")
            normalized = tuple(path for path in normalized if str(path or "").strip())
            if not normalized:
                raise ValueError("artifact paths source is empty")
            return normalized

        if paths is None:
            source = _ArtifactPathsSource(
                root_dir="",
                paths=_normalize_paths_input(root_or_paths),
                mode="roots",
            )
        else:
            source = _ArtifactPathsSource(
                root_dir=str(root_or_paths),
                paths=_normalize_paths_input(paths),
                mode="paths",
            )
        return cls(source_kind="paths", source_value=source, **kwargs)

    @classmethod
    def from_bytes(cls, blob: bytes, *, package_format: str, **kwargs) -> "Artifact":
        if blob is None:
            raise ValueError("blob is required")
        if not str(package_format or "").strip():
            raise ValueError("package_format is required for Artifact.from_bytes")
        return cls(source_kind="bytes", source_value=bytes(blob), package_format=package_format, **kwargs)


@dataclass(frozen=True)
class PreparedArtifact:
    blob: bytes
    filename: str
    package_format: str
    runtime: str
    entry_module: str
    entry_callable: str
    export_mode: str
    export_methods: Tuple[str, ...]
    export_decorator: str
    dependency_policy: ArtifactDeps
    managed_global_names: Tuple[str, ...]
    content_sha256: str
    code_version: str

    @property
    def dependency_allowlist(self) -> Tuple[str, ...]:
        return self.dependency_policy.dependency_allowlist

    @property
    def dependency_policy_mode(self) -> str:
        return self.dependency_policy.mode


def _read_temp_file(path: str) -> bytes:
    with open(path, "rb") as fp:
        return fp.read()


def _package_module_blob(module: Any) -> Tuple[bytes, str]:
    tmp_path = DependencyPackager().package_module(module, **_packaging_kwargs())
    try:
        return _read_temp_file(tmp_path), f"{_default_entry_module_for_module(module)}.tar.gz"
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _package_function_blob(func: Callable) -> Tuple[bytes, str]:
    tmp_path = DependencyPackager().package_function(func, **_packaging_kwargs())
    try:
        return _read_temp_file(tmp_path), f"{str(getattr(func, '__module__', '') or 'artifact')}_{str(getattr(func, '__name__', '') or 'run')}.tar.gz"
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _package_roots_blob(paths: Iterable[Path]) -> Tuple[bytes, str]:
    tmp_path = DependencyPackager().package_roots(
        paths,
        **_packaging_kwargs(synthesize_missing_package_inits=True),
    )
    try:
        return _read_temp_file(tmp_path), "artifact_paths.tar.gz"
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _package_relative_paths_blob(source: _ArtifactPathsSource) -> Tuple[bytes, str]:
    tmp_path = DependencyPackager().package_paths(
        root_dir=source.root_dir,
        paths=source.paths,
        **_packaging_kwargs(synthesize_missing_package_inits=True),
    )
    try:
        root_name = Path(source.root_dir).name or "artifact_paths"
        return _read_temp_file(tmp_path), f"{root_name}.tar.gz"
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _prepare_artifact_blob(artifact: Artifact) -> Tuple[bytes, str]:
    if artifact.source_kind == "module":
        if not inspect.ismodule(artifact.source_value):
            raise TypeError("Artifact(source_kind='module') requires a module object")
        return _package_module_blob(artifact.source_value)

    if artifact.source_kind == "function":
        if not callable(artifact.source_value):
            raise TypeError("Artifact(source_kind='function') requires a callable")
        return _package_function_blob(artifact.source_value)

    if artifact.source_kind == "bytes":
        return bytes(artifact.source_value), ""

    if artifact.source_kind == "path":
        path = Path(str(artifact.source_value)).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"artifact path not found: {path}")
        if path.is_dir():
            tmp_path = DependencyPackager().package_roots(
                [path],
                **_packaging_kwargs(synthesize_missing_package_inits=True),
            )
            try:
                return _read_temp_file(tmp_path), f"{path.name}.tar.gz"
            finally:
                Path(tmp_path).unlink(missing_ok=True)
        return path.read_bytes(), path.name

    if artifact.source_kind == "paths":
        if not isinstance(artifact.source_value, _ArtifactPathsSource):
            raise TypeError("Artifact(source_kind='paths') requires an ArtifactPaths source")
        if artifact.source_value.mode == "paths":
            return _package_relative_paths_blob(artifact.source_value)
        return _package_roots_blob(Path(str(path)).expanduser() for path in artifact.source_value.paths)

    raise ValueError(f"unsupported artifact source kind: {artifact.source_kind}")


def _infer_entry_defaults(artifact: Artifact, *, package_format: str, filename: str) -> Tuple[str, str]:
    entry_module = _normalize_entry_module_arg(artifact.entry_module)
    entry_callable = _normalize_entry_callable_arg(artifact.entry_callable) or "run"

    if artifact.source_kind == "module" and not entry_module:
        entry_module = _default_entry_module_for_module(artifact.source_value)
    elif artifact.source_kind == "function":
        if not entry_module:
            entry_module = _default_entry_module_for_func(artifact.source_value)
        if entry_callable == "run":
            inferred_callable = str(getattr(artifact.source_value, "__name__", "") or "").strip()
            if inferred_callable:
                entry_callable = inferred_callable
    elif artifact.source_kind == "path" and not entry_module:
        entry_module = _infer_entry_module_from_artifact_path(str(artifact.source_value))
    elif artifact.source_kind == "paths" and not entry_module and isinstance(artifact.source_value, _ArtifactPathsSource):
        if artifact.source_value.mode == "paths" and len(artifact.source_value.paths) == 1:
            entry_module = _infer_entry_module_from_artifact_path(artifact.source_value.paths[0])

    if not entry_module and package_format == "py":
        fallback_stem = Path(filename).stem if filename else "artifact"
        entry_module = _default_entry_module_for_package(
            package_format=package_format,
            entry_module="",
            fallback_stem=fallback_stem,
        )
    return entry_module, entry_callable


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
    from pycloud_parallel.controlplane.code_version import _code_version_from_digest as state_code_version_from_digest

    return state_code_version_from_digest(
        digest,
        runtime=runtime,
        entry_module=entry_module,
        entry_callable=entry_callable,
        package_format=package_format,
        export_mode=export_mode,
        export_methods=export_methods,
        export_decorator=export_decorator,
        dependency_policy_mode=dependency_policy_mode,
        dependency_allowlist=dependency_allowlist,
    )


def _exports_from_policy(
    *,
    consumer_kind: str,
    export_mode: str = "",
    export_methods: Optional[Sequence[str]] = None,
    entry_callable: str = "run",
) -> ArtifactExports:
    normalized_mode = str(export_mode or "").strip().lower()
    methods = _normalize_names(export_methods or ())
    if normalized_mode not in _VALID_EXPORT_MODES:
        normalized_mode = ""
    if not normalized_mode:
        if methods:
            normalized_mode = "explicit"
        elif str(consumer_kind or "").strip() in {"task", "job"}:
            normalized_mode = "single"
        else:
            normalized_mode = "decorator"
    if normalized_mode == "single":
        return ArtifactExports.single(str(entry_callable or "").strip() or "run")
    if normalized_mode == "explicit":
        return ArtifactExports.explicit(methods)
    if normalized_mode == "all":
        return ArtifactExports.export_all()
    return ArtifactExports.use_decorator()


def _normalize_artifact_input(
    *,
    consumer_kind: str,
    source: Any = _SOURCE_UNSET,
    artifact: Optional[Artifact] = None,
    deps: Optional[ArtifactDeps] = None,
    func: Optional[Callable] = None,
    artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
    blob: Optional[bytes] = None,
    runtime: str = "py3",
    entry_module: Any = "",
    entry_callable: Any = "run",
    package_format: str = "",
    export_mode: str = "",
    export_methods: Optional[Sequence[str]] = None,
    dependency_allowlist: Optional[Sequence[str]] = None,
    managed_global_names: Optional[Sequence[str]] = None,
) -> Artifact:
    normalized_consumer = str(consumer_kind or "").strip().lower()
    if normalized_consumer not in {"service", "task", "job"}:
        raise ValueError(f"unsupported artifact consumer kind: {consumer_kind!r}")

    if source is not _SOURCE_UNSET and source is not None:
        source_kind, source_value = _coerce_source_input(source)
        if source_kind == "artifact":
            if artifact is not None:
                raise ValueError("source Artifact cannot be combined with artifact=")
            artifact = source_value
        else:
            if artifact is not None or func is not None or blob is not None or artifact_path:
                raise ValueError("source cannot be combined with artifact, func, blob, or artifact_path")
            if inspect.ismodule(entry_module) or (not isinstance(entry_callable, str) and callable(entry_callable)):
                raise ValueError("source cannot be combined with module/callable entry inputs")
            if source_kind == "function":
                func = source_value
            elif source_kind == "module":
                entry_module = source_value
            elif source_kind == "bytes":
                blob = source_value
            elif source_kind == "paths":
                artifact_path = list(source_value)
            else:
                artifact_path = source_value

    raw_entry_module = entry_module
    raw_entry_callable = entry_callable
    source_module = None
    if blob is None and not artifact_path and inspect.ismodule(raw_entry_module):
        source_module = raw_entry_module
    source_func = func
    if source_func is None and blob is None and not artifact_path and not isinstance(raw_entry_callable, str) and callable(raw_entry_callable):
        source_func = raw_entry_callable

    normalized_entry_module = _normalize_entry_module_arg(raw_entry_module)
    normalized_entry_callable = _normalize_entry_callable_arg(raw_entry_callable) or "run"

    if artifact is not None:
        if not isinstance(artifact, Artifact):
            raise TypeError("artifact must be an Artifact instance")
        if func is not None or blob is not None or artifact_path or source_module is not None or source_func is not None:
            raise ValueError("artifact cannot be combined with alternate artifact source inputs")
        effective_exports = artifact.exports
        if effective_exports is None:
            effective_exports = _exports_from_policy(
                consumer_kind=normalized_consumer,
                export_mode=export_mode,
                export_methods=export_methods,
                entry_callable=normalized_entry_callable or artifact.entry_callable,
            )
        effective_deps = _coerce_artifact_deps(
            deps or artifact.deps,
            dependency_allowlist=dependency_allowlist or (),
        )
        merged_globals = _normalize_names([*artifact.managed_global_names, *(managed_global_names or ())])
        return replace(
            artifact,
            exports=effective_exports,
            deps=effective_deps,
            managed_global_names=merged_globals,
        )

    resolved_deps = _coerce_artifact_deps(
        deps,
        dependency_allowlist=dependency_allowlist or (),
    )
    exports = _exports_from_policy(
        consumer_kind=normalized_consumer,
        export_mode=export_mode,
        export_methods=export_methods,
        entry_callable=normalized_entry_callable,
    )
    base_kwargs = {
        "runtime": runtime,
        "entry_module": normalized_entry_module,
        "entry_callable": normalized_entry_callable,
        "package_format": package_format,
        "exports": exports,
        "deps": resolved_deps,
        "managed_global_names": tuple(managed_global_names or ()),
    }
    if source_func is not None:
        return Artifact.from_function(source_func, **base_kwargs)
    if source_module is not None:
        return Artifact.from_module(source_module, **base_kwargs)
    if blob is not None:
        effective_format = _resolve_package_format(package_format, default="py")
        return Artifact.from_bytes(blob, package_format=effective_format, **{k: v for k, v in base_kwargs.items() if k != "package_format"})
    if artifact_path:
        return Artifact.from_paths(artifact_path, **base_kwargs)
    raise ValueError(
        "blob, func, artifact_path, module-object entry_module or callable-object entry_callable is required"
    )


def _prepare_artifact(
    artifact: Artifact,
    *,
    consumer_kind: str,
) -> PreparedArtifact:
    normalized_consumer = str(consumer_kind or "").strip().lower()
    if normalized_consumer not in {"service", "task", "job"}:
        raise ValueError(f"unsupported artifact consumer kind: {consumer_kind!r}")
    blob, filename = _prepare_artifact_blob(artifact)
    package_format = _resolve_package_format(artifact.package_format, filename, default="py")
    entry_module, entry_callable = _infer_entry_defaults(artifact, package_format=package_format, filename=filename)
    if not filename:
        if normalized_consumer == "service":
            fallback_stem = "service_artifact"
        elif normalized_consumer == "job":
            fallback_stem = "job_artifact"
        else:
            fallback_stem = "task_pool_artifact"
        filename = _default_artifact_filename(
            package_format=package_format,
            entry_module=entry_module,
            fallback_stem=fallback_stem,
        )
    exports = _normalize_export_policy(
        artifact.exports,
        consumer_kind=normalized_consumer,
        entry_callable=entry_callable,
    )
    deps = _normalize_dependency_policy(artifact.deps)
    content_sha256 = hashlib.sha256(blob).hexdigest()
    code_version = _code_version_from_digest(
        content_sha256,
        runtime=artifact.runtime,
        entry_module=entry_module,
        entry_callable=entry_callable,
        package_format=package_format,
        export_mode=exports.mode,
        export_methods=exports.methods,
        export_decorator=exports.decorator,
        dependency_policy_mode=deps.mode,
        dependency_allowlist=deps.dependency_allowlist,
    )
    return PreparedArtifact(
        blob=blob,
        filename=filename,
        package_format=package_format,
        runtime=artifact.runtime,
        entry_module=entry_module,
        entry_callable=entry_callable,
        export_mode=exports.mode,
        export_methods=exports.methods,
        export_decorator=exports.decorator,
        dependency_policy=deps,
        managed_global_names=_normalize_names(artifact.managed_global_names),
        content_sha256=content_sha256,
        code_version=code_version,
    )


__all__ = [
    "Artifact",
    "ArtifactDeps",
    "ArtifactExports",
    "PreparedArtifact",
]
