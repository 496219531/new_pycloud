"""
本地源码依赖分析与打包系统。

当前能力边界：
1. 自动收集本地源码模块与 package 资源
2. 保留 package 目录结构
3. 第三方依赖不自动打包，建议显式使用 dependency_allowlist
"""

from __future__ import annotations

import ast
import gzip
import hashlib
import importlib
import importlib.util
import inspect
import io
import json
import os
import shutil
import sys
import tempfile
import sysconfig
import importlib.machinery
import tarfile
import threading
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple


@dataclass(frozen=True)
class _TarSourceEntry:
    arcname: str
    source_path: Optional[Path] = None
    data: bytes = b""


_PACKAGED_PYTHON_FILE_SUFFIXES = frozenset({".py", ".pyd", ".so"})
_RUNTIME_PACKAGE_ROOTS = frozenset({"pycloud_parallel"})
_PACKAGE_CACHE_VERSION = 1
_PACKAGE_CACHE_LOCK = threading.Lock()
_PACKAGE_CACHE_DEFAULT_MAX_ENTRIES = 128
_PACKAGE_CACHE_DEFAULT_MAX_BYTES = 1024 * 1024 * 1024


def _normalize_arcname(arcname: Path | str) -> str:
    parts = [part for part in Path(str(arcname)).parts if part not in ("", ".", os.sep)]
    return str(PurePosixPath(*parts))


def _normalized_file_mode(path: Path) -> int:
    try:
        return 0o755 if os.access(path, os.X_OK) else 0o644
    except Exception:
        return 0o644


def _should_skip_packaged_path(path: Path, *, include_tests: bool = True) -> bool:
    lowered_parts = {part.lower() for part in path.parts}
    if "__pycache__" in lowered_parts:
        return True
    name = path.name.lower()
    if name.endswith((".pyc", ".pyo")):
        return True
    if include_tests:
        return False
    if name.startswith("test_") or name.endswith("_test.py"):
        return True
    if "tests" in lowered_parts or "test" in lowered_parts:
        return True
    return False


def _is_packaged_python_file(path: Path, *, include_tests: bool = True) -> bool:
    normalized = Path(path)
    if not normalized.exists() or not normalized.is_file():
        return False
    if normalized.is_symlink():
        return False
    if _should_skip_packaged_path(normalized, include_tests=include_tests):
        return False
    return normalized.suffix.lower() in _PACKAGED_PYTHON_FILE_SUFFIXES


def _new_temp_targz_path(*, prefix: str) -> str:
    fd, output_file = tempfile.mkstemp(suffix=".tar.gz", prefix=prefix)
    os.close(fd)
    return output_file


def _package_cache_root_dir() -> Optional[Path]:
    raw = str(os.environ.get("PYCLOUD_PACKAGE_CACHE_DIR", "") or "").strip()
    if raw.lower() in {"0", "false", "no", "off", "disabled"}:
        return None
    if raw:
        return Path(raw).expanduser().resolve()
    home = str(os.environ.get("PYCLOUD_HOME", "") or "").strip()
    if home:
        return (Path(home).expanduser().resolve() / "package_cache").resolve()
    return (Path.home() / ".pycloud_parallel" / "package_cache").resolve()


def _entry_fingerprint(entry: "_TarSourceEntry") -> Dict[str, object]:
    arcname = _normalize_arcname(entry.arcname)
    if entry.source_path is None:
        data = bytes(entry.data or b"")
        return {
            "arcname": arcname,
            "kind": "data",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    path = Path(entry.source_path).resolve()
    stat = path.stat()
    return {
        "arcname": arcname,
        "kind": "file",
        "path": str(path),
        "mode": _normalized_file_mode(path),
        "size": int(stat.st_size),
        "mtime_ns": int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
    }


def _entries_cache_key(entries: Iterable["_TarSourceEntry"], *, cache_scope: str) -> str:
    payload = {
        "version": _PACKAGE_CACHE_VERSION,
        "scope": str(cache_scope or ""),
        "entries": [_entry_fingerprint(entry) for entry in entries],
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _copy_cached_targz_if_available(cache_key: str, output_file: str) -> bool:
    root = _package_cache_root_dir()
    if root is None:
        return False
    cached_path = root / f"{cache_key}.tar.gz"
    if not cached_path.exists() or not cached_path.is_file():
        return False
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copyfile(str(cached_path), str(output_file))
    except FileNotFoundError:
        return False
    return True


def _store_cached_targz(cache_key: str, output_file: str) -> None:
    root = _package_cache_root_dir()
    if root is None:
        return
    root.mkdir(parents=True, exist_ok=True)
    cached_path = root / f"{cache_key}.tar.gz"
    if cached_path.exists():
        return
    fd, tmp_name = tempfile.mkstemp(prefix=f".{cache_key}.", suffix=".tmp", dir=str(root))
    os.close(fd)
    tmp_path = Path(tmp_name)
    shutil.copyfile(str(output_file), str(tmp_path))
    os.replace(str(tmp_path), str(cached_path))
    _prune_package_cache(root)


def _positive_env_int(name: str, default: int) -> int:
    try:
        return max(1, int(str(os.getenv(name, "") or default)))
    except (TypeError, ValueError):
        return max(1, int(default))


def _prune_package_cache(root: Path) -> None:
    max_entries = _positive_env_int("PYCLOUD_PACKAGE_CACHE_MAX_ENTRIES", _PACKAGE_CACHE_DEFAULT_MAX_ENTRIES)
    max_bytes = _positive_env_int("PYCLOUD_PACKAGE_CACHE_MAX_BYTES", _PACKAGE_CACHE_DEFAULT_MAX_BYTES)
    entries = []
    total_bytes = 0
    for path in root.glob("*.tar.gz"):
        try:
            stat = path.stat()
        except OSError:
            continue
        size = max(0, int(stat.st_size))
        total_bytes += size
        entries.append((float(stat.st_mtime), path, size))
    entries.sort(key=lambda item: item[0])
    while entries and (len(entries) > max_entries or total_bytes > max_bytes):
        _mtime, path, size = entries.pop(0)
        try:
            path.unlink()
        except OSError:
            continue
        total_bytes = max(0, total_bytes - size)


def _write_cached_deterministic_targz(entries: Iterable["_TarSourceEntry"], output_file: str, *, cache_scope: str) -> None:
    materialized_entries = list(entries)
    cache_key = _entries_cache_key(materialized_entries, cache_scope=cache_scope)
    with _PACKAGE_CACHE_LOCK:
        if _copy_cached_targz_if_available(cache_key, output_file):
            return
    _write_deterministic_targz(materialized_entries, output_file)
    with _PACKAGE_CACHE_LOCK:
        if _copy_cached_targz_if_available(cache_key, output_file):
            return
        _store_cached_targz(cache_key, output_file)


def _iter_directory_entries(
    dir_path: Path,
    *,
    include_tests: bool = True,
    prefix: Path | None = None,
    synthesize_missing_package_inits: bool = False,
) -> List[_TarSourceEntry]:
    root = Path(dir_path).resolve()
    effective_prefix = Path(prefix) if prefix is not None else Path()
    entries: List[_TarSourceEntry] = []
    package_files = [
        path
        for path in root.rglob("*")
        if _is_packaged_python_file(path, include_tests=include_tests)
    ]

    if synthesize_missing_package_inits:
        dirs_to_check = {path.parent for path in package_files}
        if prefix is not None:
            dirs_to_check.add(root)
        for d in sorted(dirs_to_check, key=lambda item: str(item.relative_to(root))):
            synthetic_path = d / "__init__.py"
            if synthetic_path.exists():
                continue
            if _should_skip_packaged_path(synthetic_path, include_tests=include_tests):
                continue
            arcname = effective_prefix / d.relative_to(root) / "__init__.py"
            entries.append(_TarSourceEntry(arcname=_normalize_arcname(arcname), data=b""))

    for file_path in sorted(package_files, key=lambda item: str(item.relative_to(root))):
        arcname = effective_prefix / file_path.relative_to(root)
        entries.append(_TarSourceEntry(arcname=_normalize_arcname(arcname), source_path=file_path))
    return entries


def _iter_roots_entries(
    roots: Iterable[Path],
    *,
    include_tests: bool = True,
    synthesize_missing_package_inits: bool = False,
) -> List[_TarSourceEntry]:
    entries: List[_TarSourceEntry] = []
    for root in sorted((Path(item).resolve() for item in roots), key=lambda item: str(item)):
        if not root.exists():
            continue
        if root.is_dir():
            entries.extend(
                _iter_directory_entries(
                    root,
                    include_tests=include_tests,
                    prefix=Path(root.name),
                    synthesize_missing_package_inits=synthesize_missing_package_inits,
                )
            )
            continue
        if not _is_packaged_python_file(root, include_tests=include_tests):
            continue
        entries.append(_TarSourceEntry(arcname=_normalize_arcname(root.name), source_path=root))
    return entries


def _iter_relative_path_entries(
    *,
    root_dir: Path,
    paths: Iterable[str | os.PathLike[str]],
    include_tests: bool = True,
    synthesize_missing_package_inits: bool = False,
) -> List[_TarSourceEntry]:
    root = Path(root_dir).resolve()
    normalized: List[Path] = []
    for item in paths:
        p = (root / str(item)).resolve()
        if not p.exists():
            raise FileNotFoundError(f"path not found: {item}")
        if p != root and root not in p.parents:
            raise ValueError(f"path escapes root_dir: {item}")
        normalized.append(p)

    entries: List[_TarSourceEntry] = []
    for p in sorted(normalized, key=lambda item: str(item.relative_to(root))):
        rel = p.relative_to(root)
        if p.is_dir():
            entries.extend(
                _iter_directory_entries(
                    p,
                    include_tests=include_tests,
                    prefix=Path(rel),
                    synthesize_missing_package_inits=synthesize_missing_package_inits,
                )
            )
            continue
        if not _is_packaged_python_file(p, include_tests=include_tests):
            continue
        entries.append(_TarSourceEntry(arcname=_normalize_arcname(rel), source_path=p))
    return entries


def _write_deterministic_targz(entries: Iterable[_TarSourceEntry], output_file: str) -> None:
    deduped: Dict[str, _TarSourceEntry] = {}
    for entry in entries:
        arcname = _normalize_arcname(entry.arcname)
        if not arcname:
            continue
        deduped[arcname] = _TarSourceEntry(arcname=arcname, source_path=entry.source_path, data=entry.data)

    with open(output_file, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            with tarfile.open(fileobj=gz, mode="w") as tar:
                for arcname in sorted(deduped):
                    entry = deduped[arcname]
                    if entry.source_path is not None:
                        source_path = Path(entry.source_path)
                        info = tar.gettarinfo(str(source_path), arcname=arcname)
                        if not info.isfile():
                            continue
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        info.mtime = 0
                        info.mode = _normalized_file_mode(source_path)
                        info.pax_headers = {}
                        with source_path.open("rb") as fh:
                            tar.addfile(info, fh)
                        continue

                    data = bytes(entry.data or b"")
                    info = tarfile.TarInfo(name=arcname)
                    info.size = len(data)
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    info.mtime = 0
                    info.mode = 0o644
                    info.type = tarfile.REGTYPE
                    info.pax_headers = {}
                    tar.addfile(info, io.BytesIO(data))


def _synthesize_package_init_entries(entries: Iterable[_TarSourceEntry]) -> List[_TarSourceEntry]:
    deduped: Dict[str, _TarSourceEntry] = {}
    for entry in entries:
        arcname = _normalize_arcname(entry.arcname)
        if not arcname:
            continue
        deduped[arcname] = _TarSourceEntry(arcname=arcname, source_path=entry.source_path, data=entry.data)

    synthetic_names: Set[str] = set()
    for arcname in list(deduped):
        path = PurePosixPath(arcname)
        for parent in path.parents:
            if str(parent) in ("", "."):
                continue
            init_arcname = _normalize_arcname(parent / "__init__.py")
            if init_arcname not in deduped:
                synthetic_names.add(init_arcname)

    for init_arcname in sorted(synthetic_names):
        deduped[init_arcname] = _TarSourceEntry(arcname=init_arcname, data=b"")
    return [deduped[key] for key in sorted(deduped)]


class DependencyAnalyzer:
    """依赖分析器"""

    def __init__(self):
        self.stdlib_modules = self._get_stdlib_modules()

    def _get_stdlib_modules(self) -> Set[str]:
        """获取 Python 标准库模块列表"""
        # Python 3.10+ 有 stdlib_module_names
        if hasattr(sys, 'stdlib_module_names'):
            return set(sys.stdlib_module_names)

        # 备用方法：从 sys.path 猜测
        stdlib_paths = set()
        for key in ("stdlib", "platstdlib"):
            value = sysconfig.get_paths().get(key)
            if value:
                stdlib_paths.add(Path(value))
        stdlib_paths.update(
            {
                Path(sys.prefix) / "lib",
                Path(sys.base_prefix) / "lib",
                Path(sys.exec_prefix) / "lib",
                Path(sys.prefix) / "Lib",
                Path(sys.base_prefix) / "Lib",
                Path(sys.exec_prefix) / "Lib",
            }
        )
        stdlib_modules = set()

        for name, module in sys.modules.items():
            if module is None:
                continue
            try:
                module_file = getattr(module, '__file__', '')
                if module_file and any(stdlib in Path(module_file).parents for stdlib in stdlib_paths):
                    stdlib_modules.add(name.split('.')[0])
            except Exception:
                pass

        return stdlib_modules

    def analyze_function(self, func: Callable) -> Dict[str, Any]:
        """分析函数的所有依赖

        Args:
            func: 要分析的函数

        Returns:
            依赖信息字典
        """
        result = {
            "function_name": func.__name__,
            "module": func.__module__,
            "file": None,
            "source_file": None,
            "imports": [],
            "local_files": [],
            "stdlib_modules": [],
            "third_party_modules": [],
            "local_modules": [],
        }

        # 获取函数所在文件
        module = None
        try:
            module = importlib.import_module(func.__module__)
            result["file"] = getattr(module, '__file__', None)
            result["source_file"] = inspect.getsourcefile(func)
        except (ImportError, AttributeError):
            pass

        module_source = self._get_module_source(func)
        if module_source:
            result["imports"] = self._extract_imports_from_source(module_source)

        if module is not None:
            module_infos = self._collect_loaded_module_infos(module, root_source=module_source)
            root_name = str(getattr(module, "__name__", "") or "").strip()
            root_file = str(result.get("source_file") or result.get("file") or "").strip()
            for item in module_infos:
                item_name = str(item.get("name", "") or "").strip()
                item_file = str(item.get("file", "") or "").strip()
                if not item_name or not item_file:
                    continue
                if item_name == root_name and item_file == root_file:
                    continue
                result["local_modules"].append(item)
                result["local_files"].append(item_file)

        return result

    def analyze_module(self, module_name: str | ModuleType) -> Dict[str, Any]:
        """分析模块的所有依赖

        Args:
            module_name: 模块名或已加载的模块对象

        Returns:
            依赖信息字典
        """
        try:
            module = module_name if inspect.ismodule(module_name) else importlib.import_module(str(module_name))
        except ImportError as e:
            return {
                "error": f"无法导入模块: {e}",
                "module_name": str(module_name),
            }

        normalized_module_name = str(getattr(module, "__name__", "") or module_name or "").strip()
        result = {
            "module_name": normalized_module_name,
            "file": getattr(module, '__file__', None),
            "imports": [],
            "local_files": [],
            "stdlib_modules": [],
            "third_party_modules": [],
            "local_modules": [],
        }

        # 获取模块源码
        module_file = getattr(module, '__file__', None)
        source = self._read_module_source(module_file)
        if source:
            result["imports"] = self._extract_imports_from_source(source)
        elif module_file and str(module_file).endswith('.py'):
            result["error"] = "读取模块文件失败"

        module_infos = self._collect_loaded_module_infos(module, root_source=source)
        root_file = str(result.get("file") or "").strip()
        for item in module_infos:
            item_name = str(item.get("name", "") or "").strip()
            item_file = str(item.get("file", "") or "").strip()
            if not item_name or not item_file:
                continue
            if item_name == normalized_module_name and item_file == root_file:
                continue
            result["local_modules"].append(item)
            result["local_files"].append(item_file)

        return result

    def _get_module_source(self, func: Callable) -> Optional[str]:
        """获取函数所在模块的源码"""
        try:
            # 获取函数所在模块的文件路径
            module_file = inspect.getsourcefile(func)
            if not module_file:
                return None

            # 读取整个模块的源码
            with open(module_file, 'r', encoding='utf-8') as f:
                return f.read()
        except (OSError, TypeError):
            return None

    def _read_module_source(self, module_file: str | os.PathLike[str] | None) -> str:
        path = Path(str(module_file or "")).resolve()
        if not path.exists() or path.suffix.lower() != ".py":
            return ""
        try:
            with path.open("r", encoding="utf-8") as fh:
                return fh.read()
        except Exception:
            return ""

    def _collect_loaded_module_infos(
        self,
        module: ModuleType,
        *,
        root_source: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        pending: List[Tuple[ModuleType, Optional[str]]] = [(module, root_source)]
        seen_names: Set[str] = set()
        collected: List[Dict[str, str]] = []

        while pending:
            current_module, current_source = pending.pop()
            current_name = str(getattr(current_module, "__name__", "") or "").strip()
            if not current_name or current_name in seen_names:
                continue
            seen_names.add(current_name)

            current_file = self._module_file_from_object(current_module)
            if not current_file or not self._is_local_module(current_file):
                continue

            current_path = Path(current_file).resolve()
            if not _is_packaged_python_file(current_path):
                continue
            collected.append({
                "name": current_name,
                "file": str(current_path),
            })

            for parent_module in reversed(self._iter_parent_package_modules(current_module)):
                parent_name = str(getattr(parent_module, "__name__", "") or "").strip()
                if parent_name and parent_name not in seen_names:
                    pending.append((parent_module, None))

            source = current_source if current_source is not None else self._read_module_source(current_path)
            if not source:
                continue

            imports_ast = self._extract_imports_from_source(source)
            for imported_module in reversed(self._resolve_imported_modules(current_module, imports_ast)):
                imported_name = str(getattr(imported_module, "__name__", "") or "").strip()
                if imported_name and imported_name not in seen_names:
                    pending.append((imported_module, None))

        return collected

    def _iter_parent_package_modules(self, module: ModuleType) -> List[ModuleType]:
        module_name = str(getattr(module, "__name__", "") or "").strip()
        if not module_name or "." not in module_name:
            return []

        parents: List[ModuleType] = []
        parts = module_name.split(".")
        for index in range(1, len(parts)):
            parent_name = ".".join(parts[:index])
            parent_module = self._load_module_object(parent_name)
            if parent_module is not None:
                parents.append(parent_module)
        return parents

    def _resolve_imported_modules(
        self,
        module: ModuleType,
        imports_ast: Iterable[Dict[str, str]],
    ) -> List[ModuleType]:
        resolved: List[ModuleType] = []
        seen_names: Set[str] = set()
        for imp in imports_ast:
            imported_module = self._resolve_imported_module(module, imp)
            if imported_module is None:
                continue
            imported_name = str(getattr(imported_module, "__name__", "") or "").strip()
            if not imported_name or imported_name in seen_names:
                continue
            resolved.append(imported_module)
            seen_names.add(imported_name)
        return resolved

    def _resolve_imported_module(self, module: ModuleType, imp: Dict[str, str]) -> Optional[ModuleType]:
        module_dict = getattr(module, "__dict__", {})
        imp_type = str(imp.get("type", "") or "")

        if imp_type == "import":
            imported_name = str(imp.get("module", "") or "").strip()
            if not imported_name:
                return None

            loaded_module = self._load_module_object(imported_name)
            if loaded_module is not None:
                return loaded_module

            bound_name = str(imp.get("asname", "") or "").strip() or imported_name.split(".", 1)[0]
            bound_module = self._module_from_object(module_dict.get(bound_name))
            if bound_module is not None:
                return self._load_module_object(imported_name) or bound_module

            return self._load_module_object(imported_name)

        if imp_type != "from...import":
            return None

        alias_name = str(imp.get("name", "") or "").strip()
        bound_name = str(imp.get("asname", "") or alias_name or "").strip()
        if bound_name:
            bound_module = self._module_from_object(module_dict.get(bound_name))
            if bound_module is not None:
                return bound_module

        resolved_base = self._resolve_import_module(
            imp,
            current_module_name=str(getattr(module, "__name__", "") or ""),
            current_package=str(getattr(module, "__package__", "") or ""),
        )
        if not resolved_base:
            return None

        if alias_name and alias_name != "*":
            child_module = self._load_module_object(f"{resolved_base}.{alias_name}")
            if child_module is not None:
                return child_module

        return self._load_module_object(resolved_base)

    def _module_from_object(self, obj: Any) -> Optional[ModuleType]:
        if inspect.ismodule(obj):
            return obj

        module_name = str(getattr(obj, "__module__", "") or "").strip()
        if not module_name:
            return None
        return self._load_module_object(module_name)

    def _load_module_object(self, module_name: str) -> Optional[ModuleType]:
        normalized = str(module_name or "").strip()
        if not normalized:
            return None

        loaded = sys.modules.get(normalized)
        if inspect.ismodule(loaded):
            return loaded

        try:
            imported = importlib.import_module(normalized)
        except Exception:
            return None
        return imported if inspect.ismodule(imported) else None

    def _module_file_from_object(self, module: ModuleType) -> str:
        module_file = str(getattr(module, "__file__", "") or "").strip()
        if not module_file:
            return ""
        resolved = Path(module_file).resolve()
        if not resolved.exists():
            return ""
        return str(resolved)

    def _extract_imports_from_source(self, source: str) -> List[Dict[str, str]]:
        """从源码提取 import 语句"""
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return []

        imports = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append({
                        "type": "import",
                        "module": alias.name,
                        "asname": alias.asname or "",
                    })
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    imports.append({
                        "type": "from...import",
                        "module": module,
                        "name": alias.name,
                        "asname": alias.asname or "",
                        "level": int(getattr(node, "level", 0) or 0),
                    })

        return imports

    def _resolve_import_module(
        self,
        imp: Dict[str, str],
        *,
        current_module_name: str,
        current_package: str,
    ) -> str:
        imp_type = str(imp.get("type", "") or "")
        module_name = str(imp.get("module", "") or "").strip()
        if imp_type == "import":
            return module_name

        if imp_type != "from...import":
            return module_name

        alias_name = str(imp.get("name", "") or "").strip()
        level = int(imp.get("level", 0) or 0)
        anchor_package = str(current_package or "").strip()
        if not anchor_package and current_module_name and "." in current_module_name:
            anchor_package = current_module_name.rsplit(".", 1)[0]

        resolved_base = module_name
        if level > 0:
            relative_name = "." * level + module_name
            try:
                resolved_base = importlib.util.resolve_name(relative_name, anchor_package)
            except Exception:
                resolved_base = ""

        resolved_base = str(resolved_base or "").strip()
        if alias_name and alias_name != "*" and resolved_base:
            alias_candidate = f"{resolved_base}.{alias_name}"
            if self._find_module_file(alias_candidate):
                return alias_candidate

        if not resolved_base and alias_name and alias_name != "*" and level > 0:
            try:
                alias_only = importlib.util.resolve_name("." * level + alias_name, anchor_package)
            except Exception:
                alias_only = ""
            if alias_only and self._find_module_file(alias_only):
                return alias_only

        return resolved_base

    def _find_module_file(self, module_name: str) -> Optional[str]:
        """查找模块文件路径"""
        module = self._load_module_object(module_name)
        if module is not None:
            module_file = self._module_file_from_object(module)
            if module_file:
                return module_file

        try:
            spec = importlib.machinery.PathFinder.find_spec(module_name)
            if spec and spec.origin:
                return str(Path(spec.origin).resolve())
            return None
        except (ImportError, ModuleNotFoundError, ValueError):
            return None

    def _is_local_module(self, module_file: str) -> bool:
        """判断是否是本地模块（非标准库、非 site-packages）"""
        if not module_file:
            return False

        path = Path(module_file)
        if any(part in _RUNTIME_PACKAGE_ROOTS for part in path.parts):
            return False

        # 排除标准库
        if 'site-packages' in path.parts or 'dist-packages' in path.parts:
            return False

        # 排除标准库路径
        if any(stdlib in path.parents for stdlib in [
            Path(sys.prefix) / "lib",
            Path(sys.base_prefix) / "lib",
            Path(sys.exec_prefix) / "lib",
            Path(sys.prefix) / "Lib",
            Path(sys.base_prefix) / "Lib",
            Path(sys.exec_prefix) / "Lib",
        ]):
            return False

        return True


class DependencyPackager:
    """依赖打包器"""

    def __init__(self, analyzer: Optional[DependencyAnalyzer] = None):
        self.analyzer = analyzer or DependencyAnalyzer()

    def package_function(
        self,
        func: Callable,
        *,
        output_file: Optional[str] = None,
        include_tests: bool = True,
    ) -> str:
        """打包函数及其所有依赖

        Args:
            func: 要打包的函数
            output_file: 输出文件路径（.tar.gz），如果为 None 则使用临时文件
            include_tests: 是否包含测试文件

        Returns:
            打包文件的路径
        """
        # 分析依赖
        deps = self.analyzer.analyze_function(func)

        if deps.get("error"):
            raise RuntimeError(deps["error"])

        if output_file is None:
            output_file = _new_temp_targz_path(prefix="pycloud_func_")

        module_entries = self._build_function_entries(func, deps=deps, include_tests=include_tests)
        _write_cached_deterministic_targz(module_entries, output_file, cache_scope="function")

        return output_file

    def package_module(
        self,
        module_name: str | ModuleType,
        *,
        output_file: Optional[str] = None,
        include_tests: bool = True,
    ) -> str:
        """打包模块及其所有依赖

        Args:
            module_name: 模块名
            output_file: 输出文件路径
            include_tests: 是否包含测试文件

        Returns:
            打包文件的路径
        """
        try:
            loaded_module = module_name if inspect.ismodule(module_name) else importlib.import_module(str(module_name))
        except ImportError as exc:
            raise RuntimeError(f"无法导入模块: {exc}") from exc
        deps = self.analyzer.analyze_module(loaded_module)

        if deps.get("error"):
            raise RuntimeError(deps["error"])

        if output_file is None:
            output_file = _new_temp_targz_path(prefix="pycloud_module_")

        module_entries = self._build_module_entries(
            module_name=str(getattr(loaded_module, "__name__", "") or deps.get("module_name") or ""),
            module_file=str(deps.get("file") or ""),
            deps=deps,
            include_tests=include_tests,
        )
        _write_cached_deterministic_targz(module_entries, output_file, cache_scope="module")

        return output_file

    def package_roots(
        self,
        roots: Iterable[Path],
        *,
        output_file: Optional[str] = None,
        include_tests: bool = True,
        synthesize_missing_package_inits: bool = False,
    ) -> str:
        if output_file is None:
            output_file = _new_temp_targz_path(prefix="pycloud_roots_")
        entries = _iter_roots_entries(
            roots,
            include_tests=include_tests,
            synthesize_missing_package_inits=synthesize_missing_package_inits,
        )
        _write_cached_deterministic_targz(entries, output_file, cache_scope="roots")
        return output_file

    def package_directory(
        self,
        dir_path: str | os.PathLike[str],
        *,
        output_file: Optional[str] = None,
        include_tests: bool = True,
    ) -> str:
        root = Path(dir_path).resolve()
        if not root.exists():
            raise FileNotFoundError(f"artifact path not found: {dir_path}")
        if not root.is_dir():
            raise ValueError(f"artifact path must be a directory: {dir_path}")
        if output_file is None:
            output_file = _new_temp_targz_path(prefix="pycloud_dir_")
        entries = _iter_directory_entries(
            root,
            include_tests=include_tests,
            synthesize_missing_package_inits=True,
        )
        _write_cached_deterministic_targz(entries, output_file, cache_scope="directory")
        return output_file

    def package_paths(
        self,
        *,
        root_dir: str | os.PathLike[str],
        paths: Iterable[str | os.PathLike[str]],
        output_file: Optional[str] = None,
        include_tests: bool = True,
        synthesize_missing_package_inits: bool = False,
    ) -> str:
        if output_file is None:
            output_file = _new_temp_targz_path(prefix="pycloud_paths_")
        entries = _iter_relative_path_entries(
            root_dir=Path(root_dir),
            paths=paths,
            include_tests=include_tests,
            synthesize_missing_package_inits=synthesize_missing_package_inits,
        )
        _write_cached_deterministic_targz(entries, output_file, cache_scope="paths")
        return output_file

    def _build_function_entries(
        self,
        func: Callable,
        *,
        deps: Dict[str, Any],
        include_tests: bool,
    ) -> List[_TarSourceEntry]:
        module_name = str(func.__module__ or "").strip()
        module_file = str(deps.get("source_file") or deps.get("file") or "").strip()
        return self._build_module_entries(
            module_name=module_name,
            module_file=module_file,
            deps=deps,
            include_tests=include_tests,
        )

    def _build_module_entries(
        self,
        *,
        module_name: str,
        module_file: str,
        deps: Dict[str, Any],
        include_tests: bool,
    ) -> List[_TarSourceEntry]:
        entries: Dict[str, _TarSourceEntry] = {}
        for item_name, item_file in self._iter_dependency_module_files(
            module_name=module_name,
            module_file=module_file,
            deps=deps,
        ):
            path = Path(item_file).resolve()
            if not _is_packaged_python_file(path, include_tests=include_tests):
                continue
            arcname = self._arcname_for_module_file(item_name, path)
            if not arcname:
                continue
            entries[arcname] = _TarSourceEntry(arcname=arcname, source_path=path)
        for arcname, source_path in self._iter_project_import_root_files(
            module_name=module_name,
            module_file=module_file,
            deps=deps,
            include_tests=include_tests,
        ):
            entries.setdefault(arcname, _TarSourceEntry(arcname=arcname, source_path=source_path))
        project_root = self._infer_project_root(module_name=module_name, module_file=module_file)
        if project_root is not None:
            self._expand_entries_with_project_import_roots(
                entries,
                project_root=project_root,
                include_tests=include_tests,
            )
        return _synthesize_package_init_entries(entries.values())

    def _iter_dependency_module_files(
        self,
        *,
        module_name: str,
        module_file: str,
        deps: Dict[str, Any],
    ) -> List[Tuple[str, str]]:
        items: List[Tuple[str, str]] = []
        normalized_root_name = str(module_name or "").strip()
        normalized_root_file = str(module_file or "").strip()
        if normalized_root_name and normalized_root_file:
            items.append((normalized_root_name, normalized_root_file))

        for item in deps.get("local_modules", []):
            item_name = str(item.get("name", "") or "").strip()
            item_file = str(item.get("file", "") or "").strip()
            if not item_name or not item_file:
                continue
            items.append((item_name, item_file))
        return items

    def _arcname_for_module_file(self, module_name: str, module_file: Path) -> str:
        normalized_name = str(module_name or "").strip()
        path = Path(module_file).resolve()
        if not normalized_name:
            return _normalize_arcname(path.name)

        module_parts = [part for part in normalized_name.split(".") if part]
        if not module_parts:
            return _normalize_arcname(path.name)

        if path.name == "__init__.py":
            return _normalize_arcname(Path(*module_parts) / "__init__.py")

        if len(module_parts) == 1:
            return _normalize_arcname(path.name)

        return _normalize_arcname(Path(*module_parts[:-1]) / path.name)

    def _iter_project_import_root_files(
        self,
        *,
        module_name: str,
        module_file: str,
        deps: Dict[str, Any],
        include_tests: bool,
    ) -> List[Tuple[str, Path]]:
        project_root = self._infer_project_root(module_name=module_name, module_file=module_file)
        if project_root is None:
            return []
        root_names = self._collect_project_import_root_names(deps)
        entries: List[Tuple[str, Path]] = []
        for root_name in sorted(root_names):
            if not root_name or root_name in self.analyzer.stdlib_modules:
                continue
            root_path = (project_root / root_name).resolve()
            if root_path.is_dir():
                for entry in _iter_directory_entries(
                    root_path,
                    include_tests=include_tests,
                    prefix=Path(root_name),
                    synthesize_missing_package_inits=True,
                ):
                    if entry.source_path is not None:
                        entries.append((_normalize_arcname(entry.arcname), Path(entry.source_path).resolve()))
                continue
            py_path = project_root / f"{root_name}.py"
            if _is_packaged_python_file(py_path, include_tests=include_tests):
                entries.append((_normalize_arcname(py_path.name), py_path.resolve()))
        return entries

    def _collect_project_import_root_names(self, deps: Dict[str, Any]) -> Set[str]:
        roots: Set[str] = set()
        for item in deps.get("imports", []) or []:
            roots.update(self._import_root_names_from_ast_item(item))
        for item in deps.get("local_modules", []) or []:
            item_name = str(item.get("name", "") or "").strip()
            if item_name:
                roots.add(item_name.split(".", 1)[0])
        return roots

    def _expand_entries_with_project_import_roots(
        self,
        entries: Dict[str, _TarSourceEntry],
        *,
        project_root: Path,
        include_tests: bool,
    ) -> None:
        seen_roots: Set[str] = set()
        while True:
            root_names: Set[str] = set()
            for entry in list(entries.values()):
                source_path = entry.source_path
                if source_path is None or Path(source_path).suffix.lower() != ".py":
                    continue
                source = self.analyzer._read_module_source(source_path)
                if not source:
                    continue
                root_names.update(self._collect_import_root_names_from_source(source))
                root_names.update(
                    self._collect_relative_import_roots_from_entry(
                        source,
                        arcname=str(entry.arcname or ""),
                    )
                )

            added = False
            for root_name in sorted(root_names):
                if not root_name or root_name in seen_roots or root_name in self.analyzer.stdlib_modules:
                    continue
                seen_roots.add(root_name)
                for arcname, source_path in self._iter_project_root_files(
                    project_root=project_root,
                    root_name=root_name,
                    include_tests=include_tests,
                ):
                    if arcname in entries:
                        continue
                    entries[arcname] = _TarSourceEntry(arcname=arcname, source_path=source_path)
                    added = True
            if not added:
                return

    def _collect_import_root_names_from_source(self, source: str) -> Set[str]:
        roots: Set[str] = set()
        for item in self.analyzer._extract_imports_from_source(source):
            roots.update(self._import_root_names_from_ast_item(item))
        return roots

    def _collect_relative_import_roots_from_entry(self, source: str, *, arcname: str) -> Set[str]:
        roots: Set[str] = set()
        package_parts = list(PurePosixPath(_normalize_arcname(arcname)).parent.parts)
        for item in self.analyzer._extract_imports_from_source(source):
            if str(item.get("type", "") or "") != "from...import":
                continue
            level = int(item.get("level", 0) or 0)
            if level <= 0:
                continue
            base_parts = package_parts[: max(0, len(package_parts) - level + 1)]
            module_name = str(item.get("module", "") or "").strip()
            if module_name:
                base_parts.extend(part for part in module_name.split(".") if part)
            alias_name = str(item.get("name", "") or "").strip()
            if alias_name and alias_name != "*":
                candidate_parts = [*base_parts, *[part for part in alias_name.split(".") if part]]
            else:
                candidate_parts = base_parts
            if candidate_parts:
                roots.add(candidate_parts[0])
        return roots

    def _import_root_names_from_ast_item(self, item: Dict[str, str]) -> Set[str]:
        roots: Set[str] = set()
        imp_type = str(item.get("type", "") or "")
        module_name = str(item.get("module", "") or "").strip()
        alias_name = str(item.get("name", "") or "").strip()
        if imp_type == "import" and module_name:
            roots.add(module_name.split(".", 1)[0])
        elif imp_type == "from...import":
            if module_name:
                roots.add(module_name.split(".", 1)[0])
            elif alias_name and int(item.get("level", 0) or 0) == 0:
                roots.add(alias_name.split(".", 1)[0])
        return roots

    def _iter_project_root_files(
        self,
        *,
        project_root: Path,
        root_name: str,
        include_tests: bool,
    ) -> List[Tuple[str, Path]]:
        root_path = (project_root / root_name).resolve()
        if root_path.is_dir():
            return [
                (_normalize_arcname(entry.arcname), Path(entry.source_path).resolve())
                for entry in _iter_directory_entries(
                    root_path,
                    include_tests=include_tests,
                    prefix=Path(root_name),
                    synthesize_missing_package_inits=True,
                )
                if entry.source_path is not None
            ]
        py_path = project_root / f"{root_name}.py"
        if _is_packaged_python_file(py_path, include_tests=include_tests):
            return [(_normalize_arcname(py_path.name), py_path.resolve())]
        return []

    def _infer_project_root(self, *, module_name: str, module_file: str) -> Optional[Path]:
        normalized_name = str(module_name or "").strip()
        normalized_file = str(module_file or "").strip()
        if not normalized_name or not normalized_file:
            return None
        path = Path(normalized_file).resolve()
        module_parts = [part for part in normalized_name.split(".") if part]
        if not module_parts:
            return path.parent if path.exists() else None
        try:
            relative = Path(*module_parts[:-1], "__init__.py") if path.name == "__init__.py" else Path(*module_parts[:-1], path.name)
            root = path
            for _ in relative.parts:
                root = root.parent
            return root
        except Exception:
            return path.parent if path.exists() else None


def package_module_for_debug(
    module_name: str | ModuleType,
    *,
    output_file: Optional[str] = None,
    include_tests: bool = True,
) -> Dict[str, Any]:
    """本地打包模块并返回调试信息。"""
    packager = DependencyPackager()
    package_path = packager.package_module(
        module_name,
        output_file=output_file,
        include_tests=include_tests,
    )
    with tarfile.open(package_path, "r:gz") as tar:
        entries = sorted(tar.getnames())
    return {
        "package_path": str(Path(package_path).resolve()),
        "entries": entries,
    }
