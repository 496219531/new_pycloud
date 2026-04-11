"""
本地源码依赖分析与打包系统。

当前能力边界：
1. 自动收集本地源码模块与 package 资源
2. 保留 package 目录结构
3. 第三方依赖不自动打包，建议显式使用 dependency_allowlist
"""

import ast
import gzip
import importlib
import importlib.util
import inspect
import io
import os
import sys
import tempfile
import sysconfig
import importlib.machinery
import tarfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple


@dataclass(frozen=True)
class _TarSourceEntry:
    arcname: str
    source_path: Optional[Path] = None
    data: bytes = b""


def _normalize_arcname(arcname: Path | str) -> str:
    parts = [part for part in Path(str(arcname)).parts if part not in ("", ".", os.sep)]
    return str(PurePosixPath(*parts))


def _normalized_file_mode(path: Path) -> int:
    try:
        return 0o755 if os.access(path, os.X_OK) else 0o644
    except Exception:
        return 0o644


def _should_skip_packaged_path(path: Path, *, include_tests: bool = False) -> bool:
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


def _new_temp_targz_path(*, prefix: str) -> str:
    fd, output_file = tempfile.mkstemp(suffix=".tar.gz", prefix=prefix)
    os.close(fd)
    return output_file


def _iter_directory_entries(
    dir_path: Path,
    *,
    include_tests: bool = False,
    prefix: Path | None = None,
    synthesize_missing_package_inits: bool = False,
) -> List[_TarSourceEntry]:
    root = Path(dir_path).resolve()
    effective_prefix = Path(prefix) if prefix is not None else Path()
    entries: List[_TarSourceEntry] = []

    if synthesize_missing_package_inits:
        dirs_to_check = {root}
        dirs_to_check.update(path for path in root.rglob("*") if path.is_dir())
        for d in sorted(dirs_to_check, key=lambda item: str(item.relative_to(root))):
            synthetic_path = d / "__init__.py"
            if synthetic_path.exists():
                continue
            if _should_skip_packaged_path(synthetic_path, include_tests=include_tests):
                continue
            arcname = effective_prefix / d.relative_to(root) / "__init__.py"
            entries.append(_TarSourceEntry(arcname=_normalize_arcname(arcname), data=b""))

    for file_path in sorted((path for path in root.rglob("*") if path.is_file()), key=lambda item: str(item.relative_to(root))):
        if file_path.is_symlink():
            continue
        if _should_skip_packaged_path(file_path, include_tests=include_tests):
            continue
        arcname = effective_prefix / file_path.relative_to(root)
        entries.append(_TarSourceEntry(arcname=_normalize_arcname(arcname), source_path=file_path))
    return entries


def _iter_roots_entries(
    roots: Iterable[Path],
    *,
    include_tests: bool = False,
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
        if root.is_symlink():
            continue
        if _should_skip_packaged_path(root, include_tests=include_tests):
            continue
        entries.append(_TarSourceEntry(arcname=_normalize_arcname(root.name), source_path=root))
    return entries


def _iter_relative_path_entries(
    *,
    root_dir: Path,
    paths: Iterable[str | os.PathLike[str]],
    include_tests: bool = False,
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
        if p.is_symlink():
            continue
        if _should_skip_packaged_path(p, include_tests=include_tests):
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

        current_package = ""
        try:
            if module is not None:
                current_package = str(getattr(module, "__package__", "") or "")
        except Exception:
            current_package = ""

        # 分析函数所在模块的源码（包含函数体内的 import）
        module_source = self._get_module_source(func)
        if module_source:
            imports_ast = self._extract_imports_from_source(module_source)
            result["imports"] = imports_ast
            self._classify_imports(
                imports_ast,
                result,
                current_module_name=str(func.__module__ or ""),
                current_package=current_package,
            )
            self._expand_local_dependencies(result, current_package=current_package)

        return result

    def analyze_module(self, module_name: str) -> Dict[str, Any]:
        """分析模块的所有依赖

        Args:
            module_name: 模块名

        Returns:
            依赖信息字典
        """
        try:
            module = importlib.import_module(module_name)
        except ImportError as e:
            return {
                "error": f"无法导入模块: {e}",
                "module_name": module_name,
            }

        result = {
            "module_name": module_name,
            "file": getattr(module, '__file__', None),
            "imports": [],
            "local_files": [],
            "stdlib_modules": [],
            "third_party_modules": [],
            "local_modules": [],
        }

        # 获取模块源码
        module_file = getattr(module, '__file__', None)
        if module_file and module_file.endswith('.py'):
            try:
                with open(module_file, 'r', encoding='utf-8') as f:
                    source = f.read()

                imports_ast = self._extract_imports_from_source(source)
                result["imports"] = imports_ast
                self._classify_imports(
                    imports_ast,
                    result,
                    current_module_name=module_name,
                    current_package=str(getattr(module, "__package__", "") or ""),
                )
                self._expand_local_dependencies(result, current_package=str(getattr(module, "__package__", "") or ""))

            except Exception as e:
                result["error"] = f"读取模块文件失败: {e}"

        return result

    def _get_function_source(self, func: Callable) -> Optional[str]:
        """获取函数源码"""
        try:
            return inspect.getsource(func)
        except (OSError, TypeError):
            return None

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

    def _classify_imports(
        self,
        imports_ast: Iterable[Dict[str, str]],
        result: Dict[str, Any],
        *,
        current_module_name: str,
        current_package: str,
    ) -> None:
        seen_stdlib: Set[str] = set(result.get("stdlib_modules", []))
        seen_third_party: Set[str] = set(result.get("third_party_modules", []))
        seen_local_names: Set[str] = {item.get("name", "") for item in result.get("local_modules", [])}
        seen_local_files: Set[str] = set(result.get("local_files", []))

        for imp in imports_ast:
            resolved_module = self._resolve_import_module(
                imp,
                current_module_name=current_module_name,
                current_package=current_package,
            )
            module_name = str(resolved_module or imp.get("module") or "").strip()
            if not module_name:
                continue

            if module_name.split(".")[0] in self.stdlib_modules:
                if module_name not in seen_stdlib:
                    result["stdlib_modules"].append(module_name)
                    seen_stdlib.add(module_name)
                continue

            module_file = self._find_module_file(module_name)
            if module_file and self._is_local_module(module_file):
                if module_name not in seen_local_names:
                    result["local_modules"].append({
                        "name": module_name,
                        "file": module_file,
                    })
                    seen_local_names.add(module_name)
                if module_file not in seen_local_files:
                    result["local_files"].append(module_file)
                    seen_local_files.add(module_file)
                continue

            if module_name not in seen_third_party:
                result["third_party_modules"].append(module_name)
                seen_third_party.add(module_name)

    def _expand_local_dependencies(self, result: Dict[str, Any], *, current_package: str) -> None:
        pending = [item for item in result.get("local_modules", []) if isinstance(item, dict)]
        seen_modules = {str(item.get("name", "")).strip() for item in pending if str(item.get("name", "")).strip()}
        seen_files = set(result.get("local_files", []))

        while pending:
            item = pending.pop()
            module_name = str(item.get("name", "") or "").strip()
            module_file = str(item.get("file", "") or "").strip()
            if not module_name or not module_file:
                continue
            if module_file in seen_files:
                continue
            seen_files.add(module_file)

            source = ""
            if module_file.endswith(".py"):
                try:
                    with open(module_file, "r", encoding="utf-8") as f:
                        source = f.read()
                except Exception:
                    source = ""
            if not source:
                continue

            imports_ast = self._extract_imports_from_source(source)
            local_before = {str(item.get("name", "")).strip() for item in result.get("local_modules", [])}
            self._classify_imports(
                imports_ast,
                result,
                current_module_name=module_name,
                current_package=current_package,
            )
            for new_item in result.get("local_modules", []):
                name = str(new_item.get("name", "") or "").strip()
                if name and name not in seen_modules and name not in local_before:
                    pending.append(new_item)
                    seen_modules.add(name)

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
        try:
            spec = importlib.machinery.PathFinder.find_spec(module_name)
            if spec and spec.origin:
                return spec.origin
            return None
        except (ImportError, ModuleNotFoundError, ValueError):
            return None

    def _is_local_module(self, module_file: str) -> bool:
        """判断是否是本地模块（非标准库、非 site-packages）"""
        if not module_file:
            return False

        path = Path(module_file)

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
        include_tests: bool = False,
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

        roots_to_package = self._collect_function_roots(func, deps=deps)

        if output_file is None:
            output_file = _new_temp_targz_path(prefix="pycloud_func_")

        self.package_roots(
            roots_to_package,
            output_file=output_file,
            include_tests=include_tests,
        )

        return output_file

    def package_module(
        self,
        module_name: str,
        *,
        output_file: Optional[str] = None,
        include_tests: bool = False,
    ) -> str:
        """打包模块及其所有依赖

        Args:
            module_name: 模块名
            output_file: 输出文件路径
            include_tests: 是否包含测试文件

        Returns:
            打包文件的路径
        """
        # 分析依赖
        deps = self.analyzer.analyze_module(module_name)

        if deps.get("error"):
            raise RuntimeError(deps["error"])

        roots_to_package = self._collect_module_roots(module_name, deps=deps)

        if output_file is None:
            output_file = _new_temp_targz_path(prefix="pycloud_module_")

        self.package_roots(
            roots_to_package,
            output_file=output_file,
            include_tests=include_tests,
        )

        return output_file

    def package_roots(
        self,
        roots: Iterable[Path],
        *,
        output_file: Optional[str] = None,
        include_tests: bool = False,
        synthesize_missing_package_inits: bool = False,
    ) -> str:
        if output_file is None:
            output_file = _new_temp_targz_path(prefix="pycloud_roots_")
        entries = _iter_roots_entries(
            roots,
            include_tests=include_tests,
            synthesize_missing_package_inits=synthesize_missing_package_inits,
        )
        _write_deterministic_targz(entries, output_file)
        return output_file

    def package_directory(
        self,
        dir_path: str | os.PathLike[str],
        *,
        output_file: Optional[str] = None,
        include_tests: bool = False,
    ) -> str:
        root = Path(dir_path).resolve()
        if not root.exists():
            raise FileNotFoundError(f"artifact path not found: {dir_path}")
        if not root.is_dir():
            raise ValueError(f"artifact path must be a directory: {dir_path}")
        if output_file is None:
            output_file = _new_temp_targz_path(prefix="pycloud_dir_")
        entries = _iter_directory_entries(root, include_tests=include_tests)
        _write_deterministic_targz(entries, output_file)
        return output_file

    def package_paths(
        self,
        *,
        root_dir: str | os.PathLike[str],
        paths: Iterable[str | os.PathLike[str]],
        output_file: Optional[str] = None,
        include_tests: bool = False,
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
        _write_deterministic_targz(entries, output_file)
        return output_file

    def _collect_function_roots(self, func: Callable, *, deps: Dict[str, Any]) -> List[Path]:
        roots: List[Path] = []
        module_name = str(func.__module__ or "").strip()
        source_file = str(deps.get("source_file") or "").strip()
        if source_file:
            roots.append(self._module_root_from_name_and_file(module_name, source_file))

        for item in deps.get("local_modules", []):
            item_name = str(item.get("name", "") or "").strip()
            item_file = str(item.get("file", "") or "").strip()
            if not item_name or not item_file:
                continue
            roots.append(self._module_root_from_name_and_file(item_name, item_file))
        return self._dedupe_roots(roots)

    def _collect_module_roots(self, module_name: str, *, deps: Dict[str, Any]) -> List[Path]:
        roots: List[Path] = []
        module_file = str(deps.get("file") or "").strip()
        if module_file:
            roots.append(self._module_root_from_name_and_file(module_name, module_file))

        for item in deps.get("local_modules", []):
            item_name = str(item.get("name", "") or "").strip()
            item_file = str(item.get("file", "") or "").strip()
            if not item_name or not item_file:
                continue
            roots.append(self._module_root_from_name_and_file(item_name, item_file))
        return self._dedupe_roots(roots)

    def _module_root_from_name_and_file(self, module_name: str, module_file: str) -> Path:
        path = Path(module_file).resolve()
        parts = [part for part in str(module_name or "").split(".") if part]
        if path.name == "__init__.py":
            package_depth = len(parts)
            current = path.parent
            for _ in range(max(0, package_depth - 1)):
                current = current.parent
            return current
        if len(parts) <= 1:
            return path
        current = path.parent
        for _ in range(max(0, len(parts) - 2)):
            current = current.parent
        return current

    def _dedupe_roots(self, roots: Iterable[Path]) -> List[Path]:
        normalized: List[Path] = []
        for raw in roots:
            path = Path(raw).resolve()
            if not path.exists():
                continue
            normalized.append(path)

        normalized = sorted(set(normalized), key=lambda item: (len(item.parts), str(item)))
        deduped: List[Path] = []
        for path in normalized:
            if any(path == existing or existing in path.parents for existing in deduped if existing.is_dir()):
                continue
            deduped.append(path)
        return deduped

    def _create_tar_package(
        self,
        roots: List[Path],
        output_file: str,
        *,
        include_tests: bool = False,
    ) -> None:
        """创建 tar.gz 包。"""
        self.package_roots(
            roots,
            output_file=output_file,
            include_tests=include_tests,
        )

    def _should_skip_path(self, path: Path) -> bool:
        return _should_skip_packaged_path(path, include_tests=False)


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


def auto_deploy_function(
    func: Callable,
    *,
    infocenter_target: str,
    runtime: str,
    entry_module: Optional[str] = None,
    entry_callable: Optional[str] = None,
    include_tests: bool = False,
    **kwargs
):
    """自动部署函数（自动打包本地源码依赖）

    Args:
        func: 要部署的函数
        infocenter_target: InfoCenter 地址
        runtime: Python 运行时版本
        entry_module: 入口模块名（如果为 None，自动使用函数所在模块）
        entry_callable: 入口函数名（如果为 None，使用函数名）
        include_tests: 是否包含测试文件
        **kwargs: 其他部署参数

    Returns:
        TaskPoolSession 或 DeployedService 实例
    """
    from pycloud_parallel import TaskPoolSession

    # 打包函数和依赖
    packager = DependencyPackager()
    package_path = packager.package_function(
        func,
        include_tests=include_tests,
    )

    # 确定入口点
    if entry_module is None:
        raw_module_name = str(getattr(func, "__module__", "") or "").strip()
        if raw_module_name and raw_module_name != "__main__":
            entry_module = raw_module_name
        else:
            entry_module = _infer_entry_module_from_source_file(
                inspect.getsourcefile(func) or inspect.getfile(func) or ""
            ) or raw_module_name or "user_function"
    if entry_callable is None:
        entry_callable = func.__name__

    # 读取文件内容
    try:
        with open(package_path, "rb") as f:
            blob = f.read()

        # 上传并部署
        return TaskPoolSession.from_infocenter(
            infocenter_target=infocenter_target,
            job_id=f"auto-{func.__name__}",
            blob=blob,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format="tar.gz",
            **kwargs,
        )
    finally:
        Path(package_path).unlink(missing_ok=True)
