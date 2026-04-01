"""
自动依赖检测和打包系统

参考 cloudpickle 的逻辑，自动分析函数/模块的依赖，并打包成可上传的文件。
"""

import ast
import importlib
import inspect
import os
import sys
import tempfile
import tarfile
import hashlib
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


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
        stdlib_paths = {
            Path(sys.prefix) / "lib",
            Path(sys.base_prefix) / "lib",
        }
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
        try:
            module = importlib.import_module(func.__module__)
            result["file"] = getattr(module, '__file__', None)
            result["source_file"] = inspect.getsourcefile(func)
        except (ImportError, AttributeError):
            pass

        # 分析函数所在模块的源码（包含函数体内的 import）
        module_source = self._get_module_source(func)
        if module_source:
            imports_ast = self._extract_imports_from_source(module_source)
            result["imports"] = imports_ast

            # 分类导入
            for imp in imports_ast:
                module_name = imp["module"]

                if not module_name:
                    continue

                # 检查是否是标准库
                if module_name.split('.')[0] in self.stdlib_modules:
                    result["stdlib_modules"].append(module_name)
                else:
                    # 检查是否是本地模块
                    module_file = self._find_module_file(module_name)
                    if module_file and self._is_local_module(module_file):
                        result["local_modules"].append({
                            "name": module_name,
                            "file": module_file,
                        })
                        result["local_files"].append(module_file)
                    else:
                        # 第三方库
                        result["third_party_modules"].append(module_name)

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

                # 分类导入
                for imp in imports_ast:
                    module_name_imp = imp["module"]

                    if not module_name_imp:
                        continue

                    if module_name_imp.split('.')[0] in self.stdlib_modules:
                        result["stdlib_modules"].append(module_name_imp)
                    else:
                        module_file_imp = self._find_module_file(module_name_imp)
                        if module_file_imp and self._is_local_module(module_file_imp):
                            result["local_modules"].append({
                                "name": module_name_imp,
                                "file": module_file_imp,
                            })
                            result["local_files"].append(module_file_imp)
                        else:
                            result["third_party_modules"].append(module_name_imp)

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
                    })

        return imports

    def _find_module_file(self, module_name: str) -> Optional[str]:
        """查找模块文件路径"""
        try:
            spec = importlib.util.find_spec(module_name)
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

        # 收集需要打包的文件
        files_to_package = []

        # 1. 函数所在文件
        if deps["source_file"]:
            files_to_package.append(deps["source_file"])

        # 2. 本地依赖文件
        for file_path in deps["local_files"]:
            if file_path not in files_to_package:
                files_to_package.append(file_path)

        # 3. 查找相关文件（__init__.py 等）
        for file_path in list(files_to_package):
            related_files = self._find_related_files(file_path, include_tests)
            files_to_package.extend(related_files)

        # 去重
        files_to_package = list(set(files_to_package))

        # 创建 tar.gz
        if output_file is None:
            output_file = tempfile.mktemp(suffix=".tar.gz", prefix="pycloud_func_")

        self._create_tar_package(files_to_package, output_file, func.__name__)

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

        # 收集需要打包的文件
        files_to_package = []

        # 1. 模块文件
        if deps["file"]:
            files_to_package.append(deps["file"])

        # 2. 本地依赖文件
        for file_path in deps["local_files"]:
            if file_path not in files_to_package:
                files_to_package.append(file_path)

        # 3. 查找相关文件
        for file_path in list(files_to_package):
            related_files = self._find_related_files(file_path, include_tests)
            files_to_package.extend(related_files)

        # 去重
        files_to_package = list(set(files_to_package))

        # 创建 tar.gz
        if output_file is None:
            output_file = tempfile.mktemp(suffix=".tar.gz", prefix="pycloud_module_")

        self._create_tar_package(files_to_package, output_file, module_name)

        return output_file

    def _find_related_files(
        self,
        file_path: str,
        include_tests: bool = False,
    ) -> List[str]:
        """查找相关文件（__init__.py, __init__.pyi 等）"""
        related = []
        path = Path(file_path)

        # 如果是 .py 文件，检查同目录的 __init__.py
        if path.suffix == ".py":
            dir_path = path.parent
            init_file = dir_path / "__init__.py"

            if init_file.exists() and init_file != path:
                related.append(str(init_file))

            # 检查 .pyi 类型文件
            pyi_file = path.with_suffix(".pyi")
            if pyi_file.exists():
                related.append(str(pyi_file))

            # 递归检查父目录的 __init__.py
            parent = dir_path
            while parent != Path.cwd():
                parent_init = parent / "__init__.py"
                if parent_init.exists():
                    related.append(str(parent_init))
                    parent = parent.parent
                else:
                    break

        # 可选：包含测试文件
        if include_tests:
            test_patterns = [
                "test_*.py",
                "*_test.py",
                "tests/*.py",
                "test/*.py",
            ]
            # 简化实现，暂不展开

        return related

    def _create_tar_package(
        self,
        files: List[str],
        output_file: str,
        base_name: str,
    ) -> None:
        """创建 tar.gz 包"""
        with tarfile.open(output_file, "w:gz") as tar:
            for file_path in files:
                path = Path(file_path)

                if not path.exists():
                    continue

                # 计算包内路径（保持相对结构）
                if path.is_absolute():
                    # 使用相对于当前工作目录的路径
                    try:
                        arcname = path.relative_to(Path.cwd())
                    except ValueError:
                        # 如果不在 cwd 下，使用文件名
                        arcname = path.name
                else:
                    arcname = path

                tar.add(path, arcname=arcname)


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
    """自动部署函数（自动检测依赖并打包）

    Args:
        func: 要部署的函数
        infocenter_target: InfoCenter 地址
        runtime: Python 运行时版本
        entry_module: 入口模块名（如果为 None，自动使用函数所在模块）
        entry_callable: 入口函数名（如果为 None，使用函数名）
        include_tests: 是否包含测试文件
        **kwargs: 其他部署参数

    Returns:
        TaskSubmitter 或 DeployedService 实例
    """
    from pycloud_parallel import TaskSubmitter

    # 打包函数和依赖
    packager = DependencyPackager()
    package_path = packager.package_function(
        func,
        include_tests=include_tests,
    )

    # 计算 SHA256
    with open(package_path, "rb") as f:
        sha256 = hashlib.sha256(f.read()).hexdigest()

    # 确定入口点
    if entry_module is None:
        entry_module = func.__module__
    if entry_callable is None:
        entry_callable = func.__name__

    # 读取文件内容
    with open(package_path, "rb") as f:
        blob = f.read()

    # 上传并部署
    return TaskSubmitter.deploy_from_blob(
        infocenter_target=infocenter_target,
        blob=blob,
        filename=Path(package_path).name,
        runtime=runtime,
        entry_module=entry_module,
        entry_callable=entry_callable,
        package_format="tar.gz",
        export_mode="single",
        **kwargs,
    )
