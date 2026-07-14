from __future__ import annotations

import ast
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "pycloud_parallel"


MODERN_ANNOTATION_PATTERNS = [
    re.compile(r"\b(?:list|dict|set|tuple|type|frozenset)\s*\["),
    re.compile(r"\b[A-Za-z_][A-Za-z0-9_.]*\s*\|\s*(?:None|[A-Za-z_][A-Za-z0-9_.]*)"),
]
PY38_RUNTIME_UNSUBSCRIPTABLE_BUILTINS = {
    "dict",
    "frozenset",
    "list",
    "set",
    "tuple",
    "type",
}
ALLOWED_CANCEL_FUTURES_CALLERS = {
    Path("src/pycloud_parallel/runtime/executors.py"),
}


class _RuntimeBuiltinGenericVisitor(ast.NodeVisitor):
    def __init__(self):
        self.annotation_depth = 0
        self.offenders = []

    def visit_FunctionDef(self, node):
        self._visit_function_like(node)

    def visit_AsyncFunctionDef(self, node):
        self._visit_function_like(node)

    def _visit_function_like(self, node):
        for decorator in node.decorator_list:
            self.visit(decorator)
        for default in list(node.args.defaults) + [item for item in node.args.kw_defaults if item is not None]:
            self.visit(default)
        self.visit(node.args)
        if node.returns is not None:
            self._visit_annotation(node.returns)
        for stmt in node.body:
            self.visit(stmt)

    def visit_arg(self, node):
        if node.annotation is not None:
            self._visit_annotation(node.annotation)

    def visit_AnnAssign(self, node):
        self.visit(node.target)
        if node.annotation is not None:
            self._visit_annotation(node.annotation)
        if node.value is not None:
            self.visit(node.value)

    def visit_Subscript(self, node):
        if (
            self.annotation_depth == 0
            and isinstance(node.value, ast.Name)
            and node.value.id in PY38_RUNTIME_UNSUBSCRIPTABLE_BUILTINS
        ):
            self.offenders.append((node.lineno, node.col_offset, node.value.id))
        self.generic_visit(node)

    def _visit_annotation(self, node):
        self.annotation_depth += 1
        try:
            self.visit(node)
        finally:
            self.annotation_depth -= 1


def test_modern_annotations_are_postponed_for_python38_imports():
    offenders = []
    for path in SOURCE_ROOT.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "from __future__ import annotations" in text:
            continue
        if any(pattern.search(text) for pattern in MODERN_ANNOTATION_PATTERNS):
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []


def test_py39_builtin_generics_are_not_evaluated_at_runtime_for_python38():
    offenders = []
    for path in SOURCE_ROOT.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        visitor = _RuntimeBuiltinGenericVisitor()
        visitor.visit(ast.parse(text, filename=str(path)))
        for line, column, name in visitor.offenders:
            relpath = path.relative_to(REPO_ROOT)
            offenders.append(f"{relpath}:{line}:{column} {name}[...]")

    assert offenders == []


def test_executor_shutdown_cancel_futures_falls_back_for_python38_style_executor():
    from pycloud_parallel.runtime.executors import _shutdown_executor

    class _Py38StyleExecutor:
        def __init__(self):
            self.calls = []

        def shutdown(self, *, wait=True, **kwargs):
            self.calls.append({"wait": wait, **kwargs})
            if "cancel_futures" in kwargs:
                raise TypeError("shutdown() got an unexpected keyword argument 'cancel_futures'")

    executor = _Py38StyleExecutor()

    _shutdown_executor(executor, wait=False, cancel_futures=True)

    assert executor.calls == [
        {"wait": False, "cancel_futures": True},
        {"wait": False},
    ]


def test_py39_executor_shutdown_cancel_futures_uses_compat_helper():
    offenders = []
    for path in SOURCE_ROOT.rglob("*.py"):
        relpath = path.relative_to(REPO_ROOT)
        if relpath in ALLOWED_CANCEL_FUTURES_CALLERS:
            continue
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(r"\.shutdown\([^)]*\bcancel_futures\s*=", text):
            line = text.count("\n", 0, match.start()) + 1
            offenders.append(f"{relpath}:{line}")

    assert offenders == []
