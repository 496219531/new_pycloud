from __future__ import annotations

"""中文说明：AST 改写器。

核心思路：仅改写“安全子集”的 for 循环（循环体最终是 append），
把每次迭代提取为独立函数并交给 `foreach` 并行执行。
若不满足安全条件，明确回退串行，避免破坏原语义。
"""

import ast
import copy
import inspect
import textwrap
from dataclasses import dataclass
from typing import Callable, Optional, Tuple


@dataclass
class RewriteResult:
    """AST 改写结果。

    记录函数改写的结果和相关信息。

    Attributes:
        function: 改写后的函数（如果改写成功）
        rewritten_loops: 改写的 for 循环数量
        reason: 如果改写失败，说明原因
    """
    function: Optional[Callable]
    rewritten_loops: int
    reason: str = ""


def _is_append_stmt(stmt: ast.stmt) -> bool:
    """检查语句是否是 list.append() 调用。

    Args:
        stmt: AST 语句节点

    Returns:
        bool: 如果是 append 调用返回 True
    """
    if not isinstance(stmt, ast.Expr):
        return False
    call = stmt.value
    if not isinstance(call, ast.Call):
        return False
    if not isinstance(call.func, ast.Attribute):
        return False
    if call.func.attr != "append":
        return False
    if len(call.args) != 1 or call.keywords:
        return False
    return True


def _has_forbidden_nodes(stmts: list) -> bool:
    """检查语句列表中是否包含禁止的节点。

    这些语法通常意味着跨迭代依赖或复杂控制流，
    v1 版本直接禁用改写以确保安全。

    Args:
        stmts: 要检查的语句列表

    Returns:
        bool: 如果包含禁止的节点返回 True
    """
    # 这些语法通常意味着跨迭代依赖或复杂控制流，v1 直接禁改写更稳妥。
    forbidden = (
        ast.Break,
        ast.Continue,
        ast.Return,
        ast.Yield,
        ast.YieldFrom,
        ast.Raise,
        ast.Try,
        ast.With,
        ast.AsyncWith,
        ast.Await,
        ast.Global,
        ast.Nonlocal,
        ast.AugAssign,
    )
    for stmt in stmts:
        for node in ast.walk(stmt):
            if isinstance(node, forbidden):
                return True
    return False


class LoopParallelTransformer(ast.NodeTransformer):
    """for 循环并行化转换器。

    将符合条件的 for 循环转换为使用 foreach 的并行版本。

    Attributes:
        rewritten_loops: 已改写的循环数量
    """

    def __init__(self) -> None:
        super().__init__()
        self.rewritten_loops = 0

    def visit_For(self, node: ast.For):  # noqa: N802
        """访问 for 节点并尝试改写。

        只改写满足以下条件的 for 循环：
        - 没有 else 分支
        - 循环体非空
        - 最后一条语句是 list.append()
        - 不包含禁止的语法结构

        Args:
            node: for 循环节点

        Returns:
            改写后的节点列表（helper 函数 + foreach 调用 + extend）
        """
        # 只改写简单 for；其余情况全部保持原样。
        node = self.generic_visit(node)
        if not isinstance(node, ast.For):
            return node

        if node.orelse:
            return node
        if len(node.body) < 1:
            return node
        if not _is_append_stmt(node.body[-1]):
            return node
        if _has_forbidden_nodes(node.body[:-1]):
            return node

        append_stmt = node.body[-1]
        append_call = append_stmt.value  # type: ignore[assignment]
        append_receiver = append_call.func.value  # type: ignore[assignment]
        append_arg = append_call.args[0]  # type: ignore[index]

        if not isinstance(append_receiver, (ast.Name, ast.Attribute)):
            return node

        loop_id = self.rewritten_loops
        self.rewritten_loops += 1
        helper_name = f"__pc_loop_fn_{loop_id}"
        helper_item = f"__pc_item_{loop_id}"
        helper_values = f"__pc_loop_values_{loop_id}"

        helper_body = [
            ast.Assign(
                targets=[copy.deepcopy(node.target)],
                value=ast.Name(id=helper_item, ctx=ast.Load()),
            )
        ]
        helper_body.extend(copy.deepcopy(node.body[:-1]))
        helper_body.append(ast.Return(value=copy.deepcopy(append_arg)))

        helper_fn = ast.FunctionDef(
            name=helper_name,
            args=ast.arguments(
                posonlyargs=[],
                args=[ast.arg(arg=helper_item)],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[],
            ),
            body=helper_body,
            decorator_list=[],
            returns=None,
            type_comment=None,
        )

        foreach_assign = ast.Assign(
            targets=[ast.Name(id=helper_values, ctx=ast.Store())],
            value=ast.Call(
                func=ast.Name(id="__pc_foreach", ctx=ast.Load()),
                args=[copy.deepcopy(node.iter), ast.Name(id=helper_name, ctx=ast.Load())],
                keywords=[],
            ),
        )

        extend_stmt = ast.Expr(
            value=ast.Call(
                func=ast.Attribute(value=copy.deepcopy(append_receiver), attr="extend", ctx=ast.Load()),
                args=[ast.Name(id=helper_values, ctx=ast.Load())],
                keywords=[],
            )
        )
        return [helper_fn, foreach_assign, extend_stmt]


def rewrite_function(func: Callable, foreach_callable: Callable) -> RewriteResult:
    """改写函数，将符合条件的 for 循环并行化。

    改写流程：
    1. 检查函数是否使用闭包（不支持）
    2. 获取函数源代码
    3. 解析为 AST
    4. 应用 LoopParallelTransformer
    5. 编译并执行改写后的代码
    6. 保留原函数的默认值和注解

    Args:
        func: 要改写的函数
        foreach_callable: foreach 可调用对象

    Returns:
        RewriteResult: 改写结果，包含改写后的函数或失败原因
    """
    # 闭包变量在 v1 里不改写，避免捕获语义和序列化边界复杂化。
    if func.__code__.co_freevars:
        return RewriteResult(function=None, rewritten_loops=0, reason="freevars_not_supported")

    try:
        source = inspect.getsource(func)
    except (OSError, TypeError):
        return RewriteResult(function=None, rewritten_loops=0, reason="source_unavailable")

    source = textwrap.dedent(source)
    try:
        module = ast.parse(source)
    except SyntaxError:
        return RewriteResult(function=None, rewritten_loops=0, reason="source_parse_failed")

    target_node = None
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == func.__name__:
            target_node = node
            break
        if isinstance(node, ast.AsyncFunctionDef) and node.name == func.__name__:
            return RewriteResult(function=None, rewritten_loops=0, reason="async_not_supported")

    if target_node is None:
        return RewriteResult(function=None, rewritten_loops=0, reason="function_not_found_in_source")

    target_node.decorator_list = []

    transformer = LoopParallelTransformer()
    module = transformer.visit(module)
    ast.fix_missing_locations(module)

    if transformer.rewritten_loops == 0:
        return RewriteResult(function=None, rewritten_loops=0, reason="no_supported_loops")

    global_ns = dict(func.__globals__)
    global_ns["__pc_foreach"] = foreach_callable
    local_ns = {}

    try:
        compiled = compile(module, filename=inspect.getsourcefile(func) or "<pycloud>", mode="exec")
        exec(compiled, global_ns, local_ns)
    except Exception as exc:
        return RewriteResult(function=None, rewritten_loops=0, reason=f"compile_failed:{exc!r}")

    new_func = local_ns.get(func.__name__) or global_ns.get(func.__name__)
    if not callable(new_func):
        return RewriteResult(function=None, rewritten_loops=0, reason="compiled_function_missing")

    new_func.__defaults__ = func.__defaults__
    new_func.__kwdefaults__ = func.__kwdefaults__
    new_func.__annotations__ = dict(func.__annotations__)
    return RewriteResult(function=new_func, rewritten_loops=transformer.rewritten_loops)
