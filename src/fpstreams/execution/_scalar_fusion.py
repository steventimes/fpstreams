"""Compile closed scalar map/filter stages into one callback-free Python loop."""

from __future__ import annotations

import ast
import struct
from collections.abc import Callable, Iterator
from functools import lru_cache
from typing import Any, Literal, cast

from ..expressions.scalar import (
    _OPCODE_NAMES,
    _OPCODES,
    _UNARY_OPCODES,
    Expr,
    FExpr,
    _ast_binary,
)
from ..expressions.typed_ir import ExpressionSource
from ..physical.plan import CompiledExpressionPhysicalNode, PhysicalNode
from ..planning.sync import FilterOp, MapOp

_MAX_FUSED_INSTRUCTIONS = 128
_MAX_FUSED_STAGES = 32
_MAX_TOTAL_FUSED_INSTRUCTIONS = 512
_UNARY_OPERATIONS = frozenset(("neg", "not", "abs"))
_BINARY_OPERATIONS = frozenset(_OPCODES) - {
    "item",
    "const",
    *_UNARY_OPERATIONS,
}

ScalarLoop = Callable[[Iterator[Any]], Iterator[Any]]
EncodedOperand = int | bytes
EncodedInstructions = tuple[tuple[int, EncodedOperand], ...]
ScalarStage = tuple[
    Literal["map", "filter"],
    bool,
    Literal["integer", "float"],
    EncodedInstructions,
]


def compile_scalar_fusion(nodes: tuple[PhysicalNode, ...]) -> ScalarLoop | None:
    """Return one generated loop for a closed scalar map/filter physical stage run."""
    if not nodes or len(nodes) > _MAX_FUSED_STAGES:
        return None
    stages: list[ScalarStage] = []
    total_instructions = 0
    for node in nodes:
        if not isinstance(node, CompiledExpressionPhysicalNode):
            return None
        operation = node.operation
        if isinstance(operation, MapOp):
            operation_kind: Literal["map", "filter"] = "map"
            negate = False
        elif isinstance(operation, FilterOp):
            operation_kind = "filter"
            negate = operation.negate
        else:  # pragma: no cover - guarded by the physical node constructor
            return None

        root = node.expression.root
        if node.expression.source is ExpressionSource.INTEGER and type(root) is Expr:
            scalar_kind: Literal["integer", "float"] = "integer"
            expression_type: type[Expr] | type[FExpr] = Expr
        elif node.expression.source is ExpressionSource.FLOAT and type(root) is FExpr:
            scalar_kind = "float"
            expression_type = FExpr
        else:
            return None
        if not _is_canonical_scalar_graph(root, expression_type):
            return None
        instructions = root.native_instructions()
        if len(instructions) > _MAX_FUSED_INSTRUCTIONS:
            return None
        total_instructions += len(instructions)
        if total_instructions > _MAX_TOTAL_FUSED_INSTRUCTIONS:
            return None
        if scalar_kind == "integer":
            encoded: EncodedInstructions = tuple(
                (opcode, cast(int, operand)) for opcode, operand in instructions
            )
        else:
            encoded = tuple(
                (opcode, struct.pack("!d", cast(float, operand)))
                for opcode, operand in instructions
            )
        stages.append((operation_kind, negate, scalar_kind, encoded))
    try:
        return _compile_scalar_loop(tuple(stages))
    except (RecursionError, RuntimeError, SyntaxError, TypeError, ValueError):
        return None


def _is_canonical_scalar_graph(
    root: Expr | FExpr,
    expression_type: type[Expr] | type[FExpr],
) -> bool:
    """Accept only exact graphs produced by the public Expr/FExpr constructors."""
    stack: list[Expr | FExpr] = [root]
    while stack:
        current = stack.pop()
        if type(current) is not expression_type:
            return False
        operation = current.operation
        if operation == "item":
            if current.left is not None or current.right is not None or current.value is not None:
                return False
            continue
        if operation == "const":
            expected_value_type = int if expression_type is Expr else float
            if (
                current.left is not None
                or current.right is not None
                or type(current.value) is not expected_value_type
            ):
                return False
            continue
        if current.value is not None or type(current.left) is not expression_type:
            return False
        stack.append(current.left)
        if operation in _UNARY_OPERATIONS:
            if current.right is not None:
                return False
            continue
        if operation not in _BINARY_OPERATIONS or type(current.right) is not expression_type:
            return False
        stack.append(current.right)
    return True


@lru_cache(maxsize=1_024)
def _compile_scalar_loop(stages: tuple[ScalarStage, ...]) -> ScalarLoop:
    """Compile and cache one restricted generator for structurally equal stage runs."""
    body: list[ast.stmt] = [
        ast.Assign(
            targets=[ast.Name(id="_current", ctx=ast.Store())],
            value=ast.Name(id="_item", ctx=ast.Load()),
        )
    ]
    for operation_kind, negate, scalar_kind, instructions in stages:
        expression = _expression_ast(scalar_kind, instructions)
        if operation_kind == "map":
            body.append(
                ast.Assign(
                    targets=[ast.Name(id="_current", ctx=ast.Store())],
                    value=expression,
                )
            )
            continue
        body.append(
            ast.If(
                test=(expression if negate else ast.UnaryOp(op=ast.Not(), operand=expression)),
                body=[ast.Continue()],
                orelse=[],
            )
        )
    body.append(ast.Expr(value=ast.Yield(value=ast.Name(id="_current", ctx=ast.Load()))))
    function = ast.FunctionDef(
        name="_fpstreams_scalar_loop",
        args=ast.arguments(
            posonlyargs=[],
            args=[ast.arg(arg="_source")],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[],
        ),
        body=[
            ast.For(
                target=ast.Name(id="_item", ctx=ast.Store()),
                iter=ast.Name(id="_source", ctx=ast.Load()),
                body=body,
                orelse=[],
            )
        ],
        decorator_list=[],
        returns=None,
        type_comment=None,
        type_params=[],
    )
    module = ast.Module(body=[function], type_ignores=[])
    namespace: dict[str, Any] = {
        "__builtins__": {},
        "_abs": abs,
        "_bool": bool,
        "_float": float,
    }
    code = compile(
        ast.fix_missing_locations(module),
        "<fpstreams-scalar-fusion>",
        "exec",
    )
    exec(code, namespace)
    return cast(ScalarLoop, namespace["_fpstreams_scalar_loop"])


def _expression_ast(
    scalar_kind: Literal["integer", "float"],
    instructions: EncodedInstructions,
) -> ast.expr:
    """Rebuild one validated postfix scalar program around the current loop value."""
    values: list[ast.expr] = []
    for opcode, encoded_operand in instructions:
        if opcode == _OPCODES["item"]:
            item: ast.expr = ast.Name(id="_current", ctx=ast.Load())
            if scalar_kind == "float":
                item = ast.Call(
                    func=ast.Name(id="_float", ctx=ast.Load()),
                    args=[item],
                    keywords=[],
                )
            values.append(item)
            continue
        operand: int | float = (
            cast(int, encoded_operand)
            if scalar_kind == "integer"
            else struct.unpack("!d", cast(bytes, encoded_operand))[0]
        )
        if opcode == _OPCODES["const"]:
            values.append(ast.Constant(operand))
            continue
        if opcode in _UNARY_OPCODES:
            if not values:
                raise ValueError("malformed scalar fusion program: missing operand")
            value = values.pop()
            if opcode == _OPCODES["neg"]:
                values.append(ast.UnaryOp(op=ast.USub(), operand=value))
            elif opcode == _OPCODES["abs"]:
                values.append(
                    ast.Call(
                        func=ast.Name(id="_abs", ctx=ast.Load()),
                        args=[value],
                        keywords=[],
                    )
                )
            else:
                values.append(ast.UnaryOp(op=ast.Not(), operand=value))
            continue
        if len(values) < 2:
            raise ValueError("malformed scalar fusion program: missing right operand")
        right = values.pop()
        left = values.pop()
        operation = _OPCODE_NAMES.get(opcode)
        if operation is None:
            raise ValueError(f"malformed scalar fusion opcode {opcode}")
        values.append(_ast_binary(operation, left, right))
    if len(values) != 1:
        raise ValueError("malformed scalar fusion program: unexpected operands")
    return values[0]
