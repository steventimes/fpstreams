"""Compile exact structured Rows stages into one query-bound generator loop."""

from __future__ import annotations

import ast
from collections.abc import Callable, Iterator
from itertools import chain
from typing import Any, cast

from ..errors import SelectionError
from ..expressions.row import RowExpr
from ..expressions.row_ir import (
    Binary,
    Call,
    Cast,
    Coalesce,
    Field,
    IfElse,
    InputRow,
    IsNull,
    Literal,
    Unary,
)
from ..expressions.scalar import _OPCODES as _SCALAR_OPCODES
from ..planning.arrow import RowStageDescriptor, _row_stage_descriptor
from ..planning.sync import FilterOp, MapOp

_MAX_DEPTH = 128
_MAX_OCCURRENCES = 512
_EAGER_FUSION_ROWS = 384
_DIRECT_SELECT_FUSION_ROWS = 2_048
_FUSION_WARMUP_ROWS = 512
_EXACT_CASTS = (float, int, str, bool)
_ARITHMETIC_OPERATORS: dict[str, ast.operator] = {
    "+": ast.Add(),
    "-": ast.Sub(),
    "*": ast.Mult(),
    "/": ast.Div(),
    "//": ast.FloorDiv(),
    "%": ast.Mod(),
    "**": ast.Pow(),
}
_COMPARISON_OPERATORS: dict[str, ast.cmpop] = {
    "==": ast.Eq(),
    "!=": ast.NotEq(),
    "<": ast.Lt(),
    "<=": ast.LtE(),
    ">": ast.Gt(),
    ">=": ast.GtE(),
}
_I64_ROW_BINARY_OPCODES = {
    "+": _SCALAR_OPCODES["add"],
    "-": _SCALAR_OPCODES["sub"],
    "*": _SCALAR_OPCODES["mul"],
    "//": _SCALAR_OPCODES["floordiv"],
    "%": _SCALAR_OPCODES["mod"],
    "==": _SCALAR_OPCODES["eq"],
    "!=": _SCALAR_OPCODES["ne"],
    "<": _SCALAR_OPCODES["lt"],
    "<=": _SCALAR_OPCODES["le"],
    ">": _SCALAR_OPCODES["gt"],
    ">=": _SCALAR_OPCODES["ge"],
}
_I64_ROW_UNARY_OPCODES = {
    "neg": _SCALAR_OPCODES["neg"],
    "not": _SCALAR_OPCODES["not"],
}
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1
RowsOperation = MapOp | FilterOp
RowsLoop = Callable[[Iterator[Any]], Iterator[Any]]
RowsFilterSink = Callable[[list[Any], Iterator[Any]], tuple[Any | None, bool]]
I64RowFilterPlan = tuple[str, tuple[tuple[int, int], ...], bool]


def execute_rows_fusion(
    iterator: Iterator[Any],
    operations: tuple[RowsOperation, ...],
    *,
    exact_rows: int | None = None,
    eager: bool = True,
) -> Iterator[Any] | None:
    """Return a lazy adaptive Rows loop, or ``None`` for an unsupported operation run.

    Small and early-terminated streams stay on the inexpensive interpreted path.  A known-long
    builtin iterator compiles immediately, while an unknown-length stream waits for a measured
    prefix.  The boundary row is prepended to the generated loop without reordering or an extra
    upstream pull.
    """
    descriptors = _descriptors(operations)
    if descriptors is None or not _eligible_descriptors(descriptors):
        return None
    if (
        eager
        and exact_rows is not None
        and exact_rows < _DIRECT_SELECT_FUSION_ROWS
        and _is_direct_select(descriptors)
    ):
        # Building one query-local AST costs more than a single builtin map can recover on a
        # small projection. Richer expressions keep their lower crossover and still fuse.
        return None
    if exact_rows is not None:
        if eager and exact_rows >= _EAGER_FUSION_ROWS:
            compiled = compile_rows_fusion(operations)
            if compiled is not None:
                return compiled(iterator)
        # A sized-small source can never amortize compilation.  A downstream bound disables
        # eager compilation but may still justify it if filtering consumes a long prefix.
        compile_after = _FUSION_WARMUP_ROWS if not eager else None
    else:
        compile_after = _FUSION_WARMUP_ROWS
    return _adaptive_rows_fusion(iterator, operations, compile_after=compile_after)


def _is_direct_select(descriptors: tuple[RowStageDescriptor, ...]) -> bool:
    """Return whether one stage projects only exact top-level string fields."""
    if len(descriptors) != 1 or descriptors[0].kind != "select":
        return False
    return all(
        type(selector) is str and "." not in selector
        for _name, selector in descriptors[0].selectors
    )


def _adaptive_rows_fusion(
    iterator: Iterator[Any],
    operations: tuple[RowsOperation, ...],
    *,
    compile_after: int | None,
) -> Iterator[Any]:
    """Interpret a bounded prefix, then switch once to the generated exact-dict loop."""
    process = _fallback_processor(operations)
    exact_prefix = True
    for index, item in enumerate(iterator):
        if (
            compile_after is not None
            and index == compile_after
            and exact_prefix
            and type(item) is dict
        ):
            compiled = compile_rows_fusion(operations)
            if compiled is not None:
                yield from compiled(chain((item,), iterator))
                return
        exact_prefix = exact_prefix and type(item) is dict
        try:
            accepted, current = process(item)
        except StopIteration:
            return
        if accepted:
            yield current


def compile_rows_fusion(operations: tuple[RowsOperation, ...]) -> RowsLoop | None:
    """Return one exact-dict loop for a closed Rows run, or ``None`` for canonical execution.

    Compilation is deliberately query local: literal objects, selectors, and fallback
    callbacks live only in this returned function's slot tuple.  Sources and Mapping
    subclasses are never sampled while planning.  At row time, non-exact dictionaries take
    the original callback path for the whole run so their custom lookup semantics remain
    observable.
    """
    descriptors = _descriptors(operations)
    if descriptors is None or not any(
        descriptor.kind in {"with_columns", "select", "cast", "fill_nulls"}
        for descriptor in descriptors
    ):
        return None
    builder = _RowsLoopBuilder(operations, descriptors)
    try:
        return builder.compile()
    except (KeyError, TypeError, ValueError, RecursionError, SyntaxError):
        # Unsupported or excessively complex graphs stay on the established executor.  The
        # fallback decision is made without opening the source or invoking user objects.
        return None


def _filter_sink_descriptors(operation: FilterOp) -> tuple[RowStageDescriptor, ...] | None:
    """Return the one closed filter descriptor accepted by the eager Rows sink."""
    descriptors = _descriptors((operation,))
    if descriptors is None:
        return None
    descriptor = descriptors[0]
    if (
        descriptor.kind != "where"
        or descriptor.equalities
        or descriptor.predicate is None
        or not _eligible_selector(descriptor.predicate)
    ):
        return None
    return descriptors


def rows_filter_sink_eligible(operation: FilterOp) -> bool:
    """Return whether one filter can be compiled without opening or sampling its source."""
    return _filter_sink_descriptors(operation) is not None


def compile_rows_filter_sink(operation: FilterOp) -> RowsFilterSink | None:
    """Return one eager exact-dict filter sink without changing lazy iterator semantics."""
    descriptors = _filter_sink_descriptors(operation)
    if descriptors is None:
        return None
    builder = _RowsLoopBuilder((operation,), descriptors)
    try:
        return builder.compile_filter_sink()
    except (KeyError, TypeError, ValueError, RecursionError, SyntaxError):
        return None


def _i64_row_children(node: object) -> tuple[object, ...] | None:
    """Return children only for nodes represented exactly by the scalar i64 opcodes."""
    node_type = type(node)
    if node_type is Field:
        field = cast(Field, node)
        return () if type(field.name) is str and "." not in field.name else None
    if node_type is Literal:
        return ()
    if node_type is Unary:
        unary = cast(Unary, node)
        if type(unary.kind) is str and unary.kind in _I64_ROW_UNARY_OPCODES:
            return (unary.operand,)
        return None
    if node_type is Binary:
        binary = cast(Binary, node)
        if type(binary.kind) is str and binary.kind in _I64_ROW_BINARY_OPCODES:
            return binary.left, binary.right
    return None


def lower_i64_row_filter(operation: FilterOp) -> I64RowFilterPlan | None:
    """Lower one single-field integer RowExpr filter to the scalar postfix vocabulary.

    The native prefix owns only exact i64 values.  Unsupported rows and arithmetic overflow
    return to the Python sink at execution time, while unsupported graph shapes decline here
    without opening the source.  Boolean ``and`` and ``or`` are deliberately excluded because
    RowExpr short-circuits them but the scalar opcode vocabulary evaluates both operands.
    """
    if type(operation) is not FilterOp or type(operation.negate) is not bool:
        return None
    predicate = operation.predicate
    if type(predicate) is not RowExpr:
        return None

    instructions: list[tuple[int, int]] = []
    field: str | None = None
    occurrences = 0
    pending: list[tuple[object, int, bool]] = [(predicate._node, 1, False)]
    while pending:
        node, depth, visited = pending.pop()
        if not visited:
            occurrences += 1
            if depth > _MAX_DEPTH or occurrences > _MAX_OCCURRENCES:
                return None
            children = _i64_row_children(node)
            if children is None:
                return None
            pending.append((node, depth, True))
            pending.extend((child, depth + 1, False) for child in reversed(children))
            continue

        if type(node) is Field:
            if type(node.name) is not str or field is not None:
                # Binding one scalar input cannot collapse multiple observable dict lookups.
                return None
            field = node.name
            instructions.append((_SCALAR_OPCODES["item"], 0))
        elif type(node) is Literal:
            value = node.value
            if type(value) is not int or not _I64_MIN <= value <= _I64_MAX:
                return None
            instructions.append((_SCALAR_OPCODES["const"], value))
        elif type(node) is Unary:
            instructions.append((_I64_ROW_UNARY_OPCODES[node.kind], 0))
        else:
            binary = cast(Binary, node)
            instructions.append((_I64_ROW_BINARY_OPCODES[binary.kind], 0))

    if field is None:
        return None
    return field, tuple(instructions), operation.negate


def _descriptors(
    operations: tuple[RowsOperation, ...],
) -> tuple[RowStageDescriptor, ...] | None:
    """Recover exact structured descriptors without invoking an operation callback."""
    descriptors: list[RowStageDescriptor] = []
    for operation in operations:
        candidate = operation.function if isinstance(operation, MapOp) else operation.predicate
        descriptor = _row_stage_descriptor(candidate)
        if descriptor is None:
            if isinstance(operation, FilterOp) and isinstance(candidate, RowExpr):
                descriptor = RowStageDescriptor("where", predicate=candidate)
            else:
                return None
        if isinstance(operation, MapOp) and descriptor.kind not in {
            "with_columns",
            "select",
            "cast",
            "fill_nulls",
        }:
            return None
        if isinstance(operation, FilterOp) and descriptor.kind != "where":
            return None
        descriptors.append(descriptor)
    return tuple(descriptors)


def _eligible_descriptors(descriptors: tuple[RowStageDescriptor, ...]) -> bool:
    """Validate the cheap structural subset without allocating or compiling an AST."""
    has_map = False
    for descriptor in descriptors:
        if descriptor.kind in {"with_columns", "select"}:
            has_map = True
            if any(
                not _eligible_map_selector(selector) for _name, selector in descriptor.selectors
            ):
                return False
            continue
        if descriptor.kind == "cast":
            has_map = True
            if any(not callable(converter) for _name, converter in descriptor.selectors):
                return False
            continue
        if descriptor.kind == "fill_nulls":
            has_map = True
            if any(
                isinstance(replacement, RowExpr) and not _eligible_selector(replacement)
                for _name, replacement in descriptor.selectors
            ):
                return False
            continue
        if (
            descriptor.kind != "where"
            or descriptor.equalities
            or descriptor.predicate is None
            or not _eligible_selector(descriptor.predicate)
        ):
            return False
    return has_map


def _eligible_map_selector(selector: object) -> bool:
    """Accept one closed row graph or an opaque callback retained in a query-local slot."""
    if isinstance(selector, RowExpr):
        return _eligible_selector(selector)
    if callable(selector):
        return True
    return _eligible_selector(selector)


def _eligible_selector(selector: object) -> bool:
    """Accept a top-level field name or one bounded exact-dictionary row graph."""
    if isinstance(selector, str):
        return "." not in selector
    root = selector._node if isinstance(selector, RowExpr) else selector
    return _is_bounded_exact_graph(root)


def _fallback_processor(
    operations: tuple[RowsOperation, ...],
) -> Callable[[Any], tuple[bool, Any]]:
    """Build the reference interpreter for warmup, tiny streams, and fallback rows."""

    def process(item: Any) -> tuple[bool, Any]:
        current = item
        for operation in operations:
            if isinstance(operation, MapOp):
                current = operation.function(current)
            elif bool(operation.predicate(current)) is operation.negate:
                return False, current
        return True, current

    return process


class _RowsLoopBuilder:
    """Lower supported row IR and stage descriptors to safe statement-level AST."""

    def __init__(
        self,
        operations: tuple[RowsOperation, ...],
        descriptors: tuple[RowStageDescriptor, ...],
    ) -> None:
        self._operations = operations
        self._descriptors = descriptors
        self._slots: list[Any] = []
        self._temporary = 0

    def compile(self) -> RowsLoop:
        """Compile a generator whose globals expose no builtins or source-text interpolation."""
        body = self._loop_body()
        return cast(
            RowsLoop,
            self._compile_function(
                "_fpstreams_rows_loop",
                ("_source",),
                [
                    ast.For(
                        target=ast.Name(id="_row", ctx=ast.Store()),
                        iter=ast.Name(id="_source", ctx=ast.Load()),
                        body=body,
                        orelse=[],
                    )
                ],
                filename="<fpstreams-rows-fusion>",
            ),
        )

    def compile_filter_sink(self) -> RowsFilterSink:
        """Compile one eager appender that stops before the first non-exact dictionary."""
        if len(self._operations) != 1 or not isinstance(operation := self._operations[0], FilterOp):
            raise ValueError("a direct Rows filter sink requires one filter operation")
        descriptor = self._descriptors[0]
        exact_body: list[ast.stmt] = [
            ast.Assign([ast.Name(id="_current", ctx=ast.Store())], self._name("_row")),
            *self._filter_stage(operation, descriptor),
            ast.Expr(ast.Call(self._name("_append"), [self._name("_row")], [])),
        ]
        type_function = self._slot(type)
        dict_type = self._slot(dict)
        completed = ast.Tuple([ast.Constant(None), ast.Constant(True)], ast.Load())
        loop = ast.For(
            target=ast.Name(id="_row", ctx=ast.Store()),
            iter=ast.Name(id="_source", ctx=ast.Load()),
            body=[
                ast.If(
                    ast.Compare(
                        ast.Call(type_function, [self._name("_row")], []),
                        [ast.IsNot()],
                        [dict_type],
                    ),
                    [ast.Return(ast.Tuple([self._name("_row"), ast.Constant(False)], ast.Load()))],
                    [],
                ),
                ast.Try(
                    body=exact_body,
                    handlers=[
                        ast.ExceptHandler(
                            type=self._slot(StopIteration),
                            name=None,
                            body=[ast.Return(completed)],
                        )
                    ],
                    orelse=[],
                    finalbody=[],
                ),
            ],
            orelse=[],
        )
        return cast(
            RowsFilterSink,
            self._compile_function(
                "_fpstreams_rows_filter_sink",
                ("_output", "_source"),
                [
                    ast.Assign(
                        [ast.Name(id="_append", ctx=ast.Store())],
                        ast.Attribute(self._name("_output"), "append", ast.Load()),
                    ),
                    loop,
                    ast.Return(completed),
                ],
                filename="<fpstreams-rows-filter-sink>",
            ),
        )

    def _compile_function(
        self,
        name: str,
        arguments: tuple[str, ...],
        body: list[ast.stmt],
        *,
        filename: str,
    ) -> Callable[..., Any]:
        """Bind generated code to query-local slots without exposing Python builtins."""
        slot_arguments = [
            ast.arg(arg=f"_fpstreams_slot_{index}") for index in range(len(self._slots))
        ]
        slot_defaults: list[ast.expr] = [
            ast.Subscript(
                ast.Name(id="_fpstreams_slots", ctx=ast.Load()),
                ast.Constant(index),
                ast.Load(),
            )
            for index in range(len(self._slots))
        ]
        function = ast.FunctionDef(
            name=name,
            args=ast.arguments(
                posonlyargs=[],
                args=[*(ast.arg(arg=argument) for argument in arguments), *slot_arguments],
                vararg=None,
                kwonlyargs=[],
                kw_defaults=[],
                defaults=slot_defaults,
            ),
            body=body,
            decorator_list=[],
            returns=None,
            type_comment=None,
            type_params=[],
        )
        namespace: dict[str, Any] = {
            "__builtins__": {},
            "_fpstreams_slots": tuple(self._slots),
        }
        code = compile(
            ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[])),
            filename,
            "exec",
        )
        exec(code, namespace)
        return cast(Callable[..., Any], namespace[name])

    def _loop_body(self) -> list[ast.stmt]:
        """Build exact-type dispatch followed by each stage in encounter order."""
        fallback = self._slot(_fallback_processor(self._operations))
        type_function = self._slot(type)
        dict_type = self._slot(dict)
        fallback_value = ast.Name(id="_fallback", ctx=ast.Load())
        body: list[ast.stmt] = [
            ast.If(
                test=ast.Compare(
                    ast.Call(type_function, [self._name("_row")], []),
                    [ast.IsNot()],
                    [dict_type],
                ),
                body=[
                    ast.Try(
                        body=[
                            ast.Assign(
                                [ast.Name(id="_fallback", ctx=ast.Store())],
                                ast.Call(fallback, [self._name("_row")], []),
                            )
                        ],
                        handlers=[
                            ast.ExceptHandler(
                                type=self._slot(StopIteration),
                                name=None,
                                body=[ast.Return(value=None)],
                            )
                        ],
                        orelse=[],
                        finalbody=[],
                    ),
                    ast.If(
                        ast.Subscript(fallback_value, ast.Constant(0), ast.Load()),
                        [
                            ast.Expr(
                                ast.Yield(
                                    ast.Subscript(fallback_value, ast.Constant(1), ast.Load())
                                )
                            )
                        ],
                        [],
                    ),
                    ast.Continue(),
                ],
                orelse=[],
            )
        ]
        exact_body: list[ast.stmt] = [
            ast.Assign([ast.Name(id="_current", ctx=ast.Store())], self._name("_row")),
        ]
        for operation, descriptor in zip(self._operations, self._descriptors, strict=True):
            if isinstance(operation, MapOp):
                exact_body.extend(self._map_stage(descriptor))
            else:
                exact_body.extend(self._filter_stage(operation, descriptor))
        exact_body.append(ast.Expr(ast.Yield(self._name("_current"))))
        body.append(
            ast.Try(
                body=exact_body,
                handlers=[
                    ast.ExceptHandler(
                        type=self._slot(StopIteration),
                        name=None,
                        body=[ast.Return(value=None)],
                    )
                ],
                orelse=[],
                finalbody=[],
            )
        )
        return body

    def _map_stage(self, descriptor: RowStageDescriptor) -> list[ast.stmt]:
        """Copy/enrich or project a dictionary while preserving selector declaration order."""
        output_name = self._temp("record")
        if descriptor.kind in {"with_columns", "cast", "fill_nulls"}:
            statements: list[ast.stmt] = [
                ast.Assign(
                    [ast.Name(id=output_name, ctx=ast.Store())],
                    ast.Call(
                        ast.Attribute(self._name("_current"), "copy", ast.Load()),
                        [],
                        [],
                    ),
                )
            ]
        elif descriptor.kind == "select":
            statements = [
                ast.Assign(
                    [ast.Name(id=output_name, ctx=ast.Store())],
                    ast.Dict(keys=[], values=[]),
                )
            ]
        else:
            raise ValueError("filter descriptor used for a map stage")

        record = ast.Name(id=output_name, ctx=ast.Load())
        if descriptor.kind in {"with_columns", "select"}:
            # Every sibling selector receives the pre-stage dictionary, even after an earlier
            # output column has been assigned to the copied result.
            for name, selector in descriptor.selectors:
                expression_statements, value = self._selector(selector, self._name("_current"))
                statements.extend(expression_statements)
                statements.append(
                    ast.Assign(
                        [ast.Subscript(record, ast.Constant(name), ast.Store())],
                        value,
                    )
                )
        elif descriptor.kind == "cast":
            for name, converter in descriptor.selectors:
                statements.append(
                    ast.If(
                        ast.Compare(
                            ast.Constant(name),
                            [ast.NotIn()],
                            [record],
                        ),
                        [
                            ast.Raise(
                                ast.Call(
                                    self._slot(SelectionError),
                                    [ast.Constant(f"cast column {name!r} is missing")],
                                    [],
                                ),
                                None,
                            )
                        ],
                        [],
                    )
                )
                statements.append(
                    ast.Assign(
                        [ast.Subscript(record, ast.Constant(name), ast.Store())],
                        ast.Call(
                            self._slot(converter),
                            [ast.Subscript(record, ast.Constant(name), ast.Load())],
                            [],
                        ),
                    )
                )
        elif descriptor.kind == "fill_nulls":
            for name, replacement in descriptor.selectors:
                current_name = self._temp("nullable")
                statements.append(
                    ast.Assign(
                        [ast.Name(id=current_name, ctx=ast.Store())],
                        ast.Call(
                            ast.Attribute(record, "get", ast.Load()),
                            [ast.Constant(name)],
                            [],
                        ),
                    )
                )
                if isinstance(replacement, RowExpr):
                    replacement_statements, value = self._expression(
                        replacement._node,
                        self._name("_current"),
                    )
                else:
                    replacement_statements = []
                    value = self._slot(replacement)
                replacement_statements.append(
                    ast.Assign(
                        [ast.Subscript(record, ast.Constant(name), ast.Store())],
                        value,
                    )
                )
                statements.append(
                    ast.If(
                        ast.Compare(
                            ast.Name(id=current_name, ctx=ast.Load()),
                            [ast.Is()],
                            [ast.Constant(None)],
                        ),
                        replacement_statements,
                        [],
                    )
                )
        else:
            raise ValueError("unsupported map descriptor")
        statements.append(
            ast.Assign(
                [ast.Name(id="_current", ctx=ast.Store())],
                ast.Name(id=output_name, ctx=ast.Load()),
            )
        )
        return statements

    def _filter_stage(self, operation: FilterOp, descriptor: RowStageDescriptor) -> list[ast.stmt]:
        """Lower one pure predicate; equality/callback mixtures remain canonical barriers."""
        if descriptor.equalities or descriptor.predicate is None:
            raise ValueError("structured equality filters use the canonical callback path")
        statements, predicate = self._selector(descriptor.predicate, self._name("_current"))
        rejected = predicate if operation.negate else ast.UnaryOp(ast.Not(), predicate)
        statements.append(
            ast.If(
                rejected,
                [ast.Continue()],
                [],
            )
        )
        return statements

    def _selector(self, selector: object, row: ast.expr) -> tuple[list[ast.stmt], ast.expr]:
        """Lower top-level string selectors and safe RowExpr roots only."""
        if isinstance(selector, RowExpr):
            root = selector._node
            if not _is_bounded_exact_graph(root):
                raise ValueError("selector is not in the exact-dict closed subset")
            return self._expression(root, row)
        if callable(selector):
            value_name = self._temp("callback")
            statement = ast.Assign(
                [ast.Name(id=value_name, ctx=ast.Store())],
                ast.Call(self._slot(selector), [row], []),
            )
            return [statement], ast.Name(id=value_name, ctx=ast.Load())
        if isinstance(selector, str):
            if "." in selector:
                raise ValueError("path selectors retain canonical Mapping semantics")
            return self._expression(Field(selector), row)
        root = selector
        if not _is_bounded_exact_graph(root):
            raise ValueError("selector is not in the exact-dict closed subset")
        return self._expression(root, row)

    def _expression(self, node: object, row: ast.expr) -> tuple[list[ast.stmt], ast.expr]:
        """Linearize one IR occurrence so lookup and operator exception order is unchanged."""
        if isinstance(node, InputRow):
            return [], row
        if isinstance(node, Literal):
            return [], self._slot(node.value)
        if isinstance(node, Field):
            return self._field(node, row)
        if isinstance(node, Unary):
            statements, operand = self._expression(node.operand, row)
            if node.kind == "not":
                value: ast.expr = ast.UnaryOp(ast.Not(), operand)
            elif node.kind == "neg":
                value = ast.UnaryOp(ast.USub(), operand)
            elif node.kind == "abs":
                value = ast.Call(self._slot(abs), [operand], [])
            else:
                raise ValueError(f"unsupported unary operator {node.kind!r}")
            return statements, self._assign_value(statements, value)
        if isinstance(node, Binary):
            return self._binary(node, row)
        if isinstance(node, Cast):
            if not any(node.target is target for target in _EXACT_CASTS):
                raise ValueError("custom casts retain canonical callback semantics")
            statements, value = self._expression(node.value, row)
            return statements, self._assign_value(
                statements, ast.Call(self._slot(node.target), [value], [])
            )
        if isinstance(node, IsNull):
            statements, value = self._expression(node.value, row)
            comparison = ast.Compare(
                value,
                [ast.IsNot() if node.negate else ast.Is()],
                [ast.Constant(None)],
            )
            return statements, self._assign_value(statements, comparison)
        if isinstance(node, Call):
            return self._call(node, row)
        if isinstance(node, Coalesce):
            return self._coalesce(node, row)
        if isinstance(node, IfElse):
            return self._if_else(node, row)
        raise ValueError(f"unsupported row node {type(node).__name__}")

    def _field(self, node: Field, row: ast.expr) -> tuple[list[ast.stmt], ast.expr]:
        """Wrap only the exact dictionary lookup, never a later user operator failure."""
        value_name = self._temp("field")
        error_name = self._temp("lookup_error")
        message = f"Could not resolve selector {node.name!r}; failed at {node.name!r}"
        statement = ast.Try(
            body=[
                ast.Assign(
                    [ast.Name(id=value_name, ctx=ast.Store())],
                    ast.Subscript(row, ast.Constant(node.name), ast.Load()),
                )
            ],
            handlers=[
                ast.ExceptHandler(
                    type=ast.Tuple(
                        [
                            self._slot(AttributeError),
                            self._slot(KeyError),
                            self._slot(TypeError),
                        ],
                        ast.Load(),
                    ),
                    name=error_name,
                    body=[
                        ast.Raise(
                            ast.Call(self._slot(SelectionError), [ast.Constant(message)], []),
                            ast.Name(id=error_name, ctx=ast.Load()),
                        )
                    ],
                )
            ],
            orelse=[],
            finalbody=[],
        )
        return [statement], ast.Name(id=value_name, ctx=ast.Load())

    def _binary(self, node: Binary, row: ast.expr) -> tuple[list[ast.stmt], ast.expr]:
        """Preserve left-to-right evaluation, including strict-bool short circuiting."""
        left_statements, left = self._expression(node.left, row)
        if node.kind in {"and", "or"}:
            result_name = self._temp("boolean")
            right_statements, right = self._expression(node.right, row)
            truth = ast.Call(self._slot(bool), [left], [])
            condition: ast.expr
            if node.kind == "and":
                initial = ast.Constant(False)
                condition = truth
            else:
                initial = ast.Constant(True)
                condition = ast.UnaryOp(ast.Not(), truth)
            body = [*right_statements]
            body.append(
                ast.Assign(
                    [ast.Name(id=result_name, ctx=ast.Store())],
                    ast.Call(self._slot(bool), [right], []),
                )
            )
            left_statements.extend(
                [
                    ast.Assign([ast.Name(id=result_name, ctx=ast.Store())], initial),
                    ast.If(condition, body, []),
                ]
            )
            return left_statements, ast.Name(id=result_name, ctx=ast.Load())

        right_statements, right = self._expression(node.right, row)
        statements = [*left_statements, *right_statements]
        if node.kind in _ARITHMETIC_OPERATORS:
            value: ast.expr = ast.BinOp(left, _ARITHMETIC_OPERATORS[node.kind], right)
        elif node.kind in _COMPARISON_OPERATORS:
            value = ast.Compare(left, [_COMPARISON_OPERATORS[node.kind]], [right])
        else:
            raise ValueError(f"unsupported binary operator {node.kind!r}")
        return statements, self._assign_value(statements, value)

    def _call(self, node: Call, row: ast.expr) -> tuple[list[ast.stmt], ast.expr]:
        """Evaluate call arguments in order before invoking the supported closed operation."""
        statements: list[ast.stmt] = []
        arguments: list[ast.expr] = []
        for argument in node.arguments:
            argument_statements, value = self._expression(argument, row)
            statements.extend(argument_statements)
            arguments.append(value)
        if node.kind in {"lower", "upper", "strip"} and len(arguments) == 1:
            result: ast.expr = ast.Call(ast.Attribute(arguments[0], node.kind, ast.Load()), [], [])
        elif node.kind == "contains" and len(arguments) == 2:
            result = ast.Compare(arguments[1], [ast.In()], [arguments[0]])
        elif node.kind == "isin" and len(arguments) == 2:
            result = ast.Compare(arguments[0], [ast.In()], [arguments[1]])
        else:
            raise ValueError(f"unsupported row call {node.kind!r}")
        return statements, self._assign_value(statements, result)

    def _coalesce(self, node: Coalesce, row: ast.expr) -> tuple[list[ast.stmt], ast.expr]:
        """Evaluate later candidates only while the retained value is exactly None."""
        if not node.values:
            raise ValueError("empty coalesce graph")
        statements, first = self._expression(node.values[0], row)
        result_name = self._temp("coalesce")
        statements.append(ast.Assign([ast.Name(id=result_name, ctx=ast.Store())], first))
        for child in node.values[1:]:
            child_statements, value = self._expression(child, row)
            child_statements.append(ast.Assign([ast.Name(id=result_name, ctx=ast.Store())], value))
            statements.append(
                ast.If(
                    ast.Compare(
                        ast.Name(id=result_name, ctx=ast.Load()),
                        [ast.Is()],
                        [ast.Constant(None)],
                    ),
                    child_statements,
                    [],
                )
            )
        return statements, ast.Name(id=result_name, ctx=ast.Load())

    def _if_else(self, node: IfElse, row: ast.expr) -> tuple[list[ast.stmt], ast.expr]:
        """Evaluate only the selected branch after one truth test of the condition."""
        statements, condition = self._expression(node.condition, row)
        yes_statements, yes = self._expression(node.yes, row)
        no_statements, no = self._expression(node.no, row)
        result_name = self._temp("conditional")
        yes_statements.append(ast.Assign([ast.Name(id=result_name, ctx=ast.Store())], yes))
        no_statements.append(ast.Assign([ast.Name(id=result_name, ctx=ast.Store())], no))
        statements.append(
            ast.If(
                ast.Call(self._slot(bool), [condition], []),
                yes_statements,
                no_statements,
            )
        )
        return statements, ast.Name(id=result_name, ctx=ast.Load())

    def _assign_value(self, statements: list[ast.stmt], value: ast.expr) -> ast.expr:
        """Append one assignment so every user operation completes before later lookups."""
        name = self._temp("value")
        statements.append(ast.Assign([ast.Name(id=name, ctx=ast.Store())], value))
        return ast.Name(id=name, ctx=ast.Load())

    def _slot(self, value: Any) -> ast.Name:
        """Bind an external object by identity without rendering it into compiler source."""
        index = len(self._slots)
        self._slots.append(value)
        return ast.Name(id=f"_fpstreams_slot_{index}", ctx=ast.Load())

    def _temp(self, role: str) -> str:
        name = f"_fpstreams_{role}_{self._temporary}"
        self._temporary += 1
        return name

    @staticmethod
    def _name(name: str) -> ast.Name:
        return ast.Name(id=name, ctx=ast.Load())


def _is_bounded_exact_graph(root: object) -> bool:
    """Reject opaque/path/index graphs and bound statement expansion before recursion."""
    occurrences = 0
    stack: list[tuple[object, int]] = [(root, 1)]
    while stack:
        node, depth = stack.pop()
        occurrences += 1
        if depth > _MAX_DEPTH or occurrences > _MAX_OCCURRENCES:
            return False
        children = _exact_children(node)
        if children is None:
            return False
        stack.extend((child, depth + 1) for child in reversed(children))
    return True


def _exact_children(node: object) -> tuple[object, ...] | None:
    """Return children only for nodes whose exact-dict semantics can be emitted safely."""
    if isinstance(node, (InputRow, Literal)):
        return ()
    if isinstance(node, Field):
        return () if type(node.name) is str and "." not in node.name else None
    if isinstance(node, Unary):
        return (node.operand,) if node.kind in {"not", "neg", "abs"} else None
    if isinstance(node, Binary):
        if node.kind in _ARITHMETIC_OPERATORS or node.kind in _COMPARISON_OPERATORS:
            return (node.left, node.right)
        return (node.left, node.right) if node.kind in {"and", "or"} else None
    if isinstance(node, Cast):
        return (node.value,) if any(node.target is target for target in _EXACT_CASTS) else None
    if isinstance(node, IsNull):
        return (node.value,)
    if isinstance(node, Coalesce):
        return node.values if node.values else None
    if isinstance(node, Call):
        arity = {"lower": 1, "upper": 1, "strip": 1, "contains": 2, "isin": 2}
        return node.arguments if arity.get(node.kind) == len(node.arguments) else None
    if isinstance(node, IfElse):
        return (node.condition, node.yes, node.no)
    # Path, Index, GetItem, PythonUDF, and unknown third-party nodes retain the canonical
    # selector/evaluator implementation.  Those paths depend on richer object protocols.
    return None
