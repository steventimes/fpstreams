"""Lower canonical pair-wide RowExpr graphs to the shared native i64 program."""

from __future__ import annotations

import builtins
from types import FunctionType

from .._provenance import (
    builtin_endpoints_are_live,
    capture_builtin_endpoints,
)
from ..expressions.row import RowExpr
from ..expressions.row_eval import LazyRowEvaluator, RowProgram, cached_row_program
from ..expressions.row_ir import Binary, Index, Unary
from ..expressions.row_ir import Literal as RowLiteral
from ..expressions.scalar import _OPCODES
from ..expressions.selectors import compile_selector

PairI64Program = tuple[tuple[int, int], ...]

_BUILTIN_ANY = builtins.any
_BUILTIN_BOOL = builtins.bool
_BUILTIN_DICT: type[dict[object, object]] = builtins.dict
_BUILTIN_FROZENSET = builtins.frozenset
_BUILTIN_GETATTR = builtins.getattr
_BUILTIN_GLOBALS = builtins.globals
_BUILTIN_ID = builtins.id
_BUILTIN_INT: type[int] = builtins.int
_BUILTIN_LEN = builtins.len
_BUILTIN_OBJECT = builtins.object
_BUILTIN_REVERSED = builtins.reversed
_BUILTIN_STR: type[str] = builtins.str
_BUILTIN_TUPLE: type[tuple[object, ...]] = builtins.tuple
_BUILTIN_TYPE = builtins.type
_BUILTIN_VALUE_ERROR = builtins.ValueError
_BUILTIN_ZIP = builtins.zip
_MODULE_GLOBALS = _BUILTIN_GLOBALS()
_CANONICAL_BUILTIN_ENDPOINTS_ARE_LIVE = builtin_endpoints_are_live
_PAIR_BUILTINS = capture_builtin_endpoints(
    ("_BUILTIN_ANY", _BUILTIN_ANY, "any"),
    ("_BUILTIN_BOOL", _BUILTIN_BOOL, "bool"),
    ("_BUILTIN_DICT", _BUILTIN_DICT, "dict"),
    ("_BUILTIN_FROZENSET", _BUILTIN_FROZENSET, "frozenset"),
    ("_BUILTIN_GETATTR", _BUILTIN_GETATTR, "getattr"),
    ("_BUILTIN_GLOBALS", _BUILTIN_GLOBALS, "globals"),
    ("_BUILTIN_ID", _BUILTIN_ID, "id"),
    ("_BUILTIN_INT", _BUILTIN_INT, "int"),
    ("_BUILTIN_LEN", _BUILTIN_LEN, "len"),
    ("_BUILTIN_OBJECT", _BUILTIN_OBJECT, "object"),
    ("_BUILTIN_REVERSED", _BUILTIN_REVERSED, "reversed"),
    ("_BUILTIN_STR", _BUILTIN_STR, "str"),
    ("_BUILTIN_TUPLE", _BUILTIN_TUPLE, "tuple"),
    ("_BUILTIN_TYPE", _BUILTIN_TYPE, "type"),
    ("_BUILTIN_VALUE_ERROR", _BUILTIN_VALUE_ERROR, "ValueError"),
    ("_BUILTIN_ZIP", _BUILTIN_ZIP, "zip"),
    revalidate=True,
)
_CANONICAL_PAIR_BUILTINS = _PAIR_BUILTINS
_MAX_DEPTH = 128
_MAX_OCCURRENCES = 512
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1
_PAIR_KEY_OPCODE = 19
_ADD_OPCODE = _OPCODES["add"]
_SUB_OPCODE = _OPCODES["sub"]
_MUL_OPCODE = _OPCODES["mul"]
_FLOORDIV_OPCODE = _OPCODES["floordiv"]
_MOD_OPCODE = _OPCODES["mod"]
_EQ_OPCODE = _OPCODES["eq"]
_NE_OPCODE = _OPCODES["ne"]
_LT_OPCODE = _OPCODES["lt"]
_LE_OPCODE = _OPCODES["le"]
_GT_OPCODE = _OPCODES["gt"]
_GE_OPCODE = _OPCODES["ge"]
_NEG_OPCODE = _OPCODES["neg"]
_NOT_OPCODE = _OPCODES["not"]
_ITEM_OPCODE = _OPCODES["item"]
_CONST_OPCODE = _OPCODES["const"]
_CANONICAL_OPCODE_CONSTANTS = (
    ("_PAIR_KEY_OPCODE", _PAIR_KEY_OPCODE),
    ("_ADD_OPCODE", _ADD_OPCODE),
    ("_SUB_OPCODE", _SUB_OPCODE),
    ("_MUL_OPCODE", _MUL_OPCODE),
    ("_FLOORDIV_OPCODE", _FLOORDIV_OPCODE),
    ("_MOD_OPCODE", _MOD_OPCODE),
    ("_EQ_OPCODE", _EQ_OPCODE),
    ("_NE_OPCODE", _NE_OPCODE),
    ("_LT_OPCODE", _LT_OPCODE),
    ("_LE_OPCODE", _LE_OPCODE),
    ("_GT_OPCODE", _GT_OPCODE),
    ("_GE_OPCODE", _GE_OPCODE),
    ("_NEG_OPCODE", _NEG_OPCODE),
    ("_NOT_OPCODE", _NOT_OPCODE),
    ("_ITEM_OPCODE", _ITEM_OPCODE),
    ("_CONST_OPCODE", _CONST_OPCODE),
)


def _binary_opcode(kind: str) -> int | None:
    """Return the closed native opcode for one exact pair binary kind."""
    match kind:
        case "+":
            return _ADD_OPCODE
        case "-":
            return _SUB_OPCODE
        case "*":
            return _MUL_OPCODE
        case "//":
            return _FLOORDIV_OPCODE
        case "%":
            return _MOD_OPCODE
        case "==":
            return _EQ_OPCODE
        case "!=":
            return _NE_OPCODE
        case "<":
            return _LT_OPCODE
        case "<=":
            return _LE_OPCODE
        case ">":
            return _GT_OPCODE
        case ">=":
            return _GE_OPCODE
        case _:
            return None


def _unary_opcode(kind: str) -> int | None:
    """Return the closed native opcode for one exact pair unary kind."""
    match kind:
        case "neg":
            return _NEG_OPCODE
        case "not":
            return _NOT_OPCODE
        case _:
            return None


_CANONICAL_BINARY_OPCODE = _binary_opcode
_CANONICAL_BINARY_OPCODE_CODE = _binary_opcode.__code__
_CANONICAL_UNARY_OPCODE = _unary_opcode
_CANONICAL_UNARY_OPCODE_CODE = _unary_opcode.__code__
_GROUP_ARITHMETIC_OPCODES = _BUILTIN_FROZENSET(
    {
        _PAIR_KEY_OPCODE,
        _ITEM_OPCODE,
        _CONST_OPCODE,
        _ADD_OPCODE,
        _SUB_OPCODE,
        _MUL_OPCODE,
        _FLOORDIV_OPCODE,
        _MOD_OPCODE,
        _NEG_OPCODE,
    }
)
_GROUP_COMPUTED_KEY_OPCODES = _BUILTIN_FROZENSET(
    {
        _ADD_OPCODE,
        _SUB_OPCODE,
        _MUL_OPCODE,
        _FLOORDIV_OPCODE,
        _NEG_OPCODE,
    }
)
_CANONICAL_ROW_EXPR_CALL = RowExpr.__call__
_CANONICAL_ROW_EXPR_CALL_CODE = RowExpr.__call__.__code__
_CANONICAL_ROW_EXPR_GETATTRIBUTE = RowExpr.__getattribute__
_CANONICAL_LAZY_ROW_EVALUATOR_CALL = LazyRowEvaluator.__call__
_CANONICAL_LAZY_ROW_EVALUATOR_CALL_CODE = LazyRowEvaluator.__call__.__code__
_CANONICAL_LAZY_ROW_EVALUATOR_GETATTRIBUTE = LazyRowEvaluator.__getattribute__
_CANONICAL_ROW_PROGRAM_CALL = RowProgram.__call__
_CANONICAL_ROW_PROGRAM_CALL_CODE = RowProgram.__call__.__code__
_CANONICAL_CACHED_ROW_PROGRAM = cached_row_program
_CANONICAL_CACHED_ROW_PROGRAM_CODE = cached_row_program.__code__
_CANONICAL_INDEX_SELECTOR = compile_selector(0)
_CANONICAL_INDEX_SELECTOR_CODE = _CANONICAL_INDEX_SELECTOR.__code__
_CANONICAL_SELECTOR_GLOBALS = compile_selector.__globals__
_CANONICAL_SELECTOR_BUILTINS = _BUILTIN_GETATTR(compile_selector, "__builtins__", None)
_MISSING_SLOTS = _BUILTIN_OBJECT()


def _pair_runtime_is_canonical() -> bool:
    """Reject a pair kernel if any import-time implementation primitive was polluted."""
    return (
        _CANONICAL_BUILTIN_ENDPOINTS_ARE_LIVE(
            _MODULE_GLOBALS,
            "_PAIR_BUILTINS",
            _CANONICAL_PAIR_BUILTINS,
        )
        is True
    )


_CANONICAL_PAIR_RUNTIME_IS_CANONICAL = _pair_runtime_is_canonical
_CANONICAL_PAIR_RUNTIME_IS_CANONICAL_CODE = _pair_runtime_is_canonical.__code__


def _cached_pair_manifest(
    program: RowProgram,
) -> tuple[object, dict[int, tuple[object, type[object], dict[str, object]]]] | None:
    """Return identity-indexed immutable graph fields from central provenance."""
    manifest = program.graph_manifest
    if _BUILTIN_TYPE(manifest) is not _BUILTIN_TUPLE or _BUILTIN_LEN(manifest) != 2:
        return None
    nodes, entries = manifest
    if (
        _BUILTIN_TYPE(nodes) is not _BUILTIN_TUPLE
        or not nodes
        or _BUILTIN_TYPE(entries) is not _BUILTIN_TUPLE
    ):
        return None
    views: dict[int, tuple[object, type[object], dict[str, object]]] = {}
    for entry in entries:
        if _BUILTIN_TYPE(entry) is not _BUILTIN_TUPLE or _BUILTIN_LEN(entry) != 3:
            return None
        node, node_type, attributes = entry
        if (
            _BUILTIN_TYPE(node_type) is not _BUILTIN_TYPE
            or _BUILTIN_TYPE(attributes) is not _BUILTIN_TUPLE
        ):
            return None
        attribute_map: dict[str, object] = {}
        for attribute in attributes:
            if _BUILTIN_TYPE(attribute) is not _BUILTIN_TUPLE or _BUILTIN_LEN(attribute) != 2:
                return None
            name, value = attribute
            if _BUILTIN_TYPE(name) is not _BUILTIN_STR:
                return None
            attribute_map[name] = value
        views[_BUILTIN_ID(node)] = (node, node_type, attribute_map)
    return nodes[0], views


def _cached_pair_program_matches_ir(program: RowProgram) -> bool:  # noqa: C901
    """Require generated selector/literal slots to encode the retained pair IR exactly."""
    manifest = _cached_pair_manifest(program)
    if manifest is None:
        return False
    root, views = manifest
    expression = program.expression
    if _BUILTIN_TYPE(expression) is not FunctionType:
        return False
    names = expression.__code__.co_names
    if (
        _BUILTIN_LEN(names) != 1
        or _BUILTIN_TYPE(names[0]) is not _BUILTIN_STR
        or not names[0].startswith("_fpstreams_slots_")
        or _BUILTIN_TYPE(expression.__globals__) is not _BUILTIN_DICT
    ):
        return False
    slots = expression.__globals__.get(names[0], _MISSING_SLOTS)
    if _BUILTIN_TYPE(slots) is not _BUILTIN_TUPLE:
        return False

    expected: list[tuple[int, object]] = []
    pending = [root]
    occurrences = 0
    while pending:
        node = pending.pop()
        occurrences += 1
        if occurrences > _MAX_OCCURRENCES:
            return False
        view = views.get(_BUILTIN_ID(node))
        if view is None or view[0] is not node:
            return False
        _trusted_node, node_type, attributes = view
        if node_type is Index:
            expected.append((0, attributes.get("index")))
        elif node_type is RowLiteral:
            expected.append((1, attributes.get("value")))
        elif node_type is Unary:
            pending.append(attributes.get("operand"))
        elif node_type is Binary:
            pending.extend((attributes.get("right"), attributes.get("left")))
        else:
            return False
    if _BUILTIN_LEN(slots) != _BUILTIN_LEN(expected):
        return False

    for value, (kind, expected_value) in _BUILTIN_ZIP(slots, expected, strict=True):
        if kind == 1:
            if value is not expected_value:
                return False
            continue
        closure = value.__closure__ if _BUILTIN_TYPE(value) is FunctionType else None
        if (
            _BUILTIN_TYPE(value) is not FunctionType
            or value.__code__ is not _CANONICAL_INDEX_SELECTOR_CODE
            or value.__globals__ is not _CANONICAL_SELECTOR_GLOBALS
            or _BUILTIN_GETATTR(value, "__builtins__", None) is not _CANONICAL_SELECTOR_BUILTINS
            or value.__defaults__ is not None
            or value.__kwdefaults__ is not None
            or value.__code__.co_freevars != ("selector",)
            or _BUILTIN_TYPE(closure) is not _BUILTIN_TUPLE
            or _BUILTIN_LEN(closure) != 1
        ):
            return False
        try:
            selector = closure[0].cell_contents
        except _BUILTIN_VALUE_ERROR:
            return False
        if _BUILTIN_TYPE(selector) is not _BUILTIN_INT or selector != expected_value:
            return False
    return True


_CANONICAL_CACHED_PAIR_PROGRAM_MATCHES_IR = _cached_pair_program_matches_ir
_CANONICAL_CACHED_PAIR_PROGRAM_MATCHES_IR_CODE = _cached_pair_program_matches_ir.__code__


def _cached_row_program_is_canonical(evaluator: LazyRowEvaluator) -> bool:
    """Accept an untouched program compiled and recorded by this evaluator."""
    program = evaluator._program
    if program is None:
        return True
    if _CANONICAL_CACHED_ROW_PROGRAM.__code__ is not _CANONICAL_CACHED_ROW_PROGRAM_CODE:
        return False
    trusted = _CANONICAL_CACHED_ROW_PROGRAM(evaluator)
    return _BUILTIN_BOOL(
        trusted is program
        and _MODULE_GLOBALS.get("_cached_pair_program_matches_ir")
        is _CANONICAL_CACHED_PAIR_PROGRAM_MATCHES_IR
        and _CANONICAL_CACHED_PAIR_PROGRAM_MATCHES_IR.__code__
        is _CANONICAL_CACHED_PAIR_PROGRAM_MATCHES_IR_CODE
        and _CANONICAL_CACHED_PAIR_PROGRAM_MATCHES_IR(program)
    )


def _canonical_row_evaluator(expression: object) -> LazyRowEvaluator | None:
    """Return the untouched evaluator shared by pair-i64 lowering lanes."""
    if (
        _BUILTIN_TYPE(expression) is not RowExpr
        or RowExpr.__getattribute__ is not _CANONICAL_ROW_EXPR_GETATTRIBUTE
        or LazyRowEvaluator.__getattribute__ is not _CANONICAL_LAZY_ROW_EVALUATOR_GETATTRIBUTE
    ):
        return None
    row_expression = expression
    evaluator = row_expression._evaluate
    if not (
        RowExpr.__dict__.get("__call__") is _CANONICAL_ROW_EXPR_CALL
        and _CANONICAL_ROW_EXPR_CALL.__code__ is _CANONICAL_ROW_EXPR_CALL_CODE
        and _BUILTIN_TYPE(evaluator) is LazyRowEvaluator
        and evaluator.node is row_expression._node
        and LazyRowEvaluator.__dict__.get("__call__") is _CANONICAL_LAZY_ROW_EVALUATOR_CALL
        and _CANONICAL_LAZY_ROW_EVALUATOR_CALL.__code__ is _CANONICAL_LAZY_ROW_EVALUATOR_CALL_CODE
    ):
        return None
    return evaluator


def pair_i64_row_expr_is_canonical(expression: object) -> bool:
    """Keep grouped lowering on its established uncompiled RowExpr boundary."""
    evaluator = _canonical_row_evaluator(expression)
    return evaluator is not None and evaluator._program is None


def pair_i64_row_filter_expr_is_canonical(expression: object) -> bool:
    """Accept fresh or provenance-matched cached RowExpr programs for filtering."""
    evaluator = _canonical_row_evaluator(expression)
    return _BUILTIN_BOOL(
        evaluator is not None
        and _cached_row_program_is_canonical(evaluator)
        and RowProgram.__dict__.get("__call__") is _CANONICAL_ROW_PROGRAM_CALL
        and _CANONICAL_ROW_PROGRAM_CALL.__code__ is _CANONICAL_ROW_PROGRAM_CALL_CODE
    )


def _pair_node_view(
    node: object,
    views: dict[int, tuple[object, type[object], dict[str, object]]] | None,
) -> tuple[type[object], dict[str, object]] | None:
    """Read cached fields from a manifest, or exact live fields for a fresh graph."""
    if views is not None:
        view = views.get(_BUILTIN_ID(node))
        if view is None or view[0] is not node:
            return None
        return view[1], view[2]
    node_type = _BUILTIN_TYPE(node)
    if node_type is Index:
        index_node: Index = node  # type: ignore[assignment]
        return node_type, {"index": index_node.index}
    if node_type is RowLiteral:
        literal_node: RowLiteral = node  # type: ignore[assignment]
        return node_type, {"value": literal_node.value}
    if node_type is Unary:
        unary: Unary = node  # type: ignore[assignment]
        return node_type, {"kind": unary.kind, "operand": unary.operand}
    if node_type is Binary:
        binary: Binary = node  # type: ignore[assignment]
        return node_type, {"kind": binary.kind, "left": binary.left, "right": binary.right}
    return None


def _children(
    node: object,
    views: dict[int, tuple[object, type[object], dict[str, object]]] | None = None,
) -> tuple[object, ...] | None:
    """Return children for the exact i64 subset without invoking user protocols."""
    view = _pair_node_view(node, views)
    if view is None:
        return None
    node_type, attributes = view
    if node_type is Index:
        index = attributes.get("index")
        return () if _BUILTIN_TYPE(index) is _BUILTIN_INT and index in (0, 1) else None
    if node_type is RowLiteral:
        value = attributes.get("value")
        return (
            () if _BUILTIN_TYPE(value) is _BUILTIN_INT and _I64_MIN <= value <= _I64_MAX else None
        )
    if node_type is Unary:
        kind = attributes.get("kind")
        opcode = _CANONICAL_UNARY_OPCODE(kind) if _BUILTIN_TYPE(kind) is _BUILTIN_STR else None
        return (
            (attributes.get("operand"),)
            if _BUILTIN_TYPE(kind) is _BUILTIN_STR and opcode is not None
            else None
        )
    if node_type is Binary:
        kind = attributes.get("kind")
        opcode = _CANONICAL_BINARY_OPCODE(kind) if _BUILTIN_TYPE(kind) is _BUILTIN_STR else None
        if _BUILTIN_TYPE(kind) is _BUILTIN_STR and opcode is not None:
            return attributes.get("left"), attributes.get("right")
    return None


def lower_pair_i64_expression(  # noqa: C901 - iterative closed-IR compiler
    expression: object,
    *,
    require_pair_reference: bool = False,
    allow_cached_program: bool = False,
) -> PairI64Program | None:
    """Lower the closed pair-i64 RowExpr subset to native postfix instructions.

    Grouped expressions may be constant-only. Pair filters retain their historic
    requirement that a program actually reference one of the two input columns.
    """
    if not (
        _MODULE_GLOBALS.get("_pair_runtime_is_canonical") is _CANONICAL_PAIR_RUNTIME_IS_CANONICAL
        and _CANONICAL_PAIR_RUNTIME_IS_CANONICAL.__code__
        is _CANONICAL_PAIR_RUNTIME_IS_CANONICAL_CODE
        and _CANONICAL_PAIR_RUNTIME_IS_CANONICAL()
        and _CANONICAL_BINARY_OPCODE.__code__ is _CANONICAL_BINARY_OPCODE_CODE
        and _CANONICAL_UNARY_OPCODE.__code__ is _CANONICAL_UNARY_OPCODE_CODE
        and not _BUILTIN_ANY(
            _BUILTIN_TYPE(_MODULE_GLOBALS.get(name)) is not _BUILTIN_INT
            or _MODULE_GLOBALS.get(name) != opcode
            for name, opcode in _CANONICAL_OPCODE_CONSTANTS
        )
        and (
            pair_i64_row_filter_expr_is_canonical(expression)
            if allow_cached_program
            else pair_i64_row_expr_is_canonical(expression)
        )
    ):
        return None
    row_expression: RowExpr = expression  # type: ignore[assignment]

    cached_views: dict[int, tuple[object, type[object], dict[str, object]]] | None = None
    root = row_expression._node
    if allow_cached_program:
        evaluator = row_expression._evaluate
        candidate = evaluator._program if _BUILTIN_TYPE(evaluator) is LazyRowEvaluator else None
        if candidate is not None:
            trusted = _CANONICAL_CACHED_ROW_PROGRAM(evaluator)
            if trusted is not candidate:
                return None
            manifest = _cached_pair_manifest(trusted)
            if manifest is None:
                return None
            root, cached_views = manifest

    instructions: list[tuple[int, int]] = []
    references_pair = False
    occurrences = 0
    pending: list[tuple[object, int, bool]] = [(root, 1, False)]
    while pending:
        node, depth, visited = pending.pop()
        if not visited:
            occurrences += 1
            if depth > _MAX_DEPTH or occurrences > _MAX_OCCURRENCES:
                return None
            children = _children(node, cached_views)
            if children is None:
                return None
            pending.append((node, depth, True))
            pending.extend((child, depth + 1, False) for child in _BUILTIN_REVERSED(children))
            continue
        view = _pair_node_view(node, cached_views)
        if view is None:  # pragma: no cover - validated on first visit
            return None
        node_type, attributes = view
        if node_type is Index:
            references_pair = True
            opcode = _PAIR_KEY_OPCODE if attributes["index"] == 0 else _ITEM_OPCODE
            instructions.append((opcode, 0))
        elif node_type is RowLiteral:
            literal = attributes["value"]
            if _BUILTIN_TYPE(literal) is not _BUILTIN_INT:
                return None
            instructions.append((_CONST_OPCODE, literal))
        elif node_type is Unary:
            kind = attributes["kind"]
            unary_opcode = (
                _CANONICAL_UNARY_OPCODE(kind) if _BUILTIN_TYPE(kind) is _BUILTIN_STR else None
            )
            if unary_opcode is None:  # pragma: no cover - validated by _children
                return None
            instructions.append((unary_opcode, 0))
        else:
            kind = attributes["kind"]
            binary_opcode = (
                _CANONICAL_BINARY_OPCODE(kind) if _BUILTIN_TYPE(kind) is _BUILTIN_STR else None
            )
            if binary_opcode is None:  # pragma: no cover - validated by _children
                return None
            instructions.append((binary_opcode, 0))
    if require_pair_reference and not references_pair:
        return None
    return (*instructions,)


def lower_pair_i64_group_key(expression: object) -> PairI64Program | None:
    """Lower a computed integer key while excluding observable literal/leaf identity."""
    program = lower_pair_i64_expression(expression, require_pair_reference=True)
    if program is None or _BUILTIN_ANY(
        opcode not in _GROUP_ARITHMETIC_OPCODES for opcode, _ in program
    ):
        return None
    root_opcode = program[-1][0]
    if root_opcode in _GROUP_COMPUTED_KEY_OPCODES:
        return program
    # CPython may return the dividend object itself for exact-int modulo. The native kernel
    # can retain that identity only for this common, closed bucketization shape.
    if not (
        root_opcode == _MOD_OPCODE
        and _BUILTIN_LEN(program) == 3
        and program[0][0] in {_PAIR_KEY_OPCODE, _ITEM_OPCODE}
        and program[1][0] == _CONST_OPCODE
    ):
        return None
    return program


def lower_pair_i64_group_value(expression: object) -> PairI64Program | None:
    """Lower one pure arithmetic value program, including an exact-i64 constant."""
    program = lower_pair_i64_expression(expression)
    if program is None or _BUILTIN_ANY(
        opcode not in _GROUP_ARITHMETIC_OPCODES for opcode, _ in program
    ):
        return None
    return program


def lower_pair_i64_row_filter(expression: object) -> PairI64Program | None:
    """Lower a pair filter while preserving its established input-reference boundary."""
    return lower_pair_i64_expression(
        expression,
        require_pair_reference=True,
        allow_cached_program=True,
    )
