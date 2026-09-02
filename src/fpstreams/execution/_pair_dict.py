"""Consume structured pair-map tails directly into dictionaries."""

from __future__ import annotations

import builtins
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Literal, cast

from ..errors import DuplicateKeyError
from ..expressions import scalar as _scalar_expressions
from ..expressions.scalar import (
    _OPCODES,
    _UNARY_OPCODES,
    Expr,
    FExpr,
    _compile_float_evaluator,
    _compile_int_evaluator,
    _compile_scalar_callable,
)
from ..physical.plan import BackendPayload, PhysicalPlan
from ..planning._pair_stages import PAIR_KEY_SELECTOR, PairFilterDescriptor, PairMapDescriptor
from ..planning.logical import Pipeline
from ..planning.source import (
    _CANONICAL_RETAINED_SEQUENCE,
    _CANONICAL_SOURCE_OPEN,
    Source,
)
from ..planning.sync import FilterOp, MapOp, Operation, UniqueOp
from ..runtime.query import QueryRuntime
from . import sync_ops as _sync_ops

DuplicatePolicy = Literal["error", "first", "last"]
_BUILTIN_DICT: type[dict[Any, Any]] = builtins.dict
_BUILTIN_FLOAT: type[float] = builtins.float
_BUILTIN_INT: type[int] = builtins.int
_BUILTIN_LEN = builtins.len
_BUILTIN_LIST: type[list[Any]] = builtins.list
_BUILTIN_RANGE = builtins.range
_BUILTIN_SET: type[set[Any]] = builtins.set
_BUILTIN_STR: type[str] = builtins.str
_BUILTIN_TUPLE: type[tuple[Any, ...]] = builtins.tuple
_BUILTIN_TYPE = builtins.type
_LIST_ITERATOR_TYPE: type[Any] = _BUILTIN_TYPE(iter([]))
_TUPLE_ITERATOR_TYPE: type[Any] = _BUILTIN_TYPE(iter(()))
_PAIR_UNIQUE_MIN_ROWS = 32_768
_PAIR_VALUE_FILTER_MIN_ROWS = 1_024
_PAIR_VALUE_MAP_MIN_ROWS = 4_096
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1
# CPython rounds exact integers up to half an ulp beyond its largest finite float.
_F64_EXACT_INT_MAX = (1 << 1024) - (1 << 971) + (1 << 970) - 1
_PAIR_UNIQUE_PREFIX_SAMPLE = 128
_PAIR_UNIQUE_SPREAD_SAMPLE = 128
_CANONICAL_UNIQUE_HANDLER = _sync_ops.OPERATION_HANDLERS[UniqueOp]
_CANONICAL_EXPR_NATIVE_INSTRUCTIONS = Expr.native_instructions
_CANONICAL_EXPR_PYTHON_EVALUATOR = Expr._python_evaluator
_CANONICAL_FEXPR_NATIVE_INSTRUCTIONS = FExpr.native_instructions
_CANONICAL_FEXPR_PYTHON_EVALUATOR = FExpr._python_evaluator
_CANONICAL_COMPILE_INT_EVALUATOR = _compile_int_evaluator
_CANONICAL_COMPILE_FLOAT_EVALUATOR = _compile_float_evaluator
_CANONICAL_COMPILE_SCALAR_CALLABLE = _compile_scalar_callable
_PAIR_I64_FILTER_OPCODES = frozenset(_OPCODES.values()) - {_OPCODES["truediv"]}
_PAIR_F64_FILTER_OPCODES = frozenset(_OPCODES.values()) - {
    _OPCODES["floordiv"],
    _OPCODES["mod"],
}
_PAIR_FILTER_UNARY_OPCODES = frozenset(_UNARY_OPCODES)
_PAIR_FILTER_LEAF_OPCODES = frozenset((_OPCODES["item"], _OPCODES["const"]))
_PAIR_SCALAR_BOOL_OPCODES = frozenset(
    (
        _OPCODES["eq"],
        _OPCODES["ne"],
        _OPCODES["lt"],
        _OPCODES["le"],
        _OPCODES["gt"],
        _OPCODES["ge"],
        _OPCODES["and"],
        _OPCODES["or"],
        _OPCODES["not"],
    )
)
_PAIR_FLOAT_COMPARISON_OPCODES = frozenset(
    (
        _OPCODES["eq"],
        _OPCODES["ne"],
        _OPCODES["lt"],
        _OPCODES["le"],
        _OPCODES["gt"],
        _OPCODES["ge"],
    )
)
_PAIR_FLOAT_LOGICAL_OPCODES = frozenset((_OPCODES["and"], _OPCODES["or"]))
_PAIR_FLOAT_INTEGER_RESULT_OPCODES = frozenset((_OPCODES["add"], _OPCODES["sub"], _OPCODES["mul"]))


@dataclass(frozen=True, slots=True)
class PairDictConsumer:
    """Collect canonical pair iterators using one validated duplicate policy."""

    policy: DuplicatePolicy

    def __call__(self, iterator: Iterator[Any]) -> dict[Any, Any]:
        """Retain the public dictionary semantics on unspecialized execution paths."""
        result: dict[Any, Any] = {}
        if self.policy == "last":
            for key, value in iterator:
                result[key] = value
            return result

        if self.policy == "first":
            for key, value in iterator:
                if key not in result:
                    result[key] = value
            return result

        for key, value in iterator:
            if key in result:
                raise DuplicateKeyError(f"Duplicate key: {key!r}")
            result[key] = value
        return result


_CANONICAL_PAIR_DICT_CALL = PairDictConsumer.__call__


def is_canonical_pair_dict_consumer(consumer: object) -> bool:
    """Return whether a tail fusion can safely bypass the live consumer call."""
    if type(consumer) is not PairDictConsumer:
        return False
    policy = consumer.policy
    if type(policy) is not str:
        return False
    if policy not in ("error", "first", "last"):
        return False
    return PairDictConsumer.__dict__.get("__call__") is _CANONICAL_PAIR_DICT_CALL


def _sample_high_cardinality_pairs(source: list[Any] | tuple[Any, ...]) -> bool:
    """Recognize a representative exact-pair sample without invoking user protocols."""
    size = _BUILTIN_LEN(source)
    if size < _PAIR_UNIQUE_MIN_ROWS:
        return False

    keys: set[int | str] = _BUILTIN_SET()
    sampled = 0
    prefix_count = _PAIR_UNIQUE_PREFIX_SAMPLE if size >= _PAIR_UNIQUE_PREFIX_SAMPLE else size
    for index in _BUILTIN_RANGE(prefix_count):
        try:
            pair = source[index]
        except IndexError:
            return False
        if _BUILTIN_TYPE(pair) is not _BUILTIN_TUPLE or _BUILTIN_LEN(pair) != 2:
            return False
        key = pair[0]
        if _BUILTIN_TYPE(key) not in (_BUILTIN_INT, _BUILTIN_STR):
            return False
        keys.add(key)
        sampled += 1

    # If even an entirely distinct spread sample cannot reach the 75% cutoff, its outcome is
    # already decided. This keeps the low-cardinality path cheap without changing classification.
    if (_BUILTIN_LEN(keys) + _PAIR_UNIQUE_SPREAD_SAMPLE) * 4 < (
        sampled + _PAIR_UNIQUE_SPREAD_SAMPLE
    ) * 3:
        return False

    # Spread the second half over the whole retained source so both cyclic and clustered
    # low-cardinality layouts normally remain on the established set-backed iterator path.
    denominator = _PAIR_UNIQUE_SPREAD_SAMPLE + 1
    for position in _BUILTIN_RANGE(1, denominator):
        try:
            pair = source[(position * size) // denominator]
        except IndexError:
            return False
        if _BUILTIN_TYPE(pair) is not _BUILTIN_TUPLE or _BUILTIN_LEN(pair) != 2:
            return False
        key = pair[0]
        if _BUILTIN_TYPE(key) not in (_BUILTIN_INT, _BUILTIN_STR):
            return False
        keys.add(key)
        sampled += 1

    return _BUILTIN_LEN(keys) * 4 >= sampled * 3


_CANONICAL_SAMPLE_HIGH_CARDINALITY_PAIRS = _sample_high_cardinality_pairs


def _seeded_pair_unique(
    iterator: Iterator[Any],
    hashable: set[Any],
) -> Iterator[Any]:
    """Resume the canonical pair-key uniqueness loop from an exact-key prefix."""
    unhashable: list[Any] = []
    for item in iterator:
        key = item[0]
        try:
            if key in hashable:
                continue
            hashable.add(key)
        except TypeError:
            if any(key == seen for seen in unhashable):
                continue
            unhashable.append(key)
        yield item


def _prepend_pair_unique_boundary(first: Any, source: Iterator[Any]) -> Iterator[Any]:
    """Yield the native boundary once, then resume the still-open retained iterator."""
    yield first
    del first
    yield from source


def _consume_unique_suffix_into_dict(
    output: dict[Any, Any],
    iterator: Iterator[Any],
    policy: DuplicatePolicy,
) -> dict[Any, Any]:
    """Continue unique-by-key plus PairDictConsumer against one seeded result."""
    # `set(output)` bulk-presizes and therefore uses a different collision layout than the
    # canonical UniqueOp. Incremental insertion reproduces its growth history before a custom
    # suffix key can observe equality-comparison order.
    hashable: set[Any] = _BUILTIN_SET()
    for key in output:
        hashable.add(key)
    unique = _seeded_pair_unique(iterator, hashable)
    if policy == "last":
        for key, value in unique:
            output[key] = value
        return output
    if policy == "first":
        for key, value in unique:
            if key not in output:
                output[key] = value
        return output
    for key, value in unique:
        if key in output:
            raise DuplicateKeyError(f"Duplicate key: {key!r}")
        output[key] = value
    return output


def _consume_opened_canonical(
    source: Iterator[Any],
    operations: tuple[Operation, ...],
    consumer: PairDictConsumer,
    runtime: QueryRuntime,
    open_operations: Callable[..., Any],
) -> dict[Any, Any]:
    """Finish a post-open guard failure without opening the retained source twice."""
    with open_operations(source, operations, runtime=runtime) as iterator:
        return consumer(iterator)


def try_consume_pair_unique_to_dict(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    operations: tuple[Operation, ...],
    consumer: PairDictConsumer,
    open_operations: Callable[..., Any],
) -> tuple[bool, dict[Any, Any] | None]:
    """Adaptively push a retained high-cardinality unique-pair terminal into Rust."""
    from ..runtime.failpoints import has_active_failpoints
    from .sync import open_operations as canonical_open_operations

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or _BUILTIN_TYPE(pipeline.engine) is not builtins.str
        or pipeline.engine != "auto"
        or physical.source is not pipeline.source
        or _BUILTIN_TYPE(consumer) is not PairDictConsumer
        or PairDictConsumer.__dict__.get("__call__") is not _CANONICAL_PAIR_DICT_CALL
        or _BUILTIN_TYPE(consumer.policy) is not _BUILTIN_STR
        or consumer.policy not in ("error", "first", "last")
        or open_operations is not canonical_open_operations
        or _BUILTIN_TYPE(pipeline.source) is not Source
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or _BUILTIN_TYPE(operations) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(operations) != 1
        or _BUILTIN_TYPE(operation := operations[0]) is not UniqueOp
        or operation is not pipeline.operations[0]
        or operation.key is not PAIR_KEY_SELECTOR
        or _sync_ops.OPERATION_HANDLERS.get(UniqueOp) is not _CANONICAL_UNIQUE_HANDLER
    ):
        return False, None
    payload = physical.backend_payload
    if (
        not isinstance(payload, BackendPayload)
        or payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None
    retained = pipeline.source.retained_sequence()
    if _BUILTIN_TYPE(retained) not in (_BUILTIN_LIST, _BUILTIN_TUPLE):
        return False, None
    source_values = cast(list[Any] | tuple[Any, ...], retained)
    if (
        _BUILTIN_LEN(source_values) < _PAIR_UNIQUE_MIN_ROWS
        or has_active_failpoints()
        or not _CANONICAL_SAMPLE_HIGH_CARDINALITY_PAIRS(source_values)
    ):
        return False, None

    try:
        from .. import _native
    except ImportError:
        return False, None
    raw_endpoint = getattr(_native, "pair_unique_exact_prefix_v1", None)
    if not callable(raw_endpoint):
        return False, None
    endpoint = cast(
        Callable[
            [dict[Any, Any], Iterator[Any]],
            tuple[Any | None, bool] | None,
        ],
        raw_endpoint,
    )

    with QueryRuntime() as runtime:
        try:
            source_iterator = pipeline.source.open()
        except StopIteration as error:
            raise RuntimeError("generator raised StopIteration") from error
        source_iterator_owned = True
        active_error: BaseException | None = None
        try:
            physical_module = sys.modules.get("fpstreams.execution.physical")
            live_open_operations = (
                None if physical_module is None else physical_module.__dict__.get("open_operations")
            )
            live_endpoint = getattr(_native, "pair_unique_exact_prefix_v1", None)
            if (
                has_active_failpoints()
                or _BUILTIN_TYPE(consumer) is not PairDictConsumer
                or PairDictConsumer.__dict__.get("__call__") is not _CANONICAL_PAIR_DICT_CALL
                or _BUILTIN_TYPE(consumer.policy) is not _BUILTIN_STR
                or consumer.policy not in ("error", "first", "last")
                or _sync_ops.OPERATION_HANDLERS.get(UniqueOp) is not _CANONICAL_UNIQUE_HANDLER
                or _BUILTIN_TYPE(operation) is not UniqueOp
                or _BUILTIN_LEN(pipeline.operations) != 1
                or operation is not pipeline.operations[0]
                or operation.key is not PAIR_KEY_SELECTOR
                or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
                or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
                or live_open_operations is not canonical_open_operations
                or live_endpoint is not endpoint
                or _BUILTIN_TYPE(source_iterator) not in (_LIST_ITERATOR_TYPE, _TUPLE_ITERATOR_TYPE)
                or not _CANONICAL_SAMPLE_HIGH_CARDINALITY_PAIRS(source_values)
            ):
                source_iterator_owned = False
                return True, _consume_opened_canonical(
                    source_iterator,
                    operations,
                    consumer,
                    runtime,
                    cast(Callable[..., Any], live_open_operations),
                )

            output: dict[Any, Any] = _BUILTIN_DICT()
            native = endpoint(output, source_iterator)
            if native is None:
                source_iterator_owned = False
                return True, _consume_opened_canonical(
                    source_iterator,
                    operations,
                    consumer,
                    runtime,
                    canonical_open_operations,
                )
            first_incompatible, completed = native
            if completed:
                return True, output
            remaining = _prepend_pair_unique_boundary(first_incompatible, source_iterator)
            try:
                return True, _consume_unique_suffix_into_dict(output, remaining, consumer.policy)
            finally:
                cast(Any, remaining).close()
        except BaseException as error:
            active_error = error
            raise
        finally:
            from .sync_ops import close_iterators

            if source_iterator_owned:
                close_iterators((source_iterator,), active_error=active_error)


def consume_pair_map_to_dict(
    iterator: Iterator[Any],
    descriptor: PairMapDescriptor,
    policy: DuplicatePolicy,
) -> dict[Any, Any]:
    """Fuse a final key/value map with dictionary collection."""
    if descriptor.side == "pair":
        return _consume_mapped_pairs(
            iterator,
            descriptor.callback,
            cast(Literal["first", "last"], policy),
        )
    callback = _compile_scalar_callable(descriptor.callback)
    if descriptor.side == "value":
        return _consume_mapped_values(iterator, callback, policy)
    return _consume_mapped_keys(iterator, callback, policy)


def consume_pair_filter_to_dict(  # noqa: C901 - policy-local loops keep branches out of the sink
    iterator: Iterator[Any],
    descriptor: PairFilterDescriptor,
    policy: DuplicatePolicy,
) -> dict[Any, Any]:
    """Fuse a final two-argument pair filter with dictionary collection."""
    callback = descriptor.callback
    result: dict[Any, Any] = {}
    if policy == "last":
        for pair in iterator:
            try:
                predicate_result = callback(pair[0], pair[1])
            except StopIteration:
                return result
            try:
                if not predicate_result:
                    del predicate_result, pair
                    continue
            except StopIteration:
                return result
            except BaseException:
                del pair, predicate_result
                raise
            del predicate_result
            try:
                key, value = pair
                result[key] = value
            finally:
                del pair
        return result

    if policy == "first":
        for pair in iterator:
            try:
                predicate_result = callback(pair[0], pair[1])
            except StopIteration:
                return result
            try:
                if not predicate_result:
                    del predicate_result, pair
                    continue
            except StopIteration:
                return result
            except BaseException:
                del pair, predicate_result
                raise
            del predicate_result
            try:
                key, value = pair
                if key not in result:
                    result[key] = value
            finally:
                del pair
        return result

    for pair in iterator:
        try:
            predicate_result = callback(pair[0], pair[1])
        except StopIteration:
            return result
        try:
            if not predicate_result:
                del predicate_result, pair
                continue
        except StopIteration:
            return result
        except BaseException:
            del pair, predicate_result
            raise
        del predicate_result
        try:
            key, value = pair
            if key in result:
                raise DuplicateKeyError(f"Duplicate key: {key!r}")
            result[key] = value
        finally:
            del pair
    return result


def consume_pair_side_filter_to_dict(
    iterator: Iterator[Any],
    descriptor: PairFilterDescriptor,
    policy: Literal["first", "last"],
) -> dict[Any, Any]:
    """Fuse a final key- or value-only pair filter with common dictionary policies."""
    callback = _compile_scalar_callable(descriptor.callback)
    index = 0 if descriptor.target == "key" else 1
    result: dict[Any, Any] = {}
    if policy == "last":
        for pair in iterator:
            try:
                predicate_result = callback(pair[index])
            except StopIteration:
                return result
            try:
                if not predicate_result:
                    del predicate_result, pair
                    continue
            except StopIteration:
                return result
            except BaseException:
                del pair, predicate_result
                raise
            del predicate_result
            try:
                key, value = pair
                result[key] = value
            finally:
                del pair
        return result

    for pair in iterator:
        try:
            predicate_result = callback(pair[index])
        except StopIteration:
            return result
        try:
            if not predicate_result:
                del predicate_result, pair
                continue
        except StopIteration:
            return result
        except BaseException:
            del pair, predicate_result
            raise
        del predicate_result
        try:
            key, value = pair
            if key not in result:
                result[key] = value
        finally:
            del pair
    return result


_CANONICAL_CONSUME_PAIR_SIDE_FILTER = consume_pair_side_filter_to_dict


def _pair_value_filter_expression_kind(callback: object) -> Literal["i64", "f64"] | None:
    """Recognize one exact canonical scalar-expression graph without compiling it."""
    from ._scalar_fusion import _is_canonical_scalar_graph

    if _BUILTIN_TYPE(callback) is Expr:
        if (
            Expr.__dict__.get("native_instructions") is not _CANONICAL_EXPR_NATIVE_INSTRUCTIONS
            or Expr.__dict__.get("_python_evaluator") is not _CANONICAL_EXPR_PYTHON_EVALUATOR
            or not _is_canonical_scalar_graph(callback, Expr)
        ):
            return None
        return "i64"
    if _BUILTIN_TYPE(callback) is FExpr:
        if (
            FExpr.__dict__.get("native_instructions") is not _CANONICAL_FEXPR_NATIVE_INSTRUCTIONS
            or FExpr.__dict__.get("_python_evaluator") is not _CANONICAL_FEXPR_PYTHON_EVALUATOR
            or not _is_canonical_scalar_graph(callback, FExpr)
        ):
            return None
        return "f64"
    return None


def _validated_pair_value_filter_instructions(
    callback: Expr | FExpr,
    expression_kind: Literal["i64", "f64"],
) -> tuple[tuple[int, int], ...] | tuple[tuple[int, float], ...] | None:
    """Return an exact, well-formed postfix program without coercing cache contents."""
    instructions: Any = callback.native_instructions()
    if _BUILTIN_TYPE(instructions) is not _BUILTIN_TUPLE or not instructions:
        return None
    depth = 0
    integer = expression_kind == "i64"
    for instruction in instructions:
        if _BUILTIN_TYPE(instruction) is not _BUILTIN_TUPLE or _BUILTIN_LEN(instruction) != 2:
            return None
        opcode, operand = instruction
        if _BUILTIN_TYPE(opcode) is not _BUILTIN_INT:
            return None
        if integer:
            if _BUILTIN_TYPE(operand) is not _BUILTIN_INT or not -(1 << 63) <= operand < 1 << 63:
                return None
            if opcode not in _PAIR_I64_FILTER_OPCODES:
                return None
        else:
            if _BUILTIN_TYPE(operand) is not float or opcode not in _PAIR_F64_FILTER_OPCODES:
                return None
        if opcode in _PAIR_FILTER_LEAF_OPCODES:
            depth += 1
        elif opcode in _PAIR_FILTER_UNARY_OPCODES:
            if depth < 1:
                return None
        else:
            if depth < 2:
                return None
            depth -= 1
    if depth != 1:
        return None
    return cast(
        tuple[tuple[int, int], ...] | tuple[tuple[int, float], ...],
        instructions,
    )


def _validated_pair_value_filter_predicate(
    callback: Expr | FExpr,
    expression_kind: Literal["i64", "f64"],
    instructions: tuple[tuple[int, int], ...] | tuple[tuple[int, float], ...],
) -> Callable[[Any], Any] | None:
    """Snapshot the exact Python predicate represented by the native instructions."""
    if expression_kind == "i64":
        if (
            _scalar_expressions.__dict__.get("_compile_int_evaluator")
            is not _CANONICAL_COMPILE_INT_EVALUATOR
        ):
            return None
        expected = _CANONICAL_COMPILE_INT_EVALUATOR(cast(tuple[tuple[int, int], ...], instructions))
    else:
        if (
            _scalar_expressions.__dict__.get("_compile_float_evaluator")
            is not _CANONICAL_COMPILE_FLOAT_EVALUATOR
        ):
            return None
        expected = _CANONICAL_COMPILE_FLOAT_EVALUATOR(
            cast(tuple[tuple[int, float], ...], instructions)
        )
    predicate = _compile_scalar_callable(callback)
    return predicate if predicate is expected else None


def _validated_pair_float_map_result_is_bool(
    instructions: tuple[tuple[int, float], ...],
) -> bool | None:
    """Infer safe Python float/bool results and reject an intermediate integer result."""
    result_is_bool: list[bool] = []
    for opcode, _operand in instructions:
        if opcode in _PAIR_FILTER_LEAF_OPCODES:
            result_is_bool.append(False)
            continue
        if opcode in _PAIR_FILTER_UNARY_OPCODES:
            operand_is_bool = result_is_bool.pop()
            if opcode in (_OPCODES["neg"], _OPCODES["abs"]) and operand_is_bool:
                return None
            result_is_bool.append(opcode == _OPCODES["not"])
            continue
        right_is_bool = result_is_bool.pop()
        left_is_bool = result_is_bool.pop()
        if opcode in _PAIR_FLOAT_COMPARISON_OPCODES or opcode in _PAIR_FLOAT_LOGICAL_OPCODES:
            result_is_bool.append(True)
        elif opcode in _PAIR_FLOAT_INTEGER_RESULT_OPCODES:
            if left_is_bool and right_is_bool:
                return None
            result_is_bool.append(False)
        else:
            result_is_bool.append(False)
    return result_is_bool[0]


def _prepend_pair_value_filter_boundary(first: Any, source: Iterator[Any]) -> Iterator[Any]:
    """Yield one untouched native boundary, then the still-open source suffix."""
    yield first
    del first
    yield from source


def _consume_pair_value_filter_suffix(
    output: dict[Any, Any],
    iterator: Iterator[Any],
    predicate: Callable[[Any], Any],
    policy: Literal["first", "last"],
) -> dict[Any, Any]:
    """Resume the canonical value-filter sink from a caller-owned dictionary."""
    if policy == "last":
        for pair in iterator:
            try:
                predicate_result = predicate(pair[1])
            except StopIteration:
                return output
            try:
                if not predicate_result:
                    del predicate_result, pair
                    continue
            except StopIteration:
                return output
            except BaseException:
                del pair, predicate_result
                raise
            del predicate_result
            try:
                key, value = pair
                output[key] = value
            finally:
                del pair
        return output

    for pair in iterator:
        try:
            predicate_result = predicate(pair[1])
        except StopIteration:
            return output
        try:
            if not predicate_result:
                del predicate_result, pair
                continue
        except StopIteration:
            return output
        except BaseException:
            del pair, predicate_result
            raise
        del predicate_result
        try:
            key, value = pair
            if key not in output:
                output[key] = value
        finally:
            del pair
    return output


def try_consume_pair_value_filter_to_dict(  # noqa: C901 - ownership handoff stays explicit
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    operations: tuple[Operation, ...],
    consumer: PairDictConsumer,
    open_operations: Callable[..., Any],
) -> tuple[bool, dict[Any, Any] | None]:
    """Push one retained exact value-expression filter into its native dictionary sink."""
    from ..runtime.failpoints import has_active_failpoints
    from .sync import open_operations as canonical_open_operations

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or _BUILTIN_TYPE(pipeline.engine) is not _BUILTIN_STR
        or pipeline.engine != "auto"
        or physical.source is not pipeline.source
        or _BUILTIN_TYPE(consumer) is not PairDictConsumer
        or PairDictConsumer.__dict__.get("__call__") is not _CANONICAL_PAIR_DICT_CALL
        or _BUILTIN_TYPE(cast(object, consumer.policy)) is not _BUILTIN_STR
        or consumer.policy not in ("first", "last")
        or open_operations is not canonical_open_operations
        or _BUILTIN_TYPE(pipeline.source) is not Source
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or pipeline.source.capabilities.reiterable is not True
        or _BUILTIN_TYPE(operations) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(operations) != 1
        or _BUILTIN_TYPE(pipeline.operations) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(pipeline.operations) != 1
        or _BUILTIN_TYPE(operation := operations[0]) is not FilterOp
        or operation is not pipeline.operations[0]
        or operation.negate is not False
        or _BUILTIN_TYPE(descriptor := operation.predicate) is not PairFilterDescriptor
        or _BUILTIN_TYPE(cast(object, descriptor.target)) is not _BUILTIN_STR
        or descriptor.target != "value"
        or (expression_kind := _pair_value_filter_expression_kind(descriptor.callback)) is None
        or _compile_scalar_callable is not _CANONICAL_COMPILE_SCALAR_CALLABLE
    ):
        return False, None
    callback = cast(Expr | FExpr, descriptor.callback)
    payload = physical.backend_payload
    if (
        not isinstance(payload, BackendPayload)
        or payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None
    retained = pipeline.source.retained_sequence()
    if _BUILTIN_TYPE(retained) not in (_BUILTIN_LIST, _BUILTIN_TUPLE):
        return False, None
    source_values = cast(list[Any] | tuple[Any, ...], retained)
    if _BUILTIN_LEN(source_values) < _PAIR_VALUE_FILTER_MIN_ROWS or has_active_failpoints():
        return False, None

    try:
        from .. import _native
    except ImportError:
        return False, None
    endpoint_name = (
        "pair_i64_value_filter_to_dict_exact_prefix_v1"
        if expression_kind == "i64"
        else "pair_f64_value_filter_to_dict_exact_prefix_v1"
    )
    raw_endpoint = getattr(_native, endpoint_name, None)
    if not callable(raw_endpoint):
        return False, None
    endpoint = cast(Callable[..., tuple[Any | None, bool] | None], raw_endpoint)

    with QueryRuntime() as runtime:
        try:
            source_iterator = pipeline.source.open()
        except StopIteration as error:
            raise RuntimeError("generator raised StopIteration") from error
        source_iterator_owned = True
        active_error: BaseException | None = None
        try:
            physical_module = sys.modules.get("fpstreams.execution.physical")
            live_open_operations = (
                None if physical_module is None else physical_module.__dict__.get("open_operations")
            )
            live_endpoint = getattr(_native, endpoint_name, None)
            if (
                has_active_failpoints()
                or physical.root is not None
                or physical.parallel is not None
                or physical.source is not pipeline.source
                or _BUILTIN_TYPE(pipeline.engine) is not _BUILTIN_STR
                or pipeline.engine != "auto"
                or _BUILTIN_TYPE(consumer) is not PairDictConsumer
                or PairDictConsumer.__dict__.get("__call__") is not _CANONICAL_PAIR_DICT_CALL
                or _BUILTIN_TYPE(cast(object, consumer.policy)) is not _BUILTIN_STR
                or consumer.policy not in ("first", "last")
                or _BUILTIN_TYPE(operation) is not FilterOp
                or _BUILTIN_TYPE(pipeline.operations) is not _BUILTIN_TUPLE
                or _BUILTIN_LEN(pipeline.operations) != 1
                or operation is not pipeline.operations[0]
                or operation.negate is not False
                or _BUILTIN_TYPE(operation.predicate) is not PairFilterDescriptor
                or operation.predicate is not descriptor
                or _BUILTIN_TYPE(cast(object, descriptor.target)) is not _BUILTIN_STR
                or descriptor.target != "value"
                or descriptor.callback is not callback
                or _pair_value_filter_expression_kind(callback) != expression_kind
                or _compile_scalar_callable is not _CANONICAL_COMPILE_SCALAR_CALLABLE
                or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
                or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
                or _BUILTIN_LEN(source_values) < _PAIR_VALUE_FILTER_MIN_ROWS
                or live_open_operations is not canonical_open_operations
                or live_endpoint is not endpoint
                or _BUILTIN_TYPE(source_iterator) not in (_LIST_ITERATOR_TYPE, _TUPLE_ITERATOR_TYPE)
            ):
                source_iterator_owned = False
                return True, _consume_opened_canonical(
                    source_iterator,
                    operations,
                    consumer,
                    runtime,
                    cast(Callable[..., Any], live_open_operations),
                )
            live_retained = pipeline.source.retained_sequence()
            if live_retained is not source_values or _BUILTIN_TYPE(live_retained) not in (
                _BUILTIN_LIST,
                _BUILTIN_TUPLE,
            ):
                source_iterator_owned = False
                return True, _consume_opened_canonical(
                    source_iterator,
                    operations,
                    consumer,
                    runtime,
                    canonical_open_operations,
                )

            instructions = _validated_pair_value_filter_instructions(
                callback,
                expression_kind,
            )
            if instructions is None:
                source_iterator_owned = False
                return True, _consume_opened_canonical(
                    source_iterator,
                    operations,
                    consumer,
                    runtime,
                    canonical_open_operations,
                )
            predicate = _validated_pair_value_filter_predicate(
                callback,
                expression_kind,
                instructions,
            )
            if predicate is None:
                source_iterator_owned = False
                return True, _consume_opened_canonical(
                    source_iterator,
                    operations,
                    consumer,
                    runtime,
                    canonical_open_operations,
                )
            output: dict[Any, Any] = _BUILTIN_DICT()
            native = endpoint(
                output,
                source_iterator,
                instructions,
                consumer.policy == "first",
            )
            if native is None:
                source_iterator_owned = False
                return True, _consume_opened_canonical(
                    source_iterator,
                    operations,
                    consumer,
                    runtime,
                    canonical_open_operations,
                )
            first_incompatible, completed = native
            if completed:
                return True, output
            remaining = _prepend_pair_value_filter_boundary(first_incompatible, source_iterator)
            try:
                return True, _consume_pair_value_filter_suffix(
                    output,
                    remaining,
                    predicate,
                    consumer.policy,
                )
            finally:
                cast(Any, remaining).close()
        except BaseException as error:
            active_error = error
            raise
        finally:
            from .sync_ops import close_iterators

            if source_iterator_owned:
                close_iterators((source_iterator,), active_error=active_error)


def _consume_mapped_pairs(
    iterator: Iterator[Any],
    callback: Callable[[Any, Any], Any],
    policy: Literal["first", "last"],
) -> dict[Any, Any]:
    """Map complete pairs and collect callback outputs without an intermediate iterator."""
    result: dict[Any, Any] = {}
    if policy == "last":
        for pair in iterator:
            try:
                mapped = callback(pair[0], pair[1])
            except StopIteration:
                return result
            except BaseException:
                del pair
                raise
            del pair
            try:
                key, value = mapped
                result[key] = value
            finally:
                del mapped
        return result

    for pair in iterator:
        try:
            mapped = callback(pair[0], pair[1])
        except StopIteration:
            return result
        except BaseException:
            del pair
            raise
        del pair
        try:
            key, value = mapped
            if key not in result:
                result[key] = value
        finally:
            del mapped
    return result


def _consume_mapped_values(
    iterator: Iterator[Any],
    callback: Callable[[Any], Any],
    policy: DuplicatePolicy,
) -> dict[Any, Any]:
    """Map values and collect them without allocating intermediate pairs."""
    result: dict[Any, Any] = {}
    if policy == "last":
        for pair in iterator:
            try:
                key = pair[0]
                value = callback(pair[1])
            except StopIteration:
                return result
            del pair
            result[key] = value
        return result

    if policy == "first":
        for pair in iterator:
            try:
                key = pair[0]
                value = callback(pair[1])
            except StopIteration:
                return result
            del pair
            if key not in result:
                result[key] = value
        return result

    for pair in iterator:
        try:
            key = pair[0]
            value = callback(pair[1])
        except StopIteration:
            return result
        del pair
        if key in result:
            raise DuplicateKeyError(f"Duplicate key: {key!r}")
        result[key] = value
    return result


_CANONICAL_CONSUME_MAPPED_VALUES = _consume_mapped_values


@dataclass(frozen=True, slots=True)
class _PairValueMapSnapshot:
    """Pre-open identities required by the scalar value-map native sink."""

    operation: MapOp
    descriptor: PairMapDescriptor
    callback: Expr | FExpr
    expression_kind: Literal["i64", "f64"]
    source_values: list[Any] | tuple[Any, ...]
    endpoint_name: str
    endpoint: Callable[..., tuple[Any | None, bool] | None]


def _pair_value_map_first_row_compatible(
    source: list[Any] | tuple[Any, ...],
    expression_kind: Literal["i64", "f64"],
) -> bool:
    """Reject a known first-row boundary before paying the native transition cost."""
    row = source[0]
    if _BUILTIN_TYPE(row) is not _BUILTIN_TUPLE or _BUILTIN_LEN(row) != 2:
        return False
    key = row[0]
    if _BUILTIN_TYPE(key) not in (_BUILTIN_INT, _BUILTIN_STR):
        return False
    value = row[1]
    if _BUILTIN_TYPE(value) is _BUILTIN_FLOAT:
        return expression_kind == "f64"
    if _BUILTIN_TYPE(value) is not _BUILTIN_INT:
        return False
    if expression_kind == "i64":
        return _I64_MIN <= value <= _I64_MAX
    return -_F64_EXACT_INT_MAX <= value <= _F64_EXACT_INT_MAX


_CANONICAL_PAIR_VALUE_MAP_FIRST_ROW_COMPATIBLE = _pair_value_map_first_row_compatible


def prepare_pair_value_map_to_dict(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    operations: tuple[Operation, ...],
    consumer: PairDictConsumer | None,
    open_operations: Callable[..., Any],
) -> _PairValueMapSnapshot | None:
    """Snapshot a closed scalar value-map sink before opening its live source."""
    from ..runtime.failpoints import has_active_failpoints
    from .sync import open_operations as canonical_open_operations

    if consumer is None:
        return None
    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or physical.source is not pipeline.source
        or _BUILTIN_TYPE(pipeline.engine) is not _BUILTIN_STR
        or pipeline.engine != "auto"
        or _BUILTIN_TYPE(consumer) is not PairDictConsumer
        or PairDictConsumer.__dict__.get("__call__") is not _CANONICAL_PAIR_DICT_CALL
        or _BUILTIN_TYPE(cast(object, consumer.policy)) is not _BUILTIN_STR
        or consumer.policy not in ("first", "last")
        or open_operations is not canonical_open_operations
        or _BUILTIN_TYPE(pipeline.source) is not Source
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or pipeline.source.capabilities.reiterable is not True
        or _BUILTIN_TYPE(operations) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(operations) != 1
        or _BUILTIN_TYPE(pipeline.operations) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(pipeline.operations) != 1
        or _BUILTIN_TYPE(operation := operations[0]) is not MapOp
        or operation is not pipeline.operations[0]
        or _BUILTIN_TYPE(descriptor := operation.function) is not PairMapDescriptor
        or _BUILTIN_TYPE(cast(object, descriptor.side)) is not _BUILTIN_STR
        or descriptor.side != "value"
        or (expression_kind := _pair_value_filter_expression_kind(descriptor.callback)) is None
        or _compile_scalar_callable is not _CANONICAL_COMPILE_SCALAR_CALLABLE
        or has_active_failpoints()
    ):
        return None
    callback = cast(Expr | FExpr, descriptor.callback)
    payload = physical.backend_payload
    if (
        not isinstance(payload, BackendPayload)
        or payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return None
    retained = pipeline.source.retained_sequence()
    if (
        _BUILTIN_TYPE(retained) not in (_BUILTIN_LIST, _BUILTIN_TUPLE)
        or _BUILTIN_LEN(cast(Any, retained)) < _PAIR_VALUE_MAP_MIN_ROWS
        or _pair_value_map_first_row_compatible
        is not _CANONICAL_PAIR_VALUE_MAP_FIRST_ROW_COMPATIBLE
        or not _CANONICAL_PAIR_VALUE_MAP_FIRST_ROW_COMPATIBLE(
            cast(list[Any] | tuple[Any, ...], retained),
            expression_kind,
        )
    ):
        return None

    try:
        from .. import _native
    except ImportError:
        return None
    endpoint_name = (
        "pair_i64_value_map_to_dict_exact_prefix_v1"
        if expression_kind == "i64"
        else "pair_f64_value_map_to_dict_exact_prefix_v1"
    )
    raw_endpoint = getattr(_native, endpoint_name, None)
    if not callable(raw_endpoint):
        return None
    return _PairValueMapSnapshot(
        operation=operation,
        descriptor=descriptor,
        callback=callback,
        expression_kind=expression_kind,
        source_values=cast(list[Any] | tuple[Any, ...], retained),
        endpoint_name=endpoint_name,
        endpoint=cast(Callable[..., tuple[Any | None, bool] | None], raw_endpoint),
    )


def _prepend_pair_value_map_boundary(first: Any, source: Iterator[Any]) -> Iterator[Any]:
    """Yield one untouched native boundary, then the still-open exact-sequence suffix."""
    yield first
    del first
    yield from source


def _consume_pair_value_map_suffix(
    output: dict[Any, Any],
    iterator: Iterator[Any],
    callback: Callable[[Any], Any],
    policy: Literal["first", "last"],
) -> dict[Any, Any]:
    """Resume the canonical value-map sink from a natively mapped exact prefix."""
    if policy == "last":
        for pair in iterator:
            try:
                key = pair[0]
                value = callback(pair[1])
            except StopIteration:
                return output
            del pair
            output[key] = value
        return output

    for pair in iterator:
        try:
            key = pair[0]
            value = callback(pair[1])
        except StopIteration:
            return output
        del pair
        if key not in output:
            output[key] = value
    return output


def try_consume_pair_value_map_to_dict_opened(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    operations: tuple[Operation, ...],
    consumer: PairDictConsumer,
    descriptor: PairMapDescriptor,
    iterator: Iterator[Any],
    snapshot: _PairValueMapSnapshot | None,
) -> tuple[bool, dict[Any, Any] | None]:
    """Map one closed scalar-expression pair tail in Rust on an already-opened iterator."""
    from ..runtime.failpoints import has_active_failpoints
    from .sync import open_operations as canonical_open_operations

    if snapshot is None or pipeline is None:
        return False, None
    operation = snapshot.operation
    callback = snapshot.callback
    expression_kind = snapshot.expression_kind
    if (
        physical.root is not None
        or physical.parallel is not None
        or physical.source is not pipeline.source
        or _BUILTIN_TYPE(pipeline.engine) is not _BUILTIN_STR
        or pipeline.engine != "auto"
        or _BUILTIN_TYPE(consumer) is not PairDictConsumer
        or PairDictConsumer.__dict__.get("__call__") is not _CANONICAL_PAIR_DICT_CALL
        or _BUILTIN_TYPE(cast(object, consumer.policy)) is not _BUILTIN_STR
        or consumer.policy not in ("first", "last")
        or _BUILTIN_TYPE(operations) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(operations) != 1
        or _BUILTIN_TYPE(pipeline.operations) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(pipeline.operations) != 1
        or _BUILTIN_TYPE(operations[0]) is not MapOp
        or operations[0] is not operation
        or operation is not pipeline.operations[0]
        or _BUILTIN_TYPE(operation.function) is not PairMapDescriptor
        or operation.function is not descriptor
        or descriptor is not snapshot.descriptor
        or _BUILTIN_TYPE(cast(object, descriptor.side)) is not _BUILTIN_STR
        or descriptor.side != "value"
        or descriptor.callback is not callback
        or _pair_value_filter_expression_kind(callback) != expression_kind
        or _compile_scalar_callable is not _CANONICAL_COMPILE_SCALAR_CALLABLE
        or _BUILTIN_TYPE(pipeline.source) is not Source
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or _BUILTIN_TYPE(iterator) not in (_LIST_ITERATOR_TYPE, _TUPLE_ITERATOR_TYPE)
        or has_active_failpoints()
    ):
        return False, None
    payload = physical.backend_payload
    if (
        not isinstance(payload, BackendPayload)
        or payload.arrow_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None

    physical_module = sys.modules.get("fpstreams.execution.physical")
    if (
        physical_module is None
        or physical_module.__dict__.get("open_operations") is not canonical_open_operations
    ):
        return False, None
    retained = pipeline.source.retained_sequence()
    if (
        _BUILTIN_TYPE(retained) not in (_BUILTIN_LIST, _BUILTIN_TUPLE)
        or retained is not snapshot.source_values
        or _BUILTIN_LEN(cast(Any, retained)) < _PAIR_VALUE_MAP_MIN_ROWS
    ):
        return False, None

    try:
        from .. import _native
    except ImportError:
        return False, None
    endpoint_name = snapshot.endpoint_name
    endpoint = snapshot.endpoint

    if (
        has_active_failpoints()
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or getattr(_native, endpoint_name, None) is not endpoint
    ):
        return False, None

    instructions = _validated_pair_value_filter_instructions(callback, expression_kind)
    if instructions is None or not any(opcode == _OPCODES["item"] for opcode, _ in instructions):
        return False, None
    result_is_bool = instructions[-1][0] in _PAIR_SCALAR_BOOL_OPCODES
    if expression_kind == "f64":
        inferred_result_is_bool = _validated_pair_float_map_result_is_bool(
            cast(tuple[tuple[int, float], ...], instructions)
        )
        if inferred_result_is_bool is None:
            return False, None
        result_is_bool = inferred_result_is_bool
    evaluator = _validated_pair_value_filter_predicate(
        callback,
        expression_kind,
        instructions,
    )
    if evaluator is None:
        return False, None

    output: dict[Any, Any] = _BUILTIN_DICT()
    native = endpoint(
        output,
        iterator,
        instructions,
        consumer.policy == "first",
        result_is_bool,
    )
    if native is None:
        return False, None
    first_incompatible, completed = native
    if completed:
        return True, output
    remaining = _prepend_pair_value_map_boundary(first_incompatible, iterator)
    try:
        return True, _consume_pair_value_map_suffix(
            output,
            remaining,
            evaluator,
            consumer.policy,
        )
    finally:
        cast(Any, remaining).close()


def _consume_mapped_keys(
    iterator: Iterator[Any],
    callback: Callable[[Any], Any],
    policy: DuplicatePolicy,
) -> dict[Any, Any]:
    """Map keys and collect them without allocating intermediate pairs."""
    result: dict[Any, Any] = {}
    if policy == "last":
        for pair in iterator:
            try:
                key = callback(pair[0])
                value = pair[1]
            except StopIteration:
                return result
            del pair
            result[key] = value
        return result

    if policy == "first":
        for pair in iterator:
            try:
                key = callback(pair[0])
                value = pair[1]
            except StopIteration:
                return result
            del pair
            if key not in result:
                result[key] = value
        return result

    for pair in iterator:
        try:
            key = callback(pair[0])
            value = pair[1]
        except StopIteration:
            return result
        del pair
        if key in result:
            raise DuplicateKeyError(f"Duplicate key: {key!r}")
        result[key] = value
    return result
