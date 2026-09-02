"""Lower pair-wide RowExpr filters into a guarded retained-sequence sink."""

from __future__ import annotations

import builtins
import sys
from collections.abc import Callable, Iterator
from typing import Any, Literal, cast

from ..errors import DuplicateKeyError
from ..physical.plan import BackendPayload, PhysicalPlan
from ..planning._pair_stages import PairFilterDescriptor
from ..planning.logical import Pipeline
from ..planning.pair_i64_expression import (
    lower_pair_i64_row_filter,
    pair_i64_row_filter_expr_is_canonical,
)
from ..planning.source import (
    _CANONICAL_RETAINED_SEQUENCE,
    _CANONICAL_SOURCE_OPEN,
    Source,
)
from ..planning.sync import FilterOp, Operation
from ..runtime.query import QueryRuntime
from . import _pair_dict

DuplicatePolicy = Literal["error", "first", "last"]
_BUILTIN_DICT: type[dict[Any, Any]] = builtins.dict
_BUILTIN_LEN = builtins.len
_BUILTIN_LIST: type[list[Any]] = builtins.list
_BUILTIN_STR: type[str] = builtins.str
_BUILTIN_TUPLE: type[tuple[Any, ...]] = builtins.tuple
_BUILTIN_TYPE = builtins.type
_LIST_ITERATOR_TYPE: type[Iterator[Any]] = _BUILTIN_TYPE(iter([]))
_TUPLE_ITERATOR_TYPE: type[Iterator[Any]] = _BUILTIN_TYPE(iter(()))
_PAIR_ROW_FILTER_MIN_ROWS = 128
_CANONICAL_PAIR_DICT_CALL = _pair_dict.PairDictConsumer.__call__


def _consume_into_dict(  # noqa: C901 - policy-local loops avoid per-row dispatch
    output: dict[Any, Any],
    iterator: Iterator[Any],
    callback: Callable[[Any], Any],
    policy: DuplicatePolicy,
) -> dict[Any, Any]:
    """Evaluate one pair-row callback and collect into a caller-owned dictionary."""
    if policy == "last":
        for pair in iterator:
            try:
                predicate_result = callback(pair)
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

    if policy == "first":
        for pair in iterator:
            try:
                predicate_result = callback(pair)
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

    for pair in iterator:
        try:
            predicate_result = callback(pair)
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
            if key in output:
                raise DuplicateKeyError(f"Duplicate key: {key!r}")
            output[key] = value
        finally:
            del pair
    return output


def consume_pair_row_filter_to_dict(
    iterator: Iterator[Any],
    descriptor: PairFilterDescriptor,
    policy: DuplicatePolicy,
) -> dict[Any, Any]:
    """Fuse a final one-argument pair RowExpr with dictionary collection."""
    return _consume_into_dict({}, iterator, descriptor.callback, policy)


def _prepend_boundary(first: Any, source: Iterator[Any]) -> Iterator[Any]:
    """Yield one untouched native boundary followed by the live source suffix."""
    yield first
    del first
    yield from source


def _retained_pair_sequence(source: Source[Any]) -> list[Any] | tuple[Any, ...] | None:
    """Return one exact retained sequence without widening accepted source shapes."""
    retained = source.retained_sequence()
    if _BUILTIN_TYPE(retained) not in (_BUILTIN_LIST, _BUILTIN_TUPLE):
        return None
    return cast(list[Any] | tuple[Any, ...], retained)


def _consume_opened_canonical(
    source: Iterator[Any],
    operations: tuple[Operation, ...],
    consumer: Any,
    runtime: QueryRuntime,
    open_operations: Callable[..., Any],
) -> dict[Any, Any]:
    """Resume a post-open decline without reopening a one-shot source."""
    with open_operations(source, operations, runtime=runtime) as iterator:
        return cast(dict[Any, Any], consumer(iterator))


def try_consume_pair_row_filter_to_dict(
    physical: PhysicalPlan,
    pipeline: Pipeline | None,
    operations: tuple[Operation, ...],
    consumer: Any,
    open_operations: Callable[..., Any],
) -> tuple[bool, dict[Any, Any] | None]:
    """Push one retained exact-i64 pair RowExpr into Rust, or decline before claiming."""
    from ..runtime.failpoints import has_active_failpoints
    from .sync import open_operations as canonical_open_operations

    if (
        physical.root is not None
        or pipeline is None
        or physical.parallel is not None
        or physical.source is not pipeline.source
        or _BUILTIN_TYPE(pipeline.engine) is not _BUILTIN_STR
        or pipeline.engine != "auto"
        or _BUILTIN_TYPE(consumer) is not _pair_dict.PairDictConsumer
        or _pair_dict.PairDictConsumer.__dict__.get("__call__") is not _CANONICAL_PAIR_DICT_CALL
        or _BUILTIN_TYPE(cast(object, consumer.policy)) is not _BUILTIN_STR
        or consumer.policy not in ("first", "last")
        or open_operations is not canonical_open_operations
        or _BUILTIN_TYPE(pipeline.source) is not Source
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or _BUILTIN_TYPE(operations) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(operations) != 1
        or _BUILTIN_TYPE(pipeline.operations) is not _BUILTIN_TUPLE
        or _BUILTIN_LEN(pipeline.operations) != 1
        or _BUILTIN_TYPE(operation := operations[0]) is not FilterOp
        or operation is not pipeline.operations[0]
        or operation.negate is not False
        or _BUILTIN_TYPE(descriptor := operation.predicate) is not PairFilterDescriptor
        or descriptor.target != "row"
        or not pair_i64_row_filter_expr_is_canonical(callback := descriptor.callback)
        or has_active_failpoints()
    ):
        return False, None
    payload = physical.backend_payload
    if (
        not isinstance(payload, BackendPayload)
        or payload.arrow_prefix is not None
        or payload.numpy_prefix is not None
        or payload.native_decision is None
        or payload.native_decision.engine != "python"
    ):
        return False, None
    retained = _retained_pair_sequence(pipeline.source)
    if (
        retained is None
        or _BUILTIN_LEN(retained) < _PAIR_ROW_FILTER_MIN_ROWS
        or (instructions := lower_pair_i64_row_filter(callback)) is None
    ):
        return False, None

    try:
        from .. import _native
    except ImportError:
        return False, None
    raw_endpoint = getattr(_native, "pair_i64_row_filter_to_dict_exact_prefix_v1", None)
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
            live_endpoint = getattr(_native, "pair_i64_row_filter_to_dict_exact_prefix_v1", None)
            live_instructions = lower_pair_i64_row_filter(callback)
            if (
                has_active_failpoints()
                or physical.root is not None
                or physical.parallel is not None
                or physical.source is not pipeline.source
                or _BUILTIN_TYPE(pipeline.engine) is not _BUILTIN_STR
                or pipeline.engine != "auto"
                or _BUILTIN_TYPE(consumer) is not _pair_dict.PairDictConsumer
                or _pair_dict.PairDictConsumer.__dict__.get("__call__")
                is not _CANONICAL_PAIR_DICT_CALL
                or _BUILTIN_TYPE(cast(object, consumer.policy)) is not _BUILTIN_STR
                or consumer.policy not in ("first", "last")
                or _BUILTIN_TYPE(operation) is not FilterOp
                or _BUILTIN_TYPE(pipeline.operations) is not _BUILTIN_TUPLE
                or _BUILTIN_LEN(pipeline.operations) != 1
                or operation is not pipeline.operations[0]
                or operation.negate is not False
                or _BUILTIN_TYPE(operation.predicate) is not PairFilterDescriptor
                or operation.predicate is not descriptor
                or descriptor.target != "row"
                or descriptor.callback is not callback
                or live_instructions != instructions
                or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
                or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
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

            live_retained = _retained_pair_sequence(pipeline.source)
            if (
                live_retained is not retained
                or _BUILTIN_LEN(live_retained) < _PAIR_ROW_FILTER_MIN_ROWS
            ):
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
            remaining = _prepend_boundary(first_incompatible, source_iterator)
            try:
                return True, _consume_into_dict(
                    output,
                    remaining,
                    callback,
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
