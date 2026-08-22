"""Execution of recursively compiled record relation trees."""

from __future__ import annotations

import builtins as _builtins
import sys as _sys
from abc import get_cache_token
from collections import namedtuple as _namedtuple
from collections.abc import Callable, Iterator, Mapping
from contextlib import suppress
from dataclasses import replace
from importlib import import_module
from itertools import chain, islice
from types import BuiltinFunctionType, CodeType, FunctionType, MappingProxyType
from typing import Any, cast

from ..collecting.aggregation import (
    _MISSING,
    AggregationItems,
    Aggregator,
    native_group_aggregation,
)
from ..collecting.program import run_collector_program
from ..errors import DuplicateKeyError, SelectionError
from ..physical.plan import PhysicalNode, PhysicalPlan
from ..physical.relational import (
    ArrowGlobalSumSpec,
    ClosedGroupSpec,
    CompositeCountSumSpec,
    GlobalAggregatePhysicalNode,
    GroupAggregatePhysicalNode,
    JoinPhysicalNode,
    JoinStrategy,
    NativeCallableGroupSpec,
    NativeFixedI64GroupSpec,
    NativeGroupSumSpec,
    NativeRecordGroupSumSpec,
    PhysicalRelNode,
    PipelinePhysicalNode,
    SimpleGroupSumSpec,
    SourcePhysicalNode,
)
from ..planning.arrow_source import ArrowBatchSource
from ..planning.logical import LogicalPlan, Pipeline, SourceNode
from ..planning.source import Source
from ..runtime.query import QueryRuntime
from ..tabular import records as _records
from ..tabular.join import (
    _check_unique_key,
    _direct_mapping_mro,
    _fill_unmatched_join_plan,
    _join_targets,
    _JoinTargetCache,
    _merge_join_plan_snapshot,
    _merge_join_records,
    _merge_join_snapshot,
    _select_direct_mapping_field,
    _semi_or_anti_join,
    execute_left_join,
    execute_right_or_full_join,
)
from ..tabular.records import _as_record, _remember_columns
from ..tabular.spill import spilled_group_aggregate, spilled_join
from ..tabular.spill_limits import SpillLimits
from . import execute

_RECORD_JOIN_V1_MAX_FIELDS = 24
# PyArrow kernel dispatch dominates ordered scalar folding below this measured crossover.
_ARROW_PYTHON_EXTREME_MAX_ROWS = 128
_ARROW_CSV_GROUP_MIN_BYTES = 32 * 1_024
_ARROW_FILE_GROUP_MIN_ROWS = 1_024
_ARROW_GROUP_BATCH_SCALAR_MAX_ROWS = 128
_ARROW_UNIQUE_JOIN_MIN_ROWS = 128
_CALLABLE_GROUP_HASH_WARMUP_ROWS = 32
_EXPECTED_ARROW_JOIN_ERRORS = (
    ArithmeticError,
    NotImplementedError,
    TypeError,
    ValueError,
)
_NONE_TYPE = type(None)
_BUILTINS_DICT = _builtins.__dict__
_BUILTIN_HASH = _BUILTINS_DICT["hash"]
_RECORD_GLOBALS = vars(_records)
_CANONICAL_RECORD_CONTINUATIONS = _RECORD_GLOBALS["_RECORD_CONTINUATIONS"]
_RELATIONAL_GLOBALS = globals()
_CANONICAL_BUILTIN_HASH = (
    type(_BUILTIN_HASH) is BuiltinFunctionType
    and _BUILTIN_HASH.__module__ == "builtins"
    and _BUILTIN_HASH.__name__ == "hash"
)
_GETTRACE = _sys.gettrace
_GETPROFILE = _sys.getprofile


def _exact_builtin_group_key_type(key_type: type[Any]) -> bool:
    """Return whether removing one redundant hash call is unobservable for this exact type."""
    return (
        key_type is int
        or key_type is str
        or key_type is bytes
        or key_type is bool
        or key_type is float
        or key_type is complex
        or key_type is _NONE_TYPE
    )


def _standard_namedtuple_record_type(row_type: type[Any]) -> bool:
    """Recognize an unmodified standard-library-style direct NamedTuple class safely."""
    if type(row_type) is not type or row_type.__bases__ != (tuple,):
        return False
    namespace = vars(row_type)
    fields = namespace.get("_fields")
    slots = namespace.get("__slots__")
    return (
        type(fields) is tuple
        and all(type(field) is str for field in fields)
        and type(slots) is tuple
        and not slots
        and type(namespace.get("_asdict")) is FunctionType
        and type(namespace.get("__new__")) is staticmethod
    )


def _make_standard_namedtuple_record_adapter(
    record_types: tuple[type[Any], ...],
) -> Callable[[Any], dict[str, Any]] | None:
    """Skip generic protocol discovery while its exact NamedTuple proof remains live."""
    abc_token = get_cache_token()
    if (
        not record_types
        or len(record_types) > 2
        or any(
            not _standard_namedtuple_record_type(row_type)
            or issubclass(row_type, Mapping)
            or hasattr(row_type, "__dataclass_fields__")
            for row_type in record_types
        )
        or get_cache_token() != abc_token
    ):
        return None
    first_type = record_types[0]
    second_type = record_types[-1]

    def adapt(row: Any) -> dict[str, Any]:
        # An earlier selector may replace a later live-list row after native preflight. Compare
        # exact identities without equality dispatch; an unobserved type needs the complete
        # canonical protocol order even when it also exposes ``_asdict``.
        row_type = type(row)
        if row_type is not first_type and row_type is not second_type:
            return _as_record(row)
        try:
            continuations = cast(
                tuple[
                    Callable[[Any], dict[str, Any]],
                    Callable[[Any], dict[str, Any]],
                ],
                _RECORD_GLOBALS["_RECORD_CONTINUATIONS"],
            )
        except KeyError:
            return _as_record(row)
        # Mapping priority can change after an arbitrary subclass-hook/cache transition without
        # advancing the ABC token. Recheck the actual protocol before observing live `_asdict`.
        if isinstance(row, Mapping):
            return continuations[0](row)
        return continuations[1](row)

    return adapt


def _native_standard_namedtuple_record_adapter(
    native_module: Any,
    record_types: tuple[type[Any], ...],
    fallback: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    """Create a guarded native snapshot adapter when tracing cannot observe `_asdict`."""
    factory = getattr(native_module, "standard_namedtuple_record_adapter_v1", None)
    if not callable(factory):
        return fallback
    adapter = factory(
        record_types,
        fallback,
        get_cache_token,
        get_cache_token(),
        _namedtuple,
        CodeType,
        Mapping,
        _CANONICAL_RECORD_CONTINUATIONS,
        _RECORD_GLOBALS,
        _GETTRACE,
        _GETPROFILE,
    )
    return fallback if adapter is None else cast(Callable[[Any], dict[str, Any]], adapter)


def _observed_callable_join_record_types(
    left: list[Any] | tuple[Any, ...],
    right: list[Any] | tuple[Any, ...],
    *,
    allow_namedtuple: bool,
) -> tuple[tuple[type[Any], ...], bool] | None:
    """Return narrow exact-type capabilities observed at the two source heads.

    The native v2 ABI rechecks every initial row by exact type before invoking any adapter or
    selector.  Sampling only the first row keeps this Python gate constant-time; a later distinct
    type simply makes native preflight decline.  Exact lists may change concurrently between this
    observation and native preflight, so an empty-race is conservatively treated like no token.
    The boolean result records whether any accepted type requires canonical ``_as_record``
    conversion instead of the Mapping-only built-in ``dict`` adapter.
    """
    observed: list[type[Any]] = []
    requires_record_adapter = False
    for rows in (left, right):
        try:
            row = rows[0]
        except IndexError:
            continue
        row_type = type(row)
        if row_type is dict:
            continue
        if row_type is not MappingProxyType and _direct_mapping_mro(row_type) is None:
            if not allow_namedtuple or not _standard_namedtuple_record_type(row_type):
                return None
            requires_record_adapter = True
        if all(row_type is not known for known in observed):
            observed.append(row_type)
    return tuple(observed), requires_record_adapter


def _compile_native_direct_join_key(field: str) -> Callable[[Any], Any]:
    """Bind one direct field with the canonical selector error boundary."""

    def select(row: Any) -> Any:
        try:
            return row[field]
        except (AttributeError, KeyError, TypeError) as error:
            raise SelectionError(
                f"Could not resolve selector {field!r}; failed at {field!r}"
            ) from error

    return select


def _try_native_hashable_record_join(
    native_module: Any,
    root: JoinPhysicalNode,
    left: list[Any] | tuple[Any, ...],
    right: list[Any] | tuple[Any, ...],
    left_key: Callable[[Any], Any],
    right_key: Callable[[Any], Any],
    *,
    many: bool,
    require_non_dict: bool = False,
    allow_namedtuple: bool = False,
) -> list[dict[str, Any]] | None:
    """Try one exact-type-preflight hashable record ABI without selector replay."""
    observed = _observed_callable_join_record_types(
        left,
        right,
        allow_namedtuple=allow_namedtuple,
    )
    if observed is None:
        return None
    record_types, requires_record_adapter = observed
    if require_non_dict and (
        not record_types or any(type(row) is dict for rows in (left, right) for row in rows)
    ):
        # The canonical direct-field executor can traverse exact dictionaries without
        # rehashing protocol-sensitive field-name subclasses. The broader callable ABI cannot
        # make that promise, so this secondary specialization is Mapping-only.
        return None
    callback_version = 2 if record_types else 1
    cardinality = "many" if many else "unique"
    callback_kernel = getattr(
        native_module,
        f"join_hashable_{cardinality}_records_v{callback_version}",
        None,
    )
    if not callable(callback_kernel):
        return None
    if requires_record_adapter:
        record_adapter = _make_standard_namedtuple_record_adapter(record_types) or _as_record
        if record_adapter is not _as_record:
            record_adapter = _native_standard_namedtuple_record_adapter(
                native_module,
                record_types,
                record_adapter,
            )
    else:
        record_adapter = dict
    arguments = (
        left,
        right,
        left_key,
        right_key,
        record_adapter,
        root.spec.logical.how == "left",
        root.spec.logical.suffix,
        root.spec.shared_names,
    )
    if record_types:
        return cast(list[dict[str, Any]] | None, callback_kernel(*arguments, record_types))
    return cast(list[dict[str, Any]] | None, callback_kernel(*arguments))


def _try_native_direct_mapping_record_join(
    native_module: Any,
    root: JoinPhysicalNode,
    left: list[Any] | tuple[Any, ...],
    right: list[Any] | tuple[Any, ...],
    left_field: str,
    right_field: str,
    *,
    many: bool,
) -> list[dict[str, Any]] | None:
    """Pass exact field tokens through a Mapping-only, preflight-before-effects ABI."""
    observed = _observed_callable_join_record_types(
        left,
        right,
        allow_namedtuple=False,
    )
    if observed is None:
        return None
    record_types, requires_record_adapter = observed
    if not record_types or requires_record_adapter:
        return None
    cardinality = "many" if many else "unique"
    kernel = getattr(
        native_module,
        f"join_hashable_{cardinality}_direct_records_v1",
        None,
    )
    if not callable(kernel):
        return None
    return cast(
        list[dict[str, Any]] | None,
        kernel(
            left,
            right,
            left_field,
            right_field,
            root.spec.logical.how == "left",
            root.spec.logical.suffix,
            root.spec.shared_names,
            record_types,
        ),
    )


def _try_native_i64_record_join(
    native_module: Any,
    root: JoinPhysicalNode,
    left: list[Any] | tuple[Any, ...],
    right: list[Any] | tuple[Any, ...],
) -> list[dict[str, Any]] | None:
    """Try the narrow exact-dict integer-key ABI before broader guarded kernels."""
    max_fields = getattr(native_module, "record_join_v1_max_fields", None)
    if type(max_fields) is not int or max_fields != _RECORD_JOIN_V1_MAX_FIELDS:
        return None
    native = root.native_record_i64
    assert native is not None
    left_join = root.spec.logical.how == "left"
    many_kernel = getattr(native_module, "join_i64_many_dict_rows_v1", None)
    if root.spec.logical.validate == "m:m" and callable(many_kernel):
        return cast(
            list[dict[str, Any]] | None,
            many_kernel(
                left,
                right,
                native.left_field,
                native.right_field,
                left_join,
            ),
        )

    unique_kernel = getattr(native_module, "join_i64_unique_dict_rows_v1", None)
    if not callable(unique_kernel):
        return None
    return cast(
        list[dict[str, Any]] | None,
        unique_kernel(
            left,
            right,
            native.left_field,
            native.right_field,
            left_join,
        ),
    )


def try_native_pair_sum(
    plan: LogicalPlan,
    aggregations: AggregationItems,
) -> dict[Any, dict[str, Any]] | None:
    """Aggregate one guarded direct pair source without opening its Python iterator."""
    from ..runtime.failpoints import has_active_failpoints

    if (
        plan.engine != "auto"
        or has_active_failpoints()
        or len(aggregations) != 1
        or not isinstance(plan.root, SourceNode)
    ):
        return None
    output_name, aggregation = aggregations[0]
    if type(output_name) is not str or type(aggregation) is not Aggregator:
        return None
    native = native_group_aggregation(aggregation)
    if native is None or native.kind != "sum" or native.selector is not None:
        return None

    source = plan.root.source
    capabilities = source.capabilities
    rows = source.native_data
    if not capabilities.reiterable or not capabilities.ordered or type(rows) not in (list, tuple):
        return None

    try:
        from .. import _native
    except ImportError:
        return None
    kernel = getattr(_native, "group_sum_i64_exact_pairs_v1", None)
    if not callable(kernel):
        return None
    groups = kernel(rows)
    if groups is None:
        return None
    return {key: {output_name: total} for key, total in groups}


def _retained_arrow_join_table(pa: Any, descriptor: ArrowBatchSource) -> Any | None:
    """Normalize one retained Table or RecordBatch without opening its row adapter."""
    retained = descriptor.materialized_data
    if descriptor.kind == "table" and isinstance(retained, pa.Table):
        return retained
    if descriptor.kind == "record_batch" and isinstance(retained, pa.RecordBatch):
        try:
            return pa.Table.from_batches([retained], schema=descriptor.schema_hint)
        except _EXPECTED_ARROW_JOIN_ERRORS:
            return None
    return None


def _arrow_join_primitive_schema(pa: Any, schema: Any) -> bool:
    """Accept only fields whose Arrow conversion yields immutable Python primitives."""
    checks = tuple(
        predicate
        for name in (
            "is_null",
            "is_boolean",
            "is_integer",
            "is_floating",
            "is_string",
            "is_large_string",
            "is_binary",
            "is_large_binary",
            "is_fixed_size_binary",
        )
        if callable(predicate := getattr(pa.types, name, None))
    )
    return all(any(check(field.type) for check in checks) for field in schema)


def _arrow_join_key_type(pa: Any, key_type: Any) -> bool:
    """Recognize key scalars whose Arrow lookup equality matches Python equality."""
    types = pa.types
    return bool(
        types.is_boolean(key_type)
        or types.is_integer(key_type)
        or types.is_string(key_type)
        or types.is_large_string(key_type)
        or types.is_binary(key_type)
        or types.is_large_binary(key_type)
    )


def _retained_arrow_left_batches(retained: Any, descriptor: ArrowBatchSource) -> Iterator[Any]:
    """Yield the same left batch boundaries used by the canonical Arrow row adapter."""
    if descriptor.kind == "table":
        yield from retained.to_batches(max_chunksize=descriptor.batch_size)
        return
    for offset in range(0, retained.num_rows, descriptor.batch_size):
        yield retained.slice(offset, descriptor.batch_size)


def _arrow_unique_join_batch_rows(
    pa: Any,
    pc: Any,
    left_batch: Any,
    positions: Any,
    *,
    how: str,
    suffix: str,
    right_outputs: tuple[tuple[int, str, str], ...],
    right_payloads: tuple[list[Any], ...],
) -> list[dict[str, Any]]:
    """Join one original left batch and preserve its dictionary-key identity boundary."""
    left = pa.Table.from_batches([left_batch], schema=left_batch.schema)
    if how == "inner":
        matched = pc.is_valid(positions)
        left = left.filter(matched)
        positions = pc.filter(positions, matched)
    rows = left.to_pylist()
    if not right_outputs:
        return cast(list[dict[str, Any]], rows)

    right_positions = positions.to_pylist()
    for row, right_position in zip(rows, right_positions, strict=True):
        for (_field_index, name, target), payload in zip(
            right_outputs, right_payloads, strict=True
        ):
            # Canonical target caching intentionally defers only generated suffix
            # strings, minting one fresh key per output row.
            output_target = target if target is name else f"{name}{suffix}"
            row[output_target] = None if right_position is None else payload[right_position]
    return cast(list[dict[str, Any]], rows)


def try_retained_arrow_unique_join(plan: PhysicalPlan) -> list[dict[str, Any]] | None:
    """Materialize one guarded top-level retained Arrow m:1 join by column position."""
    from ..runtime.failpoints import has_active_failpoints, hit

    root = plan.root
    if (
        plan.terminal.name != "list"
        or plan.engine != "auto"
        or plan.parallel is not None
        or has_active_failpoints()
        or not isinstance(root, JoinPhysicalNode)
        or root.arrow_unique is None
        or not isinstance(root.left, SourcePhysicalNode)
        or not isinstance(root.right, SourcePhysicalNode)
    ):
        return None
    left_descriptor = root.left.source.native_data
    right_descriptor = root.right.source.native_data
    if not isinstance(left_descriptor, ArrowBatchSource) or not isinstance(
        right_descriptor, ArrowBatchSource
    ):
        return None

    try:
        pa = import_module("pyarrow")
        pc = import_module("pyarrow.compute")
    except ImportError:
        return None
    left = _retained_arrow_join_table(pa, left_descriptor)
    right = _retained_arrow_join_table(pa, right_descriptor)
    if (
        left is None
        or right is None
        or right.num_rows == 0
        or left.num_rows + right.num_rows < _ARROW_UNIQUE_JOIN_MIN_ROWS
    ):
        return None

    marker = root.arrow_unique
    left_names = tuple(left.schema.names)
    right_names = tuple(right.schema.names)
    if (
        left_names.count(marker.left_field) != 1
        or right_names.count(marker.right_field) != 1
        or not _arrow_join_primitive_schema(pa, left.schema)
        or not _arrow_join_primitive_schema(pa, right.schema)
    ):
        return None
    left_key = left[marker.left_field]
    right_key = right[marker.right_field]
    if (
        left_key.type != right_key.type
        or not _arrow_join_key_type(pa, left_key.type)
        or left_key.null_count
        or right_key.null_count
    ):
        return None

    try:
        right.validate(full=True)
        distinct = pc.count_distinct(right_key, mode="all").as_py()
    except _EXPECTED_ARROW_JOIN_ERRORS:
        return None
    if distinct != right.num_rows:
        return None

    logical = root.spec.logical
    shared_names = set(root.spec.shared_names)
    right_field_indices = tuple(
        field_index for field_index, name in enumerate(right_names) if name not in shared_names
    )
    try:
        right_payloads = tuple(
            cast(list[Any], right.column(field_index).to_pylist())
            for field_index in right_field_indices
        )
    except _EXPECTED_ARROW_JOIN_ERRORS:
        return None
    try:
        left.validate(full=True)
    except _EXPECTED_ARROW_JOIN_ERRORS:
        return None
    try:
        targets = _join_targets(
            left_names,
            right_names,
            shared_names=shared_names,
            suffix=logical.suffix,
        )
        positions = pc.index_in(left_key, value_set=right_key)
    except (DuplicateKeyError, *_EXPECTED_ARROW_JOIN_ERRORS):
        return None
    right_outputs = tuple(
        (field_index, name, target)
        for field_index, (name, target) in enumerate(targets)
        if name not in shared_names
    )

    root.right.source.open_native(ArrowBatchSource)
    hit("source.open.after")
    root.left.source.open_native(ArrowBatchSource)
    hit("source.open.after")
    rows: list[dict[str, Any]] = []
    left_offset = 0
    try:
        for left_batch in _retained_arrow_left_batches(
            left_descriptor.materialized_data, left_descriptor
        ):
            batch_positions = positions.slice(left_offset, left_batch.num_rows)
            left_offset += left_batch.num_rows
            rows.extend(
                _arrow_unique_join_batch_rows(
                    pa,
                    pc,
                    left_batch,
                    batch_positions,
                    how=logical.how,
                    suffix=logical.suffix,
                    right_outputs=right_outputs,
                    right_payloads=right_payloads,
                )
            )
    except _EXPECTED_ARROW_JOIN_ERRORS:
        return None
    return rows


def try_native_record_join(plan: PhysicalPlan) -> list[dict[str, Any]] | None:
    """Materialize one guarded top-level exact-record join without opening either source."""
    from ..runtime.failpoints import has_active_failpoints

    root = plan.root
    if (
        plan.terminal.name != "list"
        or plan.engine != "auto"
        or has_active_failpoints()
        or not isinstance(root, JoinPhysicalNode)
        or (
            root.native_record_i64 is None
            and not root.native_callable_unique
            and not root.native_callable_many
        )
        or not isinstance(root.left, SourcePhysicalNode)
        or not isinstance(root.right, SourcePhysicalNode)
        or not root.left.source.capabilities.reiterable
        or not root.right.source.capabilities.reiterable
    ):
        return None
    left = root.left.source.native_data
    right = root.right.source.native_data
    if type(left) not in (list, tuple) or type(right) not in (list, tuple):
        return None

    try:
        from .. import _native
    except ImportError:
        return None

    if root.native_callable_unique or root.native_callable_many:
        return _try_native_hashable_record_join(
            _native,
            root,
            left,
            right,
            root.spec.left_key,
            root.spec.right_key,
            many=root.native_callable_many,
            allow_namedtuple=True,
        )

    native = root.native_record_i64
    assert native is not None
    direct_rows = _try_native_i64_record_join(_native, root, left, right)
    if direct_rows is not None:
        return direct_rows
    direct_mapping_rows = _try_native_direct_mapping_record_join(
        _native,
        root,
        left,
        right,
        native.left_field,
        native.right_field,
        many=root.spec.logical.validate == "m:m",
    )
    if direct_mapping_rows is not None:
        return direct_mapping_rows
    return _try_native_hashable_record_join(
        _native,
        root,
        left,
        right,
        _compile_native_direct_join_key(native.left_field),
        _compile_native_direct_join_key(native.right_field),
        many=root.spec.logical.validate == "m:m",
        require_non_dict=True,
    )


def try_direct_group_list(
    plan: PhysicalPlan,
) -> tuple[list[dict[str, Any]] | None, PhysicalPlan]:
    """Return a directly materialized group result, or a no-retry fallback plan.

    A top-level list terminal can return either the fixed composite Python result or a native
    materialization without forwarding every output row through the relational generators. If
    a speculative native ABI declines, clear its markers in the local immutable plan so the
    canonical fallback does not repeat that scan.
    """
    root = plan.root
    if plan.terminal.name != "list" or not isinstance(root, GroupAggregatePhysicalNode):
        return None, plan
    if (
        root.composite_count_sum is not None
        and root.partitions is None
        and isinstance(root.input, SourcePhysicalNode)
    ):
        values = root.input.source.open()
        return (
            _materialize_composite_count_sum(values, root, root.composite_count_sum),
            plan,
        )
    if plan.engine != "auto" or (
        root.arrow_i64_sum is None
        and root.native_i64_sum is None
        and root.native_record_i64_sum is None
        and root.native_fixed_i64_group is None
        and root.native_callable_group is None
    ):
        return None, plan

    group_rows = _try_arrow_group_sum(root)
    if group_rows is None:
        group_rows = _try_native_group_sum(root)
    if group_rows is not None:
        return group_rows, plan
    fallback_root = replace(
        root,
        arrow_i64_sum=None,
        native_i64_sum=None,
        native_record_i64_sum=None,
        native_fixed_i64_group=None,
        native_callable_group=None,
    )
    return None, replace(plan, root=fallback_root)


def execute_relational(root: PhysicalRelNode, runtime: QueryRuntime) -> Iterator[Any]:
    """Lazily execute a relational root while its owning query runtime remains active."""
    if isinstance(root, SourcePhysicalNode):
        yield from root.source.open()
        return
    if isinstance(root, PipelinePhysicalNode):
        source = _pipeline_source(root.input, runtime)
        yield from execute(
            Pipeline(source, _physical_operations(root.stages), root.engine, root.parallel),
            runtime=runtime,
        )
        return
    if isinstance(root, JoinPhysicalNode):
        yield from _execute_join(root, runtime)
        return
    if isinstance(root, GroupAggregatePhysicalNode):
        yield from _execute_group_aggregate(root, runtime)
        return
    if isinstance(root, GlobalAggregatePhysicalNode):
        exact_count = _try_exact_global_count(root)
        if exact_count is not None:
            yield exact_count
        else:
            arrow_count = _try_arrow_global_count(root)
            if arrow_count is not None:
                yield arrow_count
            else:
                columnar_reduction = _try_arrow_global_reduction(root)
                if columnar_reduction is not None:
                    yield columnar_reduction
                else:
                    values = execute_relational(root.input, runtime)
                    yield run_collector_program(values, root.aggregations.collectors)
        return
    raise TypeError(f"unsupported physical relation: {type(root).__name__}")


def _pipeline_source(root: PhysicalRelNode, runtime: QueryRuntime) -> Source[Any]:
    """Convert a relational branch to a compatibility source without opening it early."""
    from ..planning.source import Source

    if isinstance(root, SourcePhysicalNode):
        return root.source
    return Source.defer(lambda: execute_relational(root, runtime))


def _physical_operations(nodes: tuple[PhysicalNode, ...]) -> tuple[Any, ...]:
    """Read canonical operation payloads from compiled unary stages."""
    from .physical import operations_from_physical_nodes

    return operations_from_physical_nodes(nodes)


def _execute_join(node: JoinPhysicalNode, runtime: QueryRuntime) -> Iterator[dict[str, Any]]:
    """Execute the selected stable hash-compatible join strategy."""
    left = execute_relational(node.left, runtime)
    right = execute_relational(node.right, runtime)
    spec = node.spec
    logical = spec.logical
    left_field = (
        logical.left_on if type(logical.left_on) is str and "." not in logical.left_on else None
    )
    right_field = (
        logical.right_on if type(logical.right_on) is str and "." not in logical.right_on else None
    )
    if node.strategy is JoinStrategy.GRACE_HASH:
        yield from spilled_join(
            left,
            right,
            left_key=spec.left_key,
            right_key=spec.right_key,
            how=logical.how,
            shared_names=set(spec.shared_names),
            suffix=logical.suffix,
            validate=logical.validate,
            partitions=logical.partitions or 2,
            tempdir=logical.tempdir,
            limits=logical.limits or SpillLimits(),
            as_record=_as_record,
            remember_columns=_remember_columns,
            join_targets=_join_targets,
            merge_records=_merge_join_records,
            runtime=runtime,
        )
    elif node.strategy is JoinStrategy.UNIQUE_RIGHT:
        from ..runtime.failpoints import has_active_failpoints

        if has_active_failpoints():
            yield from execute_left_join(
                left,
                right,
                left_key=spec.left_key,
                right_key=spec.right_key,
                how=logical.how,
                shared_names=set(spec.shared_names),
                suffix=logical.suffix,
                validate=logical.validate,
                left_field=left_field,
                right_field=right_field,
            )
        else:
            yield from _unique_right_join(
                left,
                right,
                left_key=spec.left_key,
                right_key=spec.right_key,
                how=logical.how,
                shared_names=set(spec.shared_names),
                suffix=logical.suffix,
                validate=logical.validate,
                left_field=left_field,
                right_field=right_field,
            )
    elif logical.how in {"semi", "anti"}:
        yield from _semi_or_anti_join(
            left,
            right,
            left_key=spec.left_key,
            right_key=spec.right_key,
            how=logical.how,
            validate=logical.validate,
        )
    elif logical.how in {"inner", "left"}:
        yield from execute_left_join(
            left,
            right,
            left_key=spec.left_key,
            right_key=spec.right_key,
            how=logical.how,
            shared_names=set(spec.shared_names),
            suffix=logical.suffix,
            validate=logical.validate,
            left_field=left_field,
            right_field=right_field,
        )
    else:
        yield from execute_right_or_full_join(
            left,
            right,
            left_key=spec.left_key,
            right_key=spec.right_key,
            how=logical.how,
            shared_names=set(spec.shared_names),
            suffix=logical.suffix,
            validate=logical.validate,
        )


def _unique_right_join(  # noqa: C901 - keep guarded mapping snapshots inline in this hot loop
    left_source: Iterator[Any],
    right_source: Iterator[Any],
    *,
    left_key: Any,
    right_key: Any,
    how: str,
    shared_names: set[str],
    suffix: str,
    validate: str,
    left_field: str | None,
    right_field: str | None,
) -> Iterator[dict[str, Any]]:
    """Join against a one-record-per-key right index after enforcing that contract."""
    columns: list[str] = []
    seen_columns: set[str] = set()
    index: dict[Any, dict[str, Any]] = {}
    cached_mapping_type: type[Any] | None = None
    cached_mapping_mro: tuple[type[Any], ...] | None = None
    right_iterator = iter(right_source)
    try:
        for row in right_iterator:
            row_type = type(row)
            if row_type is dict:
                record = row.copy()
                direct_mapping = True
            elif row_type is MappingProxyType or (
                row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro
            ):
                record = dict(row)
                direct_mapping = row_type is MappingProxyType or (
                    row_type.__mro__ is cached_mapping_mro
                )
            else:
                record = _as_record(row)
                mapping_mro = _direct_mapping_mro(row_type)
                if mapping_mro is not None:
                    cached_mapping_type = row_type
                    cached_mapping_mro = mapping_mro
                direct_mapping = mapping_mro is not None and row_type.__mro__ is mapping_mro
            key = (
                _select_direct_mapping_field(row, right_field)
                if right_field is not None and direct_mapping
                else right_key(row)
            )
            _remember_columns(record, columns, seen_columns)
            try:
                existing = index.get(key)
            except TypeError:
                raise TypeError("join keys must be hashable") from None
            if existing is not None:
                raise ValueError(
                    f"join validate={validate!r} requires unique right keys; "
                    f"found duplicate {key!r}"
                )
            try:
                index[key] = record
            except TypeError:
                raise TypeError("join keys must be hashable") from None
    finally:
        close = getattr(right_iterator, "close", None)
        if callable(close):
            close()

    seen_left: set[Any] | None = set() if validate == "1:1" else None
    target_cache = _JoinTargetCache()
    cached_mapping_type = None
    cached_mapping_mro = None
    left_iterator = iter(left_source)
    try:
        for row in left_iterator:
            row_type = type(row)
            if row_type is dict:
                left = row.copy()
                direct_mapping = True
            elif row_type is MappingProxyType or (
                row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro
            ):
                left = dict(row)
                direct_mapping = row_type is MappingProxyType or (
                    row_type.__mro__ is cached_mapping_mro
                )
            else:
                left = _as_record(row)
                mapping_mro = _direct_mapping_mro(row_type)
                if mapping_mro is not None:
                    cached_mapping_type = row_type
                    cached_mapping_mro = mapping_mro
                direct_mapping = mapping_mro is not None and row_type.__mro__ is mapping_mro
            key = (
                _select_direct_mapping_field(row, left_field)
                if left_field is not None and direct_mapping
                else left_key(row)
            )
            if seen_left is not None:
                _check_unique_key(seen_left, key, validate=validate, side="left")
            try:
                right = index.get(key)
            except TypeError:
                raise TypeError("join keys must be hashable") from None
            if right is None and how != "left":
                continue
            plan = (
                target_cache.target_plan(
                    left,
                    columns,
                    shared_names=shared_names,
                    suffix=suffix,
                )
                if target_cache.enabled
                else None
            )
            targets = (
                _join_targets(
                    left,
                    columns,
                    shared_names=shared_names,
                    suffix=suffix,
                )
                if plan is None
                else None
            )
            if right is not None:
                if plan is None:
                    assert targets is not None
                    yield _merge_join_snapshot(left, right, targets, shared_names)
                else:
                    yield _merge_join_plan_snapshot(left, right, plan, shared_names, suffix)
            else:
                if plan is None:
                    assert targets is not None
                    for name, target in targets:
                        if name not in shared_names:
                            left[target] = None
                    yield left
                else:
                    yield _fill_unmatched_join_plan(left, plan, shared_names, suffix)
    finally:
        close = getattr(left_iterator, "close", None)
        if callable(close):
            close()


def _execute_group_aggregate(
    node: GroupAggregatePhysicalNode, runtime: QueryRuntime
) -> Iterator[dict[str, Any]]:
    """Aggregate first-seen groups directly from a physical branch iterator."""
    from ..runtime.failpoints import has_active_failpoints, hit

    group_rows = _try_arrow_group_sum(node)
    if group_rows is None:
        group_rows = _try_native_group_sum(node)
    if group_rows is not None:
        yield from group_rows
        return

    # Decide specialization before Source.open() so instrumented runs retain the
    # canonical claim and transition boundaries from their first observable step.
    count_spec = None if has_active_failpoints() else node.spill_count

    # Source.open() already owns claim and failpoint handling. Opening a leaf
    # here removes one forwarding generator from aggregation hot loops; nested
    # relational inputs still require the recursive executor.
    values = (
        node.input.source.open()
        if isinstance(node.input, SourcePhysicalNode)
        else execute_relational(node.input, runtime)
    )
    items = tuple(
        zip(
            node.aggregations.collectors.layout.names,
            node.aggregations.collectors.layout.collectors,
            strict=True,
        )
    )
    if node.partitions is not None:
        yield from spilled_group_aggregate(
            values,
            key_names=node.key_names,
            keys=node.keys,
            aggregation_items=cast(Any, items),
            partitions=node.partitions,
            tempdir=node.tempdir,
            limits=node.limits or SpillLimits(),
            runtime=runtime,
            count_spec=(
                None if count_spec is None else (count_spec.key_field, count_spec.output_name)
            ),
        )
        return
    if node.composite_count_sum is not None:
        yield from _materialize_composite_count_sum(values, node, node.composite_count_sum)
        return
    if node.simple_sum is not None:
        yield from _execute_simple_group_sum(values, node, node.simple_sum)
        return
    if node.closed_group is not None:
        yield from _execute_closed_group(values, node, node.closed_group)
        return
    groups: dict[Any, tuple[Any, Any]] = {}
    instrumented = has_active_failpoints()
    multiple = len(node.keys) > 1
    iterator = iter(values)
    try:
        for row in iterator:
            key = node.select_key(row)
            try:
                hash(key)
                entry = groups.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            if entry is None:
                state = node.aggregations.collectors.initialize()
                groups[key] = (key, state)
                if instrumented:
                    hit("group.state.create.after")
            else:
                _first, state = entry
            node.aggregations.collectors.step(state, row)
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()
    for _group_key, (key, state) in groups.items():
        values_for_key = key if multiple else (key,)
        result = dict(zip(node.key_names, values_for_key, strict=True))
        result.update(node.aggregations.collectors.finish(state))
        yield result


def _materialize_composite_count_sum(
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    spec: CompositeCountSumSpec,
) -> list[dict[str, Any]]:
    """Group two direct keys without per-group collector objects or selector generators."""
    from ..runtime.failpoints import has_active_failpoints, hit

    first_selector, second_selector = spec.key_selectors
    selectors_are_indexes = (
        type(first_selector) is int
        and type(second_selector) is int
        and type(spec.value_selector) is int
    )
    select_first, select_second = node.keys
    groups: dict[tuple[Any, Any], list[Any]] = {}
    instrumented = has_active_failpoints()
    skip_redundant_hash = (
        _CANONICAL_BUILTIN_HASH
        and _RELATIONAL_GLOBALS.get("hash", _BUILTIN_HASH) is _BUILTIN_HASH
        and _BUILTINS_DICT.get("hash") is _BUILTIN_HASH
    )
    direct_row_type: type[Any] = tuple if selectors_are_indexes else dict
    iterator = iter(values)
    try:
        for row in iterator:
            row_type = type(row)
            if row_type is direct_row_type:
                direct = True
            elif (
                row_type is dict
                or row_type is MappingProxyType
                or (selectors_are_indexes and (row_type is tuple or row_type is list))
            ):
                direct_row_type = row_type
                direct = True
            else:
                direct = False
            if direct:
                try:
                    first = row[first_selector]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_selector_error(first_selector, row, error)
                try:
                    second = row[second_selector]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_selector_error(second_selector, row, error)
            else:
                first = select_first(row)
                second = select_second(row)
            key = (first, second)
            try:
                if (
                    skip_redundant_hash
                    and _exact_builtin_group_key_type(type(first))
                    and _exact_builtin_group_key_type(type(second))
                ):
                    state = groups.get(key)
                else:
                    hash(key)
                    state = groups.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            if state is None:
                state = [key, 0, 0]
                groups[key] = state
                if instrumented:
                    hit("group.state.create.after")
            state[1] = state[1] + 1
            if direct:
                try:
                    selected = row[spec.value_selector]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_selector_error(spec.value_selector, row, error)
            else:
                selected = spec.select_value(row)
            state[2] = state[2] + selected
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()

    first_name, second_name = node.key_names
    return [
        {
            first_name: key[0],
            second_name: key[1],
            spec.count_name: count,
            spec.sum_name: total,
        }
        for key, count, total in groups.values()
    ]


def _common_closed_group_mode(spec: ClosedGroupSpec) -> int:
    """Select a hand-unrolled fixed-lane shape outside the per-row loop."""
    key = spec.key_selector
    key_direct = type(key) is int or (type(key) is str and "." not in key)
    signature = tuple(operation.kind for operation in spec.lanes)
    mode = {
        ("count",): 1,
        ("count", "sum"): 2,
        ("min", "max", "first", "last"): 3,
        ("count", "sum", "min", "max", "first", "last"): 4,
    }.get(signature, 0)
    if mode == 0 or (not key_direct and (mode not in (1, 2) or not callable(key))):
        return 0
    return (
        mode
        if all(
            operation.kind == "count"
            or type(operation.selector) is int
            or (type(operation.selector) is str and "." not in operation.selector)
            for operation in spec.lanes
        )
        else 0
    )


def _execute_common_closed_group(  # noqa: C901 - measured hand-unrolled hot loop
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    spec: ClosedGroupSpec,
    mode: int,
) -> Iterator[dict[str, Any]]:
    """Execute common direct aggregation shapes without per-row lane dispatch."""
    from ..runtime.failpoints import has_active_failpoints, hit

    operations = spec.lanes
    key_selector = cast(str | int, spec.key_selector)
    key_direct = type(key_selector) is int or (
        type(key_selector) is str and "." not in key_selector
    )
    select_key = node.keys[0]
    instrumented = has_active_failpoints()
    groups: dict[Any, list[Any]] = {}

    sum_selector = operations[1].selector if mode == 2 or mode == 4 else None
    sum_value = operations[1].select_value if mode == 2 or mode == 4 else None
    extrema_offset = 2 if mode == 4 else 0
    if mode == 3 or mode == 4:
        min_operation = operations[extrema_offset]
        max_operation = operations[extrema_offset + 1]
        first_operation = operations[extrema_offset + 2]
        last_operation = operations[extrema_offset + 3]
        min_selector = min_operation.selector
        max_selector = max_operation.selector
        first_selector = first_operation.selector
        last_selector = last_operation.selector
        min_value = min_operation.select_value
        max_value = max_operation.select_value
        first_value = first_operation.select_value
        last_value = last_operation.select_value
    else:
        min_selector = max_selector = first_selector = last_selector = None
        min_value = max_value = first_value = last_value = None

    iterator = iter(values)
    try:
        for row in iterator:
            exact_dict = type(row) is dict
            if exact_dict and key_direct:
                try:
                    key = row[key_selector]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_selector_error(key_selector, row, error)
            else:
                key = select_key(row)
            try:
                hash(key)
                state = groups.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            if state is None:
                if mode == 1:
                    state = [key, 0]
                elif mode == 2:
                    state = [key, 0, 0]
                elif mode == 3:
                    state = [key, _MISSING, _MISSING, _MISSING, _MISSING]
                else:
                    state = [key, 0, 0, _MISSING, _MISSING, _MISSING, _MISSING]
                groups[key] = state
                if instrumented:
                    hit("group.state.create.after")

            if mode == 1:
                state[1] = state[1] + 1
                continue
            if mode == 2 or mode == 4:
                state[1] = state[1] + 1
                if exact_dict:
                    assert sum_selector is not None
                    try:
                        selected = row[sum_selector]
                    except (AttributeError, IndexError, KeyError, TypeError) as error:
                        _raise_selector_error(cast(str | int, sum_selector), row, error)
                else:
                    assert sum_value is not None
                    selected = sum_value(row)
                state[2] = state[2] + selected
                if mode == 2:
                    continue

            state_offset = 3 if mode == 4 else 1
            if exact_dict:
                assert min_selector is not None
                try:
                    selected = row[min_selector]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_selector_error(cast(str | int, min_selector), row, error)
            else:
                assert min_value is not None
                selected = min_value(row)
            current = state[state_offset]
            if current is _MISSING or selected < current:
                state[state_offset] = selected

            if exact_dict:
                assert max_selector is not None
                try:
                    selected = row[max_selector]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_selector_error(cast(str | int, max_selector), row, error)
            else:
                assert max_value is not None
                selected = max_value(row)
            current = state[state_offset + 1]
            if current is _MISSING or selected > current:
                state[state_offset + 1] = selected

            if state[state_offset + 2] is _MISSING:
                if exact_dict:
                    assert first_selector is not None
                    try:
                        selected = row[first_selector]
                    except (AttributeError, IndexError, KeyError, TypeError) as error:
                        _raise_selector_error(cast(str | int, first_selector), row, error)
                else:
                    assert first_value is not None
                    selected = first_value(row)
                state[state_offset + 2] = selected

            if exact_dict:
                assert last_selector is not None
                try:
                    selected = row[last_selector]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_selector_error(cast(str | int, last_selector), row, error)
            else:
                assert last_value is not None
                selected = last_value(row)
            state[state_offset + 3] = selected
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()

    key_name = node.key_names[0]
    if mode == 1:
        output_name = operations[0].output_name
        for state in groups.values():
            yield {key_name: state[0], output_name: state[1]}
    elif mode == 2:
        count_name = operations[0].output_name
        sum_name = operations[1].output_name
        for state in groups.values():
            yield {key_name: state[0], count_name: state[1], sum_name: state[2]}
    elif mode == 3:
        for state in groups.values():
            yield {
                key_name: state[0],
                operations[0].output_name: None if state[1] is _MISSING else state[1],
                operations[1].output_name: None if state[2] is _MISSING else state[2],
                operations[2].output_name: None if state[3] is _MISSING else state[3],
                operations[3].output_name: None if state[4] is _MISSING else state[4],
            }
    else:
        for state in groups.values():
            yield {
                key_name: state[0],
                operations[0].output_name: state[1],
                operations[1].output_name: state[2],
                operations[2].output_name: None if state[3] is _MISSING else state[3],
                operations[3].output_name: None if state[4] is _MISSING else state[4],
                operations[4].output_name: None if state[5] is _MISSING else state[5],
                operations[5].output_name: None if state[6] is _MISSING else state[6],
            }


def _consume_callable_key_count_exact_hash_tail(
    iterator: Iterator[Any],
    select_key: Callable[[Any], Any],
    groups: dict[Any, list[Any]],
    exact_key_type: type[Any],
) -> None:
    """Consume a proven monomorphic exact-builtin key tail while guarding later drift."""
    for row in iterator:
        key = select_key(row)
        try:
            if type(key) is exact_key_type:
                state = groups.get(key)
            else:
                hash(key)
                state = groups.get(key)
        except TypeError:
            raise TypeError("group_by keys must be hashable") from None
        if state is None:
            state = [key, 0]
            groups[key] = state
        state[1] = state[1] + 1


def _consume_callable_key_count_sum_exact_hash_tail(
    iterator: Iterator[Any],
    select_key: Callable[[Any], Any],
    groups: dict[Any, list[Any]],
    exact_key_type: type[Any],
    value_selector: str | int,
    tuple_value_index: int | None,
    direct_row_type: type[Any],
    cached_mapping_type: type[Any] | None,
    cached_mapping_mro: tuple[type[Any], ...] | None,
    cached_non_direct_type: type[Any] | None,
    select_value: Callable[[Any], Any],
) -> None:
    """Consume a callable-key count/sum tail without weakening the row-shape PIC."""
    for row in iterator:
        key = select_key(row)
        try:
            if type(key) is exact_key_type:
                state = groups.get(key)
            else:
                hash(key)
                state = groups.get(key)
        except TypeError:
            raise TypeError("group_by keys must be hashable") from None
        if state is None:
            state = [key, 0, 0]
            groups[key] = state
        state[1] = state[1] + 1
        row_type = type(row)
        if row_type is direct_row_type:
            try:
                selected = row[value_selector]
            except (AttributeError, IndexError, KeyError, TypeError) as error:
                _raise_selector_error(value_selector, row, error)
        elif row_type is cached_non_direct_type:
            selected = select_value(row)
        else:
            if row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro:
                direct_row = True
            elif (
                row_type is dict
                or (tuple_value_index is not None and row_type is tuple)
                or row_type is MappingProxyType
            ):
                direct_row_type = row_type
                direct_row = True
            else:
                mapping_mro = _direct_mapping_mro(row_type)
                if mapping_mro is None or not isinstance(row, Mapping):
                    cached_non_direct_type = row_type
                    direct_row = False
                else:
                    cached_mapping_type = row_type
                    cached_mapping_mro = mapping_mro
                    direct_row = True
            if direct_row:
                try:
                    selected = row[value_selector]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_selector_error(value_selector, row, error)
            else:
                selected = select_value(row)
        state[2] = state[2] + selected


def _consume_callable_key_value_count_sum_exact_hash_tail(
    iterator: Iterator[Any],
    select_key: Callable[[Any], Any],
    select_value: Callable[[Any], Any],
    groups: dict[Any, list[Any]],
    exact_key_type: type[Any],
) -> None:
    """Consume a two-callback count/sum tail with one redundant builtin hash removed."""
    for row in iterator:
        key = select_key(row)
        try:
            if type(key) is exact_key_type:
                state = groups.get(key)
            else:
                hash(key)
                state = groups.get(key)
        except TypeError:
            raise TypeError("group_by keys must be hashable") from None
        if state is None:
            state = [key, 0, 0]
            groups[key] = state
        state[1] = state[1] + 1
        selected = select_value(row)
        state[2] = state[2] + selected


def _consume_callable_value_count_sum_exact_hash_tail(
    iterator: Iterator[Any],
    select_key: Callable[[Any], Any],
    select_value: Callable[[Any], Any],
    groups: dict[Any, list[Any]],
    exact_key_type: type[Any],
    key_selector: str | int,
    tuple_key_index: int | None,
    direct_row_type: type[Any],
    cached_mapping_type: type[Any] | None,
    cached_mapping_mro: tuple[type[Any], ...] | None,
    cached_non_direct_type: type[Any] | None,
) -> None:
    """Consume a direct-key/callable-value tail while retaining live shape guards."""
    for row in iterator:
        row_type = type(row)
        if row_type is direct_row_type:
            try:
                key = row[key_selector]
            except (AttributeError, IndexError, KeyError, TypeError) as error:
                _raise_selector_error(key_selector, row, error)
        elif row_type is cached_non_direct_type:
            key = select_key(row)
        else:
            if row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro:
                direct_row = True
            elif (
                row_type is dict
                or (tuple_key_index is not None and row_type is tuple)
                or row_type is MappingProxyType
            ):
                direct_row_type = row_type
                direct_row = True
            else:
                mapping_mro = _direct_mapping_mro(row_type)
                if mapping_mro is None or not isinstance(row, Mapping):
                    cached_non_direct_type = row_type
                    direct_row = False
                else:
                    cached_mapping_type = row_type
                    cached_mapping_mro = mapping_mro
                    direct_row = True
            if direct_row:
                try:
                    key = row[key_selector]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_selector_error(key_selector, row, error)
            else:
                key = select_key(row)
        try:
            if type(key) is exact_key_type:
                state = groups.get(key)
            else:
                hash(key)
                state = groups.get(key)
        except TypeError:
            raise TypeError("group_by keys must be hashable") from None
        if state is None:
            state = [key, 0, 0]
            groups[key] = state
        state[1] = state[1] + 1
        selected = select_value(row)
        state[2] = state[2] + selected


def _execute_callable_key_count_group(  # noqa: C901 - keep measured row-type PIC inline
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    spec: ClosedGroupSpec,
    mode: int,
) -> Iterator[dict[str, Any]]:
    """Run callable-key count shapes without row-kind or lane dispatch in their hot loop."""
    operations = spec.lanes
    select_key = node.keys[0]
    groups: dict[Any, list[Any]] = {}
    iterator = iter(values)
    try:
        if mode == 1:
            candidate_key_type: type[Any] | None = None
            exact_builtin_candidate = False
            prefix_rows = 0
            for row in islice(iterator, _CALLABLE_GROUP_HASH_WARMUP_ROWS):
                key = select_key(row)
                try:
                    hash(key)
                    state = groups.get(key)
                except TypeError:
                    raise TypeError("group_by keys must be hashable") from None
                if state is None:
                    state = [key, 0]
                    groups[key] = state
                state[1] = state[1] + 1
                key_type = type(key)
                if candidate_key_type is None:
                    candidate_key_type = key_type
                    exact_builtin_candidate = _exact_builtin_group_key_type(key_type)
                elif key_type is not candidate_key_type:
                    exact_builtin_candidate = False
                prefix_rows += 1
            if prefix_rows == _CALLABLE_GROUP_HASH_WARMUP_ROWS and exact_builtin_candidate:
                assert candidate_key_type is not None
                _consume_callable_key_count_exact_hash_tail(
                    iterator,
                    select_key,
                    groups,
                    candidate_key_type,
                )
            else:
                for row in iterator:
                    key = select_key(row)
                    try:
                        hash(key)
                        state = groups.get(key)
                    except TypeError:
                        raise TypeError("group_by keys must be hashable") from None
                    if state is None:
                        state = [key, 0]
                        groups[key] = state
                    state[1] = state[1] + 1
        else:
            value_selector = cast(str | int, operations[1].selector)
            tuple_value_index = value_selector if type(value_selector) is int else None
            direct_row_type: type[Any] = tuple if tuple_value_index is not None else dict
            cached_mapping_type: type[Any] | None = None
            cached_mapping_mro: tuple[type[Any], ...] | None = None
            cached_non_direct_type: type[Any] | None = None
            select_value = operations[1].select_value
            assert select_value is not None
            candidate_key_type = None
            exact_builtin_candidate = False
            prefix_rows = 0
            for row in islice(iterator, _CALLABLE_GROUP_HASH_WARMUP_ROWS):
                key = select_key(row)
                try:
                    hash(key)
                    state = groups.get(key)
                except TypeError:
                    raise TypeError("group_by keys must be hashable") from None
                if state is None:
                    state = [key, 0, 0]
                    groups[key] = state
                state[1] = state[1] + 1
                row_type = type(row)
                if row_type is direct_row_type:
                    try:
                        selected = row[value_selector]
                    except (AttributeError, IndexError, KeyError, TypeError) as error:
                        _raise_selector_error(value_selector, row, error)
                elif row_type is cached_non_direct_type:
                    selected = select_value(row)
                else:
                    if row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro:
                        direct_row = True
                    elif (
                        row_type is dict
                        or (tuple_value_index is not None and row_type is tuple)
                        or row_type is MappingProxyType
                    ):
                        direct_row_type = row_type
                        direct_row = True
                    else:
                        mapping_mro = _direct_mapping_mro(row_type)
                        if mapping_mro is None or not isinstance(row, Mapping):
                            cached_non_direct_type = row_type
                            direct_row = False
                        else:
                            cached_mapping_type = row_type
                            cached_mapping_mro = mapping_mro
                            direct_row = True
                    if direct_row:
                        try:
                            selected = row[value_selector]
                        except (
                            AttributeError,
                            IndexError,
                            KeyError,
                            TypeError,
                        ) as error:
                            _raise_selector_error(value_selector, row, error)
                    else:
                        selected = select_value(row)
                state[2] = state[2] + selected
                key_type = type(key)
                if candidate_key_type is None:
                    candidate_key_type = key_type
                    exact_builtin_candidate = _exact_builtin_group_key_type(key_type)
                elif key_type is not candidate_key_type:
                    exact_builtin_candidate = False
                prefix_rows += 1
            if prefix_rows == _CALLABLE_GROUP_HASH_WARMUP_ROWS and exact_builtin_candidate:
                assert candidate_key_type is not None
                _consume_callable_key_count_sum_exact_hash_tail(
                    iterator,
                    select_key,
                    groups,
                    candidate_key_type,
                    value_selector,
                    tuple_value_index,
                    direct_row_type,
                    cached_mapping_type,
                    cached_mapping_mro,
                    cached_non_direct_type,
                    select_value,
                )
            else:
                for row in iterator:
                    key = select_key(row)
                    try:
                        hash(key)
                        state = groups.get(key)
                    except TypeError:
                        raise TypeError("group_by keys must be hashable") from None
                    if state is None:
                        state = [key, 0, 0]
                        groups[key] = state
                    state[1] = state[1] + 1
                    row_type = type(row)
                    if row_type is direct_row_type:
                        try:
                            selected = row[value_selector]
                        except (
                            AttributeError,
                            IndexError,
                            KeyError,
                            TypeError,
                        ) as error:
                            _raise_selector_error(value_selector, row, error)
                    elif row_type is cached_non_direct_type:
                        selected = select_value(row)
                    else:
                        if (
                            row_type is cached_mapping_type
                            and row_type.__mro__ is cached_mapping_mro
                        ):
                            direct_row = True
                        elif (
                            row_type is dict
                            or (tuple_value_index is not None and row_type is tuple)
                            or row_type is MappingProxyType
                        ):
                            direct_row_type = row_type
                            direct_row = True
                        else:
                            mapping_mro = _direct_mapping_mro(row_type)
                            if mapping_mro is None or not isinstance(row, Mapping):
                                cached_non_direct_type = row_type
                                direct_row = False
                            else:
                                cached_mapping_type = row_type
                                cached_mapping_mro = mapping_mro
                                direct_row = True
                        if direct_row:
                            try:
                                selected = row[value_selector]
                            except (
                                AttributeError,
                                IndexError,
                                KeyError,
                                TypeError,
                            ) as error:
                                _raise_selector_error(value_selector, row, error)
                        else:
                            selected = select_value(row)
                    state[2] = state[2] + selected
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()

    key_name = node.key_names[0]
    count_name = operations[0].output_name
    if mode == 1:
        for state in groups.values():
            yield {key_name: state[0], count_name: state[1]}
    else:
        sum_name = operations[1].output_name
        for state in groups.values():
            yield {key_name: state[0], count_name: state[1], sum_name: state[2]}


def _execute_callable_value_count_sum_group(  # noqa: C901 - measured row-type PIC
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    spec: ClosedGroupSpec,
) -> Iterator[dict[str, Any]]:
    """Run count plus an opaque sum callback in one fixed-state group loop."""
    operations = spec.lanes
    select_key = node.keys[0]
    select_value = operations[1].select_value
    assert select_value is not None
    groups: dict[Any, list[Any]] = {}
    iterator = iter(values)
    try:
        if callable(spec.key_selector):
            candidate_key_type: type[Any] | None = None
            exact_builtin_candidate = False
            prefix_rows = 0
            for row in islice(iterator, _CALLABLE_GROUP_HASH_WARMUP_ROWS):
                key = select_key(row)
                try:
                    hash(key)
                    state = groups.get(key)
                except TypeError:
                    raise TypeError("group_by keys must be hashable") from None
                if state is None:
                    state = [key, 0, 0]
                    groups[key] = state
                state[1] = state[1] + 1
                selected = select_value(row)
                state[2] = state[2] + selected
                key_type = type(key)
                if candidate_key_type is None:
                    candidate_key_type = key_type
                    exact_builtin_candidate = _exact_builtin_group_key_type(key_type)
                elif key_type is not candidate_key_type:
                    exact_builtin_candidate = False
                prefix_rows += 1
            if prefix_rows == _CALLABLE_GROUP_HASH_WARMUP_ROWS and exact_builtin_candidate:
                assert candidate_key_type is not None
                _consume_callable_key_value_count_sum_exact_hash_tail(
                    iterator,
                    select_key,
                    select_value,
                    groups,
                    candidate_key_type,
                )
            else:
                for row in iterator:
                    key = select_key(row)
                    try:
                        hash(key)
                        state = groups.get(key)
                    except TypeError:
                        raise TypeError("group_by keys must be hashable") from None
                    if state is None:
                        state = [key, 0, 0]
                        groups[key] = state
                    state[1] = state[1] + 1
                    selected = select_value(row)
                    state[2] = state[2] + selected
        else:
            key_selector = spec.key_selector
            tuple_key_index = key_selector if type(key_selector) is int else None
            direct_row_type: type[Any] = tuple if tuple_key_index is not None else dict
            cached_mapping_type: type[Any] | None = None
            cached_mapping_mro: tuple[type[Any], ...] | None = None
            cached_non_direct_type: type[Any] | None = None
            candidate_key_type = None
            exact_builtin_candidate = False
            prefix_rows = 0
            for row in islice(iterator, _CALLABLE_GROUP_HASH_WARMUP_ROWS):
                row_type = type(row)
                if row_type is direct_row_type:
                    try:
                        key = row[key_selector]
                    except (AttributeError, IndexError, KeyError, TypeError) as error:
                        _raise_selector_error(key_selector, row, error)
                elif row_type is cached_non_direct_type:
                    key = select_key(row)
                else:
                    if row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro:
                        direct_row = True
                    elif (
                        row_type is dict
                        or (tuple_key_index is not None and row_type is tuple)
                        or row_type is MappingProxyType
                    ):
                        direct_row_type = row_type
                        direct_row = True
                    else:
                        mapping_mro = _direct_mapping_mro(row_type)
                        if mapping_mro is None or not isinstance(row, Mapping):
                            cached_non_direct_type = row_type
                            direct_row = False
                        else:
                            cached_mapping_type = row_type
                            cached_mapping_mro = mapping_mro
                            direct_row = True
                    if direct_row:
                        try:
                            key = row[key_selector]
                        except (
                            AttributeError,
                            IndexError,
                            KeyError,
                            TypeError,
                        ) as error:
                            _raise_selector_error(key_selector, row, error)
                    else:
                        key = select_key(row)
                try:
                    hash(key)
                    state = groups.get(key)
                except TypeError:
                    raise TypeError("group_by keys must be hashable") from None
                if state is None:
                    state = [key, 0, 0]
                    groups[key] = state
                state[1] = state[1] + 1
                selected = select_value(row)
                state[2] = state[2] + selected
                key_type = type(key)
                if candidate_key_type is None:
                    candidate_key_type = key_type
                    exact_builtin_candidate = _exact_builtin_group_key_type(key_type)
                elif key_type is not candidate_key_type:
                    exact_builtin_candidate = False
                prefix_rows += 1
            if prefix_rows == _CALLABLE_GROUP_HASH_WARMUP_ROWS and exact_builtin_candidate:
                assert candidate_key_type is not None
                _consume_callable_value_count_sum_exact_hash_tail(
                    iterator,
                    select_key,
                    select_value,
                    groups,
                    candidate_key_type,
                    key_selector,
                    tuple_key_index,
                    direct_row_type,
                    cached_mapping_type,
                    cached_mapping_mro,
                    cached_non_direct_type,
                )
            else:
                for row in iterator:
                    row_type = type(row)
                    if row_type is direct_row_type:
                        try:
                            key = row[key_selector]
                        except (
                            AttributeError,
                            IndexError,
                            KeyError,
                            TypeError,
                        ) as error:
                            _raise_selector_error(key_selector, row, error)
                    elif row_type is cached_non_direct_type:
                        key = select_key(row)
                    else:
                        if (
                            row_type is cached_mapping_type
                            and row_type.__mro__ is cached_mapping_mro
                        ):
                            direct_row = True
                        elif (
                            row_type is dict
                            or (tuple_key_index is not None and row_type is tuple)
                            or row_type is MappingProxyType
                        ):
                            direct_row_type = row_type
                            direct_row = True
                        else:
                            mapping_mro = _direct_mapping_mro(row_type)
                            if mapping_mro is None or not isinstance(row, Mapping):
                                cached_non_direct_type = row_type
                                direct_row = False
                            else:
                                cached_mapping_type = row_type
                                cached_mapping_mro = mapping_mro
                                direct_row = True
                        if direct_row:
                            try:
                                key = row[key_selector]
                            except (
                                AttributeError,
                                IndexError,
                                KeyError,
                                TypeError,
                            ) as error:
                                _raise_selector_error(key_selector, row, error)
                        else:
                            key = select_key(row)
                    try:
                        hash(key)
                        state = groups.get(key)
                    except TypeError:
                        raise TypeError("group_by keys must be hashable") from None
                    if state is None:
                        state = [key, 0, 0]
                        groups[key] = state
                    state[1] = state[1] + 1
                    selected = select_value(row)
                    state[2] = state[2] + selected
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()

    key_name = node.key_names[0]
    count_name = operations[0].output_name
    sum_name = operations[1].output_name
    for state in groups.values():
        yield {key_name: state[0], count_name: state[1], sum_name: state[2]}


def _execute_closed_group(  # noqa: C901 - keep dispatch inline in this measured hot loop
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    spec: ClosedGroupSpec,
) -> Iterator[dict[str, Any]]:
    """Run project-owned fixed-size aggregations in contiguous per-operation lanes."""
    from ..runtime.failpoints import has_active_failpoints, hit

    operations = spec.lanes
    instrumented = has_active_failpoints()
    key_selector = spec.key_selector
    fixed_key = (
        callable(key_selector)
        or type(key_selector) is int
        or (type(key_selector) is str and "." not in key_selector)
    )
    if (
        not instrumented
        and fixed_key
        and len(operations) == 2
        and operations[0].kind == "count"
        and operations[1].kind == "sum"
        and callable(operations[1].selector)
    ):
        yield from _execute_callable_value_count_sum_group(values, node, spec)
        return
    mode = _common_closed_group_mode(spec)
    if mode in (1, 2) and callable(spec.key_selector) and not instrumented:
        yield from _execute_callable_key_count_group(values, node, spec, mode)
        return
    if mode:
        yield from _execute_common_closed_group(values, node, spec, mode)
        return

    states: tuple[list[Any], ...] = tuple([] for _operation in operations)
    initials = tuple(
        0 if operation.kind == "count" or operation.kind == "sum" else _MISSING
        for operation in operations
    )
    direct_values = tuple(
        type(operation.selector) is int
        or (type(operation.selector) is str and "." not in operation.selector)
        for operation in operations
    )
    positions: dict[Any, int] = {}
    keys: list[Any] = []
    key_selector = spec.key_selector
    select_key = node.keys[0]
    key_direct = type(key_selector) is int or (
        type(key_selector) is str and "." not in key_selector
    )
    iterator = iter(values)
    try:
        for row in iterator:
            exact_dict = type(row) is dict
            if exact_dict and key_direct:
                try:
                    key = row[key_selector]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_selector_error(cast(str | int, key_selector), row, error)
            else:
                key = select_key(row)
            try:
                hash(key)
                position = positions.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            if position is None:
                position = len(keys)
                for lane, initial in zip(states, initials, strict=True):
                    lane.append(initial)
                positions[key] = position
                keys.append(key)
                if instrumented:
                    hit("group.state.create.after")

            for lane_index, operation in enumerate(operations):
                lane = states[lane_index]
                kind = operation.kind
                current = lane[position]
                if kind == "count":
                    lane[position] = current + 1
                    continue
                if kind == "first" and current is not _MISSING:
                    continue

                selector = operation.selector
                if selector is None:
                    selected = row
                elif exact_dict and direct_values[lane_index]:
                    try:
                        selected = row[selector]
                    except (AttributeError, IndexError, KeyError, TypeError) as error:
                        _raise_selector_error(cast(str | int, selector), row, error)
                else:
                    select_value = operation.select_value
                    assert select_value is not None
                    selected = select_value(row)

                if kind == "sum":
                    lane[position] = current + selected
                elif kind == "min":
                    if current is _MISSING or selected < current:
                        lane[position] = selected
                elif kind == "max":
                    if current is _MISSING or selected > current:
                        lane[position] = selected
                else:
                    # ``first`` and ``last`` both replace their state when selected;
                    # completed first lanes were skipped before selector evaluation.
                    lane[position] = selected
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()

    key_name = node.key_names[0]
    for position, key in enumerate(keys):
        result = {key_name: key}
        for operation, lane in zip(operations, states, strict=True):
            value = lane[position]
            result[operation.output_name] = None if value is _MISSING else value
        yield result


def _execute_simple_group_sum(
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    spec: SimpleGroupSumSpec,
) -> Iterator[dict[str, Any]]:
    """Sum one selected field per group with the canonical observable ordering.

    Exact immutable built-in keys may use the hash already performed by the
    group dictionary. Custom keys retain the preliminary ``hash`` call made by
    the generic collector, including its exception and call-order semantics.
    """
    from ..runtime.failpoints import has_active_failpoints

    groups: dict[Any, list[Any]] = {}
    key_selector = spec.key_selector
    value_selector = spec.value_selector
    instrumented = has_active_failpoints()
    iterator = iter(values)
    try:
        key_direct = type(key_selector) is int or (
            type(key_selector) is str and "." not in key_selector
        )
        value_direct = type(value_selector) is int or (
            type(value_selector) is str and "." not in value_selector
        )
        direct = key_direct and value_direct
        if direct:
            _consume_direct_simple_group_sum(
                iterator,
                groups,
                node,
                spec,
                instrumented=instrumented,
            )
        else:
            _consume_canonical_simple_group_sum(
                iterator,
                groups,
                node,
                spec,
                instrumented=instrumented,
            )
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()

    key_name = node.key_names[0]
    for key, total in groups.values():
        yield {key_name: key, spec.output_name: total}


def _consume_direct_simple_group_sum(
    iterator: Iterator[Any],
    groups: dict[Any, list[Any]],
    node: GroupAggregatePhysicalNode,
    spec: SimpleGroupSumSpec,
    *,
    instrumented: bool,
) -> None:
    """Consume a direct selector shape, specializing only proven exact rows and keys."""
    from ..runtime.failpoints import hit

    try:
        row = next(iterator)
    except StopIteration:
        return

    key_selector = cast(str | int, spec.key_selector)
    value_selector = cast(str | int, spec.value_selector)
    select_key = node.keys[0]
    row_type = type(row)
    exact_dict = row_type is dict
    exact_mapping_proxy = row_type is MappingProxyType
    mapping_row = exact_dict or exact_mapping_proxy or isinstance(row, Mapping)
    stable_mapping_mro = (
        _direct_mapping_mro(row_type)
        if mapping_row and not exact_dict and not exact_mapping_proxy
        else None
    )
    if mapping_row:
        try:
            key = row[key_selector]
        except (AttributeError, IndexError, KeyError, TypeError) as error:
            _raise_selector_error(key_selector, row, error)
    else:
        key = select_key(row)

    key_type = type(key)
    fast_key_type = (
        key_type
        if (
            exact_dict
            and (
                key_type is int
                or key_type is str
                or key_type is bytes
                or key_type is bool
                or key_type is float
                or key is None
            )
        )
        else None
    )
    if fast_key_type is None:
        try:
            hash(key)
            entry = groups.get(key)
        except TypeError:
            raise TypeError("group_by keys must be hashable") from None
    else:
        entry = groups.get(key)
    if entry is None:
        entry = [key, 0]
        groups[key] = entry
        if instrumented:
            hit("group.state.create.after")
    if mapping_row:
        try:
            selected = row[value_selector]
        except (AttributeError, IndexError, KeyError, TypeError) as error:
            _raise_selector_error(value_selector, row, error)
    else:
        selected = spec.select_value(row)
    entry[1] = entry[1] + selected

    if fast_key_type is None:
        if mapping_row and (exact_dict or exact_mapping_proxy or stable_mapping_mro is not None):
            _consume_stable_mapping_simple_group_sum(
                iterator,
                groups,
                node,
                spec,
                stable_mapping_type=row_type,
                stable_mapping_mro=stable_mapping_mro,
                instrumented=instrumented,
            )
        else:
            consume = (
                _consume_mapping_simple_group_sum
                if mapping_row
                else _consume_canonical_simple_group_sum
            )
            consume(iterator, groups, node, spec, instrumented=instrumented)
        return

    _consume_exact_builtin_simple_group_sum(
        iterator,
        groups,
        node,
        spec,
        fast_key_type,
        instrumented=instrumented,
    )


def _consume_exact_builtin_simple_group_sum(
    iterator: Iterator[Any],
    groups: dict[Any, list[Any]],
    node: GroupAggregatePhysicalNode,
    spec: SimpleGroupSumSpec,
    fast_key_type: type[Any],
    *,
    instrumented: bool,
) -> None:
    """Consume remaining rows while re-proving the exact fast shape per row."""
    from ..runtime.failpoints import hit

    key_selector = cast(str | int, spec.key_selector)
    value_selector = cast(str | int, spec.value_selector)
    select_key = node.keys[0]
    for row in iterator:
        if type(row) is dict:
            try:
                key = row[key_selector]
            except (AttributeError, IndexError, KeyError, TypeError) as error:
                _raise_selector_error(key_selector, row, error)
            if type(key) is fast_key_type:
                try:
                    entry = groups.get(key)
                except TypeError:
                    # A built-in key can still compare against an earlier
                    # custom collision key stored in the same dictionary.
                    raise TypeError("group_by keys must be hashable") from None
            else:
                try:
                    hash(key)
                    entry = groups.get(key)
                except TypeError:
                    raise TypeError("group_by keys must be hashable") from None
            if entry is None:
                entry = [key, 0]
                groups[key] = entry
                if instrumented:
                    hit("group.state.create.after")
            try:
                selected = row[value_selector]
            except (AttributeError, IndexError, KeyError, TypeError) as error:
                _raise_selector_error(value_selector, row, error)
        else:
            key = select_key(row)
            try:
                hash(key)
                entry = groups.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            if entry is None:
                entry = [key, 0]
                groups[key] = entry
                if instrumented:
                    hit("group.state.create.after")
            selected = spec.select_value(row)
        entry[1] = entry[1] + selected


def _consume_stable_mapping_simple_group_sum(
    iterator: Iterator[Any],
    groups: dict[Any, list[Any]],
    node: GroupAggregatePhysicalNode,
    spec: SimpleGroupSumSpec,
    *,
    stable_mapping_type: type[Any],
    stable_mapping_mro: tuple[type[Any], ...] | None,
    instrumented: bool,
) -> None:
    """Consume a proven concrete Mapping shape without repeating its ABC check."""
    from ..runtime.failpoints import hit

    key_selector = cast(str | int, spec.key_selector)
    value_selector = cast(str | int, spec.value_selector)
    select_key = node.keys[0]
    cached_mapping_type = stable_mapping_type if stable_mapping_mro is not None else None
    cached_mapping_mro = stable_mapping_mro
    cached_non_direct_type: type[Any] | None = None
    for row in iterator:
        row_type = type(row)
        if (
            row_type is dict
            or row_type is MappingProxyType
            or (row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro)
        ):
            mapping_row = True
        else:
            mapping_mro = (
                None if row_type is cached_non_direct_type else _direct_mapping_mro(row_type)
            )
            if mapping_mro is None:
                cached_non_direct_type = row_type
                mapping_row = isinstance(row, Mapping)
            else:
                cached_mapping_type = row_type
                cached_mapping_mro = mapping_mro
                mapping_row = True
        if mapping_row:
            try:
                key = row[key_selector]
            except (AttributeError, IndexError, KeyError, TypeError) as error:
                _raise_selector_error(key_selector, row, error)
        else:
            key = select_key(row)
        try:
            hash(key)
            entry = groups.get(key)
        except TypeError:
            raise TypeError("group_by keys must be hashable") from None
        if entry is None:
            entry = [key, 0]
            groups[key] = entry
            if instrumented:
                hit("group.state.create.after")
        if mapping_row:
            try:
                selected = row[value_selector]
            except (AttributeError, IndexError, KeyError, TypeError) as error:
                _raise_selector_error(value_selector, row, error)
        else:
            selected = spec.select_value(row)
        entry[1] = entry[1] + selected


def _consume_mapping_simple_group_sum(
    iterator: Iterator[Any],
    groups: dict[Any, list[Any]],
    node: GroupAggregatePhysicalNode,
    spec: SimpleGroupSumSpec,
    *,
    instrumented: bool,
) -> None:
    """Consume direct selectors while classifying each generic Mapping row only once."""
    from ..runtime.failpoints import hit

    key_selector = cast(str | int, spec.key_selector)
    value_selector = cast(str | int, spec.value_selector)
    select_key = node.keys[0]
    for row in iterator:
        mapping_row = isinstance(row, Mapping)
        if mapping_row:
            try:
                key = row[key_selector]
            except (AttributeError, IndexError, KeyError, TypeError) as error:
                _raise_selector_error(key_selector, row, error)
        else:
            key = select_key(row)
        try:
            hash(key)
            entry = groups.get(key)
        except TypeError:
            raise TypeError("group_by keys must be hashable") from None
        if entry is None:
            entry = [key, 0]
            groups[key] = entry
            if instrumented:
                hit("group.state.create.after")
        if mapping_row:
            try:
                selected = row[value_selector]
            except (AttributeError, IndexError, KeyError, TypeError) as error:
                _raise_selector_error(value_selector, row, error)
        else:
            selected = spec.select_value(row)
        entry[1] = entry[1] + selected


def _consume_canonical_simple_group_sum(
    iterator: Iterator[Any],
    groups: dict[Any, list[Any]],
    node: GroupAggregatePhysicalNode,
    spec: SimpleGroupSumSpec,
    *,
    instrumented: bool,
) -> None:
    """Consume protocol-sensitive rows with the generic selector and hash ordering."""
    from ..runtime.failpoints import hit

    select_key = node.keys[0]
    key_selector = spec.key_selector
    value_selector = spec.value_selector
    key_is_index = type(key_selector) is int
    value_is_index = type(value_selector) is int
    key_direct = key_is_index or (type(key_selector) is str and "." not in key_selector)
    value_direct = value_is_index or (type(value_selector) is str and "." not in value_selector)
    for row in iterator:
        exact_dict = type(row) is dict
        if exact_dict and key_direct:
            try:
                key = row[key_selector]
            except (AttributeError, IndexError, KeyError, TypeError) as error:
                _raise_selector_error(cast(str | int, key_selector), row, error)
        else:
            key = select_key(row)
        try:
            hash(key)
            entry = groups.get(key)
        except TypeError:
            raise TypeError("group_by keys must be hashable") from None
        if entry is None:
            entry = [key, 0]
            groups[key] = entry
            if instrumented:
                hit("group.state.create.after")
        if exact_dict and value_direct:
            direct_value_selector = cast(str | int, value_selector)
            try:
                selected = row[direct_value_selector]
            except (AttributeError, IndexError, KeyError, TypeError) as error:
                _raise_selector_error(direct_value_selector, row, error)
        else:
            selected = spec.select_value(row)
        entry[1] = entry[1] + selected


def _raise_selector_error(selector: str | int, row: Any, error: Exception) -> None:
    """Translate exact-dict lookup failures exactly like ``compile_selector``."""
    if type(selector) is int:
        if isinstance(error, AttributeError):
            raise error
        raise SelectionError(
            f"Could not resolve index selector {selector!r} on {type(row).__name__}"
        ) from error
    if isinstance(error, IndexError):
        raise error
    raise SelectionError(
        f"Could not resolve selector {selector!r}; failed at {selector!r}"
    ) from error


def _arrow_group_table(pa: Any, descriptor: ArrowBatchSource) -> Any | None:
    """Open one reusable columnar aggregate input without adapter batch slicing."""
    retained = descriptor.materialized_data
    if descriptor.kind == "table" and isinstance(retained, pa.Table):
        return retained
    if descriptor.kind == "record_batch" and isinstance(retained, pa.RecordBatch):
        try:
            return pa.Table.from_batches([retained], schema=descriptor.schema_hint)
        except (TypeError, ValueError):
            return None
    if descriptor.columnar_opener is not None:
        table = descriptor.columnar_opener()
        return table if isinstance(table, pa.Table) else None

    batches = descriptor.open_batches()
    try:
        materialized_batches = list(batches)
    finally:
        close = getattr(batches, "close", None)
        if callable(close):
            with suppress(Exception):
                close()
    try:
        return pa.Table.from_batches(materialized_batches, schema=descriptor.schema_hint)
    except (TypeError, ValueError):
        return None


def _try_exact_global_count(node: GlobalAggregatePhysicalNode) -> dict[str, Any] | None:
    """Return one trusted direct source size while preserving instrumented execution."""
    from ..runtime.failpoints import has_active_failpoints

    name = node.exact_count_name
    if (
        name is None
        or has_active_failpoints()
        or not isinstance(node.input, SourcePhysicalNode)
        or not node.input.source.capabilities.reiterable
    ):
        return None
    size = node.input.source.capabilities.exact_size
    return None if size is None else {name: size}


def _try_arrow_global_count(node: GlobalAggregatePhysicalNode) -> dict[str, Any] | None:
    """Count one direct replayable Arrow source through its closed source terminal."""
    from ..runtime.failpoints import has_active_failpoints

    name = node.arrow_count_name
    if (
        name is None
        or has_active_failpoints()
        or not isinstance(node.input, SourcePhysicalNode)
        or not node.input.source.capabilities.reiterable
    ):
        return None
    native_data = node.input.source.native_data
    if not isinstance(native_data, ArrowBatchSource) or native_data.count_opener is None:
        return None
    descriptor = node.input.source.open_native(ArrowBatchSource)
    count = descriptor.count_opener()
    return None if count is None else {name: count}


def _arrow_i64_sum(values: Any, row_count: int, pa: Any, pc: Any) -> int:
    """Return an exact Python integer sum for one nonempty int64 Arrow array."""
    if values.null_count:
        raise TypeError("unsupported operand type(s) for +: 'int' and 'NoneType'")
    bounds = pc.min_max(values).as_py()
    maximum_absolute = max(abs(bounds["min"]), abs(bounds["max"]))
    if maximum_absolute * row_count > 2**63 - 1:
        values = pc.cast(values, pa.decimal128(38, 0))
    subtotal = pc.sum(values).as_py()
    return 0 if subtotal is None else int(subtotal)


def _arrow_extreme(current: Any, values: Any, kind: str, pc: Any) -> Any:
    """Fold one Arrow array while retaining Python's ordered null comparisons."""
    candidates = (
        values.to_pylist()
        if values.null_count or len(values) <= _ARROW_PYTHON_EXTREME_MAX_ROWS
        else (pc.min_max(values).as_py()[kind],)
    )
    for candidate in candidates:
        if (
            current is _MISSING
            or (kind == "min" and candidate < current)
            or (kind == "max" and candidate > current)
        ):
            current = candidate
    return current


def _arrow_batches_first(batches: Iterator[Any], field_index: int) -> Any:
    """Return the first selected scalar, stopping before any later batch pull."""
    for batch in batches:
        if batch.num_rows:
            return batch.column(field_index)[0].as_py()
    return None


def _arrow_batches_last(batches: Iterator[Any], field_index: int) -> Any:
    """Return the final selected scalar after exhausting all batches."""
    result: Any = _MISSING
    for batch in batches:
        if batch.num_rows:
            result = batch.column(field_index)[batch.num_rows - 1].as_py()
    return None if result is _MISSING else result


def _arrow_batches_extreme(batches: Iterator[Any], field_index: int, kind: str, pc: Any) -> Any:
    """Reduce selected batch columns to one ordered Python-compatible extreme."""
    result: Any = _MISSING
    for batch in batches:
        if batch.num_rows:
            result = _arrow_extreme(result, batch.column(field_index), kind, pc)
    return None if result is _MISSING else result


def _arrow_batches_sum(batches: Iterator[Any], field_index: int, pa: Any, pc: Any) -> int:
    """Sum selected batch columns to an unbounded Python integer."""
    total = 0
    for batch in batches:
        row_count = int(batch.num_rows)
        if row_count:
            total += _arrow_i64_sum(batch.column(field_index), row_count, pa, pc)
    return total


def _try_arrow_reader_global_reduction(
    node: GlobalAggregatePhysicalNode,
    descriptor: ArrowBatchSource,
    pa: Any,
    pc: Any,
) -> dict[str, Any] | None:
    """Consume one claimed Arrow reader without constructing row dictionaries."""
    spec = node.arrow_i64_sum
    if spec is None or not isinstance(node.input, SourcePhysicalNode):
        return None
    schema = descriptor.schema_hint
    if schema is None:
        return None
    names = schema.names
    if spec.value_field not in names:
        return None
    field_index = names.index(spec.value_field)
    if not pa.types.is_int64(schema.field(field_index).type):
        return None

    node.input.source.open_native(ArrowBatchSource)
    batches = descriptor.open_batches()
    try:
        match spec.kind:
            case "first":
                result = _arrow_batches_first(batches, field_index)
            case "last":
                result = _arrow_batches_last(batches, field_index)
            case "min" | "max":
                result = _arrow_batches_extreme(batches, field_index, spec.kind, pc)
            case "sum":
                result = _arrow_batches_sum(batches, field_index, pa, pc)
    finally:
        close = getattr(batches, "close", None)
        if callable(close):
            with suppress(Exception):
                close()
    return {spec.output_name: result}


def _reduce_arrow_table(
    table: Any,
    spec: ArrowGlobalSumSpec,
    pa: Any,
    pc: Any,
) -> dict[str, Any] | None:
    """Reduce a retained Arrow table after its source-level guards have passed."""
    if table.num_rows == 0:
        identity = 0 if spec.kind == "sum" else None
        return {spec.output_name: identity}
    names = table.schema.names
    if names.count(spec.value_field) != 1:
        return None
    values = table.column(names.index(spec.value_field))
    if not pa.types.is_int64(values.type):
        return None

    match spec.kind:
        case "first":
            result = values[0].as_py()
        case "last":
            result = values[table.num_rows - 1].as_py()
        case "min" | "max":
            if values.null_count:
                result = _arrow_extreme(_MISSING, values, spec.kind, pc)
            else:
                try:
                    result = _arrow_extreme(_MISSING, values, spec.kind, pc)
                except (ArithmeticError, NotImplementedError, TypeError, ValueError):
                    return None
        case "sum":
            if values.null_count:
                return None
            try:
                result = _arrow_i64_sum(values, table.num_rows, pa, pc)
            except (ArithmeticError, NotImplementedError, TypeError, ValueError):
                return None
    return {spec.output_name: result}


def _try_arrow_global_reduction(
    node: GlobalAggregatePhysicalNode,
) -> dict[str, Any] | None:
    """Reduce one proven Arrow int64 field without materializing its input rows."""
    from ..runtime.failpoints import has_active_failpoints

    spec = node.arrow_i64_sum
    if spec is None or has_active_failpoints() or not isinstance(node.input, SourcePhysicalNode):
        return None
    native_data = node.input.source.native_data
    if not isinstance(native_data, ArrowBatchSource) or native_data.kind not in {
        "table",
        "record_batch",
        "reader",
        "dataframe",
        "polars",
    }:
        return None

    pa = import_module("pyarrow")
    pc = import_module("pyarrow.compute")
    if native_data.kind == "reader":
        return _try_arrow_reader_global_reduction(node, native_data, pa, pc)
    if not node.input.source.capabilities.reiterable:
        return None
    descriptor = node.input.source.open_native(ArrowBatchSource)
    table = _arrow_group_table(pa, descriptor)
    if table is None:
        return None
    return _reduce_arrow_table(table, spec, pa, pc)


def _is_supported_arrow_group_key_type(types: Any, key_type: Any) -> bool:
    """Return whether Arrow keys have the same scalar equality as Python grouping."""
    logical_key_type = key_type.value_type if types.is_dictionary(key_type) else key_type
    return bool(
        types.is_null(logical_key_type)
        or types.is_boolean(logical_key_type)
        or types.is_integer(logical_key_type)
        or types.is_string(logical_key_type)
        or types.is_large_string(logical_key_type)
        or types.is_binary(logical_key_type)
        or types.is_large_binary(logical_key_type)
    )


def _prepare_arrow_group_keys(
    key_values: Any,
    types: Any,
    pc: Any,
) -> tuple[Any, list[Any]] | None:
    """Canonicalize dictionary keys and retain the logical first-seen key order."""
    if not types.is_dictionary(key_values.type):
        return key_values, pc.unique(key_values).to_pylist()
    key_values = key_values.unify_dictionaries()
    if key_values.num_chunks == 0:
        return None
    dictionary = key_values.chunk(0).dictionary
    if dictionary.null_count or pc.count_distinct(dictionary, mode="all").as_py() != len(
        dictionary
    ):
        # Arrow groups dictionary indices. Duplicate or null dictionary values can
        # therefore split values that Python regards as one logical key.
        return None
    return key_values, pc.unique(key_values).to_pylist()


def _merge_arrow_group_totals(
    groups: dict[Any, int],
    keys: list[Any],
    totals: list[Any],
) -> None:
    """Merge one complete batch result into insertion-ordered Python integer state."""
    for key, subtotal in zip(keys, totals, strict=True):
        value = int(subtotal)
        if key in groups:
            groups[key] += value
        else:
            groups[key] = 0 + value


def _consume_arrow_group_batch_as_scalars(
    groups: dict[Any, int],
    key_values: Any,
    sum_values: Any,
) -> None:
    """Continue a claimed reader through canonical scalar selection and addition."""
    keys = key_values.to_pylist()
    values = sum_values.to_pylist()
    for key, selected in zip(keys, values, strict=True):
        if key in groups:
            groups[key] = groups[key] + selected
        else:
            groups[key] = 0 + selected


def _arrow_group_batch_totals(
    key_values: Any,
    sum_values: Any,
    row_count: int,
    pa: Any,
    pc: Any,
) -> tuple[list[Any], list[Any]]:
    """Compute one all-or-nothing stable batch partial for incremental reader merge."""
    # Arrow's unique kernel explicitly preserves original order.  Keep that first-seen
    # oracle separate: a single-thread group is stable but can still expose hash order.
    encounter_order = pc.unique(key_values).to_pylist()
    bounds = pc.min_max(sum_values).as_py()
    maximum_absolute = max(abs(bounds["min"]), abs(bounds["max"]))
    wide_totals = maximum_absolute * row_count > 2**63 - 1
    if wide_totals:
        sum_values = pc.cast(sum_values, pa.decimal128(38, 0))
    grouped = (
        pa.table(
            {
                "__fpstreams_group_key": key_values,
                "__fpstreams_group_value": sum_values,
            }
        )
        .group_by("__fpstreams_group_key", use_threads=False)
        .aggregate([("__fpstreams_group_value", "sum")])
    )
    totals_by_key = dict(
        zip(grouped.column(0).to_pylist(), grouped.column(1).to_pylist(), strict=True)
    )
    return encounter_order, [totals_by_key[key] for key in encounter_order]


def _consume_arrow_group_batches(
    batches: Iterator[Any],
    *,
    key_index: int,
    value_index: int,
    pa: Any,
    pc: Any,
    groups: dict[Any, int] | None = None,
) -> dict[Any, int]:
    """Merge stable Arrow partials, switching a claimed stream to scalar folding on decline."""
    if groups is None:
        groups = {}
    use_arrow = True
    for batch in batches:
        row_count = int(batch.num_rows)
        if row_count == 0:
            continue
        key_values = batch.column(key_index)
        sum_values = batch.column(value_index)
        if (
            row_count <= _ARROW_GROUP_BATCH_SCALAR_MAX_ROWS
            or sum_values.null_count
            or not use_arrow
        ):
            _consume_arrow_group_batch_as_scalars(groups, key_values, sum_values)
            continue
        try:
            keys, totals = _arrow_group_batch_totals(
                key_values,
                sum_values,
                row_count,
                pa,
                pc,
            )
        except (ArithmeticError, NotImplementedError, TypeError, ValueError):
            use_arrow = False
            _consume_arrow_group_batch_as_scalars(groups, key_values, sum_values)
        else:
            _merge_arrow_group_totals(groups, keys, totals)
    return groups


def _consume_arrow_file_group_batches(
    first: Any,
    batches: Iterator[Any],
    *,
    key_index: int,
    value_index: int,
    pa: Any,
    pc: Any,
) -> dict[Any, int]:
    """Fold a measured small-file prefix scalarly, then switch the same stream to Arrow."""
    groups: dict[Any, int] = {}
    scalar_rows_left = _ARROW_FILE_GROUP_MIN_ROWS
    current = first
    while True:
        row_count = int(current.num_rows)
        scalar_count = min(row_count, scalar_rows_left)
        if scalar_count:
            _consume_arrow_group_batch_as_scalars(
                groups,
                current.column(key_index).slice(0, scalar_count),
                current.column(value_index).slice(0, scalar_count),
            )
            scalar_rows_left -= scalar_count
        if scalar_count < row_count:
            remainder = current.slice(scalar_count)
            return _consume_arrow_group_batches(
                chain((remainder,), batches),
                key_index=key_index,
                value_index=value_index,
                pa=pa,
                pc=pc,
                groups=groups,
            )
        try:
            current = next(batches)
        except StopIteration:
            return groups
        if current.num_rows == 0:
            continue


def _try_arrow_reader_group_sum(
    node: GroupAggregatePhysicalNode,
    descriptor: ArrowBatchSource,
    pa: Any,
    pc: Any,
) -> list[dict[str, Any]] | None:
    """Incrementally aggregate one proven one-shot reader without row dictionaries."""
    spec = node.arrow_i64_sum
    if (
        spec is None
        or not isinstance(node.input, SourcePhysicalNode)
        or descriptor.kind != "reader"
        or descriptor.reiterable
        or node.input.source.capabilities.reiterable
    ):
        return None
    schema = descriptor.schema_hint
    if schema is None:
        return None
    names = tuple(schema.names)
    if names.count(spec.key_field) != 1 or names.count(spec.value_field) != 1:
        return None
    key_index = names.index(spec.key_field)
    value_index = names.index(spec.value_field)
    key_type = schema.field(key_index).type
    types = pa.types
    if types.is_dictionary(key_type) or not _is_supported_arrow_group_key_type(types, key_type):
        return None
    if not types.is_int64(schema.field(value_index).type):
        return None

    # Every rejection above is deliberately pre-claim.  From this point onward the reader
    # cannot be reopened, so recoverable compute declines continue from the current batch.
    node.input.source.open_native(ArrowBatchSource)
    batches = descriptor.open_batches()
    try:
        groups = _consume_arrow_group_batches(
            batches,
            key_index=key_index,
            value_index=value_index,
            pa=pa,
            pc=pc,
        )
    finally:
        close = getattr(batches, "close", None)
        if callable(close):
            with suppress(Exception):
                close()

    key_name = node.key_names[0]
    return [{key_name: key, spec.output_name: total} for key, total in groups.items()]


def _try_arrow_file_group_sum(
    node: GroupAggregatePhysicalNode,
    descriptor: ArrowBatchSource,
    pa: Any,
    pc: Any,
) -> list[dict[str, Any]] | None:
    """Incrementally aggregate direct CSV/Parquet fields without retaining a whole table."""
    spec = node.arrow_i64_sum
    if (
        spec is None
        or not isinstance(node.input, SourcePhysicalNode)
        or descriptor.kind not in {"csv", "parquet"}
        or not descriptor.reiterable
        or not node.input.source.capabilities.reiterable
    ):
        return None

    node.input.source.open_native(ArrowBatchSource)
    columns = tuple(dict.fromkeys((spec.key_field, spec.value_field)))
    # Default CSV projection is admitted only with large-file evidence before this function.
    # Custom parse/callback sources have no projection opener and retain their one base stream.
    use_projection = descriptor.kind == "parquet" or descriptor.projection_opener is not None
    batches = descriptor.open_batches(columns=columns if use_projection else None)

    def canonical_from(first: Any) -> list[dict[str, Any]]:
        """Resume the exact row grouping loop from an already-opened file batch stream."""
        simple_sum = node.simple_sum
        if simple_sum is None:
            raise RuntimeError("Arrow file group sum is missing its canonical fallback")

        def rows() -> Iterator[dict[str, Any]]:
            yield from first.to_pylist()
            for later in batches:
                yield from later.to_pylist()

        return list(_execute_simple_group_sum(rows(), node, simple_sum))

    try:
        for first in batches:
            if first.num_rows == 0:
                continue
            names = tuple(first.schema.names)
            if names.count(spec.key_field) != 1 or names.count(spec.value_field) != 1:
                return canonical_from(first)
            key_index = names.index(spec.key_field)
            value_index = names.index(spec.value_field)
            types = pa.types
            key_type = first.schema.field(key_index).type
            if types.is_dictionary(key_type) or not _is_supported_arrow_group_key_type(
                types, key_type
            ):
                return canonical_from(first)
            if not types.is_int64(first.schema.field(value_index).type):
                return canonical_from(first)
            groups = _consume_arrow_file_group_batches(
                first,
                batches,
                key_index=key_index,
                value_index=value_index,
                pa=pa,
                pc=pc,
            )
            key_name = node.key_names[0]
            return [{key_name: key, spec.output_name: total} for key, total in groups.items()]
        return []
    finally:
        close = getattr(batches, "close", None)
        if callable(close):
            with suppress(Exception):
                close()


def _try_arrow_group_sum(  # noqa: C901 - guarded source/backend dispatch
    node: GroupAggregatePhysicalNode,
) -> list[dict[str, Any]] | None:
    """Aggregate a proven direct Arrow i64 field sum without boxing its input rows."""
    from ..runtime.failpoints import has_active_failpoints

    spec = node.arrow_i64_sum
    if spec is None or has_active_failpoints() or not isinstance(node.input, SourcePhysicalNode):
        return None
    native_data = node.input.source.native_data
    if not isinstance(native_data, ArrowBatchSource):
        return None

    if native_data.kind == "csv" and native_data.projection_opener is not None:
        size_opener = native_data.byte_size_opener
        if size_opener is None:
            return None
        size_bytes = size_opener()
        if type(size_bytes) is not int or size_bytes < _ARROW_CSV_GROUP_MIN_BYTES:
            return None

    pa = import_module("pyarrow")
    pc = import_module("pyarrow.compute")
    if native_data.kind == "reader":
        return _try_arrow_reader_group_sum(node, native_data, pa, pc)
    if native_data.kind in {"csv", "parquet"}:
        return _try_arrow_file_group_sum(node, native_data, pa, pc)
    if not node.input.source.capabilities.reiterable or native_data.kind not in {
        "table",
        "record_batch",
        "dataframe",
        "polars",
    }:
        return None
    descriptor = node.input.source.open_native(ArrowBatchSource)
    table = _arrow_group_table(pa, descriptor)
    if table is None:
        return None

    # Empty row execution never evaluates selectors, even if the retained schema does
    # not contain them. Preserve that timing before consulting field metadata.
    if table.num_rows == 0:
        return []
    names = table.schema.names
    if names.count(spec.key_field) != 1 or names.count(spec.value_field) != 1:
        return None
    key_values = table.column(names.index(spec.key_field))
    sum_values = table.column(names.index(spec.value_field))
    types = pa.types
    if not _is_supported_arrow_group_key_type(types, key_values.type):
        return None
    if not types.is_int64(sum_values.type) or sum_values.null_count:
        return None

    try:
        prepared_keys = _prepare_arrow_group_keys(key_values, types, pc)
        if prepared_keys is None:
            return None
        key_values, encounter_order = prepared_keys
        bounds = pc.min_max(sum_values).as_py()
        maximum_absolute = max(abs(bounds["min"]), abs(bounds["max"]))
        wide_totals = maximum_absolute * table.num_rows > 2**63 - 1
        if wide_totals:
            sum_values = pc.cast(sum_values, pa.decimal128(38, 0))
        compact = pa.table(
            {
                "__fpstreams_group_key": key_values,
                "__fpstreams_group_value": sum_values,
            }
        )
        grouped = compact.group_by("__fpstreams_group_key", use_threads=False).aggregate(
            [("__fpstreams_group_value", "sum")]
        )
    except (ArithmeticError, NotImplementedError, TypeError, ValueError):
        return None

    keys = grouped.column(0).to_pylist()
    totals = grouped.column(1).to_pylist()
    totals_by_key = dict(zip(keys, totals, strict=True))
    keys = encounter_order
    totals = [totals_by_key[key] for key in keys]
    key_name = node.key_names[0]
    if not wide_totals:
        return [
            {key_name: key, spec.output_name: total}
            for key, total in zip(keys, totals, strict=True)
        ]
    result: list[dict[str, Any]] = []
    for key, total in zip(keys, totals, strict=True):
        row = {key_name: key}
        row[spec.output_name] = int(total)
        result.append(row)
    return result


def _try_native_tuple_group_sum(
    native_module: Any,
    source: object,
    spec: NativeGroupSumSpec,
    key_name: str,
) -> list[dict[str, Any]] | None:
    """Dispatch one exact-tuple group scan across new and old native wheels."""
    rows_kernel = getattr(native_module, "group_sum_i64_rows_v1", None)
    if callable(rows_kernel):
        try:
            rows_result = rows_kernel(
                source,
                spec.key_index,
                spec.value_index,
                key_name,
                spec.output_name,
            )
        except (OverflowError, TypeError):
            return None
        # A v1 rejection already consumed its one speculative scan. Re-enter canonical
        # Python execution directly instead of retrying the older pair kernel.
        if rows_result is None:
            return None
        is_final_rows, payload = rows_result
        if is_final_rows:
            return cast(list[dict[str, Any]], payload)
        tuple_groups = cast(list[tuple[Any, int]], payload)
    else:
        # Older optional wheels expose only the pair ABI. Its None sentinel remains a clean
        # request for canonical Python fallback because the source has not been opened.
        try:
            tuple_groups = native_module.group_sum_i64_pairs(
                source,
                spec.key_index,
                spec.value_index,
            )
        except (OverflowError, TypeError):
            return None
    if tuple_groups is None:
        return None
    return [{key_name: key, spec.output_name: total} for key, total in tuple_groups]


def _try_native_record_group_sum(
    native_module: Any,
    source: object,
    spec: NativeRecordGroupSumSpec,
    key_name: str,
) -> list[dict[str, Any]] | None:
    """Dispatch one exact-record group scan across new and old native wheels."""
    rows_kernel = getattr(native_module, "group_sum_i64_dict_rows_v1", None)
    if callable(rows_kernel):
        rows_result = rows_kernel(
            source,
            spec.key_field,
            spec.value_field,
            key_name,
            spec.output_name,
        )
        # A v1 rejection already consumed its one speculative scan. Re-enter canonical
        # Python execution directly instead of retrying the older pair kernel.
        if rows_result is None:
            return None
        is_final_rows, payload = rows_result
        if is_final_rows:
            return cast(list[dict[str, Any]], payload)
        record_groups = cast(list[tuple[Any, int]], payload)
    else:
        record_kernel = getattr(native_module, "group_sum_i64_dict_rows", None)
        if not callable(record_kernel):
            return None
        record_groups = record_kernel(
            source,
            spec.key_field,
            spec.value_field,
        )
    if record_groups is None:
        return None
    return [{key_name: key, spec.output_name: total} for key, total in record_groups]


def _try_native_fixed_group(
    native_module: Any,
    source: object,
    spec: NativeFixedI64GroupSpec,
    key_name: str,
) -> list[dict[str, Any]] | None:
    """Dispatch one exact count/count-sum shape through its optional fixed ABI."""
    kernel_name = (
        "group_fixed_i64_rows_v1" if spec.row_kind == "tuple" else "group_fixed_i64_dict_rows_v1"
    )
    kernel = getattr(native_module, kernel_name, None)
    if not callable(kernel):
        return None
    try:
        result = kernel(
            source,
            spec.key_selector,
            spec.value_selector,
            key_name,
            spec.count_name,
            spec.sum_name,
        )
    except (OverflowError, TypeError):
        # Exact Python integer selectors are unbounded while the tuple ABI accepts isize.
        # Narrowing can therefore fail before the speculative row scan has opened the source.
        return None
    if result is None:
        return None
    is_final_rows, payload = result
    if is_final_rows:
        return cast(list[dict[str, Any]], payload)
    if spec.sum_name is None:
        count_groups = cast(list[tuple[Any, int]], payload)
        return [{key_name: key, spec.count_name: count} for key, count in count_groups]
    count_sum_groups = cast(list[tuple[Any, int, int]], payload)
    return [
        {
            key_name: key,
            spec.count_name: count,
            spec.sum_name: total,
        }
        for key, count, total in count_sum_groups
    ]


def _try_native_callable_group(
    native_module: Any,
    source: object,
    node: GroupAggregatePhysicalNode,
    spec: NativeCallableGroupSpec,
    key_name: str,
) -> list[dict[str, Any]] | None:
    """Run one no-replay exact-record group with its opaque callback kept intact."""
    if (
        not _CANONICAL_BUILTIN_HASH
        or _RELATIONAL_GLOBALS.get("hash", _BUILTIN_HASH) is not _BUILTIN_HASH
        or _BUILTINS_DICT.get("hash") is not _BUILTIN_HASH
    ):
        # The canonical Python loop resolves ``hash`` dynamically. Decline before the
        # first callback when either LOAD_GLOBAL tier has been replaced.
        return None
    closed = node.closed_group
    if closed is None or len(closed.lanes) != 2:
        return None
    value_accessor = closed.lanes[1].select_value
    if value_accessor is None:
        return None
    if spec.callback_side == "key":
        kernel = getattr(native_module, "group_count_sum_callable_key_dict_rows_v1", None)
        if not callable(kernel):
            return None
        payload = kernel(
            source,
            node.keys[0],
            spec.direct_field,
            value_accessor,
        )
    else:
        kernel = getattr(native_module, "group_count_sum_callable_value_dict_rows_v1", None)
        if not callable(kernel):
            return None
        payload = kernel(
            source,
            spec.direct_field,
            node.keys[0],
            value_accessor,
        )
    if payload is None:
        return None
    groups = cast(list[tuple[Any, int, Any]], payload)
    return [
        {
            key_name: key,
            spec.count_name: count,
            spec.sum_name: total,
        }
        for key, count, total in groups
    ]


def _try_native_group_sum(
    node: GroupAggregatePhysicalNode,
) -> list[dict[str, Any]] | None:
    """Run a guarded source-container scan or leave the source unopened for fallback."""
    from ..runtime.failpoints import has_active_failpoints

    # Native kernels deliberately skip Python transition hooks. Instrumented
    # runs must therefore enter the ordinary source/open execution path.
    if has_active_failpoints():
        return None
    fixed_spec = node.native_fixed_i64_group
    callable_spec = node.native_callable_group
    tuple_spec = node.native_i64_sum
    record_spec = node.native_record_i64_sum
    if (
        fixed_spec is None and callable_spec is None and tuple_spec is None and record_spec is None
    ) or not isinstance(node.input, SourcePhysicalNode):
        return None
    source = node.input.source.native_data
    exact_container = type(source) is list or type(source) is tuple
    fixed_eligible = fixed_spec is not None and exact_container
    callable_eligible = callable_spec is not None and exact_container
    tuple_eligible = tuple_spec is not None and exact_container
    record_eligible = record_spec is not None and exact_container
    fixed_record_eligible = (
        fixed_eligible and fixed_spec is not None and fixed_spec.row_kind == "dict"
    )
    if not tuple_eligible and not record_eligible and not fixed_eligible and not callable_eligible:
        return None

    try:
        from .. import _native
    except ImportError:
        # The Rust extension is optional. Source data has not been opened yet,
        # so declining this speculative fast path preserves a clean Python run.
        return None

    key_name = node.key_names[0]
    if callable_eligible and callable_spec is not None:
        return _try_native_callable_group(_native, source, node, callable_spec, key_name)
    if fixed_eligible and fixed_spec is not None and fixed_spec.row_kind == "tuple":
        return _try_native_fixed_group(_native, source, fixed_spec, key_name)
    if tuple_eligible:
        assert tuple_spec is not None
        return _try_native_tuple_group_sum(_native, source, tuple_spec, key_name)

    if not record_eligible and not fixed_record_eligible:
        return None
    max_fields = getattr(_native, "record_group_sum_max_fields", None)
    if type(max_fields) is not int or max_fields < 0:
        return None
    try:
        first_record = source[0]
    except IndexError:
        # Empty exact containers remain a valid zero-group native result. A concurrent
        # list append after this point is still guarded row-by-row inside the ABI.
        pass
    else:
        # Exact container indexing takes a strong reference on free-threaded CPython.
        # Reject subclasses before len() so speculative preflight cannot call user code.
        if type(first_record) is not dict or len(first_record) > max_fields:
            return None
    if fixed_record_eligible:
        assert fixed_spec is not None
        return _try_native_fixed_group(_native, source, fixed_spec, key_name)
    assert record_spec is not None
    return _try_native_record_group_sum(_native, source, record_spec, key_name)
