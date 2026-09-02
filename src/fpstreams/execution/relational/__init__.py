"""Execution of recursively compiled record relation trees."""

from __future__ import annotations

import builtins as _builtins
import signal as _signal
import sys as _sys
from abc import get_cache_token
from collections import namedtuple as _namedtuple
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from importlib import import_module
from itertools import chain
from types import BuiltinFunctionType, CodeType, FunctionType, MappingProxyType
from typing import Any, cast

from ...collecting._collector_base import Collector, _never_done
from ...collecting.aggregation import (
    _MISSING,
    AggregationItems,
    native_group_aggregation,
    project_count_aggregation,
)
from ...collecting.program import (
    CollectorState,
    collector_program_fast_path_is_live,
    run_collector_program,
)
from ...errors import SelectionError
from ...expressions.selectors import _direct_field, compile_selector
from ...physical.plan import (
    CompiledExpressionPhysicalNode,
    PhysicalNode,
    PhysicalPlan,
    RowPhysicalNode,
    SortPhysicalNode,
)
from ...physical.relational import (
    ArrowGlobalAggregateSpec,
    ArrowGlobalSumSpec,
    ArrowGroupAggregateSpec,
    CompositeCountSumSpec,
    GlobalAggregatePhysicalNode,
    GroupAggregatePhysicalNode,
    JoinPhysicalNode,
    NativeFixedI64GroupSpec,
    NativeGlobalI64AggregateSpec,
    NativeGroupSumSpec,
    NativeMultiI64GroupSpec,
    NativePairI64ExprGroupSumSpec,
    NativeRecordGroupSumSpec,
    PhysicalRelNode,
    PipelinePhysicalNode,
    SimpleGroupSumSpec,
    SourcePhysicalNode,
)
from ...planning.arrow_source import ArrowBatchSource
from ...planning.logical import LogicalPlan, Pipeline, SourceNode
from ...planning.pair_i64_expression import (
    lower_pair_i64_group_key,
    lower_pair_i64_group_value,
)
from ...planning.source import (
    _CANONICAL_RETAINED_SEQUENCE,
    _CANONICAL_SOURCE_CLAIM,
    _CANONICAL_SOURCE_CLAIM_CODE,
    _CANONICAL_SOURCE_NATIVE_DATA,
    _CANONICAL_SOURCE_OPEN,
    _CANONICAL_SOURCE_OPEN_CODE,
    Source,
)
from ...runtime.failpoints import has_active_failpoints as _has_active_failpoints
from ...runtime.query import QueryRuntime
from ...tabular import records as _records
from ...tabular.join import _direct_mapping_mro
from ...tabular.records import _as_record
from ...tabular.spill import require_spill_file_budget, spilled_group_aggregate
from ...tabular.spill_limits import SpillLimits
from .. import execute
from ..numpy_group import _failpoint_boundaries_are_live as _numpy_failpoints_are_live
from ..numpy_group import try_numpy_global_aggregate as _numpy_global_aggregate
from ..numpy_group import try_numpy_group_aggregate as _numpy_group_aggregate
from ..sync_ops import close_iterators
from .arrow_global import (
    ArrowGlobalHooks,
    _arrow_group_table,
    _observe_arrow_batch_rows,
    _reduce_arrow_table_impl,
    _try_arrow_global_count_impl,
    _try_arrow_global_reduction_impl,
)
from .arrow_group import (
    ArrowGroupHooks,
    _try_arrow_file_group_sum_impl,
    _try_arrow_group_sum_impl,
    _try_arrow_retained_group_aggregate_impl,
)
from .arrow_group import _arrow_group_batch_totals as _arrow_group_batch_totals_impl
from .arrow_group_rows import (
    _arrow_group_lane_arrays,
    _arrow_group_lane_lists,
    _ordered_arrow_group_rows,
)
from .arrow_group_rows import _materialize_arrow_group_rows as _materialize_arrow_group_rows_impl
from .join import _execute_join, _try_retained_arrow_unique_join

_RECORD_JOIN_V1_MAX_FIELDS = 64
_ARROW_GROUP_TABLE_MIN_GROUPS = 4_096
_ARROW_GROUP_TAKE_MIN_GROUPS = 131_072
_ARROW_CSV_GROUP_MIN_BYTES = 32 * 1_024
_ARROW_FILE_GROUP_MIN_ROWS = 1_024
_ARROW_GROUP_BATCH_SCALAR_MAX_ROWS = 128
# Below this measured crossover, claiming the reader and folding canonical rows is cheaper.
_ARROW_READER_GROUP_MULTI_MIN_ROWS = 384
_ARROW_READER_GROUP_CARDINALITY_SAMPLE_ROWS = 384
_ARROW_READER_GROUP_MAX_DISTINCT_RATIO = 0.5
_NUMPY_GLOBAL_CHUNK_ROWS = 65_536
_NUMPY_GROUP_CHUNK_ROWS = 65_536
_SYS_MAXSIZE = _sys.maxsize
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
_BUILTIN_ATTRIBUTE_ERROR = AttributeError
_BUILTIN_ANY = any
_BUILTIN_CALLABLE = callable
_BUILTIN_DICT = dict
_BUILTIN_GETATTR = getattr
_BUILTIN_INT = int
_BUILTIN_ISINSTANCE = isinstance
_BUILTIN_KEY_ERROR = KeyError
_BUILTIN_LIST = list
_BUILTIN_TUPLE: Any = tuple
_BUILTIN_TYPE = type
_BUILTIN_TYPE_ERROR = TypeError
_CANONICAL_MAPPING = Mapping
_CANONICAL_SELECTION_ERROR = SelectionError
_CANONICAL_GROUP_FIELD_SELECTOR_CODE = compile_selector("__fpstreams_group_field_probe__").__code__
_SYS_IS_GIL_ENABLED = getattr(_sys, "_is_gil_enabled", None)
_SIGNAL_GETITIMER = getattr(_signal, "getitimer", None)
_SIGNAL_SETITIMER = getattr(_signal, "setitimer", None)
_SIGNAL_INTERVAL_TIMERS = tuple(
    (name, getattr(_signal, name))
    for name in ("ITIMER_REAL", "ITIMER_VIRTUAL", "ITIMER_PROF")
    if hasattr(_signal, name)
)
_CallableJoinRecordCapability = tuple[
    type[Any],
    tuple[type[Any], ...] | None,
]


def _group_hash_replaced() -> bool:
    """Return whether canonical group execution would observe a replaced hash()."""
    return (
        not _CANONICAL_BUILTIN_HASH
        or _RELATIONAL_GLOBALS.get("hash", _BUILTIN_HASH) is not _BUILTIN_HASH
        or _BUILTINS_DICT.get("hash") is not _BUILTIN_HASH
    )


def _exact_builtin_group_key_type(key_type: type[Any]) -> bool:
    """Return whether a dict lookup's implicit hash is the only observable hash call."""
    return key_type in (int, str, bytes, bool, float, complex, _NONE_TYPE)


def _retained_aggregations_are_live(aggregations: tuple[Any, ...]) -> bool:
    """Revalidate project-owned collector lifecycles once before a closed execution."""
    for aggregation in aggregations:
        if project_count_aggregation(aggregation):
            continue
        if native_group_aggregation(aggregation) is None:
            return False
    return True


def _group_aggregations_are_live(node: GroupAggregatePhysicalNode) -> bool:
    """Revalidate a grouped plan only when it retains a closed aggregation marker."""
    has_closed_marker = _BUILTIN_ANY(
        marker is not None
        for marker in (
            node.spill_count,
            node.simple_sum,
            node.closed_group,
            node.composite_count_sum,
            node.native_fixed_i64_group,
            node.arrow_i64_sum,
            node.numpy_group,
            node.native_pair_i64_expr_sum,
            node.native_i64_sum,
            node.native_record_i64_sum,
        )
    )
    if not has_closed_marker:
        return True
    program = node.aggregations.collectors
    return collector_program_fast_path_is_live(program) and _retained_aggregations_are_live(
        program.layout.collectors
    )


def _global_aggregations_are_live(node: GlobalAggregatePhysicalNode) -> bool:
    """Revalidate collectors before any retained global shortcut can consume input."""
    has_closed_marker = _BUILTIN_ANY(
        marker is not None
        for marker in (
            node.exact_count_name,
            node.arrow_count_name,
            node.arrow_i64_sum,
            node.numpy_global,
            node.native_multi_i64,
            node.native_record_i64_sum,
        )
    )
    if not has_closed_marker:
        return True
    program = node.aggregations.collectors
    return collector_program_fast_path_is_live(program) and _retained_aggregations_are_live(
        program.layout.collectors
    )


def _without_group_aggregation_specializations(
    node: GroupAggregatePhysicalNode,
) -> GroupAggregatePhysicalNode:
    """Retain compiled keys and collectors while removing stale aggregation shortcuts."""
    return replace(
        node,
        spill_count=None,
        simple_sum=None,
        closed_group=None,
        composite_count_sum=None,
        native_fixed_i64_group=None,
        arrow_i64_sum=None,
        numpy_group=None,
        native_pair_i64_expr_sum=None,
        native_i64_sum=None,
        native_record_i64_sum=None,
    )


def _revalidated_group_node(
    node: GroupAggregatePhysicalNode,
) -> GroupAggregatePhysicalNode:
    """Return the original node or its generic-collector fallback after one guard."""
    if _group_aggregations_are_live(node):
        return node
    return _without_group_aggregation_specializations(node)


def _native_multi_group_timer_active() -> bool:
    """Conservatively detect interval timers that can mutate an open exact list."""
    if (
        _BUILTIN_GETATTR(_signal, "getitimer", None) is not _SIGNAL_GETITIMER
        or _BUILTIN_GETATTR(_signal, "setitimer", None) is not _SIGNAL_SETITIMER
        or _BUILTIN_ANY(
            _BUILTIN_GETATTR(_signal, name, None) != timer
            for name, timer in _SIGNAL_INTERVAL_TIMERS
        )
    ):
        return True
    if _SIGNAL_GETITIMER is not None:
        try:
            for _name, timer in _SIGNAL_INTERVAL_TIMERS:
                delay, interval = _SIGNAL_GETITIMER(timer)
                if delay or interval:
                    return True
        except (AttributeError, OSError, TypeError, ValueError):
            return True
    return False


def _native_execution_unsafe() -> bool:
    """Keep native whole-container scans away from concurrent mutation."""
    live_is_gil_enabled = _BUILTIN_GETATTR(_sys, "_is_gil_enabled", None)
    if live_is_gil_enabled is not _SYS_IS_GIL_ENABLED:
        return True
    if _BUILTIN_CALLABLE(_SYS_IS_GIL_ENABLED):
        try:
            if not _SYS_IS_GIL_ENABLED():
                return True
        except (RuntimeError, TypeError):
            return True

    return _native_multi_group_timer_active()


def _python_group_fast_environment_is_pristine() -> bool:
    """Keep failpoints and concurrent mutation on the authoritative row loop."""
    return not (_has_active_failpoints() or _native_execution_unsafe() or _group_hash_replaced())


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
    """Create a guarded native snapshot adapter for canonical NamedTuple rows."""
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
    )
    return fallback if adapter is None else cast(Callable[[Any], dict[str, Any]], adapter)


def _observed_callable_join_record_types(
    left: list[Any] | tuple[Any, ...],
    right: list[Any] | tuple[Any, ...],
    *,
    allow_namedtuple: bool,
) -> tuple[tuple[_CallableJoinRecordCapability, ...], bool] | None:
    """Return narrow exact-type and live-MRO capabilities observed at source heads.

    The native ABI rechecks every initial and live row before invoking an adapter or selector.
    Nominal Mapping types retain the exact MRO object that proved direct Mapping access safe;
    changing ``__bases__`` keeps the same type identity but replaces that object. Sampling only
    the first row keeps this Python gate constant-time, and native preflight declines a later
    distinct type or stale MRO before callback ownership. The boolean result records whether an
    accepted type requires canonical ``_as_record`` conversion instead of built-in ``dict``.
    """
    observed: list[_CallableJoinRecordCapability] = []
    requires_record_adapter = False
    for rows in (left, right):
        try:
            row = rows[0]
        except IndexError:
            continue
        row_type = type(row)
        if row_type is dict:
            continue
        mapping_mro = None if row_type is MappingProxyType else _direct_mapping_mro(row_type)
        if row_type is not MappingProxyType and mapping_mro is None:
            if not allow_namedtuple or not _standard_namedtuple_record_type(row_type):
                return None
            requires_record_adapter = True
        matching = next((known for known in observed if row_type is known[0]), None)
        if matching is None:
            observed.append((row_type, mapping_mro))
        elif matching[1] is not mapping_mro:
            # No callback runs during observation, but a concurrently mutated live list/type can
            # still invalidate the first head proof before native preflight owns either source.
            return None
    return tuple(observed), requires_record_adapter


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
    record_capabilities, requires_record_adapter = observed
    record_types = tuple(capability[0] for capability in record_capabilities)
    native_capabilities = tuple(
        row_type if mapping_mro is None else (row_type, mapping_mro)
        for row_type, mapping_mro in record_capabilities
    )
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
    record_adapter: (
        Callable[[Any], dict[str, Any]]
        | tuple[type[dict[Any, Any]], Callable[[Any], dict[str, Any]]]
    )
    if requires_record_adapter:
        record_adapter = _make_standard_namedtuple_record_adapter(record_types) or _as_record
        if record_adapter is not _as_record:
            record_adapter = _native_standard_namedtuple_record_adapter(
                native_module,
                record_types,
                record_adapter,
            )
    else:
        # Current native kernels parse this exact pair as a fast adapter for preflighted
        # Mapping rows plus the canonical protocol fallback for callback-replaced live rows.
        # Older wheels see a non-callable adapter and decline before invoking either selector.
        record_adapter = (dict, _as_record)
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
        return cast(list[dict[str, Any]] | None, callback_kernel(*arguments, native_capabilities))
    return cast(list[dict[str, Any]] | None, callback_kernel(*arguments))


def _try_native_direct_record_join(
    native_module: Any,
    root: JoinPhysicalNode,
    left: list[Any] | tuple[Any, ...],
    right: list[Any] | tuple[Any, ...],
    left_field: str,
    right_field: str,
    *,
    many: bool,
) -> list[dict[str, Any]] | None:
    """Pass exact fields through the suffix-aware exact-dict/Mapping ABI."""
    observed = _observed_callable_join_record_types(
        left,
        right,
        allow_namedtuple=False,
    )
    if observed is None:
        return None
    record_capabilities, requires_record_adapter = observed
    native_capabilities = tuple(
        row_type if mapping_mro is None else (row_type, mapping_mro)
        for row_type, mapping_mro in record_capabilities
    )
    if requires_record_adapter:
        return None
    cardinality = "many" if many else "unique"
    kernel = getattr(
        native_module,
        f"join_hashable_{cardinality}_direct_records_v1",
        None,
    )
    if not callable(kernel):
        return None
    # Current kernels use canonical callbacks only if a snapshot callback replaces a future row
    # with a type outside the exact preflight set. Older direct kernels see the nested token tuple
    # as an unsupported shape and decline before observing either source.
    capabilities = (
        native_capabilities,
        _as_record,
        root.spec.left_key,
        root.spec.right_key,
    )
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
            capabilities,
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

    arguments = (
        left,
        right,
        native.left_field,
        native.right_field,
        left_join,
    )
    borrowed_kernel = getattr(native_module, "join_i64_unique_dict_rows_v2", None)
    if callable(borrowed_kernel):
        joined = cast(list[dict[str, Any]] | None, borrowed_kernel(*arguments))
        if joined is not None:
            return joined

    snapshot_kernel = getattr(native_module, "join_i64_unique_dict_rows_v1", None)
    if not callable(snapshot_kernel):
        return None
    return cast(list[dict[str, Any]] | None, snapshot_kernel(*arguments))


def _try_native_pair_sum_rows(
    native_module: Any,
    rows: object,
    output_name: str,
) -> dict[Any, dict[str, Any]] | None:
    """Dispatch the narrow single-sum ABI before the generic lane kernel."""
    kernel_v2 = getattr(native_module, "group_sum_i64_exact_pairs_v2", None)
    if callable(kernel_v2):
        materialized = kernel_v2(rows, output_name)
        if materialized is None:
            return None
        is_final, groups = materialized
        if is_final:
            return cast(dict[Any, dict[str, Any]], groups)
    else:
        kernel_v1 = getattr(native_module, "group_sum_i64_exact_pairs_v1", None)
        if not callable(kernel_v1):
            return None
        groups = kernel_v1(rows)
        if groups is None:
            return None
    return {key: {output_name: total} for key, total in groups}


def _encode_native_multi_group_lane(
    kind: str,
    value_selector: int | str | None,
    output_name: str,
) -> tuple[int, int | str | None, str] | None:
    """Encode one validated aggregate lane without a mutable dispatch table."""
    match kind:
        case "count":
            code = 0
        case "sum":
            code = 1
        case "min":
            code = 2
        case "max":
            code = 3
        case _:
            return None
    return code, value_selector, output_name


def try_native_pair_aggregations(
    plan: LogicalPlan,
    aggregations: AggregationItems,
    kinds: tuple[str, ...],
) -> dict[Any, dict[str, Any]] | None:
    """Aggregate guarded whole-value lanes without opening a direct pair source."""
    if plan.engine != "auto" or not aggregations or not isinstance(plan.root, SourceNode):
        return None

    source = plan.root.source
    capabilities = source.capabilities
    rows = source.native_data
    if not capabilities.reiterable or not capabilities.ordered or type(rows) not in (list, tuple):
        return None

    try:
        from ... import _native
    except ImportError:
        return None

    if _native_execution_unsafe():
        return None

    if kinds == ("sum",) and len(aggregations) == 1:
        return _try_native_pair_sum_rows(_native, rows, aggregations[0][0])

    if len(aggregations) != len(kinds):
        return None
    lanes: list[tuple[int, int | str | None, str]] = []
    output_names: set[str] = set()
    for (output_name, _aggregation), kind in zip(aggregations, kinds, strict=True):
        if _BUILTIN_TYPE(output_name) is not str:
            return None
        lane = _encode_native_multi_group_lane(
            kind,
            None if kind == "count" else 1,
            output_name,
        )
        if lane is None:
            return None
        lanes.append(lane)
        output_names.add(output_name)

    kernel = getattr(_native, "group_multi_i64_exact_pairs_v1", None)
    if not callable(kernel):
        return None
    key_name = "\0fpstreams_pair_key"
    while key_name in output_names:
        key_name += "\0"
    grouped = kernel(rows, key_name, tuple(lanes))
    if grouped is None:
        return None
    return {row.pop(key_name): row for row in grouped}


def try_retained_arrow_unique_join(plan: PhysicalPlan) -> list[dict[str, Any]] | None:
    """Materialize one guarded top-level retained Arrow m:1 join by column position."""
    return _try_retained_arrow_unique_join(plan, import_module)


def try_native_record_join(plan: PhysicalPlan) -> list[dict[str, Any]] | None:
    """Materialize one guarded top-level exact-record join without opening either source."""
    from ...runtime.failpoints import has_active_failpoints

    root = plan.root
    if (
        plan.terminal.name != "list"
        or plan.engine != "auto"
        or has_active_failpoints()
        or not isinstance(root, JoinPhysicalNode)
        or (
            root.native_direct_fields is None
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
        from ... import _native
    except ImportError:
        return None

    direct = root.native_direct_fields
    if direct is not None:
        if root.native_record_i64 is not None:
            direct_rows = _try_native_i64_record_join(_native, root, left, right)
            if direct_rows is not None:
                return direct_rows
        direct_rows = _try_native_direct_record_join(
            _native,
            root,
            left,
            right,
            direct.left_field,
            direct.right_field,
            many=root.spec.logical.validate == "m:m",
        )
        if direct_rows is not None:
            return direct_rows
        return _try_native_hashable_record_join(
            _native,
            root,
            left,
            right,
            root.spec.left_key,
            root.spec.right_key,
            many=root.spec.logical.validate == "m:m",
            allow_namedtuple=False,
        )

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


def _try_direct_python_group_list(
    node: GroupAggregatePhysicalNode,
) -> list[dict[str, Any]] | None:
    """Materialize one canonical Python group directly from its leaf source."""
    if node.partitions is not None or not isinstance(node.input, SourcePhysicalNode):
        return None
    values = node.input.source.open()
    # Source opening is an observable boundary: failpoints and custom openers may
    # replace a retained Aggregator lifecycle before its first state is created.
    node = _revalidated_group_node(node)
    return list(_execute_python_group_values(values, node))


def try_direct_group_list(
    plan: PhysicalPlan,
) -> tuple[list[dict[str, Any]] | None, PhysicalPlan]:
    """Return a directly materialized group result, or a no-retry fallback plan.

    A top-level list terminal can return fixed-state Python or native materialization without
    forwarding every output row through the relational generators. If a speculative native ABI
    declines, clear its markers in the local immutable plan so the canonical fallback does not
    repeat that scan.
    """
    root = plan.root
    if plan.terminal.name != "list" or not isinstance(root, GroupAggregatePhysicalNode):
        return None, plan
    revalidated_root = _revalidated_group_node(root)
    if revalidated_root is not root:
        return None, replace(plan, root=revalidated_root)
    has_backend_marker = not (
        root.arrow_i64_sum is None
        and root.numpy_group is None
        and root.native_pair_i64_expr_sum is None
        and root.native_i64_sum is None
        and root.native_record_i64_sum is None
        and root.native_fixed_i64_group is None
    )
    if plan.engine != "auto" or not has_backend_marker:
        return _try_direct_python_group_list(root), plan

    group_rows = _try_arrow_group_sum(root)
    numpy_direct = False
    rust_direct = False
    if group_rows is None:
        group_rows = _numpy_group_aggregate(
            root,
            chunk_rows=_NUMPY_GROUP_CHUNK_ROWS,
            aggregations_validated=True,
        )
        numpy_direct = group_rows is not None
    if group_rows is None:
        group_rows = _try_native_group_sum(root)
        rust_direct = group_rows is not None
    if group_rows is not None:
        if numpy_direct or rust_direct:
            from ...runtime.report import _record_direct_strategy

            _record_direct_strategy(
                plan,
                "numpy_direct" if numpy_direct else "rust_direct",
                (
                    "bounded NumPy columns supplied grouped aggregation without row boxing"
                    if numpy_direct
                    else "bounded exact records supplied grouped aggregation in Rust"
                ),
            )
        return group_rows, plan
    fallback_root = replace(
        root,
        arrow_i64_sum=None,
        native_pair_i64_expr_sum=None,
        native_i64_sum=None,
        native_record_i64_sum=None,
        native_fixed_i64_group=None,
        numpy_group=None,
    )
    group_rows = _try_direct_python_group_list(fallback_root)
    if group_rows is not None:
        return group_rows, plan
    return None, replace(plan, root=fallback_root)


def try_direct_global_list(
    plan: PhysicalPlan,
) -> tuple[list[dict[str, Any]] | None, PhysicalPlan]:
    """Materialize one guarded top-level global result without a forwarding runtime."""
    root = plan.root
    if (
        plan.terminal.name != "list"
        or plan.engine != "auto"
        or not isinstance(root, GlobalAggregatePhysicalNode)
        or (
            not isinstance(root.arrow_i64_sum, ArrowGlobalAggregateSpec)
            and root.numpy_global is None
        )
        or not _global_aggregations_are_live(root)
    ):
        return None, plan
    fallback_plan = plan
    if isinstance(root.arrow_i64_sum, ArrowGlobalAggregateSpec):
        result = _try_arrow_global_reduction(root, plan)
        if result is not None:
            return [result], plan
        root = replace(root, arrow_i64_sum=None)
        fallback_plan = replace(plan, root=root)
    if root.numpy_global is None or root.exact_count_name is not None:
        return None, fallback_plan
    result = _numpy_global_aggregate(
        root,
        chunk_rows=_NUMPY_GLOBAL_CHUNK_ROWS,
        aggregations_validated=True,
    )
    if result is not None:
        from ...runtime.report import _record_direct_strategy

        _record_direct_strategy(
            plan,
            "numpy_direct",
            "bounded NumPy columns supplied global aggregation without row boxing",
        )
        return [result], plan
    return None, replace(fallback_plan, root=replace(root, numpy_global=None))


def execute_relational(
    root: PhysicalRelNode,
    runtime: QueryRuntime,
    outer_plan: PhysicalPlan | None = None,
) -> Iterator[Any]:
    """Lazily execute a relational root while its owning query runtime remains active."""
    if isinstance(root, SourcePhysicalNode):
        source_iterator = root.source.open()
        active_error: BaseException | None = None
        try:
            for item in source_iterator:  # noqa: UP028 - this layer owns the iterator close.
                yield item
        except BaseException as error:
            active_error = error
            raise
        finally:
            close_iterators((source_iterator,), active_error=active_error)
        return
    if isinstance(root, PipelinePhysicalNode):
        source = _pipeline_source(root.input, runtime)
        yield from execute(
            Pipeline(source, _physical_operations(root.stages), root.engine, root.parallel),
            runtime=runtime,
        )
        return
    if isinstance(root, JoinPhysicalNode):
        yield from _execute_join(root, runtime, execute_relational)
        return
    if isinstance(root, GroupAggregatePhysicalNode):
        yield from _execute_group_aggregate(root, runtime, outer_plan)
        return
    if isinstance(root, GlobalAggregatePhysicalNode):
        if not _global_aggregations_are_live(root):
            values = execute_relational(root.input, runtime)
            yield run_collector_program(values, root.aggregations.collectors)
            return
        exact_count = _try_exact_global_count(root)
        if exact_count is not None:
            yield exact_count
        else:
            arrow_count = _try_arrow_global_count(root)
            if arrow_count is not None:
                yield arrow_count
            else:
                columnar_reduction = _try_arrow_global_reduction(root, outer_plan)
                if columnar_reduction is not None:
                    yield columnar_reduction
                else:
                    numpy_reduction = _numpy_global_aggregate(
                        root,
                        chunk_rows=_NUMPY_GLOBAL_CHUNK_ROWS,
                        aggregations_validated=True,
                    )
                    if numpy_reduction is not None:
                        from ...runtime.report import _record_direct_strategy

                        _record_direct_strategy(
                            outer_plan,
                            "numpy_direct",
                            "bounded NumPy columns supplied global aggregation without row boxing",
                        )
                        yield numpy_reduction
                    else:
                        native_reduction = _try_native_global_i64_aggregate(root)
                        native_reason = (
                            "one retained exact-record scan reduced multiple i64 lanes in Rust"
                        )
                        if native_reduction is None:
                            native_reduction = _try_native_record_global_sum(root)
                            native_reason = (
                                "one retained exact-record i64 field was reduced in Rust"
                            )
                        if native_reduction is not None:
                            from ...runtime.report import _record_direct_strategy

                            _record_direct_strategy(
                                outer_plan,
                                "rust_direct",
                                native_reason,
                            )
                            yield native_reduction
                        else:
                            values = execute_relational(root.input, runtime)
                            yield run_collector_program(values, root.aggregations.collectors)
        return
    raise TypeError(f"unsupported physical relation: {type(root).__name__}")


def _pipeline_source(root: PhysicalRelNode, runtime: QueryRuntime) -> Source[Any]:
    """Convert a relational branch to a compatibility source without opening it early."""
    from ...planning.source import Source

    if isinstance(root, SourcePhysicalNode):
        return root.source
    return Source.defer(lambda: execute_relational(root, runtime))


def _physical_operations(nodes: tuple[PhysicalNode, ...]) -> tuple[Any, ...]:
    """Read canonical operation payloads from compiled unary stages."""
    from ..physical import operations_from_physical_nodes

    return operations_from_physical_nodes(nodes)


def _arrow_planning_operations(nodes: tuple[PhysicalNode, ...]) -> tuple[Any, ...] | None:
    """Recover original RowExpr operations when revalidating an Arrow physical marker."""
    operations: list[Any] = []
    for node in nodes:
        if not isinstance(
            node,
            (RowPhysicalNode, CompiledExpressionPhysicalNode, SortPhysicalNode),
        ):
            return None
        operations.append(node.operation)
    return tuple(operations)


def _single_collector_lifecycle(
    collector: Collector[Any, Any, Any],
) -> tuple[
    int,
    Callable[[], Any],
    Callable[[Any, Any], Any],
    Callable[[Any], bool],
]:
    """Capture one coherent lifecycle generation after an observed replacement."""
    while True:
        revision = collector._lifecycle_revision
        initializer = collector.initializer
        step = collector.step
        done = collector.done
        if collector._lifecycle_revision == revision:
            return revision, initializer, step, done


def _stable_direct_group_field(selector: Callable[[Any], Any]) -> str | None:
    """Recover one unmodified generated field selector for the local group loop."""
    if _BUILTIN_TYPE(selector) is not FunctionType:
        return None
    function = selector
    field = _direct_field(selector)
    selector_globals = function.__globals__
    selector_builtins = _BUILTIN_GETATTR(function, "__builtins__", None)
    if (
        field is None
        or _BUILTIN_TYPE(selector_builtins) is not _BUILTIN_DICT
        or function.__code__ is not _CANONICAL_GROUP_FIELD_SELECTOR_CODE
    ):
        return None
    selector_builtins = cast(dict[str, Any], selector_builtins)
    for name, canonical in (
        ("AttributeError", _BUILTIN_ATTRIBUTE_ERROR),
        ("KeyError", _BUILTIN_KEY_ERROR),
        ("TypeError", _BUILTIN_TYPE_ERROR),
        ("dict", _BUILTIN_DICT),
        ("getattr", _BUILTIN_GETATTR),
        ("isinstance", _BUILTIN_ISINSTANCE),
        ("type", _BUILTIN_TYPE),
        ("Mapping", _CANONICAL_MAPPING),
        ("SelectionError", _CANONICAL_SELECTION_ERROR),
    ):
        if selector_globals.get(name, selector_builtins.get(name)) is not canonical:
            return None
    cells = function.__closure__
    if cells is None:
        return None
    try:
        return field if cells[0].cell_contents is field else None
    except ValueError:
        return None


_GROUP_SUM_AFTER_KEY = 1
_GROUP_SUM_AFTER_LOOKUP = 2
_GROUP_SUM_AFTER_INSERT = 3
_GROUP_SUM_AFTER_ADD = 4


def _continue_factory_sum_group(  # noqa: C901 - cold lifecycle state machine
    iterator: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    collector: Collector[Any, Any, Any],
    positions: dict[Any, int],
    keys: list[Any],
    states: list[Any],
    completed: list[Any],
    *,
    phase: int,
    row: Any,
    key: Any = None,
    position: int | None = None,
) -> None:
    """Finish one interrupted row, then consume into the same dense state."""
    revision, initializer, step, done = _single_collector_lifecycle(collector)

    if phase == _GROUP_SUM_AFTER_ADD:
        assert position is not None
        if done is not _never_done:
            completed[position] = done(states[position])
    else:
        if phase == _GROUP_SUM_AFTER_KEY:
            try:
                hash(key)
                position = positions.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            live_revision = collector._lifecycle_revision
            if live_revision != revision:
                revision, initializer, step, done = _single_collector_lifecycle(collector)

        if phase in (_GROUP_SUM_AFTER_KEY, _GROUP_SUM_AFTER_LOOKUP):
            if position is None:
                state = initializer()
                live_revision = collector._lifecycle_revision
                if live_revision != revision:
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
                is_completed = done(state) if done is not _never_done else False
                live_revision = collector._lifecycle_revision
                if live_revision != revision:
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
                proposed = len(keys)
                position = positions.setdefault(key, proposed)
                if position == proposed:
                    keys.append(key)
                    states.append(state)
                    completed.append(is_completed)
                else:
                    # A key can change its equality/hash behavior between get and insert.
                    # Exact dict assignment keeps the old slot but replaces its entry.
                    keys[position] = key
                    states[position] = state
                    completed[position] = is_completed
                live_revision = collector._lifecycle_revision
                if live_revision != revision:
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
            else:
                state = states[position]
                is_completed = completed[position]
        else:
            assert phase == _GROUP_SUM_AFTER_INSERT and position is not None
            state = states[position]
            is_completed = completed[position]

        if not (done is not _never_done and is_completed):
            state = step(state, row)
            states[position] = state
            live_revision = collector._lifecycle_revision
            if live_revision != revision:
                revision, initializer, step, done = _single_collector_lifecycle(collector)
            if done is not _never_done:
                completed[position] = done(state)

    select_key = node.select_key
    for row in iterator:
        key = select_key(row)
        live_revision = collector._lifecycle_revision
        if live_revision != revision:
            revision, initializer, step, done = _single_collector_lifecycle(collector)
        try:
            hash(key)
            position = positions.get(key)
        except TypeError:
            raise TypeError("group_by keys must be hashable") from None
        live_revision = collector._lifecycle_revision
        if live_revision != revision:
            revision, initializer, step, done = _single_collector_lifecycle(collector)
        if position is None:
            state = initializer()
            live_revision = collector._lifecycle_revision
            if live_revision != revision:
                revision, initializer, step, done = _single_collector_lifecycle(collector)
            is_completed = done(state) if done is not _never_done else False
            live_revision = collector._lifecycle_revision
            if live_revision != revision:
                revision, initializer, step, done = _single_collector_lifecycle(collector)
            proposed = len(keys)
            position = positions.setdefault(key, proposed)
            if position == proposed:
                keys.append(key)
                states.append(state)
                completed.append(is_completed)
            else:
                keys[position] = key
                states[position] = state
                completed[position] = is_completed
            live_revision = collector._lifecycle_revision
            if live_revision != revision:
                revision, initializer, step, done = _single_collector_lifecycle(collector)
        else:
            state = states[position]
            is_completed = completed[position]
        if done is not _never_done and is_completed:
            continue
        state = step(state, row)
        states[position] = state
        live_revision = collector._lifecycle_revision
        if live_revision != revision:
            revision, initializer, step, done = _single_collector_lifecycle(collector)
        if done is not _never_done:
            completed[position] = done(state)


def _execute_factory_sum_group(  # noqa: C901 - hot PIC and cold deopt stay adjacent
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    collector: Collector[Any, Any, Any],
    output_name: str,
    revision: int,
    sum_select: Callable[[Any], Any],
) -> Iterator[dict[str, Any]]:
    """Run one canonical sum densely and permanently continue cold after mutation."""
    positions: dict[Any, int] = {}
    keys: list[Any] = []
    states: list[Any] = []
    deopted = False
    multiple = len(node.keys) > 1
    iterator = iter(values)
    select_key = node.select_key
    direct_key = _stable_direct_group_field(select_key)
    selector_globals = (
        cast(FunctionType, select_key).__globals__ if direct_key is not None else None
    )
    direct_value = _stable_direct_group_field(sum_select)
    value_selector_globals = (
        cast(FunctionType, sum_select).__globals__ if direct_value is not None else None
    )
    mapping_protocol_globals = _direct_mapping_mro.__globals__
    direct_mapping_type_0: type[Any] | None = None
    direct_mapping_mro_0: tuple[type[Any], ...] | None = None
    direct_mapping_type_1: type[Any] | None = None
    direct_mapping_mro_1: tuple[type[Any], ...] | None = None
    active_error: BaseException | None = None
    try:
        for row in iterator:
            if direct_key is None:
                key = select_key(row)
            else:
                try:
                    row_type = _BUILTIN_TYPE(row)
                    if row_type is _BUILTIN_DICT:
                        key = row[direct_key]
                    elif (
                        selector_globals is None
                        or selector_globals.get("Mapping") is not _CANONICAL_MAPPING
                        or mapping_protocol_globals.get("Mapping") is not _CANONICAL_MAPPING
                    ):
                        key = select_key(row)
                        direct_key = None
                    elif (
                        row_type is MappingProxyType
                        or (
                            row_type is direct_mapping_type_0
                            and row_type.__mro__ is direct_mapping_mro_0
                        )
                        or (
                            row_type is direct_mapping_type_1
                            and row_type.__mro__ is direct_mapping_mro_1
                        )
                    ):
                        key = row[direct_key]
                    else:
                        mapping_mro = _direct_mapping_mro(row_type)
                        if mapping_mro is not None and row_type.__mro__ is mapping_mro:
                            key = row[direct_key]
                            if row_type is direct_mapping_type_0:
                                direct_mapping_mro_0 = mapping_mro
                            elif row_type is direct_mapping_type_1:
                                direct_mapping_mro_1 = mapping_mro
                            elif direct_mapping_type_0 is None:
                                direct_mapping_type_0 = row_type
                                direct_mapping_mro_0 = mapping_mro
                            elif direct_mapping_type_1 is None:
                                direct_mapping_type_1 = row_type
                                direct_mapping_mro_1 = mapping_mro
                            else:
                                direct_key = None
                        else:
                            key = select_key(row)
                            direct_key = None
                except (AttributeError, KeyError, TypeError) as error:
                    raise _CANONICAL_SELECTION_ERROR(
                        f"Could not resolve selector {direct_key!r}; failed at {direct_key!r}"
                    ) from error

            try:
                hash(key)
                position = positions.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            if collector._lifecycle_revision != revision:
                deopted = True
                _continue_factory_sum_group(
                    iterator,
                    node,
                    collector,
                    positions,
                    keys,
                    states,
                    [False] * len(states),
                    phase=_GROUP_SUM_AFTER_LOOKUP,
                    row=row,
                    key=key,
                    position=position,
                )
                break
            if position is None:
                proposed = len(keys)
                position = positions.setdefault(key, proposed)
                if position == proposed:
                    keys.append(key)
                    states.append(0)
                else:
                    keys[position] = key
                    states[position] = 0
                if collector._lifecycle_revision != revision:
                    deopted = True
                    _continue_factory_sum_group(
                        iterator,
                        node,
                        collector,
                        positions,
                        keys,
                        states,
                        [False] * len(states),
                        phase=_GROUP_SUM_AFTER_INSERT,
                        row=row,
                        key=key,
                        position=position,
                    )
                    break
            if direct_value is None or direct_key is None:
                selected = sum_select(row)
            else:
                try:
                    row_type = _BUILTIN_TYPE(row)
                    if row_type is _BUILTIN_DICT:
                        selected = row[direct_value]
                    elif (
                        value_selector_globals is None
                        or value_selector_globals.get("Mapping") is not _CANONICAL_MAPPING
                    ):
                        direct_value = None
                        selected = sum_select(row)
                    elif (
                        row_type is MappingProxyType
                        or (
                            row_type is direct_mapping_type_0
                            and row_type.__mro__ is direct_mapping_mro_0
                        )
                        or (
                            row_type is direct_mapping_type_1
                            and row_type.__mro__ is direct_mapping_mro_1
                        )
                    ):
                        selected = row[direct_value]
                    else:
                        direct_value = None
                        selected = sum_select(row)
                except (AttributeError, KeyError, TypeError) as error:
                    raise _CANONICAL_SELECTION_ERROR(
                        f"Could not resolve selector {direct_value!r}; failed at {direct_value!r}"
                    ) from error
            states[position] = states[position] + selected
            if collector._lifecycle_revision != revision:
                deopted = True
                _continue_factory_sum_group(
                    iterator,
                    node,
                    collector,
                    positions,
                    keys,
                    states,
                    [False] * len(states),
                    phase=_GROUP_SUM_AFTER_ADD,
                    row=row,
                    key=key,
                    position=position,
                )
                break
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((iterator,), active_error=active_error)

    direct_finish = not deopted and collector._lifecycle_revision == revision

    if not multiple:
        key_name = node.key_names[0]
        for key, state in zip(keys, states, strict=True):
            result = {key_name: key}
            if direct_finish and collector._lifecycle_revision != revision:
                direct_finish = False
            result[output_name] = state if direct_finish else collector.finish(state)
            yield result
        return
    key_names = node.key_names
    for key, state in zip(keys, states, strict=True):
        result = dict(zip(key_names, key, strict=True))
        if direct_finish and collector._lifecycle_revision != revision:
            direct_finish = False
        result[output_name] = state if direct_finish else collector.finish(state)
        yield result


def _execute_single_collector_group(  # noqa: C901 - lifecycle guards and key PIC stay inline
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
) -> Iterator[dict[str, Any]]:
    """Group one arbitrary collector without allocating one-element program states."""
    from ...runtime.failpoints import has_active_failpoints, hit

    program = node.aggregations.collectors
    collector = program.layout.collectors[0]
    output_name = program.layout.names[0]
    revision, initializer, step, done = _single_collector_lifecycle(collector)
    instrumented = has_active_failpoints()
    if not instrumented:
        sum_hint = native_group_aggregation(cast(Any, collector))
        if sum_hint is not None and sum_hint.kind == "sum":
            sum_cells = step.__closure__
            if sum_cells is not None:
                yield from _execute_factory_sum_group(
                    values,
                    node,
                    collector,
                    output_name,
                    revision,
                    sum_cells[0].cell_contents,
                )
                return
    groups: dict[Any, list[Any]] = {}
    multiple = len(node.keys) > 1
    iterator = iter(values)
    select_key = node.select_key
    direct_key = _stable_direct_group_field(select_key)
    selector_globals = (
        cast(FunctionType, select_key).__globals__ if direct_key is not None else None
    )
    mapping_protocol_globals = _direct_mapping_mro.__globals__
    direct_mapping_type_0: type[Any] | None = None
    direct_mapping_mro_0: tuple[type[Any], ...] | None = None
    direct_mapping_type_1: type[Any] | None = None
    direct_mapping_mro_1: tuple[type[Any], ...] | None = None
    active_error: BaseException | None = None
    try:
        for row in iterator:
            if direct_key is None:
                key = select_key(row)
            else:
                try:
                    row_type = _BUILTIN_TYPE(row)
                    if row_type is _BUILTIN_DICT:
                        key = row[direct_key]
                    elif (
                        selector_globals is None
                        or selector_globals.get("Mapping") is not _CANONICAL_MAPPING
                        or mapping_protocol_globals.get("Mapping") is not _CANONICAL_MAPPING
                    ):
                        key = select_key(row)
                        direct_key = None
                    elif (
                        row_type is MappingProxyType
                        or (
                            row_type is direct_mapping_type_0
                            and row_type.__mro__ is direct_mapping_mro_0
                        )
                        or (
                            row_type is direct_mapping_type_1
                            and row_type.__mro__ is direct_mapping_mro_1
                        )
                    ):
                        key = row[direct_key]
                    else:
                        mapping_mro = _direct_mapping_mro(row_type)
                        if mapping_mro is not None and row_type.__mro__ is mapping_mro:
                            key = row[direct_key]
                            if row_type is direct_mapping_type_0:
                                direct_mapping_mro_0 = mapping_mro
                            elif row_type is direct_mapping_type_1:
                                direct_mapping_mro_1 = mapping_mro
                            elif direct_mapping_type_0 is None:
                                direct_mapping_type_0 = row_type
                                direct_mapping_mro_0 = mapping_mro
                            elif direct_mapping_type_1 is None:
                                direct_mapping_type_1 = row_type
                                direct_mapping_mro_1 = mapping_mro
                            else:
                                # Highly polymorphic streams already benefit from the ABC's
                                # own cache; avoid turning two local slots into a miss loop.
                                direct_key = None
                        else:
                            key = select_key(row)
                            direct_key = None
                except (AttributeError, KeyError, TypeError) as error:
                    raise _CANONICAL_SELECTION_ERROR(
                        f"Could not resolve selector {direct_key!r}; failed at {direct_key!r}"
                    ) from error
            live_revision = collector._lifecycle_revision
            if live_revision != revision:
                revision, initializer, step, done = _single_collector_lifecycle(collector)
            try:
                hash(key)
                entry = groups.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            live_revision = collector._lifecycle_revision
            if live_revision != revision:
                revision, initializer, step, done = _single_collector_lifecycle(collector)
            if entry is None:
                state = initializer()
                live_revision = collector._lifecycle_revision
                if live_revision != revision:
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
                completed = done(state) if done is not _never_done else False
                live_revision = collector._lifecycle_revision
                if live_revision != revision:
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
                entry = [key, state, completed]
                groups[key] = entry
                live_revision = collector._lifecycle_revision
                if live_revision != revision:
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
                if instrumented:
                    hit("group.state.create.after")
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
            else:
                state = entry[1]
                completed = entry[2]
            if done is not _never_done and completed:
                continue
            state = step(state, row)
            entry[1] = state
            live_revision = collector._lifecycle_revision
            if live_revision != revision:
                revision, initializer, step, done = _single_collector_lifecycle(collector)
            if done is not _never_done:
                entry[2] = done(state)
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((iterator,), active_error=active_error)
    if not multiple:
        key_name = node.key_names[0]
        for entry in groups.values():
            # Materialize the key before invoking a live finisher, matching the
            # general zip-then-update ordering without its temporary dictionaries.
            result = {key_name: entry[0]}
            result[output_name] = collector.finish(entry[1])
            yield result
        return
    key_names = node.key_names
    for entry in groups.values():
        result = dict(zip(key_names, entry[0], strict=True))
        result[output_name] = collector.finish(entry[1])
        yield result


def _composite_count_sum_groups(
    compact: dict[tuple[Any, Any], list[Any]],
) -> dict[Any, tuple[Any, CollectorState]]:
    """Convert exact compact states only when the composite loop must deopt."""
    return {
        state[0]: (
            state[0],
            CollectorState([state[1], state[2]], [False, False]),
        )
        for state in compact.values()
    }


def _execute_composite_count_sum(
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    spec: CompositeCountSumSpec,
) -> Iterator[dict[str, Any]]:
    """Group the closed exact two-key count/sum shape, deopting without replay."""
    first_index, second_index = spec.key_selectors
    value_index = spec.value_selector
    if (
        _BUILTIN_TYPE(first_index) is not _BUILTIN_INT
        or _BUILTIN_TYPE(second_index) is not _BUILTIN_INT
        or _BUILTIN_TYPE(value_index) is not _BUILTIN_INT
        or not _python_group_fast_environment_is_pristine()
    ):
        yield from _execute_authoritative_group(values, node)
        return

    program = node.aggregations.collectors
    count_collector, sum_collector = program.layout.collectors
    count_revision = count_collector._lifecycle_revision
    sum_revision = sum_collector._lifecycle_revision
    compact: dict[tuple[Any, Any], list[Any]] = {}
    iterator = iter(values)
    handed_off = False
    active_error: BaseException | None = None

    def deopt(row: Any) -> Iterator[dict[str, Any]]:
        nonlocal handed_off
        groups = _composite_count_sum_groups(compact)
        handed_off = True
        return _execute_authoritative_group(
            iterator,
            node,
            initial_groups=groups,
            first_row=row,
        )

    try:
        for row in iterator:
            if (
                count_collector._lifecycle_revision != count_revision
                or sum_collector._lifecycle_revision != sum_revision
            ):
                continuation = deopt(row)
                del row
                yield from continuation
                return

            row_type = _BUILTIN_TYPE(row)
            if row_type is not _BUILTIN_TUPLE and row_type is not _BUILTIN_LIST:
                continuation = deopt(row)
                del row
                yield from continuation
                return
            try:
                first = row[first_index]
                second = row[second_index]
                selected = row[value_index]
            except (IndexError, TypeError):
                continuation = deopt(row)
                del row
                yield from continuation
                return
            if (
                not _exact_builtin_group_key_type(_BUILTIN_TYPE(first))
                or not _exact_builtin_group_key_type(_BUILTIN_TYPE(second))
                or _BUILTIN_TYPE(selected) is not _BUILTIN_INT
            ):
                continuation = deopt(row)
                del row
                yield from continuation
                return

            key = (first, second)
            state = compact.get(key)
            is_new = state is None
            if state is None:
                state = [key, 1, selected]
                compact[key] = state
            else:
                previous_count = state[1]
                previous_total = state[2]
                state[1] = previous_count + 1
                state[2] = previous_total + selected

            if (
                count_collector._lifecycle_revision == count_revision
                and sum_collector._lifecycle_revision == sum_revision
            ):
                continue
            if is_new:
                del compact[key]
            else:
                state[1] = previous_count
                state[2] = previous_total
            continuation = deopt(row)
            del row
            yield from continuation
            return
    except BaseException as error:
        active_error = error
        raise
    finally:
        if not handed_off:
            close_iterators((iterator,), active_error=active_error)

    first_name, second_name = node.key_names
    for key, count, total in compact.values():
        yield {
            first_name: key[0],
            second_name: key[1],
            spec.count_name: count_collector.finish(count),
            spec.sum_name: sum_collector.finish(total),
        }


def _execute_authoritative_group(
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    *,
    initial_groups: dict[Any, tuple[Any, CollectorState]] | None = None,
    first_row: Any = _MISSING,
) -> Iterator[dict[str, Any]]:
    """Run live collector lifecycles while retaining compact first-seen group state."""
    program = node.aggregations.collectors
    if (
        initial_groups is None
        and first_row is _MISSING
        and program.single
        and not _native_execution_unsafe()
    ):
        yield from _execute_single_collector_group(values, node)
        return
    from ...runtime.failpoints import has_active_failpoints, hit

    groups: dict[Any, tuple[Any, Any]] = {} if initial_groups is None else initial_groups
    instrumented = has_active_failpoints()
    multiple = len(node.keys) > 1
    iterator = iter(values)
    rows = iterator if first_row is _MISSING else chain((first_row,), iterator)
    active_error: BaseException | None = None
    try:
        for row in rows:
            key = node.select_key(row)
            try:
                hash(key)
                entry = groups.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            if entry is None:
                state = program.initialize()
                groups[key] = (key, state)
                if instrumented:
                    hit("group.state.create.after")
            else:
                _first, state = entry
            program.step(state, row)
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((iterator,), active_error=active_error)
    for _group_key, (key, state) in groups.items():
        values_for_key = key if multiple else (key,)
        result = dict(zip(node.key_names, values_for_key, strict=True))
        result.update(program.finish(state))
        yield result


def _execute_python_group_values(
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
) -> Iterator[dict[str, Any]]:
    """Run the live collector program after every source-open mutation boundary."""
    if node.composite_count_sum is not None:
        yield from _execute_composite_count_sum(values, node, node.composite_count_sum)
        return
    yield from _execute_authoritative_group(values, node)


def _execute_group_aggregate(
    node: GroupAggregatePhysicalNode,
    runtime: QueryRuntime,
    outer_plan: PhysicalPlan | None = None,
) -> Iterator[dict[str, Any]]:
    """Aggregate first-seen groups directly from a physical branch iterator."""
    node = _revalidated_group_node(node)
    if node.partitions is not None:
        # This must run in the eager executor rather than only inside the spill
        # generator: Source.open() atomically claims a one-shot input.
        require_spill_file_budget(runtime, "group_by")
    group_rows = _try_arrow_group_sum(node)
    numpy_direct = False
    rust_direct = False
    if group_rows is None:
        group_rows = _numpy_group_aggregate(
            node,
            chunk_rows=_NUMPY_GROUP_CHUNK_ROWS,
            aggregations_validated=True,
        )
        numpy_direct = group_rows is not None
    if group_rows is None:
        group_rows = _try_native_group_sum(node)
        rust_direct = group_rows is not None
    if group_rows is not None:
        if numpy_direct or rust_direct:
            from ...runtime.report import _record_direct_strategy

            _record_direct_strategy(
                outer_plan,
                "numpy_direct" if numpy_direct else "rust_direct",
                (
                    "bounded NumPy columns supplied grouped aggregation without row boxing"
                    if numpy_direct
                    else "bounded exact records supplied grouped aggregation in Rust"
                ),
            )
        yield from group_rows
        return

    # Source.open() already owns claim and failpoint handling. Opening a leaf
    # here removes one forwarding generator from aggregation hot loops; nested
    # relational inputs still require the recursive executor.
    values = (
        node.input.source.open()
        if isinstance(node.input, SourcePhysicalNode)
        else execute_relational(node.input, runtime)
    )
    node = _revalidated_group_node(node)
    items = tuple(
        zip(
            node.aggregations.collectors.layout.names,
            node.aggregations.collectors.layout.collectors,
            strict=True,
        )
    )
    if node.partitions is not None:
        spill_count = node.spill_count
        count_spec = (
            (spill_count.key_field, spill_count.output_name)
            if spill_count is not None and not _has_active_failpoints()
            else None
        )
        yield from spilled_group_aggregate(
            values,
            key_names=node.key_names,
            keys=node.keys,
            aggregation_items=cast(Any, items),
            partitions=node.partitions,
            tempdir=node.tempdir,
            limits=node.limits or SpillLimits(),
            runtime=runtime,
            count_spec=count_spec,
        )
        return
    yield from _execute_python_group_values(values, node)


def _try_exact_global_count(node: GlobalAggregatePhysicalNode) -> dict[str, Any] | None:
    """Return one trusted direct source size while preserving instrumented execution."""
    from ...runtime.failpoints import has_active_failpoints

    name = node.exact_count_name
    if (
        name is None
        or not isinstance(node.input, SourcePhysicalNode)
        or not node.input.source.capabilities.reiterable
    ):
        return None
    descriptor = node.input.source.native_data
    descriptor_type = _BUILTIN_TYPE(descriptor)
    if (
        descriptor_type.__module__ == "fpstreams.tabular.numpy"
        and descriptor_type.__name__ == "NumpyRowSource"
    ):
        from ...tabular.numpy import NumpyRowSource, guarded_numpy_identity_source

        if descriptor_type is not NumpyRowSource or (
            not _numpy_failpoints_are_live()
            or guarded_numpy_identity_source(
                node.input.source,
                observers=False,
                exact_names=False,
            )
            is None
        ):
            return None
    elif has_active_failpoints():
        return None
    if isinstance(descriptor, ArrowBatchSource):
        node.input.source.open_native(ArrowBatchSource)
        batches = descriptor.open_batches()
        active_error: BaseException | None = None
        try:
            for batch in batches:
                _observe_arrow_batch_rows(batch)
        except BaseException as error:
            active_error = error
            raise
        finally:
            close_iterators((batches,), active_error=active_error)
    size = node.input.source.current_exact_size()
    return None if size is None else {name: size}


def _try_native_record_global_sum(
    node: GlobalAggregatePhysicalNode,
) -> dict[str, Any] | None:
    """Reduce one retained exact-record i64 field or leave it unopened for fallback."""
    from ...runtime.failpoints import has_active_failpoints

    spec = node.native_record_i64_sum
    if (
        spec is None
        or has_active_failpoints()
        or not isinstance(node.input, SourcePhysicalNode)
        or _native_execution_unsafe()
    ):
        return None
    source = node.input.source.native_data
    if type(source) not in (list, tuple):
        return None
    try:
        from ... import _native
    except ImportError:
        return None
    kernel = getattr(_native, "global_sum_i64_dict_rows_v1", None)
    if not callable(kernel):
        return None
    result = kernel(source, spec.value_field)
    return None if result is None else {spec.output_name: result}


def _try_native_global_i64_aggregate(
    node: GlobalAggregatePhysicalNode,
) -> dict[str, Any] | None:
    """Reduce ordered exact tuple/dict lanes or leave the retained source untouched."""
    from ...runtime.failpoints import has_active_failpoints

    spec = node.native_multi_i64
    if (
        not isinstance(spec, NativeGlobalI64AggregateSpec)
        or has_active_failpoints()
        or not isinstance(node.input, SourcePhysicalNode)
        or _native_execution_unsafe()
    ):
        return None
    source = node.input.source.native_data
    if type(source) not in (list, tuple):
        return None
    if spec.row_kind == "tuple":
        isize_min = -_SYS_MAXSIZE - 1
        if _BUILTIN_ANY(
            lane.value_selector is not None
            and (
                _BUILTIN_TYPE(lane.value_selector) is not _BUILTIN_INT
                or lane.value_selector < isize_min
                or lane.value_selector > _SYS_MAXSIZE
            )
            for lane in spec.lanes
        ):
            return None
    try:
        from ... import _native
    except ImportError:
        return None
    kernel_name = (
        "global_multi_i64_rows_v1" if spec.row_kind == "tuple" else "global_multi_i64_dict_rows_v1"
    )
    kernel = _BUILTIN_GETATTR(_native, kernel_name, None)
    if not _BUILTIN_CALLABLE(kernel):
        return None
    lanes: list[tuple[int, int | str | None, str]] = []
    for lane in spec.lanes:
        encoded = _encode_native_multi_group_lane(
            lane.kind,
            lane.value_selector,
            lane.output_name,
        )
        if encoded is None:
            return None
        lanes.append(encoded)
    return cast(dict[str, Any] | None, kernel(source, _BUILTIN_TUPLE(lanes)))


def _arrow_global_hooks() -> ArrowGlobalHooks:
    """Capture live owner-module seams once at global dispatch."""
    return ArrowGlobalHooks(
        import_module=import_module,
        arrow_planning_operations=_arrow_planning_operations,
        arrow_group_table=_arrow_group_table,
        reduce_arrow_table=_reduce_arrow_table,
    )


def _try_arrow_global_count(node: GlobalAggregatePhysicalNode) -> dict[str, Any] | None:
    """Count one guarded direct Arrow source or complete table-safe prefix."""
    from ...runtime.failpoints import has_active_failpoints

    if node.arrow_count_name is None or has_active_failpoints() or _native_execution_unsafe():
        return None
    return _try_arrow_global_count_impl(node, _arrow_global_hooks())


def _reduce_arrow_table(
    table: Any,
    spec: ArrowGlobalSumSpec,
    pa: Any,
    pc: Any,
) -> dict[str, Any] | None:
    """Reduce a retained Arrow table after source-level guards pass."""
    return _reduce_arrow_table_impl(table, spec, pa, pc)


def _try_arrow_global_reduction(
    node: GlobalAggregatePhysicalNode,
    outer_plan: PhysicalPlan | None = None,
) -> dict[str, Any] | None:
    """Reduce proven Arrow lanes without materializing their input rows."""
    from ...runtime.failpoints import has_active_failpoints

    if node.arrow_i64_sum is None or has_active_failpoints() or _native_execution_unsafe():
        return None
    return _try_arrow_global_reduction_impl(
        node,
        outer_plan,
        _arrow_global_hooks(),
    )


def _arrow_group_hooks() -> ArrowGroupHooks:
    """Capture live owner-module seams once at Arrow group dispatch."""
    return ArrowGroupHooks(
        import_module=import_module,
        arrow_planning_operations=_arrow_planning_operations,
        arrow_group_table=_arrow_group_table,
        execute_authoritative_group=_execute_authoritative_group,
        observe_arrow_batch_rows=_observe_arrow_batch_rows,
        arrow_group_batch_totals=_arrow_group_batch_totals,
        try_arrow_file_group_sum=_try_arrow_file_group_sum,
        try_arrow_retained_group_aggregate=_try_arrow_retained_group_aggregate,
        materialize_arrow_group_rows=_materialize_arrow_group_rows,
        batch_scalar_max_rows=_ARROW_GROUP_BATCH_SCALAR_MAX_ROWS,
        file_min_rows=_ARROW_FILE_GROUP_MIN_ROWS,
        reader_multi_min_rows=_ARROW_READER_GROUP_MULTI_MIN_ROWS,
        reader_cardinality_sample_rows=_ARROW_READER_GROUP_CARDINALITY_SAMPLE_ROWS,
        reader_max_distinct_ratio=_ARROW_READER_GROUP_MAX_DISTINCT_RATIO,
        csv_min_bytes=_ARROW_CSV_GROUP_MIN_BYTES,
    )


def _arrow_group_batch_totals(
    key_values: Any,
    sum_values: Any,
    row_count: int,
    pa: Any,
    pc: Any,
) -> tuple[list[Any], list[Any]]:
    """Compute one stable all-or-nothing Arrow batch partial."""
    return _arrow_group_batch_totals_impl(key_values, sum_values, row_count, pa, pc)


def _try_arrow_file_group_sum(
    node: GroupAggregatePhysicalNode,
    descriptor: ArrowBatchSource,
    pa: Any,
    pc: Any,
) -> list[dict[str, Any]] | None:
    """Aggregate direct CSV or Parquet fields through live owner seams."""
    return _try_arrow_file_group_sum_impl(
        node,
        descriptor,
        pa,
        pc,
        _arrow_group_hooks(),
    )


def _materialize_arrow_group_rows(
    grouped: Any,
    grouped_names: tuple[str, ...],
    aggregate_fields: list[str],
    encounter_order: Any,
    spec: ArrowGroupAggregateSpec,
    key_name: str,
    wide_sums: list[bool],
    pa: Any,
    types: Any,
    pc: Any,
) -> list[dict[str, Any]] | None:
    """Restore first-seen order through live owner-module reshape seams."""
    return _materialize_arrow_group_rows_impl(
        grouped,
        grouped_names,
        aggregate_fields,
        encounter_order,
        spec,
        key_name,
        wide_sums,
        pa,
        types,
        pc,
        table_min_groups=_ARROW_GROUP_TABLE_MIN_GROUPS,
        take_min_groups=_ARROW_GROUP_TAKE_MIN_GROUPS,
        lane_arrays=_arrow_group_lane_arrays,
        lane_lists=_arrow_group_lane_lists,
        ordered_rows=_ordered_arrow_group_rows,
    )


def _try_arrow_retained_group_aggregate(
    table: Any,
    spec: ArrowGroupAggregateSpec,
    key_name: str,
    pa: Any,
    pc: Any,
) -> list[dict[str, Any]] | None:
    """Run closed int64 lanes over one retained Arrow table."""
    return _try_arrow_retained_group_aggregate_impl(
        table,
        spec,
        key_name,
        pa,
        pc,
        _arrow_group_hooks(),
    )


def _try_arrow_group_sum(
    node: GroupAggregatePhysicalNode,
) -> list[dict[str, Any]] | None:
    """Dispatch one guarded Arrow group algorithm without row boxing."""
    from ...runtime.failpoints import has_active_failpoints

    if node.arrow_i64_sum is None or has_active_failpoints():
        return None
    return _try_arrow_group_sum_impl(node, _arrow_group_hooks())


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


def _pair_i64_expr_environment_is_pristine() -> bool:
    """Keep failpoints and concurrent mutation authoritative over native execution."""
    return not (_has_active_failpoints() or _native_execution_unsafe() or _group_hash_replaced())


def _try_native_pair_i64_expr_group_sum(
    native_module: Any,
    node: GroupAggregatePhysicalNode,
    source: list[Any] | tuple[Any, ...],
    spec: NativePairI64ExprGroupSumSpec,
) -> list[dict[str, Any]] | None:
    """Revalidate two RowExpr programs, then dispatch one whole retained scan."""
    simple_sum = node.simple_sum
    if (
        type(simple_sum) is not SimpleGroupSumSpec
        or type(spec.output_name) is not str
        or simple_sum.output_name is not spec.output_name
        or len(node.keys) != 1
        or node.keys[0] is not simple_sum.key_selector
        or node.select_key is not simple_sum.key_selector
        or simple_sum.select_value is not simple_sum.value_selector
        or not _pair_i64_expr_environment_is_pristine()
    ):
        return None
    key_program = lower_pair_i64_group_key(simple_sum.key_selector)
    value_program = lower_pair_i64_group_value(simple_sum.value_selector)
    if key_program != spec.key_program or value_program != spec.value_program:
        return None
    raw_kernel = _BUILTIN_GETATTR(native_module, "group_sum_i64_pair_expr_rows_v1", None)
    if not _BUILTIN_CALLABLE(raw_kernel):
        return None
    kernel = cast(Callable[..., tuple[bool, object] | None], raw_kernel)
    key_name = node.key_names[0]
    if _BUILTIN_TYPE(key_name) is not str:
        return None
    rows_result = kernel(
        source,
        key_program,
        value_program,
        key_name,
        spec.output_name,
    )
    if rows_result is None:
        return None
    is_final_rows, payload = rows_result
    if is_final_rows:
        return cast(list[dict[str, Any]], payload)
    groups = cast(list[tuple[int, int]], payload)
    return [{key_name: key, spec.output_name: total} for key, total in groups]


def _retained_pair_i64_expr_group_source(
    node: GroupAggregatePhysicalNode,
) -> list[Any] | tuple[Any, ...] | None:
    """Recover the exact retained source only while every source proof remains live."""
    if (
        node.engine != "auto"
        or node.parallel is not None
        or node.partitions is not None
        or type(node.input) is not SourcePhysicalNode
    ):
        return None
    source_owner = node.input.source
    if (
        type(source_owner) is not Source
        or not source_owner.capabilities.reiterable
        or not source_owner.capabilities.ordered
        or Source.__dict__.get("open") is not _CANONICAL_SOURCE_OPEN
        or _CANONICAL_SOURCE_OPEN.__code__ is not _CANONICAL_SOURCE_OPEN_CODE
        or Source.__dict__.get("_claim") is not _CANONICAL_SOURCE_CLAIM
        or _CANONICAL_SOURCE_CLAIM.__code__ is not _CANONICAL_SOURCE_CLAIM_CODE
        or Source.__dict__.get("retained_sequence") is not _CANONICAL_RETAINED_SEQUENCE
        or Source.__dict__.get("native_data") is not _CANONICAL_SOURCE_NATIVE_DATA
    ):
        return None
    retained = source_owner.retained_sequence()
    if type(retained) not in (list, tuple) or retained is not source_owner.native_data:
        return None
    return cast(list[Any] | tuple[Any, ...], retained)


def _try_native_fixed_group(
    native_module: Any,
    source: object,
    spec: NativeFixedI64GroupSpec | NativeMultiI64GroupSpec,
    key_name: str,
) -> list[dict[str, Any]] | None:
    """Dispatch one exact fixed or generic lane shape through its optional ABI."""
    if isinstance(spec, NativeMultiI64GroupSpec):
        if _group_hash_replaced() or _native_execution_unsafe():
            return None
        if spec.row_kind == "tuple":
            isize_min = -_SYS_MAXSIZE - 1
            selectors = (spec.key_selector, *(lane.value_selector for lane in spec.lanes))
            if _BUILTIN_ANY(
                selector is not None
                and (
                    _BUILTIN_TYPE(selector) is not _BUILTIN_INT
                    or selector < isize_min
                    or selector > _SYS_MAXSIZE
                )
                for selector in selectors
            ):
                return None
        kernel_name = (
            "group_multi_i64_rows_v1"
            if spec.row_kind == "tuple"
            else "group_multi_i64_dict_rows_v1"
        )
        kernel = _BUILTIN_GETATTR(native_module, kernel_name, None)
        if not _BUILTIN_CALLABLE(kernel):
            return None
        lanes: list[tuple[int, int | str | None, str]] = []
        for lane in spec.lanes:
            encoded = _encode_native_multi_group_lane(
                lane.kind,
                lane.value_selector,
                lane.output_name,
            )
            if encoded is None:
                return None
            lanes.append(encoded)
        return cast(
            "list[dict[str, Any]] | None",
            kernel(source, spec.key_selector, key_name, _BUILTIN_TUPLE(lanes)),
        )

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


def _try_native_group_sum(  # noqa: C901 - guarded source and kernel dispatch
    node: GroupAggregatePhysicalNode,
) -> list[dict[str, Any]] | None:
    """Run a guarded source-container scan or leave the source unopened for fallback."""
    from ...runtime.failpoints import has_active_failpoints

    # Native kernels deliberately skip Python transition hooks. Instrumented
    # runs must therefore enter the ordinary source/open execution path.
    if has_active_failpoints():
        return None
    fixed_spec = node.native_fixed_i64_group
    pair_expr_spec = node.native_pair_i64_expr_sum
    tuple_spec = node.native_i64_sum
    record_spec = node.native_record_i64_sum
    if not _BUILTIN_ANY(
        spec is not None for spec in (fixed_spec, pair_expr_spec, tuple_spec, record_spec)
    ) or not isinstance(node.input, SourcePhysicalNode):
        return None
    if pair_expr_spec is not None:
        source = _retained_pair_i64_expr_group_source(node)
        if source is None:
            return None
    else:
        source = node.input.source.native_data
    exact_container = type(source) is list or type(source) is tuple
    fixed_eligible = fixed_spec is not None and exact_container
    pair_expr_eligible = pair_expr_spec is not None and exact_container
    tuple_eligible = tuple_spec is not None and exact_container
    record_eligible = record_spec is not None and exact_container
    fixed_record_eligible = (
        fixed_eligible and fixed_spec is not None and fixed_spec.row_kind == "dict"
    )
    if not pair_expr_eligible and not tuple_eligible and not record_eligible and not fixed_eligible:
        return None

    try:
        from ... import _native
    except ImportError:
        # The Rust extension is optional. Source data has not been opened yet,
        # so declining this speculative fast path preserves a clean Python run.
        return None

    if pair_expr_eligible:
        assert pair_expr_spec is not None
        return _try_native_pair_i64_expr_group_sum(
            _native,
            node,
            cast(list[Any] | tuple[Any, ...], source),
            pair_expr_spec,
        )

    key_name = node.key_names[0]
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
