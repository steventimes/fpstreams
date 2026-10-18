"""Guarded native grouped aggregation and optional wheel ABI compatibility."""

from __future__ import annotations

import sys as _sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

from ...collecting._collector_base import Collector
from ...collecting.aggregation import (
    Aggregator,
    native_group_aggregation,
    project_count_aggregation,
)
from ...collecting.program import (
    collector_program_fast_path_is_live,
)
from ...expressions.selectors import _normalize_direct_row_selector
from ...physical.relational import (
    CompositeCountSumSpec,
    GroupAggregatePhysicalNode,
    NativeFixedI64GroupSpec,
    NativeGroupSumSpec,
    NativeMultiI64GroupSpec,
    NativePairI64ExprGroupSumSpec,
    NativeRecordGroupSumSpec,
    SimpleGroupSumSpec,
    SourcePhysicalNode,
)
from ...planning.pair_i64_expression import (
    lower_pair_i64_group_key,
    lower_pair_i64_group_value,
)


@dataclass(frozen=True)
class NativeGroupHooks:
    """Live owner proofs sampled at the existing native dispatch boundary."""

    hash_replaced: Callable[[], bool]
    execution_unsafe: Callable[[], bool]
    retained_source: Callable[[GroupAggregatePhysicalNode], list[Any] | tuple[Any, ...] | None]
    collector_lifecycle_is_static: Callable[[Collector[Any, Any, Any]], bool]
    composite_keys_are_live: Callable[[GroupAggregatePhysicalNode, CompositeCountSumSpec], bool]
    pair_environment_is_pristine: Callable[[], bool]


_SYS_MAXSIZE = _sys.maxsize
_BUILTIN_ANY = any
_BUILTIN_CALLABLE = callable
_BUILTIN_GETATTR = getattr
_BUILTIN_INT = int
_BUILTIN_TUPLE: Any = tuple
_BUILTIN_TYPE = type


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


def _try_native_pair_i64_expr_group_sum(
    native_module: Any,
    node: GroupAggregatePhysicalNode,
    source: list[Any] | tuple[Any, ...],
    spec: NativePairI64ExprGroupSumSpec,
    hooks: NativeGroupHooks,
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
        or not hooks.pair_environment_is_pristine()
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


def _try_native_composite_group(
    native_module: Any,
    node: GroupAggregatePhysicalNode,
    source: list[Any] | tuple[Any, ...],
    spec: CompositeCountSumSpec,
    hooks: NativeGroupHooks,
) -> list[dict[str, Any]] | None:
    """Run the two-key ABI only for live count/sum factories and index selectors."""
    if (
        _BUILTIN_TYPE(spec) is not CompositeCountSumSpec
        or _BUILTIN_TYPE(spec.key_selectors) is not _BUILTIN_TUPLE
        or hooks.hash_replaced()
        or hooks.execution_unsafe()
    ):
        return None
    indices = (*spec.key_selectors, spec.value_selector)
    if len(indices) != 3 or any(
        _BUILTIN_TYPE(index) is not _BUILTIN_INT or not -_SYS_MAXSIZE - 1 <= index <= _SYS_MAXSIZE
        for index in indices
    ):
        return None
    program = node.aggregations.collectors
    collectors = program.layout.collectors
    if (
        len(node.key_names) != 2
        or len(collectors) != 2
        or not collector_program_fast_path_is_live(program)
        or not all(hooks.collector_lifecycle_is_static(collector) for collector in collectors)
        or program.layout.names != (spec.count_name, spec.sum_name)
        or not project_count_aggregation(cast(Aggregator, collectors[0]))
        or not hooks.composite_keys_are_live(node, spec)
    ):
        return None
    native_sum = native_group_aggregation(cast(Aggregator, collectors[1]))
    if native_sum is None or native_sum.kind != "sum" or native_sum.selector is None:
        return None
    value_selector = _normalize_direct_row_selector(native_sum.selector)
    if _BUILTIN_TYPE(value_selector) is not _BUILTIN_INT or value_selector != spec.value_selector:
        return None
    kernel = _BUILTIN_GETATTR(native_module, "group_count_sum_i64_two_key_rows_v1", None)
    if not _BUILTIN_CALLABLE(kernel):
        return None
    return cast(
        list[dict[str, Any]] | None,
        kernel(source, indices, (*node.key_names, spec.count_name, spec.sum_name)),
    )


def _try_native_fixed_group(
    native_module: Any,
    source: object,
    spec: NativeFixedI64GroupSpec | NativeMultiI64GroupSpec,
    key_name: str,
    hooks: NativeGroupHooks,
) -> list[dict[str, Any]] | None:
    """Dispatch one exact fixed or generic lane shape through its optional ABI."""
    if isinstance(spec, NativeMultiI64GroupSpec):
        if hooks.hash_replaced() or hooks.execution_unsafe():
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
    hooks: NativeGroupHooks,
) -> list[dict[str, Any]] | None:
    """Run a guarded source-container scan or leave the source unopened for fallback."""
    from ...runtime.failpoints import has_active_failpoints

    # Native kernels deliberately skip Python transition hooks. Instrumented
    # runs must therefore enter the ordinary source/open execution path.
    if has_active_failpoints():
        return None
    fixed_spec = node.native_fixed_i64_group
    composite_spec = node.composite_count_sum
    pair_expr_spec = node.native_pair_i64_expr_sum
    tuple_spec = node.native_i64_sum
    record_spec = node.native_record_i64_sum
    if not _BUILTIN_ANY(
        spec is not None
        for spec in (fixed_spec, composite_spec, pair_expr_spec, tuple_spec, record_spec)
    ) or not isinstance(node.input, SourcePhysicalNode):
        return None
    if pair_expr_spec is not None or composite_spec is not None:
        source = hooks.retained_source(node)
        if source is None:
            return None
    else:
        source = node.input.source.native_data
    exact_container = type(source) is list or type(source) is tuple
    composite_eligible = composite_spec is not None and exact_container
    fixed_eligible = fixed_spec is not None and exact_container
    pair_expr_eligible = pair_expr_spec is not None and exact_container
    tuple_eligible = tuple_spec is not None and exact_container
    record_eligible = record_spec is not None and exact_container
    fixed_record_eligible = (
        fixed_eligible and fixed_spec is not None and fixed_spec.row_kind == "dict"
    )
    if not any(
        (composite_eligible, pair_expr_eligible, tuple_eligible, record_eligible, fixed_eligible)
    ):
        return None

    try:
        from ... import _native
    except ImportError:
        # The Rust extension is optional. Source data has not been opened yet,
        # so declining this speculative fast path preserves a clean Python run.
        return None

    if composite_eligible:
        assert composite_spec is not None
        return _try_native_composite_group(_native, node, source, composite_spec, hooks)

    if pair_expr_eligible:
        assert pair_expr_spec is not None
        return _try_native_pair_i64_expr_group_sum(
            _native,
            node,
            cast(list[Any] | tuple[Any, ...], source),
            pair_expr_spec,
            hooks,
        )

    key_name = node.key_names[0]
    if fixed_eligible and fixed_spec is not None and fixed_spec.row_kind == "tuple":
        return _try_native_fixed_group(_native, source, fixed_spec, key_name, hooks)
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
        return _try_native_fixed_group(_native, source, fixed_spec, key_name, hooks)
    assert record_spec is not None
    return _try_native_record_group_sum(_native, source, record_spec, key_name)
