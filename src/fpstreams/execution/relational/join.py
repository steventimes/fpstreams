"""Canonical Python execution for physical record joins."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from types import MappingProxyType
from typing import Any, cast

from ...errors import DuplicateKeyError
from ...expressions.selectors import _direct_field
from ...physical.plan import PhysicalPlan
from ...physical.relational import (
    JoinPhysicalNode,
    JoinStrategy,
    PhysicalRelNode,
    SourcePhysicalNode,
)
from ...planning.arrow_source import ArrowBatchSource
from ...runtime.iterators import closing_iterators
from ...runtime.query import QueryRuntime
from ...tabular.join import (
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
from ...tabular.records import _as_record, _remember_columns
from ...tabular.spill import spilled_join
from ...tabular.spill_limits import SpillLimits

_ARROW_UNIQUE_JOIN_MIN_ROWS = 128
_EXPECTED_ARROW_JOIN_ERRORS = (
    ArithmeticError,
    NotImplementedError,
    TypeError,
    ValueError,
)

RelationalExecutor = Callable[[PhysicalRelNode, QueryRuntime], Iterator[Any]]


def _execute_join(
    node: JoinPhysicalNode,
    runtime: QueryRuntime,
    execute_relational: RelationalExecutor,
) -> Iterator[dict[str, Any]]:
    """Execute the selected stable hash-compatible join strategy."""
    left = execute_relational(node.left, runtime)
    right = execute_relational(node.right, runtime)
    spec = node.spec
    logical = spec.logical
    left_field = _direct_field(spec.left_key)
    right_field = _direct_field(spec.right_key)
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
        from ...runtime.failpoints import has_active_failpoints

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
    with closing_iterators((right_iterator,)):
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

    seen_left: set[Any] | None = set() if validate == "1:1" else None
    target_cache = _JoinTargetCache()
    cached_mapping_type = None
    cached_mapping_mro = None
    left_iterator = iter(left_source)
    with closing_iterators((left_iterator,)):
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


def _try_retained_arrow_unique_join(
    plan: PhysicalPlan,
    import_module: Callable[[str], Any],
) -> list[dict[str, Any]] | None:
    """Materialize one guarded top-level retained Arrow m:1 join by column position."""
    from ...runtime.failpoints import has_active_failpoints, hit

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
