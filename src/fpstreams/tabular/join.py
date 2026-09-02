"""Join planning and in-memory execution for record pipelines."""

from __future__ import annotations

from abc import ABCMeta
from collections.abc import Callable, Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Any, Literal, NoReturn, TypeAlias, cast

from ..errors import SelectionError
from ..expressions.selectors import (
    Selector,
    _normalize_direct_row_selector,
    compile_selector,
)
from ..runtime.iterators import close_iterators
from .records import _as_record, _remember_columns

JoinSelector: TypeAlias = Selector | tuple[Selector, ...]
JoinValidation: TypeAlias = Literal["m:m", "1:1", "1:m", "m:1"]
_JoinRecordBucket: TypeAlias = dict[str, Any] | list[dict[str, Any]]
_JoinTargetPlan: TypeAlias = tuple[tuple[str, str | None], ...]
_FixedJoinTargets: TypeAlias = tuple[tuple[str, str], ...]
_JoinCachedLayout: TypeAlias = tuple[_JoinTargetPlan, _FixedJoinTargets | None]

_JOIN_MODES = frozenset({"inner", "left", "right", "full", "semi", "anti"})
_JOIN_VALIDATIONS = frozenset({"m:m", "1:1", "1:m", "m:1"})
_MAX_JOIN_TARGET_SHAPES = 64


def _same_name_objects(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    """Compare a common short schema by identity without user equality callbacks."""
    size = len(left)
    if size != len(right):
        return False
    if size == 0:
        return True
    if size == 1:
        return left[0] is right[0]
    if size == 2:
        return left[0] is right[0] and left[1] is right[1]
    if size == 3:
        return left[0] is right[0] and left[1] is right[1] and left[2] is right[2]
    if size == 4:
        return (
            left[0] is right[0]
            and left[1] is right[1]
            and left[2] is right[2]
            and left[3] is right[3]
        )
    return all(current is previous for current, previous in zip(left, right, strict=True))


class _JoinTargetCache:
    """Bound repeated safe field layouts without retaining unbounded row shapes.

    One instance belongs to one join, whose right schema, shared names, and suffix stay fixed.
    Cached plans may contain only original right-column strings. A generated suffix string
    remains per-row work so its identity and collision behavior stay exactly canonical. An
    unsafe layout or a 65th safe shape disables the cache for the rest of the join.
    """

    __slots__ = (
        "_last_fixed_targets",
        "_last_plan",
        "_last_shape",
        "_layouts",
        "enabled",
    )

    def __init__(self) -> None:
        self.enabled = True
        self._last_shape: tuple[str, ...] | None = None
        self._last_plan: _JoinTargetPlan = ()
        self._last_fixed_targets: _FixedJoinTargets | None = ()
        self._layouts: dict[tuple[str, ...], _JoinCachedLayout] = {}

    def _disable(self) -> None:
        """Release retained shapes and make later rows use the canonical path directly."""
        self.enabled = False
        self._last_shape = None
        self._last_plan = ()
        self._last_fixed_targets = None
        self._layouts.clear()

    @staticmethod
    def _targets(plan: _JoinTargetPlan, suffix: str) -> tuple[tuple[str, str], ...]:
        """Mint only generated suffix keys, expanding the common narrow layouts."""
        size = len(plan)
        if size == 0:
            return ()
        if size == 1:
            name0, target0 = plan[0]
            return ((name0, target0 if target0 is not None else f"{name0}{suffix}"),)
        if size == 2:
            name0, target0 = plan[0]
            name1, target1 = plan[1]
            return (
                (name0, target0 if target0 is not None else f"{name0}{suffix}"),
                (name1, target1 if target1 is not None else f"{name1}{suffix}"),
            )
        if size == 3:
            name0, target0 = plan[0]
            name1, target1 = plan[1]
            name2, target2 = plan[2]
            return (
                (name0, target0 if target0 is not None else f"{name0}{suffix}"),
                (name1, target1 if target1 is not None else f"{name1}{suffix}"),
                (name2, target2 if target2 is not None else f"{name2}{suffix}"),
            )
        return tuple(
            (name, target if target is not None else f"{name}{suffix}") for name, target in plan
        )

    def target_plan(
        self,
        left_names: dict[str, Any],
        right_names: Iterable[str],
        *,
        shared_names: set[str],
        suffix: str,
    ) -> _JoinTargetPlan | None:
        """Return a safe cached plan without expanding its fixed targets per row.

        ``None`` asks the caller to use ``_join_targets`` canonically. Only exact strings enter
        this path, so deferring suffix-string construction until merge cannot invoke user code.
        """
        if not self.enabled:
            return None
        shape = tuple(left_names)
        if self._last_shape is not None and _same_name_objects(shape, self._last_shape):
            return self._last_plan
        if type(suffix) is not str or not all(type(name) is str for name in shape):
            self._disable()
            return None

        cached_layout = self._layouts.get(shape)
        if cached_layout is not None:
            cached_plan, fixed_targets = cached_layout
            self._last_shape = shape
            self._last_plan = cached_plan
            self._last_fixed_targets = fixed_targets
            return cached_plan

        right_shape = tuple(right_names)
        if not all(type(name) is str for name in shared_names) or not all(
            type(name) is str for name in right_shape
        ):
            self._disable()
            return None
        targets = _join_targets(
            shape,
            right_shape,
            shared_names=shared_names,
            suffix=suffix,
        )
        plan: _JoinTargetPlan = tuple(
            (name, None if name is not target else target) for name, target in targets
        )
        fixed_targets = (
            cast(_FixedJoinTargets, plan)
            if all(target is not None for _name, target in plan)
            else None
        )
        if len(self._layouts) == _MAX_JOIN_TARGET_SHAPES:
            self._disable()
        else:
            self._layouts[shape] = (plan, fixed_targets)
            self._last_shape = shape
            self._last_plan = plan
            self._last_fixed_targets = fixed_targets
        return plan


def _close_iterator(
    iterator: Iterator[Any],
    *,
    active_error: BaseException | None = None,
) -> None:
    """Close one source without hiding an active join failure."""
    close_iterators((iterator,), active_error=active_error)


def _direct_mapping_mro(row_type: type[Any]) -> tuple[type[Any], ...] | None:
    """Recognize a nominal Mapping class whose repeated ABC check may be cached.

    Under the standard ABC implementation, exact ``ABCMeta`` plus ``Mapping`` in the MRO
    proves nominal (possibly indirect) inheritance. Virtual registrations and custom
    metaclasses stay on ``_as_record``; retaining the exact MRO object lets each hot loop
    notice a later ``__bases__`` mutation. Replacing standard-library ABC hooks at runtime is
    outside this optimization's stable-type contract.
    """
    if type(row_type) is not ABCMeta:
        return None
    mro = row_type.__mro__
    return mro if Mapping in mro else None


def _select_direct_mapping_field(row: Any, field: str) -> Any:
    """Read a proven Mapping field with the canonical selector error boundary."""
    try:
        return row[field]
    except (AttributeError, KeyError, TypeError) as error:
        raise SelectionError(
            f"Could not resolve selector {field!r}; failed at {field!r}"
        ) from error


def _raise_direct_composite_selector_error(
    selector: str | int,
    row: Any,
    error: AttributeError | IndexError | KeyError | TypeError,
) -> NoReturn:
    """Translate one exact-dict lookup with the canonical selector boundary."""
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


def _compose_composite_selector(
    selector: tuple[Any, ...],
    selectors: tuple[Callable[[Any], Any], ...],
) -> Callable[[Any], tuple[Any, ...]]:
    """Compose precompiled parts, specializing only exact direct pairs and triples."""
    direct = (
        type(selector) is tuple
        and len(selector) in {2, 3}
        and all(type(part) is int or (type(part) is str and "." not in part) for part in selector)
    )
    if direct and len(selector) == 2:
        first, second = cast(tuple[str | int, str | int], selector)

        def select_direct_pair(row: Any) -> tuple[Any, Any]:
            """Read the common exact-dict pair without component callable dispatch."""
            if type(row) is dict:
                try:
                    first_value = row[first]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_direct_composite_selector_error(first, row, error)
                try:
                    second_value = row[second]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_direct_composite_selector_error(second, row, error)
                return first_value, second_value
            return tuple(select(row) for select in selectors)

        return select_direct_pair
    if direct:
        first, second, third = cast(tuple[str | int, str | int, str | int], selector)

        def select_direct_triple(row: Any) -> tuple[Any, Any, Any]:
            """Read the common exact-dict triple without component callable dispatch."""
            if type(row) is dict:
                try:
                    first_value = row[first]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_direct_composite_selector_error(first, row, error)
                try:
                    second_value = row[second]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_direct_composite_selector_error(second, row, error)
                try:
                    third_value = row[third]
                except (AttributeError, IndexError, KeyError, TypeError) as error:
                    _raise_direct_composite_selector_error(third, row, error)
                return first_value, second_value, third_value
            return tuple(select(row) for select in selectors)

        return select_direct_triple

    def select_composite_key(row: Any) -> tuple[Any, ...]:
        """Return the ordered values that form a composite join key."""
        return tuple(select(row) for select in selectors)

    return select_composite_key


def _compile_join_selector(selector: JoinSelector) -> Callable[[Any], Any]:
    """Compile one selector or a tuple of selectors into a join-key function."""
    if not isinstance(selector, tuple):
        return compile_selector(_normalize_direct_row_selector(selector))
    if not selector:
        raise ValueError("composite join keys cannot be empty")
    normalized = tuple(_normalize_direct_row_selector(part) for part in selector)
    selectors = tuple(compile_selector(part) for part in normalized)
    shape = normalized if type(selector) is tuple else selector
    return _compose_composite_selector(shape, selectors)


def _normalize_join_selectors(
    *,
    on: JoinSelector | None,
    left_on: JoinSelector | None,
    right_on: JoinSelector | None,
) -> tuple[JoinSelector, JoinSelector]:
    """Resolve shared and side-specific selectors into a left/right pair."""
    if on is not None:
        if left_on is not None or right_on is not None:
            raise ValueError("use either on or left_on/right_on")
        left_on = right_on = on
    elif left_on is None or right_on is None:
        raise ValueError("join requires on or both left_on and right_on")

    if isinstance(left_on, tuple) and isinstance(right_on, tuple) and len(left_on) != len(right_on):
        raise ValueError("left_on and right_on must contain the same number of keys")
    return left_on, right_on


def _shared_join_names(
    left_on: JoinSelector,
    right_on: JoinSelector,
) -> set[str]:
    """Return same-name scalar fields that should appear only once in output."""
    if isinstance(left_on, str) and left_on == right_on and "." not in left_on:
        return {left_on}
    if isinstance(left_on, tuple) and isinstance(right_on, tuple):
        return {
            left
            for left, right in zip(left_on, right_on, strict=True)
            if isinstance(left, str) and left == right and "." not in left
        }
    return set()


def _requires_unique_left(validate: str) -> bool:
    """Return whether a validation contract requires unique left keys."""
    return validate in {"1:1", "1:m"}


def _requires_unique_right(validate: str) -> bool:
    """Return whether a validation contract requires unique right keys."""
    return validate in {"1:1", "m:1"}


def _check_unique_key(
    seen: set[Any],
    key: Any,
    *,
    validate: str,
    side: Literal["left", "right"],
) -> None:
    """Record a key or raise when it violates the requested cardinality."""
    if key in seen:
        raise ValueError(
            f"join validate={validate!r} requires unique {side} keys; found duplicate {key!r}"
        )
    seen.add(key)


def _join_key_set(
    source: Iterable[Any],
    select: Callable[[Any], Any],
    *,
    validate: str,
) -> set[Any]:
    """Index only right-side keys for a semi or anti join."""
    keys: set[Any] = set()
    unique = _requires_unique_right(validate)
    cached_mapping_type: type[Any] | None = None
    cached_mapping_mro: tuple[type[Any], ...] | None = None
    iterator = iter(source)
    active_error: BaseException | None = None
    try:
        for row in iterator:
            row_type = type(row)
            if row_type is MappingProxyType:
                dict(row)
            elif row_type is not dict:
                if row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro:
                    dict(row)
                else:
                    _as_record(row)
                    mapping_mro = _direct_mapping_mro(row_type)
                    if mapping_mro is not None:
                        cached_mapping_type = row_type
                        cached_mapping_mro = mapping_mro
            key = select(row)
            if unique and key in keys:
                raise ValueError(
                    f"join validate={validate!r} requires unique right keys; "
                    f"found duplicate {key!r}"
                )
            keys.add(key)
    except BaseException as error:
        active_error = error
        raise
    finally:
        _close_iterator(iterator, active_error=active_error)
    return keys


def _join_record_index(
    source: Iterable[Any],
    select: Callable[[Any], Any],
    *,
    validate: str,
    direct_field: str | None = None,
) -> tuple[
    tuple[str, ...],
    dict[Any, int],
    list[_JoinRecordBucket],
]:
    """Build a stable key-to-slot index and promote repeated slots in place.

    Dictionary values are integer positions, so a repeated key can replace its
    slot without assigning the user key to the index again. This preserves the
    canonical hash/equality callback trace while avoiding both a list allocation
    for every unique key and a cardinality-sensitive whole-index migration.
    """
    from ..runtime.failpoints import has_active_failpoints, hit

    instrumented = has_active_failpoints()
    columns: list[str] = []
    seen_columns: set[str] = set()
    index: dict[Any, int] = {}
    slots: list[_JoinRecordBucket] = []
    unique = _requires_unique_right(validate)
    cached_mapping_type: type[Any] | None = None
    cached_mapping_mro: tuple[type[Any], ...] | None = None
    iterator = iter(source)
    active_error: BaseException | None = None
    try:
        for row in iterator:
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
                _select_direct_mapping_field(row, direct_field)
                if direct_field is not None and direct_mapping
                else select(row)
            )
            _remember_columns(record, columns, seen_columns)
            position = index.get(key)
            if position is None:
                index[key] = len(slots)
                slots.append(record)
                if instrumented:
                    hit("join.build.insert.after")
            elif unique:
                raise ValueError(
                    f"join validate={validate!r} requires unique right keys; "
                    f"found duplicate {key!r}"
                )
            else:
                bucket = slots[position]
                if isinstance(bucket, list):
                    bucket.append(record)
                else:
                    slots[position] = [bucket, record]
                if instrumented:
                    hit("join.build.insert.after")
    except BaseException as error:
        active_error = error
        raise
    finally:
        _close_iterator(iterator, active_error=active_error)

    return tuple(columns), index, slots


def _join_position_index(
    source: Iterable[Any],
    select: Callable[[Any], Any],
    *,
    validate: str,
) -> tuple[list[dict[str, Any]], tuple[str, ...], dict[Any, list[int]]]:
    """Materialize right rows and map each key to stable row positions."""
    records: list[dict[str, Any]] = []
    columns: list[str] = []
    seen_columns: set[str] = set()
    index: dict[Any, list[int]] = {}
    unique = _requires_unique_right(validate)
    cached_mapping_type: type[Any] | None = None
    cached_mapping_mro: tuple[type[Any], ...] | None = None
    iterator = iter(source)
    active_error: BaseException | None = None
    try:
        for row in iterator:
            row_type = type(row)
            if row_type is dict:
                record = row.copy()
            elif row_type is MappingProxyType or (
                row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro
            ):
                record = dict(row)
            else:
                record = _as_record(row)
                mapping_mro = _direct_mapping_mro(row_type)
                if mapping_mro is not None:
                    cached_mapping_type = row_type
                    cached_mapping_mro = mapping_mro
            key = select(row)
            position = len(records)
            records.append(record)
            _remember_columns(record, columns, seen_columns)
            positions = index.get(key)
            if positions is None:
                index[key] = [position]
            elif unique:
                raise ValueError(
                    f"join validate={validate!r} requires unique right keys; "
                    f"found duplicate {key!r}"
                )
            else:
                positions.append(position)
    except BaseException as error:
        active_error = error
        raise
    finally:
        _close_iterator(iterator, active_error=active_error)
    return records, tuple(columns), index


def _materialize_join_rows(
    source: Iterable[Any],
    select: Callable[[Any], Any],
    *,
    validate: str,
) -> tuple[list[tuple[dict[str, Any], Any]], tuple[str, ...]]:
    """Materialize left rows for right/full joins and optionally validate keys."""
    records: list[tuple[dict[str, Any], Any]] = []
    columns: list[str] = []
    seen_columns: set[str] = set()
    seen_keys: set[Any] | None = set() if _requires_unique_left(validate) else None
    cached_mapping_type: type[Any] | None = None
    cached_mapping_mro: tuple[type[Any], ...] | None = None
    iterator = iter(source)
    active_error: BaseException | None = None
    try:
        for row in iterator:
            row_type = type(row)
            if row_type is dict:
                record = row.copy()
            elif row_type is MappingProxyType or (
                row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro
            ):
                record = dict(row)
            else:
                record = _as_record(row)
                mapping_mro = _direct_mapping_mro(row_type)
                if mapping_mro is not None:
                    cached_mapping_type = row_type
                    cached_mapping_mro = mapping_mro
            key = select(row)
            if seen_keys is not None:
                _check_unique_key(seen_keys, key, validate=validate, side="left")
            records.append((record, key))
            _remember_columns(record, columns, seen_columns)
    except BaseException as error:
        active_error = error
        raise
    finally:
        _close_iterator(iterator, active_error=active_error)
    return records, tuple(columns)


def _merge_join_records(
    left: Mapping[str, Any],
    right: dict[str, Any],
    targets: tuple[tuple[str, str], ...],
    shared_names: set[str],
) -> dict[str, Any]:
    """Merge a matched pair without overwriting shared join fields."""
    merged = dict(left)
    for name, target in targets:
        if name in shared_names and target in merged:
            continue
        if name in right:
            merged[target] = right[name]
    return merged


def _merge_join_snapshot(
    left: dict[str, Any],
    right: dict[str, Any],
    targets: tuple[tuple[str, str], ...],
    shared_names: set[str],
) -> dict[str, Any]:
    """Merge into an executor-owned left snapshot when it has one output owner."""
    for name, target in targets:
        if name in shared_names and target in left:
            continue
        if name in right:
            left[target] = right[name]
    return left


def _merge_join_plan_snapshot(
    left: dict[str, Any],
    right: dict[str, Any],
    plan: _JoinTargetPlan,
    shared_names: set[str],
    suffix: str,
) -> dict[str, Any]:
    """Apply a safe cached plan directly to one executor-owned left snapshot."""
    if shared_names:
        for name, target in plan:
            resolved = target if target is not None else f"{name}{suffix}"
            if name in shared_names and resolved in left:
                continue
            if name in right:
                left[resolved] = right[name]
        return left
    for name, target in plan:
        resolved = target if target is not None else f"{name}{suffix}"
        if name in right:
            left[resolved] = right[name]
    return left


def _fill_unmatched_join_plan(
    left: dict[str, Any],
    plan: _JoinTargetPlan,
    shared_names: set[str],
    suffix: str,
) -> dict[str, Any]:
    """Fill absent right fields from a safe plan without constructing target pairs."""
    if shared_names:
        for name, target in plan:
            if name not in shared_names:
                left[target if target is not None else f"{name}{suffix}"] = None
        return left
    for name, target in plan:
        left[target if target is not None else f"{name}{suffix}"] = None
    return left


def _join_targets(
    left_names: Iterable[str],
    right_names: Iterable[str],
    *,
    shared_names: set[str],
    suffix: str,
) -> tuple[tuple[str, str], ...]:
    """Map right-side fields to collision-free output field names."""
    left = set(left_names)
    used = set(left)
    targets: list[tuple[str, str]] = []
    for name in right_names:
        if name in shared_names:
            targets.append((name, name))
            continue
        target = f"{name}{suffix}" if name in left else name
        if target in used:
            from ..errors import DuplicateKeyError

            raise DuplicateKeyError(
                f"join maps right column {name!r} to existing output column {target!r}"
            )
        used.add(target)
        targets.append((name, target))
    return tuple(targets)


def _semi_or_anti_join(
    left_source: Iterable[Any],
    right_source: Iterable[Any],
    *,
    left_key: Callable[[Any], Any],
    right_key: Callable[[Any], Any],
    how: str,
    validate: str,
) -> Iterator[dict[str, Any]]:
    """Execute a key-only semi or anti join while streaming the left side."""
    keys = _join_key_set(right_source, right_key, validate=validate)
    seen_left: set[Any] | None = set() if _requires_unique_left(validate) else None
    cached_mapping_type: type[Any] | None = None
    cached_mapping_mro: tuple[type[Any], ...] | None = None
    iterator = iter(left_source)
    active_error: BaseException | None = None
    try:
        for row in iterator:
            # Snapshot before selecting the key. Even an exact-dict lookup can
            # invoke ``__eq__`` on a colliding user key and mutate the source.
            row_type = type(row)
            if row_type is dict:
                left = row.copy()
            elif row_type is MappingProxyType or (
                row_type is cached_mapping_type and row_type.__mro__ is cached_mapping_mro
            ):
                left = dict(row)
            else:
                left = _as_record(row)
                mapping_mro = _direct_mapping_mro(row_type)
                if mapping_mro is not None:
                    cached_mapping_type = row_type
                    cached_mapping_mro = mapping_mro
            key = left_key(row)
            if seen_left is not None:
                _check_unique_key(seen_left, key, validate=validate, side="left")
            matched = key in keys
            if matched == (how == "semi"):
                # _as_record already made the pre-selector snapshot owned by
                # this single output, so another dictionary copy is redundant.
                yield left
    except BaseException as error:
        active_error = error
        raise
    finally:
        _close_iterator(iterator, active_error=active_error)


def execute_left_join(  # noqa: C901 - keep guarded record snapshots inline in this hot loop
    left_source: Iterable[Any],
    right_source: Iterable[Any],
    *,
    left_key: Callable[[Any], Any],
    right_key: Callable[[Any], Any],
    how: str,
    shared_names: set[str],
    suffix: str,
    validate: str,
    left_field: str | None = None,
    right_field: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Execute an inner/left join using a right index and a streaming left side."""
    from ..runtime.failpoints import has_active_failpoints, hit

    instrumented = has_active_failpoints()
    right_columns, record_index, record_slots = _join_record_index(
        right_source,
        right_key,
        validate=validate,
        direct_field=right_field,
    )
    target_cache = _JoinTargetCache()
    seen_left: set[Any] | None = set() if _requires_unique_left(validate) else None
    cached_mapping_type: type[Any] | None = None
    cached_mapping_mro: tuple[type[Any], ...] | None = None
    iterator = iter(left_source)
    active_error: BaseException | None = None
    try:
        for row in iterator:
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
            position = record_index.get(key)
            if position is not None:
                record_match = record_slots[position]
                plan = (
                    target_cache.target_plan(
                        left,
                        right_columns,
                        shared_names=shared_names,
                        suffix=suffix,
                    )
                    if target_cache.enabled
                    else None
                )
                targets = (
                    _join_targets(
                        left,
                        right_columns,
                        shared_names=shared_names,
                        suffix=suffix,
                    )
                    if plan is None
                    else None
                )
                if isinstance(record_match, list):
                    if targets is None:
                        assert plan is not None
                        targets = target_cache._last_fixed_targets
                        if targets is None:
                            targets = _JoinTargetCache._targets(plan, suffix)
                    for right in record_match:
                        if instrumented:
                            hit("join.probe.match.after")
                        # One left row can produce several live outputs here,
                        # so every result needs its own dictionary snapshot.
                        yield _merge_join_records(left, right, targets, shared_names)
                else:
                    if instrumented:
                        hit("join.probe.match.after")
                    if plan is None:
                        assert targets is not None
                        yield _merge_join_snapshot(left, record_match, targets, shared_names)
                    else:
                        yield _merge_join_plan_snapshot(
                            left,
                            record_match,
                            plan,
                            shared_names,
                            suffix,
                        )
            elif how == "left":
                plan = (
                    target_cache.target_plan(
                        left,
                        right_columns,
                        shared_names=shared_names,
                        suffix=suffix,
                    )
                    if target_cache.enabled
                    else None
                )
                targets = (
                    _join_targets(
                        left,
                        right_columns,
                        shared_names=shared_names,
                        suffix=suffix,
                    )
                    if plan is None
                    else None
                )
                merged = dict(left)
                if plan is None:
                    assert targets is not None
                    for name, target in targets:
                        if name not in shared_names:
                            merged[target] = None
                    yield merged
                else:
                    yield _fill_unmatched_join_plan(merged, plan, shared_names, suffix)
    except BaseException as error:
        active_error = error
        raise
    finally:
        _close_iterator(iterator, active_error=active_error)


def execute_right_or_full_join(
    left_source: Iterable[Any],
    right_source: Iterable[Any],
    *,
    left_key: Callable[[Any], Any],
    right_key: Callable[[Any], Any],
    how: str,
    shared_names: set[str],
    suffix: str,
    validate: str,
) -> Iterator[dict[str, Any]]:
    """Execute a stable right/full join after validating both materialized sides."""
    from ..runtime.failpoints import has_active_failpoints, hit

    instrumented = has_active_failpoints()
    right_records, right_columns, position_index = _join_position_index(
        right_source,
        right_key,
        validate=validate,
    )
    left_records, left_columns = _materialize_join_rows(
        left_source,
        left_key,
        validate=validate,
    )
    targets = _join_targets(
        left_columns,
        right_columns,
        shared_names=shared_names,
        suffix=suffix,
    )
    matched_right = bytearray(len(right_records))
    for left, key in left_records:
        position_matches = position_index.get(key, ())
        if position_matches:
            for right_position in position_matches:
                matched_right[right_position] = 1
                if instrumented:
                    hit("join.probe.match.after")
                yield _merge_join_records(
                    left,
                    right_records[right_position],
                    targets,
                    shared_names,
                )
        elif how == "full":
            merged = left.copy()
            for name, target in targets:
                if name not in shared_names:
                    merged[target] = None
            yield merged

    # Right-only rows follow left-driven output, preserving existing stable order.
    for right_position, right in enumerate(right_records):
        if matched_right[right_position]:
            continue
        merged = {name: None for name in left_columns}
        for name, target in targets:
            if name in right:
                merged[target] = right[name]
        yield merged
