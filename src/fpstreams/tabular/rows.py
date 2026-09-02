"""Record-oriented pipelines and tabular data operations."""

from __future__ import annotations

import builtins as _builtins
import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from time import perf_counter_ns
from types import BuiltinFunctionType, FunctionType
from typing import Any, BinaryIO, Generic, Literal, TextIO, TypeVar, cast

from ..collecting.aggregation import (
    Aggregator,
    prepare_aggregations,
)
from ..errors import DuplicateKeyError, SelectionError
from ..expressions.row import RowExpr, lit
from ..expressions.row_eval import LazyRowEvaluator, RowProgram, cached_row_program
from ..expressions.selectors import Selector, _direct_field, compile_selector
from ..planning.arrow import RowStageDescriptor, _register_row_stage
from ..planning.explain import PlanExplanation
from ..planning.logical import (
    GlobalAggregateNode,
    JoinNode,
    JoinSpec,
    SourceNode,
    merge_engine_requests,
)
from ..planning.native import TerminalName
from ..planning.source import Source, _function_code
from ..planning.sync import Engine
from ..runtime.iterators import close_iterators
from ..runtime.report import (
    ExecutionResult,
    _record_direct_strategy,
    _start_recording,
    _stop_recording,
)
from ..streams.flow import Flow, flow
from ._text_sources import CSVRowSource, csv_flow, jsonl_flow
from .arrow import (
    arrow_source,
    columns_source,
    csv_source,
    parquet_source,
)
from .dataframe import dataframe_source
from .grouped import GroupedRows
from .io import RowsIOMixin
from .join import (
    _JOIN_MODES,
    _JOIN_VALIDATIONS,
    JoinSelector,
    JoinValidation,
    _compile_join_selector,
    _normalize_join_selectors,
)
from .polars import polars_source
from .records import _as_record, _require_unique_names
from .spill import validate_partitions
from .spill_limits import SpillLimits
from .sql import (
    ConnectionFactory,
    DBParameters,
    db_row_factory,
    sqlite_row_factory,
)

T = TypeVar("T")
R = TypeVar("R")
_BUILTIN_CALLABLE = callable
_BUILTIN_ALL = _builtins.all
_BUILTIN_ABS = _builtins.abs
_BUILTIN_ANY = _builtins.any
_BUILTIN_ATTRIBUTE_ERROR = _builtins.AttributeError
_BUILTIN_DICT = dict
_BUILTIN_FILTER = _builtins.filter
_BUILTIN_FROZENSET = frozenset
_BUILTIN_GETATTR = getattr
_BUILTIN_GLOBALS = globals
_BUILTIN_ISINSTANCE = isinstance
_BUILTIN_ITER = _builtins.iter
_BUILTIN_KEY_ERROR = _builtins.KeyError
_BUILTIN_LEN = _builtins.len
_BUILTIN_LIST: type[list[Any]] = list
_BUILTIN_RANGE = range
_BUILTIN_STR = str
_BUILTIN_TUPLE: type[tuple[Any, ...]] = tuple
_BUILTIN_TYPE = type
_BUILTIN_TYPE_ERROR = _builtins.TypeError
_BUILTIN_VALUE_ERROR = _builtins.ValueError
_BUILTIN_ZIP = zip
_CANONICAL_AS_RECORD = _as_record
_CANONICAL_AS_RECORD_CODE = _as_record.__code__
_CANONICAL_AS_RECORD_GLOBALS = _as_record.__globals__
_CANONICAL_AS_RECORD_BUILTINS = cast(
    dict[str, Any],
    _BUILTIN_GETATTR(_as_record, "__builtins__"),
)
_CANONICAL_DUPLICATE_KEY_ERROR = DuplicateKeyError
_CANONICAL_SELECTION_ERROR = SelectionError
_CANONICAL_FUNCTION_TYPE = FunctionType
_CANONICAL_DIRECT_FIELD = _direct_field
_CANONICAL_ROW_EXPR_CALL = RowExpr.__call__
_CANONICAL_ROW_EXPR_CALL_CODE = RowExpr.__call__.__code__
_CANONICAL_LAZY_ROW_EVALUATOR_CALL = LazyRowEvaluator.__call__
_CANONICAL_LAZY_ROW_EVALUATOR_CALL_CODE = LazyRowEvaluator.__call__.__code__
_CANONICAL_LAZY_ROW_EVALUATOR_GETATTRIBUTE = LazyRowEvaluator.__getattribute__
_CANONICAL_CACHED_ROW_PROGRAM = cached_row_program
_CANONICAL_CACHED_ROW_PROGRAM_CODE = cached_row_program.__code__
_CANONICAL_LAZY_ROW_EVALUATOR_LOCK_TYPE = type(LazyRowEvaluator(object())._lock)
_CANONICAL_SELECTOR_BUILTINS = cast(
    dict[str, Any], _BUILTIN_GETATTR(compile_selector, "__builtins__")
)
_CANONICAL_SELECTOR_GLOBALS = cast(
    dict[str, Any], _BUILTIN_GETATTR(compile_selector, "__globals__")
)
_CANONICAL_DIRECT_SELECT_CODE = compile_selector("__fpstreams_direct_select_probe__").__code__
_LIST_ITERATOR_TYPE: type[Iterator[Any]] = _BUILTIN_TYPE(_builtins.iter([]))
_TUPLE_ITERATOR_TYPE: type[Iterator[Any]] = _BUILTIN_TYPE(_builtins.iter(()))
_NATIVE_UNNEST_MIN_ROWS = 64


def _has_exact_string_keys(record: dict[Any, Any]) -> bool:
    """Prove a dictionary can use direct built-in string lookup semantics."""
    for name in record:  # noqa: SIM110 - avoids a measured generator frame per row
        if type(name) is not str:
            return False
    return True


_CANONICAL_HAS_EXACT_STRING_KEYS = _has_exact_string_keys


def _build_select_project(
    positional: list[tuple[str, Callable[[Any], Any]]],
    aliases: list[tuple[str, Callable[[Any], Any]]],
) -> Callable[[Any], dict[str, Any]]:
    """Bind one canonical lazy projection while keeping selector siblings isolated."""

    def project(row: Any) -> dict[str, Any]:
        """Evaluate positional and named selectors into a new projected dictionary."""
        return {name: select(row) for name, select in (*positional, *aliases)}

    return project


_CANONICAL_BUILD_SELECT_PROJECT = _build_select_project
_CANONICAL_SELECT_PROJECT_CODE = _build_select_project([], []).__code__


def _materialized_select_spec(
    function: object,
) -> tuple[tuple[str, str], ...] | None:
    """Recover one unmodified top-level direct-field projection from its sealed closure."""
    if (
        type(function) is not FunctionType
        or function.__code__ is not _CANONICAL_SELECT_PROJECT_CODE
        or function.__closure__ is None
        or function.__code__.co_freevars != ("aliases", "positional")
    ):
        return None
    closure = {
        name: cell.cell_contents
        for name, cell in zip(function.__code__.co_freevars, function.__closure__, strict=True)
    }
    aliases = closure["aliases"]
    positional = closure["positional"]
    if type(aliases) is not list or type(positional) is not list:
        return None

    direct: list[tuple[str, str]] = []
    for entry in (*positional, *aliases):
        if type(entry) is not tuple or len(entry) != 2:
            return None
        name, selector = entry
        field = _CANONICAL_DIRECT_FIELD(selector)
        if (
            type(name) is not str
            or type(field) is not str
            or type(selector) is not FunctionType
            or selector.__code__ is not _CANONICAL_DIRECT_SELECT_CODE
            or selector.__globals__ is not _CANONICAL_SELECTOR_GLOBALS
            or _BUILTIN_GETATTR(selector, "__builtins__", None) is not _CANONICAL_SELECTOR_BUILTINS
        ):
            return None
        direct.append((name, field))
    selector_globals = _CANONICAL_SELECTOR_GLOBALS
    selector_builtins = _CANONICAL_SELECTOR_BUILTINS
    for name, canonical in (
        ("AttributeError", _BUILTIN_ATTRIBUTE_ERROR),
        ("KeyError", _BUILTIN_KEY_ERROR),
        ("TypeError", _BUILTIN_TYPE_ERROR),
        ("dict", _BUILTIN_DICT),
        ("getattr", _BUILTIN_GETATTR),
        ("isinstance", _BUILTIN_ISINSTANCE),
        ("type", _BUILTIN_TYPE),
    ):
        if selector_globals.get(name, selector_builtins.get(name)) is not canonical:
            return None
    if (
        selector_globals.get("Mapping") is not Mapping
        or selector_globals.get("SelectionError") is not SelectionError
    ):
        return None
    return tuple(direct)


_CANONICAL_MATERIALIZED_SELECT_SPEC = _materialized_select_spec


def _build_with_columns_enricher(
    selectors: list[tuple[str, Callable[[Any], Any]]],
) -> Callable[[Any], dict[str, Any]]:
    """Bind one canonical computed-column transform around already compiled selectors."""

    def enrich(row: Any) -> dict[str, Any]:
        """Copy a row and evaluate every new value against that original row."""
        record = row.copy() if type(row) is dict else _as_record(row)
        for name, select in selectors:
            record[name] = select(row)
        return record

    return enrich


_CANONICAL_BUILD_WITH_COLUMNS_ENRICHER = _build_with_columns_enricher
_CANONICAL_WITH_COLUMNS_ENRICHER = _build_with_columns_enricher([])
_CANONICAL_WITH_COLUMNS_ENRICHER_CODE = _CANONICAL_WITH_COLUMNS_ENRICHER.__code__
_CANONICAL_WITH_COLUMNS_GLOBALS = _CANONICAL_WITH_COLUMNS_ENRICHER.__globals__
_CANONICAL_WITH_COLUMNS_BUILTINS = cast(
    dict[str, Any],
    _BUILTIN_GETATTR(_CANONICAL_WITH_COLUMNS_ENRICHER, "__builtins__"),
)


def _cached_with_columns_program(evaluator: object) -> RowProgram | None:
    """Return one provenance-matched program after its lazy compile has completed."""
    if (
        type(evaluator) is not LazyRowEvaluator
        or LazyRowEvaluator.__getattribute__ is not _CANONICAL_LAZY_ROW_EVALUATOR_GETATTRIBUTE
        or _CANONICAL_CACHED_ROW_PROGRAM.__code__ is not _CANONICAL_CACHED_ROW_PROGRAM_CODE
    ):
        return None
    return _CANONICAL_CACHED_ROW_PROGRAM(evaluator)


_CANONICAL_CACHED_WITH_COLUMNS_PROGRAM = _cached_with_columns_program


def _materialized_with_columns_spec(
    function: object,
) -> tuple[tuple[str, RowExpr], ...] | None:
    """Recover exact RowExpr selectors from an unmodified computed-column closure."""
    if (
        type(function) is not FunctionType
        or function.__code__ is not _CANONICAL_WITH_COLUMNS_ENRICHER_CODE
        or function.__closure__ is None
        or function.__code__.co_freevars != ("selectors",)
        or function.__globals__ is not _CANONICAL_WITH_COLUMNS_GLOBALS
        or _BUILTIN_GETATTR(function, "__builtins__", None) is not _CANONICAL_WITH_COLUMNS_BUILTINS
        or RowExpr.__dict__.get("__call__") is not _CANONICAL_ROW_EXPR_CALL
        or _CANONICAL_ROW_EXPR_CALL.__code__ is not _CANONICAL_ROW_EXPR_CALL_CODE
        or LazyRowEvaluator.__dict__.get("__call__") is not _CANONICAL_LAZY_ROW_EVALUATOR_CALL
        or _CANONICAL_LAZY_ROW_EVALUATOR_CALL.__code__
        is not _CANONICAL_LAZY_ROW_EVALUATOR_CALL_CODE
        or globals().get("_cached_with_columns_program")
        is not _CANONICAL_CACHED_WITH_COLUMNS_PROGRAM
        or _builtins.abs is not _BUILTIN_ABS
    ):
        return None
    selectors = function.__closure__[0].cell_contents
    if type(selectors) is not list:
        return None
    direct: list[tuple[str, RowExpr]] = []
    for entry in selectors:
        if type(entry) is not tuple or len(entry) != 2:
            return None
        name, selector = entry
        evaluator = selector._evaluate if type(selector) is RowExpr else None
        program = _CANONICAL_CACHED_WITH_COLUMNS_PROGRAM(evaluator)
        if (
            type(name) is not str
            or type(selector) is not RowExpr
            or type(evaluator) is not LazyRowEvaluator
            or evaluator.node is not selector._node
            or program is None
            or type(evaluator._lock) is not _CANONICAL_LAZY_ROW_EVALUATOR_LOCK_TYPE
            or evaluator._lock.locked()
        ):
            return None
        direct.append((name, selector))
    function_globals = function.__globals__
    function_builtins = cast(dict[str, Any], _BUILTIN_GETATTR(function, "__builtins__"))
    if (
        function_globals.get("_as_record") is not _CANONICAL_AS_RECORD
        or function_globals.get("type", function_builtins.get("type")) is not _BUILTIN_TYPE
        or function_globals.get("dict", function_builtins.get("dict")) is not _BUILTIN_DICT
    ):
        return None
    return tuple(direct)


_CANONICAL_MATERIALIZED_WITH_COLUMNS_SPEC = _materialized_with_columns_spec


def _build_rename_transform(columns: dict[str, str]) -> Callable[[Any], dict[str, Any]]:
    """Bind one canonical top-level rename while retaining its live mapping semantics."""

    def transform(row: Any) -> dict[str, Any]:
        """Rename fields while detecting collisions in the resulting record."""
        renamed: dict[str, Any] = {}
        for name, value in _as_record(row).items():
            target = columns.get(name, name)
            if target in renamed:
                raise ValueError(f"rename creates duplicate column {target!r}")
            renamed[target] = value
        return renamed

    return transform


_CANONICAL_BUILD_RENAME_TRANSFORM = _build_rename_transform
_CANONICAL_RENAME_TRANSFORM = _build_rename_transform({})
_CANONICAL_RENAME_TRANSFORM_CODE = _CANONICAL_RENAME_TRANSFORM.__code__
_CANONICAL_RENAME_GLOBALS = _CANONICAL_RENAME_TRANSFORM.__globals__
_CANONICAL_RENAME_BUILTINS = cast(
    dict[str, Any],
    _BUILTIN_GETATTR(_CANONICAL_RENAME_TRANSFORM, "__builtins__"),
)


def _materialized_rename_spec(function: object) -> tuple[tuple[str, str], ...] | None:
    """Recover one unmodified top-level rename from its exact live closure and globals."""
    if (
        type(function) is not FunctionType
        or function.__code__ is not _CANONICAL_RENAME_TRANSFORM_CODE
        or function.__closure__ is None
        or function.__code__.co_freevars != ("columns",)
        or function.__globals__ is not _CANONICAL_RENAME_GLOBALS
        or _BUILTIN_GETATTR(function, "__builtins__", None) is not _CANONICAL_RENAME_BUILTINS
    ):
        return None
    columns = function.__closure__[0].cell_contents
    if type(columns) is not dict:
        return None
    direct: list[tuple[str, str]] = []
    for source, target in columns.items():
        if type(source) is not str or type(target) is not str or not target:
            return None
        direct.append((source, target))
    function_globals = function.__globals__
    function_builtins = cast(dict[str, Any], _BUILTIN_GETATTR(function, "__builtins__"))
    if (
        function_globals.get("_as_record") is not _CANONICAL_AS_RECORD
        or function_globals.get("ValueError", function_builtins.get("ValueError"))
        is not _BUILTIN_VALUE_ERROR
    ):
        return None
    return tuple(direct)


_CANONICAL_MATERIALIZED_RENAME_SPEC = _materialized_rename_spec


def _specialize_drop_nulls(  # noqa: C901 - policy-specific hot loops preserve short-circuiting
    fields: tuple[str, ...],
    how: Literal["any", "all"],
    fallback: Callable[[Any], bool],
) -> Callable[[Any], bool]:
    """Build the exact-dictionary predicate selected once for one null policy."""
    if len(fields) == 1:
        field = fields[0]

        def keep_one(row: Any) -> bool:
            if type(row) is dict:
                try:
                    return row.get(field) is not None
                except (AttributeError, KeyError, TypeError):
                    return False
            return fallback(row)

        return keep_one

    field_count = len(fields)
    if how == "any":

        def keep_all_fields(row: Any) -> bool:
            if type(row) is not dict:
                return fallback(row)
            index = 0
            while index < field_count:
                try:
                    value = row.get(fields[index])
                except (AttributeError, KeyError, TypeError):
                    return False
                if value is None:
                    return False
                index += 1
            return True

        return keep_all_fields

    def keep_any_field(row: Any) -> bool:
        if type(row) is not dict:
            return fallback(row)
        index = 0
        while index < field_count:
            try:
                value = row.get(fields[index])
            except (AttributeError, KeyError, TypeError):
                value = None
            if value is not None:
                return True
            index += 1
        return False

    return keep_any_field


class _DropNullsPlan:
    """Seal direct-field null filtering behind one lazily executable project plan."""

    __slots__ = ("_fallback", "_predicate", "fields", "how")

    def __init__(
        self,
        fields: tuple[str, ...] | None,
        how: Literal["any", "all"],
        fallback: Callable[[Any], bool],
    ) -> None:
        self.fields = fields
        self.how = how
        self._fallback = fallback
        self._predicate = (
            fallback if fields is None else _specialize_drop_nulls(fields, how, fallback)
        )

    def __call__(self, row: Any) -> bool:
        """Retain the ordinary callback contract outside the sealed loop."""
        return self._predicate(row)

    def filter_rows(self, source: Iterator[Any]) -> Iterator[Any]:
        """Return the policy-specific lazy loop without a callback call per exact row."""
        if self.fields is None:
            return self._filter_record_values(source)
        if len(self.fields) == 1:
            return self._filter_one(source)
        if self.how == "any":
            return self._filter_all_fields(source)
        return self._filter_any_field(source)

    def _filter_record_values(self, source: Iterator[Any]) -> Iterator[Any]:
        """Copy each compatible record once and apply its whole-record null policy."""
        if self.how == "any":
            return self._filter_records_without_nulls(source)
        return self._filter_records_with_any_value(source)

    def _filter_records_without_nulls(self, source: Iterator[Any]) -> Iterator[Any]:
        """Keep records whose copied value snapshot contains no None value."""
        for row in source:
            record = row.copy() if _BUILTIN_TYPE(row) is _BUILTIN_DICT else _as_record(row)
            value = None
            keep = True
            for value in record.values():
                if value is None:
                    keep = False
                    break
            del value, record
            if keep:
                yield row

    def _filter_records_with_any_value(self, source: Iterator[Any]) -> Iterator[Any]:
        """Keep records whose copied value snapshot contains at least one non-None value."""
        for row in source:
            record = row.copy() if _BUILTIN_TYPE(row) is _BUILTIN_DICT else _as_record(row)
            value = None
            keep = False
            for value in record.values():
                if value is not None:
                    keep = True
                    break
            del value, record
            if keep:
                yield row

    def _filter_one(self, source: Iterator[Any]) -> Iterator[Any]:
        fields = self.fields
        assert fields is not None
        field = fields[0]
        fallback = self._fallback
        for row in source:
            if type(row) is dict:
                try:
                    keep = row.get(field) is not None
                except (AttributeError, KeyError, TypeError):
                    keep = False
            else:
                keep = fallback(row)
            if keep:
                yield row

    def extend_one_materialized(self, output: list[Any], source: Iterable[Any]) -> None:
        """Collect one direct-field policy without an intermediate generator frame."""
        fields = self.fields
        assert fields is not None and _BUILTIN_LEN(fields) == 1
        field = fields[0]
        fallback = self._fallback
        append = output.append
        source_iterator = _BUILTIN_ITER(source)
        if _BUILTIN_TYPE(source_iterator) not in (
            _LIST_ITERATOR_TYPE,
            _TUPLE_ITERATOR_TYPE,
        ):
            output.extend(self._filter_one(source_iterator))
            return

        for row in source_iterator:
            if type(row) is dict:
                try:
                    if row.get(field) is not None:
                        append(row)
                except (AttributeError, KeyError, TypeError):
                    pass
                continue
            keep = fallback(row)
            if keep:
                append(row)
            break
        else:
            return

        for row in source_iterator:
            if type(row) is dict:
                try:
                    keep = row.get(field) is not None
                except (AttributeError, KeyError, TypeError):
                    keep = False
            else:
                keep = fallback(row)
            if keep:
                append(row)

    def _filter_all_fields(  # noqa: C901 - common widths avoid a per-row policy loop
        self, source: Iterator[Any]
    ) -> Iterator[Any]:
        fields = self.fields
        assert fields is not None
        field_count = len(fields)
        fallback = self._fallback
        match field_count:
            case 2:
                first, second = fields
                for row in source:
                    if type(row) is not dict:
                        if fallback(row):
                            yield row
                        continue
                    try:
                        value = row.get(first)
                        if value is None:
                            continue
                        value = row.get(second)
                        if value is None:
                            continue
                    except (AttributeError, KeyError, TypeError):
                        continue
                    yield row
                return
            case 3:
                first, second, third = fields
                for row in source:
                    if type(row) is not dict:
                        if fallback(row):
                            yield row
                        continue
                    try:
                        value = row.get(first)
                        if value is None:
                            continue
                        value = row.get(second)
                        if value is None:
                            continue
                        value = row.get(third)
                        if value is None:
                            continue
                    except (AttributeError, KeyError, TypeError):
                        continue
                    yield row
                return
            case 4:
                first, second, third, fourth = fields
                for row in source:
                    if type(row) is not dict:
                        if fallback(row):
                            yield row
                        continue
                    try:
                        value = row.get(first)
                        if value is None:
                            continue
                        value = row.get(second)
                        if value is None:
                            continue
                        value = row.get(third)
                        if value is None:
                            continue
                        value = row.get(fourth)
                        if value is None:
                            continue
                    except (AttributeError, KeyError, TypeError):
                        continue
                    yield row
                return
        for row in source:
            if type(row) is not dict:
                if fallback(row):
                    yield row
                continue
            index = 0
            while index < field_count:
                try:
                    value = row.get(fields[index])
                except (AttributeError, KeyError, TypeError):
                    break
                if value is None:
                    break
                index += 1
            if index == field_count:
                yield row

    def _filter_any_field(  # noqa: C901 - common widths preserve lookup short-circuiting
        self, source: Iterator[Any]
    ) -> Iterator[Any]:
        fields = self.fields
        assert fields is not None
        field_count = len(fields)
        fallback = self._fallback
        match field_count:
            case 2:
                first, second = fields
                for row in source:
                    if type(row) is not dict:
                        if fallback(row):
                            yield row
                        continue
                    try:
                        value = row.get(first)
                    except (AttributeError, KeyError, TypeError):
                        value = None
                    if value is not None:
                        yield row
                        continue
                    try:
                        value = row.get(second)
                    except (AttributeError, KeyError, TypeError):
                        value = None
                    if value is not None:
                        yield row
                return
            case 3:
                first, second, third = fields
                for row in source:
                    if type(row) is not dict:
                        if fallback(row):
                            yield row
                        continue
                    try:
                        value = row.get(first)
                    except (AttributeError, KeyError, TypeError):
                        value = None
                    if value is not None:
                        yield row
                        continue
                    try:
                        value = row.get(second)
                    except (AttributeError, KeyError, TypeError):
                        value = None
                    if value is not None:
                        yield row
                        continue
                    try:
                        value = row.get(third)
                    except (AttributeError, KeyError, TypeError):
                        value = None
                    if value is not None:
                        yield row
                return
            case 4:
                first, second, third, fourth = fields
                for row in source:
                    if type(row) is not dict:
                        if fallback(row):
                            yield row
                        continue
                    try:
                        value = row.get(first)
                    except (AttributeError, KeyError, TypeError):
                        value = None
                    if value is not None:
                        yield row
                        continue
                    try:
                        value = row.get(second)
                    except (AttributeError, KeyError, TypeError):
                        value = None
                    if value is not None:
                        yield row
                        continue
                    try:
                        value = row.get(third)
                    except (AttributeError, KeyError, TypeError):
                        value = None
                    if value is not None:
                        yield row
                        continue
                    try:
                        value = row.get(fourth)
                    except (AttributeError, KeyError, TypeError):
                        value = None
                    if value is not None:
                        yield row
                return
        for row in source:
            if type(row) is not dict:
                if fallback(row):
                    yield row
                continue
            index = 0
            while index < field_count:
                try:
                    value = row.get(fields[index])
                except (AttributeError, KeyError, TypeError):
                    value = None
                if value is not None:
                    yield row
                    break
                index += 1


def _canonical_drop_nulls_filter_globals(*, whole_record: bool) -> bool:
    """Check dynamic constructors and helpers used by the canonical filter path."""
    from ..execution import sync as sync_execution

    builtin_globals = _builtins.__dict__
    sync_globals = sync_execution.__dict__
    if sync_globals.get("filter", builtin_globals.get("filter")) is not _BUILTIN_FILTER:
        return False
    return not whole_record or not (
        _BUILTIN_GLOBALS().get("_as_record") is not _CANONICAL_AS_RECORD
        or globals().get("any", builtin_globals.get("any")) is not _BUILTIN_ANY
        or globals().get("all", builtin_globals.get("all")) is not _BUILTIN_ALL
        or builtin_globals.get("dict") is not _BUILTIN_DICT
        or builtin_globals.get("type") is not _BUILTIN_TYPE
    )


def _planned_drop_nulls_filter(
    predicate: Callable[[Any], Any], source: Iterator[Any]
) -> Iterator[Any] | None:
    """Return the lazy sealed loop only for the exact project-owned predicate."""
    if type(predicate) is not _DropNullsPlan or not _canonical_drop_nulls_filter_globals(
        whole_record=predicate.fields is None
    ):
        return None
    return predicate.filter_rows(source)


_CANONICAL_DROP_NULLS_FILTER_ROWS = _DropNullsPlan.filter_rows
_CANONICAL_DROP_NULLS_FILTER_ONE = _DropNullsPlan._filter_one
_CANONICAL_DROP_NULLS_EXTEND_ONE_MATERIALIZED = _DropNullsPlan.extend_one_materialized
_CANONICAL_PLANNED_DROP_NULLS_FILTER = _planned_drop_nulls_filter


def _materialized_drop_nulls_appender(
    predicate: Callable[[Any], Any],
) -> Callable[[list[Any], Iterable[Any]], None] | None:
    """Expose only the canonical one-field plan to the retained list sink."""
    module_globals = _BUILTIN_GLOBALS()
    builtin_globals = _builtins.__dict__
    if (
        _BUILTIN_TYPE(predicate) is not _DropNullsPlan
        or predicate.fields is None
        or _BUILTIN_LEN(predicate.fields) != 1
        or not _canonical_drop_nulls_filter_globals(whole_record=False)
    ):
        return None
    namespace = _DropNullsPlan.__dict__
    if (
        namespace.get("filter_rows") is not _CANONICAL_DROP_NULLS_FILTER_ROWS
        or namespace.get("_filter_one") is not _CANONICAL_DROP_NULLS_FILTER_ONE
        or namespace.get("extend_one_materialized")
        is not _CANONICAL_DROP_NULLS_EXTEND_ONE_MATERIALIZED
        or module_globals.get("_planned_drop_nulls_filter")
        is not _CANONICAL_PLANNED_DROP_NULLS_FILTER
    ):
        return None
    for name, canonical in (
        ("dict", _BUILTIN_DICT),
        ("iter", _BUILTIN_ITER),
        ("len", _BUILTIN_LEN),
        ("type", _BUILTIN_TYPE),
    ):
        if module_globals.get(name, builtin_globals.get(name)) is not canonical:
            return None
    return predicate.extend_one_materialized


_CANONICAL_MATERIALIZED_DROP_NULLS_APPENDER = _materialized_drop_nulls_appender


def _build_unnest_transform(column: str, prefix: str) -> Callable[[Any], dict[str, Any]]:
    """Bind one canonical unnest callback without slowing ordinary lazy iteration."""

    def expand(row: Any) -> dict[str, Any]:
        record = _as_record(row)
        try:
            nested_value = record.pop(column)
        except KeyError:
            raise SelectionError(f"unnest column {column!r} is missing") from None
        nested = _as_record(nested_value)
        for name, value in nested.items():
            target = f"{prefix}{name}"
            if target in record:
                raise DuplicateKeyError(
                    f"unnest output column {target!r} collides with an existing column"
                )
            record[target] = value
        return record

    return expand


_CANONICAL_BUILD_UNNEST_TRANSFORM = _build_unnest_transform
_CANONICAL_UNNEST_TRANSFORM = _build_unnest_transform("column", "prefix")
_CANONICAL_UNNEST_TRANSFORM_CODE = _CANONICAL_UNNEST_TRANSFORM.__code__
_CANONICAL_UNNEST_TRANSFORM_GLOBALS = _CANONICAL_UNNEST_TRANSFORM.__globals__
_CANONICAL_UNNEST_TRANSFORM_BUILTINS = cast(
    dict[str, Any],
    _BUILTIN_GETATTR(_CANONICAL_UNNEST_TRANSFORM, "__builtins__"),
)


def _materialized_unnest_spec(function: object) -> tuple[str, str] | None:
    """Recover one unmodified top-level unnest closure for a direct list sink."""
    if _BUILTIN_TYPE(function) is not _CANONICAL_FUNCTION_TYPE:
        return None
    function_code = _function_code(function)
    if (
        function_code is not _CANONICAL_UNNEST_TRANSFORM_CODE
        or function.__closure__ is None
        or function_code.co_freevars != ("column", "prefix")
        or function.__globals__ is not _CANONICAL_UNNEST_TRANSFORM_GLOBALS
        or _BUILTIN_GETATTR(function, "__builtins__", None)
        is not _CANONICAL_UNNEST_TRANSFORM_BUILTINS
    ):
        return None
    column, prefix = (cell.cell_contents for cell in function.__closure__)
    if _BUILTIN_TYPE(column) is not _BUILTIN_STR or _BUILTIN_TYPE(prefix) is not _BUILTIN_STR:
        return None
    globals_ = function.__globals__
    builtins_ = cast("dict[str, Any]", _BUILTIN_GETATTR(function, "__builtins__"))
    record_globals = _CANONICAL_AS_RECORD_GLOBALS
    record_builtins = _CANONICAL_AS_RECORD_BUILTINS
    if (
        globals_.get("_as_record") is not _CANONICAL_AS_RECORD
        or _function_code(_CANONICAL_AS_RECORD) is not _CANONICAL_AS_RECORD_CODE
        or record_globals.get("type", record_builtins.get("type")) is not _BUILTIN_TYPE
        or record_globals.get("dict", record_builtins.get("dict")) is not _BUILTIN_DICT
        or globals_.get("DuplicateKeyError") is not _CANONICAL_DUPLICATE_KEY_ERROR
        or globals_.get("SelectionError") is not _CANONICAL_SELECTION_ERROR
        or globals_.get("KeyError", builtins_.get("KeyError")) is not _BUILTIN_KEY_ERROR
    ):
        return None
    return column, prefix


def _append_materialized_unnest(
    output: list[Any],
    source: Iterable[Any],
    column: str,
    prefix: str,
    allow_native: bool,
) -> Iterator[Any] | None:
    """Append one native exact prefix and return its canonical Python suffix."""
    source_iterator = cast(Iterator[Any], source)
    source_type = _BUILTIN_TYPE(source)
    native_helper = (
        _BUILTIN_GLOBALS().get("_try_native_unnest_materialize")
        if allow_native
        and (source_type is _LIST_ITERATOR_TYPE or source_type is _TUPLE_ITERATOR_TYPE)
        and cast(Any, source).__length_hint__() >= _NATIVE_UNNEST_MIN_ROWS
        else None
    )
    if native_helper is _CANONICAL_TRY_NATIVE_UNNEST_MATERIALIZE:
        handled, remaining = _CANONICAL_TRY_NATIVE_UNNEST_MATERIALIZE(
            output,
            source,
            column,
            prefix,
        )
        if handled:
            # A native boundary has not run the Python callback. Resume it through the
            # canonical operation chain, where protocol objects may mutate the live closure.
            return cast(Iterator[Any] | None, remaining)
    return source_iterator


_CANONICAL_APPEND_MATERIALIZED_UNNEST = _append_materialized_unnest
_CANONICAL_APPEND_MATERIALIZED_UNNEST_CODE = _append_materialized_unnest.__code__


def _prepend_unnest_boundary(first: Any, source: Iterator[Any]) -> Iterator[Any]:
    """Resume Python unnest at one native boundary without pulling the tail."""
    yield first
    del first
    yield from source


_CANONICAL_PREPEND_UNNEST_BOUNDARY = _prepend_unnest_boundary


def _try_native_unnest_materialize(
    output: list[Any],
    source: Iterable[Any],
    column: str,
    prefix: str,
) -> tuple[bool, Iterator[Any] | None]:
    """Append a compatible exact-dict prefix and return one resumable boundary."""
    source_type = _BUILTIN_TYPE(source)
    if (
        _BUILTIN_TYPE(output) is not _BUILTIN_LIST
        or (source_type is not _LIST_ITERATOR_TYPE and source_type is not _TUPLE_ITERATOR_TYPE)
        or _BUILTIN_TYPE(column) is not _BUILTIN_STR
        or _BUILTIN_TYPE(prefix) is not _BUILTIN_STR
    ):
        return False, None
    # Exact builtin iterators own this non-dispatching length hint, so the cost-model decision
    # consumes nothing. The native kernel still validates every row and returns the first
    # incompatible boundary untouched.
    if cast(Any, source).__length_hint__() < _NATIVE_UNNEST_MIN_ROWS:
        return False, None
    try:
        from .. import _native
    except ImportError:
        return False, None
    raw_endpoint = _BUILTIN_GETATTR(_native, "unnest_exact_dict_prefix_v1", None)
    if not _BUILTIN_CALLABLE(raw_endpoint):
        return False, None
    endpoint = cast(
        Callable[
            [list[Any], Iterable[Any], str, str],
            tuple[Any | None, bool] | None,
        ],
        raw_endpoint,
    )
    native = endpoint(output, source, column, prefix)
    if native is None:
        return False, None
    first_incompatible, completed = native
    del native
    if completed:
        return True, None
    source_iterator: Iterator[Any] = source  # type: ignore[assignment]
    remaining = _CANONICAL_PREPEND_UNNEST_BOUNDARY(first_incompatible, source_iterator)
    del first_incompatible
    return True, remaining


_CANONICAL_TRY_NATIVE_UNNEST_MATERIALIZE = _try_native_unnest_materialize
_CANONICAL_MATERIALIZED_UNNEST_SPEC = _materialized_unnest_spec
_CANONICAL_MATERIALIZED_UNNEST_SPEC_CODE = _materialized_unnest_spec.__code__


def _materialized_unnest_appender(
    function: object,
    allow_native: bool,
) -> Callable[[list[Any], Iterable[Any]], Iterator[Any] | None] | None:
    """Return the owned unnest sink for one canonical closure."""
    append_loop = _BUILTIN_GLOBALS().get("_append_materialized_unnest")
    spec_factory = _BUILTIN_GLOBALS().get("_materialized_unnest_spec")
    if (
        append_loop is not _CANONICAL_APPEND_MATERIALIZED_UNNEST
        or _function_code(_CANONICAL_APPEND_MATERIALIZED_UNNEST)
        is not _CANONICAL_APPEND_MATERIALIZED_UNNEST_CODE
        or spec_factory is not _CANONICAL_MATERIALIZED_UNNEST_SPEC
        or _function_code(_CANONICAL_MATERIALIZED_UNNEST_SPEC)
        is not _CANONICAL_MATERIALIZED_UNNEST_SPEC_CODE
    ):
        return None
    spec = _CANONICAL_MATERIALIZED_UNNEST_SPEC(function)
    if spec is None:
        return None
    column, prefix = spec

    def append_materialized(output: list[Any], source: Iterable[Any]) -> Iterator[Any] | None:
        return append_loop(
            output,
            source,
            column,
            prefix,
            allow_native,
        )

    return append_materialized


_CANONICAL_MATERIALIZED_UNNEST_APPENDER = _materialized_unnest_appender


def _explode_owned_record(
    record: dict[str, Any],
    values: Any,
    output_name: str,
    outer: bool,
) -> Iterator[dict[str, Any]]:
    """Expand a converted record while retaining its established ownership behavior."""
    if values is None:
        if outer:
            record[output_name] = None
            yield record
        return
    if isinstance(values, (str, bytes, bytearray, Mapping)) or not isinstance(values, Iterable):
        raise TypeError("explode selector must return a non-string iterable or None")

    emitted = False
    for value in values:
        emitted = True
        expanded = record.copy()
        expanded[output_name] = value
        yield expanded
    if outer and not emitted:
        record[output_name] = None
        yield record


def _explode_compatible_record(
    row: Any,
    select: Callable[[Any], Any],
    output_name: str,
    outer: bool,
) -> Iterator[dict[str, Any]]:
    """Convert before selecting, matching the public protocol fallback order."""
    record = _as_record(row)
    return _explode_owned_record(record, select(row), output_name, outer)


class _ExplodeExpansion:
    """Own explode's lazy and fully materialized execution payload."""

    __slots__ = ("direct_field", "outer", "output_name", "select")

    def __init__(
        self,
        select: Callable[[Any], Any],
        output_name: str,
        outer: bool,
        direct_field: str | None,
    ) -> None:
        self.select = select
        self.output_name = output_name
        self.outer = outer
        self.direct_field = direct_field

    def __call__(self, row: Any) -> Iterator[dict[str, Any]]:
        """Use the owned snapshot path for ordinary lazy iteration and short-circuiting."""
        return _explode_compatible_record(row, self.select, self.output_name, self.outer)

    def extend_materialized(  # noqa: C901 - keep snapshot reuse inside the measured sink
        self, output: list[Any], source: Iterable[Any]
    ) -> None:
        """Append fully owned outputs from one canonical per-row snapshot."""
        output_name = self.output_name
        outer = self.outer
        direct_field = self.direct_field
        extend = output.extend
        append = output.append
        for row in source:
            if type(row) is not dict:
                extend(self(row))
                continue
            # Canonical explode snapshots an exact dictionary before invoking its selector.
            # Keeping that order makes custom colliding-key equality, live mutation, and
            # selector errors observable exactly once without an extra full-width key proof.
            record = row.copy()
            if direct_field is None:
                values = self.select(row)
            else:
                try:
                    values = row[direct_field]
                except (AttributeError, KeyError, TypeError) as error:
                    raise SelectionError(
                        f"Could not resolve selector {direct_field!r}; failed at {direct_field!r}"
                    ) from error
            if values is None:
                if outer:
                    record[output_name] = None
                    append(record)
                continue
            values_type = type(values)
            if values_type is not list and values_type is not tuple:
                if isinstance(values, (str, bytes, bytearray, Mapping)) or not isinstance(
                    values, Iterable
                ):
                    raise TypeError("explode selector must return a non-string iterable or None")
                emitted = False
                for value in values:
                    emitted = True
                    expanded = record.copy()
                    expanded[output_name] = value
                    append(expanded)
                if outer and not emitted:
                    record[output_name] = None
                    append(record)
                continue
            if values:
                if output_name is direct_field:
                    emitted = False
                    expanded = record
                    for value in values:
                        if emitted:
                            expanded = expanded.copy()
                        else:
                            emitted = True
                        expanded[output_name] = value
                        append(expanded)
                else:
                    for value in values:
                        expanded = record.copy()
                        expanded[output_name] = value
                        append(expanded)
            elif outer:
                record[output_name] = None
                append(record)


def _unpivot_compatible_record(
    row: Any,
    columns: tuple[str, ...],
    names_to: str,
    values_to: str,
) -> Iterator[dict[str, Any]]:
    """Reshape one owned record through the established conversion path."""
    record = _as_record(row)
    missing = [name for name in columns if name not in record]
    if missing:
        raise SelectionError(f"missing columns for unpivot: {missing!r}")
    base = {name: value for name, value in record.items() if name not in columns}
    if names_to in base or values_to in base:
        raise DuplicateKeyError("unpivot output names collide with existing columns")
    for name in columns:
        yield {**base, names_to: name, values_to: record[name]}


class _UnpivotExpansion:
    """Own unpivot's lazy and fully materialized execution payload."""

    __slots__ = ("columns", "direct_fields", "names_to", "values_to")

    def __init__(
        self,
        columns: tuple[str, ...],
        names_to: str,
        values_to: str,
    ) -> None:
        self.columns = columns
        self.names_to = names_to
        self.values_to = values_to
        self.direct_fields = (
            all(type(name) is str for name in columns)
            and type(names_to) is str
            and type(values_to) is str
        )

    def __call__(self, row: Any) -> Iterator[dict[str, Any]]:
        """Use an owned snapshot for ordinary iteration and short-circuiting."""
        return _unpivot_compatible_record(
            row,
            self.columns,
            self.names_to,
            self.values_to,
        )

    def extend_materialized(self, output: list[Any], source: Iterable[Any]) -> None:
        """Append exact-dictionary outputs directly into a fully collected result."""
        columns = self.columns
        names_to = self.names_to
        values_to = self.values_to
        direct_fields = self.direct_fields
        native_helper = _BUILTIN_GLOBALS().get("_try_native_unpivot_materialize")
        if native_helper is _CANONICAL_TRY_NATIVE_UNPIVOT_MATERIALIZE:
            handled, remaining = _CANONICAL_TRY_NATIVE_UNPIVOT_MATERIALIZE(output, source, self)
            if handled:
                if remaining is None:
                    return
                source = remaining
        extend = output.extend
        if not direct_fields:
            for row in source:
                extend(self(row))
            return

        append = output.append
        for row in source:
            if type(row) is not dict or not _has_exact_string_keys(row):
                extend(self(row))
                continue

            record = row
            base = record.copy()
            try:
                selected_values = [base.pop(name) for name in columns]
            except KeyError:
                missing = [name for name in columns if name not in record]
                raise SelectionError(f"missing columns for unpivot: {missing!r}") from None
            if names_to in base or values_to in base:
                raise DuplicateKeyError("unpivot output names collide with existing columns")

            # A complete list materialization cannot expose intermediate rows. Reuse the
            # owned base for the first result, then copy that snapshot for the remainder.
            base[names_to] = columns[0]
            base[values_to] = selected_values[0]
            append(base)
            for position in range(1, len(columns)):
                reshaped = base.copy()
                reshaped[names_to] = columns[position]
                reshaped[values_to] = selected_values[position]
                append(reshaped)


_CANONICAL_UNPIVOT_EXTEND_MATERIALIZED = _UnpivotExpansion.extend_materialized


def _prepend_row_expansion_boundary(first: Any, source: Iterator[Any]) -> Iterator[Any]:
    """Resume at one native boundary with canonical previous-row lifetime."""
    yield first
    del first
    yield from source


_CANONICAL_PREPEND_ROW_EXPANSION_BOUNDARY = _prepend_row_expansion_boundary


def _try_native_unpivot_materialize(
    output: list[Any],
    source: Iterable[Any],
    expansion: _UnpivotExpansion,
) -> tuple[bool, Iterator[Any] | None]:
    """Append a compatible exact-dict prefix, then expose one Python fallback boundary."""
    if (
        _BUILTIN_TYPE(output) is not _BUILTIN_LIST
        or _BUILTIN_TYPE(source) not in {_LIST_ITERATOR_TYPE, _TUPLE_ITERATOR_TYPE}
        or _BUILTIN_TYPE(expansion) is not _UnpivotExpansion
        or not expansion.direct_fields
    ):
        return False, None
    columns = expansion.columns
    names_to = expansion.names_to
    values_to = expansion.values_to
    if (
        _BUILTIN_TYPE(columns) is not _BUILTIN_TUPLE
        or not columns
        or _BUILTIN_TYPE(names_to) is not _BUILTIN_STR
        or _BUILTIN_TYPE(values_to) is not _BUILTIN_STR
    ):
        return False, None
    for name in columns:
        if _BUILTIN_TYPE(name) is not _BUILTIN_STR:
            return False, None
    namespace = _UnpivotExpansion.__dict__
    module_globals = _BUILTIN_GLOBALS()
    if (
        namespace.get("extend_materialized") is not _CANONICAL_UNPIVOT_EXTEND_MATERIALIZED
        or module_globals.get("_has_exact_string_keys") is not _CANONICAL_HAS_EXACT_STRING_KEYS
        or module_globals.get("_prepend_row_expansion_boundary")
        is not _CANONICAL_PREPEND_ROW_EXPANSION_BOUNDARY
    ):
        return False, None
    builtin_globals = _builtins.__dict__
    for name, canonical in (
        ("dict", _BUILTIN_DICT),
        ("len", _BUILTIN_LEN),
        ("range", _BUILTIN_RANGE),
        ("str", _BUILTIN_STR),
        ("type", _BUILTIN_TYPE),
    ):
        if module_globals.get(name, builtin_globals.get(name)) is not canonical:
            return False, None
    try:
        from .. import _native
    except ImportError:
        return False, None
    raw_endpoint = _BUILTIN_GETATTR(_native, "unpivot_exact_dict_prefix_v1", None)
    if not _BUILTIN_CALLABLE(raw_endpoint):
        return False, None
    endpoint = cast(
        Callable[
            [list[Any], Iterable[Any], tuple[str, ...], str, str],
            tuple[Any | None, bool] | None,
        ],
        raw_endpoint,
    )
    native = endpoint(output, source, columns, names_to, values_to)
    if native is None:
        return False, None
    first_incompatible, completed = native
    del native
    if completed:
        return True, None
    source_iterator: Iterator[Any] = source  # type: ignore[assignment]
    remaining = _CANONICAL_PREPEND_ROW_EXPANSION_BOUNDARY(first_incompatible, source_iterator)
    del first_incompatible
    return True, remaining


_CANONICAL_TRY_NATIVE_UNPIVOT_MATERIALIZE = _try_native_unpivot_materialize


def _try_csv_identity_list(source_flow: Flow[Any]) -> tuple[bool, list[Any] | None]:
    """Collect one unmodified project CSV source without executor forwarding."""
    if _BUILTIN_TYPE(source_flow) is not Flow:
        return False, None
    try:
        pipeline = source_flow._pipeline
    except TypeError:
        return False, None
    descriptor = pipeline.source.native_data
    if (
        pipeline.engine != "auto"
        or pipeline.parallel is not None
        or pipeline.operations
        or _BUILTIN_TYPE(descriptor) is not CSVRowSource
    ):
        return False, None
    from ..runtime.failpoints import has_active_failpoints, hit

    if has_active_failpoints():
        return False, None
    _record_direct_strategy(
        None,
        "csv_direct",
        "identity Rows CSV source was materialized by its owned parser",
    )
    pipeline.source.open_native(CSVRowSource)
    hit("source.open.after")
    return True, descriptor.materialize()


def _try_native_pivot_materialize(
    source: object,
    index_fields: tuple[str, ...],
    column_field: str,
    value_field: str,
    key_names: tuple[str, ...],
    fill: Any,
) -> list[dict[str, Any]] | None:
    """Materialize one guarded retained exact pivot, or decline without opening its source."""
    if (
        _BUILTIN_TYPE(source) not in (_BUILTIN_LIST, _BUILTIN_TUPLE)
        or _BUILTIN_TYPE(index_fields) is not _BUILTIN_TUPLE
        or _BUILTIN_TYPE(key_names) is not _BUILTIN_TUPLE
        or _BUILTIN_TYPE(column_field) is not _BUILTIN_STR
        or _BUILTIN_TYPE(value_field) is not _BUILTIN_STR
        or not index_fields
        or _BUILTIN_LEN(index_fields) != _BUILTIN_LEN(key_names)
    ):
        return None
    for names in (index_fields, key_names):
        for name in names:
            if _BUILTIN_TYPE(name) is not _BUILTIN_STR:
                return None
    from ..runtime.failpoints import has_active_failpoints

    if has_active_failpoints():
        return None
    module_globals = _BUILTIN_GLOBALS()
    builtin_globals = _builtins.__dict__
    for name, canonical in (
        ("callable", _BUILTIN_CALLABLE),
        ("dict", _BUILTIN_DICT),
        ("str", _BUILTIN_STR),
        ("tuple", _BUILTIN_TUPLE),
        ("type", _BUILTIN_TYPE),
        ("ValueError", _BUILTIN_VALUE_ERROR),
        ("zip", _BUILTIN_ZIP),
    ):
        if module_globals.get(name, builtin_globals.get(name)) is not canonical:
            return None
    try:
        from .. import _native
    except ImportError:
        return None
    endpoint = _BUILTIN_GETATTR(_native, "pivot_exact_dict_rows_v1", None)
    if not _BUILTIN_CALLABLE(endpoint):
        return None
    return cast(
        Callable[
            [
                object,
                tuple[str, ...],
                str,
                str,
                tuple[str, ...],
                Any,
                type[BaseException],
            ],
            list[dict[str, Any]] | None,
        ],
        endpoint,
    )(
        source,
        index_fields,
        column_field,
        value_field,
        key_names,
        fill,
        DuplicateKeyError,
    )


class _RowExpansionPlan:
    """Seal project-owned row expansion payloads behind one terminal trust type."""

    __slots__ = ("_payload",)

    def __init__(self, payload: _ExplodeExpansion | _UnpivotExpansion) -> None:
        if type(payload) not in (_ExplodeExpansion, _UnpivotExpansion):
            raise TypeError("row expansion plans require a project-owned exact payload")
        self._payload = payload

    @classmethod
    def explode(
        cls,
        select: Callable[[Any], Any],
        output_name: str,
        outer: bool,
        direct_field: str | None,
    ) -> _RowExpansionPlan:
        """Build the sealed explode variant."""
        return cls(_ExplodeExpansion(select, output_name, outer, direct_field))

    @classmethod
    def unpivot(
        cls,
        columns: tuple[str, ...],
        names_to: str,
        values_to: str,
    ) -> _RowExpansionPlan:
        """Build the sealed unpivot variant."""
        return cls(_UnpivotExpansion(columns, names_to, values_to))

    def __call__(self, row: Any) -> Iterator[dict[str, Any]]:
        """Keep the selected variant's ordinary lazy snapshot semantics."""
        return self._payload(row)

    def extend_materialized(self, output: list[Any], source: Iterable[Any]) -> None:
        """Delegate one complete collection to the validated project payload."""
        self._payload.extend_materialized(output, source)


def _materialized_row_appender(
    function: Callable[[Any], Iterable[Any]],
) -> Callable[[list[Any], Iterable[Any]], None] | None:
    """Return a list extender only for the one sealed project expansion plan."""
    if type(function) is _RowExpansionPlan:
        return function.extend_materialized
    return None


class Rows(RowsIOMixin[T], Generic[T]):
    """A lazy record pipeline with joins, grouping, and data-system adapters."""

    __slots__ = ("_flow",)

    def __init__(self, source: Iterable[T] | Flow[T]) -> None:
        """Wrap an existing Flow or convert an iterable into a lazy row pipeline."""
        self._flow = source if isinstance(source, Flow) else flow(source)

    @staticmethod
    def from_csv(
        path: str | os.PathLike[str] | TextIO | Callable[[], TextIO],
        *,
        encoding: str = "utf-8",
        **format_parameters: Any,
    ) -> Rows[dict[str, Any]]:
        """Read CSV rows lazily from a path, caller-owned handle, or owned opener.

        Args:
            path: A path reopened for every execution, an already-open text handle consumed once
                without being closed, or a zero-argument opener whose returned handle is closed
                after each execution.
            encoding: Text encoding used only when fpstreams opens a path.
            **format_parameters: Keyword options forwarded to csv.DictReader, such as
                dialect, delimiter, or quoting.

        Returns:
            Lazy dictionaries keyed by the unique CSV header. Handle inputs are one-shot; paths
            and opener inputs are replayable.
        """
        return Rows(
            csv_flow(
                path,
                encoding=encoding,
                format_parameters=format_parameters,
            )
        )

    @staticmethod
    def scan_csv(
        path: str | os.PathLike[str],
        *,
        batch_size: int = 65_536,
        read_options: Any = None,
        parse_options: Any = None,
        convert_options: Any = None,
        memory_pool: Any = None,
    ) -> Rows[dict[str, Any]]:
        """Lazily scan typed CSV batches with optional query-level column pruning.

        Unlike :meth:`from_csv`, this explicit Arrow path infers non-string scalar types.
        PyArrow options can fix parsing and conversion behavior when inference is unsuitable.
        The incremental Arrow reader is single-threaded and freezes inferred types after its
        first byte block; use ``read_options`` or ``convert_options`` to control those choices.
        Query column pruning intentionally avoids conversion work, including conversion errors,
        in columns the query does not read.

        Args:
            path: Local CSV path reopened for each iteration.
            batch_size: Maximum rows exposed by each retained Arrow batch.
            read_options: Optional ``pyarrow.csv.ReadOptions``.
            parse_options: Optional ``pyarrow.csv.ParseOptions``.
            convert_options: Optional ``pyarrow.csv.ConvertOptions``.
            memory_pool: Optional PyArrow memory pool used by the reader.

        Returns:
            Reusable typed rows. This adapter requires the ``arrow`` extra.
        """
        return Rows(
            Flow(
                csv_source(
                    path,
                    batch_size=batch_size,
                    read_options=read_options,
                    parse_options=parse_options,
                    convert_options=convert_options,
                    memory_pool=memory_pool,
                )
            )
        )

    @staticmethod
    def from_jsonl(
        path: (str | os.PathLike[str] | TextIO | BinaryIO | Callable[[], TextIO | BinaryIO]),
        *,
        encoding: str = "utf-8",
        max_record_bytes: int | None = 8 * 1024 * 1024,
    ) -> Rows[dict[str, Any]]:
        """Read JSON objects lazily from a path, caller-owned handle, or owned opener.

        Args:
            path: A path reopened for every execution, an already-open text or binary handle
                consumed once without being closed, or a zero-argument opener whose returned
                handle is closed after each execution.
            encoding: Encoding used for paths and binary handles, and for byte accounting on text
                handles.
            max_record_bytes: Encoded-byte limit per line, or None for no limit.

        Returns:
            Lazy dictionary rows. Handle inputs are one-shot; paths and opener inputs are
            replayable. Duplicate keys and non-object records fail when consumed.
        """
        return Rows(
            jsonl_flow(
                path,
                encoding=encoding,
                max_record_bytes=max_record_bytes,
            )
        )

    @staticmethod
    def from_arrow(source: Any, *, batch_size: int = 65_536) -> Rows[dict[str, Any]]:
        """Adapt an Arrow object or C Stream provider to dictionary rows.

        Args:
            source: Reusable PyArrow Table/RecordBatch, one-shot RecordBatchReader, or an
                object implementing ``__arrow_c_stream__``. A C Stream provider is imported
                once at construction and treated as one-shot.
            batch_size: Maximum rows converted from each Arrow batch slice.

        Returns:
            Lazy Rows; reader-backed inputs may be consumed only once and are closed afterward.
        """
        return Rows(Flow(arrow_source(source, batch_size=batch_size)))

    @staticmethod
    def from_columns(
        columns: Mapping[str, Any],
        *,
        batch_size: int = 65_536,
    ) -> Rows[dict[str, Any]]:
        """Adapt an explicit mapping of independent columns through a retained Arrow table."""
        return Rows(Flow(columns_source(columns, batch_size=batch_size)))

    @staticmethod
    def from_numpy(
        array: Any,
        *,
        columns: Iterable[str] | None = None,
    ) -> Rows[dict[str, Any]]:
        """Adapt a two-dimensional NumPy array to replayable dictionary rows.

        NumPy conversion happens at construction, but each array row is converted to Python
        scalar values only when consumed. An existing ndarray is retained by reference, while
        other array-like inputs follow ``numpy.asarray`` conversion semantics.

        Args:
            array: Two-dimensional ndarray or array-like input accepted by ``numpy.asarray``.
            columns: Unique non-empty string names matching the array width. Defaults to
                ``"0"``, ``"1"``, and so on.

        Returns:
            Lazy, replayable Rows whose records follow the retained array's row order.
        """
        from .numpy import numpy_source

        return Rows(Flow(numpy_source(array, columns=columns)))

    @staticmethod
    def from_dataframe(
        frame: Any,
        *,
        batch_size: int = 65_536,
        allow_copy: bool = True,
    ) -> Rows[dict[str, Any]]:
        """Adapt an object implementing the dataframe interchange protocol through PyArrow.

        Args:
            frame: Object providing __dataframe__(), optionally with an Arrow C stream.
            batch_size: Maximum rows converted from each Arrow batch.
            allow_copy: Permit interchange conversion to allocate copied buffers.

        Returns:
            Lazy Rows that perform dataframe-to-Arrow conversion when iterated.
        """
        return Rows(
            Flow(
                dataframe_source(
                    frame,
                    batch_size=batch_size,
                    allow_copy=allow_copy,
                )
            )
        )

    from_pandas = from_dataframe

    @staticmethod
    def from_polars(
        frame: Any,
        *,
        batch_size: int = 65_536,
        maintain_order: bool = True,
        engine: Any = "auto",
    ) -> Rows[dict[str, Any]]:
        """Adapt an eager Polars DataFrame or batch-collected LazyFrame to dictionary rows.

        Args:
            frame: Polars DataFrame or LazyFrame to slice or collect.
            batch_size: Rows requested per eager slice or lazy collection batch.
            maintain_order: Preserve LazyFrame row order while collecting batches.
            engine: Polars engine used only for LazyFrame batch collection.

        Returns:
            Lazy reusable Rows; a LazyFrame is collected again for each iteration.
        """
        return Rows(
            Flow(
                polars_source(
                    frame,
                    batch_size=batch_size,
                    maintain_order=maintain_order,
                    engine=engine,
                )
            )
        )

    @staticmethod
    def from_parquet(
        source: Any,
        *,
        columns: Iterable[str] | None = None,
        filter: Any = None,
        batch_size: int = 65_536,
        use_threads: bool = True,
        filesystem: Any = None,
        partitioning: Any = None,
    ) -> Rows[dict[str, Any]]:
        """Build reusable rows from a fresh PyArrow dataset scanner per iteration.

        Args:
            source: PyArrow Dataset or dataset source accepted by pyarrow.dataset().
            columns: Unique projected column names, or None for all columns.
            filter: PyArrow dataset expression pushed into the scanner.
            batch_size: Maximum rows requested from each scanner batch.
            use_threads: Allow the PyArrow scanner to use worker threads.
            filesystem: Optional PyArrow filesystem for resolving the source.
            partitioning: Optional dataset partitioning specification.

        Returns:
            Lazy dictionary rows with projection and filtering performed by PyArrow.
        """
        return Rows(
            Flow(
                parquet_source(
                    source,
                    columns=columns,
                    filter=filter,
                    batch_size=batch_size,
                    use_threads=use_threads,
                    filesystem=filesystem,
                    partitioning=partitioning,
                )
            )
        )

    @staticmethod
    def from_db(
        connect: ConnectionFactory,
        query: str,
        parameters: DBParameters = None,
        *,
        batch_size: int = 1_000,
    ) -> Rows[dict[str, Any]]:
        """Build a reiterable DB-API query source that owns its connections and cursors.

        Args:
            connect: Zero-argument factory called once per iteration for a new connection.
            query: Statement executed by each newly opened cursor.
            parameters: Optional mapping or positional values passed to cursor.execute().
            batch_size: Maximum rows requested by each cursor.fetchmany() call.

        Returns:
            Lazy rows that close the cursor and connection on exhaustion, error, or early stop.
        """
        return Rows(
            flow.defer(
                db_row_factory(
                    connect,
                    query,
                    parameters,
                    batch_size=batch_size,
                )
            )
        )

    @staticmethod
    def from_sqlite(
        database: str | os.PathLike[str],
        query: str,
        parameters: DBParameters = None,
        *,
        batch_size: int = 1_000,
        timeout: float = 5.0,
        uri: bool = False,
    ) -> Rows[dict[str, Any]]:
        """Build a reiterable SQLite query source that owns one connection per iteration.

        Args:
            database: SQLite path or URI passed to sqlite3.connect().
            query: Statement executed by each newly opened cursor.
            parameters: Optional mapping or positional values passed to cursor.execute().
            batch_size: Maximum rows requested by each cursor.fetchmany() call.
            timeout: Seconds sqlite3 waits for a locked database.
            uri: Interpret database as a SQLite URI when true.

        Returns:
            Lazy dictionary rows that close their cursor and connection after iteration.
        """
        return Rows(
            flow.defer(
                sqlite_row_factory(
                    database,
                    query,
                    parameters,
                    batch_size=batch_size,
                    timeout=timeout,
                    uri=uri,
                )
            )
        )

    def __iter__(self) -> Iterator[T]:
        """Execute the underlying Flow and yield records lazily."""
        return iter(self._flow)

    def to_flow(self) -> Flow[T]:
        """Return the underlying Flow without copying data or changing the plan.

        Returns:
            The same lazy Flow that owns this Rows view's source and operations.
        """
        return self._flow

    def explain(self, terminal: TerminalName = "iterate") -> PlanExplanation:
        """Describe this record pipeline's execution plan without consuming it.

        Args:
            terminal: Terminal operation included in engine selection and validation.

        Returns:
            The same structured explanation produced by the underlying Flow.
        """
        return self._flow.explain(terminal)

    def to_list(self) -> list[T]:
        """Execute the record pipeline and collect its rows.

        Returns:
            A list containing the consumed results in encounter order.
        """
        if _BUILTIN_TYPE(self) is not Rows:
            return _BUILTIN_LIST(self)
        handled, values = _try_csv_identity_list(self._flow)
        if handled:
            return cast("list[T]", values)
        return self._flow.to_list()

    def run_with_report(
        self,
        terminal: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> ExecutionResult[Any]:
        """Run one eager Rows terminal and return its value with query-owned metrics."""
        if terminal not in {"to_list", "count", "first", "last"}:
            raise ValueError(f"{terminal!r} is not a reportable eager Rows terminal")
        method = getattr(self, terminal)
        recorder, token = _start_recording(
            terminal,
            str(self._flow._query("iterate").logical.engine),
        )
        started = perf_counter_ns()
        try:
            value = method(*args, **kwargs)
            return recorder.finish(value, perf_counter_ns() - started)
        finally:
            _stop_recording(token)

    def count(self) -> int:
        """Consume the pipeline and count every emitted row.

        Returns:
            The number of rows remaining after all lazy transformations.
        """
        return self._flow.count()

    def with_engine(self, engine: Engine) -> Rows[T]:
        """Return equivalent lazy Rows requesting auto, Python, or native Flow execution."""
        return Rows(self._flow.with_engine(engine))

    def concat(self, *others: Iterable[T] | Flow[T] | Rows[T]) -> Rows[T]:
        """Emit these rows followed by each supplied record source in order.

        Concatenation is lazy and preserves every input record as-is. It does not align fields,
        fill missing values, or infer a common schema.

        Args:
            *others: Rows, Flow, or record iterables opened only after earlier inputs finish.

        Returns:
            A Rows view over the ordered concatenation, or this same view when no source is given.
        """
        if not others:
            return self
        sources = tuple(
            other.to_flow() if _BUILTIN_TYPE(other) is Rows else other for other in others
        )
        return Rows(self._flow.concat(*sources))

    def first(self) -> T:
        """Return the first row and close upstream without requesting an unnecessary tail.

        Returns:
            The first emitted row.

        Raises:
            EmptyFlowError: If the pipeline emits no rows.
        """
        return self._flow.first()

    def last(self) -> T:
        """Consume the pipeline and return its final row, raising EmptyFlowError when empty.

        Returns:
            The last emitted row.
        """
        return self._flow.last()

    def take(self, count: int) -> Rows[T]:
        """Return a lazy prefix that stops and closes upstream after at most count rows.

        Args:
            count: Nonnegative maximum number of rows to emit.

        Returns:
            New Rows preserving encounter order; zero emits nothing.
        """
        return Rows(self._flow.take(count))

    limit = take
    head = take

    def skip(self, count: int) -> Rows[T]:
        """Return lazy rows after discarding the first count upstream items.

        Args:
            count: Nonnegative number of rows to consume without emitting.

        Returns:
            New Rows containing the remaining encounter-ordered rows.
        """
        return Rows(self._flow.drop(count))

    offset = skip

    def unique_by(self, selector: Selector) -> Rows[T]:
        """Keep the first row for each distinct selected key in encounter order.

        Args:
            selector: Field, path, index, expression, or callable producing a hashable key.

        Returns:
            Lazy Rows whose later duplicate keys are omitted.
        """
        return Rows(self._flow.unique_by(selector))

    distinct_by = unique_by

    def filter(self, predicate: Callable[[T], bool]) -> Rows[T]:
        """Keep rows for which predicate returns a truthy result.

        The predicate runs lazily in encounter order, and the parent Rows pipeline remains
        unchanged.

        Args:
            predicate: Callable evaluated once for each upstream row reached.

        Returns:
            New lazy Rows containing only matching rows.
        """
        return Rows(self._flow.filter(predicate))

    def map(self, function: Callable[[T], R]) -> Flow[R]:
        """Map rows into an ordinary Flow whose output may have any shape.

        Args:
            function: Lazily transforms each row into one output value.

        Returns:
            A Flow of transformed values. Call row operations on that Flow to re-enter Rows.
        """
        return self._flow.map(function)

    def flat_map(self, function: Callable[[T], Iterable[R]]) -> Flow[R]:
        """Map rows to iterables and flatten them into an ordinary Flow.

        Args:
            function: Lazily transforms each row into zero or more output values.

        Returns:
            A Flow of flattened values. Call row operations on that Flow to re-enter Rows.
        """
        return self._flow.flat_map(function)

    def where(self, predicate: Callable[[T], bool] | None = None, **equalities: Any) -> Rows[T]:
        """Require the optional predicate and every named field equality.

        Named fields are compiled once, then selected lazily from each consumed row.

        Args:
            predicate: Optional callable that must return truthy for a row.
            **equalities: Top-level or dotted field paths mapped to required values.

        Returns:
            New lazy Rows containing rows that satisfy all supplied conditions.
        """
        # Preserve a closed RowExpr as the physical filter payload.  Wrapping a predicate-only
        # call in ``matches`` would erase its IR and force every row through an opaque callback.
        if predicate is not None and not equalities:
            return self.filter(predicate)

        # Compile equality selectors once; evaluation still happens lazily per row.
        selectors = [(compile_selector(name), expected) for name, expected in equalities.items()]

        def matches(row: T) -> bool:
            """Require both the optional predicate and every named equality to match."""
            if predicate is not None and not predicate(row):
                return False
            return all(select(row) == expected for select, expected in selectors)

        return Rows(
            self._flow.filter(
                _register_row_stage(
                    matches,
                    RowStageDescriptor(
                        "where",
                        predicate=predicate,
                        equalities=tuple(equalities.items()),
                    ),
                )
            )
        )

    def with_columns(self, **columns: Selector) -> Rows[dict[str, Any]]:
        """Copy each row and add or replace fields evaluated against the original row.

        Args:
            **columns: Output field names mapped to selectors or RowExpr values.

        Returns:
            Lazy dictionary Rows; computed columns do not observe earlier additions.
        """
        selectors = [(name, compile_selector(selector)) for name, selector in columns.items()]

        return Rows(
            self._flow.map(
                _register_row_stage(
                    _CANONICAL_BUILD_WITH_COLUMNS_ENRICHER(selectors),
                    RowStageDescriptor("with_columns", selectors=tuple(columns.items())),
                )
            )
        )

    def rename(self, **columns: str) -> Rows[dict[str, Any]]:
        """Rename top-level fields while rejecting collisions in each output record.

        Args:
            **columns: Existing field names mapped to nonempty destination names.

        Returns:
            Lazy copied dictionaries; unmapped fields retain their names and order.
        """
        if any(not name for name in columns.values()):
            raise ValueError("renamed columns cannot be empty")

        transform = _CANONICAL_BUILD_RENAME_TRANSFORM(columns)
        return Rows(
            self._flow.map(
                _register_row_stage(
                    transform,
                    RowStageDescriptor("rename", selectors=tuple(columns.items())),
                )
            )
        )

    def drop(self, *columns: str) -> Rows[dict[str, Any]]:
        """Copy each row without the named top-level fields.

        Args:
            *columns: Field names to omit; absent names are ignored.

        Returns:
            Lazy dictionary Rows preserving the order of retained fields.
        """
        names = frozenset(columns)
        return Rows(
            self._flow.map(
                lambda row: {
                    name: value for name, value in _as_record(row).items() if name not in names
                }
            )
        )

    def cast(self, **columns: Callable[[Any], Any]) -> Rows[dict[str, Any]]:
        """Convert existing named fields with one callable per field.

        Args:
            **columns: Field names mapped to callable value converters.

        Returns:
            Lazy copied dictionaries; a missing field raises SelectionError when consumed.
        """
        if not columns:
            raise ValueError("cast requires at least one named converter")
        if any(not name for name in columns):
            raise ValueError("cast column names cannot be empty")
        converters = tuple(columns.items())
        for name, converter in converters:
            if not callable(converter):
                raise TypeError(f"cast converter for {name!r} must be callable")

        def transform(row: Any) -> dict[str, Any]:
            """Apply each converter to its named field and reject missing columns."""
            record = row.copy() if type(row) is dict else _as_record(row)
            for name, converter in converters:
                if name not in record:
                    raise SelectionError(f"cast column {name!r} is missing")
                record[name] = converter(record[name])
            return record

        return Rows(
            self._flow.map(
                _register_row_stage(
                    transform,
                    RowStageDescriptor("cast", selectors=converters),
                )
            )
        )

    parse = cast

    def fill_nulls(self, **replacements: object) -> Rows[dict[str, Any]]:
        """Replace missing or None named fields with constants or RowExpr results.

        Args:
            **replacements: Field names mapped to literal values or row expressions.

        Returns:
            Lazy copied dictionaries; non-None existing values are preserved.
        """
        if not replacements:
            raise ValueError("fill_nulls requires at least one named replacement")
        if any(not name for name in replacements):
            raise ValueError("fill_nulls column names cannot be empty")
        prepared: list[tuple[str, RowExpr, object, bool]] = []
        for name, value in replacements.items():
            if isinstance(value, RowExpr):
                prepared.append((name, value, None, False))
            else:
                prepared.append((name, lit(value), value, True))
        expressions = tuple(prepared)

        def transform(row: Any) -> dict[str, Any]:
            """Replace selected None fields by evaluating their replacement expressions."""
            record = row.copy() if type(row) is dict else _as_record(row)
            for name, replacement, literal_value, is_literal in expressions:
                if record.get(name) is None:
                    record[name] = literal_value if is_literal else replacement(row)
            return record

        return Rows(
            self._flow.map(
                _register_row_stage(
                    transform,
                    RowStageDescriptor(
                        "fill_nulls",
                        selectors=tuple(replacements.items()),
                    ),
                )
            )
        )

    fillna = fill_nulls

    def drop_nulls(
        self,
        *selectors: Selector,
        how: Literal["any", "all"] = "any",
    ) -> Rows[T]:
        """Drop rows according to None values in selected fields or the whole record.

        Args:
            *selectors: Fields to inspect; omitted means every field in each record.
            how: "any" drops on one null; "all" requires every inspected value to be null.

        Returns:
            Lazy Rows; a missing selected field is treated as None.
        """
        if how not in {"any", "all"}:
            raise ValueError("drop_nulls how must be 'any' or 'all'")
        selected = tuple(compile_selector(selector) for selector in selectors)
        direct_fields = (
            cast(tuple[str, ...], selectors)
            if selectors
            and all(type(selector) is str and "." not in selector for selector in selectors)
            else None
        )

        def select_or_none(select: Callable[[T], Any], row: T) -> Any:
            """Treat a missing selected field as None for null filtering."""
            try:
                return select(row)
            except SelectionError:
                return None

        def keep_compatible(row: T) -> bool:
            """Retain selector and record protocols outside the exact-dictionary path."""
            values = (
                (select_or_none(select, row) for select in selected)
                if selected
                else _as_record(row).values()
            )
            missing = (value is None for value in values)
            return not (any(missing) if how == "any" else all(missing))

        if not selectors:
            return self.filter(_DropNullsPlan(None, how, keep_compatible))
        if direct_fields is None:
            return self.filter(keep_compatible)
        return self.filter(_DropNullsPlan(direct_fields, how, keep_compatible))

    dropna = drop_nulls

    def explode(
        self,
        selector: Selector,
        *,
        into: str | None = None,
        outer: bool = False,
    ) -> Rows[dict[str, Any]]:
        """Expand a selected iterable into one copied row per element.

        Args:
            selector: Selector returning a non-string iterable or None.
            into: Output field name; required for non-top-level selectors.
            outer: Emit one row with None when the selected value is None or empty.

        Returns:
            Lazy flattened dictionary Rows that close upstream on downstream stop.
        """
        if into is None:
            if not isinstance(selector, str) or not selector or "." in selector:
                raise ValueError("explode into is required for non-top-level selectors")
            output_name = selector
        else:
            if not into:
                raise ValueError("explode output name cannot be empty")
            output_name = into
        select = compile_selector(selector)
        direct_field = selector if type(selector) is str and "." not in selector else None

        return Rows(
            self._flow.flat_map(_RowExpansionPlan.explode(select, output_name, outer, direct_field))
        )

    def unnest(self, column: str, *, prefix: str = "") -> Rows[dict[str, Any]]:
        """Replace one top-level nested record with its fields.

        Args:
            column: Non-dotted field name containing a supported record-like value.
            prefix: Text prepended to every promoted nested field.

        Returns:
            Lazy copied dictionaries; output-name collisions raise DuplicateKeyError.
        """
        if not column or "." in column:
            raise ValueError("unnest column must be a top-level name")
        return Rows(self._flow.map(_CANONICAL_BUILD_UNNEST_TRANSFORM(column, prefix)))

    def unpivot(
        self,
        *columns: str,
        names_to: str = "variable",
        values_to: str = "value",
    ) -> Rows[dict[str, Any]]:
        """Convert selected top-level fields from wide form into name/value rows.

        Args:
            *columns: Unique fields expanded in the given order.
            names_to: Noncolliding output field for each former column name.
            values_to: Noncolliding output field for each former column value.

        Returns:
            Lazy Rows emitting len(columns) records per input row.
        """
        if not columns:
            raise ValueError("unpivot requires at least one column")
        _require_unique_names(columns, operation="unpivot")
        _require_unique_names((names_to, values_to), operation="unpivot")
        return Rows(self._flow.flat_map(_RowExpansionPlan.unpivot(columns, names_to, values_to)))

    def pivot(  # noqa: C901 - keep selector setup beside its measured reshape loops
        self,
        *,
        index: Selector | tuple[Selector, ...],
        columns: Selector,
        values: Selector,
        aggregate: str | Callable[[Any, Any], Any] = "error",
        fill: Any = None,
    ) -> Rows[dict[str, Any]]:
        """Materialize long-form rows into encounter-ordered wide records.

        Args:
            index: Selector or selector tuple defining each output row and its key fields.
            columns: Selector whose values become dynamic output field names.
            values: Selector producing each pivot cell value.
            aggregate: Duplicate-cell policy: error, first, last, sum, or a reducer callable.
            fill: Value inserted for missing cells among discovered columns.

        Returns:
            Lazy pipeline that builds the full pivot only when consumed.
        """
        reducers = {"error", "first", "last", "sum"}
        canonical_callable = _BUILTIN_CALLABLE
        canonical_str = _BUILTIN_STR
        if callable(aggregate):
            aggregate_is_callable = True
        else:
            aggregate_is_callable = False
            if aggregate not in reducers:
                raise ValueError(f"aggregate must be callable or one of {sorted(reducers)!r}")

        index_fields = index if isinstance(index, tuple) else (index,)
        if not index_fields:
            raise ValueError("pivot index requires at least one selector")
        key_selectors = [compile_selector(selector) for selector in index_fields]
        key_names = [
            selector.split(".")[-1] if isinstance(selector, str) else f"key_{position}"
            for position, selector in enumerate(index_fields)
        ]
        _require_unique_names(key_names, operation="pivot")
        column_selector = compile_selector(columns)
        value_selector = compile_selector(values)
        direct_fields = (
            all(
                type(selector) is canonical_str and "." not in selector for selector in index_fields
            )
            and type(columns) is canonical_str
            and "." not in columns
            and type(values) is canonical_str
            and "." not in values
        )
        direct_indexes = cast(tuple[str, ...], index_fields) if direct_fields else ()
        direct_column = cast(str, columns) if direct_fields else ""
        direct_value = cast(str, values) if direct_fields else ""
        exact_key_names = True
        for key_name in key_names:
            if type(key_name) is not canonical_str:
                exact_key_names = False
                break
        key_name_set = _BUILTIN_FROZENSET(key_names) if exact_key_names else None
        native_key_names = _BUILTIN_TUPLE(key_names) if direct_fields else ()
        single_index = len(key_names) == 1
        canonical_dict = _BUILTIN_DICT
        canonical_zip = _BUILTIN_ZIP
        aggregate_function = (
            cast(Callable[[Any, Any], Any], aggregate)
            if aggregate_is_callable and type(aggregate) in (FunctionType, BuiltinFunctionType)
            else None
        )

        def evaluate_compatible() -> Iterator[dict[str, Any]]:  # noqa: C901
            """Evaluate general selectors and multi-field keys through canonical callables."""
            groups: dict[tuple[Any, ...], dict[str, Any]] = {}
            spare_cells: dict[str, Any] = {}
            column_names: list[str] = []
            seen_columns: set[str] = set()
            exact_columns = key_name_set is not None
            iterator = iter(self)
            active_error: BaseException | None = None
            try:
                for row in iterator:
                    key = tuple(select(row) for select in key_selectors)
                    column = str(column_selector(row))
                    if exact_columns and key_name_set is not None and type(column) is canonical_str:
                        if column in key_name_set:
                            raise ValueError(
                                f"pivot column {column!r} collides with an index column"
                            )
                        if column not in seen_columns:
                            seen_columns.add(column)
                            column_names.append(column)
                    else:
                        exact_columns = False
                        if column in key_names:
                            raise ValueError(
                                f"pivot column {column!r} collides with an index column"
                            )
                        if column not in column_names:
                            column_names.append(column)
                    cells = groups.setdefault(key, spare_cells)
                    if cells is spare_cells:
                        spare_cells = {}
                    value = value_selector(row)
                    if column not in cells:
                        cells[column] = value
                    elif aggregate_function is not None and callable is canonical_callable:
                        cells[column] = aggregate_function(cells[column], value)
                    elif not (
                        type(aggregate) is canonical_str and callable is canonical_callable
                    ) and callable(aggregate):
                        cells[column] = aggregate(cells[column], value)
                    elif aggregate == "error":
                        raise DuplicateKeyError(
                            f"multiple values for pivot key {key!r}, column {column!r}"
                        )
                    elif aggregate == "last":
                        cells[column] = value
                    elif aggregate == "sum":
                        cells[column] += value
            except BaseException as error:
                active_error = error
                raise
            finally:
                close_iterators((iterator,), active_error=active_error)

            # One exact-string template moves sparse-wide filling into dict.copy/update. A single
            # output group keeps the canonical one-dictionary path to avoid doubling peak width.
            template_eligible = exact_columns and _BUILTIN_LEN(groups) > 1
            template: dict[str, Any] | None = None
            for key, cells in groups.items():
                record_dict = dict
                record_zip = zip
                if (
                    template_eligible
                    and record_dict is canonical_dict
                    and record_zip is canonical_zip
                ):
                    if template is None:
                        template = canonical_dict.fromkeys((*key_names, *column_names), fill)
                    record = template.copy()
                    if single_index:
                        record[key_names[0]] = key[0]
                    else:
                        record.update(canonical_zip(key_names, key, strict=True))
                    record.update(cells)
                    yield record
                    continue
                record = record_dict(record_zip(key_names, key, strict=True))
                if exact_columns:
                    for name in column_names:
                        record[name] = cells.get(name, fill)
                else:
                    record.update({name: cells.get(name, fill) for name in column_names})
                yield record

        def evaluate_direct() -> Iterator[dict[str, Any]]:  # noqa: C901
            """Use direct fields while retaining canonical tuple-shaped index keys."""
            if _BUILTIN_TYPE(aggregate) is canonical_str and aggregate == "error":
                logical = self._flow._logical_plan
                root = logical.root
                if (
                    _BUILTIN_TYPE(self) is Rows
                    and _BUILTIN_TYPE(self._flow) is Flow
                    and logical.engine == "auto"
                    and logical.parallel is None
                    and _BUILTIN_TYPE(root) is SourceNode
                    and _BUILTIN_TYPE(root.source) is Source
                ):
                    retained = root.source.retained_sequence()
                    if retained is not None:
                        native = _try_native_pivot_materialize(
                            retained,
                            direct_indexes,
                            direct_column,
                            direct_value,
                            native_key_names,
                            fill,
                        )
                        if native is not None:
                            yield from native
                            return
            groups: dict[tuple[Any, ...], dict[str, Any]] = {}
            spare_cells: dict[str, Any] = {}
            column_names: list[str] = []
            seen_columns: set[str] = set()
            exact_columns = key_name_set is not None
            iterator = iter(self)
            active_error: BaseException | None = None
            try:
                for row in iterator:
                    record: dict[str, Any] = row  # type: ignore[assignment]
                    exact_record = type(record) is dict
                    if exact_record:
                        if single_index:
                            field = direct_indexes[0]
                            try:
                                key = (record[field],)
                            except (AttributeError, KeyError, TypeError) as error:
                                raise SelectionError(
                                    f"Could not resolve selector {field!r}; failed at {field!r}"
                                ) from error
                        else:
                            direct_key_parts: list[Any] = []
                            for field in direct_indexes:
                                try:
                                    direct_key_parts.append(record[field])
                                except (AttributeError, KeyError, TypeError) as error:
                                    raise SelectionError(
                                        f"Could not resolve selector {field!r}; failed at {field!r}"
                                    ) from error
                            key = tuple(direct_key_parts)
                        try:
                            selected_column = record[direct_column]
                        except (AttributeError, KeyError, TypeError) as error:
                            raise SelectionError(
                                f"Could not resolve selector {direct_column!r}; "
                                f"failed at {direct_column!r}"
                            ) from error
                        column = str(selected_column)
                    else:
                        key = tuple(select(row) for select in key_selectors)
                        column = str(column_selector(row))
                    if exact_columns and key_name_set is not None and type(column) is canonical_str:
                        if column in key_name_set:
                            raise ValueError(
                                f"pivot column {column!r} collides with an index column"
                            )
                        if column not in seen_columns:
                            seen_columns.add(column)
                            column_names.append(column)
                    else:
                        exact_columns = False
                        if column in key_names:
                            raise ValueError(
                                f"pivot column {column!r} collides with an index column"
                            )
                        if column not in column_names:
                            column_names.append(column)
                    cells = groups.setdefault(key, spare_cells)
                    if cells is spare_cells:
                        spare_cells = {}
                    if exact_record:
                        try:
                            value = record[direct_value]
                        except (AttributeError, KeyError, TypeError) as error:
                            raise SelectionError(
                                f"Could not resolve selector {direct_value!r}; "
                                f"failed at {direct_value!r}"
                            ) from error
                    else:
                        value = value_selector(row)
                    if column not in cells:
                        cells[column] = value
                    elif aggregate_function is not None and callable is canonical_callable:
                        cells[column] = aggregate_function(cells[column], value)
                    elif not (
                        type(aggregate) is canonical_str and callable is canonical_callable
                    ) and callable(aggregate):
                        cells[column] = aggregate(cells[column], value)
                    elif aggregate == "error":
                        raise DuplicateKeyError(
                            f"multiple values for pivot key {key!r}, column {column!r}"
                        )
                    elif aggregate == "last":
                        cells[column] = value
                    elif aggregate == "sum":
                        cells[column] += value
            except BaseException as error:
                active_error = error
                raise
            finally:
                close_iterators((iterator,), active_error=active_error)

            template_eligible = exact_columns and _BUILTIN_LEN(groups) > 1
            template: dict[str, Any] | None = None
            for key, cells in groups.items():
                record_dict = dict
                record_zip = zip
                if (
                    template_eligible
                    and record_dict is canonical_dict
                    and record_zip is canonical_zip
                ):
                    if template is None:
                        template = canonical_dict.fromkeys((*key_names, *column_names), fill)
                    record = template.copy()
                    if single_index:
                        record[key_names[0]] = key[0]
                    else:
                        record.update(canonical_zip(key_names, key, strict=True))
                    record.update(cells)
                    yield record
                    continue
                record = record_dict(record_zip(key_names, key, strict=True))
                if exact_columns:
                    for name in column_names:
                        record[name] = cells.get(name, fill)
                else:
                    record.update({name: cells.get(name, fill) for name in column_names})
                yield record

        return Rows(flow.defer(evaluate_direct if direct_fields else evaluate_compatible))

    def select(self, *selectors: str | int, **named: Selector) -> Rows[dict[str, Any]]:
        """Project positional and named selectors into new dictionaries.

        Args:
            *selectors: String paths or integer indexes; output names are derived automatically.
            **named: Explicit output names mapped to any supported selector.

        Returns:
            Lazy projected Rows; duplicate derived or explicit names are rejected immediately.
        """
        positional_specs = [
            (
                selector.split(".")[-1] if isinstance(selector, str) else str(selector),
                selector,
            )
            for selector in selectors
        ]
        positional = [
            (
                name,
                compile_selector(selector),
            )
            for name, selector in positional_specs
        ]
        aliases = [(name, compile_selector(selector)) for name, selector in named.items()]
        _require_unique_names(
            (name for name, _select in (*positional, *aliases)),
            operation="select",
        )

        project = _CANONICAL_BUILD_SELECT_PROJECT(positional, aliases)
        return Rows(
            self._flow.map(
                _register_row_stage(
                    project,
                    RowStageDescriptor(
                        "select", selectors=tuple((*positional_specs, *named.items()))
                    ),
                )
            )
        )

    def sort_by(
        self,
        selector: Selector,
        *,
        reverse: bool = False,
        buffer_size: int | None = None,
        tempdir: str | os.PathLike[str] | None = None,
    ) -> Rows[T]:
        """Sort rows by a selected key, in memory or through bounded external runs.

        Args:
            selector: Field, path, index, expression, or callable producing the sort key.
            reverse: Emit descending order when true.
            buffer_size: None for full in-memory sort, or positive rows per spilled run.
            tempdir: Parent directory for automatically cleaned external-sort files.

        Returns:
            Lazy stably sorted Rows.
        """
        return Rows(
            self._flow.sort_by(
                selector,
                reverse=reverse,
                buffer_size=buffer_size,
                tempdir=tempdir,
            )
        )

    def external_sort_by(
        self,
        selector: Selector,
        *,
        reverse: bool = False,
        buffer_size: int = 100_000,
        tempdir: str | os.PathLike[str] | None = None,
    ) -> Rows[T]:
        """Sort rows stably with bounded in-memory runs and temporary files.

        Each run holds at most buffer_size rows; the lazy merge closes upstream and removes
        temporary files after completion, failure, or downstream short-circuit.

        Args:
            selector: Field, path, index, expression, or callable producing the sort key.
            reverse: Emit descending order when true.
            buffer_size: Positive maximum rows held in each sorted run.
            tempdir: Parent directory for automatically cleaned run files.

        Returns:
            Lazy externally sorted Rows.
        """
        return self.sort_by(
            selector,
            reverse=reverse,
            buffer_size=buffer_size,
            tempdir=tempdir,
        )

    def aggregate(self, **aggregations: Aggregator) -> Rows[dict[str, Any]]:
        """Run named Aggregators and return a one-row pipeline.

        The computation is deferred and produces a `Rows` pipeline containing one result record.

        Args:
            **aggregations: Named aggregators evaluated during the same traversal.

        Returns:
            A lazy one-row pipeline containing the named results.
        """
        aggregation_items = prepare_aggregations(aggregations)
        logical = self._flow._logical_plan
        return Rows(
            Flow._from_logical(
                logical.with_root(GlobalAggregateNode(logical.root, aggregation_items))
            )
        )

    def group_by(self, *selectors: Selector, **named: Selector) -> GroupedRows[T]:
        """Describe grouped aggregation by positional and/or explicitly named selectors.

        Args:
            *selectors: Keys named from field paths or as key_N for other selector types.
            **named: Explicit output key names mapped to supported selectors.

        Returns:
            GroupedRows configuration; no source rows are read until aggregate() is consumed.
        """
        # Grouping remains deferred; rows are read only when aggregate() is consumed.
        if not selectors and not named:
            raise ValueError("group_by requires at least one selector")
        positional = tuple(
            (
                (selector.split(".")[-1] if isinstance(selector, str) else f"key_{position}"),
                selector,
            )
            for position, selector in enumerate(selectors)
        )
        keys = (*positional, *named.items())
        if any(not name for name, _selector in keys):
            raise ValueError("group_by names cannot be empty")
        _require_unique_names(
            (name for name, _selector in keys),
            operation="group_by",
        )
        return GroupedRows(self, keys)

    def join(
        self,
        other: Iterable[Any] | Flow[Any] | Rows[Any],
        *,
        on: JoinSelector | None = None,
        left_on: JoinSelector | None = None,
        right_on: JoinSelector | None = None,
        how: str = "inner",
        suffix: str = "_right",
        validate: JoinValidation = "m:m",
        partitions: int | None = None,
        tempdir: str | os.PathLike[str] | None = None,
        limits: SpillLimits | None = None,
    ) -> Rows[dict[str, Any]]:
        """Join this record pipeline with another source.

        Joins are lazy and preserve stable input order. Inner, left, semi, and anti joins stream
        the left source after indexing the right source. Right and full joins materialize both
        sides. Set partitions to use bounded-memory hash partitioning through temporary files.

        Args:
            other: The record iterable, Flow, or Rows pipeline to join.
            on: A selector used for both left and right keys.
            left_on: The left key selector when the two sides use different fields.
            right_on: The right key selector when the two sides use different fields.
            how: One of inner, left, right, full, semi, or anti.
            suffix: Text appended to conflicting right-side field names.
            validate: Expected key cardinality. 1:m requires unique left keys, m:1 requires
                unique right keys, 1:1 requires both, and m:m permits duplicates on both sides.
            partitions: Number of hash partitions for bounded-memory execution. Must be between
                2 and 256.
            tempdir: Parent directory for temporary partition files. Requires partitions.
            limits: Finite partition, match, and output budgets for spilled execution.

        Returns:
            Lazy dictionary Rows that execute the selected in-memory or spilled join when consumed.

        Raises:
            ValueError: If selectors, modes, partition options, or key cardinality are invalid.
            TypeError: If a key is unhashable or spilled data cannot be serialized.
            DuplicateKeyError: If suffixing would create an ambiguous output field.
            BufferLimitError: If spilled execution exceeds a configured resource budget.
        """
        if how not in _JOIN_MODES:
            raise ValueError(f"how must be one of {sorted(_JOIN_MODES)!r}")
        if validate not in _JOIN_VALIDATIONS:
            raise ValueError(f"validate must be one of {sorted(_JOIN_VALIDATIONS)!r}")
        normalized_left, normalized_right = _normalize_join_selectors(
            on=on,
            left_on=left_on,
            right_on=right_on,
        )
        # Preserve the current eager selector validation without retaining evaluators in the plan.
        _compile_join_selector(normalized_left)
        _compile_join_selector(normalized_right)
        partition_count = None if partitions is None else validate_partitions(partitions)
        if tempdir is not None and partition_count is None:
            raise ValueError("tempdir requires partitions")
        if limits is not None and partition_count is None:
            raise ValueError("limits requires partitions")
        if isinstance(other, Rows):
            right_flow = other._flow
        elif isinstance(other, Flow):
            right_flow = other
        else:
            right_flow = flow(other)
        left_logical = self._flow._logical_plan
        right_logical = right_flow._logical_plan
        joined_logical = left_logical.with_engine(
            merge_engine_requests(left_logical.engine, right_logical.engine, operation="join")
        )
        return Rows(
            Flow._from_logical(
                joined_logical.with_root(
                    JoinNode(
                        left_logical.root,
                        right_logical.root,
                        JoinSpec(
                            normalized_left,
                            normalized_right,
                            how,
                            suffix,
                            validate,
                            partition_count,
                            tempdir,
                            limits,
                        ),
                    )
                )
            )
        )
