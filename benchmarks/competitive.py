"""Cross-library benchmark measurements and human-readable comparison tables."""

from __future__ import annotations

import csv
import fnmatch
import gc
import hashlib
import json
import math
import operator
import platform
import statistics
import time
from array import array as python_array
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from itertools import islice, takewhile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, TypedDict, cast

import fpstreams
from fpstreams import item

try:
    from itertools import batched
except ImportError:  # pragma: no cover - exercised by the Python 3.11 support job

    def batched(iterable: Iterable[Any], width: int) -> Iterator[tuple[Any, ...]]:
        """Python 3.11-compatible form of itertools.batched()."""
        if width < 1:
            raise ValueError("width must be at least one")
        iterator = iter(iterable)
        while batch := tuple(islice(iterator, width)):
            yield batch


Verdict = Literal["faster", "slower", "same"]
Library = Literal["fpstreams", "python", "numpy", "pandas"]
Task = Callable[[], object]
Normalizer = Callable[[object], object]
Equality = Callable[[object, object], bool]
_MIN_MEANINGFUL_DELTA_RATIO = 0.02


def _file_sha256(path: Path) -> str:
    """Fingerprint benchmark code or a loaded extension for artifact provenance."""
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _python_package_sha256(root: Path) -> str:
    """Fingerprint importable Python sources and type metadata in stable path order."""
    digest = hashlib.sha256()
    paths = sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and (path.suffix in {".py", ".pyi"} or path.name == "py.typed")
    )
    for path in paths:
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class CaseSpec:
    """Lightweight competitive case metadata that never constructs benchmark inputs."""

    case_id: str
    api: str
    scope: Literal["compute-only", "end-to-end"] = "compute-only"
    quick: bool = False


@dataclass(frozen=True, slots=True)
class Implementation:
    """One fully consuming implementation of a competitive workload."""

    library: Library
    task: Task
    normalize: Normalizer
    variant: str | None = None


@dataclass(frozen=True, slots=True)
class CompetitiveCase:
    """One fpstreams workload and every library with a natural equivalent."""

    spec: CaseSpec
    candidate: Implementation
    references: tuple[Implementation, ...]
    outputs_equal: Equality
    ceilings: tuple[Implementation, ...] = ()


class _NominalRecord(Mapping[str, Any]):
    """Small non-dict Mapping used to exercise public protocol paths."""

    __slots__ = ("_values",)

    def __init__(self, **values: Any) -> None:
        self._values = values

    def __getitem__(self, name: str) -> Any:
        return self._values[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


_CASE_SPECS = (
    CaseSpec("flow.map", "Flow.map(...).to_list()", quick=True),
    CaseSpec("flow.filter", "Flow.filter(...).to_list()", quick=True),
    CaseSpec(
        "flow.to_numpy.int64",
        "Flow.to_numpy(dtype=int64) [preconstructed Python list]",
        quick=True,
    ),
    CaseSpec("flow.map_filter.sum", "Flow.map(...).filter(...).sum()", quick=True),
    CaseSpec(
        "flow.numpy.map_filter.sum",
        "Flow.from_numpy(1D int64).map(...).filter(...).sum()",
        quick=True,
    ),
    CaseSpec(
        "flow.numpy.map_filter.list",
        "Flow.from_numpy(1D int64).map(...).filter(...).to_list()",
    ),
    CaseSpec(
        "flow.numpy.float.map_filter.sum",
        "Flow.from_numpy(1D float64).map(...).filter(...).sum()",
        quick=True,
    ),
    CaseSpec(
        "flow.numpy.float.map_filter.list",
        "Flow.from_numpy(1D float64).map(...).filter(...).to_list()",
    ),
    CaseSpec(
        "flow.numpy.float.list",
        "Flow.from_numpy(1D float64).to_list()",
    ),
    CaseSpec(
        "flow.map_filter.sum.one_shot.callable",
        "Flow.map(callable).filter(callable).sum() [fresh one-shot source]",
        "end-to-end",
    ),
    CaseSpec("flow.flat_map", "Flow.flat_map(...).to_list()"),
    CaseSpec("flow.take", "Flow.take(...).to_list()"),
    CaseSpec("flow.drop", "Flow.drop(...).to_list()"),
    CaseSpec("flow.take_while", "Flow.take_while(...).to_list()"),
    CaseSpec(
        "flow.unique.low_cardinality",
        "Flow.unique().to_list() [low cardinality]",
        quick=True,
    ),
    CaseSpec(
        "flow.unique.high_cardinality",
        "Flow.unique().to_list() [high cardinality]",
    ),
    CaseSpec("flow.sort", "Flow.sort().to_list()"),
    CaseSpec("flow.chunk", "Flow.chunk(...).to_list()"),
    CaseSpec("flow.window", "Flow.window(...).to_list()"),
    CaseSpec("flow.scan", "Flow.scan(0, operator.add).to_list() [callable]"),
    CaseSpec("terminal.sum", "Flow.sum()", quick=True),
    CaseSpec("terminal.count", "Flow.count()"),
    CaseSpec("terminal.mean", "Flow.mean()", quick=True),
    CaseSpec("terminal.variance", "Flow.variance()"),
    CaseSpec("terminal.std", "Flow.std()"),
    CaseSpec("terminal.min", "Flow.min()"),
    CaseSpec("terminal.max", "Flow.max()"),
    CaseSpec("terminal.numpy.sum", "Flow.from_numpy(1D int64).sum()", quick=True),
    CaseSpec("terminal.numpy.mean", "Flow.from_numpy(1D int64).mean()", quick=True),
    CaseSpec(
        "terminal.numpy.variance",
        "Flow.from_numpy(1D int64).variance()",
    ),
    CaseSpec("terminal.numpy.min", "Flow.from_numpy(1D int64).min()"),
    CaseSpec("terminal.numpy.max", "Flow.from_numpy(1D int64).max()"),
    CaseSpec("terminal.numpy.float.sum", "Flow.from_numpy(1D float64).sum()", quick=True),
    CaseSpec("terminal.numpy.float.mean", "Flow.from_numpy(1D float64).mean()", quick=True),
    CaseSpec(
        "terminal.numpy.float.variance",
        "Flow.from_numpy(1D float64).variance()",
    ),
    CaseSpec("terminal.any.expression", "Flow.any(item == ...) [expression]"),
    CaseSpec("terminal.any.callable", "Flow.any(lambda value: ...) [callable]"),
    CaseSpec("terminal.all.expression", "Flow.all(item >= ...) [expression]"),
    CaseSpec("terminal.all.callable", "Flow.all(lambda value: ...) [callable]"),
    CaseSpec(
        "terminal.frequencies.low_cardinality",
        "Flow.frequencies() [low cardinality]",
    ),
    CaseSpec(
        "terminal.frequencies.high_cardinality",
        "Flow.frequencies() [high cardinality]",
    ),
    CaseSpec(
        "rows.numpy.identity",
        "Rows.from_numpy(2D int64).to_list() [preconstructed input]",
        quick=True,
    ),
    CaseSpec(
        "rows.numpy.select",
        "Rows.from_numpy(2D int64).select(...).to_list() [direct fields]",
        quick=True,
    ),
    CaseSpec(
        "rows.numpy.filter_select",
        "Rows.from_numpy(2D int64).where(direct integer comparison).select(...).to_list()",
        quick=True,
    ),
    CaseSpec(
        "rows.numpy.group_aggregate.low_cardinality",
        "Rows.from_numpy(2D int64).group_by(...).aggregate(count/sum/min/max) [low cardinality]",
        quick=True,
    ),
    CaseSpec(
        "rows.numpy.group_aggregate.high_cardinality",
        "Rows.from_numpy(2D int64).group_by(...).aggregate(count/sum/min/max) [high cardinality]",
    ),
    CaseSpec(
        "rows.numpy.aggregate",
        "Rows.from_numpy(2D int64).aggregate(count/sum/min/max)",
        quick=True,
    ),
    CaseSpec("rows.filter", "Rows.filter(...).to_list()", quick=True),
    CaseSpec("rows.select", "Rows.select(...).to_list()", quick=True),
    CaseSpec(
        "rows.with_columns.expression",
        "Rows.with_columns(next_value=col('value') + 1) [expression]",
    ),
    CaseSpec(
        "rows.with_columns.callable",
        "Rows.with_columns(next_value=lambda row: ...) [callable]",
    ),
    CaseSpec("rows.cast", "Rows.cast(...).to_list()"),
    CaseSpec("rows.fill_nulls", "Rows.fill_nulls(...).to_list()"),
    CaseSpec("rows.drop_nulls", "Rows.drop_nulls(...).to_list()"),
    CaseSpec("rows.explode", "Rows.explode(...).to_list()"),
    CaseSpec("rows.unnest", "Rows.unnest(...).to_list()"),
    CaseSpec("rows.unpivot", "Rows.unpivot(...).to_list()"),
    CaseSpec("rows.pivot", "Rows.pivot(...).to_list()"),
    CaseSpec("rows.sort", "Rows.sort_by(...).to_list()"),
    CaseSpec(
        "rows.aggregate.multi",
        "Rows.aggregate(count/sum/min/max) [preconstructed Python record list]",
        quick=True,
    ),
    CaseSpec("rows.aggregate", "Rows.aggregate(...).to_list()"),
    CaseSpec(
        "rows.group_sum.low_cardinality",
        "Rows.group_by(...).aggregate(sum)",
        quick=True,
    ),
    CaseSpec(
        "rows.group_sum.high_cardinality",
        "Rows.group_by(...).aggregate(sum) [high cardinality]",
    ),
    CaseSpec(
        "rows.group_sum.30k_cardinality.mapping_callable",
        "Rows.group_by(callable).aggregate(sum(callable)) [nominal Mapping, up to 30k groups]",
    ),
    CaseSpec(
        "rows.join.inner.unique",
        "Rows.join(..., how='inner') [m:1]",
        quick=True,
    ),
    CaseSpec("rows.join.left.unique", "Rows.join(..., how='left') [m:1]"),
    CaseSpec("rows.join.inner.many", "Rows.join(..., how='inner') [m:m]"),
    CaseSpec(
        "rows.join.inner.unique.mapping_callable",
        "Rows.join(..., callable keys) [nominal Mapping, m:1]",
    ),
    CaseSpec(
        "rows.join.inner.many.mapping_callable",
        "Rows.join(..., callable keys) [nominal Mapping, m:m]",
    ),
    CaseSpec(
        "pairs.map_values.half_cardinality",
        "Pairs.map_values(...).to_dict() [50% cardinality]",
    ),
    CaseSpec(
        "pairs.map_values.expression.half_cardinality",
        "Pairs.map_values(item * 2).to_dict() [50% cardinality]",
    ),
    CaseSpec(
        "pairs.filter.half_cardinality",
        "Pairs.filter_pairs(...).to_dict() [50% cardinality]",
    ),
    CaseSpec(
        "pairs.filter_values.expression.half_cardinality",
        "Pairs.filter_values(item % 2 == 0).to_dict() [50% cardinality]",
    ),
    CaseSpec(
        "pairs.filter_pairs.expression.half_cardinality",
        "Pairs.filter_pairs((col(0) + col(1)) % 3 == 0).to_dict() [50% cardinality]",
        quick=True,
    ),
    CaseSpec(
        "pairs.unique_keys.low_cardinality",
        "Pairs.unique_keys().to_dict() [low cardinality]",
    ),
    CaseSpec(
        "pairs.unique_keys.high_cardinality",
        "Pairs.unique_keys().to_dict() [high cardinality]",
    ),
    CaseSpec(
        "pairs.aggregate_values.low_cardinality",
        "Pairs.aggregate_values(sum=agg.sum()) [low cardinality]",
        quick=True,
    ),
    CaseSpec(
        "pairs.aggregate_values.high_cardinality",
        "Pairs.aggregate_values(sum=agg.sum()) [high cardinality]",
    ),
    CaseSpec("io.csv.read", "Rows.from_csv(...).to_list()", "end-to-end", quick=True),
    CaseSpec(
        "io.jsonl.read",
        "Rows.from_jsonl(...).to_list() [strict duplicate-key validation]",
        "end-to-end",
    ),
    CaseSpec("io.dataframe.read", "Rows.from_dataframe(...).to_list()", "end-to-end"),
    CaseSpec(
        "io.numpy.ndarray_to_named_rows",
        "Rows.from_numpy(..., columns=...).to_list() [fresh 2D ndarray -> named rows]",
        "end-to-end",
        quick=True,
    ),
    CaseSpec(
        "io.numpy.record_rows_to_array",
        "Rows(...).to_numpy(...) [fresh record rows -> 2D array]",
        "end-to-end",
        quick=True,
    ),
)


def list_competitive_cases(
    *,
    quick: bool = False,
    include: Sequence[str] = (),
) -> tuple[str, ...]:
    """List selected case identifiers without importing competitors or allocating inputs."""
    specs = (spec for spec in _CASE_SPECS if not quick or spec.quick)
    if include:
        specs = (
            spec
            for spec in specs
            if any(fnmatch.fnmatch(spec.case_id, pattern) for pattern in include)
        )
    return tuple(spec.case_id for spec in specs)


def _identity(value: object) -> object:
    return value


def _normalize_vector(value: object) -> object:
    to_list = getattr(value, "tolist", None)
    return to_list() if callable(to_list) else list(cast(Iterable[Any], value))


def _read_strict_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read the same bounded, strict JSONL contract as ``Rows.from_jsonl``."""
    line_number = 0

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for name, field_value in pairs:
            if name in value:
                raise fpstreams.DuplicateKeyError(
                    f"JSON Lines record {line_number} contains duplicate key {name!r}"
                )
            value[name] = field_value
        return value

    decode = json.JSONDecoder(object_pairs_hook=unique_object).decode
    record_limit = 8 * 1024 * 1024
    records: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        while raw_line := handle.readline(record_limit + 1):
            line_number += 1
            if len(raw_line) > record_limit:
                raise fpstreams.BufferLimitError(
                    f"JSON Lines record {line_number} bytes {len(raw_line)} exceed "
                    f"max_record_bytes={record_limit}"
                )
            line = raw_line.decode("utf-8")
            if not line or line.isspace():
                continue
            value = decode(line)
            if type(value) is not dict:
                raise fpstreams.SelectionError(f"JSON Lines record {line_number} is not an object")
            records.append(value)
    return records


def _scalar(value: object) -> object:
    item_method = getattr(value, "item", None)
    return item_method() if callable(item_method) else value


def _float_equal(left: object, right: object) -> bool:
    return abs(float(left) - float(right)) <= 1e-9 * max(1.0, abs(float(right)))


def _optional_float_equal(left: object, right: object) -> bool:
    def missing(value: object) -> bool:
        if value is None:
            return True
        try:
            return math.isnan(float(value))
        except (TypeError, ValueError):
            return False

    if missing(left) or missing(right):
        return missing(left) and missing(right)
    return _float_equal(left, right)


def _normalize_null_records(value: object) -> object:
    return [
        {
            name: None if isinstance(item_value, float) and math.isnan(item_value) else item_value
            for name, item_value in row.items()
        }
        for row in value  # type: ignore[union-attr]
    ]


def _typed_scalar(value: object) -> tuple[str, str, object]:
    value_type = type(value)
    return value_type.__module__, value_type.__qualname__, value


def _normalize_exact_records(value: object) -> object:
    if type(value) is not list:
        raise TypeError("record output must be a list")
    records: list[tuple[tuple[str, tuple[str, str, object]], ...]] = []
    for row in value:
        if type(row) is not dict:
            raise TypeError("every record must be a dictionary")
        records.append(tuple((name, _typed_scalar(item_value)) for name, item_value in row.items()))
    return tuple(records)


def _normalize_exact_matrix(value: object) -> object:
    reported_shape = getattr(value, "shape", None)
    shape: tuple[int, int] | None = None
    if reported_shape is not None:
        dimensions = tuple(int(dimension) for dimension in reported_shape)
        if len(dimensions) != 2:
            raise ValueError("matrix output must be two-dimensional")
        shape = (dimensions[0], dimensions[1])

    to_list = getattr(value, "tolist", None)
    matrix = to_list() if callable(to_list) else value
    if type(matrix) is not list:
        raise TypeError("matrix output must be a list or expose tolist()")

    width = shape[1] if shape is not None and not matrix else None
    rows: list[tuple[tuple[str, str, object], ...]] = []
    for row in matrix:
        if type(row) is not list:
            raise TypeError("every matrix row must be a list")
        if width is None:
            width = len(row)
        if len(row) != width:
            raise ValueError("matrix output must be rectangular")
        rows.append(tuple(_typed_scalar(item_value) for item_value in row))

    actual_shape = (len(rows), width or 0)
    if shape is not None and shape != actual_shape:
        raise ValueError("matrix shape does not match its materialized rows")
    return actual_shape, tuple(rows)


def _implementation(
    library: Library,
    task: Task,
    normalize: Normalizer = _identity,
    *,
    variant: str | None = None,
) -> Implementation:
    return Implementation(library, task, normalize, variant)


def _flow_case(  # noqa: C901
    spec: CaseSpec, size: int, np: Any, pd: Any
) -> CompetitiveCase:
    if spec.case_id == "flow.map_filter.sum.one_shot.callable":
        one_shot_values = range(size)

        def transform(value: int) -> int:
            return value * 3 + 1

        def keep(value: int) -> bool:
            return value % 2 == 0

        def candidate() -> object:
            return fpstreams.flow(iter(one_shot_values)).map(transform).filter(keep).sum()

        def python_one_shot() -> object:
            return sum(
                mapped for value in iter(one_shot_values) if keep(mapped := transform(value))
            )

        return CompetitiveCase(
            spec,
            _implementation("fpstreams", candidate),
            (_implementation("python", python_one_shot),),
            operator.eq,
        )

    values = list(range(size))
    array = np.arange(size, dtype=np.int64)
    series = pd.Series(array, copy=False)
    ceilings: tuple[Implementation, ...] = ()

    match spec.case_id:
        case "flow.to_numpy.int64":
            source = fpstreams.flow(values)

            def candidate() -> object:
                return source.to_numpy(dtype=np.int64)

            references = (
                _implementation(
                    "python",
                    lambda: python_array("q", values),
                    _normalize_vector,
                ),
                _implementation(
                    "numpy",
                    lambda: np.asarray(values, dtype=np.int64),
                    _normalize_vector,
                ),
                _implementation(
                    "pandas",
                    lambda: pd.Series(values, dtype=np.int64).to_numpy(copy=True),
                    _normalize_vector,
                ),
            )
        case "flow.map":
            candidate = fpstreams.flow(values).map(item * 3 + 1).to_list
            references = (
                _implementation("python", lambda: [value * 3 + 1 for value in values]),
                _implementation("numpy", lambda: (array * 3 + 1).tolist()),
                _implementation("pandas", lambda: (series * 3 + 1).tolist()),
            )
        case "flow.filter":
            candidate = fpstreams.flow(values).filter(item % 2 == 0).to_list
            references = (
                _implementation("python", lambda: [value for value in values if value % 2 == 0]),
                _implementation("numpy", lambda: array[array % 2 == 0].tolist()),
                _implementation("pandas", lambda: series[series % 2 == 0].tolist()),
            )
        case "flow.map_filter.sum":
            candidate = fpstreams.flow(values).map(item * 3 + 1).filter(item % 2 == 0).sum

            def numpy_map_filter_sum() -> object:
                mapped = array * 3 + 1
                return mapped[mapped % 2 == 0].sum()

            def pandas_map_filter_sum() -> object:
                mapped = series * 3 + 1
                return mapped[mapped % 2 == 0].sum()

            references = (
                _implementation(
                    "python",
                    lambda: sum(mapped for value in values if (mapped := value * 3 + 1) % 2 == 0),
                ),
                _implementation("numpy", numpy_map_filter_sum, _scalar),
                _implementation("pandas", pandas_map_filter_sum, _scalar),
            )
        case "flow.numpy.map_filter.sum" | "flow.numpy.map_filter.list":
            direct = fpstreams.flow.from_numpy(array).map(item * 3 + 1).filter(item % 2 == 0)

            def python_map_filter() -> list[int]:
                return [mapped for value in values if (mapped := value * 3 + 1) % 2 == 0]

            def python_map_filter_sum() -> int:
                return sum(mapped for value in values if (mapped := value * 3 + 1) % 2 == 0)

            def numpy_map_filter() -> Any:
                mapped = array * 3 + 1
                selected = mapped[mapped % 2 == 0]
                return selected.sum() if spec.case_id.endswith("sum") else selected.tolist()

            def pandas_map_filter() -> Any:
                mapped = series * 3 + 1
                selected = mapped[mapped % 2 == 0]
                return selected.sum() if spec.case_id.endswith("sum") else selected.tolist()

            if spec.case_id.endswith("sum"):
                candidate = direct.sum
                references = (
                    _implementation("python", python_map_filter_sum),
                    _implementation("numpy", numpy_map_filter, _scalar),
                    _implementation("pandas", pandas_map_filter, _scalar),
                )
            else:
                candidate = direct.to_list
                references = (
                    _implementation("python", python_map_filter),
                    _implementation("numpy", numpy_map_filter),
                    _implementation("pandas", pandas_map_filter),
                )
        case (
            "flow.numpy.float.map_filter.sum"
            | "flow.numpy.float.map_filter.list"
            | "flow.numpy.float.list"
        ):
            float_array = np.arange(size, dtype=np.float64)
            float_values = float_array.tolist()
            float_series = pd.Series(float_array, copy=False)
            direct = fpstreams.flow.from_numpy(float_array)
            lower = size * 0.375
            upper = size * 1.125
            transformed = direct.map(fpstreams.fitem * 1.5 + 0.25).filter(
                (fpstreams.fitem > lower) & (fpstreams.fitem < upper)
            )

            def python_float_map_filter() -> list[float]:
                return [
                    mapped
                    for value in float_values
                    if lower < (mapped := value * 1.5 + 0.25) < upper
                ]

            def python_float_map_filter_sum() -> float:
                return sum(
                    mapped
                    for value in float_values
                    if lower < (mapped := value * 1.5 + 0.25) < upper
                )

            def numpy_float_map_filter() -> Any:
                mapped = float_array * 1.5 + 0.25
                selected = mapped[(mapped > lower) & (mapped < upper)]
                return selected.sum() if spec.case_id.endswith("sum") else selected.tolist()

            def pandas_float_map_filter() -> Any:
                mapped = float_series * 1.5 + 0.25
                selected = mapped[(mapped > lower) & (mapped < upper)]
                return selected.sum() if spec.case_id.endswith("sum") else selected.tolist()

            if spec.case_id == "flow.numpy.float.list":
                candidate = direct.to_list
                references = (
                    _implementation("python", lambda: [float(value) for value in float_array]),
                    _implementation("numpy", float_array.tolist),
                    _implementation("pandas", float_series.tolist),
                )
            elif spec.case_id.endswith("sum"):
                candidate = transformed.sum
                references = (
                    _implementation("python", python_float_map_filter_sum),
                    _implementation("numpy", numpy_float_map_filter, _scalar),
                    _implementation("pandas", pandas_float_map_filter, _scalar),
                )
            else:
                candidate = transformed.to_list
                references = (
                    _implementation("python", python_float_map_filter),
                    _implementation("numpy", numpy_float_map_filter),
                    _implementation("pandas", pandas_float_map_filter),
                )
        case "flow.flat_map":

            def expand(value: int) -> tuple[int, int]:
                return value, value + 1

            def python_flat_map() -> list[int]:
                return [nested for value in values for nested in expand(value)]

            candidate = fpstreams.flow(values).flat_map(expand).to_list
            references = (
                _implementation("python", python_flat_map),
                _implementation(
                    "numpy",
                    lambda: np.column_stack((array, array + 1)).reshape(-1).tolist(),
                ),
            )
            ceilings = (
                _implementation(
                    "python",
                    lambda: [nested for value in values for nested in (value, value + 1)],
                    variant="python_inline_ceiling",
                ),
            )
        case "flow.take":
            count = size // 2
            candidate = fpstreams.flow(values).take(count).to_list
            references = (
                _implementation("python", lambda: values[:count]),
                _implementation("numpy", lambda: array[:count].tolist()),
                _implementation("pandas", lambda: series.iloc[:count].tolist()),
            )
        case "flow.drop":
            count = size // 2
            candidate = fpstreams.flow(values).drop(count).to_list
            references = (
                _implementation("python", lambda: values[count:]),
                _implementation("numpy", lambda: array[count:].tolist()),
                _implementation("pandas", lambda: series.iloc[count:].tolist()),
            )
        case "flow.take_while":
            limit = size // 2
            candidate = fpstreams.flow(values).take_while(lambda value: value < limit).to_list
            references = (
                _implementation(
                    "python",
                    lambda: list(takewhile(lambda value: value < limit, values)),
                ),
            )
        case "flow.unique.low_cardinality" | "flow.unique.high_cardinality":
            cardinality = (
                size if spec.case_id.endswith("high_cardinality") else max(1, min(16, size))
            )
            repeated = [value % cardinality for value in values]
            repeated_array = np.asarray(repeated, dtype=np.int64)
            repeated_series = pd.Series(repeated_array, copy=False)
            candidate = fpstreams.flow(repeated).unique().to_list

            def numpy_unique() -> object:
                _unique, positions = np.unique(repeated_array, return_index=True)
                return repeated_array[np.sort(positions)].tolist()

            references = (
                _implementation("python", lambda: list(dict.fromkeys(repeated))),
                _implementation("numpy", numpy_unique),
                _implementation("pandas", lambda: repeated_series.drop_duplicates().tolist()),
            )
        case "flow.sort":
            shuffled = [((value * 48_271) % max(1, size)) for value in values]
            shuffled_array = np.asarray(shuffled, dtype=np.int64)
            shuffled_series = pd.Series(shuffled_array, copy=False)
            candidate = fpstreams.flow(shuffled).sorted().to_list
            references = (
                _implementation("python", lambda: sorted(shuffled)),
                _implementation("numpy", lambda: np.sort(shuffled_array, kind="stable").tolist()),
                _implementation(
                    "pandas",
                    lambda: shuffled_series.sort_values(kind="stable").tolist(),
                ),
            )
        case "flow.chunk":
            width = 8
            candidate = fpstreams.flow(values).chunk(width).to_list
            references = (_implementation("python", lambda: list(batched(values, width))),)
        case "flow.window":
            width = 4
            step = 2
            candidate = fpstreams.flow(values).window(width, step=step).to_list
            references = (
                _implementation(
                    "python",
                    lambda: (
                        [tuple(values)]
                        if size < width
                        else [
                            tuple(values[index : index + width])
                            for index in range(0, size - width + 1, step)
                        ]
                    ),
                ),
            )
        case "flow.scan":
            scan_function = operator.add
            candidate = fpstreams.flow(values).scan(0, scan_function).to_list

            def python_scan() -> object:
                state = 0
                output: list[int] = []
                append = output.append
                for value in values:
                    state = scan_function(state, value)
                    append(state)
                return output

            references = (
                _implementation("python", python_scan),
                _implementation("numpy", lambda: np.cumsum(array).tolist()),
                _implementation("pandas", lambda: series.cumsum().tolist()),
            )
        case _:
            raise KeyError(spec.case_id)
    return CompetitiveCase(
        spec,
        _implementation(
            "fpstreams",
            candidate,
            _normalize_vector if spec.case_id == "flow.to_numpy.int64" else _identity,
        ),
        references,
        lambda left, right: left == right,
        ceilings,
    )


def _terminal_case(  # noqa: C901
    spec: CaseSpec, size: int, np: Any, pd: Any
) -> CompetitiveCase:
    values = list(range(size))
    array = np.arange(size, dtype=np.int64)
    series = pd.Series(array, copy=False)
    source = fpstreams.flow(values)

    match spec.case_id:
        case "terminal.sum":
            candidate = source.sum
            references = (
                _implementation("python", lambda: sum(values)),
                _implementation("numpy", array.sum, _scalar),
                _implementation("pandas", series.sum, _scalar),
            )
            equal = operator.eq
        case "terminal.count":
            candidate = source.count
            references = (
                _implementation("python", lambda: len(values)),
                _implementation("numpy", lambda: array.size),
                _implementation("pandas", lambda: series.size),
            )
            equal = operator.eq
        case "terminal.mean":
            candidate = source.mean
            references = (
                _implementation("python", lambda: statistics.fmean(values)),
                _implementation("numpy", array.mean, _scalar),
                _implementation("pandas", series.mean, _scalar),
            )
            equal = _float_equal
        case "terminal.variance":
            candidate = source.variance
            references = (
                _implementation(
                    "python", lambda: statistics.variance(values) if size > 1 else None
                ),
                _implementation("numpy", lambda: array.var(ddof=1) if size > 1 else None, _scalar),
                _implementation(
                    "pandas", lambda: series.var(ddof=1) if size > 1 else None, _scalar
                ),
            )
            equal = _optional_float_equal
        case "terminal.std":
            candidate = source.std
            references = (
                _implementation("python", lambda: statistics.stdev(values) if size > 1 else None),
                _implementation("numpy", lambda: array.std(ddof=1) if size > 1 else None, _scalar),
                _implementation(
                    "pandas", lambda: series.std(ddof=1) if size > 1 else None, _scalar
                ),
            )
            equal = _optional_float_equal
        case "terminal.min":
            candidate = source.min
            references = (
                _implementation("python", lambda: min(values)),
                _implementation("numpy", array.min, _scalar),
                _implementation("pandas", series.min, _scalar),
            )
            equal = operator.eq
        case "terminal.max":
            candidate = source.max
            references = (
                _implementation("python", lambda: max(values)),
                _implementation("numpy", array.max, _scalar),
                _implementation("pandas", series.max, _scalar),
            )
            equal = operator.eq
        case "terminal.numpy.sum":
            candidate = fpstreams.flow.from_numpy(array).sum
            references = (
                _implementation("python", lambda: sum(values)),
                _implementation("numpy", array.sum, _scalar),
                _implementation("pandas", series.sum, _scalar),
            )
            equal = operator.eq
        case "terminal.numpy.mean":
            candidate = fpstreams.flow.from_numpy(array).mean
            references = (
                _implementation("python", lambda: statistics.fmean(values)),
                _implementation("numpy", array.mean, _scalar),
                _implementation("pandas", series.mean, _scalar),
            )
            equal = _float_equal
        case "terminal.numpy.variance":
            candidate = fpstreams.flow.from_numpy(array).variance
            references = (
                _implementation(
                    "python", lambda: statistics.variance(values) if size > 1 else None
                ),
                _implementation("numpy", lambda: array.var(ddof=1) if size > 1 else None, _scalar),
                _implementation(
                    "pandas", lambda: series.var(ddof=1) if size > 1 else None, _scalar
                ),
            )
            equal = _optional_float_equal
        case "terminal.numpy.min":
            candidate = fpstreams.flow.from_numpy(array).min
            references = (
                _implementation("python", lambda: min(values)),
                _implementation("numpy", array.min, _scalar),
                _implementation("pandas", series.min, _scalar),
            )
            equal = operator.eq
        case "terminal.numpy.max":
            candidate = fpstreams.flow.from_numpy(array).max
            references = (
                _implementation("python", lambda: max(values)),
                _implementation("numpy", array.max, _scalar),
                _implementation("pandas", series.max, _scalar),
            )
            equal = operator.eq
        case (
            "terminal.numpy.float.sum"
            | "terminal.numpy.float.mean"
            | "terminal.numpy.float.variance"
        ):
            float_array = np.arange(size, dtype=np.float64)
            float_values = float_array.tolist()
            float_series = pd.Series(float_array, copy=False)
            float_source = fpstreams.flow.from_numpy(float_array)
            if spec.case_id.endswith("sum"):
                candidate = float_source.sum
                references = (
                    _implementation("python", lambda: sum(float_values)),
                    _implementation("numpy", float_array.sum, _scalar),
                    _implementation("pandas", float_series.sum, _scalar),
                )
                equal = _float_equal
            elif spec.case_id.endswith("mean"):
                candidate = float_source.mean
                references = (
                    _implementation("python", lambda: statistics.fmean(float_values)),
                    _implementation("numpy", float_array.mean, _scalar),
                    _implementation("pandas", float_series.mean, _scalar),
                )
                equal = _float_equal
            else:
                candidate = float_source.variance
                references = (
                    _implementation(
                        "python",
                        lambda: statistics.variance(float_values) if size > 1 else None,
                    ),
                    _implementation(
                        "numpy",
                        lambda: float_array.var(ddof=1) if size > 1 else None,
                        _scalar,
                    ),
                    _implementation(
                        "pandas",
                        lambda: float_series.var(ddof=1) if size > 1 else None,
                        _scalar,
                    ),
                )
                equal = _optional_float_equal
        case "terminal.any.expression" | "terminal.any.callable":
            if spec.case_id.endswith("expression"):
                predicate = item == size - 1

                def python_any() -> bool:
                    return any(value == size - 1 for value in values)

            else:

                def callable_any(value: int) -> bool:
                    return value == size - 1

                predicate = callable_any

                def python_any() -> bool:
                    return any(map(predicate, values))

            candidate = partial(source.any, predicate)
            references = (
                _implementation("python", python_any),
                _implementation("numpy", lambda: (array == size - 1).any(), bool),
                _implementation("pandas", lambda: series.eq(size - 1).any(), bool),
            )
            equal = operator.eq
        case "terminal.all.expression" | "terminal.all.callable":
            if spec.case_id.endswith("expression"):
                predicate = item >= 0

                def python_all() -> bool:
                    return all(value >= 0 for value in values)

            else:

                def callable_all(value: int) -> bool:
                    return value >= 0

                predicate = callable_all

                def python_all() -> bool:
                    return all(map(predicate, values))

            candidate = partial(source.all, predicate)
            references = (
                _implementation("python", python_all),
                _implementation("numpy", lambda: (array >= 0).all(), bool),
                _implementation("pandas", lambda: series.ge(0).all(), bool),
            )
            equal = operator.eq
        case "terminal.frequencies.low_cardinality" | "terminal.frequencies.high_cardinality":
            cardinality = (
                size if spec.case_id.endswith("high_cardinality") else max(1, min(16, size))
            )
            repeated = [value % cardinality for value in values]
            repeated_array = np.asarray(repeated, dtype=np.int64)
            repeated_series = pd.Series(repeated_array, copy=False)
            candidate = fpstreams.flow(repeated).frequencies
            references = (
                _implementation("python", lambda: dict(Counter(repeated))),
                _implementation(
                    "numpy",
                    lambda: {
                        int(key): int(count)
                        for key, count in zip(
                            *np.unique(repeated_array, return_counts=True), strict=True
                        )
                    },
                ),
                _implementation(
                    "pandas",
                    lambda: {
                        int(key): int(count)
                        for key, count in repeated_series.value_counts(sort=False).items()
                    },
                ),
            )
            equal = operator.eq
        case _:
            raise KeyError(spec.case_id)
    return CompetitiveCase(
        spec,
        _implementation("fpstreams", candidate),
        references,
        equal,
    )


def _numpy_row_matrix(np: Any, size: int, cardinality: int) -> Any:
    """Build the native matrix from which every case peer input is prepared."""
    indexes = np.arange(size, dtype=np.int64)
    values = np.empty((size, 3), dtype=np.int64)
    values[:, 0] = indexes % cardinality
    values[:, 1] = indexes
    values[:, 2] = indexes * 3 + 1
    return values


def _records_from_matrix_rows(
    rows: Iterable[Sequence[Any]],
    names: Sequence[str],
) -> list[dict[str, Any]]:
    """Fully materialize named records from an already selected row sequence."""
    return [dict(zip(names, row, strict=True)) for row in rows]


def _numpy_global_aggregate_case(
    spec: CaseSpec,
    rows: Any,
    python_rows: Sequence[Sequence[Any]],
    matrix: Any,
    frame: Any,
) -> CompetitiveCase:
    """Compare the same closed global lanes over each preconstructed representation."""
    candidate = rows.aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    ).to_list

    def python_aggregate() -> object:
        count = 0
        total = 0
        low = None
        high = None
        for _key, value, _payload in python_rows:
            count += 1
            total += value
            if low is None or value < low:
                low = value
            if high is None or value > high:
                high = value
        return [{"rows": count, "total": total, "low": low, "high": high}]

    def numpy_aggregate() -> object:
        selected = matrix[:, 1]
        if not len(selected):
            return [{"rows": 0, "total": 0, "low": None, "high": None}]
        return [
            {
                "rows": len(selected),
                "total": int(selected.sum()),
                "low": int(selected.min()),
                "high": int(selected.max()),
            }
        ]

    def pandas_aggregate() -> object:
        selected = frame["value"]
        if selected.empty:
            return [{"rows": 0, "total": 0, "low": None, "high": None}]
        return [
            {
                "rows": len(selected),
                "total": int(selected.sum()),
                "low": int(selected.min()),
                "high": int(selected.max()),
            }
        ]

    references = (
        _implementation("python", python_aggregate, _normalize_exact_records),
        _implementation("numpy", numpy_aggregate, _normalize_exact_records),
        _implementation("pandas", pandas_aggregate, _normalize_exact_records),
    )
    return CompetitiveCase(
        spec,
        _implementation("fpstreams", candidate, _normalize_exact_records),
        references,
        operator.eq,
    )


def _numpy_rows_case(spec: CaseSpec, size: int, np: Any, pd: Any) -> CompetitiveCase:
    """Compare guarded two-dimensional NumPy paths with equivalent native workloads."""
    high_cardinality = spec.case_id.endswith(".high_cardinality")
    cardinality = size if high_cardinality else max(1, min(16, size))
    columns = ("key", "value", "payload")
    matrix = _numpy_row_matrix(np, size, cardinality)
    python_rows = tuple(tuple(row) for row in matrix.tolist())
    frame = pd.DataFrame(matrix, columns=columns, copy=False)
    rows = fpstreams.rows.from_numpy(matrix, columns=columns)
    if spec.case_id == "rows.numpy.aggregate":
        return _numpy_global_aggregate_case(spec, rows, python_rows, matrix, frame)

    match spec.case_id:
        case "rows.numpy.identity":
            candidate = rows.to_list
            references = (
                _implementation(
                    "python",
                    lambda: _records_from_matrix_rows(python_rows, columns),
                    _normalize_exact_records,
                ),
                _implementation(
                    "numpy",
                    lambda: _records_from_matrix_rows(matrix.tolist(), columns),
                    _normalize_exact_records,
                ),
                _implementation(
                    "pandas",
                    lambda: frame.to_dict("records"),
                    _normalize_exact_records,
                ),
            )
        case "rows.numpy.select":
            selected_names = ("key", "payload")
            selected_indexes = np.asarray((0, 2), dtype=np.intp)
            selected_columns = list(selected_names)
            candidate = rows.select(*selected_names).to_list

            def numpy_select() -> object:
                selected = matrix[:, selected_indexes]
                return _records_from_matrix_rows(selected.tolist(), selected_names)

            references = (
                _implementation(
                    "python",
                    lambda: [{"key": row[0], "payload": row[2]} for row in python_rows],
                    _normalize_exact_records,
                ),
                _implementation("numpy", numpy_select, _normalize_exact_records),
                _implementation(
                    "pandas",
                    lambda: frame.loc[:, selected_columns].to_dict("records"),
                    _normalize_exact_records,
                ),
            )
        case "rows.numpy.filter_select":
            selected_names = ("key", "value")
            selected_indexes = np.asarray((0, 1), dtype=np.intp)
            selected_columns = list(selected_names)
            threshold = size // 2
            candidate = (
                rows.where(fpstreams.col("value") >= threshold).select(*selected_names).to_list
            )

            def numpy_filter_select() -> object:
                selected = matrix[matrix[:, 1] >= threshold][:, selected_indexes]
                return _records_from_matrix_rows(selected.tolist(), selected_names)

            references = (
                _implementation(
                    "python",
                    lambda: [
                        {"key": row[0], "value": row[1]}
                        for row in python_rows
                        if row[1] >= threshold
                    ],
                    _normalize_exact_records,
                ),
                _implementation("numpy", numpy_filter_select, _normalize_exact_records),
                _implementation(
                    "pandas",
                    lambda: frame.loc[
                        frame["value"] >= threshold,
                        selected_columns,
                    ].to_dict("records"),
                    _normalize_exact_records,
                ),
            )
        case (
            "rows.numpy.group_aggregate.low_cardinality"
            | "rows.numpy.group_aggregate.high_cardinality"
        ):
            candidate = (
                rows.group_by("key")
                .aggregate(
                    rows=fpstreams.agg.count(),
                    total=fpstreams.agg.sum("value"),
                    low=fpstreams.agg.min("value"),
                    high=fpstreams.agg.max("value"),
                )
                .to_list
            )

            def python_group() -> object:
                states: dict[int, list[int]] = {}
                for key, value, _payload in python_rows:
                    state = states.get(key)
                    if state is None:
                        states[key] = [1, value, value, value]
                        continue
                    state[0] += 1
                    state[1] += value
                    if value < state[2]:
                        state[2] = value
                    if value > state[3]:
                        state[3] = value
                return [
                    {
                        "key": key,
                        "rows": state[0],
                        "total": state[1],
                        "low": state[2],
                        "high": state[3],
                    }
                    for key, state in states.items()
                ]

            def numpy_group() -> object:
                keys = matrix[:, 0]
                group_values = matrix[:, 1]
                counts = np.bincount(keys, minlength=cardinality)
                totals = np.bincount(
                    keys,
                    weights=group_values,
                    minlength=cardinality,
                ).astype(np.int64)
                low_values = np.full(cardinality, np.iinfo(np.int64).max, dtype=np.int64)
                high_values = np.full(cardinality, np.iinfo(np.int64).min, dtype=np.int64)
                np.minimum.at(low_values, keys, group_values)
                np.maximum.at(high_values, keys, group_values)
                return [
                    {
                        "key": key,
                        "rows": int(counts[key]),
                        "total": int(totals[key]),
                        "low": int(low_values[key]),
                        "high": int(high_values[key]),
                    }
                    for key in range(cardinality)
                    if counts[key]
                ]

            def pandas_group() -> object:
                grouped = (
                    frame.groupby("key", sort=False)["value"]
                    .agg(rows="count", total="sum", low="min", high="max")
                    .reset_index()
                )
                return [
                    {
                        "key": int(key),
                        "rows": int(count),
                        "total": int(total),
                        "low": int(low),
                        "high": int(high),
                    }
                    for key, count, total, low, high in grouped.itertuples(
                        index=False,
                        name=None,
                    )
                ]

            references = (
                _implementation("python", python_group, _normalize_exact_records),
                _implementation("numpy", numpy_group, _normalize_exact_records),
                _implementation("pandas", pandas_group, _normalize_exact_records),
            )
        case _:
            raise KeyError(spec.case_id)

    return CompetitiveCase(
        spec,
        _implementation("fpstreams", candidate, _normalize_exact_records),
        references,
        operator.eq,
    )


def _row_records(size: int) -> list[dict[str, Any]]:
    return [
        {
            "id": index,
            "key": index % max(1, min(16, size)),
            "value": index,
            "nullable": None if index % 5 == 0 else index,
            "tags": [index, index + 1],
        }
        for index in range(size)
    ]


def _rows_case(  # noqa: C901
    spec: CaseSpec, size: int, np: Any, pd: Any
) -> CompetitiveCase:
    records = _row_records(size)
    rows = fpstreams.rows(records)
    frame = pd.DataFrame.from_records(records)
    frame["nullable"] = frame["nullable"].astype("Int64")
    references: tuple[Implementation, ...]

    match spec.case_id:
        case "rows.filter":

            def keep_row(row: Mapping[str, Any]) -> bool:
                return row["value"] % 2 == 0

            candidate = rows.filter(keep_row).to_list
            references = (
                _implementation("python", lambda: [row for row in records if keep_row(row)]),
                _implementation(
                    "pandas",
                    lambda: frame.loc[frame["value"] % 2 == 0].to_dict("records"),
                ),
            )
        case "rows.select":
            candidate = rows.select("id", "value").to_list
            references = (
                _implementation(
                    "python", lambda: [{"id": row["id"], "value": row["value"]} for row in records]
                ),
                _implementation("pandas", lambda: frame.loc[:, ["id", "value"]].to_dict("records")),
            )
        case "rows.with_columns.expression" | "rows.with_columns.callable":
            if spec.case_id.endswith("expression"):
                next_value = fpstreams.col("value") + 1

                def python_with_column() -> list[dict[str, Any]]:
                    return [{**row, "next_value": row["value"] + 1} for row in records]

            else:

                def callable_next_value(row: Mapping[str, Any]) -> object:
                    return row["value"] + 1

                next_value = callable_next_value

                def python_with_column() -> list[dict[str, Any]]:
                    return [{**row, "next_value": callable_next_value(row)} for row in records]

            candidate = rows.with_columns(next_value=next_value).to_list
            references = (
                _implementation("python", python_with_column),
                _implementation(
                    "pandas",
                    lambda: frame.assign(next_value=frame["value"] + 1).to_dict("records"),
                ),
            )
        case "rows.cast":
            candidate = rows.cast(value=str).to_list
            references = (
                _implementation(
                    "python", lambda: [{**row, "value": str(row["value"])} for row in records]
                ),
                _implementation(
                    "pandas",
                    lambda: frame.assign(value=frame["value"].astype(str)).to_dict("records"),
                ),
            )
        case "rows.fill_nulls":
            candidate = rows.fill_nulls(nullable=0).to_list
            references = (
                _implementation(
                    "python",
                    lambda: [
                        {**row, "nullable": 0 if row["nullable"] is None else row["nullable"]}
                        for row in records
                    ],
                ),
                _implementation(
                    "pandas",
                    lambda: frame.assign(
                        nullable=frame["nullable"].where(frame["nullable"].notna(), 0)
                    ).to_dict("records"),
                ),
            )
        case "rows.drop_nulls":
            candidate = rows.drop_nulls("nullable").to_list
            references = (
                _implementation(
                    "python", lambda: [row for row in records if row["nullable"] is not None]
                ),
                _implementation(
                    "pandas", lambda: frame.dropna(subset=["nullable"]).to_dict("records")
                ),
            )
        case "rows.explode":
            candidate = rows.explode("tags").to_list
            references = (
                _implementation(
                    "python",
                    lambda: [{**row, "tags": tag} for row in records for tag in row["tags"]],
                ),
                _implementation("pandas", lambda: frame.explode("tags").to_dict("records")),
            )
        case "rows.unnest":
            nested: list[dict[str, Any]] = [
                {
                    "id": index,
                    "payload": {"left": index, "right": index * 2},
                }
                for index in range(size)
            ]
            candidate = fpstreams.rows(nested).unnest("payload", prefix="payload_").to_list
            references = (
                _implementation(
                    "python",
                    lambda: [
                        {
                            "id": row["id"],
                            **{f"payload_{name}": value for name, value in row["payload"].items()},
                        }
                        for row in nested
                    ],
                ),
                _implementation(
                    "pandas",
                    lambda: pd.json_normalize(nested, sep="_").to_dict("records"),
                ),
            )
        case "rows.unpivot":
            wide = [{"id": index, "left": index, "right": index * 2} for index in range(size)]
            wide_frame = pd.DataFrame(wide)
            candidate = fpstreams.rows(wide).unpivot("left", "right").to_list

            def pandas_unpivot() -> object:
                return (
                    wide_frame.set_index("id")[["left", "right"]]
                    .stack(future_stack=True)
                    .rename_axis(index=("id", "variable"))
                    .rename("value")
                    .reset_index()
                    .to_dict("records")
                )

            references = (
                _implementation(
                    "python",
                    lambda: [
                        {"id": row["id"], "variable": name, "value": row[name]}
                        for row in wide
                        for name in ("left", "right")
                    ],
                ),
                _implementation("pandas", pandas_unpivot),
            )
        case "rows.pivot":
            long = [
                {"group": index, "name": name, "amount": index * multiplier}
                for index in range(max(1, size // 2))
                for name, multiplier in (("left", 1), ("right", 2))
            ]
            long_frame = pd.DataFrame(long)
            candidate = (
                fpstreams.rows(long).pivot(index="group", columns="name", values="amount").to_list
            )

            def python_pivot() -> object:
                pivoted: dict[object, dict[str, object]] = {}
                cells: set[tuple[object, object]] = set()
                for row in long:
                    group = row["group"]
                    cell = (group, row["name"])
                    if cell in cells:
                        raise ValueError(f"duplicate pivot cell: {cell!r}")
                    cells.add(cell)
                    output = pivoted.setdefault(group, {"group": group})
                    output[str(row["name"])] = row["amount"]
                return list(pivoted.values())

            references = (
                _implementation("python", python_pivot),
                _implementation(
                    "pandas",
                    lambda: (
                        long_frame.pivot(index="group", columns="name", values="amount")
                        .reset_index()
                        .loc[:, ["group", "left", "right"]]
                        .to_dict("records")
                    ),
                ),
            )
        case "rows.sort":
            shuffled = sorted(records, key=lambda row: (row["value"] * 48_271) % size)
            shuffled_frame = pd.DataFrame.from_records(shuffled)
            shuffled_frame["nullable"] = shuffled_frame["nullable"].astype("Int64")
            candidate = fpstreams.rows(shuffled).sort_by("value").to_list
            references = (
                _implementation("python", lambda: sorted(shuffled, key=lambda row: row["value"])),
                _implementation(
                    "pandas",
                    lambda: shuffled_frame.sort_values("value", kind="stable").to_dict("records"),
                ),
            )
        case "rows.aggregate":
            candidate = rows.aggregate(total=fpstreams.agg.sum("value")).to_list
            references = (
                _implementation(
                    "python",
                    lambda: [{"total": sum(row["value"] for row in records)}],
                ),
                _implementation("pandas", lambda: [{"total": int(frame["value"].sum())}]),
            )
        case "rows.aggregate.multi":
            candidate = rows.aggregate(
                rows=fpstreams.agg.count(),
                total=fpstreams.agg.sum("value"),
                low=fpstreams.agg.min("value"),
                high=fpstreams.agg.max("value"),
            ).to_list
            values = np.asarray([row["value"] for row in records], dtype=np.int64)
            series = frame["value"]

            def python_aggregate() -> object:
                count = 0
                total = 0
                low: int | None = None
                high: int | None = None
                for row in records:
                    value = row["value"]
                    count += 1
                    total += value
                    if low is None or value < low:
                        low = value
                    if high is None or value > high:
                        high = value
                return [{"rows": count, "total": total, "low": low, "high": high}]

            def numpy_aggregate() -> object:
                return [
                    {
                        "rows": int(values.size),
                        "total": int(values.sum()),
                        "low": None if not values.size else int(values.min()),
                        "high": None if not values.size else int(values.max()),
                    }
                ]

            def pandas_aggregate() -> object:
                return [
                    {
                        "rows": int(series.size),
                        "total": int(series.sum()),
                        "low": None if series.empty else int(series.min()),
                        "high": None if series.empty else int(series.max()),
                    }
                ]

            references = (
                _implementation("python", python_aggregate),
                _implementation("numpy", numpy_aggregate),
                _implementation("pandas", pandas_aggregate),
            )
        case _:
            raise KeyError(spec.case_id)
    normalized_references = tuple(
        _implementation(reference.library, reference.task, _normalize_null_records)
        if reference.library == "pandas"
        else reference
        for reference in references
    )
    return CompetitiveCase(
        spec,
        _implementation("fpstreams", candidate),
        normalized_references,
        lambda left, right: left == right,
    )


def _group_case(spec: CaseSpec, size: int, np: Any, pd: Any) -> CompetitiveCase:
    cardinality = (
        size if spec.case_id == "rows.group_sum.high_cardinality" else max(1, min(16, size))
    )
    mapping_callable = spec.case_id.endswith(".mapping_callable")
    if mapping_callable:
        cardinality = max(1, min(size, 30_000))
        records = [_NominalRecord(key=index % cardinality, value=index) for index in range(size)]
        keys = np.asarray([row["key"] for row in records], dtype=np.int64)
        values = np.asarray([row["value"] for row in records], dtype=np.int64)

        def select_key(row: Mapping[str, Any]) -> Any:
            return row["key"]

        def select_value(row: Mapping[str, Any]) -> Any:
            return row["value"]

        grouped = (
            fpstreams.rows(records)
            .group_by(key=select_key)
            .aggregate(total=fpstreams.agg.sum(select_value))
        )
    else:
        records = [(index % cardinality, index) for index in range(size)]
        keys = np.asarray([key for key, _value in records], dtype=np.int64)
        values = np.asarray([value for _key, value in records], dtype=np.int64)
        grouped = fpstreams.rows(records).group_by(key=0).aggregate(total=fpstreams.agg.sum(1))
    frame = pd.DataFrame({"key": keys, "value": values})

    def fpstreams_group() -> object:
        return grouped.to_list()

    def python_group() -> object:
        totals: dict[int, int] = {}
        for row in records:
            if mapping_callable:
                key, value = select_key(row), select_value(row)
            else:
                key, value = row
            totals[key] = totals.get(key, 0) + value
        return [{"key": key, "total": total} for key, total in totals.items()]

    def numpy_group() -> object:
        totals = np.bincount(keys, weights=values).astype(np.int64)
        return [{"key": key, "total": int(totals[key])} for key in range(cardinality)]

    def pandas_group() -> object:
        result = frame.groupby("key", sort=False, as_index=False)["value"].sum()
        return [
            {"key": int(key), "total": int(total)}
            for key, total in result.itertuples(index=False, name=None)
        ]

    references = [_implementation("python", python_group, _normalize_exact_records)]
    if not mapping_callable:
        references.extend(
            (
                _implementation("numpy", numpy_group, _normalize_exact_records),
                _implementation("pandas", pandas_group, _normalize_exact_records),
            )
        )

    return CompetitiveCase(
        spec,
        _implementation("fpstreams", fpstreams_group, _normalize_exact_records),
        tuple(references),
        lambda left, right: left == right,
    )


def _join_case(  # noqa: C901 - join shapes share one fairness-critical baseline
    spec: CaseSpec, size: int, np: Any, pd: Any
) -> CompetitiveCase:
    del np
    mapping_callable = spec.case_id.endswith(".mapping_callable")

    def record(**values: Any) -> Mapping[str, Any]:
        return _NominalRecord(**values) if mapping_callable else values

    left = [record(id=index, value=index) for index in range(size)]
    if ".many" in spec.case_id:
        right = [
            record(id=index, label=f"r{index}-{duplicate}")
            for index in range(0, size, 2)
            for duplicate in range(2)
        ]
        how = "inner"
        validation = "m:m"
        pandas_validation = "many_to_many"
    elif spec.case_id == "rows.join.left.unique":
        right = [record(id=index, label=f"r{index}") for index in range(0, size, 2)]
        how = "left"
        validation = "m:1"
        pandas_validation = "many_to_one"
    else:
        right = [record(id=index, label=f"r{index}") for index in range(0, size, 2)]
        how = "inner"
        validation = "m:1"
        pandas_validation = "many_to_one"
    left_frame = pd.DataFrame.from_records(left)
    right_frame = pd.DataFrame.from_records(right)

    def select_id(row: Mapping[str, Any]) -> Any:
        return row["id"]

    joined = fpstreams.rows(left).join(
        right,
        on=select_id if mapping_callable else "id",
        how=how,
        validate=validation,
    )

    def merge_match(left_row: Mapping[str, Any], right_row: Mapping[str, Any]) -> dict[str, Any]:
        if not mapping_callable:
            return {**left_row, **right_row}
        merged = dict(left_row)
        for name, value in right_row.items():
            merged[f"{name}_right" if name in merged else name] = value
        return merged

    def merge_mapping_snapshot(
        left_snapshot: dict[str, Any], right_snapshot: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Merge already-owned callable rows without paying for a second left snapshot."""
        for name, value in right_snapshot.items():
            left_snapshot[f"{name}_right" if name in left_snapshot else name] = value
        return left_snapshot

    if validation == "m:1":

        def python_join() -> object:
            index: dict[Any, Mapping[str, Any]] = {}
            for row in right:
                snapshot = dict(row) if mapping_callable else row
                key = select_id(row) if mapping_callable else row["id"]
                if key in index:
                    raise ValueError(f"duplicate right join key: {key!r}")
                index[key] = snapshot
            result: list[dict[str, Any]] = []
            for row in left:
                snapshot = dict(row) if mapping_callable else row
                key = select_id(row) if mapping_callable else row["id"]
                match = index.get(key)
                if match is not None:
                    result.append(
                        merge_mapping_snapshot(cast(dict[str, Any], snapshot), match)
                        if mapping_callable
                        else merge_match(row, match)
                    )
                elif how == "left":
                    result.append({**row, "label": None})
            return result

    else:

        def python_join() -> object:
            index: dict[Any, list[Mapping[str, Any]]] = {}
            for row in right:
                snapshot = dict(row) if mapping_callable else row
                key = select_id(row) if mapping_callable else row["id"]
                index.setdefault(key, []).append(snapshot)
            result: list[dict[str, Any]] = []
            for row in left:
                snapshot = dict(row) if mapping_callable else row
                key = select_id(row) if mapping_callable else row["id"]
                matches = index.get(key, ())
                if mapping_callable:
                    result.extend(
                        merge_mapping_snapshot(cast(dict[str, Any], snapshot).copy(), match)
                        for match in matches
                    )
                else:
                    result.extend(merge_match(row, match) for match in matches)
            return result

    def pandas_join() -> object:
        if mapping_callable:
            merged = left_frame.merge(
                right_frame.rename(columns={"id": "id_right"}),
                left_on="id",
                right_on="id_right",
                how=how,
                sort=False,
                validate=pandas_validation,
            )
        else:
            merged = left_frame.merge(
                right_frame,
                on="id",
                how=how,
                sort=False,
                validate=pandas_validation,
            )
        return merged.where(pd.notna(merged), None).to_dict("records")

    normalize = _normalize_exact_records if mapping_callable else _normalize_null_records
    references = [_implementation("python", python_join, normalize)]
    if not mapping_callable:
        references.append(_implementation("pandas", pandas_join, normalize))
    return CompetitiveCase(
        spec,
        _implementation("fpstreams", joined.to_list, normalize),
        tuple(references),
        lambda left_value, right_value: left_value == right_value,
    )


def _double_pair_value(value: int) -> int:
    """Shared callable for the Pairs candidate and its Python reference."""
    return value * 2


def _keep_even_pair(_key: int, value: int) -> bool:
    """Shared callable for the Pairs candidate and its Python reference."""
    return value % 2 == 0


def _pairs_case(spec: CaseSpec, size: int, np: Any, pd: Any) -> CompetitiveCase:
    del np
    if spec.case_id.endswith("high_cardinality"):
        cardinality = size
    elif spec.case_id.endswith("half_cardinality"):
        cardinality = max(1, size // 2)
    else:
        cardinality = max(1, min(16, size))
    values = [(index % cardinality, index) for index in range(size)]
    frame = pd.DataFrame.from_records(values, columns=("key", "value"))

    match spec.case_id:
        case "pairs.map_values.half_cardinality":
            candidate = partial(
                fpstreams.pairs(values).map_values(_double_pair_value).to_dict,
                on_duplicate="last",
            )
            references = (
                _implementation(
                    "python",
                    lambda: {key: _double_pair_value(value) for key, value in values},
                ),
            )
        case "pairs.map_values.expression.half_cardinality":
            candidate = partial(
                fpstreams.pairs(values).map_values(item * 2).to_dict,
                on_duplicate="last",
            )
            references = (
                _implementation("python", lambda: {key: value * 2 for key, value in values}),
            )
        case "pairs.filter.half_cardinality":
            candidate = partial(
                fpstreams.pairs(values).filter_pairs(_keep_even_pair).to_dict,
                on_duplicate="last",
            )
            references = (
                _implementation(
                    "python",
                    lambda: {key: value for key, value in values if _keep_even_pair(key, value)},
                ),
            )
        case "pairs.filter_values.expression.half_cardinality":
            candidate = partial(
                fpstreams.pairs(values).filter_values((item % 2) == 0).to_dict,
                on_duplicate="last",
            )
            references = (
                _implementation(
                    "python",
                    lambda: {key: value for key, value in values if value % 2 == 0},
                ),
            )
        case "pairs.filter_pairs.expression.half_cardinality":
            expression = (fpstreams.col(0) + fpstreams.col(1)) % 3 == 0
            candidate = partial(
                fpstreams.pairs(values).filter_pairs(expression).to_dict,
                on_duplicate="last",
            )
            references = (
                _implementation(
                    "python",
                    lambda: {key: value for key, value in values if (key + value) % 3 == 0},
                ),
            )
        case "pairs.unique_keys.low_cardinality" | "pairs.unique_keys.high_cardinality":
            candidate = fpstreams.pairs(values).unique_keys().to_dict

            def python_unique() -> object:
                result: dict[int, int] = {}
                for key, value in values:
                    result.setdefault(key, value)
                return result

            references = (_implementation("python", python_unique),)
        case "pairs.aggregate_values.low_cardinality" | "pairs.aggregate_values.high_cardinality":

            def python_aggregate() -> object:
                totals: dict[int, int] = {}
                for key, value in values:
                    totals[key] = totals.get(key, 0) + value
                return {key: {"total": total} for key, total in totals.items()}

            def pandas_aggregate() -> object:
                grouped = frame.groupby("key", sort=False)["value"].sum()
                return {int(key): {"total": int(total)} for key, total in grouped.items()}

            candidate = partial(
                fpstreams.pairs(values).aggregate_values,
                total=fpstreams.agg.sum(),
            )
            references = (
                _implementation("python", python_aggregate),
                _implementation("pandas", pandas_aggregate),
            )
        case _:
            raise KeyError(spec.case_id)

    return CompetitiveCase(
        spec,
        _implementation("fpstreams", candidate),
        references,
        lambda left, right: left == right,
    )


def _fresh_numpy_matrix(np: Any, size: int) -> object:
    return np.arange(size * 2, dtype=np.int64).reshape(size, 2)


def _fresh_record_rows(size: int) -> list[dict[str, int]]:
    return [{"id": index, "value": index * 2} for index in range(size)]


def _io_case(spec: CaseSpec, size: int, np: Any, pd: Any, tempdir: Path) -> CompetitiveCase:
    records = (
        _fresh_record_rows(size)
        if spec.case_id in {"io.csv.read", "io.jsonl.read", "io.dataframe.read"}
        else []
    )

    match spec.case_id:
        case "io.csv.read":
            path = tempdir / "competitive.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(("id", "value"))
                writer.writerows((record["id"], record["value"]) for record in records)

            def python_csv() -> object:
                with path.open(encoding="utf-8", newline="") as handle:
                    return list(csv.DictReader(handle))

            candidate = fpstreams.Rows.from_csv(path).to_list
            references = (
                _implementation("python", python_csv),
                _implementation(
                    "pandas",
                    lambda: pd.read_csv(path, dtype=str, keep_default_na=False).to_dict("records"),
                ),
            )
        case "io.jsonl.read":
            path = tempdir / "competitive.jsonl"
            path.write_text(
                "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
                encoding="utf-8",
            )

            def python_jsonl() -> object:
                return _read_strict_jsonl(path)

            candidate = fpstreams.Rows.from_jsonl(path).to_list
            references = (
                _implementation("python", python_jsonl),
                _implementation(
                    "pandas", lambda: pd.read_json(path, lines=True).to_dict("records")
                ),
            )
        case "io.dataframe.read":
            frame = pd.DataFrame.from_records(records)
            candidate = fpstreams.Rows.from_dataframe(frame).to_list
            references = (_implementation("pandas", lambda: frame.to_dict("records")),)
        case "io.numpy.ndarray_to_named_rows":
            columns = ("id", "value")

            def candidate() -> object:
                return fpstreams.rows.from_numpy(
                    _fresh_numpy_matrix(np, size), columns=columns
                ).to_list()

            def python_named_rows() -> object:
                return [
                    {"id": identifier, "value": value}
                    for identifier, value in _fresh_numpy_matrix(np, size).tolist()
                ]

            def pandas_named_rows() -> object:
                return pd.DataFrame(
                    _fresh_numpy_matrix(np, size), columns=columns, copy=False
                ).to_dict("records")

            references = (
                _implementation("python", python_named_rows, _normalize_exact_records),
                _implementation("pandas", pandas_named_rows, _normalize_exact_records),
            )
        case "io.numpy.record_rows_to_array":
            columns = ("id", "value")

            def candidate() -> object:
                return fpstreams.rows(_fresh_record_rows(size)).to_numpy(*columns)

            def python_matrix() -> object:
                return [[record["id"], record["value"]] for record in _fresh_record_rows(size)]

            def numpy_matrix() -> object:
                return np.asarray(
                    [[record["id"], record["value"]] for record in _fresh_record_rows(size)]
                )

            def pandas_matrix() -> object:
                return pd.DataFrame.from_records(
                    _fresh_record_rows(size), columns=columns
                ).to_numpy(copy=False)

            references = (
                _implementation("python", python_matrix, _normalize_exact_matrix),
                _implementation("numpy", numpy_matrix, _normalize_exact_matrix),
                _implementation("pandas", pandas_matrix, _normalize_exact_matrix),
            )
        case _:
            raise KeyError(spec.case_id)

    normalize = (
        _normalize_exact_records
        if spec.case_id == "io.numpy.ndarray_to_named_rows"
        else _normalize_exact_matrix
        if spec.case_id == "io.numpy.record_rows_to_array"
        else _identity
    )
    return CompetitiveCase(
        spec,
        _implementation("fpstreams", candidate, normalize),
        references,
        operator.eq,
    )


def _build_case(spec: CaseSpec, size: int, np: Any, pd: Any, tempdir: Path) -> CompetitiveCase:
    if spec.case_id.startswith("flow."):
        return _flow_case(spec, size, np, pd)
    if spec.case_id.startswith("terminal."):
        return _terminal_case(spec, size, np, pd)
    if spec.case_id.startswith("rows.numpy."):
        return _numpy_rows_case(spec, size, np, pd)
    if spec.case_id.startswith("rows.group_"):
        return _group_case(spec, size, np, pd)
    if spec.case_id.startswith("rows.join."):
        return _join_case(spec, size, np, pd)
    if spec.case_id.startswith("rows."):
        return _rows_case(spec, size, np, pd)
    if spec.case_id.startswith("pairs."):
        return _pairs_case(spec, size, np, pd)
    if spec.case_id.startswith("io."):
        return _io_case(spec, size, np, pd, tempdir)
    raise KeyError(spec.case_id)


def _measurement_record(
    implementation: Implementation,
    spec: CaseSpec,
    samples: list[float],
) -> dict[str, Any]:
    library = implementation.library
    implementation_name = implementation.variant or library
    return {
        "name": f"competitive/{implementation_name}/{spec.case_id}",
        "sample_count": len(samples),
        "samples_seconds": samples,
        "median_seconds": statistics.median(samples),
        "stdev_seconds": statistics.stdev(samples) if len(samples) > 1 else 0.0,
        "backend": library,
        "source_kind": spec.case_id.split(".", 1)[0],
        "terminal": spec.case_id,
        "scope": spec.scope,
        "baseline": None,
        "maximum_ratio": None,
    }


def _measure_case(case: CompetitiveCase, repeats: int) -> tuple[dict[str, Any], ...]:
    """Measure warmed peers in rotating order so no implementation owns every first slot."""
    implementations = (case.candidate, *case.references, *case.ceilings)
    samples: list[list[float]] = [[] for _implementation in implementations]
    implementation_count = len(implementations)
    round_count = max(repeats, implementation_count)
    for round_index in range(round_count):
        for offset in range(implementation_count):
            implementation_index = (round_index + offset) % implementation_count
            gc.collect()
            # Prime implementation-specific allocator and cache state immediately before
            # timing. Otherwise large temporary outputs from another peer can flip a
            # bimodal allocation workload's median without any code change.
            implementations[implementation_index].task()
            started = time.perf_counter()
            implementations[implementation_index].task()
            samples[implementation_index].append(time.perf_counter() - started)
    return tuple(
        _measurement_record(implementation, case.spec, implementation_samples)
        for implementation, implementation_samples in zip(
            implementations,
            samples,
            strict=True,
        )
    )


def _assert_equivalent_outputs(case: CompetitiveCase) -> None:
    """Run the correctness warm-up without retaining its potentially large results."""
    candidate_value = case.candidate.normalize(case.candidate.task())
    for reference in (*case.references, *case.ceilings):
        reference_value = reference.normalize(reference.task())
        if not case.outputs_equal(candidate_value, reference_value):
            raise RuntimeError(
                f"competitive result mismatch: {case.spec.case_id} fpstreams != {reference.library}"
            )


def run_competitive(
    *,
    size: int,
    repeats: int,
    native: Mapping[str, object],
    quick: bool = False,
    include: Sequence[str] = (),
) -> dict[str, Any]:
    """Measure selected cases after proving each competitor returns an equivalent result."""
    if size < 1:
        raise ValueError("size must be positive")
    if repeats < 1:
        raise ValueError("repeats must be positive")
    if any(not pattern for pattern in include):
        raise ValueError("competitive include patterns cannot be empty")

    try:
        import numpy as np
        import pandas as pd  # type: ignore[import-untyped]
    except ImportError as error:
        raise RuntimeError(
            "competitive benchmarks require the 'data' extra: pip install fpstreams[data]"
        ) from error

    matrix_path = Path(__file__).resolve()
    package_root = Path(fpstreams.__file__).resolve().parent
    matrix_sha256 = _file_sha256(matrix_path)
    python_package_sha256 = _python_package_sha256(package_root)

    selected_ids = set(list_competitive_cases(quick=quick, include=include))
    if not selected_ids:
        raise ValueError("competitive include patterns selected no cases")

    results: list[dict[str, Any]] = []
    comparisons: list[dict[str, Any]] = []
    ceilings: list[dict[str, Any]] = []
    with TemporaryDirectory(prefix="fpstreams-competitive-") as directory:
        tempdir = Path(directory)
        for spec in _CASE_SPECS:
            if spec.case_id not in selected_ids:
                continue
            case = _build_case(spec, size, np, pd, tempdir)
            _assert_equivalent_outputs(case)
            case_records = _measure_case(case, repeats)
            results.extend(case_records)
            candidate_record = case_records[0]
            reference_count = len(case.references)
            reference_records = case_records[1 : reference_count + 1]
            ceiling_records = case_records[reference_count + 1 :]
            for reference, reference_record in zip(
                case.references,
                reference_records,
                strict=True,
            ):
                metrics = comparison_metrics(
                    float(candidate_record["median_seconds"]),
                    float(reference_record["median_seconds"]),
                    candidate_samples=candidate_record["samples_seconds"],
                    baseline_samples=reference_record["samples_seconds"],
                )
                comparisons.append(
                    {
                        "case": spec.case_id,
                        "api": spec.api,
                        "scope": spec.scope,
                        "candidate": candidate_record["name"],
                        "baseline": reference_record["name"],
                        "baseline_library": reference.library,
                        "candidate_seconds": candidate_record["median_seconds"],
                        "baseline_seconds": reference_record["median_seconds"],
                        **metrics,
                        "outputs_equal": True,
                    }
                )
            for ceiling, ceiling_record in zip(
                case.ceilings,
                ceiling_records,
                strict=True,
            ):
                metrics = comparison_metrics(
                    float(candidate_record["median_seconds"]),
                    float(ceiling_record["median_seconds"]),
                    candidate_samples=candidate_record["samples_seconds"],
                    baseline_samples=ceiling_record["samples_seconds"],
                )
                ceilings.append(
                    {
                        "case": spec.case_id,
                        "api": spec.api,
                        "scope": spec.scope,
                        "candidate": candidate_record["name"],
                        "ceiling": ceiling_record["name"],
                        "ceiling_library": ceiling.library,
                        "ceiling_label": "Python inline ceiling",
                        "candidate_seconds": candidate_record["median_seconds"],
                        "ceiling_seconds": ceiling_record["median_seconds"],
                        **metrics,
                        "outputs_equal": True,
                        "omitted_work": "flat_map callback invocation",
                    }
                )

    if _file_sha256(matrix_path) != matrix_sha256:
        raise RuntimeError("competitive benchmark matrix changed while measurements were running")
    if _python_package_sha256(package_root) != python_package_sha256:
        raise RuntimeError("fpstreams Python sources changed while measurements were running")
    native_path = native.get("path")
    native_sha256 = native.get("sha256")
    if (
        isinstance(native_path, str)
        and isinstance(native_sha256, str)
        and _file_sha256(Path(native_path)) != native_sha256
    ):
        raise RuntimeError("fpstreams native extension changed while measurements were running")

    return {
        "schema_version": 2,
        "metadata": {
            "suite": "competitive",
            "fpstreams_version": fpstreams.__version__,
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "native": dict(native),
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "benchmark_matrix_sha256": matrix_sha256,
            "python_package_sha256": python_package_sha256,
            "provenance_verified_unchanged": True,
            "size": size,
            "repeats": repeats,
            "quick": quick,
            "scope": "compute-only unless a row is marked end-to-end",
            "methodology": {
                "inputs_preconstructed": True,
                "correctness_warmup_runs": 1,
                "timed_tasks_fully_materialize_outputs": True,
                "timed_output_normalization": False,
            },
            "libraries": {
                "fpstreams": fpstreams.__version__,
                "numpy": np.__version__,
                "pandas": pd.__version__,
            },
        },
        "results": results,
        "comparisons": comparisons,
        "ceilings": ceilings,
        "regressions": [],
    }


class ComparisonMetrics(TypedDict):
    """Derived elapsed-time relationship between fpstreams and one competitor."""

    ratio: float
    elapsed_delta_seconds: float
    elapsed_delta_percent: float
    noise_band_seconds: float
    verdict: Verdict


def _median_absolute_deviation(samples: Sequence[float]) -> float:
    if not samples:
        return 0.0
    center = statistics.median(samples)
    return statistics.median(abs(sample - center) for sample in samples)


def comparison_metrics(
    candidate_seconds: float,
    baseline_seconds: float,
    *,
    candidate_samples: Sequence[float] = (),
    baseline_samples: Sequence[float] = (),
) -> ComparisonMetrics:
    """Return elapsed deltas with a robust, sample-derived noise band."""
    durations = (
        candidate_seconds,
        baseline_seconds,
        *candidate_samples,
        *baseline_samples,
    )
    if any(not math.isfinite(duration) for duration in durations):
        raise ValueError("benchmark durations must be finite")
    if any(duration < 0 for duration in durations):
        raise ValueError("benchmark durations cannot be negative")
    if baseline_seconds == 0:
        raise ValueError("baseline duration must be positive")
    ratio = candidate_seconds / baseline_seconds
    elapsed_delta = round(candidate_seconds - baseline_seconds, 12)
    delta = round((ratio - 1.0) * 100.0, 12)
    noise_band = round(
        max(
            baseline_seconds * _MIN_MEANINGFUL_DELTA_RATIO,
            _median_absolute_deviation(candidate_samples)
            + _median_absolute_deviation(baseline_samples),
        ),
        12,
    )
    if abs(elapsed_delta) <= noise_band:
        verdict: Verdict = "same"
    elif elapsed_delta < 0:
        verdict = "faster"
    else:
        verdict = "slower"
    return {
        "ratio": ratio,
        "elapsed_delta_seconds": elapsed_delta,
        "elapsed_delta_percent": delta,
        "noise_band_seconds": noise_band,
        "verdict": verdict,
    }


def _duration(seconds: float) -> str:
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.2f} us"
    if seconds < 1.0:
        return f"{seconds * 1_000:.2f} ms"
    return f"{seconds:.3f} s"


def _difference(comparison: Mapping[str, Any]) -> str:
    verdict = str(comparison["verdict"])
    percent = abs(float(comparison["elapsed_delta_percent"]))
    ratio = float(comparison["ratio"])
    details = [f"{ratio:.2f}x"]
    elapsed_delta = comparison.get("elapsed_delta_seconds")
    if elapsed_delta is not None:
        seconds = float(elapsed_delta)
        sign = "+" if seconds >= 0 else "-"
        details.append(f"Δ {sign}{_duration(abs(seconds))}")
    noise_band = comparison.get("noise_band_seconds")
    if noise_band is not None:
        details.append(f"noise ±{_duration(float(noise_band))}")
    detail_text = ", ".join(details)
    if verdict == "same":
        return f"~ same ({detail_text})"
    return f"{percent:.1f}% {verdict} ({detail_text})"


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def line(values: Sequence[str]) -> str:
        return " | ".join(value.ljust(widths[index]) for index, value in enumerate(values))

    separator = "-+-".join("-" * width for width in widths)
    return "\n".join((line(headers), separator, *(line(row) for row in rows)))


def render_competitive(report: Mapping[str, Any]) -> None:
    """Print one long-form row per natural fpstreams/competitor pairing."""
    metadata = report["metadata"]
    libraries = metadata.get("libraries", {})
    version_text = " · ".join(
        f"{name} {version}" for name, version in libraries.items() if version is not None
    )
    print(
        "fpstreams competitive benchmark · "
        f"Python {metadata['python_version']} · {metadata['platform']} · "
        f"native {metadata['native']['profile']}"
    )
    if version_text:
        print(version_text)
    grouped: dict[str, dict[str, Mapping[str, Any]]] = {}
    ceilings_by_case: dict[str, list[Mapping[str, Any]]] = {}
    case_order: list[str] = []
    for comparison in report["comparisons"]:
        case_id = str(comparison["case"])
        if case_id not in grouped:
            case_order.append(case_id)
            grouped[case_id] = {}
        grouped[case_id][str(comparison["baseline_library"])] = comparison
    for ceiling in report.get("ceilings", ()):
        ceilings_by_case.setdefault(str(ceiling["case"]), []).append(ceiling)

    rows: list[tuple[str, ...]] = []
    display_names = {"python": "Python", "numpy": "NumPy", "pandas": "pandas"}
    for case_id in case_order:
        by_library = grouped[case_id]
        example = next(iter(by_library.values()))
        for library in ("python", "numpy", "pandas"):
            comparison = by_library.get(library)
            rows.append(
                (
                    str(example["api"]),
                    str(example["scope"]),
                    _duration(float(example["candidate_seconds"])),
                    display_names[library],
                    (
                        _duration(float(comparison["baseline_seconds"]))
                        if comparison is not None
                        else "—"
                    ),
                    _difference(comparison) if comparison is not None else "—",
                )
            )
        for ceiling in ceilings_by_case.get(case_id, ()):
            rows.append(
                (
                    str(ceiling["api"]),
                    str(ceiling["scope"]),
                    _duration(float(ceiling["candidate_seconds"])),
                    str(ceiling["ceiling_label"]),
                    _duration(float(ceiling["ceiling_seconds"])),
                    _difference(ceiling),
                )
            )
    print(
        _table(
            ("API", "Scope", "fpstreams", "Compared with", "Other", "Difference"),
            rows,
        )
    )
    if ceilings_by_case:
        print("Python inline ceiling rows omit callback invocation and are not fair comparisons.")
