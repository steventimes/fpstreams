"""Explicit NumPy adapters for record-oriented pipelines."""

from __future__ import annotations

import signal as _signal
import sys
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from importlib import import_module
from typing import Any, cast

from ..planning.numpy import (
    _BUILTIN_DICT,
    _BUILTIN_GETATTR,
    _BUILTIN_INT,
    _BUILTIN_LEN,
    _BUILTIN_MAX,
    _BUILTIN_MIN,
    _BUILTIN_TUPLE,
    _BUILTIN_TYPE,
    _BUILTIN_ZIP,
)
from ..planning.source import Source, SourceCapabilities
from ..runtime import failpoints as _failpoint_module
from .arrow import _column_names

# Small batches keep the transient ``tolist()`` matrix and freshly boxed dicts in
# CPython's small-object allocator while the final result grows independently.
# Larger batches create allocator/GC cliffs on million-row identity materialization.
_NUMPY_IDENTITY_ROW_BATCH_SIZE = 64
# Native assembly avoids transient row lists, but still benefits from bounded column batches:
# 512 rows stays on the same side of the allocator/GC cliffs across widths 1 through 8.
_NUMPY_IDENTITY_COLUMN_BATCH_SIZE = 512
_BUILTIN_FUNCTION_TYPE = type(_BUILTIN_LEN)
_SIGNAL_GETITIMER = getattr(_signal, "getitimer", None)
_SIGNAL_SETITIMER = getattr(_signal, "setitimer", None)
_SIGNAL_INTERVAL_TIMERS = tuple(
    (name, getattr(_signal, name))
    for name in ("ITIMER_REAL", "ITIMER_VIRTUAL", "ITIMER_PROF")
    if hasattr(_signal, name)
)


def numpy_module(operation: str) -> Any:
    """Import NumPy only when an explicit adapter is used."""
    try:
        return import_module("numpy")
    except ImportError:
        raise ImportError(
            f"{operation} requires the 'data' extra: pip install fpstreams[data]"
        ) from None


@dataclass(frozen=True, slots=True)
class NumpyRowSource:
    """Retain one two-dimensional ndarray and its record column names."""

    array: Any
    columns: tuple[str, ...]

    def __len__(self) -> int:
        """Return live cardinality after revalidating the retained matrix width."""
        width = _retained_numpy_width(self.array, self.columns)
        return _validate_retained_numpy_shape(self.array, width)


@dataclass(frozen=True, slots=True)
class NumpyColumnSource:
    """Retain one one-dimensional ndarray for scalar Flow evaluation."""

    array: Any

    def __len__(self) -> int:
        """Return live cardinality after checking that the column is still one-dimensional."""
        if self.array.ndim != 1:
            raise ValueError(f"from_numpy() retained array changed to {self.array.ndim} dimensions")
        return int(self.array.shape[0])

    def i64_buffer(self) -> Any | None:
        """Return the live ndarray only when its buffer exactly represents native i64 values."""
        len(self)
        dtype = self.array.dtype
        if (
            dtype.kind != "i"
            or dtype.itemsize != 8
            or not dtype.isnative
            or not self.array.flags.aligned
            or not self.array.flags.c_contiguous
        ):
            return None
        return self.array

    def f64_buffer(self) -> Any | None:
        """Return the live ndarray only when its buffer exactly represents native f64 values."""
        len(self)
        dtype = self.array.dtype
        if (
            dtype.kind != "f"
            or dtype.itemsize != 8
            or not dtype.isnative
            or not self.array.flags.aligned
            or not self.array.flags.c_contiguous
        ):
            return None
        return self.array


def numpy_i64_buffer(source: object) -> Any | None:
    """Unwrap one validated NumPy i64 column descriptor without importing NumPy eagerly."""
    if not isinstance(source, NumpyColumnSource):
        return None
    return source.i64_buffer()


def numpy_f64_buffer(source: object) -> Any | None:
    """Unwrap one validated NumPy f64 column descriptor without importing NumPy eagerly."""
    if not isinstance(source, NumpyColumnSource):
        return None
    return source.f64_buffer()


def _retained_numpy_width(values: Any, names: tuple[str, ...]) -> int:
    """Revalidate the mutable ndarray shape at the start of each scan."""
    if values.ndim != 2:
        raise ValueError(f"from_numpy() retained array changed to {values.ndim} dimensions")
    width = int(values.shape[1])
    if width != len(names):
        raise ValueError(f"from_numpy() retained array width changed from {len(names)} to {width}")
    return width


def _validate_retained_numpy_shape(values: Any, width: int) -> int:
    """Validate one live matrix shape boundary and return its current row count."""
    shape = values.shape
    if len(shape) != 2:
        raise ValueError(
            f"from_numpy() retained array changed to {len(shape)} dimensions during iteration"
        )
    if int(shape[1]) != width:
        raise ValueError("from_numpy() retained array width changed during iteration")
    return int(shape[0])


def _raise_if_numpy_dtype_changed(dtype: Any, live_dtype: Any) -> None:
    """Raise the canonical retained-array error for one non-identical dtype snapshot."""
    if live_dtype != dtype:
        raise ValueError(
            f"from_numpy() retained array dtype changed from {dtype} "
            f"to {live_dtype} during iteration"
        )


def _validate_retained_numpy_iteration(
    values: Any,
    width: int,
    dtype: Any | None = None,
) -> int:
    """Validate one live matrix boundary and return its current row count."""
    row_count = _validate_retained_numpy_shape(values, width)
    if dtype is not None:
        live_dtype = values.dtype
        if live_dtype is not dtype:
            _raise_if_numpy_dtype_changed(dtype, live_dtype)
    return row_count


def numpy_identity_columns(source: NumpyRowSource) -> dict[str, list[Any]]:
    """Transpose one live retained matrix without constructing row dictionaries."""
    values = source.array
    width = _retained_numpy_width(values, source.columns)
    dtype = getattr(values, "dtype", None)
    if not int(values.shape[0]):
        return {}
    _validate_retained_numpy_iteration(values, width, dtype)
    columns = values.T.tolist()
    _validate_retained_numpy_iteration(values, width, dtype)
    if len(columns) != width:
        raise ValueError("from_numpy() retained array width changed during iteration")
    return _BUILTIN_DICT(_BUILTIN_ZIP(source.columns, columns, strict=True))


def _exact_native_record_assembler() -> Callable[..., list[dict[str, Any]] | None] | None:
    """Return the genuine optional native column-to-record endpoint."""
    try:
        from .. import _native
    except ImportError:
        return None
    endpoint = _BUILTIN_GETATTR(_native, "records_from_exact_columns_v1", None)
    if (
        _BUILTIN_TYPE(endpoint) is not _BUILTIN_FUNCTION_TYPE
        or _BUILTIN_GETATTR(endpoint, "__name__", None) != "records_from_exact_columns_v1"
        or _BUILTIN_GETATTR(endpoint, "__module__", None) != "fpstreams._native"
    ):
        return None
    return cast(Callable[..., list[dict[str, Any]] | None], endpoint)


def numpy_identity_arrow_table(
    pa: Any,
    source: NumpyRowSource,
    *,
    batch_size: int,
) -> Any:
    """Build canonical Arrow batches from one live matrix without row dictionaries."""
    values = source.array
    width = _retained_numpy_width(values, source.columns)
    dtype = getattr(values, "dtype", None)
    if not int(values.shape[0]):
        return pa.table({})
    if not width:
        raise ValueError("cannot infer an Arrow schema from records without columns")

    batches: list[Any] = []
    schema = None
    offset = 0
    while True:
        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        if offset >= row_count:
            break
        columns = values[offset : offset + batch_size].T.tolist()
        _validate_retained_numpy_iteration(values, width, dtype)
        if len(columns) != width:
            raise ValueError("from_numpy() retained array width changed during iteration")
        converted_rows = len(columns[0])
        if not converted_rows:
            break
        batch = pa.RecordBatch.from_pydict(
            _BUILTIN_DICT(_BUILTIN_ZIP(source.columns, columns, strict=True)),
            schema=schema,
        )
        _validate_retained_numpy_iteration(values, width, dtype)
        if schema is None:
            schema = batch.schema
        batches.append(batch)
        offset += converted_rows
    return pa.Table.from_batches(batches) if batches else pa.table({})


def _numpy_records(  # noqa: C901 - narrow-width hot loops avoid per-row dynamic assembly
    values: Any,
    names: tuple[str, ...],
) -> Iterator[dict[str, Any]]:
    """Choose one narrow-row materializer before scanning the retained array."""
    width = _retained_numpy_width(values, names)
    dtype = getattr(values, "dtype", None)
    _validate_retained_numpy_iteration(values, width, dtype)

    match names:
        case (first,):
            for row in values:
                converted = row.tolist()
                _validate_retained_numpy_shape(values, width)
                live_dtype = values.dtype
                if live_dtype is not dtype:
                    _raise_if_numpy_dtype_changed(dtype, live_dtype)
                if len(converted) != width:
                    raise ValueError("from_numpy() retained array width changed during iteration")
                yield {first: converted[0]}
        case (first, second):
            for row in values:
                converted = row.tolist()
                _validate_retained_numpy_shape(values, width)
                live_dtype = values.dtype
                if live_dtype is not dtype:
                    _raise_if_numpy_dtype_changed(dtype, live_dtype)
                if len(converted) != width:
                    raise ValueError("from_numpy() retained array width changed during iteration")
                yield {first: converted[0], second: converted[1]}
        case (first, second, third):
            for row in values:
                converted = row.tolist()
                _validate_retained_numpy_shape(values, width)
                live_dtype = values.dtype
                if live_dtype is not dtype:
                    _raise_if_numpy_dtype_changed(dtype, live_dtype)
                if len(converted) != width:
                    raise ValueError("from_numpy() retained array width changed during iteration")
                yield {
                    first: converted[0],
                    second: converted[1],
                    third: converted[2],
                }
        case (first, second, third, fourth):
            for row in values:
                converted = row.tolist()
                _validate_retained_numpy_shape(values, width)
                live_dtype = values.dtype
                if live_dtype is not dtype:
                    _raise_if_numpy_dtype_changed(dtype, live_dtype)
                if len(converted) != width:
                    raise ValueError("from_numpy() retained array width changed during iteration")
                yield {
                    first: converted[0],
                    second: converted[1],
                    third: converted[2],
                    fourth: converted[3],
                }
        case _:
            for row in values:
                converted = row.tolist()
                _validate_retained_numpy_shape(values, width)
                live_dtype = values.dtype
                if live_dtype is not dtype:
                    _raise_if_numpy_dtype_changed(dtype, live_dtype)
                yield dict(zip(names, converted, strict=True))


def _materialize_numpy_identity_batch(
    names: tuple[str, ...],
    converted: list[list[Any]],
) -> list[dict[str, Any]]:
    """Box one validated narrow matrix batch without repeated schema scans."""
    match names:
        case (first,):
            return [{first: row[0]} for row in converted]
        case (first, second):
            return [{first: row[0], second: row[1]} for row in converted]
        case (first, second, third):
            return [{first: row[0], second: row[1], third: row[2]} for row in converted]
        case (first, second, third, fourth):
            return [
                {
                    first: row[0],
                    second: row[1],
                    third: row[2],
                    fourth: row[3],
                }
                for row in converted
            ]
        case (first, second, third, fourth, fifth):
            return [
                {
                    first: row[0],
                    second: row[1],
                    third: row[2],
                    fourth: row[3],
                    fifth: row[4],
                }
                for row in converted
            ]
        case (first, second, third, fourth, fifth, sixth):
            return [
                {
                    first: row[0],
                    second: row[1],
                    third: row[2],
                    fourth: row[3],
                    fifth: row[4],
                    sixth: row[5],
                }
                for row in converted
            ]
        case (first, second, third, fourth, fifth, sixth, seventh):
            return [
                {
                    first: row[0],
                    second: row[1],
                    third: row[2],
                    fourth: row[3],
                    fifth: row[4],
                    sixth: row[5],
                    seventh: row[6],
                }
                for row in converted
            ]
        case (first, second, third, fourth, fifth, sixth, seventh, eighth):
            return [
                {
                    first: row[0],
                    second: row[1],
                    third: row[2],
                    fourth: row[3],
                    fifth: row[4],
                    sixth: row[5],
                    seventh: row[6],
                    eighth: row[7],
                }
                for row in converted
            ]
        case _:  # pragma: no cover - the caller's width guard owns this invariant
            raise RuntimeError("unsupported NumPy identity row width")


def numpy_identity_rows(source: NumpyRowSource) -> list[dict[str, Any]]:
    """Collect common narrow records through bounded matrix-to-Python batches."""
    values = source.array
    names = source.columns
    width = _retained_numpy_width(values, names)
    dtype = getattr(values, "dtype", None)
    if not 1 <= width <= 8:
        return list(_numpy_records(values, names))

    np = sys.modules.get("numpy")
    assembler = (
        _exact_native_record_assembler()
        if np is not None
        and type(values) is _BUILTIN_GETATTR(np, "ndarray", None)
        and _BUILTIN_INT(values.shape[0]) >= _NUMPY_IDENTITY_ROW_BATCH_SIZE
        else None
    )
    if assembler is not None:
        return _numpy_identity_rows_native(source, width, dtype, assembler)

    output: list[dict[str, Any]] = []
    offset = 0
    while True:
        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        if offset >= row_count:
            break
        stop = _BUILTIN_MIN(row_count, offset + _NUMPY_IDENTITY_ROW_BATCH_SIZE)
        converted = values[offset:stop].tolist()

        # A signal handler can resize a retained matrix after NumPy has boxed the slice.
        # Trim rows removed in that interval; growth is picked up by the next loop, matching
        # the canonical live ndarray iterator without retaining one unbounded value snapshot.
        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        available = _BUILTIN_MIN(
            len(converted),
            _BUILTIN_MAX(0, row_count - offset),
        )
        if available != len(converted):
            del converted[available:]
        if not converted:
            if offset < row_count:
                raise ValueError("from_numpy() retained array row count changed during iteration")
            break

        records = _materialize_numpy_identity_batch(names, converted)

        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        available = _BUILTIN_MIN(
            len(records),
            _BUILTIN_MAX(0, row_count - offset),
        )
        if available:
            output.extend(records if available == len(records) else records[:available])
            offset += available
    return output


def _numpy_identity_rows_native(
    source: NumpyRowSource,
    width: int,
    dtype: Any,
    assembler: Callable[..., list[dict[str, Any]] | None],
) -> list[dict[str, Any]]:
    """Box private column batches and assemble records without transient row lists."""
    values = source.array
    names = source.columns
    output: list[dict[str, Any]] = []
    offset = 0
    while True:
        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        if offset >= row_count:
            break
        stop = _BUILTIN_MIN(row_count, offset + _NUMPY_IDENTITY_COLUMN_BATCH_SIZE)
        converted = values[offset:stop].T.tolist()

        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        if _BUILTIN_LEN(converted) != width:
            raise ValueError("from_numpy() retained array width changed during iteration")
        converted_rows = _BUILTIN_LEN(converted[0])
        available = _BUILTIN_MIN(
            converted_rows,
            _BUILTIN_MAX(0, row_count - offset),
        )
        if available != converted_rows:
            for column in converted:
                del column[available:]
        if not available:
            if offset < row_count:
                raise ValueError("from_numpy() retained array row count changed during iteration")
            break

        columns = _BUILTIN_TUPLE(converted)
        records = assembler(names, columns)
        if records is None:
            records = [
                _BUILTIN_DICT(_BUILTIN_ZIP(names, row, strict=True))
                for row in _BUILTIN_ZIP(*columns, strict=True)
            ]

        row_count = _validate_retained_numpy_iteration(values, width, dtype)
        available = _BUILTIN_MIN(
            _BUILTIN_LEN(records),
            _BUILTIN_MAX(0, row_count - offset),
        )
        if available:
            output.extend(records if available == _BUILTIN_LEN(records) else records[:available])
            offset += available
    return output


def _numpy_callbacks_require_python_loop() -> bool:
    """Keep Python iteration while free-threaded writes or an armed timer can race it."""
    is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if callable(is_gil_enabled) and not is_gil_enabled():
        # NumPy does not lock mutable ndarray data for readers on free-threaded CPython.
        # Keep the canonical row boundary instead of widening a concurrent snapshot window.
        return True
    getitimer = _SIGNAL_GETITIMER
    if getitimer is not None:
        if (
            getattr(_signal, "getitimer", None) is not _SIGNAL_GETITIMER
            or getattr(_signal, "setitimer", None) is not _SIGNAL_SETITIMER
            or any(getattr(_signal, name, None) != timer for name, timer in _SIGNAL_INTERVAL_TIMERS)
        ):
            return True
        try:
            for _name, timer in _SIGNAL_INTERVAL_TIMERS:
                delay, interval = getitimer(timer)
                if delay or interval:
                    return True
        except (OSError, TypeError, ValueError):
            return True
    return False


def guarded_numpy_identity_source(
    source: object,
    *,
    observers: bool = True,
    exact_names: bool = True,
) -> NumpyRowSource | None:
    """Return an owned retained row source while its runtime boundaries are safe."""
    if (
        type(source) is not Source
        or not source.capabilities.reiterable
        or _failpoint_module.has_active_failpoints()
        or (observers and _numpy_callbacks_require_python_loop())
    ):
        return None
    descriptor = source.native_data
    if (
        type(descriptor) is not NumpyRowSource
        or descriptor is not source._live_size_data
        or (exact_names and any(type(name) is not str for name in descriptor.columns))
    ):
        return None
    return descriptor


def numpy_identity_array(
    np: Any,
    source: NumpyRowSource,
    *,
    dtype: Any = None,
    copy: bool | None = None,
) -> Any:
    """Convert one identity matrix while preserving its live structural boundary."""
    values = source.array
    width = _retained_numpy_width(values, source.columns)
    source_dtype = getattr(values, "dtype", None)
    _validate_retained_numpy_iteration(values, width, source_dtype)
    result = numpy_array(np, values, dtype=dtype, copy=copy)
    _validate_retained_numpy_iteration(values, width, source_dtype)
    return result


def _numpy_scalars(values: Any) -> Iterator[Any]:
    """Read Python scalar values lazily from one retained ndarray."""
    if values.ndim != 1:
        raise ValueError(f"from_numpy() retained array changed to {values.ndim} dimensions")
    length = int(values.shape[0])
    for index in range(length):
        if values.ndim != 1 or int(values.shape[0]) != length:
            raise ValueError("from_numpy() retained array length changed during iteration")
        yield values.item(index)


def _guarded_numpy_column(
    source: object,
    buffer_name: str,
) -> Any | None:
    """Return one owned, contiguous numeric ndarray when callbacks cannot mutate it."""
    if (
        type(source) is not Source
        or not source.capabilities.reiterable
        or _failpoint_module.has_active_failpoints()
        or _numpy_callbacks_require_python_loop()
    ):
        return None
    descriptor = source.native_data
    if type(descriptor) is not NumpyColumnSource or descriptor is not source._live_size_data:
        return None
    np = sys.modules.get("numpy")
    if np is None or type(descriptor.array) is not getattr(np, "ndarray", None):
        return None
    values = getattr(descriptor, buffer_name)()
    if type(values) is not getattr(np, "ndarray", None):
        return None
    return values


def guarded_numpy_i64_column(source: object) -> Any | None:
    """Return one exact, owned i64 ndarray for direct reduction."""
    return _guarded_numpy_column(source, "i64_buffer")


def guarded_numpy_f64_column(source: object) -> Any | None:
    """Return one exact, owned f64 ndarray for direct reduction."""
    return _guarded_numpy_column(source, "f64_buffer")


def numpy_scalar_source(
    values: Any,
    *,
    columns: Iterable[str] | None = None,
) -> Source[Any]:
    """Build a replayable scalar source from an already normalized 1D ndarray."""
    if columns is not None:
        raise ValueError("from_numpy() columns are only valid for two-dimensional arrays")
    descriptor = NumpyColumnSource(values)

    def scalars() -> Iterator[Any]:
        return _numpy_scalars(values)

    return Source(
        scalars,
        SourceCapabilities(
            reiterable=True,
            exact_size=int(values.shape[0]),
            ordered=True,
        ),
        native_data=descriptor,
        live_size_data=descriptor,
    )


def numpy_source(
    array: Any,
    *,
    columns: Iterable[str] | None = None,
) -> Source[dict[str, Any]]:
    """Build a replayable source that converts ndarray rows only when pulled."""
    np = numpy_module("from_numpy()")
    values = np.asarray(array)
    if values.ndim != 2:
        raise ValueError(
            f"from_numpy() expects a two-dimensional array, got {values.ndim} dimensions"
        )

    width = int(values.shape[1])
    names = (
        tuple(str(index) for index in range(width))
        if columns is None
        else _column_names(columns, operation="from_numpy")
    )
    if len(names) != width:
        raise ValueError(f"from_numpy() expected {width} columns, got {len(names)}")

    descriptor = NumpyRowSource(values, names)

    def records() -> Iterator[dict[str, Any]]:
        return _numpy_records(values, names)

    return Source(
        records,
        SourceCapabilities(
            reiterable=True,
            exact_size=int(values.shape[0]),
            ordered=True,
        ),
        native_data=descriptor,
        live_size_data=descriptor,
    )


def numpy_array(
    np: Any,
    values: Any,
    *,
    dtype: Any = None,
    copy: bool | None = None,
) -> Any:
    """Apply the installed NumPy version's copy semantics."""
    if copy is None:
        return np.asarray(values, dtype=dtype)
    return np.array(values, dtype=dtype, copy=copy)
