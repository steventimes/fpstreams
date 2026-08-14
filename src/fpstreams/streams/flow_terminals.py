"""Terminal operations shared by synchronous Flow instances."""

from __future__ import annotations

import builtins
import csv
import heapq
import json
import math
import operator
import os
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from numbers import Real
from typing import Any, Generic, TypeVar, cast

from ..collecting.aggregation import (
    Aggregator,
    finish_native_aggregations,
    native_aggregation_items,
    native_first_only,
    prepare_aggregations,
    run_aggregations,
)
from ..collecting.collector import Collector, prepare_collectors, run_collectors
from ..collecting.statistics import (
    OnlineStatistics,
    StatisticsSnapshot,
    mean_from,
    std_from,
    validate_ddof,
    variance_from,
)
from ..errors import BufferLimitError, EmptyFlowError
from ..execution import (
    exact_count,
    execute,
    try_native_aggregate,
    try_native_statistics,
    try_native_terminal,
)
from ..expressions.scalar import Expr, FExpr
from ..expressions.selectors import Selector, compile_selector
from ..io_safety import spreadsheet_safe_cell
from ..planning.sync import Plan
from ..primitives.result import Err, Ok

T = TypeVar("T")
R = TypeVar("R")
C = TypeVar("C")
_MISSING = object()


class FlowTerminalsMixin(Generic[T]):
    """Terminal and reduction methods mixed into the public Flow class."""

    _plan: Plan

    def __iter__(self) -> Iterator[T]:
        raise NotImplementedError

    def _open(self) -> Any:
        raise NotImplementedError

    def filter(self, predicate: Callable[[T], Any]) -> FlowTerminalsMixin[T]:
        raise NotImplementedError

    def reject(self, predicate: Callable[[T], Any]) -> FlowTerminalsMixin[T]:
        raise NotImplementedError

    def drop(self, count: int) -> FlowTerminalsMixin[T]:
        raise NotImplementedError

    def to_list(self) -> list[T]:
        """Execute the pipeline and collect its items in a list.

        Returns:
            A list containing the consumed results in encounter order.
        """
        return list(execute(self._plan))

    def to_tuple(self) -> tuple[T, ...]:
        """Execute the pipeline and collect its items in a tuple.

        Returns:
            A tuple containing the resulting values.
        """
        return tuple(execute(self._plan))

    def to_set(self) -> set[T]:
        """Execute the pipeline and collect distinct hashable items.

        Returns:
            A set containing the distinct resulting values.
        """
        return set(execute(self._plan))

    def to_pandas(self, columns: Iterable[str] | None = None) -> Any:
        """Execute the pipeline and build a pandas DataFrame.

        Args:
            columns: The columns or column mapping used by the operation.

        Returns:
            A pandas `DataFrame` containing the emitted items.
        """
        try:
            import pandas as pd  # type: ignore[import-untyped]
        except ImportError:
            raise ImportError(
                "to_pandas() requires the 'data' extra: pip install fpstreams[data]"
            ) from None
        names = list(columns) if columns is not None else None
        return pd.DataFrame(self.to_list(), columns=names)

    to_df = to_pandas

    def to_numpy(self, dtype: Any = None) -> Any:
        """Execute the pipeline and build a NumPy array.

        Args:
            dtype: The optional NumPy data type used for the resulting array.

        Returns:
            A NumPy array containing the emitted items.
        """
        try:
            import numpy as np
        except ImportError:
            raise ImportError(
                "to_numpy() requires the 'data' extra: pip install fpstreams[data]"
            ) from None
        return np.asarray(self.to_list(), dtype=dtype)

    to_np = to_numpy

    def to_csv(
        self,
        path: str | os.PathLike[str],
        *,
        header: Iterable[str] | None = None,
        encoding: str = "utf-8",
        spreadsheet_safe: bool = False,
    ) -> None:
        """Execute the pipeline and stream its items to a CSV file.

        Args:
            path: The filesystem path to read from or write to.
            header: The CSV header policy or explicit field names.
            encoding: The text encoding used to open the file.
            spreadsheet_safe: Whether to neutralize cells that spreadsheet programs may execute.
        """
        columns = tuple(header) if header is not None else None
        with (
            self._open() as iterator,
            open(path, "w", encoding=encoding, newline="") as handle,
        ):
            writer = csv.writer(handle, lineterminator="\n")
            if columns is not None:
                writer.writerow(columns)
            for item in iterator:
                values: Iterable[Any]
                if isinstance(item, Mapping):
                    values = (
                        [item.get(column) for column in columns]
                        if columns is not None
                        else item.values()
                    )
                elif isinstance(item, (list, tuple)):
                    values = item
                else:
                    values = (item,)
                writer.writerow(
                    [spreadsheet_safe_cell(value) for value in values]
                    if spreadsheet_safe
                    else values
                )

    def to_json(
        self,
        path: str | os.PathLike[str],
        *,
        encoding: str = "utf-8",
        ensure_ascii: bool = False,
        default: Callable[[Any], Any] | None = None,
    ) -> None:
        """Execute the pipeline and stream its items to one JSON array.

        Args:
            path: The filesystem path to read from or write to.
            encoding: The text encoding used to open the file.
            ensure_ascii: Whether JSON output escapes non-ASCII characters.
            default: The value returned when no matching item is available.

        """
        encoder = (
            json.JSONEncoder(ensure_ascii=ensure_ascii)
            if default is None
            else json.JSONEncoder(ensure_ascii=ensure_ascii, default=default)
        )
        with self._open() as iterator, open(path, "w", encoding=encoding) as handle:
            handle.write("[")
            first = True
            for item in iterator:
                if not first:
                    handle.write(",")
                first = False
                for chunk in encoder.iterencode(item):
                    handle.write(chunk)
            handle.write("]")

    def describe(self) -> dict[str, int | float]:
        """Return count and one-pass summary statistics for numeric items.

        Returns:
            A dictionary containing the computed keys and values.
        """
        count = 0
        numeric_count = 0
        total = 0.0
        compensation = 0.0
        mean = 0.0
        squared_deviations = 0.0
        minimum = maximum = 0.0

        with self._open() as iterator:
            for item in iterator:
                count += 1
                if not isinstance(item, Real):
                    continue
                value = float(item)
                numeric_count += 1
                combined = total + value
                if math.isfinite(total) and math.isfinite(value) and math.isfinite(combined):
                    compensation += (
                        (total - combined) + value
                        if abs(total) >= abs(value)
                        else (value - combined) + total
                    )
                else:
                    compensation = 0.0
                total = combined

                delta = value - mean
                mean += delta / numeric_count
                squared_deviations += delta * (value - mean)
                if numeric_count == 1:
                    minimum = maximum = value
                else:
                    minimum = value if value < minimum else minimum
                    maximum = value if value > maximum else maximum

        if count == 0:
            return {}
        result: dict[str, int | float] = {"count": count}
        if numeric_count == 0:
            return result
        if numeric_count != count:
            result["numeric_count"] = numeric_count
        result.update(
            {
                "sum": total + compensation,
                "min": minimum,
                "max": maximum,
                "mean": mean,
            }
        )
        if numeric_count > 1:
            result["std"] = math.sqrt(max(squared_deviations, 0.0) / (numeric_count - 1))
        return result

    def aggregate(self, **aggregations: Aggregator) -> dict[str, Any]:
        """Compute several named aggregations while traversing the flow once.

        All named aggregators are updated during the same source traversal.

        Args:
            **aggregations: Named aggregators evaluated during the same traversal.

        Returns:
            A dictionary containing the computed keys and values.
        """
        items = prepare_aggregations(aggregations)
        first_name = native_first_only(items)
        if first_name is not None:
            native, result = try_native_terminal(self._plan, "first")
            if native:
                return {first_name: result}
        if native_aggregation_items(items):
            native, snapshot = try_native_aggregate(self._plan)
            if native:
                if snapshot is None:
                    raise RuntimeError("native aggregate result is missing")
                return finish_native_aggregations(items, snapshot)
        return run_aggregations(self, items)

    summarize = aggregate

    def collect(
        self,
        collector: Callable[[Iterable[T]], C] | Collector[T, Any, C] | None = None,
        /,
        **collectors: Collector[T, Any, Any],
    ) -> C | dict[str, Any]:
        """Reduce the pipeline with one Collector or named Collectors.

        One collector returns its finished value. Named collectors share one source traversal
        and return a dictionary.

        Args:
            collector: The collector used to reduce input items.
            **collectors: Named collectors evaluated during the same traversal.

        Returns:
            The collector result, or a dictionary of results for named collectors.
        """
        if collector is not None and collectors:
            raise TypeError("collect accepts either one collector or named collectors, not both")
        if collector is not None:
            return collector(self)
        return run_collectors(self, prepare_collectors(cast(Any, collectors)))

    def join(self, separator: str = "") -> str:
        """Convert items to strings and join them with separator.

        This is a string terminal operation; it consumes the flow and does not perform a
        relational join.

        Args:
            separator: The string inserted between adjacent string representations.

        Returns:
            One string containing every item separated by `separator`.
        """
        return separator.join(str(item) for item in self)

    def for_each(self, action: Callable[[T], Any]) -> None:
        """Execute action once for every item.

        Args:
            action: The side-effecting callable invoked for each matching item.
        """
        for item in self:
            action(item)

    def partition(self, predicate: Callable[[T], bool]) -> tuple[list[T], list[T]]:
        """Collect matching and non-matching items in separate lists.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            A tuple containing the resulting values.
        """
        matches: list[T] = []
        misses: list[T] = []
        for item in self:
            (matches if predicate(item) else misses).append(item)
        return matches, misses

    def partition_results(self) -> tuple[list[Any], list[Exception]]:
        """Separate Result values into successes and failures.

        Returns:
            A tuple containing the resulting values.
        """
        successes: list[Any] = []
        failures: list[Exception] = []
        for result in self:
            if isinstance(result, Ok):
                successes.append(result.value)
            elif isinstance(result, Err):
                failures.append(result.error)
            else:
                raise TypeError("partition_results() requires Result values")
        return successes, failures

    def to_async(self) -> Any:
        """View this synchronous pipeline as an AsyncFlow.

        Returns:
            An `AsyncFlow` that yields the same items.
        """
        from .async_flow import aflow

        return aflow(self)

    def first(self, default: Any = _MISSING) -> T | Any:
        """Return the first item without consuming an unnecessary tail.

        Args:
            default: The value returned when no matching item is available.

        Returns:
            The first item, or `default` when the flow is empty.

        Raises:
            EmptyFlowError: If the flow is empty and no default is supplied.
        """
        native, result = try_native_terminal(self._plan, "first")
        if native:
            if result is not None:
                return cast(T, result)
            if default is _MISSING:
                raise EmptyFlowError("first() requires at least one item")
            return default
        with self._open() as iterator:
            try:
                return next(iterator)
            except StopIteration:
                if default is _MISSING:
                    raise EmptyFlowError("first() requires at least one item") from None
                return default

    def last(self, default: Any = _MISSING) -> T | Any:
        """Return the last item, or default when the flow is empty.

        Args:
            default: The value returned when no matching item is available.

        Returns:
            The last item, or `default` when the flow is empty.
        """
        native, result = try_native_terminal(self._plan, "last")
        if native:
            if result is not None:
                return cast(T, result)
            if default is _MISSING:
                raise EmptyFlowError("last() requires at least one item")
            return default
        found = default
        for item in self:
            found = item
        if found is _MISSING:
            raise EmptyFlowError("last() requires at least one item")
        return found

    def find(self, predicate: Callable[[T], Any], default: Any = _MISSING) -> T | Any:
        """Return the first matching item, a default, or raise EmptyFlowError.

        Args:
            predicate: A callable that decides whether an item matches.
            default: The value returned when no matching item is available.

        Returns:
            The first matching item, or `default` when no item matches.
        """
        matches = self.filter(predicate)
        if default is not _MISSING:
            return matches.first(default)
        try:
            return matches.first()
        except EmptyFlowError:
            raise EmptyFlowError("find() found no matching item") from None

    def find_index(self, predicate: Callable[[T], Any]) -> int | None:
        """Return the index of the first matching item, or None.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            The matching or computed value, or `None` when unavailable.
        """
        if not callable(predicate):
            raise TypeError("predicate must be callable")
        with self._open() as iterator:
            for position, item in enumerate(iterator):
                if predicate(item):
                    return position
        return None

    def index_of(self, value: T) -> int | None:
        """Return the index of the first equal value, or None.

        Args:
            value: The value consumed by this operation.

        Returns:
            The matching or computed value, or `None` when unavailable.
        """
        return self.find_index(lambda item: item == value)

    def nth(self, index: int, default: Any = _MISSING) -> T | Any:
        """Return the item at a positive or negative index.

        Args:
            index: The zero-based item or field position to select.
            default: The value returned when no matching item is available.

        Returns:
            The selected item, or `default` when the index is out of range.
        """
        position = operator.index(index)
        if position >= 0:
            candidate = self.drop(position)
            if default is not _MISSING:
                return candidate.first(default)
            try:
                return candidate.first()
            except EmptyFlowError:
                raise EmptyFlowError(f"nth({position}) is out of range") from None
        if position == -1:
            if default is not _MISSING:
                return self.last(default)
            try:
                return self.last()
            except EmptyFlowError:
                raise EmptyFlowError("nth(-1) is out of range") from None

        width = -position
        with self._open() as iterator:
            tail: deque[T] = deque(iterator, maxlen=width)
        if len(tail) == width:
            return tail[0]
        if default is not _MISSING:
            return default
        raise EmptyFlowError(f"nth({position}) is out of range")

    def count(self) -> int:
        """Count all items produced by the pipeline.

        Returns:
            The number of matching input items.
        """
        known_size = exact_count(self._plan)
        if known_size is not None:
            return known_size
        native, result = try_native_terminal(self._plan, "count")
        if native:
            return cast(int, result)
        return builtins.sum(1 for _ in self)

    def sum(self, start: Any = 0) -> Any:
        """Add all items, starting with start.

        Args:
            start: The first index, numeric value, or additive identity to use.

        Returns:
            The total of `start` and all emitted items.
        """
        if type(start) is int and start == 0:
            native, result = try_native_terminal(self._plan, "sum")
            if native:
                return result
        return builtins.sum(cast(Iterable[Any], self), start)

    def _statistics_snapshot(self) -> StatisticsSnapshot:
        native, snapshot = try_native_statistics(self._plan)
        if native:
            if snapshot is None:
                raise RuntimeError("native statistics result is missing")
            return snapshot
        statistics = OnlineStatistics()
        with self._open() as iterator:
            for item in iterator:
                statistics.accept(item)
        return statistics.snapshot()

    def mean(self) -> float | None:
        """Return the arithmetic mean, or None for an empty flow.

        Returns:
            The matching or computed value, or `None` when unavailable.
        """
        return mean_from(self._statistics_snapshot())

    average = mean

    def variance(self, *, ddof: int = 1) -> float | None:
        """Return the variance, or None when too few values are available.

        Args:
            ddof: Delta degrees of freedom used in the variance divisor.

        Returns:
            The matching or computed value, or `None` when unavailable.
        """
        validate_ddof(ddof)
        return variance_from(self._statistics_snapshot(), ddof)

    def std(self, *, ddof: int = 1) -> float | None:
        """Return the standard deviation, or None when too few values are available.

        Args:
            ddof: Delta degrees of freedom used in the variance divisor.

        Returns:
            The matching or computed value, or `None` when unavailable.
        """
        validate_ddof(ddof)
        return std_from(self._statistics_snapshot(), ddof)

    def min(self, *, key: Callable[[T], Any] | None = None) -> T:
        """Return the smallest item and raise on an empty flow.

        Args:
            key: The callable or selector used to derive a key.

        Returns:
            The smallest item according to `key`.
        """
        if key is None:
            native, result = try_native_terminal(self._plan, "min")
            if native:
                if result is None:
                    raise EmptyFlowError("min() requires at least one item")
                return cast(T, result)
        try:
            return cast(T, builtins.min(cast(Iterable[Any], self), key=key))
        except ValueError:
            raise EmptyFlowError("min() requires at least one item") from None

    def max(self, *, key: Callable[[T], Any] | None = None) -> T:
        """Return the largest item and raise on an empty flow.

        Args:
            key: The callable or selector used to derive a key.

        Returns:
            The largest item according to `key`.
        """
        if key is None:
            native, result = try_native_terminal(self._plan, "max")
            if native:
                if result is None:
                    raise EmptyFlowError("max() requires at least one item")
                return cast(T, result)
        try:
            return cast(T, builtins.max(cast(Iterable[Any], self), key=key))
        except ValueError:
            raise EmptyFlowError("max() requires at least one item") from None

    def top(self, count: int, *, key: Selector | None = None) -> list[T]:
        """Return up to count largest items without sorting the entire result.

        Args:
            count: The requested number of items.
            key: The callable or selector used to derive a key.

        Returns:
            A list containing the consumed results in encounter order.
        """
        if count < 0:
            raise ValueError("top count must be non-negative")
        select = None if key is None else cast(Callable[[T], Any], compile_selector(key))
        with self._open() as iterator:
            return heapq.nlargest(count, iterator, key=cast(Any, select))

    greatest = top

    def bottom(self, count: int, *, key: Selector | None = None) -> list[T]:
        """Return up to count smallest items without sorting the entire result.

        Args:
            count: The requested number of items.
            key: The callable or selector used to derive a key.

        Returns:
            A list containing the consumed results in encounter order.
        """
        if count < 0:
            raise ValueError("bottom count must be non-negative")
        select = None if key is None else cast(Callable[[T], Any], compile_selector(key))
        with self._open() as iterator:
            return heapq.nsmallest(count, iterator, key=cast(Any, select))

    least = bottom

    def minmax(self, *, key: Selector | None = None) -> tuple[T, T]:
        """Return the smallest and largest items in one traversal.

        Args:
            key: The callable or selector used to derive a key.

        Returns:
            A tuple containing the resulting values.
        """
        select: Callable[[T], Any] = (
            (lambda item: item) if key is None else cast(Callable[[T], Any], compile_selector(key))
        )
        with self._open() as iterator:
            try:
                minimum = maximum = next(iterator)
            except StopIteration:
                raise EmptyFlowError("minmax() requires at least one item") from None
            minimum_key = maximum_key = select(minimum)
            for item in iterator:
                item_key = select(item)
                if item_key < minimum_key:
                    minimum = item
                    minimum_key = item_key
                if item_key > maximum_key:
                    maximum = item
                    maximum_key = item_key
        return minimum, maximum

    def any(self, predicate: Callable[[T], bool] = bool) -> bool:
        """Return whether at least one item satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            Whether the condition described above is true.
        """
        if predicate is bool:
            native, result = try_native_terminal(self._plan, "any")
            if native:
                return bool(result)
        elif isinstance(predicate, (Expr, FExpr)):
            filtered = self.filter(cast(Callable[[T], Any], predicate))
            native, result = try_native_terminal(filtered._plan, "first")
            if native:
                return result is not None
        with self._open() as iterator:
            return builtins.any(predicate(item) for item in iterator)

    def all(self, predicate: Callable[[T], bool] = bool) -> bool:
        """Return whether every item satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            Whether the condition described above is true.
        """
        if predicate is bool:
            native, result = try_native_terminal(self._plan, "all")
            if native:
                return bool(result)
        elif isinstance(predicate, (Expr, FExpr)):
            rejected = self.reject(cast(Callable[[T], Any], predicate))
            native, result = try_native_terminal(rejected._plan, "first")
            if native:
                return result is None
        with self._open() as iterator:
            return builtins.all(predicate(item) for item in iterator)

    def none(self, predicate: Callable[[T], bool] = bool) -> bool:
        """Return whether no item satisfies predicate.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            Whether the condition described above is true.
        """
        return not self.any(predicate)

    def reduce(
        self,
        function: Callable[[Any, T], Any],
        initial: Any = _MISSING,
    ) -> Any:
        """Combine items from left to right with an optional initial value.

        Args:
            function: The callable applied by this operation.
            initial: The initial accumulator value. When omitted, the first item is used where
                supported.

        Returns:
            The final left-to-right accumulator.
        """
        with self._open() as iterator:
            if initial is _MISSING:
                try:
                    accumulator = next(iterator)
                except StopIteration:
                    raise EmptyFlowError("reduce() requires at least one item") from None
            else:
                accumulator = initial
            for item in iterator:
                accumulator = function(accumulator, item)
        return accumulator

    def reduce_right(
        self,
        function: Callable[[T, Any], Any],
        initial: Any = _MISSING,
        *,
        max_items: int | None = None,
    ) -> Any:
        """Combine buffered items from right to left.

        Args:
            function: The callable applied by this operation.
            initial: The initial accumulator value. When omitted, the first item is used where
                supported.
            max_items: The maximum number of source items allowed in the right-side buffer.

        Returns:
            The final right-to-left accumulator.
        """
        if not callable(function):
            raise TypeError("function must be callable")
        if max_items is not None:
            max_items = operator.index(max_items)
            if max_items < 0:
                raise ValueError("max_items must be non-negative")
        values: list[T] = []
        with self._open() as iterator:
            for item in iterator:
                if max_items is not None and len(values) >= max_items:
                    raise BufferLimitError(f"reduce_right() exceeded max_items={max_items}")
                values.append(item)
        if initial is _MISSING:
            if not values:
                raise EmptyFlowError("reduce_right() requires at least one item")
            accumulator = values.pop()
        else:
            accumulator = initial
        for item in reversed(values):
            accumulator = function(item, accumulator)
        return accumulator

    fold_right = reduce_right

    def reduce_by(
        self,
        key: Selector,
        function: Callable[[R, T], R],
        *,
        initializer: Callable[[], R],
    ) -> dict[Any, R]:
        """Reduce items independently for each selected key.

        Args:
            key: The callable or selector used to derive a key.
            function: The callable applied by this operation.
            initializer: A zero-argument callable that creates fresh mutable state.

        Returns:
            A dictionary containing the computed keys and values.
        """
        if not callable(initializer):
            raise TypeError("initializer must be callable")
        select = compile_selector(key)
        states: dict[Any, R] = {}
        with self._open() as iterator:
            for item in iterator:
                group = select(item)
                try:
                    state = states[group]
                except KeyError:
                    state = initializer()
                except TypeError:
                    raise TypeError("reduce_by() keys must be hashable") from None
                states[group] = function(state, item)
        return states

    fold_by = reduce_by

    def frequencies(self, key: Selector | None = None) -> dict[Any, int]:
        """Count occurrences of values or selected keys.

        Args:
            key: The callable or selector used to derive a key.

        Returns:
            A dictionary containing the computed keys and values.
        """
        selector: Selector = (lambda item: item) if key is None else key
        return self.reduce_by(
            selector,
            lambda count, _item: count + 1,
            initializer=lambda: 0,
        )

    count_by = frequencies
