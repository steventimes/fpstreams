from typing import (
    Iterator, Iterable, Callable, Optional, Any, List, 
    Set, Tuple, cast, Dict, Sized, TYPE_CHECKING, Sequence
)
from ..option import Option
from ..exceptions import StreamEmptyError
from ..result import Result
from .stream_interface import BaseStream
from . import ops
from .. import rust_ops
from .common import T, R
import json
import csv
import statistics
import itertools
import functools
import numbers

if TYPE_CHECKING:
    from .async_stream import AsyncStream
class SequentialStream(BaseStream[T]):
    
    def __init__(self, iterable: Iterable[T], size_hint: Optional[int] = None):
        """
        Args:
            iterable: The data source.
            size_hint: Known size of the stream. If None, we calculate it if possible.
        """
        self._iterable = iterable 
        self._iterator: Iterator[T] = iter(iterable)
        
        # --- Fast Count Logic ---
        if size_hint is not None:
            self._size_hint = size_hint
        elif isinstance(iterable, Sized):
            self._size_hint = len(iterable)
        else:
            self._size_hint = None
        
    def __iter__(self) -> Iterator[T]:
        """Allows the stream to be used in standard for-loops."""
        return self._iterator

    @staticmethod
    def chain(stream1: "BaseStream[T]", stream2: "BaseStream[T]") -> "SequentialStream[T]":
        return SequentialStream(itertools.chain(stream1.to_list(), stream2.to_list()))

    @staticmethod
    def of(*elements: T) -> "SequentialStream[T]":
        """
        Creates a stream from a sequence of values.
        Usage: Stream.of(1, 2, 3, 4)
        """
        return SequentialStream(elements)
    
    @staticmethod
    def generate(supplier: Callable[[], T]) -> "SequentialStream[T]":
        """
        Creates an infinite stream by calling supplier() repeatedly.
        """
        def gen():
            while True:
                yield supplier()
        return SequentialStream(gen())

    @staticmethod
    def iterate(seed: T, unary_op: Callable[[T], T]) -> "SequentialStream[T]":
        """
        Creates an infinite stream: seed, f(seed), f(f(seed))...
        """
        def gen():
            state = seed
            while True:
                yield state
                state = unary_op(state)
        return SequentialStream(gen())
    
    # --- Transformations ---

    def map(self, mapper: Callable[[T], R]) -> "SequentialStream[R]":
        return SequentialStream(ops.map_gen(self._iterator, mapper), size_hint=self._size_hint)
    
    def filter(self, predicate: Callable[[T], bool]) -> "SequentialStream[T]":
        return SequentialStream(ops.filter_gen(self._iterator, predicate), size_hint=None)

    def flat_map(self, mapper: Callable[[T], Iterable[R]]) -> "SequentialStream[R]":
        return SequentialStream(ops.flat_map_gen(self._iterator, mapper), size_hint=None)
    
    def pick(self, key: Any) -> "SequentialStream[Any]":
        iterator = cast(Iterator[Any], self._iterator)
        return SequentialStream(ops.pick_gen(iterator, key))

    def filter_none(self, key: Any = None) -> "SequentialStream[T]":
        return SequentialStream(ops.filter_none_gen(self._iterator, key))

    def peek(self, action: Callable[[T], None]) -> "SequentialStream[T]":
        return SequentialStream(ops.peek_gen(self._iterator, action), size_hint=self._size_hint)

    def distinct(self) -> "SequentialStream[T]":
        if isinstance(self._iterable, (list, tuple, range)) and rust_ops.rust_available():
            distinct_values = rust_ops.distinct_list(self._iterable)
            return SequentialStream(distinct_values, size_hint=len(distinct_values))
        return SequentialStream(ops.distinct_gen(self._iterator))
    
    def distinct_by(self, key: Callable[[T], Any]) -> "SequentialStream[T]":
        return SequentialStream(ops.distinct_by_gen(self._iterator, key))

    def sorted(self, key: Callable[[T], Any] | None = None, reverse: bool = False) -> "SequentialStream[T]":
        if key is None and isinstance(self._iterable, (list, tuple, range)) and rust_ops.rust_available():
            sorted_values = rust_ops.sorted_list(self._iterable, reverse=reverse)
            return SequentialStream(sorted_values, size_hint=len(sorted_values))
        return SequentialStream(
            ops.sorted_gen(self._iterator, key, reverse), 
            size_hint=self._size_hint
        )

    def limit(self, max_size: int) -> "SequentialStream[T]":
        """
        Optimized limit. Uses slicing if source is a list/tuple/range.
        """
        if isinstance(self._iterable, (list, tuple, range)):
            if rust_ops.rust_available():
                limited = rust_ops.limit_list(self._iterable, max_size)
                return SequentialStream(limited, size_hint=len(limited))
            sliced_iterable = cast(Sequence[T], self._iterable[:max_size])
            return SequentialStream(sliced_iterable, size_hint=len(sliced_iterable))

        new_size = max_size
        if self._size_hint is not None:
            new_size = min(max_size, self._size_hint)
            
        return SequentialStream(ops.limit_gen(self._iterator, max_size), size_hint=new_size)

    def skip(self, n: int) -> "SequentialStream[T]":
        """
        Optimized skip. Uses slicing if source is a list/tuple/range.
        """
        if isinstance(self._iterable, (list, tuple, range)):
            if rust_ops.rust_available():
                skipped = rust_ops.skip_list(self._iterable, n)
                return SequentialStream(skipped, size_hint=len(skipped))
            sliced_iterable = cast(Sequence[T], self._iterable[n:])
            return SequentialStream(sliced_iterable, size_hint=len(sliced_iterable))

        new_size = None
        if self._size_hint is not None:
            new_size = max(0, self._size_hint - n)

        return SequentialStream(ops.skip_gen(self._iterator, n), size_hint=new_size)

    def take_while(self, predicate: Callable[[T], bool]) -> "SequentialStream[T]":
        return SequentialStream(ops.take_while_gen(self._iterator, predicate))

    def drop_while(self, predicate: Callable[[T], bool]) -> "SequentialStream[T]":
        return SequentialStream(ops.drop_while_gen(self._iterator, predicate))
    
    def zip(self, other: Iterable[R]) -> "SequentialStream[Tuple[T, R]]":
        return SequentialStream(ops.zip_gen(self._iterator, other))

    def zip_with_index(self, start: int = 0) -> "SequentialStream[Tuple[int, T]]":
        return SequentialStream(ops.zip_with_index_gen(self._iterator, start))
    
    def batch(self, size: int) -> "SequentialStream[List[T]]":
        """Chunks the stream into lists of size N."""
        if isinstance(self._iterable, (list, tuple, range)) and rust_ops.rust_available():
            batched = rust_ops.batch_list(self._iterable, size)
            return SequentialStream(batched, size_hint=len(batched))
        return SequentialStream(ops.batch_gen(self._iterator, size))

    def window(self, size: int, step: int = 1) -> "SequentialStream[List[T]]":
        """Creates a sliding window over the stream."""
        if isinstance(self._iterable, (list, tuple, range)) and rust_ops.rust_available():
            windowed = rust_ops.window_list(self._iterable, size, step)
            return SequentialStream(windowed, size_hint=len(windowed))
        return SequentialStream(ops.window_gen(self._iterator, size, step))

    def scan(self, identity: T, accumulator: Callable[[T, T], T]) -> "SequentialStream[T]":
        """
        Performs a cumulative reduction. 
        """
        new_size = self._size_hint + 1 if self._size_hint is not None else None
        return SequentialStream(
            ops.scan_gen(self._iterator, accumulator, identity), 
            size_hint=new_size
        )

    def zip_longest(self, other: Iterable[R], fillvalue: Any = None) -> "SequentialStream[Tuple[T, R]]":
        """
        Zips with another iterable, filling missing values instead of stopping.
        """
        return SequentialStream(ops.zip_longest_gen(self._iterator, other, fillvalue))

    def parallel(self, processes: int | None = None) -> "BaseStream[T]":
        from .parallel import ParallelStream
        return ParallelStream(self.to_list(), processes=processes)

    def to_async(self) -> "AsyncStream[T]":
        """
        Converts this synchronous stream into an AsyncStream.
        Useful for switching from processing in memory to sending data over network.
        """
        from .async_stream import AsyncStream 
        return AsyncStream.from_iterable(self)
    
    def window_by_time(self, time_extractor: Callable[[T], float], seconds: float) -> "SequentialStream[List[T]]":
        return SequentialStream(ops.window_time_gen(self._iterator, time_extractor, seconds))

    def flat_map_result(self) -> "SequentialStream[Any]":
        return SequentialStream(ops.unwrap_results_gen(self._iterator))

    def partition_results(self) -> Tuple[List[Any], List[Exception]]:
        successes = []
        failures = []
        
        for item in self._iterator:
            if isinstance(item, Result):
                if item.is_success():
                    successes.append(item.get_or_throw())
                else:
                    failures.append(item.error)
            else:
                successes.append(item)

        return successes, failures
    
    # --- Terminals ---

    def to_list(self) -> List[T]:
        return list(self._iterator)

    def to_set(self) -> Set[T]:
        return set(self._iterator)
    
    def for_each(self, action: Callable[[T], None]) -> None:
        for item in self._iterator:
            action(item)

    def count(self) -> int:
        if self._size_hint is not None:
            return self._size_hint
           
        # Fallback to O(N) iteration
        return sum(1 for _ in self._iterator)

    def find_first(self) -> Option[T]:
        try:
            return Option.of_nullable(next(self._iterator))
        except StopIteration:
            return Option.empty()

    def any_match(self, predicate: Callable[[T], bool]) -> bool:
        return ops.any_match_op(self._iterator, predicate)

    def all_match(self, predicate: Callable[[T], bool]) -> bool:
        return ops.all_match_op(self._iterator, predicate)

    def none_match(self, predicate: Callable[[T], bool]) -> bool:
        return ops.none_match_op(self._iterator, predicate)

    def collect(self, collector: Callable[[Iterable[T]], R]) -> R:
        return collector(self._iterator)

    def reduce(self, identity: R, accumulator: Callable[[R, T], R]) -> R:
        try:
            return functools.reduce(accumulator, self._iterator, identity)
        except TypeError: 
            # reduce() of empty sequence with no initial value
            raise StreamEmptyError("Cannot reduce an empty stream without an identity value.")
        
    def min(self, key: Callable[[T], Any] | None = None) -> Option[T]:
        if key is None and isinstance(self._iterable, (list, tuple, range)) and rust_ops.rust_available():
            result = rust_ops.min_list(self._iterable)
            return Option.of_nullable(result)
        result = ops.min_op(self._iterator, key)
        return Option.of_nullable(result)

    def max(self, key: Callable[[T], Any] | None = None) -> Option[T]:
        if key is None and isinstance(self._iterable, (list, tuple, range)) and rust_ops.rust_available():
            result = rust_ops.max_list(self._iterable)
            return Option.of_nullable(result)
        result = ops.max_op(self._iterator, key)
        return Option.of_nullable(result)

    def sum(self) -> Any:
        if isinstance(self._iterable, (list, tuple, range)) and rust_ops.rust_available():
            return rust_ops.sum_list(self._iterable)
        return ops.sum_op(self._iterator)
    
    def join(self, delimiter: str = "") -> str:
        return delimiter.join(map(str, self._iterator))

    def to_df(self, columns: List[str] | None = None) -> Any:
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas is required for to_df(). Install via `pip install pandas`.")
        return pd.DataFrame(self.to_list(), columns=columns)

    def to_np(self) -> Any:
        try:
            import numpy as np
        except ImportError:
            raise ImportError("NumPy is required for to_np(). Install via `pip install numpy`.")
        return np.array(self.to_list())

    def to_csv(self, filepath: str, header: List[str] | None = None) -> None:
        with open(filepath, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if header:
                writer.writerow(header)
            
            for item in self._iterator:
                if isinstance(item, dict):
                    if header:
                        writer.writerow([item.get(col) for col in header])
                    else:
                        writer.writerow(item.values())
                elif isinstance(item, (list, tuple)):
                    writer.writerow(item)
                else:
                    writer.writerow([item])

    def to_json(self, filepath: str) -> None:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.to_list(), f, ensure_ascii=False, indent=4)

    def describe(self) -> Dict[str, int | float]:
        data = self.to_list()
        if not data: 
            return {}
        
        count = len(data)
        result: Dict[str, int | float] = {"count": count}
        
        first_val = next((x for x in data if x is not None), None)
        is_numeric = isinstance(first_val, numbers.Number)

        if is_numeric:
            numeric_data = [x for x in data if isinstance(x, numbers.Number)]
            if numeric_data:
                summable_data = cast(List[int | float], numeric_data)
                result["sum"] = sum(summable_data)
                result["min"] = min(summable_data)
                result["max"] = max(summable_data)
                result["mean"] = statistics.mean(summable_data)
                if len(summable_data) > 1:
                    result["std"] = statistics.stdev(summable_data)
        
        return result
