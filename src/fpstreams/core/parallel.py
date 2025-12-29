import multiprocessing
import statistics
import functools
import numbers
import os
from typing import (
    Iterable, Callable, List, Any, Tuple, Set, cast, Dict, Iterator, TYPE_CHECKING
)
from ..option import Option
from ..result import Result
from .stream_interface import BaseStream
from . import ops
from .common import T, R
from .sequential import SequentialStream

if TYPE_CHECKING:
    from .async_stream import AsyncStream

def _sum_wrapper(it: Iterable[Any]) -> Any:
    return ops.sum_op(iter(it))

def _count_wrapper(it: Iterable[Any]) -> int:
    return sum(1 for _ in it)

class _MinMaxHelper:
    def __init__(self, mode: str, key: Callable[[Any], Any] | None = None):
        self.mode = mode
        self.key = key
    
    def __call__(self, iterator: Iterable[Any]) -> Any:
        if self.mode == "min":
            return ops.min_op(iter(iterator), self.key)
        return ops.max_op(iter(iterator), self.key)

class _ReduceHelper:
    """Helper to perform local reduction in worker processes."""
    def __init__(self, identity: Any, accumulator: Callable[[Any, Any], Any]):
        self.identity = identity
        self.accumulator = accumulator

    def __call__(self, iterator: Iterable[Any]) -> Any:
        return functools.reduce(self.accumulator, iterator, self.identity)

def _worker_process(payload: Tuple[List[Any], List[Tuple[str, Any]], Callable | None]) -> Any:
    """
    Worker process that handles a chunk of data.
    Now supports 'batch' and 'window' operations locally.
    """
    chunk, operations, reducer_func = payload
    
    iterator = iter(chunk)
    
    for op_type, arg in operations:
        if op_type == "map":
            iterator = ops.map_gen(iterator, arg)
        elif op_type == "filter":
            iterator = ops.filter_gen(iterator, arg)
        elif op_type == "flat_map":
            iterator = ops.flat_map_gen(iterator, arg)
        elif op_type == "pick":
            iterator = ops.pick_gen(iterator, arg)
        elif op_type == "filter_none":
            iterator = ops.filter_none_gen(iterator, arg)
        elif op_type == "peek":
            iterator = ops.peek_gen(iterator, arg)
        elif op_type == "distinct":
            iterator = ops.distinct_gen(iterator)
        elif op_type == "limit":
            iterator = ops.limit_gen(iterator, arg)
        elif op_type == "skip":
            iterator = ops.skip_gen(iterator, arg)
        elif op_type == "batch":
            iterator = ops.batch_gen(iterator, arg)
        elif op_type == "window":
            size, step = arg
            iterator = ops.window_gen(iterator, size, step)

    if reducer_func:
        return reducer_func(iterator)
    
    return list(iterator)

class ParallelStream(BaseStream[T]):

    def __init__(self, iterable: Iterable[T], processes: int | None = None):
        self._iterable = iterable
        self._pipeline: List[Tuple[str, Any]] = []
        self._processes = processes or os.cpu_count() or 1
        
    def __iter__(self) -> Iterator[T]:
        """
        Materializes the parallel results and returns an iterator.
        Allows: for item in Stream(data).parallel(): ...
        """
        return iter(self.to_list())

    def _fallback(self) -> SequentialStream[T]:
        return SequentialStream(self.to_list())

    # --- Transformations ---

    def map(self, mapper: Callable[[T], R]) -> "ParallelStream[R]":
        self._pipeline.append(("map", mapper))
        return cast("ParallelStream[R]", self)

    def filter(self, predicate: Callable[[T], bool]) -> "ParallelStream[T]":
        self._pipeline.append(("filter", predicate))
        return self

    def flat_map(self, mapper: Callable[[T], Iterable[R]]) -> "ParallelStream[R]":
        self._pipeline.append(("flat_map", mapper))
        return cast("ParallelStream[R]", self)
    
    def pick(self, key: Any) -> "ParallelStream[Any]":
        self._pipeline.append(("pick", key))
        return cast("ParallelStream[Any]", self)

    def filter_none(self, key: Any = None) -> "ParallelStream[T]":
        self._pipeline.append(("filter_none", key))
        return self

    def peek(self, action: Callable[[T], None]) -> "ParallelStream[T]":
        self._pipeline.append(("peek", action))
        return self

    def distinct(self) -> "ParallelStream[T]":
        self._pipeline.append(("distinct", None))
        return self


    def batch(self, size: int) -> "ParallelStream[List[T]]":
        """
        Chunks the stream into lists of the given size inside the worker.
        """
        self._pipeline.append(("batch", size))
        return cast("ParallelStream[List[T]]", self)

    def window(self, size: int, step: int = 1) -> "ParallelStream[List[T]]":
        """
        Sliding window view. Note: In parallel, this only slides within the assigned chunk.
        """
        self._pipeline.append(("window", (size, step)))
        return cast("ParallelStream[List[T]]", self)

    def window_by_time(self, time_extractor: Callable[[T], float], seconds: float) -> "BaseStream[List[T]]":
        return self._fallback().window_by_time(time_extractor, seconds)

    def flat_map_result(self) -> "ParallelStream[Any]":
        def _unwrap(item: Any) -> Iterable[Any]:
            if hasattr(item, 'is_success') and item.is_success():
                return [item.get_or_throw()]
            return []
        
        return self.flat_map(_unwrap)

    def partition_results(self) -> Tuple[List[Any], List[Exception]]:
        all_items = self.to_list()
        successes = []
        failures = []
        
        for item in all_items:
             if isinstance(item, Result):
                if item.is_success():
                    successes.append(item.get_or_throw())
                else:
                    failures.append(item.error)
             else:
                 successes.append(item)
        return successes, failures
    
    # --- Sequential Fallbacks ---

    def sorted(self, key: Callable[[T], Any] | None = None, reverse: bool = False) -> "BaseStream[T]":
        return self._fallback().sorted(key, reverse)

    def limit(self, max_size: int) -> "BaseStream[T]":
        return self._fallback().limit(max_size)

    def skip(self, n: int) -> "BaseStream[T]":
        return self._fallback().skip(n)

    def take_while(self, predicate: Callable[[T], bool]) -> "BaseStream[T]":
        return self._fallback().take_while(predicate)

    def drop_while(self, predicate: Callable[[T], bool]) -> "BaseStream[T]":
        return self._fallback().drop_while(predicate)

    def zip(self, other: Iterable[R]) -> "BaseStream[Tuple[T, R]]":
        return self._fallback().zip(other)

    def zip_with_index(self, start: int = 0) -> "BaseStream[Tuple[int, T]]":
        return self._fallback().zip_with_index(start)
        
    def zip_longest(self, other: Iterable[R], fillvalue: Any = None) -> "BaseStream[Tuple[T, R]]":
        return self._fallback().zip_longest(other, fillvalue)

    def scan(self, identity: T, accumulator: Callable[[T, T], T]) -> "BaseStream[T]":
        return self._fallback().scan(identity, accumulator)

    # --- Terminals ---

    def _execute(self, reducer: Callable[[Iterable[Any]], Any] | None = None) -> List[Any]:
        """
        Optimized execution strategy using Lazy Chunking (imap) instead of Eager Slicing.
        This prevents MemoryError on large datasets.
        """
        has_len = hasattr(self._iterable, "__len__")
        
        chunk_size = 1000
        if has_len:
            total_len = len(self._iterable) # type: ignore
            if total_len == 0:
                return []
            chunk_size = max(1, total_len // self._processes)

        # --- Chunk Alignment Logic ---
        # If 'batch' is used, we must align chunk_size to be a multiple of batch_size
        # to prevent fragmented batches at chunk boundaries.
        # This is only possible if no filtering happens before the batch.
        
        batch_op_idx = -1
        unsafe_op_before_batch = False
        batch_size = 0
        
        for i, (op, arg) in enumerate(self._pipeline):
            if op == "batch":
                batch_op_idx = i
                batch_size = arg
                break
            if op in ("filter", "flat_map", "filter_none", "distinct"):
                unsafe_op_before_batch = True
        
        if batch_op_idx != -1 and not unsafe_op_before_batch and batch_size > 0:
            multiplier = max(1, chunk_size // batch_size)
            chunk_size = multiplier * batch_size

        chunk_generator = ops.batch_gen(iter(self._iterable), chunk_size)
        
        payloads = (
            (chunk, self._pipeline, reducer) 
            for chunk in chunk_generator
        )

        with multiprocessing.Pool(self._processes) as pool:
            results = pool.imap(_worker_process, payloads)
            return list(results)

    def to_list(self) -> List[T]:
        nested_results = self._execute(reducer=None) 
        flat_list = []
        for sublist in nested_results:
            flat_list.extend(sublist)
        
        if any(op[0] == "distinct" for op in self._pipeline):
            return list(set(flat_list))
            
        return flat_list

    def to_set(self) -> Set[T]:
        return set(self.to_list())
    
    def for_each(self, action: Callable[[T], None]) -> None:
        self._pipeline.append(("peek", action))
        self._execute(reducer=None)

    def count(self) -> int:
        counts = self._execute(reducer=_count_wrapper)
        return sum(counts)

    def find_first(self) -> Option[T]:
        res = self.to_list()
        if res:
            return Option.of(res[0])
        return Option.empty()

    def any_match(self, predicate: Callable[[T], bool]) -> bool:
        return self._fallback().any_match(predicate)

    def all_match(self, predicate: Callable[[T], bool]) -> bool:
        return self._fallback().all_match(predicate)

    def none_match(self, predicate: Callable[[T], bool]) -> bool:
        return self._fallback().none_match(predicate)

    def collect(self, collector: Callable[[Iterable[T]], R]) -> R:
        return collector(self.to_list())

    def reduce(self, identity: R, accumulator: Callable[[R, T], R]) -> R:
        # Map Phase: Reduce each chunk locally
        local_results = self._execute(reducer=_ReduceHelper(identity, accumulator))
        # Reduce Phase: Reduce the results from each worker
        return functools.reduce(accumulator, local_results, identity)
    
    def min(self, key: Callable[[T], Any] | None = None) -> Option[T]:
        local_mins = self._execute(reducer=_MinMaxHelper("min", key))
        valid_mins = [x for x in local_mins if x is not None]
        if not valid_mins:
            return Option.empty()
        return Option.of_nullable(ops.min_op(iter(valid_mins), key))

    def max(self, key: Callable[[T], Any] | None = None) -> Option[T]:
        local_maxs = self._execute(reducer=_MinMaxHelper("max", key))
        valid_maxs = [x for x in local_maxs if x is not None]
        if not valid_maxs:
            return Option.empty()
        return Option.of_nullable(ops.max_op(iter(valid_maxs), key))

    def sum(self) -> Any:
        local_sums = self._execute(reducer=_sum_wrapper)
        return sum(local_sums)
    
    def join(self, delimiter: str = "") -> str:
        return delimiter.join(map(str, self.to_list()))

    # --- I/O & Data Science ---

    def to_df(self, columns: List[str] | None = None) -> Any:
        results = self.to_list()
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas is required. `pip install pandas`")
        return pd.DataFrame(results, columns=columns)

    def to_np(self) -> Any:
        results = self.to_list()
        try:
            import numpy as np
        except ImportError:
            raise ImportError("NumPy is required. `pip install numpy`")
        return np.array(results)

    def to_csv(self, filepath: str, header: List[str] | None = None) -> None:
        self._fallback().to_csv(filepath, header)

    def to_json(self, filepath: str) -> None:
        self._fallback().to_json(filepath)

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

    def parallel(self, processes: int | None = None) -> "BaseStream[T]":
        if processes:
            self._processes = processes
        return self

    def to_async(self) -> "AsyncStream[T]":
        """
        Converts the parallel stream into an AsyncStream.
        Note: This materializes the parallel results first.
        """
        from .async_stream import AsyncStream
        return AsyncStream.from_iterable(self.to_list())