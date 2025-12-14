import multiprocessing
import statistics
import functools
import numbers
from typing import (
    Iterable, Callable, Optional, List, Any, Union, Tuple, Set, cast, Dict, Iterator
)
from ..option import Option
from .stream_interface import BaseStream
from . import ops
from .common import T, R
from .sequential import SequentialStream

def _sum_wrapper(it: Iterable[Any]) -> Any:
    return ops.sum_op(iter(it))

def _count_wrapper(it: Iterable[Any]) -> int:
    return sum(1 for _ in it)

class _MinMaxHelper:
    def __init__(self, mode: str, key: Optional[Callable[[Any], Any]] = None):
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

def _worker_process(payload: Tuple[List[Any], List[Tuple[str, Any]], Optional[Callable]]) -> Any:
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
            # arg is the key (or None)
            iterator = ops.filter_none_gen(iterator, arg)
        elif op_type == "peek":
            iterator = ops.peek_gen(iterator, arg)
        elif op_type == "distinct":
            iterator = ops.distinct_gen(iterator)
        elif op_type == "limit":
            iterator = ops.limit_gen(iterator, arg)
        elif op_type == "skip":
            iterator = ops.skip_gen(iterator, arg)

    if reducer_func:
        return reducer_func(iterator)
    
    return list(iterator)

class ParallelStream(BaseStream[T]):

    def __init__(self, iterable: Iterable[T], processes: Optional[int] = None):
        self._iterable = iterable
        self._pipeline: List[Tuple[str, Any]] = []
        self._processes = processes or multiprocessing.cpu_count()
        
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

    # --- Sequential Fallbacks ---

    def sorted(self, key: Optional[Callable[[T], Any]] = None, reverse: bool = False) -> "BaseStream[T]":
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

    # --- Terminals ---

    def _execute(self, reducer: Optional[Callable[[Iterable[Any]], Any]] = None) -> List[Any]:
        data = list(self._iterable) if not isinstance(self._iterable, list) else self._iterable
        if not data:
            return []

        chunk_size = max(1, len(data) // self._processes)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
        payloads = [(chunk, self._pipeline, reducer) for chunk in chunks]

        with multiprocessing.Pool(self._processes) as pool:
            results = pool.map(_worker_process, payloads)

        return results

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
    
    def min(self, key: Optional[Callable[[T], Any]] = None) -> Option[T]:
        local_mins = self._execute(reducer=_MinMaxHelper("min", key))
        valid_mins = [x for x in local_mins if x is not None]
        if not valid_mins:
            return Option.empty()
        return Option.of_nullable(ops.min_op(iter(valid_mins), key))

    def max(self, key: Optional[Callable[[T], Any]] = None) -> Option[T]:
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

    def to_df(self, columns: Optional[List[str]] = None) -> Any:
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

    def to_csv(self, filepath: str, header: Optional[List[str]] = None) -> None:
        self._fallback().to_csv(filepath, header)

    def to_json(self, filepath: str) -> None:
        self._fallback().to_json(filepath)

    def describe(self) -> Dict[str, Union[int, float]]:
        data = self.to_list()
        if not data: 
            return {}
        
        count = len(data)
        result: Dict[str, Union[int, float]] = {"count": count}

        first_val = next((x for x in data if x is not None), None)
        is_numeric = isinstance(first_val, numbers.Number)

        if is_numeric:
            numeric_data = [x for x in data if isinstance(x, numbers.Number)]
            if numeric_data:
                summable_data = cast(List[Union[int, float]], numeric_data)
                result["sum"] = sum(summable_data)
                result["min"] = min(summable_data)
                result["max"] = max(summable_data)
                result["mean"] = statistics.mean(summable_data)
                if len(summable_data) > 1:
                    result["std"] = statistics.stdev(summable_data)
        
        return result

    def parallel(self, processes: Optional[int] = None) -> "BaseStream[T]":
        if processes:
            self._processes = processes
        return self