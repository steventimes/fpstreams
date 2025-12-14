import multiprocessing
from typing import (
    Iterable, Callable, Optional, List, Any, Union, Tuple, TypeVar, Generic
)
from ..option import Option
from .stream_interface import BaseStream
from . import ops
from .common import T, R


def _sum_wrapper(it):
    return ops.sum_op(it)

def _count_wrapper(it):
    return sum(1 for _ in it)

class _MinMaxHelper:
    """Helper to carry state (key) for min/max operations across processes."""
    def __init__(self, mode: str, key: Optional[Callable] = None):
        self.mode = mode
        self.key = key
    
    def __call__(self, iterator):
        if self.mode == "min":
            return ops.min_op(iterator, self.key)
        return ops.max_op(iterator, self.key)


def _worker_process(payload: Tuple[List[Any], List[Tuple[str, Callable]], Optional[Callable]]) -> Any:
    chunk, operations, reducer_func = payload
    
    iterator = iter(chunk)
    for op_type, func in operations:
        if op_type == "map":
            iterator = ops.map_gen(iterator, func)
        elif op_type == "filter":
            iterator = ops.filter_gen(iterator, func)
        elif op_type == "flat_map":
            iterator = ops.flat_map_gen(iterator, func)
        elif op_type == "peek":
            iterator = ops.peek_gen(iterator, func)

    if reducer_func:
        return reducer_func(iterator)
    
    return list(iterator)


class ParallelStream(BaseStream[T]):
    def __init__(self, iterable: Iterable[T], processes: Optional[int] = None):
        self._iterable = iterable
        self._processes = processes or multiprocessing.cpu_count()
        self._pipeline: List[Tuple[str, Callable]] = []


    def map(self, mapper: Callable[[T], R]) -> "ParallelStream[R]":
        self._pipeline.append(("map", mapper))
        return self  # type: ignore

    def filter(self, predicate: Callable[[T], bool]) -> "ParallelStream[T]":
        self._pipeline.append(("filter", predicate))
        return self
    
    def flat_map(self, mapper: Callable[[T], Iterable[R]]) -> "ParallelStream[R]":
        self._pipeline.append(("flat_map", mapper))
        return self  # type: ignore

    def peek(self, action: Callable[[T], None]) -> "ParallelStream[T]":
        self._pipeline.append(("peek", action))
        return self

    def sorted(self, key=None, reverse=False):
        results = self.to_list()
        from .sequential import SequentialStream
        return SequentialStream(results).sorted(key, reverse)

    def distinct(self):
        results = self.to_list()
        from .sequential import SequentialStream
        return SequentialStream(results).distinct()

    def limit(self, max_size):
        results = self.to_list()
        from .sequential import SequentialStream
        return SequentialStream(results).limit(max_size)

    def skip(self, n):
        results = self.to_list()
        from .sequential import SequentialStream
        return SequentialStream(results).skip(n)


    def _get_chunks(self) -> List[List[T]]:
        data = list(self._iterable)
        if not data: return []
        chunk_size = max(1, len(data) // self._processes)
        return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

    def _execute_map_reduce(self, local_reduce: Callable, global_reduce: Callable) -> Any:
        # logic for auto fallback to sequential if data is small. Might implement in future version.
        # data = list(self._iterable)
        # if len(data) < 1000:
        #      from .sequential import SequentialStream
        #      pass
        chunks = self._get_chunks()
        if not chunks: return None

        payloads = [(chunk, self._pipeline, local_reduce) for chunk in chunks]

        with multiprocessing.Pool(processes=self._processes) as pool:
            chunk_results = pool.map(_worker_process, payloads)

        valid_results = [r for r in chunk_results if r is not None]
        if not valid_results:
            return None
            
        return global_reduce(valid_results)

    def to_list(self) -> List[T]:
        chunks = self._get_chunks()
        if not chunks: return []

        payloads = [(chunk, self._pipeline, None) for chunk in chunks]

        with multiprocessing.Pool(processes=self._processes) as pool:
            result_chunks = pool.map(_worker_process, payloads)

        flat_results = []
        for r in result_chunks:
            flat_results.extend(r)
        return flat_results


    def reduce(self, accumulator: Callable[[T, T], T], identity: Union[T, None] = None) -> Union[T, None]:
        processed_list = self.to_list()
        return ops.reduce_op(iter(processed_list), accumulator, identity)

    def min(self, key: Optional[Callable[[T], Any]] = None) -> "Option[T]":
        helper = _MinMaxHelper("min", key)
        val = self._execute_map_reduce(local_reduce=helper, global_reduce=helper)
        return Option.of_nullable(val)

    def max(self, key: Optional[Callable[[T], Any]] = None) -> "Option[T]":
        helper = _MinMaxHelper("max", key)
        val = self._execute_map_reduce(local_reduce=helper, global_reduce=helper)
        return Option.of_nullable(val)

    def sum(self) -> Any:
        val = self._execute_map_reduce(local_reduce=_sum_wrapper, global_reduce=_sum_wrapper)
        return val or 0

    def count(self) -> int:
        val = self._execute_map_reduce(local_reduce=_count_wrapper, global_reduce=sum)
        return val or 0
    
    def for_each(self, action: Callable[[T], None]) -> None:
        self.peek(action).to_list()

    def find_first(self) -> "Option[T]":
        res = self.to_list()
        val = res[0] if res else None
        return Option.of_nullable(val)
    
    def take_while(self, predicate): return self._fallback().take_while(predicate)
    def drop_while(self, predicate): return self._fallback().drop_while(predicate)
    def zip(self, other): return self._fallback().zip(other)
    def zip_with_index(self): return self._fallback().zip_with_index()
    def collect(self, collector): return collector(self.to_list())
    def any_match(self, predicate): return any(predicate(x) for x in self.to_list())
    def all_match(self, predicate): return all(predicate(x) for x in self.to_list())
    def none_match(self, predicate): return not self.any_match(predicate)
    def to_set(self): return set(self.to_list())

    def parallel(self, processes: Optional[int] = None) -> "BaseStream[T]":
        if processes: self._processes = processes
        return self

    def pluck(self, key: Any) -> "ParallelStream[Any]":
        """
        Extracts a value by key. (Parallelized as a Map operation)
        """
        # We wrap the key access in a function so it can be pickled
        def _pluck_wrapper(item):
            return item[key]
        
        self._pipeline.append(("map", _pluck_wrapper))
        return self # type: ignore

    def drop_none(self, key: Any = None) -> "ParallelStream[T]":
        """
        Drops None values. (Parallelized as a Filter operation)
        """
        if key is not None:
            def _filter_key(item):
                return item.get(key) is not None
            self._pipeline.append(("filter", _filter_key))
        else:
            def _filter_none(item):
                return item is not None
            self._pipeline.append(("filter", _filter_none))
        return self

    # --- Terminals (Materialize -> Convert) ---

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

    def join(self, delimiter: str = "") -> str:
        return delimiter.join(map(str, self.to_list()))

    def to_csv(self, filepath: str, header: Optional[List[str]] = None) -> None:
        # Fallback to sequential write
        self._fallback().to_csv(filepath, header)

    def to_json(self, filepath: str) -> None:
        self._fallback().to_json(filepath)

    def describe(self) -> dict:
        return self._fallback().describe()
    
    def _fallback(self):
        results = self.to_list()
        from .sequential import SequentialStream
        return SequentialStream(results)