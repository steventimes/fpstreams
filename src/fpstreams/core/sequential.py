from typing import Iterator, Iterable, Callable, Optional, Any, List, Set, Union, Tuple, cast, Dict
from ..option import Option
from .stream_interface import BaseStream
from . import ops
from .common import T, R
import json
import csv
import statistics
import itertools
import functools
import numbers

class SequentialStream(BaseStream[T]):
    
    def __init__(self, iterable: Iterable[T]):
        self._iterator: Iterator[T] = iter(iterable)
        
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
    
    # --- Transformations ---

    def map(self, mapper: Callable[[T], R]) -> "SequentialStream[R]":
        return SequentialStream(ops.map_gen(self._iterator, mapper))

    def filter(self, predicate: Callable[[T], bool]) -> "SequentialStream[T]":
        return SequentialStream(ops.filter_gen(self._iterator, predicate))

    def flat_map(self, mapper: Callable[[T], Iterable[R]]) -> "SequentialStream[R]":
        return SequentialStream(ops.flat_map_gen(self._iterator, mapper))

    def pick(self, key: Any) -> "SequentialStream[Any]":
        iterator = cast(Iterator[Any], self._iterator)
        return SequentialStream(ops.pick_gen(iterator, key))

    def filter_none(self, key: Any = None) -> "SequentialStream[T]":
        return SequentialStream(ops.filter_none_gen(self._iterator, key))

    def peek(self, action: Callable[[T], None]) -> "SequentialStream[T]":
        return SequentialStream(ops.peek_gen(self._iterator, action))

    def distinct(self) -> "SequentialStream[T]":
        return SequentialStream(ops.distinct_gen(self._iterator))

    def sorted(self, key: Optional[Callable[[T], Any]] = None, reverse: bool = False) -> "SequentialStream[T]":
        return SequentialStream(ops.sorted_gen(self._iterator, key, reverse))

    def limit(self, max_size: int) -> "SequentialStream[T]":
        return SequentialStream(ops.limit_gen(self._iterator, max_size))

    def skip(self, n: int) -> "SequentialStream[T]":
        return SequentialStream(ops.skip_gen(self._iterator, n))

    def take_while(self, predicate: Callable[[T], bool]) -> "SequentialStream[T]":
        return SequentialStream(ops.take_while_gen(self._iterator, predicate))

    def drop_while(self, predicate: Callable[[T], bool]) -> "SequentialStream[T]":
        return SequentialStream(ops.drop_while_gen(self._iterator, predicate))
    
    def zip(self, other: Iterable[R]) -> "SequentialStream[Tuple[T, R]]":
        return SequentialStream(ops.zip_gen(self._iterator, other))

    def zip_with_index(self, start: int = 0) -> "SequentialStream[Tuple[int, T]]":
        return SequentialStream(ops.zip_with_index_gen(self._iterator, start))

    # --- Terminals ---

    def to_list(self) -> List[T]:
        return list(self._iterator)

    def to_set(self) -> Set[T]:
        return set(self._iterator)
    
    def for_each(self, action: Callable[[T], None]) -> None:
        for item in self._iterator:
            action(item)

    def count(self) -> int:
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
        return functools.reduce(accumulator, self._iterator, identity)
    
    def min(self, key: Optional[Callable[[T], Any]] = None) -> Option[T]:
        result = ops.min_op(self._iterator, key)
        return Option.of_nullable(result)

    def max(self, key: Optional[Callable[[T], Any]] = None) -> Option[T]:
        result = ops.max_op(self._iterator, key)
        return Option.of_nullable(result)

    def sum(self) -> Any:
        return ops.sum_op(self._iterator)
    
    def join(self, delimiter: str = "") -> str:
        return delimiter.join(map(str, self._iterator))

    def to_df(self, columns: Optional[List[str]] = None) -> Any:
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

    def to_csv(self, filepath: str, header: Optional[List[str]] = None) -> None:
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
        from .parallel import ParallelStream
        return ParallelStream(self.to_list(), processes=processes)