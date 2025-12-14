from typing import Iterator, Iterable, Callable, Optional, Any, List, Set, Union, Tuple, cast
from ..option import Option
from .stream_interface import BaseStream
from . import ops
from .common import T, R
import json
import csv
import statistics

class SequentialStream(BaseStream[T]):
    def __init__(self, iterable: Iterable[T]):
        self._iterator: Iterator[T] = iter(iterable)

    @staticmethod
    def concat(stream1: "BaseStream[T]", stream2: "BaseStream[T]") -> "SequentialStream[T]":
        """
        Concatenates two streams into one.
        """
        def iterator_yielder():
            yield from stream1.to_list() if isinstance(stream1, BaseStream) and not isinstance(stream1, SequentialStream) else stream1._iterator # type: ignore
            yield from stream2.to_list() if isinstance(stream2, BaseStream) and not isinstance(stream2, SequentialStream) else stream2._iterator # type: ignore

        return SequentialStream(iterator_yielder())
    
    def map(self, mapper: Callable[[T], R]) -> "SequentialStream[R]":
        return SequentialStream(ops.map_gen(self._iterator, mapper))

    def filter(self, predicate: Callable[[T], bool]) -> "SequentialStream[T]":
        return SequentialStream(ops.filter_gen(self._iterator, predicate))

    def flat_map(self, mapper: Callable[[T], Iterable[R]]) -> "SequentialStream[R]":
        return SequentialStream(ops.flat_map_gen(self._iterator, mapper))

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

    def zip_with_index(self) -> "SequentialStream[Tuple[int, T]]":
        return SequentialStream(ops.zip_with_index_gen(self._iterator))

    def min(self, key: Optional[Callable[[T], Any]] = None) -> "Option[T]":
        val = ops.min_op(self._iterator, key)
        return Option.of_nullable(val)

    def max(self, key: Optional[Callable[[T], Any]] = None) -> "Option[T]":
        val = ops.max_op(self._iterator, key)
        return Option.of_nullable(val)

    def sum(self) -> Any:
        return ops.sum_op(self._iterator)

    def for_each(self, action: Callable[[T], None]) -> None:
        for item in self._iterator:
            action(item)

    def to_list(self) -> List[T]:
        return list(self._iterator)

    def to_set(self) -> Set[T]:
        return set(self._iterator)

    def reduce(self, accumulator: Callable[[T, T], T], identity: Union[T, None] = None) -> Union[T, None]:
        return ops.reduce_op(self._iterator, accumulator, identity)

    def count(self) -> int:
        return ops.count_op(self._iterator)

    def find_first(self) -> "Option[T]":
        val = ops.find_first_op(self._iterator)
        return Option.of_nullable(val)

    def any_match(self, predicate: Callable[[T], bool]) -> bool:
        return ops.any_match_op(self._iterator, predicate)

    def all_match(self, predicate: Callable[[T], bool]) -> bool:
        return ops.all_match_op(self._iterator, predicate)

    def none_match(self, predicate: Callable[[T], bool]) -> bool:
        return ops.none_match_op(self._iterator, predicate)

    def collect(self, collector: Callable[[Iterable[T]], R]) -> R:
        return collector(self._iterator)

    def parallel(self, processes: Optional[int] = None) -> "BaseStream[T]":
        from .parallel import ParallelStream
        return ParallelStream(self._iterator, processes)
    
    def join(self, delimiter: str = "") -> str:
        return delimiter.join(map(str, self._iterator))

    def to_csv(self, filepath: str, header: Optional[List[str]] = None) -> None:
        with open(filepath, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if header:
                writer.writerow(header)
            for item in self._iterator:
                if isinstance(item, (list, tuple)):
                    writer.writerow(item)
                elif isinstance(item, dict) and header:
                    writer.writerow([str(item.get(col, "")) for col in header])
                else:
                    writer.writerow([str(item)])

    def to_json(self, filepath: str) -> None:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.to_list(), f, default=str)

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

    def pluck(self, key: Any) -> "SequentialStream[Any]":
        iterator = cast(Iterator[Any], self._iterator)
        return SequentialStream(ops.pluck_gen(iterator, key))

    def drop_none(self, key: Any = None) -> "SequentialStream[T]":
        if key is not None:
            iterator = cast(Iterator[Any], self._iterator)
            new_iter = ops.drop_none_key_gen(iterator, key)
            return SequentialStream(cast(Iterator[T], new_iter))
        return SequentialStream(ops.drop_none_gen(self._iterator))

    def describe(self) -> dict:
        data = self.to_list()
        if not data: return {}
        
        try:
            numeric_data = cast(List[float], data)
            
            return {
                "count": len(numeric_data),
                "sum": sum(numeric_data),
                "min": min(numeric_data),
                "max": max(numeric_data),
                "mean": statistics.mean(numeric_data)
            }
        except TypeError:
            return {"count": len(data), "info": "Non-numeric data"}