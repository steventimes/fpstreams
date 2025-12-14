from typing import Iterator, Iterable, Callable, Optional, Any, List, Set, Union, Tuple
from ..option import Option
from .stream_interface import BaseStream
from . import ops
from .common import T, R

class SequentialStream(BaseStream[T]):
    def __init__(self, iterable: Iterable[T]):
        self._iterator: Iterator[T] = iter(iterable)

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