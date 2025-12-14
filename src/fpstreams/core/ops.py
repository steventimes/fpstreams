import itertools
import functools
from typing import Iterator, Callable, Iterable, Optional, Any, cast, Tuple, Union
from .common import T, R, SupportsRichComparison


def map_gen(iterator: Iterator[T], mapper: Callable[[T], R]) -> Iterator[R]:
    return map(mapper, iterator)

def filter_gen(iterator: Iterator[T], predicate: Callable[[T], bool]) -> Iterator[T]:
    return filter(predicate, iterator)

def flat_map_gen(iterator: Iterator[T], mapper: Callable[[T], Iterable[R]]) -> Iterator[R]:
    mapped_iterators = map(mapper, iterator)
    return itertools.chain.from_iterable(mapped_iterators)

def peek_gen(iterator: Iterator[T], action: Callable[[T], None]) -> Iterator[T]:
    for item in iterator:
        action(item)
        yield item

def distinct_gen(iterator: Iterator[T]) -> Iterator[T]:
    seen = set()
    for item in iterator:
        if item not in seen:
            seen.add(item)
            yield item

def sorted_gen(iterator: Iterator[T], key: Optional[Callable[[T], Any]], reverse: bool) -> Iterator[T]:
    # sorted() returns a list, so turn it back into an iterator
    if key is None:
        sortable_iter = cast(Iterator[SupportsRichComparison], iterator)
        return iter(sorted(sortable_iter, reverse=reverse))  # type: ignore
    return iter(sorted(iterator, key=key, reverse=reverse))

def limit_gen(iterator: Iterator[T], max_size: int) -> Iterator[T]:
    return itertools.islice(iterator, max_size)

def skip_gen(iterator: Iterator[T], n: int) -> Iterator[T]:
    return itertools.islice(iterator, n, None)

def take_while_gen(iterator: Iterator[T], predicate: Callable[[T], bool]) -> Iterator[T]:
    return itertools.takewhile(predicate, iterator)

def drop_while_gen(iterator: Iterator[T], predicate: Callable[[T], bool]) -> Iterator[T]:
    return itertools.dropwhile(predicate, iterator)

def zip_gen(iterator: Iterator[T], other: Iterable[R]) -> Iterator[Tuple[T, R]]:
    return zip(iterator, other)

def zip_with_index_gen(iterator: Iterator[T]) -> Iterator[Tuple[int, T]]:
    return enumerate(iterator)

def reduce_op(iterator: Iterator[T], accumulator: Callable[[T, T], T], identity: Union[T, None]) -> Union[T, None]:
    if identity is not None:
        return functools.reduce(accumulator, iterator, identity)
    try:
        return functools.reduce(accumulator, iterator)
    except TypeError:
        return None

def count_op(iterator: Iterator[T]) -> int:
    return sum(1 for _ in iterator)

def find_first_op(iterator: Iterator[T]) -> Optional[T]:
    return next(iterator, None)

def any_match_op(iterator: Iterator[T], predicate: Callable[[T], bool]) -> bool:
    return any(predicate(item) for item in iterator)

def all_match_op(iterator: Iterator[T], predicate: Callable[[T], bool]) -> bool:
    return all(predicate(item) for item in iterator)

def none_match_op(iterator: Iterator[T], predicate: Callable[[T], bool]) -> bool:
    return not any_match_op(iterator, predicate)

def min_op(iterator: Iterator[T], key: Optional[Callable[[T], Any]] = None) -> Optional[T]:
    try:
        return min(iterator, key=key) if key else min(iterator) # type: ignore
    except ValueError:
        return None
    
def max_op(iterator: Iterator[T], key: Optional[Callable[[T], Any]] = None) -> Optional[T]:
    try:
        return max(iterator, key=key) if key else max(iterator) # type: ignore
    except ValueError:
        return None

def sum_op(iterator: Iterator[T], start: Any = 0) -> Any:
    return sum(cast(Iterable[Any], iterator), start)