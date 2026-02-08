import itertools
from typing import Iterator, Callable, Iterable, Any, cast, Tuple, List, Deque
from collections import deque
from .common import T, R, SupportsRichComparison

# --- Generators  ---

def map_gen(iterator: Iterator[T], mapper: Callable[[T], R]) -> Iterator[R]:
    return map(mapper, iterator)

def filter_gen(iterator: Iterator[T], predicate: Callable[[T], bool]) -> Iterator[T]:
    return filter(predicate, iterator)

def flat_map_gen(iterator: Iterator[T], mapper: Callable[[T], Iterable[R]]) -> Iterator[R]:
    mapped_iterators = map(mapper, iterator)
    return itertools.chain.from_iterable(mapped_iterators)

def pick_gen(iterator: Iterator[Any], key: Any) -> Iterator[Any]:
    for item in iterator:
        try:
            if isinstance(item, dict):
                yield item.get(key)
            elif isinstance(item, (list, tuple)) and isinstance(key, int):
                yield item[key] if 0 <= key < len(item) else None
            elif hasattr(item, str(key)):
                yield getattr(item, str(key))
            else:
                yield None
        except (IndexError, AttributeError, TypeError):
            yield None

def filter_none_gen(iterator: Iterator[T], key: Any = None) -> Iterator[T]:
    if key is None:
        for item in iterator:
            if item is not None:
                yield item
    else:
        for item in iterator:
            val = None
            if isinstance(item, dict):
                val = item.get(key)
            elif hasattr(item, str(key)):
                val = getattr(item, str(key))
            
            if val is not None:
                yield item

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

def distinct_by_gen(iterator: Iterator[T], key: Callable[[T], Any]) -> Iterator[T]:
    seen = set()
    for item in iterator:
        marker = key(item)
        if marker not in seen:
            seen.add(marker)
            yield item
            
def sorted_gen(iterator: Iterator[T], key: Callable[[T], Any] | None, reverse: bool) -> Iterator[T]:
    if key is None:
        sortable_iter = cast(Iterator[SupportsRichComparison], iterator)
        sorted_list = sorted(sortable_iter, reverse=reverse)
        return cast(Iterator[T], iter(sorted_list))
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

def zip_with_index_gen(iterator: Iterator[T], start: int) -> Iterator[Tuple[int, T]]:
    return enumerate(iterator, start)

def zip_longest_gen(iterator: Iterator[T], other: Iterable[R], fillvalue: Any) -> Iterator[Tuple[T, R]]:
    return itertools.zip_longest(iterator, other, fillvalue=fillvalue)

def batch_gen(iterator: Iterator[T], size: int) -> Iterator[List[T]]:
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch

def window_gen(iterator: Iterator[T], size: int, step: int) -> Iterator[List[T]]:
    """
    Sliding window generator.
    Example: window([1,2,3,4], size=3, step=1) -> [1,2,3], [2,3,4]
    """
    window: Deque[T] = deque()
    
    # Fill first window
    for _ in range(size):
        try:
            window.append(next(iterator))
        except StopIteration:
            break
            
    if len(window) == size:
        yield list(window)
        
    # Slide
    steps_taken = 0
    for item in iterator:
        window.append(item)
        window.popleft()
        
        steps_taken += 1
        if steps_taken >= step:
            yield list(window)
            steps_taken = 0

def scan_gen(iterator: Iterator[T], func: Callable[[Any, T], Any], identity: Any) -> Iterator[Any]:
    """
    Like reduce, but yields intermediate results.
    """
    accumulator = identity
    yield accumulator
    for item in iterator:
        accumulator = func(accumulator, item)
        yield accumulator

# --- Terminal Operations ---

def any_match_op(iterator: Iterator[T], predicate: Callable[[T], bool]) -> bool:
    return any(predicate(x) for x in iterator)

def all_match_op(iterator: Iterator[T], predicate: Callable[[T], bool]) -> bool:
    return all(predicate(x) for x in iterator)

def none_match_op(iterator: Iterator[T], predicate: Callable[[T], bool]) -> bool:
    return not any(predicate(x) for x in iterator)

def min_op(iterator: Iterator[T], key: Callable[[T], Any] | None) -> Any | None:
    try:
        return min(iterator, key=key) if key else min(iterator) # type: ignore
    except ValueError:
        return None

def max_op(iterator: Iterator[T], key: Callable[[T], Any] | None) -> Any | None:
    try:
        return max(iterator, key=key) if key else max(iterator) # type: ignore
    except ValueError:
        return None

def sum_op(iterator: Iterator[T]) -> Any:
    return sum(iterator) # type: ignore

def window_time_gen(iterator: Iterator[T], time_extractor: Callable[[T], float], window_sec: float) -> Iterator[List[T]]:
    """
    Groups elements based on time windows.
    Assumes the stream is roughly ordered by time.
    """
    try:
        first = next(iterator)
    except StopIteration:
        return

    current_batch = [first]
    window_start = time_extractor(first)

    for item in iterator:
        item_time = time_extractor(item)
        if item_time < window_start + window_sec:
            current_batch.append(item)
        else:
            yield current_batch
            current_batch = [item]
            window_start = item_time 
    
    if current_batch:
        yield current_batch

def unwrap_results_gen(iterator: Iterator[Any]) -> Iterator[Any]:
    """
    Flattens a stream of Result[T] objects. 
    Yields T if Success, ignores Failure.
    """
    for item in iterator:
        if hasattr(item, 'is_success') and item.is_success():
            yield item.get_or_throw()