import functools
import itertools
from typing import (
    TypeVar,
    Generic,
    Iterable,
    Callable,
    List,
    Set,
    Optional,
    Any,
    Iterator,
    Union,
    cast,
    Protocol
)
from .option import Option

# type of the elements currently in the stream
T = TypeVar("T")
# return type of a transformation
R = TypeVar("R")

# define sortable
class SupportsRichComparison(Protocol):
    def __lt__(self, other: Any) -> bool: ...

class Stream(Generic[T]):
    """
    A sequence of elements supporting sequential and aggregate operations.
    Inspired by Java Streams and JavaScript Array methods.
    
    This stream is lazy: intermediate operations (map, filter) are not executed
    until a terminal operation (to_list, reduce) is invoked.
    """

    def __init__(self, iterable: Iterable[T]):
        self._iterator: Iterator[T] = iter(iterable)

    @classmethod
    def of(cls, *args: T) -> "Stream[T]":
        """Creates a Stream from specific iterables data structure."""
        return cls(args)

    def map(self, mapper: Callable[[T], R]) -> "Stream[R]":
        """Returns a stream consisting of the results of applying the given function."""
        return Stream(map(mapper, self._iterator))

    def filter(self, predicate: Callable[[T], bool]) -> "Stream[T]":
        """Returns a stream consisting of the elements that match the given predicate."""
        return Stream(filter(predicate, self._iterator))

    def flat_map(self, mapper: Callable[[T], Iterable[R]]) -> "Stream[R]":
        """
        Returns a stream consisting of the results of replacing each element 
        with the contents of a mapped stream.
        """
        mapped_iterators = map(mapper, self._iterator)
        return Stream(itertools.chain.from_iterable(mapped_iterators))

    def peek(self, action: Callable[[T], None]) -> "Stream[T]":
        """
        Performs the provided action on each element as elements are consumed
        from the resulting stream.
        """
        def peeking_generator():
            for item in self._iterator:
                action(item)
                yield item
        return Stream(peeking_generator())

    def distinct(self) -> "Stream[T]":
        """Returns a stream consisting of the distinct elements."""
        def distinct_generator():
            distinct = set()
            for item in self._iterator:
                if item not in distinct:
                    distinct.add(item)
                    yield item
        return Stream(distinct_generator())

    def sorted(self, key: Optional[Callable[[T], Any]] = None, reverse: bool = False) -> "Stream[T]":
        """
        Returns a stream consisting of the elements of this stream, sorted.
        """
        if key is None:
            sortable_iter = cast(Iterator[SupportsRichComparison], self._iterator)
            return Stream(cast(Iterator[T], sorted(sortable_iter, reverse=reverse)))
        else:
            return Stream(sorted(self._iterator, key=key, reverse=reverse))

    def limit(self, max_size: int) -> "Stream[T]":
        """Returns a stream consisting of the elements truncated to be no longer than max_size."""
        return Stream(itertools.islice(self._iterator, max_size))

    def skip(self, n: int) -> "Stream[T]":
        """Returns a stream consisting of the remaining elements after discarding the first n."""
        return Stream(itertools.islice(self._iterator, n, None))

    def for_each(self, action: Callable[[T], None]) -> None:
        """Performs an action for each element of this stream."""
        for item in self._iterator:
            action(item)

    def to_list(self) -> List[T]:
        """Collects elements into a Python List."""
        return list(self._iterator)

    def to_set(self) -> Set[T]:
        """Collects elements into a Python Set."""
        return set(self._iterator)

    def reduce(self, accumulator: Callable[[T, T], T], init: Union[T, None] = None) -> Union[T, None]:
        """
        Performs a reduction on the elements of this stream.
        """
        if init is not None:
            return functools.reduce(accumulator, self._iterator, init)
        
        try:
            return functools.reduce(accumulator, self._iterator)
        except TypeError:
            return None

    def count(self) -> int:
        """Returns the count of elements in this stream."""
        return sum(1 for _ in self._iterator)

    def find_first(self) -> "Option[T]":
        """
        Returns an Option describing the first element of this stream, 
        or an empty Option if the stream is empty.
        """
        try:
            value = next(self._iterator)
            return Option.of(value)
        except StopIteration:
            return Option.empty()

    def any_match(self, predicate: Callable[[T], bool]) -> bool:
        """Returns whether any elements of this stream match the provided predicate."""
        return any(predicate(item) for item in self._iterator)

    def all_match(self, predicate: Callable[[T], bool]) -> bool:
        """Returns whether all elements of this stream match the provided predicate."""
        return all(predicate(item) for item in self._iterator)

    def none_match(self, predicate: Callable[[T], bool]) -> bool:
        """Returns whether no elements of this stream match the provided predicate."""
        return not self.any_match(predicate)
    
    def collect(self, collector: Callable[[Iterable[T]], R]) -> R:
        """
        Performs a mutable reduction operation on the elements of this stream.
        """
        return collector(self._iterator)
    
    def take_while(self, predicate: Callable[[T], bool]) -> "Stream[T]":
        """
        Returns a stream consisting of the longest prefix of elements 
        that satisfy the given predicate.
        """
        return Stream(itertools.takewhile(predicate, self._iterator))

    def drop_while(self, predicate: Callable[[T], bool]) -> "Stream[T]":
        """
        Returns a stream consisting of the remaining elements after dropping
        the longest prefix of elements that satisfy the given predicate.
        """
        return Stream(itertools.dropwhile(predicate, self._iterator))
    
    def zip(self, other: Iterable[R]) -> "Stream[tuple[T, R]]":
        """
        Combines this stream with another iterable into a stream of pairs (tuples).
        The stream stops when the shorter of the two runs out.
        """
        return Stream(zip(self._iterator, other))

    def zip_with_index(self) -> "Stream[tuple[int, T]]":
        """
        Returns a stream of (index, element) pairs.
        """
        return Stream(enumerate(self._iterator))