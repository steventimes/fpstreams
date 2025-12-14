from abc import ABC, abstractmethod
from typing import Generic, Callable, Iterable, Optional, Any, List, Set, Tuple
from .common import T, R

class BaseStream(ABC, Generic[T]):
    """
    The main interface for Stream operations.
    
    All implementations (SequentialStream, ParallelStream) must adhere to this contract.
    """
    
    # --- Transformation Operations ---

    @abstractmethod
    def map(self, mapper: Callable[[T], R]) -> "BaseStream[R]": 
        """
        Returns a stream consisting of the results of applying the given function to the elements of this stream.
        
        Args:
            mapper: A non-interfering, stateless function to apply to each element.
            
        Returns:
            A new Stream containing the transformed elements.
        """
        ...
    
    @abstractmethod
    def filter(self, predicate: Callable[[T], bool]) -> "BaseStream[T]":
        """
        Returns a stream consisting of the elements of this stream that match the given predicate.
        
        Args:
            predicate: A non-interfering, stateless predicate to apply to each element.
        """
        ...

    @abstractmethod
    def flat_map(self, mapper: Callable[[T], Iterable[R]]) -> "BaseStream[R]":
        """
        Returns a stream consisting of the results of replacing each element of this stream 
        with the contents of a mapped iterable produced by applying the provided mapping function.
        """
        ...

    @abstractmethod
    def pick(self, key: Any) -> "BaseStream[Any]":
        """
        Extracts a value by key or index from each element in the stream.
        
        This is a syntactic sugar for `.map(lambda x: x[key])`.
        
        Args:
            key: The dictionary key or list index to extract.
            
        Returns:
            A stream containing the extracted values. If a key is missing, it may yield None (implementation dependent).
        """
        ...

    @abstractmethod
    def filter_none(self, key: Any = None) -> "BaseStream[T]":
        """
        Removes None values.
        If key is provided, removes elements where element[key] is None.
        """
        ...

    @abstractmethod
    def distinct(self) -> "BaseStream[T]":
        """
        Returns a stream consisting of the distinct elements (according to Object.equals(Object)) of this stream.
        """
        ...

    @abstractmethod
    def sorted(self, key: Optional[Callable[[T], Any]] = None, reverse: bool = False) -> "BaseStream[T]":
        """
        Returns a stream consisting of the elements of this stream, sorted according to the provided key.
        """
        ...

    @abstractmethod
    def peek(self, action: Callable[[T], None]) -> "BaseStream[T]":
        """
        Returns a stream consisting of the elements of this stream, additionally performing the provided action 
        on each element as elements are consumed from the resulting stream.
        """
        ...

    @abstractmethod
    def limit(self, max_size: int) -> "BaseStream[T]":
        """
        Returns a stream consisting of the elements of this stream, truncated to be no longer than ``max_size`` in length.
        """
        ...

    @abstractmethod
    def skip(self, n: int) -> "BaseStream[T]":
        """
        Returns a stream consisting of the remaining elements of this stream after discarding the first ``n`` elements.
        """
        ...

    @abstractmethod
    def take_while(self, predicate: Callable[[T], bool]) -> "BaseStream[T]":
        """
        Returns a stream consisting of the longest prefix of elements taken from this stream that match the given predicate.
        """
        ...

    @abstractmethod
    def drop_while(self, predicate: Callable[[T], bool]) -> "BaseStream[T]":
        """
        Returns a stream consisting of the remaining elements of this stream after dropping the longest prefix 
        of elements that match the given predicate.
        """
        ...
    
    @abstractmethod
    def zip(self, other: Iterable[R]) -> "BaseStream[Tuple[T, R]]":
        """
        Combines elements of this stream with another iterable into tuples.
        Stops when the shortest iterable is exhausted.
        
        Args:
            other: The iterable to zip with.
            
        Returns:
            A stream of tuples (item_from_this, item_from_other).
        """
        ...
        
    @abstractmethod
    def zip_with_index(self, start: int = 0) -> "BaseStream[Tuple[int, T]]":
        """
        Returns a stream of (index, element) pairs.
        
        Args:
            start: The starting index (default is 0).
            
        Returns:
            Stream of tuples like (0, element0), (1, element1), ...
        """
        ...

    # --- Terminal Operations ---

    @abstractmethod
    def collect(self, collector: Callable[[Iterable[T]], R]) -> R: 
        """
        Performs a mutable reduction operation on the elements of this stream using a Collector.
        
        Args:
            collector: A function (usually from ``Collectors``) that aggregates the stream.
        """
        ...
        
    @abstractmethod
    def reduce(self, identity: R, accumulator: Callable[[R, T], R]) -> R:
        """
        Performs a reduction on the elements of this stream, using an
        associative accumulation function, and an identity value.
        """
        ...

    @abstractmethod
    def to_list(self) -> List[T]:
        """Accumulates the elements of this stream into a List."""
        ...

    @abstractmethod
    def to_set(self) -> Set[T]:
        """Accumulates the elements of this stream into a Set."""
        ...

    @abstractmethod
    def for_each(self, action: Callable[[T], None]) -> None:
        """Performs an action for each element of this stream."""
        ...

    @abstractmethod
    def count(self) -> int:
        """Returns the count of elements in this stream."""
        ...
    
    @abstractmethod
    def find_first(self) -> Any: 
        """Returns an Option describing the first element of this stream, or an empty Option if the stream is empty."""
        ...

    @abstractmethod
    def any_match(self, predicate: Callable[[T], bool]) -> bool:
        """Returns whether any elements of this stream match the provided predicate."""
        ...

    @abstractmethod
    def all_match(self, predicate: Callable[[T], bool]) -> bool:
        """Returns whether all elements of this stream match the provided predicate."""
        ...

    @abstractmethod
    def none_match(self, predicate: Callable[[T], bool]) -> bool:
        """Returns whether no elements of this stream match the provided predicate."""
        ...

    @abstractmethod
    def min(self, key: Optional[Callable[[T], Any]] = None) -> Any:
        """Returns the minimum element of this stream according to the provided key."""
        ...

    @abstractmethod
    def max(self, key: Optional[Callable[[T], Any]] = None) -> Any: 
        """Returns the maximum element of this stream according to the provided key."""
        ...

    @abstractmethod
    def sum(self) -> Any:
        """Returns the sum of elements in this stream."""
        ...
    
    @abstractmethod
    def join(self, delimiter: str = "") -> str:
        """Joins the string representation of elements with the given delimiter."""
        ...

    # --- Data Science & I/O (v0.3.0) ---

    @abstractmethod
    def describe(self) -> dict:
        """
        Calculates descriptive statistics (count, mean, min, max, std).
        Safely handles non-numeric data by returning only counts.
        """
        ...

    @abstractmethod
    def to_df(self, columns: Optional[List[str]] = None) -> Any:
        """
        Converts the stream into a Pandas DataFrame.
        Requires ``pandas`` to be installed.
        """
        ...

    @abstractmethod
    def to_csv(self, filepath: str, header: Optional[List[str]] = None) -> None:
        """
        Writes the stream elements to a CSV file.
        For SequentialStream, this is done row-by-row to save memory.
        """
        ...

    @abstractmethod
    def to_json(self, filepath: str) -> None:
        """
        Writes the stream elements to a JSON file.
        """
        ...
    
    @abstractmethod
    def parallel(self, processes: Optional[int] = None) -> "BaseStream[T]":
        """
        Returns a parallel stream that executes operations on multiple cores.
        """
        ...