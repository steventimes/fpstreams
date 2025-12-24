from typing import TypeVar, Generic, Callable

T = TypeVar("T")
R = TypeVar("R")

class Option(Generic[T]):
    """
    A container object which may or may not contain a non-null value.
    Replaces None checks.
    """

    def __init__(self, value: T | None):
        self._value = value

    @classmethod
    def of(cls, value: T) -> "Option[T]":
        """Returns an Option describing the given non-null value."""
        if value is None:
            raise ValueError("Option.of() cannot be called with None. Use Option.of_nullable() instead.")
        return cls(value)

    @classmethod
    def of_nullable(cls, value: T | None) -> "Option[T]":
        """Returns an Option describing the given value, if non-null, otherwise returns an empty Option."""
        return cls(value)

    @classmethod
    def empty(cls) -> "Option[T]":
        """Returns an empty Option instance."""
        return cls(None)

    def is_present(self) -> bool:
        """Returns True if there is a value present, otherwise False."""
        return self._value is not None

    def is_empty(self) -> bool:
        """Returns True if there is no value present, otherwise False."""
        return self._value is None

    def if_present(self, action: Callable[[T], None]) -> None:
        """If a value is present, invokes the specified consumer with the value, otherwise does nothing."""
        if self._value is not None:
            action(self._value)

    def filter(self, predicate: Callable[[T], bool]) -> "Option[T]":
        """
        If a value is present and matches the given predicate, return an Option describing the value,
        otherwise return an empty Option.
        """
        if self._value is None:
            return self
        
        if predicate(self._value):
            return self
        return Option.empty()

    def map(self, mapper: Callable[[T], R]) -> "Option[R]":
        """
        If a value is present, apply the provided mapping function to it, 
        and if the result is non-null, return an Option describing the result.
        """
        if self._value is None:
            return Option.empty()
        
        result = mapper(self._value)
        return Option.of_nullable(result)

    def flat_map(self, mapper: Callable[[T], "Option[R]"]) -> "Option[R]":
        """
        If a value is present, apply the provided Option-bearing mapping function to it, 
        return that result, otherwise return an empty Option.
        """
        if self._value is None:
            return Option.empty()
        
        return mapper(self._value)

    def or_else(self, other: T) -> T:
        """Return the value if present, otherwise return other."""
        return self._value if self._value is not None else other

    def or_else_get(self, supplier: Callable[[], T]) -> T:
        """Return the value if present, otherwise invoke other and return the result of that invocation."""
        return self._value if self._value is not None else supplier()

    def or_else_throw(self, exception_supplier: Callable[[], Exception]) -> T:
        """Return the contained value, if present, otherwise throw an exception to be created by the provided supplier."""
        if self._value is not None:
            return self._value
        raise exception_supplier()

    def __repr__(self) -> str:
        return f"Option({self._value})" if self._value is not None else "Option.empty"
    
    def __bool__(self) -> bool:
        return self._value is not None