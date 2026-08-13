"""An immutable optional-value container."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


@dataclass(frozen=True, slots=True)
class Option(Generic[T]):
    """A compatibility container for a value that may be absent."""

    _value: T | None

    @classmethod
    def of(cls, value: T) -> Option[T]:
        """Wrap a non-None value; use empty() or of_nullable() for absence.

        Args:
            value: The value consumed by this operation.

        Returns:
            An `Option` containing the resulting value, or an empty option.
        """
        if value is None:
            raise ValueError("Option.of(None) is invalid; use Option.empty()")
        return cls(value)

    @classmethod
    def of_nullable(cls, value: T | None) -> Option[T]:
        """Wrap value in an Option; None produces an empty Option.

        Args:
            value: The value consumed by this operation.

        Returns:
            An `Option` containing the resulting value, or an empty option.
        """
        return cls(value)

    @classmethod
    def empty(cls) -> Option[T]:
        """Return an Option with no value.

        Returns:
            An `Option` containing the resulting value, or an empty option.
        """
        return cls(None)

    def is_present(self) -> bool:
        """Return whether this Option contains a value.

        Returns:
            Whether the condition described above is true.
        """
        return self._value is not None

    def is_empty(self) -> bool:
        """Return whether this Option contains no value.

        Returns:
            Whether the condition described above is true.
        """
        return self._value is None

    def if_present(self, action: Callable[[T], None]) -> None:
        """Run action when a value is present.

        Args:
            action: The side-effecting callable invoked for each matching item.
        """
        if self._value is not None:
            action(self._value)

    def filter(self, predicate: Callable[[T], bool]) -> Option[T]:
        """Keep the value only when predicate returns true.

        An empty option stays empty. A present value is retained only when the predicate is
        true.

        Args:
            predicate: A callable that decides whether an item matches.

        Returns:
            An `Option` containing the resulting value, or an empty option.
        """
        if self._value is None or predicate(self._value):
            return self
        return Option.empty()

    def map(self, mapper: Callable[[T], R | None]) -> Option[R]:
        """Transform a present value; None becomes an empty Option.

        The callable runs only for a present value. A mapped `None` becomes an empty `Option`.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            An `Option` containing the resulting value, or an empty option.
        """
        if self._value is None:
            return Option.empty()
        return Option.of_nullable(mapper(self._value))

    def flat_map(self, mapper: Callable[[T], Option[R]]) -> Option[R]:
        """Transform a present value with an Option-returning function.

        The callable runs only for a present value and must return another `Option`.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            An `Option` containing the resulting value, or an empty option.
        """
        if self._value is None:
            return Option.empty()
        return mapper(self._value)

    def unwrap(self) -> T:
        """Return the value or raise ValueError when empty.

        Returns:
            The contained value.

        Raises:
            ValueError: If this option is empty.
        """
        if self._value is None:
            raise ValueError("cannot unwrap an empty Option")
        return self._value

    def or_else(self, other: T) -> T:
        """Return the value when present, otherwise other.

        Args:
            other: The other iterable, flow, value, or fallback used by the operation.

        Returns:
            The contained value, or `other` when this option is empty.
        """
        return self._value if self._value is not None else other

    def or_else_get(self, supplier: Callable[[], T]) -> T:
        """Return the value when present, otherwise call supplier.

        Args:
            supplier: A zero-argument callable that supplies a value or iterable.

        Returns:
            The contained value, or the value supplied when this option is empty.
        """
        return self._value if self._value is not None else supplier()

    def or_else_throw(self, exception: Callable[[], Exception]) -> T:
        """Return the value when present, otherwise raise a supplied exception.

        Args:
            exception: A zero-argument callable that creates the exception to raise.

        Returns:
            The contained value.

        Raises:
            Exception: The exception produced by `exception` when this option is empty.
        """
        if self._value is None:
            raise exception()
        return self._value

    def __bool__(self) -> bool:
        return self.is_present()

    def __repr__(self) -> str:
        return f"Option({self._value!r})" if self else "Option.empty"
