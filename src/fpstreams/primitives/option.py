"""An immutable optional-value type in which `None` represents absence."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


@dataclass(frozen=True, slots=True)
class Option(Generic[T]):
    """Store either one non-`None` value or an empty state.

    Instances are immutable and cannot distinguish a present `None` from absence. Use
    :meth:`of`, :meth:`of_nullable`, or :meth:`empty` to make that choice explicit.
    """

    _value: T | None

    @classmethod
    def of(cls, value: T) -> Option[T]:
        """Create a present option from a non-`None` value.

        Args:
            value: Value to store.

        Returns:
            A present option containing `value`.

        Raises:
            ValueError: If `value` is `None`.
        """
        if value is None:
            raise ValueError("Option.of(None) is invalid; use Option.empty()")
        return cls(value)

    @classmethod
    def of_nullable(cls, value: T | None) -> Option[T]:
        """Create a present option from `value`, or an empty option from `None`.

        Args:
            value: Nullable value to convert.

        Returns:
            `Option.empty()` for `None`; otherwise an option containing `value`.
        """
        return cls(value)

    @classmethod
    def empty(cls) -> Option[T]:
        """Create an option whose stored state is absent.

        Returns:
            An empty option.
        """
        return cls(None)

    def is_present(self) -> bool:
        """Report whether this option stores a non-`None` value.

        Returns:
            `True` for a present value and `False` for an empty option.
        """
        return self._value is not None

    def is_empty(self) -> bool:
        """Report whether this option stores `None` as its empty marker.

        Returns:
            `True` for an empty option and `False` for a present value.
        """
        return self._value is None

    def if_present(self, action: Callable[[T], None]) -> None:
        """Invoke `action` once with the stored value, if one is present.

        Args:
            action: Side-effecting callable skipped for an empty option.
        """
        if self._value is not None:
            action(self._value)

    def filter(self, predicate: Callable[[T], bool]) -> Option[T]:
        """Keep a present value only when `predicate` accepts it.

        Empty options and accepted values return the current immutable instance. A rejected
        value produces a new empty option. Exceptions from `predicate` propagate.

        Args:
            predicate: Callable evaluated only for a present value.

        Returns:
            This option when empty or accepted; otherwise an empty option.
        """
        if self._value is None or predicate(self._value):
            return self
        return Option.empty()

    def map(self, mapper: Callable[[T], R | None]) -> Option[R]:
        """Map a present value and convert a mapped `None` to absence.

        The mapper is skipped for an empty option. Unlike :class:`Result`, this method does
        not capture mapper exceptions.

        Args:
            mapper: Callable applied to the stored value.

        Returns:
            An option containing the mapped value, or an empty option when either input or
            output is `None`.
        """
        if self._value is None:
            return Option.empty()
        return Option.of_nullable(mapper(self._value))

    def flat_map(self, mapper: Callable[[T], Option[R]]) -> Option[R]:
        """Return the option produced by mapping a present value.

        The mapper is skipped for an empty option. Its return value and any exception are
        passed through without additional wrapping or validation.

        Args:
            mapper: Option-returning callable applied to the stored value.

        Returns:
            The mapper's result, or an empty option when this option is empty.
        """
        if self._value is None:
            return Option.empty()
        return mapper(self._value)

    def unwrap(self) -> T:
        """Extract the stored value, rejecting an empty option.

        Returns:
            The contained value.

        Raises:
            ValueError: If this option is empty.
        """
        if self._value is None:
            raise ValueError("cannot unwrap an empty Option")
        return self._value

    def or_else(self, other: T) -> T:
        """Return the stored value, or the eagerly supplied fallback when empty.

        Args:
            other: Value to return only when this option is empty.

        Returns:
            The contained value, or `other` when this option is empty.
        """
        return self._value if self._value is not None else other

    def or_else_get(self, supplier: Callable[[], T]) -> T:
        """Return the stored value, or lazily call `supplier` when empty.

        Args:
            supplier: Zero-argument fallback callable, skipped for a present value.

        Returns:
            The contained value, or the value supplied when this option is empty.
        """
        return self._value if self._value is not None else supplier()

    def or_else_throw(self, exception: Callable[[], Exception]) -> T:
        """Return the stored value, or construct and raise an exception when empty.

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
        """Treat a present option as true and an empty option as false."""
        return self.is_present()

    def __repr__(self) -> str:
        """Render present values as `Option(value)` and absence as `Option.empty`."""
        return f"Option({self._value!r})" if self else "Option.empty"
