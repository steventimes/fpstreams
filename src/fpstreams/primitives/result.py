"""Represent successful values and captured exceptions without implicit raising."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, cast

T = TypeVar("T")
R = TypeVar("R")


class Result(Generic[T]):
    """Common interface for a successful :class:`Ok` or failed :class:`Err` value."""

    @classmethod
    def success(cls, value: T) -> Result[T]:
        """Create an :class:`Ok` containing `value`.

        Args:
            value: Successful value to store.

        Returns:
            A new successful result.
        """
        return Ok(value)

    @classmethod
    def failure(cls, error: Exception) -> Result[T]:
        """Create an :class:`Err` containing `error`.

        Args:
            error: The exception stored in a failed result.

        Returns:
            A new failed result typed for the caller's expected success value.
        """
        return cast(Result[T], Err(error))

    @classmethod
    def of(cls, function: Callable[[], T]) -> Result[T]:
        """Call a zero-argument function and capture ordinary exceptions as failure.

        A normal return becomes :class:`Ok`; an :class:`Exception` becomes :class:`Err`.
        Exceptions outside the `Exception` hierarchy, such as `KeyboardInterrupt`, propagate.

        Args:
            function: Zero-argument computation to evaluate immediately.

        Returns:
            `Ok(function())`, or `Err(error)` when the call raises `error`.
        """
        try:
            return Ok(function())
        except Exception as error:
            return cast(Result[T], Err(error))

    @property
    def error(self) -> Exception | None:
        """Expose the stored failure exception, or `None` for success.

        Returns:
            The :class:`Err` exception or `None` for :class:`Ok`.
        """
        raise NotImplementedError

    def is_success(self) -> bool:
        """Report whether this instance is an :class:`Ok`.

        Returns:
            `True` for `Ok` and `False` for `Err`.
        """
        return isinstance(self, Ok)

    def is_failure(self) -> bool:
        """Report whether this instance is an :class:`Err`.

        Returns:
            `True` for `Err` and `False` for `Ok`.
        """
        return isinstance(self, Err)

    def map(self, mapper: Callable[[T], R]) -> Result[R]:
        """Map an `Ok` value while preserving an existing `Err`.

        The mapper runs only for success. Its return value becomes a new `Ok`, and any ordinary
        exception it raises becomes `Err`. A failed result returns itself unchanged.

        Args:
            mapper: Callable applied to a successful value.

        Returns:
            The mapped success, captured mapper failure, or original failed result.
        """
        raise NotImplementedError

    def and_then(self, mapper: Callable[[T], Result[R]]) -> Result[R]:
        """Chain an `Ok` through a result-returning callable and bypass an `Err`.

        Exceptions raised while mapping a success are captured as `Err`. The mapper's return
        value is passed through without runtime type validation.

        Args:
            mapper: Result-returning callable applied only to a successful value.

        Returns:
            The mapper's result, a captured mapper exception, or the original failure.
        """
        raise NotImplementedError

    def flat_map(self, mapper: Callable[[T], Result[R]]) -> Result[R]:
        """Alias :meth:`and_then` for chaining result-returning computations.

        The callable runs only for success; existing failures pass through unchanged.

        Args:
            mapper: Result-returning callable applied only to a successful value.

        Returns:
            Exactly `self.and_then(mapper)`.
        """
        return self.and_then(mapper)

    def map_err(self, mapper: Callable[[Exception], Exception]) -> Result[T]:
        """Map an `Err` exception while preserving an existing `Ok`.

        Mapper exceptions are not captured; they propagate to the caller.

        Args:
            mapper: Callable that converts the stored exception.

        Returns:
            A new `Err` for a failure, or the original successful result.
        """
        raise NotImplementedError

    def map_error(self, mapper: Callable[[Exception], Exception]) -> Result[T]:
        """Alias :meth:`map_err` for transforming a stored exception.

        Args:
            mapper: Callable that converts the stored exception.

        Returns:
            Exactly `self.map_err(mapper)`.
        """
        return self.map_err(mapper)

    def on_success(self, action: Callable[[T], None]) -> Result[T]:
        """Run a side effect for `Ok` and return this same result instance.

        The action is skipped for `Err`; exceptions raised by the action propagate.

        Args:
            action: Callable invoked with the successful value.

        Returns:
            `self`, unchanged.
        """
        if isinstance(self, Ok):
            action(self.value)
        return self

    def on_failure(self, action: Callable[[Exception], None]) -> Result[T]:
        """Run a side effect for `Err` and return this same result instance.

        The action is skipped for `Ok`; exceptions raised by the action propagate.

        Args:
            action: Callable invoked with the stored exception.

        Returns:
            `self`, unchanged.
        """
        if isinstance(self, Err):
            action(self.error)
        return self

    def unwrap(self) -> T:
        """Extract an `Ok` value or raise the exception stored by `Err`.

        Returns:
            The successful value.

        Raises:
            Exception: The stored failure exception when this result is unsuccessful.
        """
        raise NotImplementedError

    def get_or_throw(self) -> T:
        """Alias :meth:`unwrap` for extracting success or raising failure.

        Returns:
            The successful value.
        """
        return self.unwrap()

    def get_or_else(self, default: T) -> T:
        """Return the `Ok` value or an eagerly supplied default for `Err`.

        Args:
            default: Value returned only for a failed result.

        Returns:
            The successful value, or `default` for a failure.
        """
        return self.value if isinstance(self, Ok) else default


@dataclass(frozen=True, slots=True)
class Ok(Result[T]):
    """An immutable successful result containing `value`."""

    value: T

    @property
    def error(self) -> None:
        """Return `None` because this result has no failure exception.

        Returns:
            Always `None`.
        """
        return None

    def map(self, mapper: Callable[[T], R]) -> Result[R]:
        """Map `value` into a new `Ok`, capturing mapper exceptions as `Err`.

        The callable transforms the successful value; an exception raised by it is captured as
        `Err`.

        Args:
            mapper: Callable applied to `value`.

        Returns:
            `Ok(mapper(value))`, or `Err(error)` if the mapper raises `error`.
        """
        try:
            return Ok(mapper(self.value))
        except Exception as error:
            return Err(error)

    def and_then(self, mapper: Callable[[T], Result[R]]) -> Result[R]:
        """Return `mapper(value)`, capturing an ordinary mapper exception as `Err`.

        Args:
            mapper: Result-returning callable applied to `value`.

        Returns:
            The mapper's result or an `Err` containing its raised exception.
        """
        try:
            return mapper(self.value)
        except Exception as error:
            return Err(error)

    def map_err(self, mapper: Callable[[Exception], Exception]) -> Result[T]:
        """Skip the error mapper and preserve this successful result.

        Args:
            mapper: Unused because this result has no error.

        Returns:
            `self`.
        """
        return self

    def unwrap(self) -> T:
        """Return the contained successful value.

        Returns:
            The successful value.
        """
        return self.value


@dataclass(frozen=True, slots=True)
class Err(Result[Any]):
    """An immutable failed result containing an exception."""

    error: Exception = field()

    def map(self, mapper: Callable[[Any], R]) -> Result[R]:
        """Skip the success mapper and preserve this failed result.

        A failed result bypasses the callable and preserves its original exception.

        Args:
            mapper: Unused because this result has no successful value.

        Returns:
            `self`.
        """
        return self

    def and_then(self, mapper: Callable[[Any], Result[R]]) -> Result[R]:
        """Skip result chaining and preserve this failed result.

        Args:
            mapper: Unused because this result has no successful value.

        Returns:
            `self`.
        """
        return self

    def map_err(self, mapper: Callable[[Exception], Exception]) -> Result[Any]:
        """Replace the stored exception with `mapper(error)` in a new `Err`.

        Exceptions from `mapper` propagate instead of being captured.

        Args:
            mapper: Callable applied to the stored exception.

        Returns:
            A new failed result containing the mapped exception.
        """
        return Err(mapper(self.error))

    def unwrap(self) -> Any:
        """Raise the stored exception instead of returning a value.

        Returns:
            This method does not return normally; it raises the stored exception.

        Raises:
            Exception: The stored failure exception.
        """
        raise self.error
