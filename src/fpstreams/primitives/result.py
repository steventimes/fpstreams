"""Success and failure values for explicit error handling."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, cast

T = TypeVar("T")
R = TypeVar("R")


class Result(Generic[T]):
    """A value that is either :class:`Ok` or :class:`Err`."""

    @classmethod
    def success(cls, value: T) -> Result[T]:
        """Create a successful Result containing value.

        Args:
            value: The value consumed by this operation.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        return Ok(value)

    @classmethod
    def failure(cls, error: Exception) -> Result[T]:
        """Create a failed Result containing error.

        Args:
            error: The exception stored in a failed result.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        return cast(Result[T], Err(error))

    @classmethod
    def of(cls, function: Callable[[], T]) -> Result[T]:
        """Call function and capture its return value or exception.

        Args:
            function: The callable applied by this operation.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        try:
            return Ok(function())
        except Exception as error:
            return cast(Result[T], Err(error))

    @property
    def error(self) -> Exception | None:
        """Return the failure exception, or None for success.

        Returns:
            The matching or computed value, or `None` when unavailable.
        """
        raise NotImplementedError

    def is_success(self) -> bool:
        """Return whether this Result contains a successful value.

        Returns:
            Whether the condition described above is true.
        """
        return isinstance(self, Ok)

    def is_failure(self) -> bool:
        """Return whether this Result contains an exception.

        Returns:
            Whether the condition described above is true.
        """
        return isinstance(self, Err)

    def map(self, mapper: Callable[[T], R]) -> Result[R]:
        """Transform a successful value while preserving failure.

        The callable runs only for success. Existing failures pass through unchanged, and raised
        exceptions become failures.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        raise NotImplementedError

    def and_then(self, mapper: Callable[[T], Result[R]]) -> Result[R]:
        """Chain an operation that returns another Result.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        raise NotImplementedError

    def flat_map(self, mapper: Callable[[T], Result[R]]) -> Result[R]:
        """Chain an operation that returns another Result.

        The callable runs only for success and must return another `Result`; failures pass
        through unchanged.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        return self.and_then(mapper)

    def map_err(self, mapper: Callable[[Exception], Exception]) -> Result[T]:
        """Transform the exception while preserving success.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        raise NotImplementedError

    def map_error(self, mapper: Callable[[Exception], Exception]) -> Result[T]:
        """Transform the exception while preserving success.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        return self.map_err(mapper)

    def on_success(self, action: Callable[[T], None]) -> Result[T]:
        """Run action for a successful value and return this Result.

        Args:
            action: The side-effecting callable invoked for each matching item.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        if isinstance(self, Ok):
            action(self.value)
        return self

    def on_failure(self, action: Callable[[Exception], None]) -> Result[T]:
        """Run action for a failure exception and return this Result.

        Args:
            action: The side-effecting callable invoked for each matching item.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        if isinstance(self, Err):
            action(self.error)
        return self

    def unwrap(self) -> T:
        """Return the successful value or raise the failure exception.

        Returns:
            The successful value.

        Raises:
            Exception: The stored failure exception when this result is unsuccessful.
        """
        raise NotImplementedError

    def get_or_throw(self) -> T:
        """Return the successful value or raise the failure exception.

        Returns:
            The successful value.
        """
        return self.unwrap()

    def get_or_else(self, default: T) -> T:
        """Return the successful value, otherwise default.

        Args:
            default: The value returned when no matching item is available.

        Returns:
            The successful value, or `default` for a failure.
        """
        return self.value if isinstance(self, Ok) else default


@dataclass(frozen=True, slots=True)
class Ok(Result[T]):
    """A successful Result containing a value."""

    value: T

    @property
    def error(self) -> None:
        """Return None because this Result is successful.

        Returns:
            Always `None`.
        """
        return None

    def map(self, mapper: Callable[[T], R]) -> Result[R]:
        """Transform a successful value while preserving failure.

        The callable transforms the successful value; an exception raised by it is captured as
        `Err`.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        try:
            return Ok(mapper(self.value))
        except Exception as error:
            return Err(error)

    def and_then(self, mapper: Callable[[T], Result[R]]) -> Result[R]:
        """Chain an operation that returns another Result.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        try:
            return mapper(self.value)
        except Exception as error:
            return Err(error)

    def map_err(self, mapper: Callable[[Exception], Exception]) -> Result[T]:
        """Transform the exception while preserving success.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        return self

    def unwrap(self) -> T:
        """Return the successful value or raise the failure exception.

        Returns:
            The successful value.
        """
        return self.value


@dataclass(frozen=True, slots=True)
class Err(Result[Any]):
    """A failed Result containing an exception."""

    error: Exception = field()

    def map(self, mapper: Callable[[Any], R]) -> Result[R]:
        """Transform a successful value while preserving failure.

        A failed result bypasses the callable and preserves its original exception.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        return self

    def and_then(self, mapper: Callable[[Any], Result[R]]) -> Result[R]:
        """Chain an operation that returns another Result.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        return self

    def map_err(self, mapper: Callable[[Exception], Exception]) -> Result[Any]:
        """Transform the exception while preserving success.

        Args:
            mapper: The callable used to transform each selected value.

        Returns:
            A `Result` containing the transformed success or preserved failure.
        """
        return Err(mapper(self.error))

    def unwrap(self) -> Any:
        """Return the successful value or raise the failure exception.

        Returns:
            This method does not return normally; it raises the stored exception.

        Raises:
            Exception: The stored failure exception.
        """
        raise self.error
