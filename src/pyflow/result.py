from typing import TypeVar, Generic, Callable, Union, Any, cast

T = TypeVar("T")
R = TypeVar("R")

class Result(Generic[T]):
    """
    A container for a value that represents either a successful result (Success)
    or a failure (Failure). 
    Replaces try/except blocks in functional pipelines.
    """

    def __init__(self, value: Union[T, None], error: Union[Exception, None], is_success: bool):
        self._value = value
        self._error = error
        self._is_success = is_success

    @classmethod
    def success(cls, value: T) -> "Result[T]":
        """Creates a successful Result."""
        return cls(value, None, True)

    @classmethod
    def failure(cls, error: Exception) -> "Result[T]":
        """Creates a failed Result."""
        return cls(None, error, False)

    @classmethod
    def of(cls, func: Callable[[], T]) -> "Result[T]":
        """
        Executes the provided function. 
        Returns Success(value) if it works, or Failure(exception) if it crashes.
        """
        try:
            return cls.success(func())
        except Exception as e:
            return cls.failure(e)

    def is_success(self) -> bool:
        return self._is_success

    def is_failure(self) -> bool:
        return not self._is_success

    def map(self, mapper: Callable[[T], R]) -> "Result[R]":
        """
        If Success, applies the mapper to the value.
        If Failure, returns the existing Failure (skips the mapper).
        """
        if self._is_success:
            try:
                val = cast(T, self._value)
                return Result.success(mapper(val))
            except Exception as e:
                return Result.failure(e)
        else:
            return Result.failure(cast(Exception, self._error))

    def flat_map(self, mapper: Callable[[T], "Result[R]"]) -> "Result[R]":
        """
        If Success, returns the result of applying the mapper (which must return a Result).
        If Failure, returns the existing Failure.
        """
        if self._is_success:
            try:
                val = cast(T, self._value)
                return mapper(val)
            except Exception as e:
                return Result.failure(e)
        return Result.failure(cast(Exception, self._error))

    def map_error(self, mapper: Callable[[Exception], Exception]) -> "Result[T]":
        """
        If Failure, allows you to transform the exception.
        If Success, does nothing.
        """
        if not self._is_success:
            err = cast(Exception, self._error)
            return Result.failure(mapper(err))
        return self

    def on_success(self, action: Callable[[T], None]) -> "Result[T]":
        """Executes action if Success."""
        if self._is_success:
            action(cast(T, self._value))
        return self

    def on_failure(self, action: Callable[[Exception], None]) -> "Result[T]":
        """Executes action if Failure."""
        if not self._is_success:
            action(cast(Exception, self._error))
        return self

    def get_or_else(self, default: T) -> T:
        """Returns the value if Success, or the default value if Failure."""
        if self._is_success:
            return cast(T, self._value)
        return default

    def get_or_throw(self) -> T:
        """Returns the value if Success, otherwise raises the stored exception."""
        if self._is_success:
            return cast(T, self._value)
        raise cast(Exception, self._error)

    def __repr__(self) -> str:
        if self._is_success:
            return f"Result.success({self._value})"
        return f"Result.failure({self._error})"