"""Own synchronous source openers and enforce their replayability contracts."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, replace
from gc import get_referents as _get_referents
from threading import Lock
from types import CodeType, FunctionType, MethodType
from typing import Any, Generic, TypeVar, cast

from ..errors import FlowConsumedError
from .semantics import (
    Cardinality,
    OrderingGuarantee,
    StreamFacts,
    TerminationEvidence,
    facts_from_capabilities,
)

T = TypeVar("T")
_SAFE_SIZED_TYPES = (list, tuple, range, str, bytes, dict, set, frozenset)
_NATIVE_SOURCE_TYPES = (list, tuple, range)
_NO_LIVE_SIZE = object()
_EMPTY_FACTORY_CELL = object()
_BUILTIN_LEN = len
_BUILTIN_TUPLE: type[tuple[Any, ...]] = tuple
_BUILTIN_TYPE = type
_BUILTIN_VALUE_ERROR = ValueError


def _function_code(function: FunctionType) -> CodeType | None:
    """Read a function's code identity without emitting the audited ``__code__`` event."""
    for referent in _get_referents(function):
        if _BUILTIN_TYPE(referent) is CodeType:
            return referent
    return None


def _function_closure_values(factory: FunctionType) -> tuple[Any, ...]:
    """Snapshot closure contents by identity while tolerating valid empty cells."""
    values: list[Any] = []
    for cell in factory.__closure__ or ():
        try:
            value = cell.cell_contents
        except _BUILTIN_VALUE_ERROR:
            value = _EMPTY_FACTORY_CELL
        values.append(value)
    return _BUILTIN_TUPLE(values)


@dataclass(frozen=True, slots=True)
class _CapturedFactoryCallable:
    """Freeze one function or bound method retained directly by an opener closure."""

    function: FunctionType
    code: CodeType | None
    closure: tuple[Any, ...]
    bound_self: Any


def _callable_parts(value: Any) -> tuple[FunctionType, Any] | None:
    """Return the Python function and optional receiver for one exact callable shape."""
    if _BUILTIN_TYPE(value) is FunctionType:
        return value, None
    if _BUILTIN_TYPE(value) is MethodType:
        function = value.__func__
        return (function, value.__self__) if _BUILTIN_TYPE(function) is FunctionType else None
    return None


def _captured_factory_callables(
    closure: tuple[Any, ...],
) -> tuple[_CapturedFactoryCallable | None, ...]:
    """Snapshot one level of callable code retained by an opener."""
    captured: list[_CapturedFactoryCallable | None] = []
    for value in closure:
        parts = _callable_parts(value)
        if parts is None:
            captured.append(None)
            continue
        function, bound_self = parts
        captured.append(
            _CapturedFactoryCallable(
                function,
                _function_code(function),
                _function_closure_values(function),
                bound_self,
            )
        )
    return _BUILTIN_TUPLE(captured)


@dataclass(frozen=True, slots=True)
class SourceCapabilities:
    """Record whether a source is reiterable, its safe exact size, and its ordering."""

    reiterable: bool
    exact_size: int | None
    ordered: bool = True


class Source(Generic[T]):
    """Open a synchronous source while atomically enforcing one-shot consumption.

    The source also retains conservative semantic facts and, for supported containers, the
    original data needed to cross into a native execution engine without first opening Python
    iteration.
    """

    __slots__ = (
        "_claimed",
        "_factory",
        "_initial_factory",
        "_initial_factory_callables",
        "_initial_factory_closure",
        "_initial_factory_code",
        "_initial_factory_function",
        "_initial_factory_self",
        "_live_size_data",
        "_lock",
        "_native_data",
        "_retained_sequence_data",
        "_track_factory_code",
        "capabilities",
        "facts",
    )

    def __init__(
        self,
        factory: Callable[[], Iterator[T]],
        capabilities: SourceCapabilities,
        native_data: Any = None,
        *,
        facts: StreamFacts | None = None,
        live_size_data: Any = _NO_LIVE_SIZE,
        track_factory_code: bool = True,
    ) -> None:
        """Store the opener and derive semantic facts when callers do not provide them."""
        self._factory = factory
        self._initial_factory = factory
        self._track_factory_code = track_factory_code
        parts = _callable_parts(factory) if track_factory_code else None
        self._initial_factory_function = None if parts is None else parts[0]
        self._initial_factory_self = None if parts is None else parts[1]
        self._initial_factory_code = None if parts is None else _function_code(parts[0])
        self._initial_factory_closure = () if parts is None else _function_closure_values(parts[0])
        self._initial_factory_callables = _captured_factory_callables(self._initial_factory_closure)
        self._live_size_data = live_size_data
        self._retained_sequence_data: list[Any] | tuple[Any, ...] | range | None = None
        self.capabilities = capabilities
        self._claimed = False
        self._lock = Lock()
        self._native_data = native_data
        self.facts = facts or facts_from_capabilities(
            reiterable=capabilities.reiterable,
            exact_size=capabilities.exact_size,
            ordered=capabilities.ordered,
        )

    @classmethod
    def from_iterable(cls, value: Iterable[T]) -> Source[T]:
        """Describe an iterable, treating iterator instances as atomically claimed one-shots.

        Exact size is trusted only for built-in containers with side-effect-free ``len``;
        lists, tuples, and ranges are additionally retained as native-engine inputs.
        """
        safely_sized = type(value) in _SAFE_SIZED_TYPES
        exact_size = len(cast(Any, value)) if safely_sized else None
        ordered = not isinstance(value, (set, frozenset))
        if not isinstance(value, Iterator):
            source = cls(
                lambda: iter(value),
                SourceCapabilities(
                    reiterable=True,
                    exact_size=exact_size,
                    ordered=ordered,
                ),
                native_data=value if type(value) in _NATIVE_SOURCE_TYPES else None,
                live_size_data=value if safely_sized else _NO_LIVE_SIZE,
                track_factory_code=False,
            )
            if type(value) in _NATIVE_SOURCE_TYPES:
                source._retained_sequence_data = cast(
                    "list[Any] | tuple[Any, ...] | range",
                    value,
                )
            return source
        iterator = iter(value)
        return cls(
            lambda: iterator,
            SourceCapabilities(reiterable=False, exact_size=exact_size),
            track_factory_code=False,
        )

    def _factory_is_pristine(self) -> bool:
        """Return whether retained metadata still describes the original opener."""
        if self._factory is not self._initial_factory:
            return False
        if not self._track_factory_code:
            return True
        parts = _callable_parts(self._factory)
        if parts is None:
            return True
        function, bound_self = parts
        if (
            function is not self._initial_factory_function
            or bound_self is not self._initial_factory_self
            or _function_code(function) is not self._initial_factory_code
        ):
            return False
        closure = _function_closure_values(function)
        if _BUILTIN_LEN(closure) != _BUILTIN_LEN(self._initial_factory_closure):
            return False
        index = 0
        while index < _BUILTIN_LEN(closure):
            if closure[index] is not self._initial_factory_closure[index]:
                return False
            captured = self._initial_factory_callables[index]
            if captured is not None:
                nested = _callable_parts(closure[index])
                if nested is None:
                    return False
                nested_function, nested_self = nested
                if (
                    nested_function is not captured.function
                    or nested_self is not captured.bound_self
                    or _function_code(nested_function) is not captured.code
                ):
                    return False
                nested_closure = _function_closure_values(nested_function)
                if _BUILTIN_LEN(nested_closure) != _BUILTIN_LEN(captured.closure):
                    return False
                nested_index = 0
                while nested_index < _BUILTIN_LEN(nested_closure):
                    if nested_closure[nested_index] is not captured.closure[nested_index]:
                        return False
                    nested_index += 1
            index += 1
        return True

    def current_exact_size(self) -> int | None:
        """Return trusted cardinality, refreshing retained exact built-ins when mutable."""
        if not self._factory_is_pristine():
            return None
        if self._live_size_data is not _NO_LIVE_SIZE:
            return len(self._live_size_data)
        return self.capabilities.exact_size

    def current_facts(self) -> StreamFacts:
        """Refresh cardinality facts for retained built-ins while preserving source semantics."""
        if not self._factory_is_pristine():
            return replace(
                self.facts,
                termination=TerminationEvidence.UNKNOWN,
                cardinality=Cardinality.unknown(),
                ordering=OrderingGuarantee.UNKNOWN,
            )
        if self._live_size_data is _NO_LIVE_SIZE:
            return self.facts
        return replace(
            self.facts,
            termination=TerminationEvidence.PROVEN_FINITE,
            cardinality=Cardinality.exact(len(self._live_size_data)),
        )

    def retained_sequence(self) -> list[Any] | tuple[Any, ...] | range | None:
        """Return an exact sequence retained by ``from_iterable``, if this source owns one."""
        if not self._factory_is_pristine():
            return None
        retained = self._retained_sequence_data
        return retained if retained is self._native_data else None

    @property
    def native_data(self) -> Any:
        """Return native metadata only while it still describes the live factory."""
        return self._native_data if self._factory_is_pristine() else None

    @native_data.setter
    def native_data(self, value: Any) -> None:
        """Replace native metadata without changing source-factory provenance."""
        self._native_data = value

    @classmethod
    def defer(
        cls, factory: Callable[[], Iterable[T]], *, facts: StreamFacts | None = None
    ) -> Source[T]:
        """Create a reopenable source whose factory is invoked for each evaluation."""
        return cls(
            lambda: iter(factory()),
            SourceCapabilities(reiterable=True, exact_size=None),
            facts=facts
            or facts_from_capabilities(
                reiterable=True,
                exact_size=None,
                ordered=True,
                reopenable=True,
            ),
        )

    def open(self) -> Iterator[T]:
        """Claim the source when necessary, then create its Python iterator."""
        from ..runtime.failpoints import hit

        self._claim()
        iterator = self._factory()
        try:
            hit("source.open.after")
        except BaseException as error:
            from ..runtime.iterators import close_iterators

            close_iterators((iterator,), active_error=error)
            raise
        return iterator

    def _claim(self) -> None:
        """Atomically reject a second evaluation of a non-reiterable source."""
        if self.capabilities.reiterable:
            return
        with self._lock:
            if self._claimed:
                raise FlowConsumedError(
                    "This flow wraps a one-shot source that has already been consumed. "
                    "Use flow.defer(factory) to create a fresh source per evaluation."
                )
            self._claimed = True

    def open_native(self, expected_type: type[Any]) -> Any:
        """Claim and return retained native data after validating its expected container type."""
        native_data = self.native_data
        if not isinstance(native_data, expected_type):
            raise TypeError(f"source does not provide {expected_type.__name__} native data")
        self._claim()
        return native_data


# Consumers can load lazily, so method provenance must be captured in this eager owner module.
_CANONICAL_SOURCE_OPEN = Source.open
_CANONICAL_SOURCE_OPEN_CODE = Source.open.__code__
_CANONICAL_SOURCE_OPEN_NATIVE = Source.open_native
_CANONICAL_SOURCE_OPEN_NATIVE_CODE = Source.open_native.__code__
_CANONICAL_SOURCE_NATIVE_DATA = Source.native_data
_CANONICAL_SOURCE_CLAIM = Source._claim
_CANONICAL_SOURCE_CLAIM_CODE = Source._claim.__code__
_CANONICAL_SOURCE_CURRENT_EXACT_SIZE = Source.current_exact_size
_CANONICAL_SOURCE_CURRENT_EXACT_SIZE_CODE = Source.current_exact_size.__code__
_CANONICAL_RETAINED_SEQUENCE = Source.retained_sequence
