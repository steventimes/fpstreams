"""Stateful gatherer protocols for custom intermediate operations."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast, overload

T = TypeVar("T")
S = TypeVar("S")
R = TypeVar("R")
RR = TypeVar("RR")


def _finish_empty(state: object) -> tuple[()]:
    return ()


def _finish_push_empty(state: object, downstream: Downstream[object]) -> None:
    return None


def _initialize_stateless() -> None:
    return None


class Downstream(Generic[R]):
    """A short-circuit-aware output channel owned by a Gatherer."""

    __slots__ = ("_push", "_rejecting")

    def __init__(self, push: Callable[[R], bool], *, rejecting: bool = False) -> None:
        if not callable(push):
            raise TypeError("push must be callable")
        self._push = push
        self._rejecting = rejecting

    def push(self, value: R) -> bool:
        """Emit value and return whether the downstream accepts more output.

        Args:
            value: The value consumed by this operation.

        Returns:
            Whether the condition described above is true.
        """
        if self._rejecting:
            return False
        accepted = self._push(value)
        if type(accepted) is not bool:
            raise TypeError("downstream push callback must return a bool")
        if not accepted:
            self._rejecting = True
        return accepted

    def is_rejecting(self) -> bool:
        """Return whether the downstream has stopped accepting output.

        Returns:
            Whether the condition described above is true.
        """
        return self._rejecting


LegacyIntegrator = Callable[[S, T], Iterable[R]]
PushIntegrator = Callable[[S, T, Downstream[R]], bool]
LegacyFinisher = Callable[[S], Iterable[R]]
PushFinisher = Callable[[S, Downstream[R]], None]
Combiner = Callable[[S, S], S]


@dataclass(slots=True)
class _CompositeState:
    left: Any
    right: Any
    left_proceed: bool = True
    right_proceed: bool = True


@dataclass(frozen=True, slots=True, init=False)
class Gatherer(Generic[T, S, R]):
    """A stateful intermediate operation that can emit zero or more values."""

    initializer: Callable[[], S]
    integrator: LegacyIntegrator[S, T, R] | PushIntegrator[S, T, R]
    finisher: LegacyFinisher[S, R] | PushFinisher[S, R]
    combiner: Combiner[S] | None
    greedy: bool
    _push_mode: bool

    def __init__(
        self,
        initializer: Callable[[], S],
        integrator: LegacyIntegrator[S, T, R],
        finisher: LegacyFinisher[S, R] = _finish_empty,
    ) -> None:
        self._set_fields(
            initializer=initializer,
            integrator=integrator,
            finisher=finisher,
            combiner=None,
            greedy=True,
            push_mode=False,
        )

    def _set_fields(
        self,
        *,
        initializer: Callable[[], S],
        integrator: LegacyIntegrator[S, T, R] | PushIntegrator[S, T, R],
        finisher: LegacyFinisher[S, R] | PushFinisher[S, R],
        combiner: Combiner[S] | None,
        greedy: bool,
        push_mode: bool,
    ) -> None:
        values = {
            "initializer": initializer,
            "integrator": integrator,
            "finisher": finisher,
        }
        for name, value in values.items():
            if not callable(value):
                raise TypeError(f"{name} must be callable")
        if combiner is not None and not callable(combiner):
            raise TypeError("combiner must be callable")
        if type(greedy) is not bool:
            raise TypeError("greedy must be a bool")
        object.__setattr__(self, "initializer", initializer)
        object.__setattr__(self, "integrator", integrator)
        object.__setattr__(self, "finisher", finisher)
        object.__setattr__(self, "combiner", combiner)
        object.__setattr__(self, "greedy", greedy)
        object.__setattr__(self, "_push_mode", push_mode)

    @overload
    @classmethod
    def of(
        cls,
        initializer_or_integrator: PushIntegrator[None, T, R],
        *,
        finisher: PushFinisher[None, R] | None = None,
        combiner: Combiner[None] | None = None,
        greedy: bool = False,
    ) -> Gatherer[T, None, R]: ...

    @overload
    @classmethod
    def of(
        cls,
        initializer_or_integrator: Callable[[], S],
        integrator: PushIntegrator[S, T, R],
        *,
        finisher: PushFinisher[S, R] | None = None,
        combiner: Combiner[S] | None = None,
        greedy: bool = False,
    ) -> Gatherer[T, S, R]: ...

    @classmethod
    def of(
        cls,
        initializer_or_integrator: Callable[..., Any],
        integrator: Callable[..., Any] | None = None,
        *,
        finisher: Callable[..., Any] | None = None,
        combiner: Callable[..., Any] | None = None,
        greedy: bool = False,
    ) -> Gatherer[Any, Any, Any]:
        """Build a push-based gatherer with optional state, finishing, and combining.

        Args:
            initializer_or_integrator: A state initializer, or the integrator when no
                initializer is required.
            integrator: A callable that consumes one item and may emit downstream values.
            finisher: A callable that converts accumulated state into the final result.
            combiner: A callable that combines two partial gatherer states.
            greedy: Whether the integrator should continue after downstream rejection.

        Returns:
            A reusable `Gatherer` implementing the described stateful operation.
        """
        if integrator is None:
            initializer: Callable[[], Any] = _initialize_stateless
            push_integrator = cast("PushIntegrator[Any, Any, Any]", initializer_or_integrator)
        else:
            initializer = cast(Callable[[], Any], initializer_or_integrator)
            push_integrator = cast("PushIntegrator[Any, Any, Any]", integrator)
        push_finisher = cast(
            "PushFinisher[Any, Any]",
            _finish_push_empty if finisher is None else finisher,
        )
        state_combiner = cast("Combiner[Any] | None", combiner)
        gatherer: Gatherer[Any, Any, Any] = object.__new__(cls)
        gatherer._set_fields(
            initializer=initializer,
            integrator=push_integrator,
            finisher=push_finisher,
            combiner=state_combiner,
            greedy=greedy,
            push_mode=True,
        )
        return gatherer

    @overload
    @classmethod
    def of_sequential(
        cls,
        initializer_or_integrator: PushIntegrator[None, T, R],
        *,
        finisher: PushFinisher[None, R] | None = None,
        greedy: bool = False,
    ) -> Gatherer[T, None, R]: ...

    @overload
    @classmethod
    def of_sequential(
        cls,
        initializer_or_integrator: Callable[[], S],
        integrator: PushIntegrator[S, T, R],
        *,
        finisher: PushFinisher[S, R] | None = None,
        greedy: bool = False,
    ) -> Gatherer[T, S, R]: ...

    @classmethod
    def of_sequential(
        cls,
        initializer_or_integrator: Callable[..., Any],
        integrator: Callable[..., Any] | None = None,
        *,
        finisher: Callable[..., Any] | None = None,
        greedy: bool = False,
    ) -> Gatherer[Any, Any, Any]:
        """Build a push-based gatherer whose state cannot be combined in parallel.

        Args:
            initializer_or_integrator: A state initializer, or the integrator when no
                initializer is required.
            integrator: A callable that consumes one item and may emit downstream values.
            finisher: A callable that converts accumulated state into the final result.
            greedy: Whether the integrator should continue after downstream rejection.

        Returns:
            A reusable `Gatherer` implementing the described stateful operation.
        """
        if integrator is None:
            return cls.of(
                initializer_or_integrator,
                finisher=finisher,
                combiner=None,
                greedy=greedy,
            )
        return cls.of(
            initializer_or_integrator,
            integrator,
            finisher=finisher,
            combiner=None,
            greedy=greedy,
        )

    def _integrate(self, state: S, item: T, downstream: Downstream[R]) -> bool:
        if self._push_mode:
            integrate = cast("PushIntegrator[S, T, R]", self.integrator)
            proceed = integrate(state, item, downstream)
            if type(proceed) is not bool:
                raise TypeError("gatherer integrator must return a bool")
            return proceed and not downstream.is_rejecting()

        integrate_legacy = cast("LegacyIntegrator[S, T, R]", self.integrator)
        return all(downstream.push(value) for value in integrate_legacy(state, item))

    def _finish(self, state: S, downstream: Downstream[R]) -> None:
        if self._push_mode:
            finish = cast("PushFinisher[S, R]", self.finisher)
            finish(state, downstream)
            return

        finish_legacy = cast("LegacyFinisher[S, R]", self.finisher)
        for value in finish_legacy(state):
            if not downstream.push(value):
                return

    def and_then(self, other: Gatherer[R, Any, RR]) -> Gatherer[T, Any, RR]:
        """Compose this gatherer with another gatherer without an intermediate collection.

        Args:
            other: The other iterable, flow, value, or fallback used by the operation.

        Returns:
            A reusable `Gatherer` implementing the described stateful operation.
        """
        if not isinstance(other, Gatherer):
            raise TypeError("other must be a Gatherer")

        def initialize() -> _CompositeState:
            return _CompositeState(self.initializer(), other.initializer())

        def integrate(
            state: _CompositeState,
            item: T,
            downstream: Downstream[RR],
        ) -> bool:
            if not state.left_proceed or not state.right_proceed or downstream.is_rejecting():
                return False

            def push_right(value: R) -> bool:
                if not state.right_proceed or downstream.is_rejecting():
                    state.right_proceed = False
                    return False
                state.right_proceed = other._integrate(state.right, value, downstream)
                return state.right_proceed

            bridge = Downstream(push_right, rejecting=not state.right_proceed)
            state.left_proceed = self._integrate(state.left, item, bridge)
            return state.left_proceed and state.right_proceed and not downstream.is_rejecting()

        def finish(state: _CompositeState, downstream: Downstream[RR]) -> None:
            def push_right(value: R) -> bool:
                if not state.right_proceed or downstream.is_rejecting():
                    state.right_proceed = False
                    return False
                state.right_proceed = other._integrate(state.right, value, downstream)
                return state.right_proceed

            bridge = Downstream(
                push_right,
                rejecting=not state.right_proceed or downstream.is_rejecting(),
            )
            self._finish(state.left, bridge)
            other._finish(state.right, downstream)

        def combine(left: _CompositeState, right: _CompositeState) -> _CompositeState:
            left_combiner = self.combiner
            right_combiner = other.combiner
            if left_combiner is None or right_combiner is None:
                raise RuntimeError("sequential gatherers cannot combine state")
            return _CompositeState(
                left_combiner(left.left, right.left),
                right_combiner(left.right, right.right),
                left.left_proceed and right.left_proceed,
                left.right_proceed and right.right_proceed,
            )

        combiner = combine if self.combiner is not None and other.combiner is not None else None
        return Gatherer.of(
            initialize,
            integrate,
            finisher=finish,
            combiner=combiner,
            greedy=self.greedy and other.greedy,
        )
