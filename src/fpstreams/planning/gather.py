"""Define legacy and push-based stateful gatherers with downstream short-circuiting."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast, overload

T = TypeVar("T")
S = TypeVar("S")
R = TypeVar("R")
RR = TypeVar("RR")


def _finish_empty(state: object) -> tuple[()]:
    """Provide the legacy default finisher, which emits no trailing values."""
    return ()


def _finish_push_empty(state: object, downstream: Downstream[object]) -> None:
    """Provide the push-mode default finisher, which emits nothing."""
    return None


def _initialize_stateless() -> None:
    """Initialize a stateless push gatherer with ``None`` state."""
    return None


class Downstream(Generic[R]):
    """Forward gatherer output while making downstream rejection permanent."""

    __slots__ = ("_push", "_rejecting")

    def __init__(self, push: Callable[[R], bool], *, rejecting: bool = False) -> None:
        """Validate the callback and store it with the initial rejection state."""
        if not callable(push):
            raise TypeError("push must be callable")
        self._push = push
        self._rejecting = rejecting

    def push(self, value: R) -> bool:
        """Offer one value to the callback and latch rejection when it returns ``False``.

        Once rejecting, the channel returns ``False`` without invoking the callback. Callback
        results must be actual booleans so short-circuit behavior cannot depend on truthiness.
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
        """Return the latched downstream rejection state."""
        return self._rejecting


LegacyIntegrator = Callable[[S, T], Iterable[R]]
PushIntegrator = Callable[[S, T, Downstream[R]], bool]
LegacyFinisher = Callable[[S], Iterable[R]]
PushFinisher = Callable[[S, Downstream[R]], None]
Combiner = Callable[[S, S], S]


@dataclass(slots=True)
class _CompositeState:
    """Hold both composed gatherer states and their independent proceed flags."""

    left: Any
    right: Any
    left_proceed: bool = True
    right_proceed: bool = True


@dataclass(frozen=True, slots=True, init=False)
class Gatherer(Generic[T, S, R]):
    """Describe a stateful intermediate operation in legacy iterable or push-callback form."""

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
        """Construct the legacy iterable-emitting form with no combiner and greedy metadata."""
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
        """Validate callbacks and metadata, then populate the frozen gatherer fields internally."""
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
    ) -> Gatherer[T, None, R]:
        """Type the stateless push form whose sole callable is the integrator."""
        ...

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
    ) -> Gatherer[T, S, R]:
        """Type the stateful push form with separate initializer and integrator callables."""
        ...

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
        """Build a push-mode gatherer in stateless or explicitly initialized form.

        Omitting ``integrator`` makes the first callable a stateless integrator with ``None``
        state. The finisher defaults to no output. ``combiner`` and ``greedy`` are retained
        as metadata for state composition; sequential integration still stops on rejection.
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
    ) -> Gatherer[T, None, R]:
        """Type the stateless push form with no state combiner."""
        ...

    @overload
    @classmethod
    def of_sequential(
        cls,
        initializer_or_integrator: Callable[[], S],
        integrator: PushIntegrator[S, T, R],
        *,
        finisher: PushFinisher[S, R] | None = None,
        greedy: bool = False,
    ) -> Gatherer[T, S, R]:
        """Type the initialized push form with no state combiner."""
        ...

    @classmethod
    def of_sequential(
        cls,
        initializer_or_integrator: Callable[..., Any],
        integrator: Callable[..., Any] | None = None,
        *,
        finisher: Callable[..., Any] | None = None,
        greedy: bool = False,
    ) -> Gatherer[Any, Any, Any]:
        """Build the push-mode form through ``of`` while always storing no state combiner."""
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
        """Integrate one item and report whether both gatherer and downstream may continue.

        Push integrators must return a real boolean. Legacy integrators yield values that are
        forwarded only until downstream rejects them.
        """
        if self._push_mode:
            integrate = cast("PushIntegrator[S, T, R]", self.integrator)
            proceed = integrate(state, item, downstream)
            if type(proceed) is not bool:
                raise TypeError("gatherer integrator must return a bool")
            return proceed and not downstream.is_rejecting()

        integrate_legacy = cast("LegacyIntegrator[S, T, R]", self.integrator)
        return all(downstream.push(value) for value in integrate_legacy(state, item))

    def _finish(self, state: S, downstream: Downstream[R]) -> None:
        """Run the configured finisher and stop forwarding legacy output after rejection."""
        if self._push_mode:
            finish = cast("PushFinisher[S, R]", self.finisher)
            finish(state, downstream)
            return

        finish_legacy = cast("LegacyFinisher[S, R]", self.finisher)
        for value in finish_legacy(state):
            if not downstream.push(value):
                return

    def and_then(self, other: Gatherer[R, Any, RR]) -> Gatherer[T, Any, RR]:
        """Feed this gatherer directly into ``other`` through a short-circuiting bridge.

        Both states and proceed flags are retained together. This finisher sends left-finisher
        output through the right integrator before running the right finisher. A composite state
        combiner exists only when both component gatherers provide one.
        """
        if not isinstance(other, Gatherer):
            raise TypeError("other must be a Gatherer")

        def initialize() -> _CompositeState:
            """Initialize independent left and right states for the composed gatherer."""
            return _CompositeState(self.initializer(), other.initializer())

        def integrate(
            state: _CompositeState,
            item: T,
            downstream: Downstream[RR],
        ) -> bool:
            """Integrate one left input while bridging its emissions into the right
            gatherer."""
            if not state.left_proceed or not state.right_proceed or downstream.is_rejecting():
                return False

            def push_right(value: R) -> bool:
                """Push a left emission through the right integrator and update its proceed flag."""
                if not state.right_proceed or downstream.is_rejecting():
                    state.right_proceed = False
                    return False
                state.right_proceed = other._integrate(state.right, value, downstream)
                return state.right_proceed

            bridge = Downstream(push_right, rejecting=not state.right_proceed)
            state.left_proceed = self._integrate(state.left, item, bridge)
            return state.left_proceed and state.right_proceed and not downstream.is_rejecting()

        def finish(state: _CompositeState, downstream: Downstream[RR]) -> None:
            """Finish left through the bridge, then finish right into the final downstream."""

            def push_right(value: R) -> bool:
                """Feed left-finisher output through the right integrator while it still
                accepts input."""
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
            """Combine component states and proceed only when both partial states do."""
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
