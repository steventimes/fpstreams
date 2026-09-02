"""Immutable summaries for one explicitly observed eager terminal execution."""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ExecutionReport:
    """Sanitized plan and query-owned resource metrics for one terminal call.

    Task counts cover only asyncio tasks owned by ``QueryRuntime``. File counts
    cover only SpillStore handles, and spill bytes are cumulative framed payload
    bytes written rather than current disk usage.
    """

    terminal: str
    requested_engine: str
    compiler_engine: str
    strategy: str
    reason: str
    elapsed_ns: int
    peak_owned_async_tasks: int
    peak_spill_files: int
    spill_bytes_written: int


@dataclass(frozen=True, slots=True)
class ExecutionResult(Generic[T]):
    """Pair one eager terminal value with its immutable execution report."""

    value: T
    report: ExecutionReport


class _ExecutionRecorder:
    """Collect bounded scalar evidence without retaining plans, sources, or values."""

    __slots__ = (
        "_active",
        "_compiler_engine",
        "_default_requested_engine",
        "_has_plan",
        "_peak_owned_async_tasks",
        "_peak_spill_files",
        "_primary_plan_identity",
        "_reason",
        "_requested_engine",
        "_spill_bytes_written",
        "_strategy",
        "terminal",
    )

    def __init__(self, terminal: str, default_requested_engine: str) -> None:
        self.terminal = terminal
        self._active = True
        self._default_requested_engine = default_requested_engine
        self._has_plan = False
        self._requested_engine = ""
        self._compiler_engine = ""
        self._strategy = ""
        self._reason = ""
        self._primary_plan_identity: int | None = None
        self._peak_owned_async_tasks = 0
        self._peak_spill_files = 0
        self._spill_bytes_written = 0

    def record_sync_plan(self, physical: Any) -> None:
        """Copy the already-selected synchronous plan decision once."""
        planned = str(physical.decision.selected_engine)
        reason = str(physical.decision.reason)
        self._record_plan(
            self._default_requested_engine,
            planned,
            f"planned:{planned}",
            reason,
            plan_identity=id(physical),
        )

    def record_async_plan(self) -> None:
        """Record the current single-process async scheduler without retaining its plan."""
        self._record_plan(
            "async",
            "async",
            "async_scheduler",
            "query uses the bounded async scheduler",
        )

    def record_metadata(self, requested_engine: str, reason: str) -> None:
        """Override a compatible compiler choice with a no-scan metadata strategy."""
        if not self._active or self._has_plan:
            return
        self._has_plan = True
        self._requested_engine = requested_engine
        self._compiler_engine = "not_compiled"
        self._strategy = "metadata"
        self._reason = reason

    def record_direct_strategy(
        self,
        physical: Any | None,
        strategy: str,
        reason: str,
    ) -> None:
        """Record a proven direct route only for this report's outer physical plan."""
        if not self._active:
            return
        if physical is None:
            if self._has_plan:
                return
            self._record_plan(
                self._default_requested_engine,
                "not_compiled",
                strategy,
                reason,
            )
            return
        if self._primary_plan_identity == id(physical):
            self._strategy = strategy
            self._reason = reason

    def _record_plan(
        self,
        requested: str,
        compiler_engine: str,
        strategy: str,
        reason: str,
        *,
        plan_identity: int | None = None,
    ) -> None:
        # Composite sources may execute child physical plans inside one public terminal.
        # The first plan is the outer request; later plans must never alter its semantics.
        if not self._active or self._has_plan:
            return
        self._has_plan = True
        self._requested_engine = requested
        self._compiler_engine = compiler_engine
        self._strategy = strategy
        self._reason = reason
        self._primary_plan_identity = plan_identity

    def record_metrics(self, metrics: Any) -> None:
        """Merge one closed runtime using maxima for peaks and a sum for bytes written."""
        if not self._active:
            return
        self._peak_owned_async_tasks = max(
            self._peak_owned_async_tasks,
            int(metrics.high_water_tasks),
        )
        self._peak_spill_files = max(
            self._peak_spill_files,
            int(metrics.high_water_open_files),
        )
        self._spill_bytes_written += int(metrics.spill_bytes)

    def finish(self, value: T, elapsed_ns: int) -> ExecutionResult[T]:
        """Freeze the successful execution after all runtime cleanup has completed."""
        if not self._has_plan:
            if self.terminal == "count":
                self.record_metadata(
                    self._default_requested_engine,
                    "trusted exact source cardinality answered the terminal without a scan",
                )
            elif self.terminal in {"any", "none"}:
                self._record_plan(
                    self._default_requested_engine,
                    "not_compiled",
                    "python_direct",
                    "opaque predicate uses the direct Python identity pipeline",
                )
            elif self.terminal == "collect":
                self._record_plan(
                    self._default_requested_engine,
                    "not_compiled",
                    "dynamic_collector",
                    "collector returned without consuming the input pipeline",
                )
            else:
                raise RuntimeError(
                    "run_with_report terminal completed without an observable execution plan"
                )
        return ExecutionResult(
            value,
            ExecutionReport(
                terminal=self.terminal,
                requested_engine=self._requested_engine,
                compiler_engine=self._compiler_engine,
                strategy=self._strategy,
                reason=self._reason,
                elapsed_ns=elapsed_ns,
                peak_owned_async_tasks=self._peak_owned_async_tasks,
                peak_spill_files=self._peak_spill_files,
                spill_bytes_written=self._spill_bytes_written,
            ),
        )

    def deactivate(self) -> None:
        """Ignore work inherited by tasks that outlive the observed terminal."""
        self._active = False

    @property
    def active(self) -> bool:
        """Return whether this recorder still owns its original terminal scope."""
        return self._active


_ACTIVE_RECORDER: ContextVar[_ExecutionRecorder | None] = ContextVar(
    "fpstreams_execution_recorder",
    default=None,
)


def _start_recording(
    terminal: str,
    requested_engine: str,
) -> tuple[_ExecutionRecorder, Token[_ExecutionRecorder | None]]:
    """Install an execution-local recorder and return its reset token."""
    recorder = _ExecutionRecorder(terminal, requested_engine)
    return recorder, _ACTIVE_RECORDER.set(recorder)


def _stop_recording(token: Token[_ExecutionRecorder | None]) -> None:
    """Restore the enclosing reporting scope even after terminal failure."""
    recorder = _ACTIVE_RECORDER.get()
    if recorder is not None:
        recorder.deactivate()
    _ACTIVE_RECORDER.reset(token)


def _record_sync_plan(physical: Any) -> None:
    """Copy a synchronous plan only when explicit reporting is active."""
    recorder = _ACTIVE_RECORDER.get()
    if recorder is not None and recorder.active:
        recorder.record_sync_plan(physical)


def _record_async_plan() -> None:
    """Record one async physical execution only when reporting is active."""
    recorder = _ACTIVE_RECORDER.get()
    if recorder is not None and recorder.active:
        recorder.record_async_plan()


def _record_direct_strategy(
    physical: Any | None,
    strategy: str,
    reason: str,
) -> None:
    """Record a direct terminal route without retaining its physical plan."""
    recorder = _ACTIVE_RECORDER.get()
    if recorder is not None and recorder.active:
        recorder.record_direct_strategy(physical, strategy, reason)


def _current_recorder() -> _ExecutionRecorder | None:
    """Return the active recorder for a newly created query runtime."""
    recorder = _ACTIVE_RECORDER.get()
    return recorder if recorder is not None and recorder.active else None
