"""Composition root for query-scoped resource, spill, and task ownership."""

from __future__ import annotations

import asyncio
from typing import Literal

from .files import FileManager
from .limits import QueryLimits
from .metrics import QueryMetrics
from .report import _current_recorder
from .resources import ResourceRegistry, _add_cleanup_failure
from .spill import SpillFileRegistry
from .tasks import TaskRuntime, _await_cleanup_task, _start_cleanup_task


class QueryRuntime:
    """Own all resources acquired while one compiled query is executing."""

    def __init__(self, limits: QueryLimits | None = None) -> None:
        self.limits = limits or QueryLimits()
        self.metrics = QueryMetrics()
        self.resources = ResourceRegistry()
        self.files = FileManager(self.limits, self.metrics, self.resources)
        self.spills = SpillFileRegistry(self.resources, self.metrics)
        self.tasks = TaskRuntime(self.limits, self.metrics)
        self._closed = False
        self._close_task: asyncio.Future[None] | None = None
        self._report_recorder = _current_recorder()

    def __enter__(self) -> QueryRuntime:
        """Enter synchronous runtime ownership."""
        return self

    def __exit__(
        self, exc_type: object, exc: BaseException | None, traceback: object
    ) -> Literal[False]:
        """Close resources while preserving any active pipeline error."""
        self.close(exc)
        return False

    async def __aenter__(self) -> QueryRuntime:
        """Enter asynchronous runtime ownership."""
        return self

    async def __aexit__(
        self, exc_type: object, exc: BaseException | None, traceback: object
    ) -> Literal[False]:
        """Asynchronously close task and resource ownership."""
        await self.aclose(exc)
        return False

    def close(self, active_error: BaseException | None = None) -> None:
        """Close synchronous ownership once."""
        if self._closed:
            return
        self._closed = True
        try:
            self.files.close()
            self.resources.close(active_error)
        finally:
            self._record_report_metrics()

    async def aclose(self, active_error: BaseException | None = None) -> None:
        """Close task and resource ownership once."""
        if self.tasks._admitting or self.tasks._owns(asyncio.current_task()):
            raise RuntimeError("owned task cannot close its query runtime")
        if self._close_task is None:
            if self._closed:
                return
            close_task = _start_cleanup_task(self._aclose_impl())
            self._close_task = close_task
            self._closed = True
        try:
            await _await_cleanup_task(self._close_task)
        except BaseException as error:
            if active_error is None:
                raise
            summary = f"cleanup failed: {type(error).__name__}: {error}"
            notes = getattr(active_error, "__notes__", ()) or ()
            if summary not in notes:
                _add_cleanup_failure(active_error, [error])
        finally:
            self._record_report_metrics()

    async def _aclose_impl(self) -> None:
        """Run every asynchronous cleanup phase under one cancellation-safe owner."""
        self.files.close()
        await self.tasks._aclose_from_owner()
        await self.resources.aclose()

    def _record_report_metrics(self) -> None:
        """Copy metrics to the execution-local recorder at most once."""
        recorder = self._report_recorder
        if recorder is None:
            return
        self._report_recorder = None
        recorder.record_metrics(self.metrics)
