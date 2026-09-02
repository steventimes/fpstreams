"""Direct identity terminals for retained synchronous AsyncFlow sources."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable
from typing import Any, cast

from ..planning.async_ import AsyncLogicalPlan, _AsyncSource
from ..planning.async_utils import closing_async_iterators

_RETAINED_TYPES = (list, tuple, range)


async def _consume_opened_identity(iterator: AsyncIterator[Any], terminal: str) -> Any:
    """Run one identity terminal after ownership has already opened its source."""
    if terminal == "list":
        return [item async for item in iterator]
    if terminal == "tuple":
        return tuple([item async for item in iterator])
    count = 0
    async for _item in iterator:
        count += 1
    return count


async def _consume_retained_identity_terminal(
    flow: Any,
    plan: AsyncLogicalPlan[Any],
    source: _AsyncSource[Any],
    retained: list[Any] | tuple[Any, ...] | range,
    terminal: str,
) -> Any:
    """Open once, then answer one retained identity terminal when the source is unchanged."""
    from ..runtime.failpoints import has_active_failpoints
    from ..runtime.report import _record_async_plan, _record_direct_strategy

    iterator = source.open()
    direct = False
    async with closing_async_iterators((iterator,)):
        live_retained = source.retained_sequence()
        if (
            live_retained is not retained
            or flow._logical_plan is not plan
            or plan.source is not source
            or plan.operations
            or has_active_failpoints()
        ):
            value = await _consume_opened_identity(iterator, terminal)
        elif terminal == "list":
            value = list(retained)
            direct = True
        elif terminal == "tuple":
            value = tuple(list(retained))
            direct = True
        else:
            exact_size = source.current_exact_size()
            if exact_size is None:
                value = await _consume_opened_identity(iterator, terminal)
            else:
                value = exact_size
                direct = True

    if direct:
        _record_direct_strategy(
            None,
            "python_direct",
            "an exact retained synchronous sequence answered the async identity terminal",
        )
    else:
        _record_async_plan()
    return value


def try_retained_identity_terminal(
    flow: Any,
    terminal: str,
) -> Awaitable[Any] | None:
    """Return deferred direct work for an exact retained synchronous sequence."""
    plan = flow._logical_plan
    if type(plan) is not AsyncLogicalPlan or type(plan.operations) is not tuple or plan.operations:
        return None
    source = plan.source
    if type(source) is not _AsyncSource:
        return None
    retained = source._retained_sequence
    if source._opener is not source._retained_opener or type(retained) not in _RETAINED_TYPES:
        return None
    if terminal not in {"count", "list", "tuple"}:
        return None
    retained_values = cast(list[Any] | tuple[Any, ...] | range, retained)
    from ..runtime.failpoints import has_active_failpoints

    if has_active_failpoints():
        return None
    return _consume_retained_identity_terminal(
        flow,
        plan,
        source,
        retained_values,
        terminal,
    )
