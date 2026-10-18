"""First-seen grouping with live Collector state and iterator ownership."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from ...collecting._collector_base import Collector, _never_done
from ...physical.relational import (
    GroupAggregatePhysicalNode,
)
from ..sync_ops import close_iterators


@dataclass(frozen=True)
class CollectorGroupHooks:
    """Owner namespace stays live across key hashing and Collector callbacks."""

    namespace: dict[str, Any]
    execution_unsafe: Callable[[], bool]


_BUILTIN_TYPE = type
_CANONICAL_COLLECTOR_ACCESS = (
    ("__getattribute__", object.__getattribute__),
    *(
        (name, Collector.__dict__[name])
        for name in ("initializer", "step", "done", "_lifecycle_revision")
    ),
)


def _collector_lifecycle_is_static(collector: Collector[Any, Any, Any]) -> bool:
    """Cache only inherited slot reads; dynamic getters use the collector program."""
    owner = _BUILTIN_TYPE(collector)
    if _BUILTIN_TYPE(owner) is not _BUILTIN_TYPE:
        return False
    lineage = owner.__mro__
    if any(_BUILTIN_TYPE(base) is not _BUILTIN_TYPE for base in lineage):
        return False
    for name, expected in _CANONICAL_COLLECTOR_ACCESS:
        for base in lineage:
            namespace = base.__dict__
            if name in namespace:
                if namespace[name] is not expected:
                    return False
                break
        else:
            return False
    return True


def _single_collector_lifecycle(
    collector: Collector[Any, Any, Any],
) -> tuple[
    int,
    Callable[[], Any],
    Callable[[Any, Any], Any],
    Callable[[Any], bool],
]:
    """Capture one coherent lifecycle generation after an observed replacement."""
    while True:
        revision = collector._lifecycle_revision
        initializer = collector.initializer
        step = collector.step
        done = collector.done
        if collector._lifecycle_revision == revision:
            return revision, initializer, step, done


def _execute_single_collector_group(  # noqa: C901 - lifecycle mutation boundaries stay inline
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    hooks: CollectorGroupHooks,
) -> Iterator[dict[str, Any]]:
    """Group one arbitrary collector without allocating one-element program states."""
    from ...runtime.failpoints import has_active_failpoints, hit

    program = node.aggregations.collectors
    collector = program.layout.collectors[0]
    output_name = program.layout.names[0]
    revision, initializer, step, done = _single_collector_lifecycle(collector)
    instrumented = has_active_failpoints()
    groups: dict[Any, list[Any]] = {}
    multiple = len(node.keys) > 1
    iterator = iter(values)
    select_key = node.select_key
    active_error: BaseException | None = None
    try:
        for row in iterator:
            key = select_key(row)
            # Hashing may replace a hook just installed by key selection. Refresh
            # after lookup so the cache does not extend that transient hook's life.
            try:
                (hooks.namespace["hash"] if "hash" in hooks.namespace else hash)(key)  # noqa: SIM401
                entry = groups.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            live_revision = collector._lifecycle_revision
            if live_revision != revision:
                revision, initializer, step, done = _single_collector_lifecycle(collector)
            if entry is None:
                state = initializer()
                live_revision = collector._lifecycle_revision
                if live_revision != revision:
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
                entry = [key, state, done(state) if done is not _never_done else False]
                del state
                live_revision = collector._lifecycle_revision
                if live_revision != revision:
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
                groups[key] = entry
                live_revision = collector._lifecycle_revision
                if live_revision != revision:
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
                if instrumented:
                    hit("group.state.create.after")
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
            if done is not _never_done:
                if entry[2]:
                    continue
                # Truth testing can replace the step needed for this same row.
                live_revision = collector._lifecycle_revision
                if live_revision != revision:
                    revision, initializer, step, done = _single_collector_lifecycle(collector)
            entry[1] = step(entry[1], row)
            live_revision = collector._lifecycle_revision
            if live_revision != revision:
                revision, initializer, step, done = _single_collector_lifecycle(collector)
            if done is not _never_done:
                entry[2] = done(entry[1])
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((iterator,), active_error=active_error)
    # Keep the input entry and items iterator lifetimes aligned with the general program.
    if not multiple:
        key_name = node.key_names[0]
        for _group_key, output_entry in groups.items():
            # Rebind the key before a live finisher so an unused input key is released
            # at the same point as in the general program.
            key = output_entry[0]
            result = {key_name: key}
            result[output_name] = collector.finish(output_entry[1])
            yield result
        return
    key_names = node.key_names
    for _group_key, output_entry in groups.items():
        key = output_entry[0]
        result = dict(zip(key_names, key, strict=True))
        result[output_name] = collector.finish(output_entry[1])
        yield result


def execute_collector_group(
    values: Iterator[Any],
    node: GroupAggregatePhysicalNode,
    hooks: CollectorGroupHooks,
) -> Iterator[dict[str, Any]]:
    """Run live collector lifecycles while retaining compact first-seen group state."""
    program = node.aggregations.collectors
    if (
        program.single
        and not hooks.execution_unsafe()
        and _collector_lifecycle_is_static(program.layout.collectors[0])
    ):
        yield from _execute_single_collector_group(values, node, hooks)
        return
    from ...runtime.failpoints import has_active_failpoints, hit

    groups: dict[Any, tuple[Any, Any]] = {}
    instrumented = has_active_failpoints()
    multiple = len(node.keys) > 1
    iterator = iter(values)
    active_error: BaseException | None = None
    try:
        for row in iterator:
            key = node.select_key(row)
            try:
                (hooks.namespace["hash"] if "hash" in hooks.namespace else hash)(key)  # noqa: SIM401
                entry = groups.get(key)
            except TypeError:
                raise TypeError("group_by keys must be hashable") from None
            if entry is None:
                state = program.initialize()
                groups[key] = (key, state)
                if instrumented:
                    hit("group.state.create.after")
            else:
                _first, state = entry
            program.step(state, row)
    except BaseException as error:
        active_error = error
        raise
    finally:
        close_iterators((iterator,), active_error=active_error)
    for _group_key, (key, state) in groups.items():
        values_for_key = key if multiple else (key,)
        result = dict(zip(node.key_names, values_for_key, strict=True))
        result.update(program.finish(state))
        yield result
