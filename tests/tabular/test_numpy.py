"""tabular numpy regression contracts."""

from __future__ import annotations

import gc
import math
import signal
import sys
import traceback
import weakref
from collections.abc import Callable, Iterator, Mapping
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

import fpstreams

from ._tabular_support import (
    _run_inline_python as _run_inline_python,
)


def test_direct_numpy_global_aggregate_never_opens_python_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct integer lanes must reduce the retained ndarray without forwarding or boxing."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import relational
    from fpstreams.streams import flow_terminals

    matrix = np.asarray([[9, 2], [8, -3], [7, 4], [6, 1]] * 16, dtype=np.int64)
    aggregated = fpstreams.rows.from_numpy(matrix, columns=("id", "value")).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    relation = aggregated.explain("list").to_dict()["relations"]

    assert relation["candidate"] == "numpy_reduce"
    assert relation["guarded"] is True

    def forbidden_collector(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("NumPy global aggregate must not collect Python rows")

    def forbidden_executor(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("NumPy global aggregate must not enter the forwarding runtime")

    monkeypatch.setattr(relational, "run_collector_program", forbidden_collector)
    monkeypatch.setattr(flow_terminals, "execute_physical", forbidden_executor)

    execution = aggregated.run_with_report("to_list")

    assert execution.value == [{"rows": 64, "total": 64, "low": -3, "high": 4}]
    assert execution.report.strategy == "numpy_direct"


def test_direct_numpy_global_list_preserves_exact_count_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A count-only matrix keeps the cheaper trusted-cardinality shortcut."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import relational

    aggregated = fpstreams.rows.from_numpy(
        np.arange(64, dtype=np.int64).reshape(-1, 1),
        columns=("value",),
    ).aggregate(rows=fpstreams.agg.count())

    def forbidden_numpy(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("exact NumPy count must not enter the reduction kernel")

    monkeypatch.setattr(relational, "_numpy_global_aggregate", forbidden_numpy)

    assert aggregated.to_list() == [{"rows": 64}]


@pytest.mark.parametrize("dtype_name", ["int64", "float64"])
def test_direct_numpy_global_sum_and_mean_preserve_chunked_order(
    monkeypatch: pytest.MonkeyPatch,
    dtype_name: str,
) -> None:
    """Columnar sum/mean lanes must continue one state across bounded chunks."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import relational
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    if dtype_name == "int64":
        raw = [2**53, 1, -(2**53), 3, -7, 11] * 11
    else:
        raw = [1e16, 1.0, -1e16, 3.0, -0.0, 5e-324] * 11
    matrix = np.asarray(raw, dtype=dtype_name).reshape(-1, 1)
    source = fpstreams.rows.from_numpy(matrix, columns=("value",))
    automatic = source.aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        mean=fpstreams.agg.mean("value"),
        duplicate_mean=fpstreams.agg.mean("value"),
    )
    canonical = source.with_engine("python").aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        mean=fpstreams.agg.mean("value"),
        duplicate_mean=fpstreams.agg.mean("value"),
    )
    plan = compile_query(automatic._flow._query("list"))

    assert isinstance(plan.root, GlobalAggregatePhysicalNode)
    assert plan.root.numpy_global is not None
    expected = canonical.to_list()

    def forbidden_collector(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("NumPy sum/mean must not collect Python rows")

    monkeypatch.setattr(relational, "_NUMPY_GLOBAL_CHUNK_ROWS", 7)
    monkeypatch.setattr(relational, "run_collector_program", forbidden_collector)

    assert automatic.to_list() == expected


def test_compiled_numpy_mean_revalidates_statistics_globals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A compiled mean must deopt when OnlineStatistics would resolve new globals."""
    import builtins

    np = pytest.importorskip("numpy")
    from fpstreams.collecting.aggregation import native_group_aggregation
    from fpstreams.collecting.statistics import OnlineStatistics
    from fpstreams.execution.physical import execute_physical
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    mean = fpstreams.agg.mean("value")
    source = fpstreams.rows.from_numpy(
        np.arange(64, dtype=np.int64).reshape(-1, 1),
        columns=("value",),
    )
    automatic = source.aggregate(mean=mean)
    canonical = source.with_engine("python").aggregate(mean=mean)
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    assert isinstance(automatic_plan.root, GlobalAggregatePhysicalNode)
    assert automatic_plan.root.numpy_global is not None

    def doubled_float(value: object) -> float:
        return builtins.float(value) * 2.0

    monkeypatch.setitem(OnlineStatistics.accept.__globals__, "float", doubled_float)

    assert native_group_aggregation(mean) is None
    assert (
        list(execute_physical(automatic_plan))
        == list(execute_physical(canonical_plan))
        == [{"mean": 63.0}]
    )


@pytest.mark.parametrize("changed_dtype", ["int32", ">i8"])
def test_compiled_numpy_global_mean_revalidates_live_dtype(
    changed_dtype: str,
) -> None:
    """A retained matrix must still satisfy the planner's exact mean layout at execution."""
    import warnings

    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.planning.compiler import compile_query

    matrix = np.arange(32, dtype=np.int64).reshape(-1, 1)
    source = fpstreams.rows.from_numpy(matrix, columns=("value",))
    automatic = source.aggregate(
        total=fpstreams.agg.sum("value"),
        mean=fpstreams.agg.mean("value"),
    )
    canonical = source.with_engine("python").aggregate(
        total=fpstreams.agg.sum("value"),
        mean=fpstreams.agg.mean("value"),
    )
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        matrix.dtype = np.dtype(changed_dtype)
        matrix.shape = (-1, 1)

    assert list(execute_physical(automatic_plan)) == list(execute_physical(canonical_plan))


def test_compiled_numpy_integer_extrema_revalidate_float_dtype() -> None:
    """Integer min/max lanes must deopt instead of acquiring NumPy's NaN semantics."""
    import warnings

    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.planning.compiler import compile_query

    matrix = np.arange(32, dtype=np.int64).reshape(-1, 1)
    source = fpstreams.rows.from_numpy(matrix, columns=("value",))
    automatic = source.aggregate(
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    canonical = source.with_engine("python").aggregate(
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        matrix.dtype = np.float64
    matrix[:4, 0] = [1.0, np.nan, -0.0, 2.0]
    matrix[4:, 0] = 1.0

    expected = [{"low": -0.0, "high": 2.0}]
    assert list(execute_physical(canonical_plan)) == expected
    assert list(execute_physical(automatic_plan)) == expected


@pytest.mark.parametrize("mutation", ["setattr", "slot"])
def test_compiled_numpy_mean_revalidates_statistics_type_layout(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    """The direct mean lane requires the state type's original write boundaries."""
    np = pytest.importorskip("numpy")
    from fpstreams.collecting.aggregation import native_group_aggregation
    from fpstreams.collecting.statistics import OnlineStatistics
    from fpstreams.execution.physical import execute_physical
    from fpstreams.planning.compiler import compile_query

    mean = fpstreams.agg.mean("value")
    query = fpstreams.rows.from_numpy(
        np.arange(32, dtype=np.int64).reshape(-1, 1),
        columns=("value",),
    ).aggregate(mean=mean)
    plan = compile_query(query._flow._query("list"))

    if mutation == "setattr":

        def observed_setattr(_self: object, _name: str, _value: object) -> None:
            raise RuntimeError("observed statistics setattr")

        monkeypatch.setattr(OnlineStatistics, "__setattr__", observed_setattr)
        error = RuntimeError
    else:
        monkeypatch.setattr(OnlineStatistics, "total", 123.0)
        error = AttributeError

    assert native_group_aggregation(mean) is None
    with pytest.raises(error):
        list(execute_physical(plan))


def test_numpy_global_endpoint_probe_does_not_observe_live_callable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Internal Rust endpoint validation must not expose a new builtin callback boundary."""
    import builtins

    np = pytest.importorskip("numpy")
    from fpstreams import _native
    from fpstreams.execution.physical import execute_physical
    from fpstreams.planning.compiler import compile_query

    query = fpstreams.rows.from_numpy(
        np.arange(32, dtype=np.float64).reshape(-1, 1),
        columns=("value",),
    ).aggregate(total=fpstreams.agg.sum("value"), mean=fpstreams.agg.mean("value"))
    plan = compile_query(query._flow._query("list"))
    canonical_callable = builtins.callable
    endpoints = (_native.update_sum_f64_buffer_v1, _native.update_mean_f64_buffer_v1)

    def observed_callable(value: object) -> bool:
        if any(value is endpoint for endpoint in endpoints):
            raise RuntimeError("endpoint callable observed")
        return canonical_callable(value)

    monkeypatch.setattr(builtins, "callable", observed_callable)

    assert list(execute_physical(plan)) == [{"total": 496.0, "mean": 15.5}]

    def unexpected_endpoint(*_args: object) -> object:
        raise AssertionError("a replacement endpoint must not run")

    monkeypatch.setattr(_native, "update_mean_f64_buffer_v1", unexpected_endpoint)
    assert list(execute_physical(plan)) == [{"total": 496.0, "mean": 15.5}]


def test_numpy_global_aggregate_preserves_live_shape_errors() -> None:
    """Planning must not replace the canonical error for a retained array changed to 0-D."""
    np = pytest.importorskip("numpy")

    matrix = np.asarray([[2]], dtype=np.int64)
    aggregated = fpstreams.rows.from_numpy(matrix, columns=("value",)).aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
    )
    matrix.resize((), refcheck=False)

    with pytest.raises(ValueError, match="changed to 0 dimensions"):
        aggregated.to_list()


@pytest.mark.parametrize("mode", ["global", "grouped"])
@pytest.mark.parametrize("mutation", ["sum", "count"])
def test_compiled_numpy_aggregate_revalidates_live_aggregators(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    mutation: str,
) -> None:
    """A compiled NumPy choice must decline when a retained collector changes later."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.physical.relational import (
        GlobalAggregatePhysicalNode,
        GroupAggregatePhysicalNode,
    )
    from fpstreams.planning.compiler import compile_query

    rows = fpstreams.agg.count()
    total = fpstreams.agg.sum("value")
    source = fpstreams.rows.from_numpy(
        np.asarray([[index % 2, 1] for index in range(32)], dtype=np.int64),
        columns=("key", "value"),
    )
    if mode == "global":
        automatic = source.aggregate(rows=rows, total=total)
        canonical = source.with_engine("python").aggregate(rows=rows, total=total)
    else:
        automatic = source.group_by("key").aggregate(rows=rows, total=total)
        canonical = source.with_engine("python").group_by("key").aggregate(rows=rows, total=total)
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    node_type = GlobalAggregatePhysicalNode if mode == "global" else GroupAggregatePhysicalNode
    assert isinstance(automatic_plan.root, node_type)
    assert isinstance(canonical_plan.root, node_type)
    if mode == "global":
        assert automatic_plan.root.numpy_global is not None
        assert canonical_plan.root.numpy_global is None
    else:
        assert automatic_plan.root.numpy_group is not None
        assert canonical_plan.root.numpy_group is None

    if mutation == "sum":

        def step_factory() -> Callable[[int, dict[str, int]], int]:
            def select(row: dict[str, int]) -> int:
                return row["value"]

            def step(current: int, row: dict[str, int]) -> int:
                return current + select(row) * 9

            return step

        monkeypatch.setattr(total.step, "__code__", step_factory().__code__)
        expected_global = [{"rows": 32, "total": 288}]
        expected_grouped = [
            {"key": 0, "rows": 16, "total": 144},
            {"key": 1, "rows": 16, "total": 144},
        ]
    else:

        def count_step(current: int, _row: object) -> int:
            return current + 9

        monkeypatch.setattr(rows.step, "__code__", count_step.__code__)
        expected_global = [{"rows": 288, "total": 32}]
        expected_grouped = [
            {"key": 0, "rows": 144, "total": 16},
            {"key": 1, "rows": 144, "total": 16},
        ]

    expected = expected_global if mode == "global" else expected_grouped
    assert list(execute_physical(canonical_plan)) == expected
    assert list(execute_physical(automatic_plan)) == expected


def test_compiled_numpy_count_revalidates_before_exact_size_shortcut(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale count lifecycle must decline the earlier exact-cardinality shortcut."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    rows = fpstreams.agg.count()
    source = fpstreams.rows.from_numpy(
        np.ones((32, 1), dtype=np.int64),
        columns=("value",),
    )
    automatic = source.aggregate(rows=rows)
    canonical = source.with_engine("python").aggregate(rows=rows)
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    assert isinstance(automatic_plan.root, GlobalAggregatePhysicalNode)
    assert automatic_plan.root.exact_count_name == "rows"

    def count_step(current: int, _row: object) -> int:
        return current + 9

    monkeypatch.setattr(rows.step, "__code__", count_step.__code__)

    assert list(execute_physical(canonical_plan)) == [{"rows": 288}]
    assert list(execute_physical(automatic_plan)) == [{"rows": 288}]


@pytest.mark.parametrize("kind", ["min", "max"])
def test_compiled_numpy_extreme_revalidates_lifecycle_globals(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    """A live lifecycle globals change must invalidate a compiled NumPy extreme."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    extreme = getattr(fpstreams.agg, kind)("value")
    source = fpstreams.rows.from_numpy(
        np.zeros((32, 1), dtype=np.int64),
        columns=("value",),
    )
    automatic = source.aggregate(result=extreme)
    canonical = source.with_engine("python").aggregate(result=extreme)
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    assert isinstance(automatic_plan.root, GlobalAggregatePhysicalNode)
    assert automatic_plan.root.numpy_global is not None

    monkeypatch.setitem(extreme.finish.__globals__, "_MISSING", 0)

    assert list(execute_physical(canonical_plan)) == [{"result": None}]
    assert list(execute_physical(automatic_plan)) == [{"result": None}]


def test_numpy_global_aggregate_deopts_after_source_factory_code_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retained NumPy metadata must not outlive the Python opener that describes it."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.physical import execute_physical
    from fpstreams.physical.relational import GlobalAggregatePhysicalNode
    from fpstreams.planning.compiler import compile_query

    table = fpstreams.rows.from_numpy(
        np.ones((32, 1), dtype=np.int64),
        columns=("value",),
    )
    source = table._flow._pipeline.source
    automatic = table.aggregate(total=fpstreams.agg.sum("value"))
    canonical = table.with_engine("python").aggregate(total=fpstreams.agg.sum("value"))
    automatic_plan = compile_query(automatic._flow._query("list"))
    canonical_plan = compile_query(canonical._flow._query("list"))

    assert isinstance(automatic_plan.root, GlobalAggregatePhysicalNode)
    assert automatic_plan.root.numpy_global is not None

    def replacement_factory() -> Callable[[], Iterator[dict[str, int]]]:
        names = ("value",)
        values = [9] * 32

        def replacement() -> Iterator[dict[str, int]]:
            return iter({names[0]: 9} for _index in range(32 if values is not None else 0))

        return replacement

    replacement = replacement_factory()
    monkeypatch.setattr(source._factory, "__code__", replacement.__code__)

    assert "candidate" not in automatic.explain("list").to_dict()["relations"]
    assert (
        list(execute_physical(automatic_plan))
        == list(execute_physical(canonical_plan))
        == [{"total": 288}]
    )


@pytest.mark.parametrize("mode", ["global", "grouped"])
@pytest.mark.parametrize(
    "mutation",
    ["selector_closure", "selector_code", "selector_inner_closure", "step_code"],
)
def test_numpy_aggregates_deopt_after_mutated_group_sum_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    mutation: str,
) -> None:
    """A mutated factory sum must execute its live Python selector and step."""
    np = pytest.importorskip("numpy")

    total = fpstreams.agg.sum("value")
    closure = dict(
        zip(
            total.step.__code__.co_freevars,
            total.step.__closure__ or (),
            strict=True,
        )
    )
    if mutation == "selector_closure":
        monkeypatch.setattr(
            closure["select"],
            "cell_contents",
            lambda row: row["value"] * 3,
        )
        expected_global = [{"total": 96}]
        expected_grouped = [
            {"key": 0, "total": 48},
            {"key": 1, "total": 48},
        ]
    elif mutation == "selector_code":

        def selector_factory() -> Callable[[dict[str, int]], int]:
            selector = "value"

            def replacement(row: dict[str, int]) -> int:
                return row[selector] * 4

            return replacement

        replacement_selector = selector_factory()
        select = closure["select"].cell_contents
        monkeypatch.setattr(select, "__code__", replacement_selector.__code__)
        expected_global = [{"total": 128}]
        expected_grouped = [
            {"key": 0, "total": 64},
            {"key": 1, "total": 64},
        ]
    elif mutation == "selector_inner_closure":
        select = closure["select"].cell_contents
        select_closure = dict(
            zip(
                select.__code__.co_freevars,
                select.__closure__ or (),
                strict=True,
            )
        )
        monkeypatch.setattr(select_closure["selector"], "cell_contents", "other")
        expected_global = [{"total": 96}]
        expected_grouped = [
            {"key": 0, "total": 48},
            {"key": 1, "total": 48},
        ]
    else:

        def replacement_factory() -> Callable[[int, dict[str, int]], int]:
            def select(row: dict[str, int]) -> int:
                return row["value"]

            def replacement(current: int, row: dict[str, int]) -> int:
                return current + select(row) * 2

            return replacement

        replacement = replacement_factory()
        monkeypatch.setattr(total.step, "__code__", replacement.__code__)
        expected_global = [{"total": 64}]
        expected_grouped = [
            {"key": 0, "total": 32},
            {"key": 1, "total": 32},
        ]

    matrix = np.asarray([[index % 2, 1, 3] for index in range(32)], dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("key", "value", "other"))
    if mode == "global":
        automatic = source.aggregate(total=total).to_list()
        canonical = source.with_engine("python").aggregate(total=total).to_list()
        expected = expected_global
    else:
        automatic = source.group_by("key").aggregate(total=total).to_list()
        canonical = source.with_engine("python").group_by("key").aggregate(total=total).to_list()
        expected = expected_grouped

    assert canonical == expected
    assert automatic == expected


def test_numpy_aggregates_safely_deopt_for_an_empty_selector_cell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An emptied live selector cell must raise canonically after lowering declines."""
    np = pytest.importorskip("numpy")

    total = fpstreams.agg.sum("value")
    closure = dict(
        zip(
            total.step.__code__.co_freevars,
            total.step.__closure__ or (),
            strict=True,
        )
    )
    monkeypatch.delattr(closure["select"], "cell_contents")
    matrix = np.ones((32, 1), dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("value",))

    for engine in ("auto", "python"):
        with pytest.raises(NameError, match="free variable 'select'"):
            source.with_engine(engine).aggregate(total=total).to_list()


@pytest.mark.parametrize("mode", ["global", "grouped"])
def test_numpy_aggregates_deopt_after_mutated_project_count_code(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """A mutated project count step must not retain exact or NumPy lowering."""
    np = pytest.importorskip("numpy")

    rows = fpstreams.agg.count()

    def replacement(current: int, _row: object) -> int:
        return current + 2

    monkeypatch.setattr(rows.step, "__code__", replacement.__code__)
    matrix = np.asarray([[index % 2, 1] for index in range(32)], dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
    if mode == "global":
        automatic = source.aggregate(rows=rows, total=fpstreams.agg.sum("value")).to_list()
        canonical = (
            source.with_engine("python")
            .aggregate(rows=rows, total=fpstreams.agg.sum("value"))
            .to_list()
        )
        expected = [{"rows": 64, "total": 32}]
    else:
        automatic = (
            source.group_by("key").aggregate(rows=rows, total=fpstreams.agg.sum("value")).to_list()
        )
        canonical = (
            source.with_engine("python")
            .group_by("key")
            .aggregate(rows=rows, total=fpstreams.agg.sum("value"))
            .to_list()
        )
        expected = [
            {"key": 0, "rows": 32, "total": 16},
            {"key": 1, "rows": 32, "total": 16},
        ]

    assert canonical == expected
    assert automatic == expected


def test_numpy_adapter_is_explicit_lazy_and_replayable() -> None:
    np = pytest.importorskip("numpy")

    source = np.asarray([[1, 2], [3, 4]])
    table = fpstreams.rows.from_numpy(source)

    source[0, 0] = 10
    assert table.to_list() == [
        {"0": 10, "1": 2},
        {"0": 3, "1": 4},
    ]
    source[1, 1] = 40
    assert table.to_list() == [
        {"0": 10, "1": 2},
        {"0": 3, "1": 40},
    ]
    assert table._flow._pipeline.source.capabilities.exact_size == 2
    assert table._flow._pipeline.source.capabilities.reiterable is True

    nested = [[1, 2]]
    converted = fpstreams.flow.from_numpy(nested, columns=["left", "right"])
    nested[0][0] = 99
    assert converted.select("left", "right").to_list() == [{"left": 1, "right": 2}]


def test_identity_numpy_to_list_skips_executor_forwarding_with_fresh_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact Rows and Flow collect canonical fresh dictionaries at the source boundary."""
    np = pytest.importorskip("numpy")
    from fpstreams.streams import flow_terminals

    values = np.asarray([[1, 2], [1, 2]], dtype=np.int64)
    source = fpstreams.rows.from_numpy(values, columns=("left", "right"))

    def reject_forwarding(*_arguments: object, **_options: object) -> Iterator[object]:
        raise AssertionError("identity NumPy to_list must not enter executor forwarding")

    monkeypatch.setattr(flow_terminals, "execute_physical", reject_forwarding)

    row_values = source.to_list()
    flow_values = source.to_flow().to_list()

    assert (
        row_values
        == flow_values
        == [
            {"left": 1, "right": 2},
            {"left": 1, "right": 2},
        ]
    )
    assert row_values[0] is not row_values[1]
    assert row_values[0] is not flow_values[0]


@pytest.mark.parametrize("width", [2, 8])
def test_numpy_identity_rows_boxes_values_in_bounded_matrix_batches(width: int) -> None:
    """The eager identity sink avoids both per-row ndarray calls and one giant snapshot."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular.numpy import (
        _NUMPY_IDENTITY_ROW_BATCH_SIZE,
        NumpyRowSource,
        numpy_identity_rows,
    )

    batch_size = _NUMPY_IDENTITY_ROW_BATCH_SIZE
    values = np.arange((batch_size + 3) * width, dtype=np.int64).reshape(-1, width)
    converted_batches: list[int] = []

    class TrackingMatrix:
        ndim = 2
        shape = values.shape

        def __iter__(self) -> Iterator[object]:
            raise AssertionError("identity collection must not convert one ndarray row at a time")

        def __getitem__(self, selected: slice) -> object:
            assert isinstance(selected, slice)
            batch = values[selected]
            converted_batches.append(len(batch))
            return batch

    names = tuple(f"c{index}" for index in range(width))
    result = numpy_identity_rows(NumpyRowSource(TrackingMatrix(), names))

    assert converted_batches == [batch_size, 3]
    assert len(result) == batch_size + 3
    assert result[0] == dict(zip(names, range(width), strict=True))
    last_start = (batch_size + 2) * width
    assert result[-1] == dict(zip(names, range(last_start, last_start + width), strict=True))

    class EmptyBatch:
        def tolist(self) -> list[object]:
            return []

    class StalledMatrix:
        ndim = 2
        shape = (1, 1)

        def __getitem__(self, _selected: slice) -> EmptyBatch:
            return EmptyBatch()

    with pytest.raises(ValueError, match="row count changed during iteration"):
        numpy_identity_rows(NumpyRowSource(StalledMatrix(), ("value",)))


def test_numpy_identity_rows_preserves_layout_dtype_and_object_identity() -> None:
    """Batched boxing keeps ndarray ordering, scalar conversion, and opaque object identity."""
    np = pytest.importorskip("numpy")

    for dtype in (np.int64, np.float64):
        base = np.arange(80, dtype=dtype).reshape(10, 8)
        if dtype is np.float64:
            base = base / 3
        matrices = (
            base.copy(order="C"),
            np.asfortranarray(base),
            base[:, ::2],
            base[::-1, 3::-1],
        )
        for matrix in matrices:
            for width in range(1, matrix.shape[1] + 1):
                values = matrix[:, :width]
                names = tuple(f"c{index}" for index in range(width))
                expected = [dict(zip(names, row, strict=True)) for row in values.tolist()]
                source = fpstreams.rows.from_numpy(values, columns=names)
                assert source.to_list() == expected
                assert source.with_engine("python").to_list() == expected

    protocol_calls: list[str] = []

    class OpaqueValue:
        def __int__(self) -> int:
            protocol_calls.append("int")
            raise AssertionError("object ndarray values must not be coerced")

        def __float__(self) -> float:
            protocol_calls.append("float")
            raise AssertionError("object ndarray values must not be coerced")

        def __index__(self) -> int:
            protocol_calls.append("index")
            raise AssertionError("object ndarray values must not be coerced")

        def __iter__(self) -> Iterator[object]:
            protocol_calls.append("iter")
            raise AssertionError("object ndarray values must stay opaque")

    markers = [[OpaqueValue() for _ in range(8)] for _ in range(3)]
    objects = np.empty((3, 8), dtype=object)
    for row_index, row in enumerate(markers):
        objects[row_index] = row
    names = tuple(f"c{index}" for index in range(8))
    records = fpstreams.rows.from_numpy(objects, columns=names).to_list()

    assert protocol_calls == []
    for row_index, record in enumerate(records):
        assert all(
            record[name] is markers[row_index][column_index]
            for column_index, name in enumerate(names)
        )
    for width in range(1, 9):
        assert fpstreams.rows.from_numpy(np.empty((0, width))).to_list() == []


def test_numpy_native_record_assembly_covers_identity_and_projected_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Private column batches bypass both Python record builders when the ABI is genuine."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_prefix
    from fpstreams.tabular import numpy as numpy_adapter

    values = np.arange(520 * 5, dtype=np.int64).reshape(520, 5)
    names = tuple(f"c{index}" for index in range(5))
    source = fpstreams.rows.from_numpy(values, columns=names)

    def reject_python_builder(*_arguments: object, **_options: object) -> object:
        raise AssertionError("genuine native record assembly must own this private batch")

    monkeypatch.setattr(numpy_adapter, "_materialize_numpy_identity_batch", reject_python_builder)
    monkeypatch.setattr(numpy_prefix, "_records_from_columns", reject_python_builder)

    assert source.to_list() == [dict(zip(names, row, strict=True)) for row in values.tolist()]
    assert source.select("c0", "c2", "c4").to_list() == [
        {"c0": row[0], "c2": row[2], "c4": row[4]} for row in values.tolist()
    ]


def test_numpy_record_assembly_rejects_replaced_native_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Python replacement cannot inherit the trusted private-column boundary."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native

    calls = 0

    def replaced(*_arguments: object) -> list[dict[str, object]]:
        nonlocal calls
        calls += 1
        return []

    monkeypatch.setattr(_native, "records_from_exact_columns_v1", replaced)
    values = np.arange(30, dtype=np.int64).reshape(10, 3)
    source = fpstreams.rows.from_numpy(values, columns=("left", "middle", "right"))

    assert source.to_list() == [
        {"left": row[0], "middle": row[1], "right": row[2]} for row in values.tolist()
    ]
    assert source.select("left", "right").to_list() == [
        {"left": row[0], "right": row[2]} for row in values.tolist()
    ]
    assert calls == 0


def test_numpy_identity_rows_releases_partial_batches_after_memory_error() -> None:
    """A failed later batch cannot leak the already boxed partial result."""
    from fpstreams.tabular.numpy import (
        _NUMPY_IDENTITY_ROW_BATCH_SIZE,
        NumpyRowSource,
        numpy_identity_rows,
    )

    references: list[weakref.ReferenceType[object]] = []
    calls = 0

    class Marker:
        pass

    class ConvertedBatch:
        def tolist(self) -> list[list[Marker]]:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise MemoryError("second NumPy row batch")
            rows = [[Marker(), Marker()] for _ in range(_NUMPY_IDENTITY_ROW_BATCH_SIZE)]
            references.extend(weakref.ref(value) for row in rows for value in row)
            return rows

    class FailingMatrix:
        ndim = 2
        shape = (_NUMPY_IDENTITY_ROW_BATCH_SIZE + 1, 2)

        def __getitem__(self, _selected: slice) -> ConvertedBatch:
            return ConvertedBatch()

    try:
        numpy_identity_rows(NumpyRowSource(FailingMatrix(), ("left", "right")))
    except MemoryError as error:
        assert str(error) == "second NumPy row batch"
        frames = error.__traceback__
        error.__traceback__ = None
        if frames is not None:
            traceback.clear_frames(frames)
        del frames
    else:  # pragma: no cover - the synthetic second batch always fails
        raise AssertionError("the synthetic second batch did not fail")

    gc.collect()
    assert references
    assert all(reference() is None for reference in references)


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="requires POSIX interval timers")
def test_numpy_to_list_matches_live_signal_mutation_and_resize_boundaries() -> None:
    """Auto batching and canonical iteration agree on signal-time growth and shape changes."""
    row_count = 100_000

    def consume(
        engine: str, action: str
    ) -> tuple[list[dict[str, int]] | None, BaseException | None]:
        np = pytest.importorskip("numpy")
        values = np.zeros((row_count, 2), dtype=np.int64)
        source = fpstreams.rows.from_numpy(values, columns=("left", "right")).with_engine(engine)
        fired = False

        def mutate(_signal_number: int, _frame: object) -> None:
            nonlocal fired
            fired = True
            if action == "mutate":
                values[-1] = (7, 9)
            elif action == "grow":
                values.resize((row_count + 1, 2), refcheck=False)
                values[-1] = (7, 9)
            elif action == "shrink":
                values.resize((row_count - 1, 2), refcheck=False)
            else:
                values.resize((values.size,), refcheck=False)

        previous_handler = signal.signal(signal.SIGALRM, mutate)
        previous_timer = signal.getitimer(signal.ITIMER_REAL)
        signal.setitimer(signal.ITIMER_REAL, 0.001)
        try:
            try:
                result = source.to_list()
            except BaseException as error:
                result = None
                captured = error
            else:
                captured = None
        finally:
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)
            signal.signal(signal.SIGALRM, previous_handler)
        assert fired
        return result, captured

    for action in ("mutate", "grow", "shrink", "reshape"):
        canonical = consume("python", action)
        automatic = consume("auto", action)
        if action == "reshape":
            assert canonical[0] is automatic[0] is None
            assert [type(canonical[1]), type(automatic[1])] == [ValueError, ValueError]
            assert (
                str(canonical[1])
                == str(automatic[1])
                == ("from_numpy() retained array changed to 1 dimensions during iteration")
            )
        else:
            assert canonical[1] is automatic[1] is None
            assert automatic[0] == canonical[0]
            expected_rows = row_count + (1 if action == "grow" else -1 if action == "shrink" else 0)
            assert automatic[0] is not None
            assert len(automatic[0]) == expected_rows
            if action in {"mutate", "grow"}:
                assert automatic[0][-1] == {"left": 7, "right": 9}


def test_numpy_identity_sinks_deopt_on_free_threaded_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutable ndarray batching stays behind the GIL-build boundary."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular import numpy as numpy_adapter

    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)

    def reject_direct(_source: object) -> list[dict[str, object]]:
        raise AssertionError("free-threaded Python must retain canonical ndarray iteration")

    monkeypatch.setattr(numpy_adapter, "numpy_identity_rows", reject_direct)
    names = tuple(f"c{index}" for index in range(8))
    values = np.arange(8, dtype=np.int64).reshape(1, 8)
    source = fpstreams.rows.from_numpy(values, columns=names)
    execution = source.run_with_report("to_list")

    assert execution.value == [dict(zip(names, range(8), strict=True))]
    assert execution.report.strategy == "planned:python"


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="requires POSIX interval timers")
def test_numpy_direct_row_paths_deopt_while_an_interval_timer_is_active() -> None:
    """Identity, safe-prefix, and grouped sinks share one conservative timer gate."""
    np = pytest.importorskip("numpy")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2], [1, 3]], dtype=np.int64),
        columns=("key", "value"),
    )
    queries = (
        source,
        source.where(fpstreams.col("value") >= 2).select("key"),
        source.group_by("key").aggregate(total=fpstreams.agg.sum("value")),
    )
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    signal.setitimer(signal.ITIMER_REAL, 60.0)
    try:
        executions = tuple(query.run_with_report("to_list") for query in queries)
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)

    assert [execution.value for execution in executions] == [
        [{"key": 1, "value": 2}, {"key": 1, "value": 3}],
        [{"key": 1}, {"key": 1}],
        [{"key": 1, "total": 5}],
    ]
    assert [execution.report.strategy for execution in executions] == [
        "planned:python",
        "planned:python",
        "planned:python",
    ]


@pytest.mark.skipif(not hasattr(signal, "SIGUSR1"), reason="requires a user signal")
def test_numpy_direct_row_paths_ignore_an_idle_custom_signal_handler() -> None:
    """Installing an unrelated handler alone must not disable columnar execution."""
    np = pytest.importorskip("numpy")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2], [1, 3]], dtype=np.int64),
        columns=("key", "value"),
    )
    queries = (
        source,
        source.where(fpstreams.col("value") >= 2).select("key"),
        source.group_by("key").aggregate(total=fpstreams.agg.sum("value")),
    )
    previous_handler = signal.signal(signal.SIGUSR1, lambda *_arguments: None)
    try:
        executions = tuple(query.run_with_report("to_list") for query in queries)
    finally:
        signal.signal(signal.SIGUSR1, previous_handler)

    assert [execution.value for execution in executions] == [
        [{"key": 1, "value": 2}, {"key": 1, "value": 3}],
        [{"key": 1}, {"key": 1}],
        [{"key": 1, "total": 5}],
    ]
    assert [execution.report.strategy for execution in executions] == [
        "numpy_direct",
        "numpy_direct",
        "numpy_direct",
    ]


def test_identity_numpy_to_list_reports_its_real_direct_strategy() -> None:
    """Reported NumPy identity collection names the source-level route it executed."""
    np = pytest.importorskip("numpy")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2], [3, 4]], dtype=np.int64),
        columns=("left", "right"),
    )

    rows_execution = source.run_with_report("to_list")
    flow_execution = source.to_flow().run_with_report("to_list")

    assert (
        rows_execution.value
        == flow_execution.value
        == [
            {"left": 1, "right": 2},
            {"left": 3, "right": 4},
        ]
    )
    assert rows_execution.report.compiler_engine == "python"
    assert flow_execution.report.compiler_engine == "python"
    assert rows_execution.report.strategy == "numpy_direct"
    assert flow_execution.report.strategy == "numpy_direct"


@pytest.mark.parametrize("width", [5, 6, 7, 8])
def test_wide_identity_numpy_to_list_uses_the_bounded_direct_strategy(width: int) -> None:
    """Common wider records share the bounded source-level identity collector."""
    np = pytest.importorskip("numpy")

    names = tuple(f"c{index}" for index in range(width))
    values = np.arange(width * 2, dtype=np.int64).reshape(2, width)
    execution = fpstreams.rows.from_numpy(values, columns=names).run_with_report("to_list")

    assert execution.value == [dict(zip(names, row, strict=True)) for row in values.tolist()]
    assert execution.report.strategy == "numpy_direct"


def test_wider_identity_numpy_to_list_keeps_the_canonical_fallback() -> None:
    """Unbounded schema specialization stops after the common eight-column range."""
    np = pytest.importorskip("numpy")

    names = tuple(f"c{index}" for index in range(9))
    values = np.arange(18, dtype=np.int64).reshape(2, 9)
    execution = fpstreams.rows.from_numpy(values, columns=names).run_with_report("to_list")

    assert execution.value == [dict(zip(names, row, strict=True)) for row in values.tolist()]
    assert execution.report.strategy == "planned:python"


def test_numpy_to_list_subclasses_keep_their_canonical_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rows iteration and Flow execution subclasses both bypass the retained direct sink."""
    np = pytest.importorskip("numpy")
    from fpstreams.streams.flow import Flow
    from fpstreams.tabular import numpy as numpy_adapter
    from fpstreams.tabular.rows import Rows

    marker = {"left": 9, "right": 10}

    class CustomRows(Rows[dict[str, int]]):
        def __iter__(self) -> Iterator[dict[str, int]]:
            return iter((marker,))

    class CustomFlow(Flow[dict[str, int]]):
        pass

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2]], dtype=np.int64),
        columns=("left", "right"),
    )
    custom_rows = CustomRows(source._flow)
    custom_flow = CustomFlow(source._flow._pipeline.source)

    def reject_direct(_source: object) -> list[dict[str, object]]:
        raise AssertionError("Flow subclasses must stay on canonical physical execution")

    monkeypatch.setattr(numpy_adapter, "numpy_identity_rows", reject_direct)

    assert custom_rows.to_list() == [marker]
    assert custom_flow.to_list() == [{"left": 1, "right": 2}]


def test_identity_numpy_to_list_declines_python_operations_and_failpoints() -> None:
    """Only an uninstrumented auto identity plan may collect retained NumPy rows directly."""
    np = pytest.importorskip("numpy")
    from fpstreams.runtime.failpoints import failpoint

    values = np.asarray([[1, 2], [3, 4]], dtype=np.int64)
    source = fpstreams.rows.from_numpy(values, columns=("left", "right"))

    forced = source.with_engine("python").run_with_report("to_list")
    assert forced.value == [{"left": 1, "right": 2}, {"left": 3, "right": 4}]
    assert forced.report.strategy == "planned:python"
    assert source.select("left").to_list() == [{"left": 1}, {"left": 3}]

    failure = RuntimeError("observed NumPy source open")
    with (
        failpoint("source.open.after", failure),
        pytest.raises(RuntimeError, match="observed NumPy source open") as captured,
    ):
        source.to_list()
    assert captured.value is failure


@pytest.mark.parametrize("name", ["min", "max"])
def test_numpy_identity_rows_keep_internal_extrema_stable(name: str) -> None:
    """Batch bounds use canonical helpers that the Python row source never resolves."""
    import builtins

    np = pytest.importorskip("numpy")
    rows = fpstreams.rows.from_numpy(
        np.asarray([[1, 2], [3, 4]], dtype=np.int64),
        columns=("left", "right"),
    )
    original = getattr(builtins, name)
    try:
        setattr(builtins, name, lambda *_args, **_kwargs: False)
        execution = rows.run_with_report("to_list")
    finally:
        setattr(builtins, name, original)

    assert execution.value == [{"left": 1, "right": 2}, {"left": 3, "right": 4}]
    assert execution.report.strategy == "numpy_direct"


def test_numpy_prefix_snapshots_selection_error_before_lazy_import() -> None:
    """Lazy prefix loading must keep the exception class bound by eager selectors."""
    completed = _run_inline_python(
        """
import sys
import fpstreams
import fpstreams.errors as errors

selection_error_type = errors.SelectionError
assert "fpstreams.execution.numpy_prefix" not in sys.modules

class ReplacedSelectionError(selection_error_type):
    pass

errors.SelectionError = ReplacedSelectionError

import numpy as np
rows = fpstreams.rows.from_numpy(
    np.asarray([[1]], dtype=np.int64),
    columns=("value",),
)
automatic = rows.select("missing")
canonical = rows.with_engine("python").select("missing")

def capture_error(query):
    try:
        query.to_list()
    except BaseException as error:
        return error
    raise AssertionError("missing selector unexpectedly succeeded")

canonical_error = capture_error(canonical)
automatic_error = capture_error(automatic)
assert type(automatic_error) is type(canonical_error) is selection_error_type
assert str(automatic_error) == str(canonical_error)
"""
    )

    assert completed.returncode == 0, completed.stderr


def test_numpy_kernels_do_not_execute_lazy_typing_casts() -> None:
    """Runtime kernels should not depend on a typing-only helper captured during lazy import."""
    completed = _run_inline_python(
        """
import sys
import typing
import fpstreams
import numpy as np

rows = fpstreams.rows.from_numpy(
    np.asarray([[1, 2], [1, 3]], dtype=np.int64),
    columns=("key", "value"),
)
computed_auto = rows.with_columns(next=fpstreams.col("value") + 1)
computed_python = rows.with_engine("python").with_columns(next=fpstreams.col("value") + 1)
group_auto = rows.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
group_python = rows.with_engine("python").group_by("key").aggregate(
    total=fpstreams.agg.sum("value")
)
assert "fpstreams.execution.numpy_prefix" not in sys.modules
assert "fpstreams.execution.numpy_group" not in sys.modules

original_cast = typing.cast
def changed_cast(target, value):
    if getattr(target, "__origin__", None) is list:
        return []
    if getattr(target, "__name__", None) == "NumpyGroupAggregateSpec":
        raise RuntimeError("typing.cast reached grouped execution")
    return original_cast(target, value)

typing.cast = changed_cast
try:
    expected_computed = computed_python.to_list()
    expected_grouped = group_python.to_list()
    first_computed = computed_auto.run_with_report("to_list")
    computed = computed_auto.run_with_report("to_list")
    grouped = group_auto.run_with_report("to_list")
finally:
    typing.cast = original_cast

assert computed.value == expected_computed
assert grouped.value == expected_grouped
assert first_computed.report.strategy == "planned:python"
assert computed.report.strategy == "numpy_direct"
assert grouped.report.strategy == "numpy_direct"
"""
    )

    assert completed.returncode == 0, completed.stderr


def test_numpy_identity_source_declines_replaced_instance_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retained NumPy metadata cannot outrank a replaced live source factory."""
    np = pytest.importorskip("numpy")

    query = fpstreams.rows.from_numpy(
        np.asarray([[1], [2]], dtype=np.int64),
        columns=("value",),
    )
    source = query._flow._pipeline.source
    opens = 0

    def replacement() -> Iterator[dict[str, int]]:
        nonlocal opens
        opens += 1
        return iter(({"value": 9},))

    monkeypatch.setattr(source, "_factory", replacement)
    execution = query.run_with_report("to_list")

    assert execution.value == [{"value": 9}]
    assert execution.report.strategy == "planned:python"
    assert opens == 1


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="requires POSIX interval timers")
def test_numpy_identity_to_numpy_copy_false_ignores_row_observers() -> None:
    """Row observers and unused column protocols cannot change ndarray copy semantics."""
    np = pytest.importorskip("numpy")

    class ArmedColumn(str):
        calls = 0

        def __hash__(self) -> int:
            type(self).calls += 1
            return super().__hash__()

    values = np.asarray([[1, 2], [3, 4]], dtype=np.int64)
    name = ArmedColumn("left")
    rows = fpstreams.rows.from_numpy(values, columns=(name, "right"))
    ArmedColumn.calls = 0

    def trace(_frame: object, _event: str, _argument: object) -> object:
        return trace

    previous_trace = sys.gettrace()
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    sys.settrace(trace)
    signal.setitimer(signal.ITIMER_REAL, 60.0)
    try:
        result = rows.to_numpy(copy=False)
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        sys.settrace(previous_trace)

    assert np.shares_memory(result, values)
    assert ArmedColumn.calls == 0


def test_identity_numpy_to_columns_never_boxes_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retained matrix should transpose its Python values without row dictionaries."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular import io as tabular_io

    values = np.asarray(
        [[1, float("nan"), "first"], [2, -0.0, None]],
        dtype=object,
    )
    source = fpstreams.rows.from_numpy(values, columns=("id", "score", "label"))

    def reject_rows(_row: object) -> dict[str, object]:
        raise AssertionError("identity NumPy to_columns must not box rows")

    monkeypatch.setattr(tabular_io, "_as_record", reject_rows)

    columns = source.to_columns()

    assert list(columns) == ["id", "score", "label"]
    assert columns["id"] == [1, 2]
    assert math.isnan(columns["score"][0])
    assert math.copysign(1.0, columns["score"][1]) == -1.0
    assert columns["label"] == ["first", None]


def test_identity_numpy_arrow_materializers_never_box_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Arrow and pandas sinks should share the retained matrix column conversion."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular import io as tabular_io

    values = np.asarray([[1.25, 2.5], [3.5, float("nan")]], dtype=np.float32)
    source = fpstreams.rows.from_numpy(values, columns=("left", "right"))

    def reject_rows(_row: object) -> Mapping[str, object]:
        raise AssertionError("identity NumPy Arrow sinks must not box rows")

    monkeypatch.setattr(tabular_io, "_record_view", reject_rows)

    table = source.to_arrow(batch_size=1)
    frame = source.to_pandas(batch_size=1)

    assert table.schema == pa.schema([("left", pa.float64()), ("right", pa.float64())])
    assert [batch.num_rows for batch in table.to_batches()] == [1, 1]
    assert table.column("left").to_pylist() == [1.25, 3.5]
    assert table.column("right")[0].as_py() == 2.5
    assert math.isnan(table.column("right")[1].as_py())
    assert list(frame.columns) == ["left", "right"]
    assert frame["left"].tolist() == [1.25, 3.5]
    assert math.isnan(frame["right"].iloc[1])


def test_numpy_arrow_resize_after_first_batch_has_one_adapter_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto and Python scans should reject a live dimensionality change identically."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular import arrow as arrow_adapter
    from fpstreams.tabular import io as tabular_io

    def resizing_arrow_modules(values: object) -> Callable[[], tuple[object, object, object]]:
        resized = False

        class RecordBatchProxy:
            @staticmethod
            def from_pydict(columns: object, schema: object = None) -> pa.RecordBatch:
                nonlocal resized
                batch = pa.RecordBatch.from_pydict(columns, schema=schema)
                if not resized:
                    resized = True
                    values.resize((values.size,), refcheck=False)  # type: ignore[attr-defined]
                return batch

        class PyArrowProxy:
            RecordBatch = RecordBatchProxy

            def __getattr__(self, name: str) -> object:
                return getattr(pa, name)

        proxy = PyArrowProxy()

        def modules() -> tuple[object, object, object]:
            return proxy, ds, pq

        return modules

    errors: list[BaseException] = []
    for engine in ("auto", "python"):
        values = np.asarray([[1, 2], [3, 4]], dtype=np.int64)
        source = fpstreams.rows.from_numpy(values, columns=("left", "right")).with_engine(engine)
        arrow_modules = resizing_arrow_modules(values)

        monkeypatch.setattr(tabular_io, "_arrow_modules", arrow_modules)
        monkeypatch.setattr(arrow_adapter, "_arrow_modules", arrow_modules)

        try:
            source.to_arrow(batch_size=1)
        except BaseException as error:
            errors.append(error)
        else:
            raise AssertionError(f"{engine} scan accepted a retained 2D-to-1D resize")

        values.resize((2, 2), refcheck=False)
        assert source.to_arrow(batch_size=1).to_pylist() == [
            {"left": 1, "right": 2},
            {"left": 3, "right": 4},
        ]

    assert [type(error) for error in errors] == [ValueError, ValueError]
    assert [str(error) for error in errors] == [
        "from_numpy() retained array changed to 1 dimensions during iteration",
        "from_numpy() retained array changed to 1 dimensions during iteration",
    ]


def test_numpy_to_list_resize_after_sink_entry_has_one_adapter_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct and forced-Python list sinks reject the same live dimensionality change."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular import numpy as numpy_adapter

    canonical_width = numpy_adapter._retained_numpy_width
    errors: list[BaseException] = []
    for engine in ("auto", "python"):
        names = tuple(f"c{index}" for index in range(8))
        values = np.arange(16, dtype=np.int64).reshape(2, 8)
        source = fpstreams.rows.from_numpy(values, columns=names).with_engine(engine)
        resized = False

        def resize_after_entry(matrix: object, names: tuple[str, ...]) -> int:
            nonlocal resized
            width = canonical_width(matrix, names)
            if not resized:
                resized = True
                matrix.resize((matrix.size,), refcheck=False)  # type: ignore[attr-defined]
            return width

        monkeypatch.setattr(numpy_adapter, "_retained_numpy_width", resize_after_entry)
        try:
            source.to_list()
        except BaseException as error:
            errors.append(error)
        else:
            raise AssertionError(f"{engine} list accepted a retained 2D-to-1D resize")

        monkeypatch.setattr(numpy_adapter, "_retained_numpy_width", canonical_width)
        values.resize((2, 8), refcheck=False)
        assert source.to_list() == [dict(zip(names, row, strict=True)) for row in values.tolist()]

    assert [type(error) for error in errors] == [ValueError, ValueError]
    assert [str(error) for error in errors] == [
        "from_numpy() retained array changed to 1 dimensions during iteration",
        "from_numpy() retained array changed to 1 dimensions during iteration",
    ]


def test_numpy_group_deopts_after_executor_builtin_alias_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Group kernels cannot observe a replaced executor integer constructor."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group

    source = (
        fpstreams.rows.from_numpy(
            np.asarray([[1, 10], [1, 20], [2, 30]], dtype=np.int64),
            columns=("key", "value"),
        )
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )
    first = source.run_with_report("to_list")
    assert first.report.strategy == "numpy_direct"

    def unexpected(*_arguments: object, **_keywords: object) -> object:
        raise AssertionError("replaced executor int reached NumPy grouping")

    monkeypatch.setattr(numpy_group, "_BUILTIN_INT", unexpected)
    execution = source.run_with_report("to_list")

    assert execution.value == [
        {"key": 1, "rows": 2, "total": 30},
        {"key": 2, "rows": 1, "total": 30},
    ]
    assert execution.report.strategy == "planned:python"


def test_numpy_with_columns_compiles_before_reusing_its_cached_direct_plan() -> None:
    """Fresh lazy expressions compile canonically once before a later direct execution."""
    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.asarray([[1], [2]], dtype=np.int64),
        columns=("value",),
    ).with_columns(score=fpstreams.col("value") + 1)

    first = source.run_with_report("to_list")
    second = source.run_with_report("to_list")

    assert (
        first.value
        == second.value
        == [
            {"value": 1, "score": 2},
            {"value": 2, "score": 3},
        ]
    )
    assert first.report.strategy == "planned:python"
    assert second.report.strategy == "numpy_direct"


def test_flow_from_numpy_adapts_one_dimensional_scalars_lazily() -> None:
    """A 1D explicit adapter must stay live and emit ordinary Python scalar values."""
    np = pytest.importorskip("numpy")

    source = np.asarray([1, 2, 3], dtype=np.int64)
    pipeline = fpstreams.flow.from_numpy(source)

    source[1] = 20
    assert pipeline.to_list() == [1, 20, 3]
    assert all(type(value) is int for value in pipeline)
    source.resize((4,), refcheck=False)
    source[3] = 40
    assert pipeline.count() == 4
    assert pipeline.with_engine("python").to_list() == [1, 20, 3, 40]

    source.resize((2, 2), refcheck=False)
    with pytest.raises(ValueError, match="changed to 2 dimensions"):
        pipeline.count()

    with pytest.raises(ValueError, match=r"columns.*two-dimensional"):
        fpstreams.flow.from_numpy(np.arange(2), columns=("value",))


def test_flow_from_numpy_i64_supports_exact_native_full_scan_terminals() -> None:
    """The buffer kernel must preserve Python integers and statistical results."""
    np = pytest.importorskip("numpy")

    maximum = 2**63 - 1
    wide_source = np.asarray([maximum, maximum], dtype=np.int64)
    wide = fpstreams.flow.from_numpy(wide_source).with_engine("native")
    assert wide.sum() == 2 * maximum
    assert type(wide.sum()) is int
    assert wide.aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum()) == {
        "rows": 2,
        "total": 2 * maximum,
    }
    assert wide.min() == maximum
    assert wide.max() == maximum
    wide_source[0] = -maximum
    assert wide.sum() == 0

    statistics = fpstreams.flow.from_numpy(np.asarray([1, 2, 3, 4], dtype=np.int64)).with_engine(
        "native"
    )
    assert statistics.mean() == 2.5
    assert statistics.variance() == pytest.approx(5 / 3)
    assert statistics.std() == pytest.approx((5 / 3) ** 0.5)

    empty = fpstreams.flow.from_numpy(np.asarray([], dtype=np.int64)).with_engine("native")
    assert empty.sum() == 0
    assert empty.mean() is None
    assert empty.variance() is None
    with pytest.raises(fpstreams.EmptyFlowError):
        empty.min()

    automatic = fpstreams.flow.from_numpy(np.arange(32, dtype=np.int64))
    assert automatic.explain("sum").to_dict()["selected_engine"] == "native"
    assert automatic.explain("list").to_dict()["selected_engine"] == "native"
    assert automatic.to_list() == list(range(32))

    from fpstreams.runtime.failpoints import failpoint

    failure = RuntimeError("instrumented NumPy source")
    with failpoint("source.open.after", failure), pytest.raises(RuntimeError) as captured:
        automatic.sum()
    assert captured.value is failure


@pytest.mark.parametrize("dtype", ["int64", "float64"])
def test_flow_from_numpy_terminal_planning_classifies_buffer_once(
    monkeypatch: pytest.MonkeyPatch,
    dtype: str,
) -> None:
    """One engine decision should not repeatedly revalidate one retained buffer."""
    np = pytest.importorskip("numpy")
    from fpstreams.planning import native

    pipeline = fpstreams.flow.from_numpy(np.arange(32, dtype=dtype))
    original = native._numpy_buffer_kind
    calls = 0

    def counted_buffer_kind(source: object) -> object:
        nonlocal calls
        calls += 1
        return original(source)

    monkeypatch.setattr(native, "_numpy_buffer_kind", counted_buffer_kind)

    decision = native.select_terminal_engine(pipeline._pipeline, "sum")

    assert decision.engine == "native"
    assert calls == 1


@pytest.mark.parametrize("engine", ["auto", "native"])
@pytest.mark.parametrize("dtype", ["int64", "float64"])
def test_flow_from_numpy_operation_planning_classifies_buffer_once(
    monkeypatch: pytest.MonkeyPatch,
    engine: str,
    dtype: str,
) -> None:
    """A fused decision should reuse its validated buffer kind throughout planning."""
    np = pytest.importorskip("numpy")
    from fpstreams.planning import native

    expression = fpstreams.item + 1 if dtype == "int64" else fpstreams.fitem + 1.0
    pipeline = (
        fpstreams.flow.from_numpy(np.arange(32, dtype=dtype)).map(expression).with_engine(engine)
    )
    original = native._numpy_buffer_kind
    calls = 0

    def counted_buffer_kind(source: object) -> object:
        nonlocal calls
        calls += 1
        return original(source)

    monkeypatch.setattr(native, "_numpy_buffer_kind", counted_buffer_kind)

    decision = native.select_terminal_engine(pipeline._pipeline, "sum")

    assert decision.engine == "native"
    assert calls == 1


def test_flow_from_numpy_i64_identity_min_max_use_numpy_direct_reduction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact identity extrema may skip planning without widening NumPy semantics."""
    np = pytest.importorskip("numpy")

    values = np.asarray(
        [2**63 - 1, -7, -(2**63), 11, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12],
        dtype=np.int64,
    )
    pipeline = fpstreams.flow.from_numpy(values)
    tiny = fpstreams.flow.from_numpy(np.asarray([2, 1], dtype=np.int64))

    minimum = pipeline.run_with_report("min")
    maximum = pipeline.run_with_report("max")

    assert minimum.value == -(2**63)
    assert maximum.value == 2**63 - 1
    assert type(minimum.value) is type(maximum.value) is int
    assert minimum.report.compiler_engine == maximum.report.compiler_engine == "not_compiled"
    assert minimum.report.strategy == maximum.report.strategy == "numpy_direct"

    forced = pipeline.with_engine("python").run_with_report("min")
    transformed = pipeline.map(fpstreams.item + 0).run_with_report("max")
    strided = fpstreams.flow.from_numpy(values[::2]).run_with_report("min")

    assert forced.value == strided.value == -(2**63)
    assert transformed.value == 2**63 - 1
    assert forced.report.strategy != "numpy_direct"
    assert transformed.report.strategy != "numpy_direct"
    assert strided.report.strategy != "numpy_direct"

    empty = fpstreams.flow.from_numpy(np.asarray([], dtype=np.int64))
    with pytest.raises(fpstreams.EmptyFlowError, match=r"min\(\)") as empty_failure:
        empty.min()
    assert empty_failure.value.__context__ is None

    from fpstreams.runtime.failpoints import failpoint

    failure = RuntimeError("instrumented NumPy min")
    with failpoint("source.open.after", failure), pytest.raises(RuntimeError) as captured:
        pipeline.min()
    assert captured.value is failure

    from fpstreams.runtime import failpoints as failpoints_module

    def replaced_hit(name: str) -> None:
        raise RuntimeError(f"replaced hit:{name}")

    with monkeypatch.context() as scoped:
        scoped.setattr(failpoints_module, "hit", replaced_hit)
        with pytest.raises(RuntimeError, match=r"replaced hit:source.open.after"):
            tiny.min()

    import builtins

    for terminal in ("min", "max"):
        message = f"replaced builtin {terminal}"

        def replaced(*_args: object, _message: str = message, **_kwargs: object) -> object:
            raise RuntimeError(_message)

        captured_builtin: BaseException | None = None
        with monkeypatch.context() as scoped:
            scoped.setattr(builtins, terminal, replaced)
            try:
                getattr(tiny, terminal)()
            except BaseException as error:
                captured_builtin = error
        assert type(captured_builtin) is RuntimeError
        assert str(captured_builtin) == message

    from fpstreams.tabular.numpy import NumpyColumnSource

    def replaced_len(_source: object) -> int:
        raise RuntimeError("replaced NumPy column length")

    with monkeypatch.context() as scoped:
        scoped.setattr(NumpyColumnSource, "__len__", replaced_len)
        with pytest.raises(RuntimeError, match="replaced NumPy column length"):
            pipeline.min()

    def replaced_size(_source: object) -> int:
        raise RuntimeError("replaced exact size")

    from fpstreams.planning.source import Source

    with monkeypatch.context() as scoped:
        scoped.setattr(Source, "current_exact_size", replaced_size)
        with pytest.raises(RuntimeError, match="replaced exact size"):
            pipeline.min()

    rebound = fpstreams.flow.from_numpy(np.asarray([0, 1], dtype=np.int64))
    rebound_source = rebound._logical_plan.root.source  # type: ignore[attr-defined]
    rebound_source.native_data = NumpyColumnSource(np.asarray([101, 102], dtype=np.int64))
    rebound_execution = rebound.run_with_report("min")
    assert rebound_execution.value == 0
    assert rebound_execution.report.strategy != "numpy_direct"

    from fpstreams.streams import flow_terminals

    with (
        monkeypatch.context() as scoped,
        failpoint("source.open.after", RuntimeError("active patched failpoint")),
    ):
        scoped.setattr(flow_terminals, "has_active_failpoints", lambda: False)
        with pytest.raises(RuntimeError, match="active patched failpoint"):
            tiny.min()


def test_flow_from_numpy_i64_fuses_expression_pipelines() -> None:
    """A numeric buffer source should compose with ordinary native map/filter stages."""
    np = pytest.importorskip("numpy")

    source = np.arange(16, dtype=np.int64)
    pipeline = (
        fpstreams.flow.from_numpy(source)
        .map(fpstreams.item * 3 + 1)
        .filter(fpstreams.item % 2 == 0)
    )

    expected = [4, 10, 16, 22, 28, 34, 40, 46]
    assert pipeline.with_engine("native").to_list() == expected
    assert pipeline.with_engine("native").sum() == 200
    assert pipeline.explain("sum").to_dict()["selected_engine"] == "native"
    movement = pipeline.explain("list").to_dict()["data_movement"]
    assert movement["scans_source"] is True
    assert movement["copies_source"] is True

    source[0] = 3
    assert pipeline.with_engine("native").to_list() == [10, *expected]

    overflowing = fpstreams.flow.from_numpy(np.asarray([2**62, *([0] * 7)], dtype=np.int64)).map(
        fpstreams.item * 4
    )
    assert overflowing.to_list() == [2**64, *([0] * 7)]
    with pytest.raises(OverflowError):
        overflowing.with_engine("native").to_list()

    short_circuiting = fpstreams.flow.from_numpy(np.asarray([2, 0], dtype=np.int64)).map(
        10 // fpstreams.item
    )
    assert short_circuiting.first() == 5
    assert short_circuiting.aggregate(first=fpstreams.agg.first()) == {"first": 5}
    with pytest.raises(fpstreams.NativeUnsupportedError, match="short-circuiting"):
        short_circuiting.with_engine("native").first()
    with pytest.raises(fpstreams.NativeUnsupportedError, match="short-circuiting"):
        short_circuiting.with_engine("native").aggregate(first=fpstreams.agg.first())


def test_flow_from_numpy_non_native_i64_layout_stays_on_the_python_path() -> None:
    """Strided and opposite-endian arrays must not be silently copied into the buffer kernel."""
    np = pytest.importorskip("numpy")

    strided = np.arange(64, dtype=np.int64)[::2]
    pipeline = fpstreams.flow.from_numpy(strided)
    assert pipeline.to_list() == list(range(0, 64, 2))
    assert pipeline.explain("sum").to_dict()["selected_engine"] == "python"
    with pytest.raises(fpstreams.NativeUnsupportedError):
        pipeline.with_engine("native").sum()

    storage = np.arange(8 * 33 + 1, dtype=np.uint8)
    unaligned = storage[1:].view(np.int64)
    unaligned_flow = fpstreams.flow.from_numpy(unaligned)
    assert unaligned.flags.c_contiguous and not unaligned.flags.aligned
    assert unaligned_flow.explain("sum").to_dict()["selected_engine"] == "python"
    with pytest.raises(fpstreams.NativeUnsupportedError):
        unaligned_flow.with_engine("native").sum()

    opposite_dtype = ">i8" if sys.byteorder == "little" else "<i8"
    opposite = np.asarray([1, 2, 3], dtype=opposite_dtype)
    assert fpstreams.flow.from_numpy(opposite).sum() == 6

    from fpstreams import _native

    with pytest.raises(TypeError, match="native-endian"):
        _native.aggregate_i64_buffer_masked_v1(opposite, [], 1 << 1)


def test_flow_from_numpy_f64_preserves_python_full_scan_terminal_semantics() -> None:
    """A float buffer fast path must keep Python numeric and empty-flow contracts."""
    np = pytest.importorskip("numpy")

    cancellation_values = np.asarray([1e16, 1.0, -1e16], dtype=np.float64)
    cancellation = fpstreams.flow.from_numpy(cancellation_values)
    expected_sum = cancellation.with_engine("python").sum()
    assert cancellation.explain("list").to_dict()["selected_engine"] == "python"
    assert cancellation.with_engine("native").to_list() == cancellation_values.tolist()
    assert cancellation.with_engine("native").sum() == expected_sum
    assert cancellation.with_engine("native").mean() == cancellation.with_engine("python").mean()
    assert cancellation.with_engine("native").variance() == pytest.approx(
        cancellation.with_engine("python").variance()
    )

    aggregate_cancellation = fpstreams.flow.from_numpy(
        np.asarray([1e16, 1.0, -1e16, 0.0, 0.0, 0.0, 0.0, 0.0], dtype=np.float64)
    )
    expected_total = aggregate_cancellation.with_engine("python").aggregate(
        total=fpstreams.agg.sum()
    )
    automatic_total = aggregate_cancellation.aggregate(total=fpstreams.agg.sum())
    assert automatic_total == expected_total
    assert type(automatic_total["total"]) is float
    mixed_total = aggregate_cancellation.with_engine("native").aggregate(
        total=fpstreams.agg.sum(), minimum=fpstreams.agg.min()
    )
    assert mixed_total == aggregate_cancellation.with_engine("python").aggregate(
        total=fpstreams.agg.sum(), minimum=fpstreams.agg.min()
    )
    assert type(mixed_total["total"]) is float

    automatic = fpstreams.flow.from_numpy(np.arange(32, dtype=np.float64))
    assert automatic.explain("list").to_dict()["selected_engine"] == "native"
    assert automatic.to_list() == np.arange(32, dtype=np.float64).tolist()

    empty = fpstreams.flow.from_numpy(np.asarray([], dtype=np.float64)).with_engine("native")
    assert empty.sum() == 0
    assert type(empty.sum()) is int
    assert empty.aggregate(total=fpstreams.agg.sum()) == {"total": 0}
    assert type(empty.aggregate(total=fpstreams.agg.sum())["total"]) is int
    assert empty.mean() is None
    assert empty.variance() is None

    filtered_empty = fpstreams.flow.from_numpy(np.arange(16, dtype=np.float64)).filter(
        fpstreams.fitem < 0.0
    )
    assert filtered_empty.with_engine("native").sum() == 0
    assert type(filtered_empty.with_engine("native").sum()) is int
    assert filtered_empty.with_engine("native").aggregate(total=fpstreams.agg.sum()) == {"total": 0}
    assert (
        type(filtered_empty.with_engine("native").aggregate(total=fpstreams.agg.sum())["total"])
        is int
    )

    nan = float("nan")
    leading_nan = fpstreams.flow.from_numpy(np.asarray([nan, 1.0], dtype=np.float64)).with_engine(
        "native"
    )
    assert math.isnan(leading_nan.min())
    assert math.isnan(leading_nan.max())

    signed_zero = fpstreams.flow.from_numpy(np.asarray([-0.0, 0.0], dtype=np.float64)).with_engine(
        "native"
    )
    assert math.copysign(1.0, signed_zero.min()) == -1.0
    assert math.copysign(1.0, signed_zero.max()) == -1.0


def test_flow_from_numpy_f64_fuses_expression_pipelines_without_layout_coercion() -> None:
    """Float expressions should reuse the generic kernel only for exact contiguous buffers."""
    np = pytest.importorskip("numpy")

    source = np.arange(32, dtype=np.float64)
    pipeline = (
        fpstreams.flow.from_numpy(source)
        .map(fpstreams.fitem * 1.5 + 0.25)
        .filter(fpstreams.fitem > 20.0)
    )
    expected = pipeline.with_engine("python").to_list()
    assert pipeline.with_engine("native").to_list() == expected
    assert pipeline.count() == len(expected)
    assert pipeline.with_engine("native").count() == len(expected)
    assert pipeline.aggregate(rows=fpstreams.agg.count()) == {"rows": len(expected)}
    assert pipeline.with_engine("native").sum() == pipeline.with_engine("python").sum()
    assert pipeline.explain("sum").to_dict()["selected_engine"] == "native"

    source[31] = 64.0
    assert pipeline.with_engine("native").to_list() == pipeline.with_engine("python").to_list()

    strided = fpstreams.flow.from_numpy(np.arange(64, dtype=np.float64)[::2])
    assert strided.explain("sum").to_dict()["selected_engine"] == "python"
    with pytest.raises(fpstreams.NativeUnsupportedError):
        strided.with_engine("native").sum()

    opposite_dtype = ">f8" if sys.byteorder == "little" else "<f8"
    opposite = fpstreams.flow.from_numpy(np.asarray([1.0, 2.0, 3.0], dtype=opposite_dtype))
    assert opposite.sum() == 6.0
    assert opposite.explain("sum").to_dict()["selected_engine"] == "python"


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("dtype", ["int64", "float64"])
@pytest.mark.parametrize("size", [3, 16])
def test_numpy_frequencies_reads_after_each_key_callback(
    engine: str, dtype: str, size: int
) -> None:
    """A key callback's array edit must be visible when the next value is read."""
    np = pytest.importorskip("numpy")
    values = np.arange(1, size + 1, dtype=dtype)
    calls = []

    def select(value):
        calls.append(value)
        if len(calls) == 1:
            values[1] = 9
        return value

    counts = fpstreams.flow.from_numpy(values).with_engine(engine).frequencies(select)
    expected_values = values.tolist()
    expected = {}
    for value in expected_values:
        expected[value] = expected.get(value, 0) + 1
    assert calls == expected_values
    assert counts == expected
    assert list(counts) == list(expected)


@pytest.mark.parametrize("dtype", ["int64", "float64"])
def test_numpy_frequency_report_records_callback_sensitive_python_execution(dtype: str) -> None:
    """The report separates a compiled native plan from the callback-safe terminal route."""
    np = pytest.importorskip("numpy")
    calls = []

    def select(value):
        calls.append(value)
        return value % 3

    result = fpstreams.flow.from_numpy(np.arange(16, dtype=dtype)).run_with_report(
        "frequencies", select
    )
    assert result.value == {0: 6, 1: 5, 2: 5}
    assert len(calls) == 16
    assert result.report.requested_engine == "auto"
    assert result.report.compiler_engine == "native"
    assert result.report.strategy == "python_frequency"


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.parametrize("change", ["grow", "shrink", "reshape", "dtype_width"])
@pytest.mark.filterwarnings("ignore:Setting the dtype on a NumPy array:DeprecationWarning")
def test_numpy_scalar_iteration_rejects_structure_changes_from_key(
    engine: str, change: str
) -> None:
    """Structural edits fail before a second value reaches the key callback."""
    np = pytest.importorskip("numpy")
    values = np.arange(16, dtype=np.int64)
    calls = []

    def select(value):
        calls.append(value)
        if change == "grow":
            values.resize(20, refcheck=False)
        elif change == "shrink":
            values.resize(0, refcheck=False)
        elif change == "reshape":
            values.resize((4, 4), refcheck=False)
        else:
            values.dtype = np.int32
        return value

    with pytest.raises(ValueError, match="retained array length changed during iteration"):
        fpstreams.flow.from_numpy(values).with_engine(engine).frequencies(select)
    assert calls == [0]


@pytest.mark.parametrize("engine", ["python", "auto"])
@pytest.mark.filterwarnings("ignore:Setting the dtype on a NumPy array:DeprecationWarning")
def test_numpy_scalar_iteration_reads_live_same_width_dtype(engine: str) -> None:
    """Changing dtype without changing length affects the next scalar's type and value."""
    np = pytest.importorskip("numpy")
    values = np.arange(16, dtype=np.int64)
    calls = []

    def select(value):
        calls.append(value)
        if len(calls) == 1:
            values.dtype = np.float64
            values[1:] = 3.5
        return value

    counts = fpstreams.flow.from_numpy(values).with_engine(engine).frequencies(select)
    assert counts == {0: 1, 3.5: 15}
    assert type(calls[0]) is int
    assert all(type(value) is float for value in calls[1:])


def test_numpy_scalar_iteration_keeps_custom_shape_protocol() -> None:
    """Nonstandard scalar sources keep their shape reads and never need a size property."""
    from fpstreams.tabular.numpy import _numpy_scalars

    calls = []

    class Array:
        ndim = 1

        @property
        def shape(self):
            calls.append("shape")
            return (2,)

        def item(self, index):
            calls.append(index)
            return index + 1

        @property
        def size(self):
            raise AssertionError("custom array size must not be read")

    assert list(_numpy_scalars(Array())) == [1, 2]
    assert calls == ["shape", "shape", 0, "shape", 1]


def test_numpy_adapter_count_tracks_a_resized_retained_array() -> None:
    """Live ndarray row cardinality stays aligned with lazy iteration and explain output."""
    np = pytest.importorskip("numpy")

    source = np.asarray([[1], [2]])
    table = fpstreams.rows.from_numpy(source, columns=["value"])
    assert table.count() == 2

    source.resize((3, 1), refcheck=False)
    source[2, 0] = 3

    assert table.count() == 3
    assert table.to_list() == [{"value": 1}, {"value": 2}, {"value": 3}]
    explanation = table.explain("count").to_dict()
    assert explanation["source"]["exact_size"] == 3
    assert explanation["semantics"]["output"]["cardinality"] == {
        "kind": "exact",
        "value": 3,
    }

    source.resize((1, 3), refcheck=False)
    with pytest.raises(ValueError, match="retained array width changed"):
        table.count()
    with pytest.raises(ValueError, match="retained array width changed"):
        table.explain("count")
    with pytest.raises(ValueError, match="retained array width changed"):
        table.to_list()

    changing = np.array([[0, 1], [2, 3]])
    iterator = iter(fpstreams.rows.from_numpy(changing, columns=("left", "right")))
    assert next(iterator) == {"left": 0, "right": 1}
    changing.resize((2, 3), refcheck=False)
    with pytest.raises(ValueError, match="width changed during iteration"):
        next(iterator)


def test_numpy_adapter_validates_shape_and_column_names() -> None:
    np = pytest.importorskip("numpy")

    with pytest.raises(ValueError, match="two-dimensional"):
        fpstreams.rows.from_numpy(np.asarray([1, 2]))
    with pytest.raises(ValueError, match="2 columns"):
        fpstreams.rows.from_numpy(np.asarray([[1, 2]]), columns=["only"])
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column 'value'"):
        fpstreams.rows.from_numpy(
            np.asarray([[1, 2]]),
            columns=["value", "value"],
        )
    with pytest.raises(TypeError, match="column names must be strings"):
        fpstreams.rows.from_numpy(np.asarray([[1]]), columns=[0])
    with pytest.raises(ValueError, match="column names cannot be empty"):
        fpstreams.rows.from_numpy(np.asarray([[1]]), columns=[""])


def test_rows_to_numpy_preserves_selectors_dtype_and_empty_shapes() -> None:
    np = pytest.importorskip("numpy")

    table = fpstreams.rows([{"a": 1}, {"a": 2, "b": 3}])
    matrix = table.to_numpy(dtype=object)
    selected = fpstreams.rows([{"a": 1, "b": None}, {"a": 2, "b": 3}]).to_numpy(
        "b",
        lambda row: row["a"] * 10,
        dtype=object,
    )

    assert matrix.shape == (2, 2)
    assert matrix.tolist() == [[1, None], [2, 3]]
    assert selected.shape == (2, 2)
    assert selected.dtype == np.dtype("object")
    assert selected.tolist() == [[None, 10], [3, 20]]
    empty = fpstreams.rows([]).to_numpy()
    typed_empty = fpstreams.rows([]).to_numpy("a", "b", dtype=np.int16)
    retained_empty = fpstreams.rows.from_numpy(np.empty((0, 3), dtype=np.float32)).to_numpy()
    assert empty.shape == (0, 0)
    assert empty.dtype == np.dtype("float64")
    assert typed_empty.shape == (0, 2)
    assert typed_empty.dtype == np.dtype("int16")
    assert retained_empty.shape == (0, 3)
    assert retained_empty.dtype == np.dtype("float32")

    with pytest.raises(fpstreams.SelectionError, match="missing"):
        fpstreams.rows([{"a": 1}]).to_numpy("missing")


def test_rows_to_numpy_direct_fields_bypass_generated_selectors_for_exact_dicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact dictionaries use the direct field path while mixed rows retain fallback semantics."""
    from fpstreams.tabular import io as tabular_io

    original_compile = tabular_io.compile_selector
    selected_rows: list[object] = []

    def compile_tracked(selector: object) -> Any:
        compiled = original_compile(selector)  # type: ignore[arg-type]

        def tracked(row: object) -> object:
            selected_rows.append(row)
            return compiled(row)

        return tracked

    class DictSubclass(dict[str, int]):
        pass

    exact = {"a": 1, "b": 2, "c": 3}
    fallback = DictSubclass(a=4, b=5, c=6)
    monkeypatch.setattr(tabular_io, "compile_selector", compile_tracked)

    matrix = fpstreams.rows([exact, fallback]).to_numpy("a", "b", "c")

    assert matrix.tolist() == [[1, 2, 3], [4, 5, 6]]
    assert selected_rows == [fallback, fallback, fallback]

    class CollidingKey:
        def __hash__(self) -> int:
            return hash("b")

        def __eq__(self, _other: object) -> bool:
            raise TypeError("broken key equality")

    with pytest.raises(fpstreams.SelectionError, match="'b'") as captured:
        fpstreams.rows([{"a": 1, CollidingKey(): 2}]).to_numpy("a", "b")
    assert isinstance(captured.value.__cause__, TypeError)


def test_rows_to_numpy_direct_fields_preserve_nested_dtype_shapes() -> None:
    """Direct selectors retain the list-of-rows input contract passed to NumPy."""
    np = pytest.importorskip("numpy")
    records = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    structured_dtype = np.dtype([("left", "i4"), ("right", "i4")])
    subarray_dtype = np.dtype(("i4", (2,)))

    direct_structured = fpstreams.rows(records).to_numpy("a", "b", dtype=structured_dtype)
    fallback_structured = fpstreams.rows(records).to_numpy(
        lambda row: row["a"], lambda row: row["b"], dtype=structured_dtype
    )
    direct_subarray = fpstreams.rows(records).to_numpy("a", dtype=subarray_dtype)
    fallback_subarray = fpstreams.rows(records).to_numpy(lambda row: row["a"], dtype=subarray_dtype)

    assert direct_structured.shape == fallback_structured.shape == (2, 2)
    assert direct_structured.dtype == fallback_structured.dtype
    assert direct_structured.tolist() == fallback_structured.tolist()
    assert direct_subarray.shape == fallback_subarray.shape == (2, 1, 2)
    assert direct_subarray.dtype == fallback_subarray.dtype
    assert direct_subarray.tolist() == fallback_subarray.tolist()


def test_rows_to_numpy_obeys_installed_numpy_copy_modes() -> None:
    np = pytest.importorskip("numpy")

    source = np.asarray([[1, 2], [3, 4]], dtype=np.int64)
    table = fpstreams.rows.from_numpy(source, columns=["a", "b"])

    reused = table.to_numpy(copy=None)
    required_view = table.to_numpy(copy=False)
    copied = table.to_numpy(copy=True)

    assert np.shares_memory(reused, source)
    assert np.shares_memory(required_view, source)
    assert not np.shares_memory(copied, source)
    if int(np.__version__.split(".")[0]) >= 2:
        with pytest.raises(ValueError, match="copy"):
            table.to_numpy(dtype=np.float64, copy=False)
        with pytest.raises(ValueError, match="copy"):
            fpstreams.rows([{"a": 1}]).to_numpy(copy=False)
    else:
        np.testing.assert_array_equal(
            table.to_numpy(dtype=np.float64, copy=False),
            np.array(source, dtype=np.float64, copy=False),
        )
        np.testing.assert_array_equal(
            fpstreams.rows([{"a": 1}]).to_numpy(copy=False),
            np.array([[1]], copy=False),
        )


def test_numpy_group_planner_marks_only_direct_integer_columnar_shapes() -> None:
    """Planning should expose the bounded NumPy candidate without widening its contract."""
    np = pytest.importorskip("numpy")
    from fpstreams.physical.relational import GroupAggregatePhysicalNode, NumpyGroupAggregateSpec
    from fpstreams.planning.compiler import compile_query

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2], [1, 3]], dtype=np.int64),
        columns=("key", "value"),
    )
    grouped = source.group_by("key").aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    physical = compile_query(grouped._flow._query("list"))
    assert isinstance(physical.root, GroupAggregatePhysicalNode)
    assert isinstance(physical.root.numpy_group, NumpyGroupAggregateSpec)
    assert tuple(lane.kind for lane in physical.root.numpy_group.lanes) == (
        "count",
        "sum",
        "min",
        "max",
    )

    unsupported = (
        source.with_engine("python").group_by("key").aggregate(total=fpstreams.agg.sum("value")),
        source.group_by(lambda row: row["key"]).aggregate(total=fpstreams.agg.sum("value")),
        fpstreams.rows.from_numpy(
            np.asarray([[1.0, 2.0]], dtype=np.float64),
            columns=("key", "value"),
        )
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value")),
    )
    for query in unsupported:
        fallback = compile_query(query._flow._query("list"))
        assert isinstance(fallback.root, GroupAggregatePhysicalNode)
        assert fallback.root.numpy_group is None


def test_numpy_group_aggregate_is_bounded_columnar_single_pass_and_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One chunk factorization should feed every lane and preserve first-seen order."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    matrix = np.asarray(
        [[2, 4], [1, -7], [2, 3], [3, 9], [1, 5], [2, -8], [3, 1]],
        dtype=np.int64,
    )
    source = fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
    query = source.group_by("key").aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
        total_again=fpstreams.agg.sum("value"),
        low=fpstreams.agg.min("value"),
        high=fpstreams.agg.max("value"),
    )
    original_factorization = numpy_group._factorize_numpy_group_keys
    chunk_sizes: list[int] = []

    def bounded_factorization(module: object, values: object) -> object:
        chunk_sizes.append(len(values))  # type: ignore[arg-type]
        assert len(values) <= 3  # type: ignore[arg-type]
        return original_factorization(module, values)

    def forbidden_rows(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("supported NumPy grouping must not enter the row executor")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 3)
    monkeypatch.setattr(relational, "_execute_python_group_values", forbidden_rows)
    monkeypatch.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
    monkeypatch.setattr(numpy_group, "_factorize_numpy_group_keys", bounded_factorization)
    execution = query.run_with_report("to_list")

    expected = [
        {"key": 2, "rows": 3, "total": -1, "total_again": -1, "low": -8, "high": 4},
        {"key": 1, "rows": 2, "total": -2, "total_again": -2, "low": -7, "high": 5},
        {"key": 3, "rows": 2, "total": 10, "total_again": 10, "low": 1, "high": 9},
    ]
    assert execution.value == expected
    assert chunk_sizes == [3, 3]
    assert execution.report.strategy == "numpy_direct"
    chunk_sizes.clear()
    assert list(query) == expected
    assert chunk_sizes == [3, 3]


def test_numpy_group_native_state_reduces_i64_chunks_without_numpy_factorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One stable native scan should serve repeated lanes and multiple bounded chunks."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    matrix = np.asarray(
        [
            [2, 4],
            [1, -7],
            [2, 3],
            [3, 9],
            [1, 5],
            [2, -8],
            [3, 1],
        ],
        dtype=np.int64,
    )
    query = (
        fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            total_again=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
    )

    def forbidden_factorization(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("exact i64 chunks should use the native grouped state")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 3)
    monkeypatch.setattr(
        numpy_group,
        "_factorize_numpy_group_keys",
        forbidden_factorization,
    )

    execution = query.run_with_report("to_list")

    assert execution.value == [
        {"key": 2, "rows": 3, "total": -1, "total_again": -1, "low": -8, "high": 4},
        {"key": 1, "rows": 2, "total": -2, "total_again": -2, "low": -7, "high": 5},
        {"key": 3, "rows": 2, "total": 10, "total_again": 10, "low": 1, "high": 9},
    ]
    assert execution.report.strategy == "numpy_direct"


def test_numpy_group_native_state_reads_strided_source_columns_without_v1_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The GIL build should aggregate direct column views without a packed intermediate."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native
    from fpstreams.execution import relational

    matrix = np.asarray(
        [
            [2, 40, 4],
            [1, 70, -7],
            [2, 30, 3],
            [3, 90, 9],
            [1, 50, 5],
            [2, 80, -8],
            [3, 10, 1],
        ],
        dtype=np.int64,
    )
    query = (
        fpstreams.rows.from_numpy(matrix, columns=("key", "payload", "value"))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
    )
    real_partial = _native.numpy_group_strided_partial_v2
    observed_strides: list[tuple[tuple[int, ...], tuple[int, ...]]] = []

    def tracked_partial(keys: object, values: object, mask: int) -> object:
        observed_strides.append((keys.strides, values.strides))  # type: ignore[attr-defined]
        return real_partial(keys, values, mask)

    def forbidden_v1(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("the GIL build should not repack stable NumPy columns")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 3)
    monkeypatch.setattr(_native, "numpy_group_partial_v1", forbidden_v1)
    monkeypatch.setattr(_native, "numpy_group_strided_partial_v2", tracked_partial)

    assert query.to_list() == [
        {"key": 2, "rows": 3, "total": -1, "low": -8, "high": 4},
        {"key": 1, "rows": 2, "total": -2, "low": -7, "high": 5},
        {"key": 3, "rows": 2, "total": 10, "low": 1, "high": 9},
    ]
    assert observed_strides == [((24,), (24,)), ((24,), (24,)), ((24,), (24,))]


def test_numpy_group_native_partial_commits_only_after_live_revalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shortened retained matrix must discard and recompute its pending native chunk."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native
    from fpstreams.execution import numpy_group, relational

    live = np.asarray([[2, 10], [1, 20], [3, 30]], dtype=np.int64)
    query = (
        fpstreams.rows.from_numpy(live, columns=("key", "value"))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
            low=fpstreams.agg.min("value"),
            high=fpstreams.agg.max("value"),
        )
    )
    real_partial = _native.numpy_group_strided_partial_v2
    real_commit = _native.numpy_group_commit_v1
    partial_sizes: list[int] = []
    commits = 0

    def shrinking_partial(keys: object, values: object, mask: int) -> object:
        partial = real_partial(keys, values, mask)
        partial_sizes.append(len(keys))  # type: ignore[arg-type]
        if len(partial_sizes) == 1:
            live.resize((2, 2), refcheck=False)
        return partial

    def tracked_commit(state: object, partial: object) -> None:
        nonlocal commits
        commits += 1
        real_commit(state, partial)

    def forbidden_factorization(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("a stable retry should remain in the native grouped state")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 3)
    monkeypatch.setattr(_native, "numpy_group_strided_partial_v2", shrinking_partial)
    monkeypatch.setattr(_native, "numpy_group_commit_v1", tracked_commit)
    monkeypatch.setattr(
        numpy_group,
        "_factorize_numpy_group_keys",
        forbidden_factorization,
    )

    assert query.to_list() == [
        {"key": 2, "rows": 1, "total": 10, "low": 10, "high": 10},
        {"key": 1, "rows": 1, "total": 20, "low": 20, "high": 20},
    ]
    assert partial_sizes == [3, 2]
    assert commits == 1


def test_numpy_group_native_decline_discards_state_and_restarts_numpy_from_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later ABI decline must not leak already committed native rows into fallback."""
    np = pytest.importorskip("numpy")
    from fpstreams import _native
    from fpstreams.execution import numpy_group, relational

    matrix = np.asarray(
        [[2, 10], [1, 20], [2, 30], [3, 40], [1, 50], [3, 60]],
        dtype=np.int64,
    )
    query = (
        fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    real_partial = _native.numpy_group_strided_partial_v2
    original_factorization = numpy_group._factorize_numpy_group_keys
    partial_sizes: list[int] = []
    fallback_chunks: list[tuple[int, ...]] = []

    def declining_partial(keys: object, values: object, mask: int) -> object:
        partial_sizes.append(len(keys))  # type: ignore[arg-type]
        if len(partial_sizes) == 2:
            return None
        return real_partial(keys, values, mask)

    def tracked_factorization(module: object, keys: object) -> object:
        fallback_chunks.append(tuple(int(key) for key in keys))  # type: ignore[union-attr]
        return original_factorization(module, keys)

    def forbidden_finalize(_state: object) -> object:
        raise AssertionError("a declined native state must never be finalized")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 3)
    monkeypatch.setattr(_native, "numpy_group_strided_partial_v2", declining_partial)
    monkeypatch.setattr(_native, "numpy_group_finalize_v1", forbidden_finalize)
    monkeypatch.setattr(
        numpy_group,
        "_factorize_numpy_group_keys",
        tracked_factorization,
    )

    assert query.to_list() == [
        {"key": 2, "total": 40},
        {"key": 1, "total": 70},
        {"key": 3, "total": 100},
    ]
    assert partial_sizes == [3, 3]
    assert fallback_chunks == [(2, 1, 2), (3, 1, 3)]


def test_numpy_group_reuses_only_a_closed_compact_domain_with_a_count_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Holes, missing counts, and wide domains must retain full factorization."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    original_factorization = numpy_group._factorize_numpy_group_keys

    def factorized_chunks(
        keys: list[int],
        *,
        include_count: bool,
        chunk_rows: int = 4,
    ) -> tuple[list[tuple[int, ...]], list[dict[str, object]]]:
        matrix = np.column_stack(
            (np.asarray(keys, dtype=np.int64), np.arange(len(keys), dtype=np.int64))
        )
        source = fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
        if include_count:
            query = source.group_by("key").aggregate(
                rows=fpstreams.agg.count(),
                total=fpstreams.agg.sum("value"),
            )
        else:
            query = source.group_by("key").aggregate(total=fpstreams.agg.sum("value"))
        chunks: list[tuple[int, ...]] = []

        def tracked_factorization(module: object, values: object) -> object:
            chunks.append(tuple(int(value) for value in values))  # type: ignore[union-attr]
            return original_factorization(module, values)

        with monkeypatch.context() as scoped:
            scoped.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", chunk_rows)
            scoped.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
            scoped.setattr(
                numpy_group,
                "_factorize_numpy_group_keys",
                tracked_factorization,
            )
            result = query.to_list()
        return chunks, result

    hole_chunks, hole_result = factorized_chunks(
        [0, 2, 0, 2, 0, 1, 2, 0, 2, 1, 0, 2],
        include_count=True,
    )
    assert hole_chunks == [(0, 2, 0, 2), (0, 1, 2, 0)]
    assert hole_result == [
        {"key": 0, "rows": 5, "total": 23},
        {"key": 2, "rows": 5, "total": 29},
        {"key": 1, "rows": 2, "total": 14},
    ]

    no_count_chunks, no_count_result = factorized_chunks(
        [0, 1, 0, 1, 1, 0, 1, 0],
        include_count=False,
    )
    assert no_count_chunks == [(0, 1, 0, 1), (1, 0, 1, 0)]
    assert no_count_result == [
        {"key": 0, "total": 14},
        {"key": 1, "total": 14},
    ]

    high_cardinality_chunks, high_cardinality_result = factorized_chunks(
        [0, 100, 200, 300, 300, 200, 100, 0],
        include_count=True,
    )
    assert high_cardinality_chunks == [
        (0, 100, 200, 300),
        (300, 200, 100, 0),
    ]
    assert high_cardinality_result == [
        {"key": 0, "rows": 2, "total": 7},
        {"key": 100, "rows": 2, "total": 7},
        {"key": 200, "rows": 2, "total": 7},
        {"key": 300, "rows": 2, "total": 7},
    ]

    tail_chunks, tail_result = factorized_chunks(
        [index % 65 for index in range(1_040)] + [0],
        include_count=True,
        chunk_rows=1_040,
    )
    assert [len(chunk) for chunk in tail_chunks] == [1_040, 1]
    assert tail_chunks[-1] == (0,)
    assert tail_result[0] == {"key": 0, "rows": 17, "total": 8_840}


def test_numpy_group_closed_domain_reuse_covers_fixed_width_integer_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Known dense codes remain exact at signed, unsigned, boolean, and endian edges."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    opposite = ">" if sys.byteorder == "little" else "<"
    cases = (
        (np.dtype(np.int64), -(2**63), -(2**63) + 1),
        (np.dtype(np.uint64), 2**64 - 2, 2**64 - 1),
        (np.dtype(np.bool_), False, True),
        (np.dtype(f"{opposite}i8"), -(2**63), -(2**63) + 1),
        (np.dtype(f"{opposite}u8"), 2**64 - 2, 2**64 - 1),
    )
    original_factorization = numpy_group._factorize_numpy_group_keys

    def tracking_factorization(sizes: list[int]) -> object:
        def tracked(module: object, values: object) -> object:
            sizes.append(len(values))  # type: ignore[arg-type]
            return original_factorization(module, values)

        return tracked

    for dtype, lower, upper in cases:
        matrix = np.empty((8, 2), dtype=dtype)
        matrix[:, 0] = [upper, lower, upper, lower, lower, upper, lower, upper]
        matrix[:, 1] = True if dtype.kind == "b" else 1
        query = (
            fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
            .group_by("key")
            .aggregate(
                rows=fpstreams.agg.count(),
                total=fpstreams.agg.sum("value"),
                low=fpstreams.agg.min("value"),
                high=fpstreams.agg.max("value"),
            )
        )
        factorized_sizes: list[int] = []

        with monkeypatch.context() as scoped:
            scoped.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 4)
            scoped.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
            scoped.setattr(
                numpy_group,
                "_factorize_numpy_group_keys",
                tracking_factorization(factorized_sizes),
            )
            result = query.to_list()

        unit = True if dtype.kind == "b" else 1
        assert factorized_sizes == [4]
        assert result == [
            {"key": upper, "rows": 4, "total": 4, "low": unit, "high": unit},
            {"key": lower, "rows": 4, "total": 4, "low": unit, "high": unit},
        ]
        expected_key_type = bool if dtype.kind == "b" else int
        assert all(type(row["key"]) is expected_key_type for row in result)


def test_numpy_group_single_chunk_does_not_build_an_unused_dense_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A terminal chunk keeps the original factorizer without a state-only rescan."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    matrix = np.asarray([[1, 2], [0, 3], [1, 4], [0, 5]], dtype=np.int64)
    query = (
        fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )

    def forbidden_domain(*_args: object) -> object:
        raise AssertionError("the final chunk cannot reuse newly discovered state")

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", len(matrix))
    monkeypatch.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
    monkeypatch.setattr(numpy_group, "_closed_numpy_group_domain", forbidden_domain)

    assert query.to_list() == [
        {"key": 1, "rows": 2, "total": 6},
        {"key": 0, "rows": 2, "total": 8},
    ]


def test_numpy_group_closed_domain_rechecks_a_shrinking_live_array(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dense-state reuse must recompute a partial chunk after retained-array shrinkage."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    live = np.asarray(
        [[1, 10], [0, 1], [1, 20], [0, 2], [1, 30], [0, 3], [1, 40], [0, 4]],
        dtype=np.int64,
    )
    query = (
        fpstreams.rows.from_numpy(live, columns=("key", "value"))
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )
    original_reuse = numpy_group._factorize_closed_numpy_group_domain
    reuse_sizes: list[int] = []

    def shrink_after_reuse(module: object, values: object, domain: object) -> object:
        result = original_reuse(module, values, domain)  # type: ignore[arg-type]
        reuse_sizes.append(len(values))  # type: ignore[arg-type]
        if len(reuse_sizes) == 1:
            live.resize((6, 2), refcheck=False)
        return result

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 4)
    monkeypatch.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
    monkeypatch.setattr(
        numpy_group,
        "_factorize_closed_numpy_group_domain",
        shrink_after_reuse,
    )

    assert query.to_list() == [
        {"key": 1, "rows": 3, "total": 60},
        {"key": 0, "rows": 3, "total": 6},
    ]
    assert reuse_sizes == [4, 2]


def test_numpy_group_closed_domain_discards_speculative_state_after_regrowth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shrink retry cannot reuse removed keys to reorder a subsequently grown tail."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    live = np.asarray(
        [
            [3, 30],
            [0, 0],
            [1, 10],
            [2, 20],
            [3, 31],
            [0, 1],
            [1, 11],
            [2, 21],
        ],
        dtype=np.int64,
    )
    query = (
        fpstreams.rows.from_numpy(live, columns=("key", "value"))
        .group_by("key")
        .aggregate(rows=fpstreams.agg.count(), total=fpstreams.agg.sum("value"))
    )
    original_factorization = numpy_group._factorize_numpy_group_keys
    calls = 0

    def resize_between_retries(module: object, values: object) -> object:
        nonlocal calls
        result = original_factorization(module, values)
        calls += 1
        if calls == 1:
            live.resize((2, 2), refcheck=False)
        elif calls == 2:
            live.resize((4, 2), refcheck=False)
            live[2:] = [[2, 200], [1, 100]]
        return result

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 4)
    monkeypatch.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)
    monkeypatch.setattr(
        numpy_group,
        "_factorize_numpy_group_keys",
        resize_between_retries,
    )

    assert query.to_list() == [
        {"key": 3, "rows": 1, "total": 30},
        {"key": 0, "rows": 1, "total": 0},
        {"key": 2, "rows": 1, "total": 200},
        {"key": 1, "rows": 1, "total": 100},
    ]


@pytest.mark.parametrize(
    ("names", "values", "expected"),
    [
        (
            ("left", "right"),
            [[10, 20], [30, 40]],
            [
                {"key": 2, "left": 10, "right": 30},
                {"key": 1, "left": 20, "right": 40},
            ],
        ),
        (
            ("first", "second", "third"),
            [[10, 20], [30, 40], [50, 60]],
            [
                {"key": 2, "first": 10, "second": 30, "third": 50},
                {"key": 1, "first": 20, "second": 40, "third": 60},
            ],
        ),
        (
            ("first", "second", "third", "fourth"),
            [[10, 20], [30, 40], [50, 60], [70, 80]],
            [
                {
                    "key": 2,
                    "first": 10,
                    "second": 30,
                    "third": 50,
                    "fourth": 70,
                },
                {
                    "key": 1,
                    "first": 20,
                    "second": 40,
                    "third": 60,
                    "fourth": 80,
                },
            ],
        ),
    ],
)
def test_numpy_group_fixed_width_materialization_reads_lane_metadata_once(
    names: tuple[str, ...],
    values: list[list[int]],
    expected: list[dict[str, int]],
) -> None:
    """Two-to-four-lane output must not rescan lane metadata for every group."""
    from fpstreams.execution.numpy_group import _materialize_numpy_group_rows
    from fpstreams.physical.relational import NumpyGroupLaneSpec

    class CountingLaneStates(list[list[int]]):
        iterations = 0

        def __iter__(self) -> Iterator[list[int]]:
            self.iterations += 1
            return super().__iter__()

    lane_states = CountingLaneStates(values)
    lanes = tuple(NumpyGroupLaneSpec(name, "sum", "value") for name in names)

    result = _materialize_numpy_group_rows([2, 1], lane_states, "key", lanes)

    assert result == expected
    assert lane_states.iterations == 0


def test_numpy_group_aggregate_keeps_python_integer_and_boolean_semantics() -> None:
    """Chunk partials must never wrap, and NumPy scalar keys must become Python scalars."""
    np = pytest.importorskip("numpy")
    maximum = 2**63 - 1
    cases = (
        (
            np.asarray([[1, 120], [1, 120], [-1, -120]], dtype=np.int8),
            [
                {"key": 1, "rows": 2, "total": 240, "low": 120, "high": 120},
                {"key": -1, "rows": 1, "total": -120, "low": -120, "high": -120},
            ],
        ),
        (
            np.asarray([[1, maximum], [1, maximum]], dtype=np.int64),
            [{"key": 1, "rows": 2, "total": maximum * 2, "low": maximum, "high": maximum}],
        ),
        (
            np.asarray([[1, 2**64 - 1], [1, 2**64 - 1]], dtype=np.uint64),
            [
                {
                    "key": 1,
                    "rows": 2,
                    "total": (2**64 - 1) * 2,
                    "low": 2**64 - 1,
                    "high": 2**64 - 1,
                }
            ],
        ),
        (
            np.asarray([[True, True], [False, True], [True, False]], dtype=np.bool_),
            [
                {"key": True, "rows": 2, "total": 1, "low": False, "high": True},
                {"key": False, "rows": 1, "total": 1, "low": True, "high": True},
            ],
        ),
        (
            np.asarray([[2, 4], [1, -3], [2, 5]], dtype=">i8"),
            [
                {"key": 2, "rows": 2, "total": 9, "low": 4, "high": 5},
                {"key": 1, "rows": 1, "total": -3, "low": -3, "high": -3},
            ],
        ),
    )
    for matrix, expected in cases:
        result = (
            fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
            .group_by("key")
            .aggregate(
                rows=fpstreams.agg.count(),
                total=fpstreams.agg.sum("value"),
                low=fpstreams.agg.min("value"),
                high=fpstreams.agg.max("value"),
            )
            .to_list()
        )
        assert result == expected
        assert all(
            type(row["key"]) is type(expected_row["key"])
            for row, expected_row in zip(result, expected, strict=True)
        )
        assert all(
            type(row[name]) is type(expected_row[name])
            for row, expected_row in zip(result, expected, strict=True)
            for name in ("rows", "total", "low", "high")
        )

    empty = np.empty((0, 1), dtype=np.int64)
    assert (
        fpstreams.rows.from_numpy(empty, columns=("present",))
        .group_by("missing")
        .aggregate(total=fpstreams.agg.sum("also_missing"))
        .to_list()
    ) == []
    with pytest.raises(fpstreams.SelectionError) as missing:
        (
            fpstreams.rows.from_numpy(np.asarray([[1]], dtype=np.int64), columns=("present",))
            .group_by("missing")
            .aggregate(total=fpstreams.agg.sum("also_missing"))
            .to_list()
        )
    assert isinstance(missing.value.__cause__, KeyError)

    maximum_key = 2**63 - 1
    assert (
        fpstreams.rows.from_numpy(
            np.asarray([[maximum_key], [maximum_key]], dtype=np.int64),
            columns=("value",),
        )
        .group_by(bucket="value")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
    ) == [{"bucket": maximum_key, "total": maximum_key * 2}]


def test_numpy_group_low_cardinality_overflow_sum_stays_columnar() -> None:
    """Exact wide totals must not fall back to a Python loop for every source row."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.numpy_group import _exact_numpy_group_sum

    class NoRowList(np.ndarray):
        def tolist(self) -> list[object]:
            raise AssertionError("low-cardinality exact grouping must remain columnar")

    maximum = 2**63 - 1
    values = np.asarray([maximum, -maximum] * 8, dtype=np.int64).view(NoRowList)
    inverse = np.asarray([0, 1] * 8, dtype=np.intp).view(NoRowList)

    totals = _exact_numpy_group_sum(np, values, inverse, 2)

    assert list(totals) == [maximum * 8, -maximum * 8]
    assert all(type(value) is int for value in totals)


def test_numpy_group_float_exact_integer_sum_uses_bincount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Safely bounded integer sums should use the faster exact weighted reduction."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.numpy_group import _exact_numpy_group_sum

    values = np.asarray([100_000, -100_000, 7, -3] * 8, dtype=np.int32)
    inverse = np.arange(32, dtype=np.intp) % 4
    original_bincount = np.bincount
    calls = 0

    def tracked_bincount(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return original_bincount(*args, **kwargs)

    monkeypatch.setattr(np, "bincount", tracked_bincount)

    totals = _exact_numpy_group_sum(np, values, inverse, 4)

    assert totals.tolist() == [800_000, -800_000, 56, -24]
    assert totals.dtype == np.dtype(np.int64)
    assert calls == 1

    one_group = _exact_numpy_group_sum(np, values, np.zeros(32, dtype=np.intp), 1)
    assert one_group.tolist() == [32]
    assert calls == 1

    native_i64 = values.astype(np.int64)
    assert _exact_numpy_group_sum(np, native_i64, inverse, 4).tolist() == [
        800_000,
        -800_000,
        56,
        -24,
    ]
    assert calls == 1


def test_numpy_group_keeps_internal_list_checks_stable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An implementation-only list lookup must not change public grouped results or routing."""
    import builtins

    np = pytest.importorskip("numpy")
    maximum = 2**63 - 1
    query = (
        fpstreams.rows.from_numpy(
            np.asarray([[1, maximum], [1, maximum]], dtype=np.int64),
            columns=("key", "value"),
        )
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )
    from fpstreams.execution import relational

    def forbidden_rows(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("an implementation-only list replacement must stay columnar")

    monkeypatch.setattr(relational, "_execute_python_group_values", forbidden_rows)
    original_list = builtins.list
    try:
        builtins.list = lambda *_args, **_kwargs: ["changed"]
        result = tuple(query)
    finally:
        builtins.list = original_list

    assert result == ({"key": 1, "total": maximum * 2},)


@pytest.mark.parametrize(
    ("dtype_name", "left", "right"),
    [
        ("int64", -(2**63), 2**63 - 1),
        ("uint64", 2**64 - 1, 2**63),
        ("opposite_i64", -(2**63), 2**63 - 1),
        ("opposite_u64", 2**64 - 1, 2**63),
    ],
)
def test_numpy_group_exact_limb_sum_covers_extremes_and_byte_orders(
    dtype_name: str,
    left: int,
    right: int,
) -> None:
    """Both signed limbs and endian conversions must remain exact without row boxing."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution.numpy_group import _exact_numpy_group_sum

    class NoRowList(np.ndarray):
        def tolist(self) -> list[object]:
            raise AssertionError("the exact limb path must not box source rows")

    dtype = {
        "int64": "int64",
        "uint64": "uint64",
        "opposite_i64": ">i8" if sys.byteorder == "little" else "<i8",
        "opposite_u64": ">u8" if sys.byteorder == "little" else "<u8",
    }[dtype_name]
    values = np.asarray([left, right] * 8, dtype=dtype).view(NoRowList)
    inverse = np.asarray([0, 1] * 8, dtype=np.intp).view(NoRowList)

    totals = _exact_numpy_group_sum(np, values, inverse, 2)

    assert totals == [left * 8, right * 8]
    assert all(type(value) is int for value in totals)


def test_numpy_group_exact_limb_sum_keeps_high_cardinality_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The measured low-cardinality kernel must not penalize one-group-per-row data."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group

    def forbidden_limb(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("high-cardinality groups must keep the Python fallback")

    monkeypatch.setattr(numpy_group, "_exact_limb_group_sum", forbidden_limb)
    maximum = 2**63 - 1
    values = np.full(16, maximum, dtype=np.int64)
    inverse = np.arange(16, dtype=np.intp)

    assert numpy_group._exact_numpy_group_sum(np, values, inverse, 16) == [maximum] * 16


def test_numpy_group_exact_limb_partials_merge_across_bounded_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Python integer state must merge limb partials without wrapping or reordering keys."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational

    assert relational._NUMPY_GROUP_CHUNK_ROWS <= numpy_group._MAX_EXACT_LIMB_ROWS
    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 16)
    maximum = 2**63 - 1
    minimum = -(2**63)
    matrix = np.asarray([[2, maximum], [1, minimum]] * 20, dtype=np.int64)

    result = (
        fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
        .group_by("key")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("value"),
        )
        .to_list()
    )

    assert result == [
        {"key": 2, "rows": 20, "total": maximum * 20},
        {"key": 1, "rows": 20, "total": minimum * 20},
    ]


def test_numpy_group_aggregate_deopts_for_failpoints_and_tracks_source_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failpoints and live shape changes keep their canonical boundaries."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, relational
    from fpstreams.runtime.failpoints import failpoint

    matrix = np.asarray([[1, 2], [1, 3]], dtype=np.int64)
    source = fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
    expected = [{"key": 1, "total": 5}]

    def forbidden_columnar(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("observable execution must bypass NumPy grouped columns")

    original_group_aggregate = numpy_group._numpy_group_aggregate
    monkeypatch.setattr(numpy_group, "_numpy_group_aggregate", forbidden_columnar)
    assert (
        source.with_engine("python")
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
        .to_list()
        == expected
    )
    with failpoint("unrelated.numpy.group", RuntimeError("unused")):
        assert (
            source.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list() == expected
        )

    monkeypatch.setattr(numpy_group, "_numpy_group_aggregate", original_group_aggregate)

    changing = np.asarray([[1, 2], [1, 3], [2, 4], [2, 5]], dtype=np.int64)
    changing_source = fpstreams.rows.from_numpy(changing, columns=("key", "value"))
    original_factorization = numpy_group._factorize_numpy_group_keys
    calls = 0

    monkeypatch.setattr(numpy_group, "_native_numpy_group_endpoints", lambda: None)

    def reshape_after_first_chunk(module: object, values: object) -> object:
        nonlocal calls
        result = original_factorization(module, values)
        calls += 1
        if calls == 1:
            changing.resize((changing.size,), refcheck=False)
        return result

    monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", 2)
    monkeypatch.setattr(numpy_group, "_factorize_numpy_group_keys", reshape_after_first_chunk)
    with pytest.raises(ValueError, match="changed to 1 dimensions during iteration"):
        changing_source.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()

    def aggregate_after_resize(
        initial: list[list[int]],
        resized: list[list[int]],
        *,
        chunk_rows: int,
    ) -> list[dict[str, object]]:
        live = np.asarray(initial, dtype=np.int64)
        live_query = (
            fpstreams.rows.from_numpy(live, columns=("key", "value"))
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
        )
        resize_calls = 0

        def resize_after_first_chunk(module: object, values: object) -> object:
            nonlocal resize_calls
            result = original_factorization(module, values)
            resize_calls += 1
            if resize_calls == 1:
                live.resize((len(resized), 2), refcheck=False)
                live[:] = resized
            return result

        monkeypatch.setattr(relational, "_NUMPY_GROUP_CHUNK_ROWS", chunk_rows)
        monkeypatch.setattr(numpy_group, "_factorize_numpy_group_keys", resize_after_first_chunk)
        return live_query.to_list()

    assert aggregate_after_resize(
        [[1, 2], [1, 3], [2, 4], [2, 5]],
        [[1, 2]],
        chunk_rows=3,
    ) == [{"key": 1, "total": 2}]
    assert aggregate_after_resize(
        [[1, 2], [1, 3]],
        [[1, 2], [1, 3], [2, 4], [1, 7]],
        chunk_rows=2,
    ) == [{"key": 1, "total": 12}, {"key": 2, "total": 4}]


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="requires POSIX interval timers")
@pytest.mark.parametrize("changed_dtype", ["float64", "uint64"])
def test_numpy_group_matches_python_during_signal_time_same_width_dtype_changes(
    monkeypatch: pytest.MonkeyPatch,
    changed_dtype: str,
) -> None:
    """An active timer keeps public auto grouping on Python's live dtype boundary."""
    import warnings

    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group
    from fpstreams.tabular import numpy as numpy_adapter

    canonical_validation = numpy_adapter._validate_retained_numpy_iteration

    def consume(engine: str) -> tuple[list[dict[str, object]] | None, BaseException | None]:
        matrix = np.zeros((200_000, 2), dtype=np.int64)
        query = (
            fpstreams.rows.from_numpy(matrix, columns=("key", "value"))
            .with_engine(engine)
            .group_by("key")
            .aggregate(total=fpstreams.agg.sum("value"))
        )
        fired = False
        armed = False

        def change_dtype(_signal_number: int, _frame: object) -> None:
            nonlocal fired
            fired = True
            matrix.dtype = np.dtype(changed_dtype)

        def arm_after_validation(values: object, width: int, dtype: object) -> int:
            nonlocal armed
            row_count = canonical_validation(values, width, dtype)
            if values is matrix and not armed:
                armed = True
                signal.setitimer(signal.ITIMER_REAL, 0.0001)
            return row_count

        previous_handler = signal.signal(signal.SIGALRM, change_dtype)
        previous_timer = signal.getitimer(signal.ITIMER_REAL)
        signal.setitimer(signal.ITIMER_REAL, 0)
        try:
            with monkeypatch.context() as scoped, warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                scoped.setattr(
                    numpy_adapter,
                    "_validate_retained_numpy_iteration",
                    arm_after_validation,
                )
                scoped.setattr(
                    numpy_group,
                    "_validate_retained_numpy_iteration",
                    arm_after_validation,
                )
                try:
                    result = query.to_list()
                except BaseException as error:
                    result = None
                    captured = error
                else:
                    captured = None
        finally:
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)
            signal.signal(signal.SIGALRM, previous_handler)
        assert fired
        return result, captured

    canonical = consume("python")
    automatic = consume("auto")
    assert canonical[0] is automatic[0] is None
    assert [type(canonical[1]), type(automatic[1])] == [ValueError, ValueError]
    assert (
        str(canonical[1])
        == str(automatic[1])
        == (
            f"from_numpy() retained array dtype changed from int64 to {changed_dtype} "
            "during iteration"
        )
    )


def test_numpy_row_prefix_explain_marks_only_complete_safe_plans() -> None:
    """Explain should expose only full guarded NumPy row prefixes selected by planning."""
    np = pytest.importorskip("numpy")

    integers = fpstreams.rows.from_numpy(
        np.asarray([[1, 4], [2, 3]], dtype=np.int64),
        columns=("left", "right"),
    )
    eligible = (
        integers.where(fpstreams.col("left") >= 1)
        .where(5 > fpstreams.col("right"))  # noqa: SIM300 - exercise literal-left lowering
        .select("left", copied="left")
    )
    explanation = eligible.explain("list").to_dict()

    assert explanation["numpy_prefix"] == {
        "operation_count": 3,
        "guarded": True,
    }
    assert explanation["stages"] == [
        {
            "engine": "numpy",
            "operations": ["filter", "filter", "map"],
            "fused": True,
        }
    ]

    projected_objects = fpstreams.rows.from_numpy(
        np.asarray([[object(), object()]], dtype=object),
        columns=("left", "right"),
    ).select(alias="right")
    assert projected_objects.explain("list").to_dict()["numpy_prefix"] == {
        "operation_count": 1,
        "guarded": True,
    }

    unsupported = (
        integers.with_engine("python").select("left"),
        fpstreams.rows.from_numpy(
            np.asarray([[1.0, 2.0]], dtype=np.float64),
            columns=("left", "right"),
        )
        .where(fpstreams.col("left") >= 1)
        .select("left"),
        integers.where(lambda row: row["left"] >= 1).select("left"),
    )
    for query in unsupported:
        assert query.explain("list").to_dict()["numpy_prefix"] is None


def test_numpy_row_prefix_rejects_rebound_native_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement native descriptor must not detach execution from the source factory."""
    np = pytest.importorskip("numpy")
    from fpstreams.tabular.numpy import NumpyRowSource

    query = fpstreams.rows.from_numpy(
        np.asarray([[1], [0]], dtype=np.int64),
        columns=("value",),
    ).where(fpstreams.col("value") > 0)
    source = query._flow._pipeline.source
    source.native_data = NumpyRowSource(
        np.asarray([[0], [0]], dtype=np.int64),
        ("value",),
    )

    execution = query.run_with_report("to_list")

    assert execution.value == [{"value": 1}]
    assert execution.report.strategy == "planned:python"


def test_numpy_row_prefix_executes_exact_integer_with_columns_without_input_rows() -> None:
    """A pure computed column should stay columnar until final output dictionaries exist."""
    np = pytest.importorskip("numpy")
    values = np.asarray([[-2, 10], [0, 20], [3, 30]], dtype=np.int64)
    source = fpstreams.rows.from_numpy(values, columns=("left", "right"))
    query = source.with_columns(
        score=fpstreams.col("left") * 3 + 1,
        original=fpstreams.col("left"),
    ).select("score", "original", "right")

    first = query.run_with_report("to_list")
    execution = query.run_with_report("to_list")
    explanation = query.explain("list").to_dict()["numpy_prefix"]

    assert execution.value == [
        {"score": -5, "original": -2, "right": 10},
        {"score": 1, "original": 0, "right": 20},
        {"score": 10, "original": 3, "right": 30},
    ]
    assert execution.value == query.with_engine("python").to_list()
    assert first.value == execution.value
    assert first.report.strategy == "planned:python"
    assert execution.report.strategy == "numpy_direct"
    assert explanation == {
        "operation_count": 2,
        "guarded": True,
    }
    assert query.explain("list").to_dict()["numpy_prefix"] == explanation


def test_numpy_row_prefix_computed_columns_keep_python_integer_and_sibling_semantics() -> None:
    """Computed columns remain unbounded Python integers and read the original sibling values."""
    np = pytest.importorskip("numpy")
    maximum = 2**63 - 1
    minimum = -(2**63)
    source = fpstreams.rows.from_numpy(
        np.asarray([[maximum], [minimum]], dtype=np.int64),
        columns=("value",),
    )
    query = source.with_columns(
        value=fpstreams.col("value") + 1,
        sibling=fpstreams.col("value"),
        tripled=fpstreams.col("value") * 3,
    ).select("value", "sibling", "tripled")

    first = query.run_with_report("to_list")
    execution = query.run_with_report("to_list")

    assert execution.value == [
        {"value": maximum + 1, "sibling": maximum, "tripled": maximum * 3},
        {"value": minimum + 1, "sibling": minimum, "tripled": minimum * 3},
    ]
    assert execution.value == query.with_engine("python").to_list()
    assert first.value == execution.value
    assert first.report.strategy == "planned:python"
    assert execution.report.strategy == "numpy_direct"


def test_numpy_row_prefix_computed_constant_subtrees_use_python_arithmetic() -> None:
    """Literal-only subtrees must not inherit fixed-width NumPy scalar behavior."""
    np = pytest.importorskip("numpy")
    maximum = 2**63 - 1
    source = fpstreams.rows.from_numpy(
        np.asarray([[1]], dtype=np.int64),
        columns=("value",),
    )
    query = source.with_columns(
        small=fpstreams.lit(1) + 2,
        overflow=fpstreams.lit(maximum) + 1,
        multiplied=fpstreams.lit(2**62) * 4,
        booleans=fpstreams.lit(True) + True,
        nested=fpstreams.col("value") + (fpstreams.lit(maximum) + 1),
        negative=-fpstreams.lit(-(2**80)),
        magnitude=abs(fpstreams.lit(-(2**80))),
    )

    first = query.run_with_report("to_list")
    execution = query.run_with_report("to_list")
    expected = query.with_engine("python").to_list()

    assert (
        execution.value
        == expected
        == [
            {
                "value": 1,
                "small": 3,
                "overflow": 2**63,
                "multiplied": 2**64,
                "booleans": 2,
                "nested": 2**63 + 1,
                "negative": 2**80,
                "magnitude": 2**80,
            }
        ]
    )
    assert all(type(value) is int for value in execution.value[0].values())
    assert first.value == execution.value
    assert first.report.strategy == "planned:python"
    assert execution.report.strategy == "numpy_direct"


def test_numpy_row_prefix_deopts_deep_computed_expression_trees() -> None:
    """Columnar evaluation must not recurse beyond Python's iterative RowExpr fallback."""
    np = pytest.importorskip("numpy")
    expression = fpstreams.col("value")
    for _index in range(1_200):
        expression = expression + 1
    query = fpstreams.rows.from_numpy(
        np.asarray([[1]], dtype=np.int64),
        columns=("value",),
    ).with_columns(result=expression)

    execution = query.run_with_report("to_list")

    assert execution.value == [{"value": 1, "result": 1_201}]
    assert execution.report.strategy == "planned:python"


def test_numpy_row_prefix_cached_fallback_ignores_python_recursion_limit() -> None:
    """A low-limit iterative compile remains a safe reusable Python fallback."""
    np = pytest.importorskip("numpy")
    expression = fpstreams.col("value")
    for _index in range(80):
        expression = expression + 1
    query = fpstreams.rows.from_numpy(
        np.asarray([[1]], dtype=np.int64),
        columns=("value",),
    ).with_columns(result=expression)

    previous_limit = sys.getrecursionlimit()
    try:
        sys.setrecursionlimit(100)
        first = query.run_with_report("to_list")
        execution = query.run_with_report("to_list")
    finally:
        sys.setrecursionlimit(previous_limit)

    assert execution.value == [{"value": 1, "result": 81}]
    assert first.value == execution.value
    assert first.report.strategy == "planned:python"
    assert execution.report.strategy == "planned:python"
    assert query.explain("list").to_dict()["numpy_prefix"] is None


def test_numpy_row_prefix_computed_columns_preserve_python_integer_identity() -> None:
    """Bare fields share row objects while evaluated constants remain per-row results."""
    np = pytest.importorskip("numpy")

    def query() -> fpstreams.Rows[dict[str, object]]:
        return fpstreams.rows.from_numpy(
            np.asarray([[1], [2]], dtype=np.int64),
            columns=("row",),
        ).with_columns(
            source=fpstreams.col("row") + 999_999,
            first=fpstreams.col("row") + 999_999,
            bare=fpstreams.col("row"),
            calculated=fpstreams.lit(1_000_000) + 1,
            literal=fpstreams.lit(1_000_000),
        )

    automatic_query = query()
    first = automatic_query.run_with_report("to_list")
    automatic = automatic_query.run_with_report("to_list")
    canonical = query().with_engine("python").to_list()

    for rows in (first.value, automatic.value, canonical):
        assert rows[0]["source"] == rows[0]["first"] == 1_000_000
        assert rows[0]["source"] is not rows[0]["first"]
        assert rows[0]["bare"] is rows[0]["row"]
        assert rows[0]["calculated"] == rows[1]["calculated"] == 1_000_001
        assert rows[0]["calculated"] is not rows[1]["calculated"]
        assert rows[0]["literal"] is rows[1]["literal"]
    assert first.report.strategy == "planned:python"
    assert automatic.report.strategy == "numpy_direct"

    def absolute_query() -> fpstreams.Rows[dict[str, object]]:
        return fpstreams.rows.from_numpy(
            np.asarray([[10**12], [-(10**12)]], dtype=np.int64),
            columns=("value",),
        ).with_columns(magnitude=abs(fpstreams.col("value")))

    automatic_absolute_query = absolute_query()
    first_absolute = automatic_absolute_query.run_with_report("to_list")
    automatic_absolute = automatic_absolute_query.run_with_report("to_list")
    canonical_absolute = absolute_query().with_engine("python").to_list()
    for rows in (first_absolute.value, automatic_absolute.value, canonical_absolute):
        assert rows[0]["magnitude"] is rows[0]["value"]
        assert rows[1]["magnitude"] == 10**12
    assert automatic_absolute.value == canonical_absolute
    assert first_absolute.report.strategy == "planned:python"
    assert automatic_absolute.report.strategy == "numpy_direct"


def test_numpy_row_prefix_computed_columns_keep_lazy_missing_field_boundaries() -> None:
    """A missing dependency is observed only when a row reaches its computed stage."""
    np = pytest.importorskip("numpy")
    empty = fpstreams.rows.from_numpy(
        np.empty((0, 1), dtype=np.int64),
        columns=("present",),
    ).with_columns(result=fpstreams.col("missing") + 1)
    skipped = (
        fpstreams.rows.from_numpy(
            np.asarray([[1], [2]], dtype=np.int64),
            columns=("present",),
        )
        .where(fpstreams.col("present") < 0)
        .with_columns(result=fpstreams.col("missing") + 1)
    )

    assert empty.to_list() == []
    assert skipped.to_list() == []

    reached = fpstreams.rows.from_numpy(
        np.asarray([[1]], dtype=np.int64),
        columns=("present",),
    ).with_columns(result=fpstreams.col("missing") + 1)
    with pytest.raises(fpstreams.SelectionError) as automatic:
        reached.to_list()
    with pytest.raises(fpstreams.SelectionError) as canonical:
        reached.with_engine("python").to_list()
    assert str(automatic.value) == str(canonical.value)
    assert type(automatic.value.__cause__) is type(canonical.value.__cause__) is KeyError
    assert automatic.value.__context__ is automatic.value.__cause__
    assert canonical.value.__context__ is canonical.value.__cause__


def test_numpy_row_prefix_executes_one_short_circuit_conjunction_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A primitive AND tree should remain one planned filter while evaluating left to right."""
    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.asarray([[0, 5], [1, 4], [2, 3], [3, 2], [4, 1]], dtype=np.int64),
        columns=("left", "right"),
    )
    query = source.where(
        (fpstreams.col("left") >= 1) & (fpstreams.col("right") < 4) & (10 > fpstreams.col("left"))  # noqa: SIM300 - test reversed comparison lowering
    ).select("left", "right")

    execution = query.run_with_report("to_list")

    assert execution.value == [
        {"left": 2, "right": 3},
        {"left": 3, "right": 2},
        {"left": 4, "right": 1},
    ]
    assert execution.value == query.with_engine("python").to_list()
    assert execution.report.strategy == "numpy_direct"
    assert query.explain("list").to_dict()["numpy_prefix"] == {
        "operation_count": 2,
        "guarded": True,
    }

    with monkeypatch.context() as scoped:
        scoped.setattr(np, "greater_equal", lambda _left, _right: False)
        scoped.setattr(np, "logical_and", lambda _left, _right: False)
        ufunc_execution = query.run_with_report("to_list")
    assert ufunc_execution.value == [
        {"left": 2, "right": 3},
        {"left": 3, "right": 2},
        {"left": 4, "right": 1},
    ]
    assert ufunc_execution.report.strategy == "numpy_direct"

    import operator

    stable_helpers = source.where(
        (fpstreams.col("left") < 2**100) & (fpstreams.col("right") > 0)
    ).select("left", "right")
    with monkeypatch.context() as scoped:
        scoped.setattr(operator, "eq", lambda _left, _right: False)
        scoped.setattr(operator, "and_", lambda _left, _right: False)
        scoped.setattr(np, "ascontiguousarray", lambda values: values * 0)
        helper_execution = stable_helpers.run_with_report("to_list")
    assert helper_execution.value == [
        {"left": 0, "right": 5},
        {"left": 1, "right": 4},
        {"left": 2, "right": 3},
        {"left": 3, "right": 2},
        {"left": 4, "right": 1},
    ]
    assert helper_execution.report.strategy == "numpy_direct"


def test_numpy_row_prefix_conjunction_preserves_reachable_missing_rhs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing right operand is skipped only when every active left operand is false."""
    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.asarray([[0], [1]], dtype=np.int64),
        columns=("value",),
    )
    skipped = source.where((fpstreams.col("value") < 0) & (fpstreams.col("missing") > 0))
    reached = source.where((fpstreams.col("value") >= 0) & (fpstreams.col("missing") > 0))

    skipped_execution = skipped.run_with_report("to_list")
    assert skipped_execution.value == []
    assert skipped_execution.report.strategy == "numpy_direct"

    with pytest.raises(fpstreams.SelectionError) as automatic:
        reached.to_list()
    with pytest.raises(fpstreams.SelectionError) as canonical:
        reached.with_engine("python").to_list()
    assert str(automatic.value) == str(canonical.value)
    assert type(automatic.value.__cause__) is type(canonical.value.__cause__) is KeyError

    from fpstreams.execution import numpy_prefix

    monkeypatch.setattr(numpy_prefix, "_NUMPY_PREFIX_CHUNK_ROWS", 2)
    later_chunk = fpstreams.rows.from_numpy(
        np.asarray([[0], [0], [1]], dtype=np.int64),
        columns=("value",),
    ).where((fpstreams.col("value") > 0) & (fpstreams.col("missing") > 0))
    with pytest.raises(fpstreams.SelectionError) as later_automatic:
        later_chunk.to_list()
    with pytest.raises(fpstreams.SelectionError) as later_canonical:
        later_chunk.with_engine("python").to_list()
    assert str(later_automatic.value) == str(later_canonical.value)


def test_numpy_row_prefix_rechecks_filter_dtype_at_execution_boundary() -> None:
    """A retained dtype changed after compilation must deopt before NumPy comparison."""
    import warnings

    np = pytest.importorskip("numpy")
    from fpstreams.execution.numpy_prefix import try_numpy_prefix_list

    values = np.asarray([[1], [2]], dtype=np.int64)
    query = fpstreams.rows.from_numpy(values, columns=("value",)).where(fpstreams.col("value") > 0)
    physical, pipeline = query._flow._terminal_context("list")
    assert physical.backend_payload is not None
    assert physical.backend_payload.numpy_prefix is not None

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        values.dtype = np.dtype("float64")
    assert try_numpy_prefix_list(query._flow, physical, pipeline) == (False, None)
    assert query.to_list() == query.with_engine("python").to_list()


def test_numpy_row_prefix_materializes_lists_and_columns_without_python_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A complete primitive prefix should reach both sinks before the row executor."""
    np = pytest.importorskip("numpy")
    from fpstreams.streams import flow_terminals
    from fpstreams.streams.flow import Flow

    values = np.asarray(
        [[0, 9, 100], [1, 8, 101], [2, 7, 102], [3, 6, 103], [4, 5, 104]],
        dtype=np.int64,
    )

    def query() -> fpstreams.Rows[dict[str, object]]:
        return (
            fpstreams.rows.from_numpy(values, columns=("left", "right", "tail"))
            .where(fpstreams.col("left") >= 1)
            .where(4 > fpstreams.col("left"))  # noqa: SIM300 - literal-left comparison
            .select(id="left", copied="left", payload="tail")
        )

    def forbidden_rows(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("a safe NumPy prefix must not enter the Python row executor")

    monkeypatch.setattr(flow_terminals, "execute_physical", forbidden_rows)
    execution = query().run_with_report("to_list")
    assert execution.value == [
        {"id": 1, "copied": 1, "payload": 101},
        {"id": 2, "copied": 2, "payload": 102},
        {"id": 3, "copied": 3, "payload": 103},
    ]
    assert execution.report.strategy == "numpy_direct"

    def forbidden_consume(
        self: Flow[object],
        consumer: object,
    ) -> object:
        del self, consumer
        raise AssertionError("a safe NumPy prefix must not consume Python rows")

    monkeypatch.setattr(Flow, "_consume", forbidden_consume)
    columns = query().to_columns()
    assert columns == {
        "id": [1, 2, 3],
        "copied": [1, 2, 3],
        "payload": [101, 102, 103],
    }
    assert columns["id"] is not columns["copied"]


def test_numpy_row_prefix_preserves_empty_and_nonempty_missing_field_boundaries() -> None:
    """Missing direct fields should stay lazy until the first row reaching their stage."""
    np = pytest.importorskip("numpy")

    empty = fpstreams.rows.from_numpy(
        np.empty((0, 1), dtype=np.int64),
        columns=("present",),
    )
    assert empty.select("missing").to_list() == []
    assert empty.where(fpstreams.col("missing") == 1).select("present").to_columns() == {}

    nonempty = fpstreams.rows.from_numpy(
        np.asarray([[1]], dtype=np.int64),
        columns=("present",),
    )
    queries = (
        nonempty.select("missing"),
        nonempty.where(fpstreams.col("missing") == 1).select("present"),
    )
    for query in queries:
        with pytest.raises(fpstreams.SelectionError) as automatic:
            query.to_list()
        with pytest.raises(fpstreams.SelectionError) as canonical:
            query.with_engine("python").to_list()
        assert str(automatic.value) == str(canonical.value)
        assert type(automatic.value.__cause__) is type(canonical.value.__cause__) is KeyError

    assert nonempty.where(fpstreams.col("present") < 0).select("missing").to_list() == []
    with pytest.raises(fpstreams.SelectionError):
        nonempty.where(fpstreams.col("present") > 0).select("missing").to_list()


@pytest.mark.parametrize(
    ("dtype", "raw_values"),
    [
        ("bool", [False, True]),
        ("int8", [-128, -1, 0, 1, 127]),
        ("uint8", [0, 1, 127, 255]),
        ("int64", [-(2**63), -1, 0, 1, 2**63 - 1]),
        ("uint64", [0, 1, 2**63, 2**64 - 1]),
    ],
)
def test_numpy_row_prefix_compares_exact_python_integer_literals_without_coercion(
    dtype: str,
    raw_values: list[int | bool],
) -> None:
    """Out-of-domain literals fold exactly instead of entering NumPy's coercion rules."""
    import operator

    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.asarray(raw_values, dtype=dtype).reshape(-1, 1),
        columns=("value",),
    )
    comparisons = (
        operator.eq,
        operator.ne,
        operator.lt,
        operator.le,
        operator.gt,
        operator.ge,
    )
    literals = (
        False,
        True,
        -(2**100),
        -1,
        0,
        1,
        2,
        127,
        128,
        255,
        256,
        2**63 - 1,
        2**64 - 1,
        2**100,
    )
    for compare in comparisons:
        for literal in literals:
            for reversed_operands in (False, True):
                field = fpstreams.col("value")
                predicate = (
                    compare(literal, field) if reversed_operands else compare(field, literal)
                )
                automatic = source.where(predicate).select("value").to_list()
                canonical = source.with_engine("python").where(predicate).select("value").to_list()
                assert automatic == canonical


@pytest.mark.parametrize("protocol", ["hash", "equality"])
@pytest.mark.parametrize("literal_left", [False, True])
def test_numpy_filter_declines_custom_integer_types_without_metaclass_protocols(
    protocol: str, literal_left: bool
) -> None:
    """Exact-type guards must not hash or compare a user-defined numeric class."""
    np = pytest.importorskip("numpy")

    class Meta(type):
        def __hash__(cls) -> int:
            if protocol == "hash":
                raise RuntimeError("metaclass hashed")
            return hash(int)  # Force equality if a set-based guard is reintroduced.

        def __eq__(cls, other: object) -> bool:
            raise RuntimeError("metaclass compared")

    class Limit(int, metaclass=Meta):
        pass

    source = fpstreams.rows.from_numpy(
        np.asarray([[1], [2], [3]], dtype=np.int64), columns=("value",)
    )
    field, literal = fpstreams.col("value"), fpstreams.lit(Limit(1))
    predicate = literal < field if literal_left else field > literal
    query = source.where(predicate)

    assert query.to_list() == query.with_engine("python").to_list() == [{"value": 2}, {"value": 3}]


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        (fpstreams.lit(2) == fpstreams.col("value"), [2]),
        (fpstreams.lit(2) != fpstreams.col("value"), [1, 3]),
        (fpstreams.lit(2) < fpstreams.col("value"), [3]),
        (fpstreams.lit(2) <= fpstreams.col("value"), [2, 3]),
        (fpstreams.lit(2) > fpstreams.col("value"), [1]),
        (fpstreams.lit(2) >= fpstreams.col("value"), [1, 2]),
    ],
)
def test_numpy_row_prefix_preserves_explicit_literal_left_comparisons(
    predicate: object,
    expected: list[int],
) -> None:
    """Literal expression nodes keep Python ordering when NumPy reverses the comparison."""
    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.asarray([[1], [2], [3]], dtype=np.int64),
        columns=("value",),
    )
    query = source.where(predicate).select("value")

    execution = query.run_with_report("to_list")

    assert execution.value == [{"value": value} for value in expected]
    assert execution.value == query.with_engine("python").to_list()
    assert execution.report.strategy == "numpy_direct"


def test_numpy_row_prefix_declines_an_empty_projection() -> None:
    """Zero-column records retain the canonical row path and their hidden cardinality."""
    np = pytest.importorskip("numpy")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1], [2]], dtype=np.int64),
        columns=("value",),
    ).select()

    assert source.explain("list").to_dict()["numpy_prefix"] is None
    assert source.to_list() == [{}, {}]
    assert source.to_columns() == {}


def test_numpy_row_prefix_declines_zero_width_rename_without_losing_cardinality() -> None:
    """A schema-only rename cannot represent nonzero rows when no output columns exist."""
    np = pytest.importorskip("numpy")
    source = fpstreams.rows.from_numpy(
        np.empty((3, 0), dtype=np.int64),
        columns=(),
    ).rename()

    execution = source.run_with_report("to_list")

    assert execution.value == source.with_engine("python").to_list() == [{}, {}, {}]
    assert execution.report.strategy == "planned:python"
    assert source.explain("list").to_dict()["numpy_prefix"] is None


@pytest.mark.parametrize(
    "route",
    ["identity", "columns", "arrow", "prefix", "group"],
)
def test_numpy_direct_row_paths_deopt_before_hashing_string_subclass_columns(
    route: str,
) -> None:
    """Planning and direct sinks cannot add or remove observable column-name hash calls."""
    np = pytest.importorskip("numpy")

    class ArmedColumn(str):
        armed = False
        calls = 0

        def __hash__(self) -> int:
            if self.armed:
                type(self).calls += 1
            return super().__hash__()

    def consume(engine: str) -> tuple[object, int, str | None]:
        name = ArmedColumn("value")
        rows = fpstreams.rows.from_numpy(
            np.asarray([[1], [2]], dtype=np.int64),
            columns=(name,),
        ).with_engine(engine)
        query: object
        if route == "prefix":
            query = rows.where(fpstreams.col("value") > 0).select("value")
        elif route == "group":
            query = rows.group_by("value").aggregate(rows=fpstreams.agg.count())
        else:
            query = rows
        name.armed = True
        ArmedColumn.calls = 0
        if route == "columns":
            result = rows.to_columns()
            strategy = None
        elif route == "arrow":
            result = rows.to_arrow().to_pylist()
            strategy = None
        else:
            execution = query.run_with_report("to_list")  # type: ignore[attr-defined]
            result = execution.value
            strategy = execution.report.strategy
        return result, ArmedColumn.calls, strategy

    canonical = consume("python")
    automatic = consume("auto")
    assert automatic[:2] == canonical[:2]
    assert canonical[1] > 0
    if route in {"identity", "prefix", "group"}:
        assert automatic[2] == "planned:python"


@pytest.mark.parametrize("changed_dtype", ["float64", "uint64"])
@pytest.mark.parametrize("sink", ["list", "columns", "arrow", "prefix", "group"])
def test_numpy_row_scans_match_python_on_threaded_same_width_dtype_changes(
    monkeypatch: pytest.MonkeyPatch,
    changed_dtype: str,
    sink: str,
) -> None:
    """A real worker transition is detected by every optimized retained-row scan."""
    import warnings
    from threading import Event, Thread

    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_group, numpy_prefix
    from fpstreams.tabular import numpy as numpy_adapter

    canonical_validation = numpy_adapter._validate_retained_numpy_iteration

    def consume(engine: str) -> BaseException | None:
        values = np.zeros((20_000, 2), dtype=np.int64)
        rows = fpstreams.rows.from_numpy(values, columns=("key", "value")).with_engine(engine)
        ready = Event()
        fired = Event()
        worker_errors: list[BaseException] = []
        released = False

        def mutate() -> None:
            try:
                if not ready.wait(5):
                    raise TimeoutError("NumPy scan never reached its first validated boundary")
                values.dtype = np.dtype(changed_dtype)
            except BaseException as error:
                worker_errors.append(error)
            finally:
                fired.set()

        def release_after_validation(
            matrix: object,
            width: int,
            dtype: object | None = None,
        ) -> int:
            nonlocal released
            row_count = canonical_validation(matrix, width, dtype)
            if matrix is values and not released:
                released = True
                ready.set()
                if not fired.wait(5):
                    raise TimeoutError("dtype mutation worker did not finish")
            return row_count

        worker = Thread(target=mutate)
        worker.start()
        try:
            with (
                monkeypatch.context() as scoped,
                warnings.catch_warnings(),
            ):
                warnings.simplefilter("ignore", DeprecationWarning)
                for module in (numpy_adapter, numpy_prefix, numpy_group):
                    scoped.setattr(
                        module,
                        "_validate_retained_numpy_iteration",
                        release_after_validation,
                    )
                try:
                    if sink == "list":
                        rows.to_list()
                    elif sink == "columns":
                        rows.to_columns()
                    elif sink == "arrow":
                        rows.to_arrow(batch_size=1_024)
                    elif sink == "prefix":
                        rows.where(fpstreams.col("key") == 0).select("value").to_list()
                    else:
                        rows.group_by("key").aggregate(total=fpstreams.agg.sum("value")).to_list()
                except BaseException as error:
                    captured = error
                else:
                    captured = None
        finally:
            worker.join(5)
        assert not worker.is_alive()
        assert fired.is_set()
        assert worker_errors == []
        return captured

    canonical = consume("python")
    automatic = consume("auto")
    assert [type(canonical), type(automatic)] == [ValueError, ValueError]
    assert (
        str(canonical)
        == str(automatic)
        == (
            f"from_numpy() retained array dtype changed from int64 to {changed_dtype} "
            "during iteration"
        )
    )


@pytest.mark.parametrize("action", ["shrink", "grow", "reshape", "width", "dtype"])
def test_numpy_row_prefix_preserves_live_matrix_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    """Each bounded prefix chunk revalidates live length, width, dimensions, and dtype."""
    import warnings

    np = pytest.importorskip("numpy")
    from fpstreams.execution import numpy_prefix

    initial = (
        [[1, 10], [2, 20]]
        if action == "grow"
        else [
            [1, 10],
            [2, 20],
            [3, 30],
            [4, 40],
        ]
    )
    values = np.asarray(initial, dtype=np.int64)
    original = numpy_prefix._compare_integer_column
    fired = False

    def mutate_after_comparison(column: object, spec: object) -> object:
        nonlocal fired
        result = original(column, spec)  # type: ignore[arg-type]
        if fired:
            return result
        fired = True
        if action == "shrink":
            values.resize((2, 2), refcheck=False)
        elif action == "grow":
            values.resize((4, 2), refcheck=False)
            values[2:] = ((3, 30), (4, 40))
        elif action == "reshape":
            values.resize((values.size,), refcheck=False)
        elif action == "width":
            values.resize((2, 4), refcheck=False)
        else:
            values.dtype = np.dtype("uint64")
        return result

    monkeypatch.setattr(numpy_prefix, "_compare_integer_column", mutate_after_comparison)
    query = (
        fpstreams.rows.from_numpy(values, columns=("key", "value"))
        .where(fpstreams.col("key") > 0)
        .select("key", "value")
    )
    if action in {"reshape", "width", "dtype"}:
        message = {
            "reshape": "changed to 1 dimensions",
            "width": "width changed",
            "dtype": "dtype changed from int64 to uint64",
        }[action]
        with warnings.catch_warnings(), pytest.raises(ValueError, match=message):
            warnings.simplefilter("ignore", DeprecationWarning)
            query.to_list()
        return

    assert query.to_list() == [
        {"key": 1, "value": 10},
        {"key": 2, "value": 20},
        *([{"key": 3, "value": 30}, {"key": 4, "value": 40}] if action == "grow" else []),
    ]


def test_numpy_group_consumes_safe_filtered_projection_and_rename_prefixes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Safe schema-only prefixes should feed the existing bounded NumPy group kernel."""
    np = pytest.importorskip("numpy")
    from fpstreams.execution import relational

    values = np.asarray(
        [[2, 4, 1], [1, 7, 0], [2, 3, 1], [1, 5, 1], [3, 9, 1]],
        dtype=np.int64,
    )

    def forbidden_rows(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("a safe NumPy group prefix must not execute Python rows")

    monkeypatch.setattr(relational, "_execute_python_group_values", forbidden_rows)
    source = fpstreams.rows.from_numpy(values, columns=("key", "value", "keep"))
    grouped = (
        source.where(fpstreams.col("keep") == 1)
        .select(bucket="key", amount="value")
        .group_by("bucket")
        .aggregate(
            rows=fpstreams.agg.count(),
            total=fpstreams.agg.sum("amount"),
            low=fpstreams.agg.min("amount"),
            high=fpstreams.agg.max("amount"),
        )
    )
    renamed = (
        source.where(fpstreams.col("keep") == 1)
        .rename(key="bucket", value="amount")
        .group_by("bucket")
        .aggregate(total=fpstreams.agg.sum("amount"))
    )
    conjoined = (
        source.where((fpstreams.col("keep") == 1) & (fpstreams.col("value") >= 4))
        .group_by("key")
        .aggregate(total=fpstreams.agg.sum("value"))
    )

    expected = [
        {"bucket": 2, "rows": 2, "total": 7, "low": 3, "high": 4},
        {"bucket": 1, "rows": 1, "total": 5, "low": 5, "high": 5},
        {"bucket": 3, "rows": 1, "total": 9, "low": 9, "high": 9},
    ]
    execution = grouped.run_with_report("to_list")
    assert execution.value == expected
    assert execution.report.strategy == "numpy_direct"
    assert grouped.explain("list").to_dict()["relations"]["candidate"] == "numpy_hash"
    assert renamed.to_list() == [
        {"bucket": 2, "total": 7},
        {"bucket": 1, "total": 5},
        {"bucket": 3, "total": 9},
    ]
    conjoined_execution = conjoined.run_with_report("to_list")
    assert conjoined_execution.value == [
        {"key": 2, "total": 4},
        {"key": 1, "total": 5},
        {"key": 3, "total": 9},
    ]
    assert conjoined_execution.report.strategy == "numpy_direct"


@pytest.mark.parametrize("prefixed", [False, True], ids=["direct", "prefix"])
@pytest.mark.parametrize("replacement", ["global", "builtins"])
def test_numpy_group_deopts_before_replaced_hash_is_observed(
    monkeypatch: pytest.MonkeyPatch,
    prefixed: bool,
    replacement: str,
) -> None:
    """Columnar grouping cannot bypass either dynamic lookup tier of canonical hash()."""
    import builtins

    np = pytest.importorskip("numpy")
    from fpstreams.execution import relational

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2, 1], [1, 3, 1], [2, 4, 0]], dtype=np.int64),
        columns=("key", "value", "keep"),
    )
    if prefixed:
        source = source.where(fpstreams.col("keep") == 1).select("key", "value")
    query = source.group_by("key").aggregate(
        rows=fpstreams.agg.count(),
        total=fpstreams.agg.sum("value"),
    )
    original_hash = builtins.hash
    calls: list[object] = []

    def observed_hash(value: object) -> int:
        calls.append(value)
        return original_hash(value)

    with monkeypatch.context() as scoped:
        if replacement == "global":
            scoped.setattr(relational, "hash", observed_hash, raising=False)
        else:
            scoped.setattr(builtins, "hash", observed_hash)
        result = query.to_list()

    expected = (
        [{"key": 1, "rows": 2, "total": 5}]
        if prefixed
        else [
            {"key": 1, "rows": 2, "total": 5},
            {"key": 2, "rows": 1, "total": 4},
        ]
    )
    assert calls
    assert result == expected


@pytest.mark.parametrize("prefixed", [False, True], ids=["direct", "prefix"])
def test_numpy_group_run_with_report_preserves_the_outer_route(prefixed: bool) -> None:
    """Keep the root summary while observing direct routes inside child groups."""
    np = pytest.importorskip("numpy")

    source = fpstreams.rows.from_numpy(
        np.asarray([[1, 2, 1], [1, 3, 1], [2, 4, 0]], dtype=np.int64),
        columns=("key", "value", "keep"),
    )
    if prefixed:
        source = source.where(fpstreams.col("keep") == 1).select("key", "value")
    grouped = source.group_by("key").aggregate(total=fpstreams.agg.sum("value"))

    root_execution = grouped.run_with_report("count")
    nested_execution = grouped.select("key").run_with_report("count")

    assert root_execution.value == (1 if prefixed else 2)
    assert root_execution.report.strategy == "numpy_direct"
    assert nested_execution.value == root_execution.value
    assert nested_execution.report.strategy == "planned:python"
    assert any(
        event.kind == "strategy"
        and event.strategy == "numpy_direct"
        and event.compiler_engine is None
        for event in nested_execution.report.events
    )
