"""Rows text and database I/O ownership, parsing, and transaction contracts."""

from __future__ import annotations

import io
import json
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import fpstreams
from fpstreams import flow


def test_rows_stream_csv_and_json_lines_round_trips(tmp_path) -> None:
    records = [{"name": "Ada", "score": 10}, {"name": "Lin", "score": 20}]
    csv_path = tmp_path / "scores.csv"
    jsonl_path = tmp_path / "scores.jsonl"

    fpstreams.rows(records).to_csv(csv_path)
    csv_rows = fpstreams.rows.from_csv(csv_path)
    assert csv_rows.to_list() == [
        {"name": "Ada", "score": "10"},
        {"name": "Lin", "score": "20"},
    ]
    assert csv_rows.to_list()[0]["name"] == "Ada"

    fpstreams.rows(records).to_jsonl(jsonl_path)
    assert fpstreams.rows.from_jsonl(jsonl_path).to_list() == records


@pytest.mark.parametrize("sink", ["to_csv", "to_jsonl"])
def test_rows_atomic_export_preserves_old_file_on_failure(tmp_path, sink) -> None:
    target = tmp_path / "output"
    reference = tmp_path / "reference"
    data = [{"id": 1, "name": "雪"}, {"id": 2, "name": "next"}]
    getattr(fpstreams.rows(data), sink)(reference)
    getattr(fpstreams.rows(iter(data)), sink)(target, atomic=True)
    assert target.read_bytes() == reference.read_bytes()
    failure = RuntimeError("source failed")
    closed = []

    def source():
        try:
            yield data[0]
            raise failure
        finally:
            closed.append(True)

    with pytest.raises(RuntimeError) as caught:
        getattr(fpstreams.rows(source()), sink)(target, atomic=True)
    assert caught.value is failure
    assert closed == [True]
    assert target.read_bytes() == reference.read_bytes()
    assert sorted(tmp_path.iterdir()) == sorted([target, reference])


@pytest.mark.parametrize("sink", ["to_csv", "to_jsonl"])
def test_rows_atomic_export_checks_destination_before_opening_source(tmp_path, sink) -> None:
    target = tmp_path / "output"
    target.write_text("old")
    opened = []

    def source():
        opened.append(True)
        return iter([{"id": 1}])

    query = flow.defer(source).rows()
    with pytest.raises(FileExistsError):
        getattr(query, sink)(target, atomic=True, if_exists="error")
    with pytest.raises(ValueError, match="atomic"):
        getattr(query, sink)(target, if_exists="error")
    assert opened == []
    assert target.read_text() == "old"


@pytest.mark.parametrize("sink", ["to_csv", "to_jsonl"])
def test_rows_atomic_export_waits_for_cleanup(tmp_path, sink) -> None:
    target = tmp_path / "output"
    target.write_text("old")
    failure = OSError("close failed")

    def source():
        try:
            yield {"id": 1}
        finally:
            assert target.read_text() == "old"
            raise failure

    with pytest.raises(OSError) as caught:
        getattr(fpstreams.rows(source()), sink)(target, atomic=True)
    assert caught.value is failure
    assert target.read_text() == "old"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize("sink", ["to_csv", "to_jsonl"])
def test_rows_atomic_export_does_not_overwrite_a_concurrent_creator(tmp_path, sink) -> None:
    target = tmp_path / "output"

    def source():
        target.write_text("other writer")
        yield {"id": 1}

    with pytest.raises(FileExistsError):
        getattr(fpstreams.rows(source()), sink)(target, atomic=True, if_exists="error")
    assert target.read_text() == "other writer"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize("sink", ["to_csv", "to_jsonl", "to_parquet"])
@pytest.mark.parametrize("if_exists", ["error", "replace"])
def test_rows_export_anchors_destination_before_source_opens(
    tmp_path, monkeypatch, sink, if_exists
) -> None:
    initial, unrelated = tmp_path / "initial", tmp_path / "unrelated"
    initial.mkdir()
    unrelated.mkdir()
    (unrelated / "output").write_text("unrelated")
    monkeypatch.chdir(initial)

    def source():
        monkeypatch.chdir(unrelated)
        return iter([{"id": 7}])

    options = {} if sink == "to_parquet" else {"atomic": True}
    getattr(flow.defer(source).rows(), sink)("output", if_exists=if_exists, **options)
    assert (unrelated / "output").read_text() == "unrelated"
    target = initial / "output"
    if sink == "to_parquet":
        assert pq.read_table(target).to_pylist() == [{"id": 7}]
    elif sink == "to_jsonl":
        assert fpstreams.rows.from_jsonl(target).to_list() == [{"id": 7}]
    else:
        assert fpstreams.rows.from_csv(target).to_list() == [{"id": "7"}]
    assert list(initial.iterdir()) == [target]


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("safe", [False, True])
def test_rows_csv_safety_includes_inferred_and_empty_headers(
    tmp_path, explicit, empty, safe
) -> None:
    import csv

    names = ["=1+1", " @command", "normal"]
    records = [] if empty else [dict(zip(names, [3, "+text", -2], strict=True))]
    options = {"fieldnames": names} if explicit else {}
    target = tmp_path / "headers.csv"
    fpstreams.rows(records).to_csv(target, spreadsheet_safe=safe, **options)
    expected = []
    if explicit or not empty:
        expected.append(["'=1+1", "' @command", "normal"] if safe else names)
    if not empty:
        expected.append(["3", "'+text" if safe else "+text", "-2"])
    with target.open(newline="") as handle:
        assert list(csv.reader(handle)) == expected


def test_rows_jsonl_custom_serializer_and_atomic_failure(tmp_path) -> None:
    from datetime import date

    target = tmp_path / "events.jsonl"
    stamp = date(2026, 9, 30)
    seen = []

    def serialize(value):
        seen.append(value)
        return value.isoformat()

    fpstreams.rows([{"date": stamp, "name": "雪"}]).to_jsonl(
        target, default=serialize, ensure_ascii=True, atomic=True
    )
    assert seen == [stamp]
    assert "\\u96ea" in target.read_text()
    assert fpstreams.rows.from_jsonl(target).to_list() == [{"date": "2026-09-30", "name": "雪"}]
    previous = target.read_bytes()
    with pytest.raises(TypeError):
        fpstreams.rows([{"value": object()}]).to_jsonl(target, atomic=True)
    assert target.read_bytes() == previous
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize("sink", ["to_csv", "to_jsonl"])
def test_rows_text_sinks_keep_record_error_primary_when_generator_close_fails(
    tmp_path: Path,
    sink: str,
) -> None:
    def records() -> Iterator[object]:
        try:
            yield object()
        finally:
            raise OSError("row source close failed")

    with pytest.raises(fpstreams.SelectionError) as captured:
        getattr(fpstreams.rows(records()), sink)(tmp_path / sink)

    assert captured.value.__notes__ == ["cleanup failed with OSError: row source close failed"]


def test_rows_text_factories_keep_the_v2_path_keyword(tmp_path: Path) -> None:
    csv_path = tmp_path / "records.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    jsonl_path = tmp_path / "records.jsonl"
    jsonl_path.write_text('{"id": 1}\n', encoding="utf-8")

    assert fpstreams.rows.from_csv(path=csv_path).to_list() == [{"id": "1"}]
    assert fpstreams.Rows.from_csv(path=csv_path).to_list() == [{"id": "1"}]
    assert fpstreams.rows.from_jsonl(path=jsonl_path).to_list() == [{"id": 1}]
    assert fpstreams.Rows.from_jsonl(path=jsonl_path).to_list() == [{"id": 1}]


def test_rows_csv_file_handles_and_openers_have_explicit_ownership() -> None:
    handle = io.StringIO("ignored prefix\nid,name\n1,Ada\n2,Lin\n")
    assert handle.readline() == "ignored prefix\n"
    one_shot = fpstreams.rows.from_csv(handle)

    assert one_shot.first() == {"id": "1", "name": "Ada"}
    assert not handle.closed
    with pytest.raises(fpstreams.FlowConsumedError):
        one_shot.to_list()

    opened: list[io.StringIO] = []

    def opener() -> io.StringIO:
        source = io.StringIO("id,name\n1,Ada\n2,Lin\n")
        opened.append(source)
        return source

    replayable = fpstreams.rows.from_csv(opener)
    expected = [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Lin"}]
    assert replayable.first() == expected[0]
    assert opened[-1].closed
    assert replayable.to_list() == expected
    assert replayable.to_list() == expected
    assert len(opened) == 3
    assert all(source.closed for source in opened)


def test_rows_csv_identity_list_direct_sink_is_narrow_and_preserves_pep479(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.tabular.io import text as _text_sources

    path = tmp_path / "identity.csv"
    path.write_text("id,name\n1,Ada\n2,Lin\n", encoding="utf-8")
    calls: list[str] = []
    original = _text_sources.CSVRowSource.materialize

    def tracked(source: _text_sources.CSVRowSource) -> list[dict[str, Any]]:
        calls.append(source.kind)
        return original(source)

    monkeypatch.setattr(_text_sources.CSVRowSource, "materialize", tracked)
    expected = [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Lin"}]

    assert fpstreams.rows.from_csv(path).to_list() == expected
    assert calls == ["path"]
    calls.clear()
    assert fpstreams.rows.from_csv(path).select("id").to_list() == [
        {"id": "1"},
        {"id": "2"},
    ]
    assert fpstreams.rows.from_csv(path).with_engine("python").to_list() == expected
    assert calls == []

    def stopped_opener() -> io.StringIO:
        raise StopIteration("stopped")

    with pytest.raises(RuntimeError, match="generator raised StopIteration"):
        fpstreams.rows.from_csv(stopped_opener).to_list()


def test_rows_csv_direct_sink_revalidates_bound_source_method_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retained CSV metadata must not outlive its bound Python opener method."""
    rows = fpstreams.rows.from_csv(lambda: io.StringIO("value\n1\n"))
    from fpstreams.tabular.io import text as _text_sources

    def replacement(_source: object) -> Iterator[dict[str, str]]:
        yield {"value": "9"}

    monkeypatch.setattr(
        _text_sources.CSVRowSource.open_records,
        "__code__",
        replacement.__code__,
    )
    expected = [{"value": "9"}]

    assert rows.with_engine("python").to_list() == expected
    assert rows.to_list() == expected


def test_rows_csv_identity_list_declines_failpoints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fpstreams.runtime.failpoints import failpoint
    from fpstreams.tabular.io import text as _text_sources

    path = tmp_path / "observed.csv"
    path.write_text("id\n1\n", encoding="utf-8")

    def unexpected_materialization(_source: _text_sources.CSVRowSource) -> list[dict[str, Any]]:
        raise AssertionError("instrumented execution must use the canonical pipeline")

    monkeypatch.setattr(_text_sources.CSVRowSource, "materialize", unexpected_materialization)
    with failpoint("unrelated.transition", RuntimeError("unused")):
        assert fpstreams.rows.from_csv(path).to_list() == [{"id": "1"}]

    failure = RuntimeError("observed source open")
    with failpoint("source.open.after", failure), pytest.raises(RuntimeError) as raised:
        fpstreams.rows.from_csv(path).to_list()
    assert raised.value is failure

    monkeypatch.undo()
    execution = fpstreams.rows.from_csv(path).run_with_report("to_list")
    assert execution.value == [{"id": "1"}]
    assert execution.report.terminal == "to_list"
    assert execution.report.compiler_engine == "not_compiled"
    assert execution.report.strategy == "csv_direct"


def test_rows_jsonl_file_handles_and_openers_preserve_byte_limits() -> None:
    payload = b'{"id":1}\n{"id":2}\n'
    handle = io.BytesIO(payload)
    one_shot = fpstreams.rows.from_jsonl(handle, max_record_bytes=9)

    assert one_shot.first() == {"id": 1}
    assert not handle.closed
    with pytest.raises(fpstreams.FlowConsumedError):
        one_shot.to_list()

    opened: list[io.StringIO] = []

    def opener() -> io.StringIO:
        source = io.StringIO('{"word":"雪"}\n')
        opened.append(source)
        return source

    replayable = fpstreams.rows.from_jsonl(opener, max_record_bytes=17)
    assert replayable.first() == {"word": "雪"}
    assert opened[-1].closed
    assert replayable.to_list() == [{"word": "雪"}]
    assert replayable.to_list() == [{"word": "雪"}]
    assert len(opened) == 3
    assert all(source.closed for source in opened)


@pytest.mark.parametrize(
    ("kind", "payload", "error_type"),
    [
        ("csv", "id,id\n1,2\n", fpstreams.DuplicateKeyError),
        ("jsonl", "{invalid}\n", json.JSONDecodeError),
    ],
)
def test_text_opener_close_failure_is_a_note_on_the_parse_error(
    kind: str,
    payload: str,
    error_type: type[BaseException],
) -> None:
    class Handle(io.StringIO):
        def __init__(self, value: str) -> None:
            super().__init__(value)
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            super().close()
            raise OSError("text handle close failed")

    opened: list[Handle] = []

    def opener() -> Handle:
        handle = Handle(payload)
        opened.append(handle)
        return handle

    values = fpstreams.rows.from_csv(opener) if kind == "csv" else fpstreams.rows.from_jsonl(opener)
    with pytest.raises(error_type) as captured:
        values.to_list()

    assert captured.value.__notes__ == ["cleanup failed with OSError: text handle close failed"]
    assert len(opened) == 1
    assert opened[0].close_calls == 1

    truncated_unicode = io.BytesIO('{"word":"雪"}\n'.encode())
    with pytest.raises(fpstreams.BufferLimitError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(truncated_unicode, max_record_bytes=10).to_list()
    assert not truncated_unicode.closed


def test_scan_csv_is_reusable_typed_and_keeps_from_csv_string_semantics(tmp_path: Path) -> None:
    """The Arrow scanner is explicit, so the established CSV adapter stays compatible."""
    import pyarrow.csv as pacsv

    path = tmp_path / "typed.csv"
    path.write_text("id,name,active\n1,Ada,true\n2,Lin,false\n", encoding="utf-8")

    scanned = fpstreams.rows.scan_csv(path, batch_size=1)
    expected = [
        {"id": 1, "name": "Ada", "active": True},
        {"id": 2, "name": "Lin", "active": False},
    ]
    assert scanned.to_list() == expected
    assert scanned.to_list() == expected
    assert fpstreams.rows.from_csv(path).first() == {
        "id": "1",
        "name": "Ada",
        "active": "true",
    }
    # Explicit column types exercise the same option path on every supported PyArrow release.
    strings = pacsv.ConvertOptions(
        column_types={name: pa.string() for name in ("id", "name", "active")}
    )
    assert fpstreams.rows.scan_csv(path, convert_options=strings).first() == {
        "id": "1",
        "name": "Ada",
        "active": "true",
    }


@pytest.mark.parametrize("terminal", ["to_list", "count", "to_arrow", "arrow_batches"])
def test_scan_csv_projection_preserves_late_unused_column_conversion_error(
    tmp_path: Path, terminal: str
) -> None:
    """A late type violation outside the inferred first block remains observable."""
    path = tmp_path / "late-type-error.csv"
    with path.open("w", encoding="utf-8") as handle:
        handle.write("id,value,unused\n")
        for index in range(250_000):
            handle.write(f"{index},{index},{'bad' if index == 249_999 else 0}\n")
    for filter_count in (1, 2):
        query = fpstreams.rows.scan_csv(path).where(fpstreams.col("id") >= 249_990)
        if filter_count == 2:
            query = query.where(fpstreams.col("id") < 249_995)
        query = query.select("value")
        errors = []
        for candidate in (query.with_engine("python"), query):
            with pytest.raises(pa.ArrowInvalid) as caught:
                result = getattr(candidate, terminal)()
                if terminal == "arrow_batches":
                    list(result)
            errors.append(str(caught.value))
        assert errors[0] == errors[1]


def test_scan_csv_keeps_source_fields_before_query_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Typed scans validate every visible source field before query projection."""
    from fpstreams.execution import arrow as arrow_execution

    path = tmp_path / "wide.csv"
    path.write_text(
        "id,payload,unused_a,unused_b\n1,one,10,100\n2,two,20,200\n3,three,30,300\n",
        encoding="utf-8",
    )
    schemas: list[tuple[str, ...]] = []
    execute_batch_program = arrow_execution._execute_batch_program

    def traced_batch_program(
        batch: object, operations: tuple[object, ...], projection: object
    ) -> Iterator[object]:
        schemas.append(tuple(batch.schema.names))  # type: ignore[attr-defined]
        yield from execute_batch_program(batch, operations, projection)

    monkeypatch.setattr(arrow_execution, "_execute_batch_program", traced_batch_program)

    result = (
        fpstreams.rows.scan_csv(path, batch_size=2)
        .where(fpstreams.col("id") >= 2)
        .select("payload")
        .to_list()
    )

    assert result == [{"payload": "two"}, {"payload": "three"}]
    assert schemas == [("id", "payload", "unused_a", "unused_b")] * 2


def _observe_csv_reader_options(query, block_sizes):
    """Observe fpstreams reader setup without replacing the guarded PyArrow entrypoint."""
    descriptor = query._flow._terminal_context("list")[1].source.native_data
    opener = descriptor.opener
    outer = dict(zip(opener.__code__.co_freevars, opener.__closure__, strict=True))
    stream = outer["stream"].cell_contents
    cells = dict(zip(stream.__code__.co_freevars, stream.__closure__, strict=True))
    open_reader = cells["open_reader"].cell_contents

    def traced_open_reader(*args, **options):
        configured = options.get("input_read_options")
        block_sizes.append(None if configured is None else configured.block_size)
        return open_reader(*args, **options)

    cells["open_reader"].cell_contents = traced_open_reader


def test_scan_csv_projection_uses_one_typed_source_open(tmp_path: Path) -> None:
    """Query projection neither probes nor reopens a typed CSV source."""
    path = tmp_path / "probe.csv"
    path.write_text("id,unused\n1,10\n2,20\n", encoding="utf-8")
    block_sizes: list[int | None] = []
    query = fpstreams.rows.scan_csv(path).select("id")
    _observe_csv_reader_options(query, block_sizes)

    assert query.to_list() == [{"id": 1}, {"id": 2}]
    assert block_sizes == [None]


@pytest.mark.parametrize("long_header", [False, True])
def test_scan_csv_projection_keeps_default_inference_for_long_headers_and_records(
    tmp_path: Path, long_header: bool
) -> None:
    """Long headers and records retain Arrow's default inference with one source open."""
    path = tmp_path / "long.csv"
    long_value = "x" * 150_000
    if long_header:
        path.write_text(f"{long_value},id\nignored,1\n", encoding="utf-8")
    else:
        path.write_text(f"payload,id\n{long_value},1\n", encoding="utf-8")
    block_sizes: list[int | None] = []
    query = fpstreams.rows.scan_csv(path).select("id")
    _observe_csv_reader_options(query, block_sizes)

    assert query.to_list() == [{"id": 1}]
    assert block_sizes == [None]


def test_scan_csv_projection_probe_preserves_later_parse_error_precedence(tmp_path: Path) -> None:
    """A small schema probe cannot displace the default reader's later parse failure."""
    import pyarrow as pa

    path = tmp_path / "duplicate-header-late-ragged.csv"
    rows = ["id,id", *(f"{index},{index}" for index in range(9000)), "only-one"]
    path.write_text("\n".join(rows), encoding="utf-8")
    assert path.stat().st_size > 64 * 1024

    with pytest.raises(pa.ArrowInvalid) as baseline:
        fpstreams.rows.scan_csv(path).to_list()
    with pytest.raises(pa.ArrowInvalid) as projected:
        fpstreams.rows.scan_csv(path).select("id").to_list()

    assert str(projected.value) == str(baseline.value)


def test_scan_csv_preserves_deferred_and_missing_field_behavior(tmp_path: Path) -> None:
    """Projection discovery must not turn missing fields into eager Arrow schema errors."""
    path = tmp_path / "scope.csv"
    path.write_text("present,hidden\n1,7\n", encoding="utf-8")

    with pytest.raises(fpstreams.SelectionError) as canonical:
        fpstreams.rows([{"present": 1, "hidden": 7}]).select("missing").to_list()
    with pytest.raises(fpstreams.SelectionError) as projected:
        fpstreams.rows.scan_csv(path).select("missing").to_list()
    assert str(projected.value) == str(canonical.value)
    assert type(projected.value.__cause__) is type(canonical.value.__cause__) is KeyError

    header_only = tmp_path / "header-only.csv"
    header_only.write_text("present\n", encoding="utf-8")
    assert fpstreams.rows.scan_csv(header_only).select("missing").to_list() == []

    missing = tmp_path / "missing.csv"
    deferred = fpstreams.rows.scan_csv(missing)
    with pytest.raises(FileNotFoundError):
        deferred.to_list()


def test_scan_csv_closes_early_terminal_and_projected_streams(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both short-circuited and projected reading release their single owned stream."""
    path = tmp_path / "close.csv"
    path.write_text("id,unused\n1,10\n2,20\n", encoding="utf-8")
    opened: list[object] = []
    from fpstreams.tabular import arrow as arrow_adapter

    close = arrow_adapter._close

    def tracked_close(resource: object) -> None:
        if isinstance(resource, pa.NativeFile):
            opened.append(resource)
        close(resource)

    monkeypatch.setattr(arrow_adapter, "_close", tracked_close)

    assert fpstreams.rows.scan_csv(path).first() == {"id": 1, "unused": 10}
    assert len(opened) == 1
    assert opened[0].closed  # type: ignore[attr-defined]

    opened.clear()
    assert fpstreams.rows.scan_csv(path).select("id").to_list() == [{"id": 1}, {"id": 2}]
    assert len(opened) == 1
    assert all(stream.closed for stream in opened)  # type: ignore[attr-defined]


def test_scan_csv_does_not_repeat_invalid_row_callbacks_for_projection(tmp_path: Path) -> None:
    """Projection retains one typed scan and calls each invalid-row callback once."""
    import pyarrow.csv as pacsv

    path = tmp_path / "invalid.csv"
    path.write_text("id,value\n1,one\n2,two,extra\n3,three\n", encoding="utf-8")
    invalid_rows: list[str] = []

    def skip_invalid(row: object) -> str:
        invalid_rows.append(row.text)  # type: ignore[attr-defined]
        return "skip"

    options = pacsv.ParseOptions(invalid_row_handler=skip_invalid)
    assert fpstreams.rows.scan_csv(path, parse_options=options).select("id").to_list() == [
        {"id": 1},
        {"id": 3},
    ]
    assert invalid_rows == ["2,two,extra"]

    invalid_rows.clear()
    assert fpstreams.rows.scan_csv(path, parse_options=options).select("id").first() == {"id": 1}
    assert invalid_rows == ["2,two,extra"]


def test_scan_csv_parse_options_mutation_cannot_enable_duplicate_callbacks(tmp_path: Path) -> None:
    """A supplied mutable ParseOptions keeps the query on the one-open batch projection path."""
    import pyarrow.csv as pacsv

    path = tmp_path / "mutable-invalid.csv"
    path.write_text("id,value\n1,one\n2,two,extra\n3,three\n", encoding="utf-8")
    options = pacsv.ParseOptions()
    query = fpstreams.rows.scan_csv(path, parse_options=options).select("id")
    invalid_rows: list[str] = []

    def skip_invalid(row: object) -> str:
        invalid_rows.append(row.text)  # type: ignore[attr-defined]
        return "skip"

    options.invalid_row_handler = skip_invalid

    assert query.to_list() == [{"id": 1}, {"id": 3}]
    assert invalid_rows == ["2,two,extra"]


def test_scan_csv_keeps_arrow_empty_ragged_and_missing_column_contracts(tmp_path: Path) -> None:
    """The typed scanner intentionally exposes Arrow's stricter structural CSV semantics."""
    import pyarrow.csv as pacsv

    empty = tmp_path / "empty.csv"
    empty.write_bytes(b"")
    with pytest.raises(pa.ArrowInvalid, match="Empty CSV"):
        fpstreams.rows.scan_csv(empty).to_list()

    ragged = tmp_path / "ragged.csv"
    ragged.write_text("id,value\n1\n", encoding="utf-8")
    with pytest.raises(pa.ArrowInvalid, match="Expected 2 columns"):
        fpstreams.rows.scan_csv(ragged).to_list()

    regular = tmp_path / "regular.csv"
    regular.write_text("id\n1\n", encoding="utf-8")
    strict = pacsv.ConvertOptions(include_columns=["id", "missing"])
    with pytest.raises(pa.ArrowKeyError, match="missing"):
        fpstreams.rows.scan_csv(regular, convert_options=strict).to_list()
    nullable = pacsv.ConvertOptions(include_columns=["id", "missing"], include_missing_columns=True)
    assert fpstreams.rows.scan_csv(regular, convert_options=nullable).to_list() == [
        {"id": 1, "missing": None}
    ]


def test_rows_csv_can_neutralize_spreadsheet_formula_cells(tmp_path) -> None:
    path = tmp_path / "safe-rows.csv"

    fpstreams.rows([{"formula": "+1", "spaced": "  -2", "command": "@run", "number": -3}]).to_csv(
        path, spreadsheet_safe=True
    )

    assert path.read_text(encoding="utf-8") == (
        "formula,spaced,command,number\n'+1,'  -2,'@run,-3\n"
    )


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig"])
def test_jsonl_record_limit_counts_bytes_before_decoding(tmp_path, encoding) -> None:
    path = tmp_path / "bounded.jsonl"
    payload = '{"word":"雪"}\n'.encode(encoding)
    path.write_bytes(payload)

    assert fpstreams.rows.from_jsonl(
        path, encoding=encoding, max_record_bytes=len(payload)
    ).to_list() == [{"word": "雪"}]
    assert fpstreams.rows.from_jsonl(path, encoding=encoding, max_record_bytes=None).to_list() == [
        {"word": "雪"}
    ]
    with pytest.raises(fpstreams.BufferLimitError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(
            path, encoding=encoding, max_record_bytes=len(payload) - 1
        ).to_list()


@pytest.mark.parametrize("source_kind", ["path", "borrowed", "opener"])
@pytest.mark.parametrize("record_limit", [None, 64])
@pytest.mark.parametrize("signature_after", ["start", "blank", "record", "in-string"])
def test_borrowed_jsonl_signature_is_only_consumed_at_stream_start(
    tmp_path, source_kind, record_limit, signature_after
) -> None:
    """Binary and text path readers agree on UTF-8 signatures and handle ownership."""
    import codecs

    first = b'{"id":1}\n'
    if signature_after == "start":
        payload = codecs.BOM_UTF8 + first
    elif signature_after == "blank":
        payload = b"\n" + codecs.BOM_UTF8 + first
    elif signature_after == "record":
        payload = first + codecs.BOM_UTF8 + b'{"id":2}\n'
    else:
        payload = codecs.BOM_UTF8 + first + '{"text":"\ufeff"}\n'.encode("utf-8")
    path = tmp_path / "signature.jsonl"
    path.write_bytes(payload)
    handles = []

    def opener():
        handle = io.BytesIO(payload)
        handles.append(handle)
        return handle

    if source_kind == "path":
        source = path
    elif source_kind == "borrowed":
        source = opener()
    else:
        source = opener
    values = fpstreams.rows.from_jsonl(source, encoding="utf_8_sig", max_record_bytes=record_limit)
    assert len(handles) == (1 if source_kind == "borrowed" else 0)
    if signature_after in {"blank", "record"}:
        offending = "\ufeff" + ('{"id":1}\n' if signature_after == "blank" else '{"id":2}\n')
        with pytest.raises(json.JSONDecodeError) as expected:
            json.JSONDecoder().decode(offending)
        with pytest.raises(json.JSONDecodeError) as caught:
            values.to_list()
        assert (caught.value.msg, caught.value.pos) == (expected.value.msg, expected.value.pos)
    else:
        expected_rows = [{"id": 1}]
        if signature_after == "in-string":
            expected_rows.append({"text": "\ufeff"})
        assert values.to_list() == expected_rows
    if source_kind != "path":
        assert len(handles) == 1
        assert handles[0].closed is (source_kind == "opener")
        if source_kind == "borrowed":
            handles[0].close()


def test_unbounded_jsonl_path_uses_incremental_text_decoding(tmp_path: Path) -> None:
    """Multibyte newline encodings must not be split by a binary readline boundary."""
    path = tmp_path / "utf16.jsonl"
    path.write_text('{"word":"雪"}\n{"word":"月"}\n', encoding="utf-16")

    assert fpstreams.rows.from_jsonl(path, encoding="utf-16", max_record_bytes=None).to_list() == [
        {"word": "雪"},
        {"word": "月"},
    ]


def test_jsonl_multibyte_newlines_work_for_binary_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wide binary sources preserve byte bounds, record edges, and handle ownership."""
    first_line = '{"word":"雪\u010a\u0a01"}\n'
    second_line = '{"word":"月"}\n'
    expected = [{"word": "雪\u010a\u0a01"}, {"word": "月"}]
    original_open = open
    path_handles: list[Any] = []

    def tracked_open(*args: Any, **kwargs: Any) -> Any:
        handle = original_open(*args, **kwargs)
        path_handles.append(handle)
        return handle

    monkeypatch.setattr("builtins.open", tracked_open)

    for encoding, width in (("utf-16", 2), ("utf-32", 4)):
        payload = (first_line + second_line).encode(encoding)
        first_record_bytes = width + len(first_line.encode(f"{encoding}-le"))
        second_record_bytes = len(second_line.encode(f"{encoding}-le"))
        assert len(payload) == first_record_bytes + second_record_bytes
        assert first_record_bytes > second_record_bytes
        path = tmp_path / f"{encoding}.jsonl"
        path.write_bytes(payload)

        for record_limit in (None, first_record_bytes):
            assert (
                fpstreams.rows.from_jsonl(
                    path,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                ).to_list()
                == expected
            )

            direct = io.BytesIO(payload)
            assert (
                fpstreams.rows.from_jsonl(
                    direct,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                ).to_list()
                == expected
            )
            assert not direct.closed

            opened: list[io.BytesIO] = []

            def opener(
                payload: bytes = payload,
                opened: list[io.BytesIO] = opened,
            ) -> io.BytesIO:
                handle = io.BytesIO(payload)
                opened.append(handle)
                return handle

            assert (
                fpstreams.rows.from_jsonl(
                    opener,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                ).to_list()
                == expected
            )
            assert len(opened) == 1
            assert opened[0].closed

            short_path_handle_count = len(path_handles)
            assert (
                fpstreams.rows.from_jsonl(
                    path,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                )
                .take(1)
                .to_list()
                == expected[:1]
            )
            assert len(path_handles) == short_path_handle_count + 1
            assert path_handles[-1].closed

            direct = io.BytesIO(payload)
            assert (
                fpstreams.rows.from_jsonl(
                    direct,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                )
                .take(1)
                .to_list()
                == expected[:1]
            )
            assert not direct.closed
            assert direct.tell() == first_record_bytes

            assert (
                fpstreams.rows.from_jsonl(
                    opener,
                    encoding=encoding,
                    max_record_bytes=record_limit,
                )
                .take(1)
                .to_list()
                == expected[:1]
            )
            assert len(opened) == 2
            assert opened[-1].closed

        with pytest.raises(fpstreams.BufferLimitError, match="max_record_bytes"):
            fpstreams.rows.from_jsonl(
                path,
                encoding=encoding,
                max_record_bytes=first_record_bytes - 1,
            ).to_list()

        direct = io.BytesIO(payload)
        with pytest.raises(fpstreams.BufferLimitError, match="max_record_bytes"):
            fpstreams.rows.from_jsonl(
                direct,
                encoding=encoding,
                max_record_bytes=first_record_bytes - 1,
            ).to_list()
        assert not direct.closed

    assert path_handles
    assert all(handle.closed for handle in path_handles)


@pytest.mark.parametrize("codec", ["utf-16", "utf-32"])
@pytest.mark.parametrize("byte_order", ["le", "be"])
@pytest.mark.parametrize("source_kind", ["path", "borrowed", "opener"])
@pytest.mark.parametrize("record_limit", [None, 64])
def test_borrowed_wide_jsonl_requires_explicit_byte_order_without_bom(
    tmp_path, codec, byte_order, source_kind, record_limit
) -> None:
    """Generic stream codecs require signatures; explicit byte-order codecs need none."""
    import codecs

    encoding = f"{codec}-{byte_order}"
    payload = '{"id":1}\n'.encode(encoding)
    signature = getattr(codecs, f"BOM_UTF{codec[-2:]}_{byte_order.upper()}")
    path = tmp_path / "byteorder.jsonl"

    class Fragmented(io.BytesIO):
        def readline(self, size=-1):
            width = 2 if codec == "utf-16" else 4
            return super().readline(width if size < 0 else min(width, size))

        def read(self, size=-1):
            return super().read(1 if size < 0 else min(1, size))

    def read(data, selected_codec, *, missing_bom=False, fragmented=False):
        path.write_bytes(data)
        handles = []

        def opener():
            handle = Fragmented(data) if fragmented else io.BytesIO(data)
            handles.append(handle)
            return handle

        if source_kind == "path":
            source = path
        elif source_kind == "borrowed":
            source = opener()
        else:
            source = opener
        values = fpstreams.rows.from_jsonl(
            source, encoding=selected_codec, max_record_bytes=record_limit
        )
        try:
            if missing_bom:
                with (
                    pytest.raises(UnicodeError) as expected,
                    io.TextIOWrapper(io.BytesIO(data), encoding=selected_codec) as reference,
                ):
                    reference.read()
                with pytest.raises(UnicodeError) as caught:
                    values.to_list()
                assert str(caught.value) == str(expected.value)
            else:
                assert values.to_list() == [{"id": 1}]
            if source_kind != "path":
                assert len(handles) == 1
                assert handles[0].closed is (source_kind == "opener")
        finally:
            for handle in handles:
                if not handle.closed:
                    handle.close()

    read(payload, encoding)
    read(signature + payload, codec)
    read(payload, codec, missing_bom=True)
    read("\U0001f600".encode(encoding), codec, missing_bom=True, fragmented=True)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16", "utf-32"])
@pytest.mark.parametrize("record_limit", [None, 128])
@pytest.mark.parametrize("ending", ["lf", "crlf", "cr", "trailing-cr"])
def test_borrowed_jsonl_line_endings_do_not_depend_on_record_limit(
    tmp_path, encoding, record_limit, ending
) -> None:
    """Managed text and binary paths split on LF; a trailing CR remains JSON whitespace."""
    if ending == "trailing-cr":
        content = '{"id":1}\r'
        expected_rows = [{"id": 1}]
    else:
        separator = {"lf": "\n", "crlf": "\r\n", "cr": "\r"}[ending]
        content = separator.join(('{"id":1}', '{"id":2}')) + separator
        expected_rows = [{"id": 1}, {"id": 2}]
    payload = content.encode(encoding)
    path = tmp_path / "newlines.jsonl"
    path.write_bytes(payload)
    for source_kind in ("path", "borrowed", "opener"):
        handles = []

        def opener(handles=handles):
            handle = io.BytesIO(payload)
            handles.append(handle)
            return handle

        source = (
            path if source_kind == "path" else opener() if source_kind == "borrowed" else opener
        )
        values = fpstreams.rows.from_jsonl(source, encoding=encoding, max_record_bytes=record_limit)
        try:
            if ending == "cr":
                with pytest.raises(json.JSONDecodeError) as expected:
                    json.JSONDecoder().decode(content)
                with pytest.raises(json.JSONDecodeError) as caught:
                    values.to_list()
                assert (caught.value.msg, caught.value.pos) == (
                    expected.value.msg,
                    expected.value.pos,
                )
            else:
                assert values.to_list() == expected_rows
            if handles:
                assert len(handles) == 1
                assert handles[0].closed is (source_kind == "opener")
        finally:
            for handle in handles:
                if not handle.closed:
                    handle.close()


@pytest.mark.parametrize("record_limit", [None, 128])
def test_borrowed_jsonl_text_keeps_caller_newline_policy(record_limit) -> None:
    """A borrowed decoded text stream keeps the caller's universal-newline behavior."""
    with io.TextIOWrapper(io.BytesIO(b'{"id":1}\r{"id":2}\r'), newline=None) as handle:
        assert fpstreams.rows.from_jsonl(handle, max_record_bytes=record_limit).to_list() == [
            {"id": 1},
            {"id": 2},
        ]
        assert not handle.closed


def test_jsonl_extra_data_error_matches_the_standard_decoder() -> None:
    """A line fast path must preserve JSON's narrow trailing-whitespace rules."""
    line = '{"id":1} \v\n'
    with pytest.raises(json.JSONDecodeError) as expected:
        json.JSONDecoder().decode(line)
    with pytest.raises(json.JSONDecodeError) as actual:
        fpstreams.rows.from_jsonl(io.StringIO(line), max_record_bytes=None).to_list()

    assert (actual.value.msg, actual.value.pos, actual.value.lineno, actual.value.colno) == (
        expected.value.msg,
        expected.value.pos,
        expected.value.lineno,
        expected.value.colno,
    )


def test_unbounded_jsonl_text_handle_does_not_reencode_records() -> None:
    """Without a byte limit, decoded text should go directly to the JSON decoder."""

    class DecodedLine(str):
        def encode(self, *_args: object, **_kwargs: object) -> bytes:
            raise AssertionError("an unbounded text record must not be encoded again")

    class TextHandle:
        def __init__(self) -> None:
            self.lines = [DecodedLine('{"id":1}\n')]

        def __iter__(self) -> Iterator[str]:
            return iter(self.lines)

        def readline(self, _size: int = -1) -> str:
            return self.lines.pop(0) if self.lines else ""

    assert fpstreams.rows.from_jsonl(TextHandle(), max_record_bytes=None).to_list() == [{"id": 1}]


def test_jsonl_reuses_one_strict_decoder_per_iteration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Decoder setup is per file scan, not repeated for every physical record."""
    path = tmp_path / "decoder.jsonl"
    path.write_text('\n{"word":"café"}\n{"nested":{"id":2}}\n', encoding="latin-1")
    original_decoder = json.JSONDecoder
    constructed: list[None] = []

    class TrackingDecoder(original_decoder):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            constructed.append(None)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(json, "JSONDecoder", TrackingDecoder)
    source = fpstreams.rows.from_jsonl(path, encoding="latin-1")

    assert source.to_list() == [{"word": "café"}, {"nested": {"id": 2}}]
    assert len(constructed) == 1
    assert source.to_list() == [{"word": "café"}, {"nested": {"id": 2}}]
    assert len(constructed) == 2


def test_jsonl_short_circuit_closes_the_bounded_binary_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "close.jsonl"
    path.write_text('{"id":1}\n{"id":2}\n', encoding="utf-8")
    original_open = open
    opened: list[Any] = []

    def tracked_open(*args: Any, **kwargs: Any) -> Any:
        handle = original_open(*args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr("builtins.open", tracked_open)

    assert fpstreams.rows.from_jsonl(path).take(1).to_list() == [{"id": 1}]
    assert len(opened) == 1
    assert opened[0].closed


def test_jsonl_record_limit_is_validated_before_opening_the_file(tmp_path) -> None:
    missing = tmp_path / "missing.jsonl"

    with pytest.raises(ValueError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(missing, max_record_bytes=0)
    with pytest.raises(TypeError, match="max_record_bytes"):
        fpstreams.rows.from_jsonl(missing, max_record_bytes=2.5)  # type: ignore[arg-type]

    deferred = fpstreams.rows.from_jsonl(missing)
    with pytest.raises(FileNotFoundError):
        deferred.to_list()


def test_rows_io_rejects_duplicate_fields_instead_of_losing_data(tmp_path) -> None:
    csv_path = tmp_path / "duplicate.csv"
    jsonl_path = tmp_path / "duplicate.jsonl"
    csv_path.write_text("id,id\n1,2\n", encoding="utf-8")
    jsonl_path.write_text(
        '\n{"ok":true}\n{"user":{"id":1,"id":2}}\n',
        encoding="utf-8",
    )

    with pytest.raises(fpstreams.DuplicateKeyError, match="CSV"):
        fpstreams.rows.from_csv(csv_path).to_list()
    with pytest.raises(fpstreams.DuplicateKeyError, match="Arrow schema"):
        fpstreams.rows.scan_csv(csv_path).to_list()
    with pytest.raises(
        fpstreams.DuplicateKeyError,
        match="JSON Lines record 3 contains duplicate key 'id'",
    ):
        fpstreams.rows.from_jsonl(jsonl_path).to_list()
    with pytest.raises(fpstreams.DuplicateKeyError, match="to_csv"):
        fpstreams.rows([{"id": 1}]).to_csv(csv_path, fieldnames=("id", "id"))


class _ReadCursor:
    def __init__(self, columns: Sequence[str], values: Sequence[tuple[Any, ...]]) -> None:
        self.description = tuple((name,) for name in columns)
        self._values = list(values)
        self.executed: tuple[str, object] | None = None
        self.fetch_sizes: list[int] = []
        self.closed = False

    def execute(self, query: str, parameters: object = None) -> None:
        self.executed = (query, parameters)

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        self.fetch_sizes.append(size)
        batch, self._values = self._values[:size], self._values[size:]
        return batch

    def close(self) -> None:
        self.closed = True


class _ReadConnection:
    def __init__(self, cursor: _ReadCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _ReadCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


class _WriteCursor:
    def __init__(self, *, fail_on: int | None = None) -> None:
        self.batches: list[list[object]] = []
        self.fail_on = fail_on
        self.closed = False

    def executemany(self, _statement: str, values: Sequence[object]) -> None:
        self.batches.append(list(values))
        if len(self.batches) == self.fail_on:
            raise RuntimeError("database write failed")

    def close(self) -> None:
        self.closed = True


class _WriteConnection:
    def __init__(self, cursor: _WriteCursor) -> None:
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> _WriteCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def test_db_source_is_lazy_reusable_batched_and_closes_on_take() -> None:
    connections: list[_ReadConnection] = []

    def connect() -> _ReadConnection:
        connection = _ReadConnection(_ReadCursor(("id", "name"), ((1, "a"), (2, "b"))))
        connections.append(connection)
        return connection

    source = fpstreams.rows.from_db(
        connect,
        "select id, name from events where id >= :minimum",
        {"minimum": 1},
        batch_size=2,
    )
    assert connections == []
    assert source.take(1).to_list() == [{"id": 1, "name": "a"}]
    assert connections[0]._cursor.executed == (
        "select id, name from events where id >= :minimum",
        {"minimum": 1},
    )
    assert connections[0]._cursor.fetch_sizes == [2]
    assert connections[0]._cursor.closed and connections[0].closed

    assert source.to_list() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert len(connections) == 2
    assert connections[1]._cursor.closed and connections[1].closed


def test_db_source_rejects_ambiguous_columns_and_still_closes() -> None:
    connection = _ReadConnection(_ReadCursor(("value", "value"), ((1, 2),)))
    with pytest.raises(fpstreams.DuplicateKeyError, match="duplicate column"):
        fpstreams.rows.from_db(lambda: connection, "select 1, 2").to_list()
    assert connection._cursor.closed and connection.closed


def test_db_source_cleanup_failure_ignores_an_ambient_outer_exception() -> None:
    class FailingCursor(_ReadCursor):
        def __init__(self) -> None:
            super().__init__(("value",), ((1,),))
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            raise OSError("cursor close failed")

    cursor = FailingCursor()
    connection = _ReadConnection(cursor)
    try:
        raise ValueError("outer")
    except ValueError as outer:
        with pytest.raises(OSError, match="cursor close failed"):
            fpstreams.rows.from_db(lambda: connection, "select 1").take(1).to_list()
        assert getattr(outer, "__notes__", ()) == ()

    assert cursor.close_calls == 1
    assert connection.closed


def test_db_sink_batches_commits_rolls_back_and_closes_upstream() -> None:
    cursor = _WriteCursor()
    connection = _WriteConnection(cursor)
    assert (
        fpstreams.rows([{"id": 1}, {"id": 2}, {"id": 3}]).to_db(
            lambda: connection,
            "insert into events values (?)",
            parameters=lambda row: (row["id"],),
            batch_size=2,
        )
        == 3
    )
    assert cursor.batches == [[(1,), (2,)], [(3,)]]
    assert connection.commits == 1 and connection.rollbacks == 0
    assert cursor.closed and connection.closed

    events: list[str] = []

    def records() -> Iterator[dict[str, int]]:
        events.append("open")
        try:
            for value in range(5):
                yield {"id": value}
        finally:
            events.append("close")

    failed_cursor = _WriteCursor(fail_on=2)
    failed = _WriteConnection(failed_cursor)
    with pytest.raises(RuntimeError, match="write failed"):
        fpstreams.rows(records()).to_db(
            lambda: failed,
            "insert into events values (?)",
            parameters=lambda row: (row["id"],),
            batch_size=2,
        )
    assert failed.commits == 0 and failed.rollbacks == 1
    assert failed_cursor.closed and failed.closed
    assert events == ["open", "close"]


def test_rows_to_db_closes_database_handles_when_source_close_raises_base_exception() -> None:
    class Source(Iterator[dict[str, int]]):
        def __init__(self) -> None:
            self.emitted = False
            self.close_calls = 0

        def __next__(self) -> dict[str, int]:
            if self.emitted:
                raise StopIteration
            self.emitted = True
            return {"id": 1}

        def close(self) -> None:
            self.close_calls += 1
            raise KeyboardInterrupt("source close interrupted")

    class DirectRows(fpstreams.Rows[dict[str, int]]):
        def __init__(self, source: Source) -> None:
            self.source = source

        def __iter__(self) -> Iterator[dict[str, int]]:
            return self.source

    source = Source()
    cursor = _WriteCursor()
    connection = _WriteConnection(cursor)

    try:
        raise ValueError("outer")
    except ValueError as outer:
        with pytest.raises(KeyboardInterrupt, match="source close interrupted"):
            DirectRows(source).to_db(
                lambda: connection,
                "insert into events values (?)",
                parameters=lambda row: (row["id"],),
                batch_size=1,
            )
        assert getattr(outer, "__notes__", ()) == ()

    assert source.close_calls == 1
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("destination", ["csv", "sqlite_append", "sqlite_replace"])
@pytest.mark.parametrize("explicit_columns", [False, True])
def test_record_sink_first_conversion_stopiteration_is_not_empty_input(
    tmp_path, destination, explicit_columns
):
    failure = StopIteration("first record conversion failed")
    events = []

    class Record:
        def _asdict(self):
            events.append("convert")
            raise failure

    def source():
        try:
            yield Record()
            events.append("unexpected second pull")
            yield {"id": 9}
        finally:
            events.append("close")

    database = tmp_path / "records.sqlite"
    fpstreams.rows([{"id": 42}]).to_sqlite(database, "events")
    query = fpstreams.rows(source())
    with pytest.raises(StopIteration) as caught:
        if destination == "csv":
            query.to_csv(tmp_path / "records.csv", fieldnames=["id"] if explicit_columns else None)
        else:
            query.to_sqlite(
                database,
                "events",
                if_exists=destination.removeprefix("sqlite_"),
                columns=["id"] if explicit_columns else None,
            )
    assert caught.value is failure
    assert events == ["convert", "close"]
    assert fpstreams.rows.from_sqlite(database, "SELECT * FROM events").to_list() == [{"id": 42}]


@pytest.mark.parametrize(
    "api,sink",
    [
        ("flow", "to_csv"),
        ("flow", "to_json"),
        ("flow", "to_jsonl"),
        ("rows", "to_csv"),
        ("rows", "to_jsonl"),
    ],
)
@pytest.mark.parametrize("atomic", [False, True], ids=["direct", "atomic"])
@pytest.mark.parametrize("body_fails", [False, True], ids=["close-only", "body-and-close"])
def test_atomic_text_export_preserves_primary_error(
    api, sink, atomic, body_fails, tmp_path, monkeypatch
) -> None:
    """Body and cleanup failures retain their order across every owned text writer."""
    import builtins

    import fpstreams.streams.flow_terminals as flow_terminals
    import fpstreams.tabular.io as rows_io

    target = tmp_path / "output"
    target.write_bytes(b"old target")
    primary = ValueError("serialization failed")
    writer_error = OSError("writer close failed")
    source_error = LookupError("source close failed")
    events = []
    handles = []
    original_open = builtins.open

    class Cell:
        def __str__(self):
            raise primary

    class Writer:
        def __init__(self, handle):
            self.handle = handle

        def __getattr__(self, name):
            return getattr(self.handle, name)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

        def close(self):
            self.handle.close()
            events.append("writer")
            raise writer_error

    def open_writer(*args, **kwargs):
        writer = Writer(original_open(*args, **kwargs))
        handles.append(writer)
        return writer

    def source():
        try:
            yield {"value": Cell() if body_fails else 1}
        finally:
            events.append("source")
            if body_fails:
                raise source_error

    def serialize(value):
        raise primary

    expected = primary if body_fails else writer_error
    options = {"atomic": atomic}
    if sink != "to_csv":
        options["default"] = serialize
    module = flow_terminals if api == "flow" else rows_io
    with monkeypatch.context() as patch:
        patch.setattr(module, "open", open_writer, raising=False)
        query = fpstreams.flow(source()) if api == "flow" else fpstreams.rows(source())
        with pytest.raises(type(expected)) as caught:
            getattr(query, sink)(target, **options)
    assert caught.value is expected
    assert len(handles) == 1 and handles[0].handle.closed
    assert events == (["writer", "source"] if body_fails else ["source", "writer"])
    if body_fails:
        notes = primary.__notes__
        writer_note = next(
            index for index, note in enumerate(notes) if "writer close failed" in note
        )
        source_note = next(
            index for index, note in enumerate(notes) if "source close failed" in note
        )
        assert writer_note < source_note
    if atomic:
        assert target.read_bytes() == b"old target"
    assert list(tmp_path.iterdir()) == [target]
