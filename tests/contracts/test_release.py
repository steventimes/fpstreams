"""Release artifacts, core failure injection, and resource ownership contracts."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
import tomllib
import zipfile
from base64 import urlsafe_b64encode
from collections.abc import AsyncIterator
from email.parser import Parser
from pathlib import Path
from typing import Any, cast

import pytest

import fpstreams
from fpstreams import aflow
from fpstreams.execution.asynchronous import execute_async_physical
from fpstreams.physical.async_plan import compile_async_query
from fpstreams.planning.source import Source
from fpstreams.runtime.failpoints import failpoint, hit
from fpstreams.runtime.limits import QueryLimits
from fpstreams.runtime.metrics import QueryMetrics
from fpstreams.runtime.query import QueryRuntime
from fpstreams.runtime.resources import ResourceRegistry
from fpstreams.runtime.tasks import TaskRole, TaskRuntime
from fpstreams.storage.spill_store import SpillStore
from fpstreams.streams.flow import flow

# --- Release and core environment contracts ---


ROOT = Path(__file__).parents[2]
BROWSER_WHEEL_BUILDER = ROOT / "scripts" / "build_browser_wheel.py"


def test_release_version_check_rejects_a_stale_public_version_constant(tmp_path: Path) -> None:
    """A correct manifest header must not hide its stale exported version value."""
    script = tmp_path / "scripts" / "check_release_version.py"
    files = (
        "scripts/check_release_version.py",
        "pyproject.toml",
        "rust/Cargo.toml",
        "rust/Cargo.lock",
        "uv.lock",
        "src/fpstreams/__init__.py",
        "tests/public_api_v2.json",
        "CHANGELOG.md",
    )
    for name in files:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, target)
    api_path = tmp_path / "tests/public_api_v2.json"
    api = json.loads(api_path.read_text())
    api["items"]["__version__"]["value"] = api["version"]
    api_path.write_text(json.dumps(api))
    valid = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    assert valid.returncode == 0, valid.stderr

    api["items"]["__version__"]["value"] = "0.0.0"
    api_path.write_text(json.dumps(api))
    stale = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    assert stale.returncode != 0
    assert "release version mismatch" in stale.stderr


def test_core_factory_types_without_optional_adapters() -> None:
    """Missing optional adapters must not erase types or hide invalid calls."""
    if importlib.util.find_spec("mypy") is None:
        pytest.skip("core typing regression requires mypy from the type quality group")
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "check_typing.py"), "--checker", "mypy"],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_rust_sources_declare_free_threading_without_unsafe_shared_state() -> None:
    """Audit every Rust module, including nested relational implementations."""
    sources = tuple((ROOT / "rust" / "src").rglob("*.rs"))
    text = "\n".join(path.read_text(encoding="utf-8") for path in sources)

    assert text.count("#[pymodule(gil_used = false)]") == 1
    assert re.search(r"static\s+mut\b", text) is None
    assert re.search(r"unsafe\s+impl\s+(?:Send|Sync)", text) is None


# --- Failpoint matrix ---


ROOT = Path(__file__).parents[2]
REQUIRED_FAILPOINTS = frozenset(
    {
        "source.open.after",
        "iterator.pull.after",
        "callback.before",
        "callback.after",
        "expression.guard.before",
        "backend.convert.after",
        "resource.register.after",
        "resource.close.before",
        "spill.mkdir.after",
        "spill.open.after",
        "spill.write.before",
        "spill.read.before",
        "spill.generation.replace.before",
        "spill.unlink.before",
        "sort.run.flush.after",
        "sort.merge.pull.after",
        "group.state.create.after",
        "join.build.insert.after",
        "join.probe.match.after",
        "task.create.after",
        "task.complete.before_publish",
        "task.cancel.before",
        "timer.arm.after",
        "timer.fire.before_publish",
        "db.connect.after",
        "db.cursor.after",
        "db.execute.after",
        "db.fetch.after",
        "db.commit.before",
        "arrow.reader.after",
        "arrow.batch.after",
    }
)


def _production_failpoints() -> set[str]:
    """Read literal ``hit(name)`` calls without importing production modules."""
    names: set[str] = set()
    for path in (ROOT / "src" / "fpstreams").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "hit"
                and len(node.args) == 1
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                names.add(node.args[0].value)
    return names


def test_every_planned_failpoint_is_reachable_from_production_code() -> None:
    """Registry-only names do not count: each one must guard an owned transition."""
    assert _production_failpoints() >= REQUIRED_FAILPOINTS


# --- Failure injection and cleanup ---


def test_failpoint_is_scoped_nested_and_disabled_by_default() -> None:
    hit("spill.write.before")
    with failpoint("spill.write.before", OSError("disk")):
        with pytest.raises(OSError, match="disk"):
            hit("spill.write.before")
        with (
            failpoint("spill.write.before", RuntimeError("inner")),
            pytest.raises(RuntimeError, match="inner"),
        ):
            hit("spill.write.before")
        with pytest.raises(OSError, match="disk"):
            hit("spill.write.before")
    hit("spill.write.before")


def test_spill_transition_failpoints_are_reachable(tmp_path) -> None:
    with (
        QueryRuntime() as runtime,
        failpoint("spill.mkdir.after", OSError("mkdir")),
        pytest.raises(OSError, match="mkdir"),
    ):
        SpillStore(runtime, parent=tmp_path, operation="test")


def test_resource_transition_failpoints_preserve_ownership() -> None:
    closed: list[bool] = []
    registry = ResourceRegistry()
    resource = object()
    with (
        failpoint("resource.register.after", RuntimeError("registered")),
        pytest.raises(RuntimeError, match="registered"),
    ):
        registry.own(resource, lambda _value: closed.append(True))

    registry.close()
    assert closed == [True]

    with (
        failpoint("resource.close.before", OSError("close")),
        pytest.raises(OSError, match="close"),
    ):
        registry = ResourceRegistry()
        registry.own(object(), lambda _value: None)
        registry.close()


def test_source_open_failpoint_closes_new_iterator() -> None:
    closed: list[bool] = []

    class Values:
        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def close(self) -> None:
            closed.append(True)

    source = Source.defer(Values)
    with (
        failpoint("source.open.after", OSError("open")),
        pytest.raises(OSError, match="open"),
    ):
        source.open()

    assert closed == [True]


def test_pull_failpoint_stops_before_callback() -> None:
    callbacks: list[int] = []
    pipeline = flow([1, 2]).map(lambda value: callbacks.append(value) or value)

    with (
        failpoint("iterator.pull.after", OSError("pull")),
        pytest.raises(OSError, match="pull"),
    ):
        pipeline.to_list()

    assert callbacks == []

    with (
        failpoint("iterator.pull.after", OSError("identity pull")),
        pytest.raises(OSError, match="identity pull"),
    ):
        flow([1, 2]).sum()


def test_task_create_failpoint_keeps_scheduled_task_owned_until_close() -> None:
    async def scenario() -> None:
        runtime = TaskRuntime(QueryLimits(), QueryMetrics())
        with (
            failpoint("task.create.after", RuntimeError("task")),
            pytest.raises(RuntimeError, match="task"),
        ):
            runtime.create_task(asyncio.sleep(0), role=TaskRole.OPERATOR)

        assert runtime.live_count == 1
        await runtime.aclose()
        assert runtime.live_count == 0

    asyncio.run(scenario())


def test_callback_failpoints_preserve_callback_prefix() -> None:
    seen: list[int] = []
    pipeline = flow([1, 2]).map(lambda value: seen.append(value) or value)

    with (
        failpoint("callback.before", OSError("before")),
        pytest.raises(OSError, match="before"),
    ):
        pipeline.to_list()
    assert seen == []

    pipeline = flow([1, 2]).map(lambda value: seen.append(value) or value)
    with (
        failpoint("callback.after", OSError("after")),
        pytest.raises(OSError, match="after"),
    ):
        pipeline.to_list()
    assert seen == [1]


@pytest.mark.parametrize("validate", ["m:m", "m:1"])
def test_join_build_failpoint_closes_right_source(validate: str) -> None:
    closed: list[bool] = []

    def right_values():
        try:
            yield {"id": 1}
        finally:
            closed.append(True)

    joined = fpstreams.rows([{"id": 1}]).join(
        fpstreams.rows(right_values()), on="id", validate=validate
    )
    with (
        failpoint("join.build.insert.after", OSError("index")),
        pytest.raises(OSError, match="index"),
    ):
        joined.to_list()

    assert closed == [True]


def test_group_state_failpoint_closes_source() -> None:
    closed: list[bool] = []

    def values():
        try:
            yield {"team": "a", "value": 1}
        finally:
            closed.append(True)

    grouped = fpstreams.rows(values()).group_by("team").aggregate(total=fpstreams.agg.sum("value"))
    with (
        failpoint("group.state.create.after", OSError("group")),
        pytest.raises(OSError, match="group"),
    ):
        grouped.to_list()

    assert closed == [True]


@pytest.mark.parametrize("validate", ["m:m", "m:1"])
def test_join_match_failpoint_closes_both_sources(validate: str) -> None:
    closed: list[str] = []

    def source(label: str):
        try:
            yield {"id": 1, label: label}
        finally:
            closed.append(label)

    joined = fpstreams.rows(source("left")).join(
        fpstreams.rows(source("right")), on="id", validate=validate
    )
    with (
        failpoint("join.probe.match.after", OSError("match")),
        pytest.raises(OSError, match="match"),
    ):
        joined.to_list()

    assert sorted(closed) == ["left", "right"]


def test_timer_arm_failpoint_releases_the_owned_task() -> None:
    async def scenario() -> None:
        runtime = TaskRuntime(QueryLimits(max_tasks=2), QueryMetrics())
        scope = runtime.scope("timer")
        from fpstreams.execution.asynchronous.timers import TimerHandle

        timer = TimerHandle(scope)
        with (
            failpoint("timer.arm.after", OSError("arm")),
            pytest.raises(OSError, match="arm"),
        ):
            await timer.arm(0)
        await timer.aclose()
        await runtime.aclose()
        assert runtime.live_count == 0

    asyncio.run(scenario())


def test_task_completion_and_cancellation_failpoints_leave_no_live_task() -> None:
    async def scenario() -> None:
        runtime = TaskRuntime(QueryLimits(max_tasks=2), QueryMetrics())
        task = runtime.create_task(asyncio.sleep(0, result=1), role=TaskRole.OPERATOR)
        with (
            failpoint("task.complete.before_publish", OSError("publish")),
            pytest.raises(OSError, match="publish"),
        ):
            await runtime.take_result(task)
        assert runtime.live_count == 0

        task = runtime.create_task(asyncio.sleep(1), role=TaskRole.OPERATOR)
        with (
            failpoint("task.cancel.before", OSError("cancel")),
            pytest.raises(OSError, match="cancel"),
        ):
            await runtime.cancel(task)
        await runtime.aclose()
        assert runtime.live_count == 0

    asyncio.run(scenario())


def test_database_failpoint_closes_query_resources() -> None:
    closed: list[str] = []

    class Cursor:
        description = (("value",),)

        def execute(self, _query: str) -> None:
            return None

        def fetchmany(self, _size: int) -> list[tuple[int]]:
            return [(1,)]

        def close(self) -> None:
            closed.append("cursor")

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

        def close(self) -> None:
            closed.append("connection")

    source = fpstreams.rows.from_db(Connection, "select 1", batch_size=1)
    with (
        failpoint("db.fetch.after", OSError("fetch")),
        pytest.raises(OSError, match="fetch"),
    ):
        source.to_list()

    assert closed == ["cursor", "connection"]


def test_database_commit_failpoint_rolls_back_and_closes_resources() -> None:
    events: list[str] = []

    class Cursor:
        def executemany(self, _statement: str, _batch: list[tuple[int]]) -> None:
            events.append("execute")

        def close(self) -> None:
            events.append("cursor.close")

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

        def commit(self) -> None:
            events.append("commit")

        def rollback(self) -> None:
            events.append("rollback")

        def close(self) -> None:
            events.append("connection.close")

    with (
        failpoint("db.commit.before", OSError("commit")),
        pytest.raises(OSError, match="commit"),
    ):
        fpstreams.rows([{"value": 1}]).to_db(Connection, "insert", batch_size=1)

    assert events == ["execute", "rollback", "cursor.close", "connection.close"]


def test_sort_flush_failpoint_removes_spill_files(tmp_path) -> None:
    pipeline = flow([4, 3, 2, 1]).external_sort_by(
        lambda value: value, buffer_size=1, tempdir=tmp_path
    )
    with (
        failpoint("sort.run.flush.after", OSError("flush")),
        pytest.raises(OSError, match="flush"),
    ):
        pipeline.to_list()

    assert list(tmp_path.iterdir()) == []


def test_arrow_reader_failpoint_closes_the_one_shot_reader() -> None:
    import pyarrow as pa

    reader = pa.RecordBatchReader.from_batches(
        pa.schema([("value", pa.int64())]), [pa.record_batch([[1]], names=["value"])]
    )
    source = fpstreams.rows.from_arrow(reader)
    with (
        failpoint("arrow.reader.after", OSError("reader")),
        pytest.raises(OSError, match="reader"),
    ):
        source.to_list()


def test_expression_guard_failpoint_prevents_source_consumption() -> None:
    consumed: list[int] = []

    def values():
        for value in range(3):
            consumed.append(value)
            yield value

    pipeline = flow(values()).map(lambda value: value + 1)
    with (
        failpoint("expression.guard.before", OSError("guard")),
        pytest.raises(OSError, match="guard"),
    ):
        pipeline.to_list()

    assert consumed == []


# --- Optional backend compatibility ---


ROOT = Path(__file__).parents[2]


def _blocked_import_script(module: str, expression: str) -> str:
    body = textwrap.indent(expression.strip(), "    ")
    return f'''
import builtins
original_import = builtins.__import__

def blocked(name, *args, **kwargs):
    if name == "{module}" or name.startswith("{module}."):
        raise ImportError("blocked optional dependency: {module}")
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked
import fpstreams
try:
{body}
except ImportError as error:
    print(error)
else:
    raise SystemExit("optional-backend call unexpectedly succeeded")
'''


def test_arrow_adapter_reports_a_clean_missing_extra_error() -> None:
    """An installed developer extra cannot hide the supported no-Arrow configuration."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _blocked_import_script(
                "pyarrow",
                "fpstreams.rows([{'id': 1}]).to_arrow()",
            ),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "blocked optional dependency: pyarrow" in result.stdout


def test_pandas_adapter_reports_a_clean_missing_extra_error() -> None:
    """The data adapter preserves its documented missing-extra failure path."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _blocked_import_script(
                "pandas",
                "fpstreams.rows([{'id': 1}]).to_pandas()",
            ),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "to_pandas() requires the 'data' extra" in result.stdout


def _isolated_base_dependency_path(tmp_path: Path) -> str:
    """Expose the one pre-3.13 base dependency without exposing optional site packages."""
    if sys.version_info >= (3, 13):
        return ""
    import typing_extensions

    dependency_dir = tmp_path / "base-dependencies"
    dependency_dir.mkdir(exist_ok=True)
    shutil.copyfile(typing_extensions.__file__, dependency_dir / "typing_extensions.py")
    return f"sys.path.insert(0, {str(dependency_dir)!r})\n"


@pytest.mark.parametrize("target", ["#stable-heading", "target.md#stable-heading"])
def test_documentation_build_rejects_renamed_link_targets(target: str, tmp_path: Path) -> None:
    """The site's strict build rejects broken local and cross-page heading links."""
    if importlib.util.find_spec("mkdocs") is None:
        pytest.skip("documentation link validation requires the docs dependency group")
    import yaml

    project_config = yaml.safe_load((ROOT / "fpstreams/mkdocs.yml").read_text(encoding="utf-8"))
    docs = tmp_path / "docs"
    docs.mkdir()
    index = docs / "index.md"
    index.write_text(f"# Home\n\n[Details]({target})\n\n## Stable heading\n", encoding="utf-8")
    other = docs / "target.md"
    other.write_text("# Stable heading\n", encoding="utf-8")
    config = tmp_path / "mkdocs.yml"
    config.write_text(
        yaml.safe_dump(
            {
                "site_name": "Anchor contract",
                "docs_dir": "docs",
                "site_dir": "site",
                "plugins": [],
                "validation": project_config.get("validation", {}),
            }
        ),
        encoding="utf-8",
    )

    def build() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "mkdocs", "build", "--strict", "--config-file", str(config)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=30,
        )

    valid = build()
    assert valid.returncode == 0, valid.stdout + valid.stderr
    changed = index if target.startswith("#") else other
    changed.write_text(
        changed.read_text(encoding="utf-8").replace("Stable heading", "Renamed heading"),
        encoding="utf-8",
    )
    broken = build()
    assert broken.returncode != 0, broken.stdout + broken.stderr
    assert "#stable-heading" in broken.stderr
    assert "anchor" in broken.stderr
    assert "WARNING" in broken.stderr


def _documentation_python_examples(document: str) -> list[tuple[str, str]]:
    """Read fenced Python examples without interpreting code comments as headings."""
    examples: list[tuple[str, str]] = []
    heading = ""
    fence = ""
    language = ""
    code: list[str] = []
    for line in document.splitlines():
        if fence:
            if line.strip() == fence:
                if language == "python":
                    examples.append((heading, "\n".join(code) + "\n"))
                fence = ""
            else:
                code.append(line)
        elif match := re.fullmatch(r"(`{3,}|~{3,})([\w-]*)\s*", line):
            fence, language = match.groups()
            code = []
        elif re.match(r"^#{1,6} ", line):
            heading = line
    assert not fence, "unclosed documentation code fence"
    assert examples, "documentation contains no Python examples"
    return examples


@pytest.mark.parametrize(
    "heading",
    [
        "Close an owned source after an early result",
        "Preserve the callback error while closing the source",
        "Cancel a query and await its cleanup",
    ],
)
def test_resource_ownership_documentation_examples(heading: str, tmp_path: Path) -> None:
    """Run the published ownership examples, including cancellation, without extras."""
    document = (ROOT / "fpstreams/docs/user-guide/concepts.md").read_text(encoding="utf-8")
    examples = [
        code
        for title, code in _documentation_python_examples(document)
        if title == f"### {heading}"
    ]
    assert len(examples) == 1, f"expected one runnable example: {heading}"
    script = (
        "import sys\n"
        + _isolated_base_dependency_path(tmp_path)
        + f"sys.path.insert(0, {str(ROOT / 'src')!r})\n"
        + examples[0]
    )
    result = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, f"{heading}:\n{result.stdout}\n{result.stderr}"


@pytest.mark.parametrize(
    ("heading", "example"),
    [
        pytest.param(heading, code, id=f"{heading.lstrip('# ')}-{index}")
        for index, (heading, code) in enumerate(
            _documentation_python_examples((ROOT / "README.md").read_text(encoding="utf-8"))
        )
    ],
)
def test_readme_python_examples(heading: str, example: str, tmp_path: Path) -> None:
    """Run every README Python block, with explicit inputs for its file adapter."""
    parquet = "flow.from_parquet(" in example
    prelude = ""
    postlude = ""
    if parquet:
        pytest.importorskip("pyarrow")
        prelude = textwrap.dedent(
            """\
            import pyarrow as pa
            import pyarrow.parquet as pq
            pq.write_table(pa.Table.from_pylist([
                {"id": 1, "status": "active", "balance": 10},
                {"id": 2, "status": "closed", "balance": 20},
                {"id": 3, "status": "active", "balance": 30},
            ]), "accounts.parquet")
            """
        )
        postlude = textwrap.dedent(
            """\
            assert pq.read_table("active-accounts.parquet").to_pylist() == [
                {"id": 1, "balance": 10}, {"id": 3, "balance": 30}
            ]
            """
        )
    script = (
        "import sys\n"
        + ("" if parquet else _isolated_base_dependency_path(tmp_path))
        + f"sys.path.insert(0, {str(ROOT / 'src')!r})\n"
        + prelude
        + example
        + postlude
    )
    result = subprocess.run(
        [sys.executable, "-I", *([] if parquet else ["-S"]), "-c", script],
        cwd=tmp_path,
        env={**os.environ, "TMPDIR": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"{heading}:\n{result.stdout}\n{result.stderr}"
    if heading == "### Close an owned iterator":
        assert result.stdout == "source closed\n[0, 2, 4]\n"
    elif heading == "### Asynchronous pipelines":
        assert result.stdout == "[20, 30, 40]\n"


def _build_browser_wheel(output_dir: Path) -> Path:
    result = subprocess.run(
        [sys.executable, str(BROWSER_WHEEL_BUILDER), "--output-dir", str(output_dir)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    wheel = output_dir / f"fpstreams-{project['version']}-py3-none-any.whl"
    manifest = json.loads((output_dir / "browser-wheel.json").read_text(encoding="utf-8"))
    assert manifest["version"] == project["version"]
    assert manifest["wheel"] == wheel.name
    assert manifest["build"] in {"development", "release"}
    assert re.fullmatch(r"[0-9a-f]{7,64}|unknown", manifest["commit"], re.IGNORECASE)
    assert manifest["engine"] == "python"
    assert manifest["ref"]
    return wheel


def test_browser_wheel_manifest_labels_a_matching_release_tag(tmp_path: Path, monkeypatch) -> None:
    """A clean matching tag must be distinguishable from same-version development sources."""
    spec = importlib.util.spec_from_file_location("browser_builder_release", BROWSER_WHEEL_BUILDER)
    assert spec is not None and spec.loader is not None
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    head = "0123456789abcdef0123456789abcdef01234567"
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    tag = f"v{project['version']}"
    monkeypatch.setenv("GITHUB_REF_NAME", tag)
    monkeypatch.setenv("GITHUB_REF_TYPE", "tag")
    monkeypatch.setenv("GITHUB_SHA", head)
    monkeypatch.setattr(
        builder, "_git_value", lambda *arguments: head if arguments[0] == "rev-parse" else ""
    )
    builder.build_browser_wheel(tmp_path)
    manifest = json.loads((tmp_path / "browser-wheel.json").read_text(encoding="utf-8"))
    assert manifest["build"] == "release"
    assert manifest["dirty"] is False
    assert manifest["commit"] == head
    assert manifest["ref"] == tag
    monkeypatch.setenv("GITHUB_REF_NAME", "v0.0.0")
    builder.build_browser_wheel(tmp_path)
    mismatched = json.loads((tmp_path / "browser-wheel.json").read_text(encoding="utf-8"))
    assert mismatched["build"] == "development"


def test_browser_wheel_has_standard_pure_python_contents(tmp_path: Path) -> None:
    """The browser artifact is a valid, self-describing wheel with no native residue."""
    wheel = _build_browser_wheel(tmp_path)

    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
        dist_info = f"fpstreams-{project['version']}.dist-info"
        metadata_path = f"{dist_info}/METADATA"
        wheel_metadata_path = f"{dist_info}/WHEEL"
        record_path = f"{dist_info}/RECORD"

        assert "fpstreams/__init__.py" in names
        assert "fpstreams/py.typed" in names
        assert {metadata_path, wheel_metadata_path, record_path} <= names
        assert all(not name.startswith(("/", "../")) and "/../" not in name for name in names)
        assert all("__pycache__" not in name.split("/") for name in names)
        assert all(not name.endswith((".so", ".pyd", ".dylib", ".pyc")) for name in names)

        metadata = Parser().parsestr(archive.read(metadata_path).decode("utf-8"))
        assert metadata["Name"] == "fpstreams"
        assert (
            metadata["Version"]
            == tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"][
                "version"
            ]
        )
        wheel_metadata = Parser().parsestr(archive.read(wheel_metadata_path).decode("utf-8"))
        assert wheel_metadata["Root-Is-Purelib"] == "true"
        assert wheel_metadata.get_all("Tag") == ["py3-none-any"]

        record_rows = {
            row.rsplit(",", maxsplit=2)[0]: row.rsplit(",", maxsplit=2)[1:]
            for row in archive.read(record_path).decode("utf-8").splitlines()
        }
        assert set(record_rows) == names
        for name in names - {record_path}:
            payload = archive.read(name)
            encoded_hash = urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=").decode()
            assert record_rows[name] == [f"sha256={encoded_hash}", str(len(payload))]
        assert record_rows[record_path] == ["", ""]


def test_browser_wheel_runs_core_apis_without_native_or_site_packages(tmp_path: Path) -> None:
    """The published wheel executes real Flow and Rows pipelines in an isolated interpreter."""
    wheel = _build_browser_wheel(tmp_path)
    script = """
import sys
sys.path.insert(0, sys.argv[1])

import fpstreams

values = fpstreams.flow(range(8)).map(lambda value: value * 2)
assert values.filter(lambda value: value > 5).sum() == 50
assert fpstreams.flow(['a', 'b', 'a']).frequencies() == {'a': 2, 'b': 1}
assert fpstreams.flow(list(range(600))).run_with_report('frequencies').value == {
    key: 1 for key in range(600)
}
left = fpstreams.rows([{'id': 2, 'name': 'b'}, {'id': 1, 'name': 'a'}])
right = fpstreams.rows([{'id': 1, 'score': 10}, {'id': 2, 'score': 20}])
assert left.sort_by('id').join(right, on='id').to_list() == [
    {'id': 1, 'name': 'a', 'score': 10},
    {'id': 2, 'name': 'b', 'score': 20},
]
"""
    script = "import sys\n" + _isolated_base_dependency_path(tmp_path) + script
    result = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script, str(wheel)],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_core_python_execution_works_without_the_native_extension() -> None:
    """Rust acceleration is optional rather than an import-time requirement."""
    script = _blocked_import_script(
        "fpstreams._native",
        """
values = fpstreams.flow(range(8)).map(lambda value: value * 2)
assert values.filter(lambda value: value > 5).sum() == 50
left = fpstreams.rows([{'id': 2, 'name': 'b'}, {'id': 1, 'name': 'a'}])
right = fpstreams.rows([{'id': 1, 'score': 10}, {'id': 2, 'score': 20}])
assert left.sort_by('id').join(right, on='id').to_list() == [
    {'id': 1, 'name': 'a', 'score': 10},
    {'id': 2, 'name': 'b', 'score': 20},
]
raise ImportError('blocked optional dependency: fpstreams._native')
""",
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "blocked optional dependency: fpstreams._native" in result.stdout


# --- Resource high-water accounting ---


@pytest.mark.asyncio
async def test_concurrent_map_task_high_water_is_independent_of_input_length() -> None:
    async def identity(value: int) -> int:
        await asyncio.sleep(0)
        return value

    physical = compile_async_query(
        cast(Any, aflow(range(1_000_000)))
        .map_async(identity, concurrency=4, ordered=False)
        ._query("iterate")
    )
    runtime = QueryRuntime(QueryLimits(max_tasks=16))
    count = 0
    async for _item in execute_async_physical(physical, runtime):
        count += 1

    assert count == 1_000_000
    # Four mappers and one bounded source-reader task, regardless of input length.
    assert runtime.metrics.high_water_tasks <= 5
    assert runtime.metrics.live_tasks == 0


@pytest.mark.asyncio
async def test_merge_task_high_water_tracks_only_live_sources() -> None:
    async def source(offset: int) -> AsyncIterator[int]:
        for value in range(100):
            yield offset + value
            await asyncio.sleep(0)

    physical = compile_async_query(aflow(source(0)).merge(source(1_000))._query("iterate"))
    runtime = QueryRuntime(QueryLimits(max_tasks=16))
    result = [item async for item in execute_async_physical(physical, runtime)]

    assert sorted(result) == [*range(100), *range(1_000, 1_100)]
    assert runtime.metrics.high_water_tasks <= 2
    assert runtime.metrics.live_tasks == 0


@pytest.mark.asyncio
async def test_timer_node_owns_at_most_one_timer_task() -> None:
    physical = compile_async_query(aflow(range(3)).delay(0.001)._query("iterate"))
    runtime = QueryRuntime(QueryLimits(max_tasks=4))

    assert [item async for item in execute_async_physical(physical, runtime)] == [0, 1, 2]
    assert runtime.metrics.high_water_tasks <= 1
    assert runtime.metrics.live_tasks == 0


@pytest.mark.parametrize(
    "case",
    ["valid", "python-outside", "native-outside", "modified", "unhashed", "no-record", "editable"],
)
def test_release_smoke_checks_imported_record_files(case: str, tmp_path: Path, monkeypatch) -> None:
    """Same-version imports need recorded paths and matching bytes in artifact checks."""
    import base64
    import csv
    from importlib.metadata import PathDistribution

    from fpstreams import _native

    spec = importlib.util.spec_from_file_location(
        "release_record_check", ROOT / "scripts/smoke_release.py"
    )
    assert spec is not None and spec.loader is not None
    smoke = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(smoke)
    package = tmp_path / "fpstreams"
    package.mkdir()
    paths = {"python": package / "__init__.py", "native": package / "_native.so"}
    for name, path in paths.items():
        path.write_bytes(f"test {name} contents".encode())
    info = tmp_path / f"fpstreams-{fpstreams.__version__}.dist-info"
    info.mkdir()
    (info / "METADATA").write_text(
        f"Name: fpstreams\nVersion: {fpstreams.__version__}\n", encoding="utf-8"
    )
    record = info / "RECORD"
    with record.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        for name, path in paths.items():
            encoded = (
                base64.urlsafe_b64encode(hashlib.sha256(path.read_bytes()).digest())
                .rstrip(b"=")
                .decode()
            )
            digest = "" if case == "unhashed" and name == "python" else f"sha256={encoded}"
            writer.writerow([path.relative_to(tmp_path).as_posix(), digest, path.stat().st_size])
    monkeypatch.setattr(smoke, "distribution", lambda name: PathDistribution(info))
    monkeypatch.setattr(fpstreams, "__file__", str(paths["python"]))
    monkeypatch.setattr(_native, "__file__", str(paths["native"]))
    expected_error = None
    if case.endswith("-outside"):
        name = case.removesuffix("-outside")
        monkeypatch.setattr(
            fpstreams if name == "python" else _native, "__file__", str(tmp_path / "outside.py")
        )
        expected_error = "outside its RECORD"
    elif case == "modified":
        paths["python"].write_bytes(b"same-version replacement")
        expected_error = "RECORD hash mismatch"
    elif case == "unhashed":
        expected_error = "no RECORD hash"
    elif case == "no-record":
        record.unlink()
        expected_error = "no file RECORD"
    elif case == "editable":
        (info / "direct_url.json").write_text(
            json.dumps({"dir_info": {"editable": True}}), encoding="utf-8"
        )
        expected_error = "installed release is editable"
    if expected_error:
        with pytest.raises(RuntimeError, match=expected_error):
            smoke._installed_files()
    else:
        assert smoke._installed_files() == {
            "python": "fpstreams/__init__.py",
            "native": "fpstreams/_native.so",
        }


@pytest.mark.parametrize("stale", ["distribution", "python", "native"])
def test_release_smoke_rejects_mixed_installed_versions(stale: str, monkeypatch) -> None:
    """Compatible operations cannot hide a stale installed version marker."""
    from fpstreams import _native

    spec = importlib.util.spec_from_file_location(
        "release_smoke_identity", ROOT / "scripts/smoke_release.py"
    )
    assert spec is not None and spec.loader is not None
    smoke = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(smoke)
    current = fpstreams.__version__
    monkeypatch.setattr(smoke, "distribution_version", lambda name: current)
    monkeypatch.setattr(_native, "version", lambda: current)
    assert smoke._installed_versions() == dict.fromkeys(
        ["distribution", "python", "native"], current
    )
    if stale == "distribution":
        monkeypatch.setattr(smoke, "distribution_version", lambda name: "0.0.0")
    elif stale == "python":
        monkeypatch.setattr(fpstreams, "__version__", "0.0.0")
    else:
        monkeypatch.setattr(_native, "version", lambda: "0.0.0")
    monkeypatch.setattr(sys, "argv", ["smoke_release.py"])
    with pytest.raises(RuntimeError, match=f"installed release version mismatch:.*{stale}=0.0.0"):
        smoke.main()


def test_release_minimal_install_contract(tmp_path: Path) -> None:
    """The smoke CLI covers the first examples and reports unsuccessful invocations."""
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "smoke_release.py")],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["versions"] == dict.fromkeys(
        ["distribution", "python", "native"], report["version"]
    )
    assert report["orders"] == [
        {"region": "eu", "orders": 2, "revenue": 48},
        {"region": "us", "orders": 1, "revenue": 20},
    ]
    assert report["async"] == [20, 30, 40]
    failed = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "smoke_release.py"), "--invalid-option"],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert failed.returncode != 0
    assert "unrecognized arguments" in failed.stderr
    with pytest.raises(json.JSONDecodeError):
        json.loads(failed.stdout)


def test_browser_manifest_build_identity(tmp_path: Path, monkeypatch) -> None:
    """Dirty or unidentifiable sources cannot masquerade as a matching release tag."""
    spec = importlib.util.spec_from_file_location("browser_builder_identity", BROWSER_WHEEL_BUILDER)
    assert spec is not None and spec.loader is not None
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    for name in ("GITHUB_SHA", "GITHUB_REF", "GITHUB_REF_NAME", "GITHUB_REF_TYPE"):
        monkeypatch.delenv(name, raising=False)
    head = "0123456789abcdef0123456789abcdef01234567"
    dirty = ""
    tag_head = head

    def git_value(*arguments):
        if arguments[0] == "rev-parse":
            return tag_head if "--verify" in arguments else head
        if arguments[0] == "status":
            return dirty
        return None

    monkeypatch.setattr(builder, "_git_value", git_value)
    builder.build_browser_wheel(tmp_path)
    manifest = json.loads((tmp_path / "browser-wheel.json").read_text())
    assert manifest["dirty"] is False
    assert manifest["commit"] == head
    assert manifest["build"] == "development"
    monkeypatch.setenv("GITHUB_SHA", head)
    monkeypatch.setenv("GITHUB_REF_NAME", "v2.1.0")
    monkeypatch.setenv("GITHUB_REF_TYPE", "tag")
    assert builder._build_provenance("2.1.0")["build"] == "release"
    tag_head = "f" * 40
    assert builder._build_provenance("2.1.0")["build"] == "development"
    tag_head = head
    dirty = " M src/fpstreams/streams/flow.py"
    assert builder._build_provenance("2.1.0")["dirty"] is True
    assert builder._build_provenance("2.1.0")["build"] == "development"
    dirty = ""
    monkeypatch.setenv("GITHUB_SHA", "f" * 40)
    assert builder._build_provenance("2.1.0")["commit"] == head
    assert builder._build_provenance("2.1.0")["build"] == "development"
    monkeypatch.setattr(builder, "_git_value", lambda *arguments: None)
    unknown = builder._build_provenance("2.1.0")
    assert unknown["dirty"] is None
    assert unknown["commit"] == "unknown"
    assert unknown["build"] == "development"


def test_sorted_documented_examples() -> None:
    from fpstreams import agg, flow

    orders = [
        {"region": "eu", "amount": 12},
        {"region": "eu", "amount": 36},
        {"region": "us", "amount": 20},
    ]
    assert flow(orders).rows().group_by_sorted("region").aggregate(
        orders=agg.count(), revenue=agg.sum("amount")
    ).to_list() == [
        {"region": "eu", "orders": 2, "revenue": 48},
        {"region": "us", "orders": 1, "revenue": 20},
    ]
    left = [{"time": 1, "event": "start"}, {"time": 3, "event": "end"}]
    right = [{"time": 2, "event": "update"}]
    assert flow(left).merge_sorted(right, key="time").to_list() == [left[0], right[0], left[1]]
    orders = [{"customer": 1, "amount": 12}, {"customer": 1, "amount": 36}]
    assert flow(orders).rows().join_sorted(
        [{"customer": 1, "name": "Ana"}], on="customer", validate="m:1"
    ).to_list() == [
        {"customer": 1, "amount": 12, "name": "Ana"},
        {"customer": 1, "amount": 36, "name": "Ana"},
    ]


def test_atomic_sink_documented_example(tmp_path) -> None:
    from fpstreams import flow

    for method, filename in (("to_json", "orders.json"), ("to_csv", "orders.csv")):
        path = tmp_path / filename
        getattr(flow([{"id": 1}]), method)(path, atomic=True, if_exists="error")
        original = path.read_bytes()
        with pytest.raises(FileExistsError):
            getattr(flow([{"id": 2}]), method)(path, atomic=True, if_exists="error")
        assert path.read_bytes() == original
