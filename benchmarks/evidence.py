"""Collect benchmark provenance outside the measured task and validate report evidence."""

from __future__ import annotations

import gc
import hashlib
import importlib
import json
import os
import platform
import subprocess
import tracemalloc
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from functools import partial
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import MethodType
from typing import Any

REPORT_SCHEMA_VERSION = 6
LIBRARIES = ("numpy", "pandas", "pyarrow", "polars", "aiofiles")
RUNTIME_ENVIRONMENT = (
    "NPY_DISABLE_CPU_FEATURES",
    "NPY_ENABLE_CPU_FEATURES",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "POLARS_MAX_THREADS",
    "PYTHONHASHSEED",
    "PYTHONMALLOC",
    "PYTHONMALLOCSTATS",
    "GLIBC_TUNABLES",
    "MALLOC_ARENA_MAX",
    "MALLOC_ARENA_TEST",
    "MALLOC_CHECK_",
    "MALLOC_MMAP_MAX_",
    "MALLOC_MMAP_THRESHOLD_",
    "MALLOC_PERTURB_",
    "MALLOC_TOP_PAD_",
    "MALLOC_TRIM_THRESHOLD_",
)
ROOT = Path(__file__).resolve().parents[1]
MATRIX_FILES = ("benchmark.py", "benchmarks/competitive.py", "benchmarks/evidence.py")
COMPARABLE_FIELDS = (
    "python_version",
    "platform",
    "machine",
    "implementation",
    "processor",
    "runtime_configuration",
    "suite",
    "size",
    "domain",
    "quick",
    "benchmark_matrix_sha256",
    "methodology",
)


def measure_python_allocation(task: Callable[[], object]) -> dict[str, int]:
    """Measure one separate task call, excluding prepared inputs and native heaps."""
    if tracemalloc.is_tracing():
        raise RuntimeError("benchmark allocation measurement requires inactive tracemalloc")
    gc.collect()
    tracemalloc.start()
    try:
        task()
        if not tracemalloc.is_tracing():
            raise RuntimeError("benchmark task stopped allocation tracing")
        return {"peak_allocation_bytes": tracemalloc.get_traced_memory()[1]}
    finally:
        tracemalloc.stop()


def file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def python_package_sha256(root: Path) -> str:
    """Hash source and typing files, excluding caches and out-of-scope directories."""
    paths = []
    for directory, names, files in os.walk(root):
        names[:] = [name for name in names if not name.lower().startswith("cosi")]
        paths.extend(
            Path(directory) / name
            for name in files
            if Path(name).suffix in {".py", ".pyi"} or name == "py.typed"
        )
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def matrix_sha256() -> str:
    digest = hashlib.sha256()
    for name in MATRIX_FILES:
        digest.update(name.encode() + b"\0")
        digest.update((ROOT / name).read_bytes() + b"\0")
    return digest.hexdigest()


def git_provenance(package_root: Path) -> dict[str, Any]:
    """Report the repository containing the imported package, or explicit unknown values."""
    try:
        revision = subprocess.run(
            ["git", "-C", str(package_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "-C", str(package_root), "status", "--porcelain", "--untracked-files=normal"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return {"git_sha": None, "git_dirty": None, "git_available": False}
    return {"git_sha": revision, "git_dirty": bool(status), "git_available": True}


def library_versions() -> dict[str, str | None]:
    import fpstreams

    libraries: dict[str, str | None] = {"fpstreams": fpstreams.__version__}
    for name in LIBRARIES:
        try:
            libraries[name] = version(name)
        except PackageNotFoundError:
            libraries[name] = None
    return libraries


def runtime_configuration() -> dict[str, Any]:
    """Capture only execution-related settings; never dump the process environment."""
    try:
        affinity = sorted(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        affinity = None
    try:
        numpy = importlib.import_module("numpy")
    except ModuleNotFoundError as error:
        if error.name != "numpy":
            raise
        numpy_runtime = None
    else:
        core = getattr(numpy, "_core", None)
        if core is None:  # NumPy 1.x exposes the same metadata through numpy.core.
            core = importlib.import_module("numpy.core")
        module = core._multiarray_umath
        introspect = getattr(numpy.lib, "introspect", None)
        targets = None if introspect is None else introspect.opt_func_info()
        numpy_runtime = {
            "cpu_baseline": list(module.__cpu_baseline__),
            "cpu_dispatch": list(module.__cpu_dispatch__),
            "cpu_features": dict(module.__cpu_features__),
            "active_targets_sha256": (
                None
                if targets is None
                else hashlib.sha256(json.dumps(targets, sort_keys=True).encode()).hexdigest()
            ),
        }
    return {
        "cpu_affinity": affinity,
        "environment": {name: os.environ.get(name) for name in RUNTIME_ENVIRONMENT},
        "numpy": numpy_runtime,
    }


def observe_task(
    task: Callable[[], object], requested_engine: str, terminal: str
) -> dict[str, Any]:
    """Run the original task once outside timing; never inspect or replay its internal source."""
    from fpstreams import Flow, Pairs, Rows
    from fpstreams.runtime.report import _start_recording, _stop_recording

    endpoint = task.func if isinstance(task, partial) else task
    if isinstance(endpoint, MethodType) and isinstance(endpoint.__self__, (Flow, Rows, Pairs)):
        terminal = endpoint.__name__
    recorder, token = _start_recording(terminal, requested_engine)
    try:
        task()
        try:
            report = recorder.finish(None, 0).report
        except RuntimeError:
            # Only recorder completion is caught. Errors from the task propagate unchanged.
            return {"status": "unknown", "reason": "task did not record an execution plan"}
        return {
            "status": "observed",
            "scope": "outer_plan",
            "requested_engine": report.requested_engine,
            "compiler_engine": report.compiler_engine,
            "strategy": report.strategy,
            "reason": report.reason,
            "peak_owned_async_tasks": report.peak_owned_async_tasks,
            "peak_spill_files": report.peak_spill_files,
            "spill_bytes_written": report.spill_bytes_written,
        }
    finally:
        _stop_recording(token)


class BenchmarkEvidence:
    """Hold small fingerprints while timing, then reject changes before emitting a report."""

    def __init__(self, suite: str, native: Mapping[str, object]) -> None:
        import fpstreams

        self.package_root = Path(fpstreams.__file__).resolve().parent
        self.metadata = {
            "suite": suite,
            "fpstreams_version": fpstreams.__version__,
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "native": dict(native),
            "libraries": library_versions(),
            "runtime_configuration": runtime_configuration(),
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "benchmark_matrix_sha256": matrix_sha256(),
            "python_package_sha256": python_package_sha256(self.package_root),
            **git_provenance(self.package_root),
        }

    def finish(self) -> dict[str, Any]:
        if matrix_sha256() != self.metadata["benchmark_matrix_sha256"]:
            raise RuntimeError("benchmark matrix changed while measurements were running")
        if python_package_sha256(self.package_root) != self.metadata["python_package_sha256"]:
            raise RuntimeError("fpstreams Python sources changed while measurements were running")
        native = self.metadata["native"]
        if native.get("available") and file_sha256(Path(native["path"])) != native["sha256"]:
            raise RuntimeError("fpstreams native extension changed while measurements were running")
        if library_versions() != self.metadata["libraries"]:
            raise RuntimeError("benchmark dependencies changed while measurements were running")
        if runtime_configuration() != self.metadata["runtime_configuration"]:
            raise RuntimeError(
                "benchmark runtime configuration changed while measurements were running"
            )
        if git_provenance(self.package_root) != {
            key: self.metadata[key] for key in ("git_sha", "git_dirty", "git_available")
        }:
            raise RuntimeError("benchmark Git state changed while measurements were running")
        return {**self.metadata, "provenance_verified_unchanged": True}


def metadata_errors(metadata: dict[str, Any]) -> list[str]:
    """Require explicit evidence, including explicit absence of optional integrations or Git."""
    required = (
        *COMPARABLE_FIELDS,
        "fpstreams_version",
        "native",
        "libraries",
        "generated_at_utc",
        "repeats",
        "python_package_sha256",
        "git_sha",
        "git_dirty",
        "git_available",
        "provenance_verified_unchanged",
    )
    for field in required:
        if field not in metadata:
            return [f"missing benchmark metadata: {field}; recreate the baseline"]
    workload_errors = _workload_errors(metadata)
    if workload_errors:
        return workload_errors
    if metadata["provenance_verified_unchanged"] is not True:
        return ["unverified benchmark provenance"]
    for field in ("benchmark_matrix_sha256", "python_package_sha256"):
        if not _is_digest(metadata[field], 64):
            return [f"invalid benchmark metadata: {field}"]
    if metadata["git_available"] is True:
        if not _is_digest(metadata["git_sha"], 40) or type(metadata["git_dirty"]) is not bool:
            return ["invalid benchmark Git provenance"]
    elif metadata["git_available"] is not False or any(
        metadata[field] is not None for field in ("git_sha", "git_dirty")
    ):
        return ["invalid benchmark Git provenance"]
    libraries = metadata["libraries"]
    if not isinstance(libraries, dict) or set(libraries) != {*LIBRARIES, "fpstreams"}:
        return ["missing benchmark metadata: libraries; recreate the baseline"]
    if any(
        value is not None and (not isinstance(value, str) or not value)
        for value in libraries.values()
    ):
        return ["invalid benchmark libraries metadata"]
    return _runtime_errors(metadata["runtime_configuration"], libraries) or _native_errors(
        metadata["native"]
    )


def _runtime_errors(configuration: object, libraries: dict[str, Any]) -> list[str]:
    error = ["invalid benchmark runtime configuration"]
    if not isinstance(configuration, dict) or set(configuration) != {
        "cpu_affinity",
        "environment",
        "numpy",
    }:
        return error
    affinity = configuration["cpu_affinity"]
    if affinity is not None and (
        not isinstance(affinity, list)
        or not affinity
        or any(type(cpu) is not int or cpu < 0 for cpu in affinity)
        or affinity != sorted(set(affinity))
    ):
        return error
    environment = configuration["environment"]
    if (
        not isinstance(environment, dict)
        or set(environment) != set(RUNTIME_ENVIRONMENT)
        or any(value is not None and not isinstance(value, str) for value in environment.values())
    ):
        return error
    numpy = configuration["numpy"]
    if libraries["numpy"] is None:
        return [] if numpy is None else error
    if not isinstance(numpy, dict) or set(numpy) != {
        "cpu_baseline",
        "cpu_dispatch",
        "cpu_features",
        "active_targets_sha256",
    }:
        return error
    for key in ("cpu_baseline", "cpu_dispatch"):
        if not isinstance(numpy[key], list) or any(
            not isinstance(value, str) or not value for value in numpy[key]
        ):
            return error
    features = numpy["cpu_features"]
    if (
        not isinstance(features, dict)
        or not features
        or any(
            not isinstance(key, str) or not key or type(value) is not bool
            for key, value in features.items()
        )
    ):
        return error
    targets = numpy["active_targets_sha256"]
    return [] if targets is None or _is_digest(targets, 64) else error


def _workload_errors(metadata: dict[str, Any]) -> list[str]:
    for field in ("size", "repeats"):
        if type(metadata[field]) is not int or metadata[field] <= 0:
            return [f"invalid benchmark metadata: {field}"]
    if type(metadata["quick"]) is not bool or not isinstance(metadata["methodology"], dict):
        return ["invalid benchmark workload settings"]
    suite = metadata["suite"]
    domains = ("int", "float", "both") if suite == "engine" else ("mixed",)
    if suite not in ("engine", "competitive") or metadata["domain"] not in domains:
        return ["invalid benchmark suite or domain"]
    if suite == "competitive":
        methodology = metadata["methodology"]
        minimum = methodology.get("sample_warmup_min_seconds")
        if (
            type(minimum) not in (int, float)
            or not 0 <= minimum < float("inf")
            or type(methodology.get("gc_before_sample_warmup")) is not bool
        ):
            return ["invalid competitive warmup methodology"]
    for field in ("fpstreams_version", "python_version", "platform", "machine", "implementation"):
        if not isinstance(metadata[field], str) or not metadata[field]:
            return [f"invalid benchmark metadata: {field}"]
    try:
        timestamp = datetime.fromisoformat(metadata["generated_at_utc"])
        if timestamp.tzinfo is None:
            return ["invalid benchmark timestamp"]
    except (TypeError, ValueError):
        return ["invalid benchmark timestamp"]
    return []


def _is_digest(value: object, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def _native_errors(native: object) -> list[str]:
    if (
        not isinstance(native, dict)
        or not {"available", "profile", "path", "sha256"} <= native.keys()
    ):
        return ["missing benchmark metadata: native; recreate the baseline"]
    if native["available"] is True:
        if (
            native["profile"] not in ("release", "debug", "unknown")
            or not isinstance(native["path"], str)
            or not native["path"]
            or not _is_digest(native["sha256"], 64)
        ):
            return ["invalid benchmark native metadata"]
    elif native["available"] is not False or native != {
        "available": False,
        "profile": "unavailable",
        "path": None,
        "sha256": None,
    }:
        return ["invalid benchmark native metadata"]
    return []
