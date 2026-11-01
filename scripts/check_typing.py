"""Check public consumer types and the expected rejection of invalid calls."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CASES = ROOT / "tests" / "typing"
RULES = {
    "assignment": "reportAssignmentType",
    "attr-defined": "reportAttributeAccessIssue",
    "arg-type": "reportArgumentType",
}


def _expected_errors(path: Path, checker: str) -> set[tuple[int, str]]:
    expected = set()
    for line, source in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if marker := re.search(r"# error: ([\w-]+)\s*$", source):
            rule = marker[1]
            if rule not in RULES:
                raise ValueError(f"unknown expected rule at {path}:{line}: {rule}")
            expected.add((line, RULES[rule] if checker == "pyright" else rule))
    if not expected:
        raise ValueError(f"no expected errors marked in {path}")
    return expected


def _diagnostics(output: str, checker: str) -> set[tuple[Path, int, str]]:
    if checker == "pyright":
        report = json.loads(output)
        return {
            (
                (ROOT / item["file"]).resolve(),
                item["range"]["start"]["line"] + 1,
                item.get("rule", ""),
            )
            for item in report["generalDiagnostics"]
            if item["severity"] in {"error", "warning"}
        }
    return {
        ((ROOT / match[1]).resolve(), int(match[2]), match[3])
        for line in output.splitlines()
        if (match := re.match(r"^(.+?):(\d+):(?:\d+:)? error: .* \[([\w-]+)\]$", line))
    }


def _command(checker: str, version: str, cache: str) -> list[str]:
    if checker == "mypy":
        if importlib.util.find_spec("mypy") is None:
            raise ImportError(
                "mypy is missing; install the type dependency group with uv sync --group type"
            )
        return [
            sys.executable,
            "-m",
            "mypy",
            "--config-file=",
            "--cache-dir",
            cache,
            "--no-site-packages",
            "--ignore-missing-imports",
            "--follow-imports=silent",
            "--no-incremental",
            "--show-error-codes",
            "--no-pretty",
            "--no-error-summary",
            "--python-version",
            version,
        ]
    entry = CASES / "node_modules" / "pyright" / "index.js"
    if not entry.is_file():
        raise FileNotFoundError(
            "Pyright is missing; run npm ci --prefix tests/typing --ignore-scripts --omit=optional"
        )
    return [
        "node",
        str(entry),
        "--project",
        str(ROOT / "pyproject.toml"),
        "--pythonpath",
        sys.executable,
        "--pythonversion",
        version,
        "--outputjson",
        "--warnings",
    ]


def check(checker: str, version: str, cache: str) -> bool:
    command = _command(checker, version, cache)
    environment = dict(os.environ, MYPYPATH=str(ROOT / "src"))
    cases = tuple(
        ((CASES / name).resolve(), positive)
        for name, positive in (
            ("valid.py", True),
            ("invalid.py", False),
            ("reduce_examples.py", True),
            ("reduce_invalid.py", False),
        )
    )
    required = {
        (path, line, rule)
        for path, positive in cases
        if not positive
        for line, rule in _expected_errors(path, checker)
    }
    allowed_lines = {item[:2] for item in required}
    result = subprocess.run(
        [*command, *(str(path) for path, _ in cases)],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if result.returncode != 1:
        print(f"{checker} Python {version}: type contract batch failed (exit {result.returncode})")
        print(result.stdout)
        return False

    observed = _diagnostics(result.stdout, checker)
    # Keep the full report: errors in positive or external files must not disappear.
    # Additional Unknown diagnostics are allowed only on marked negative lines.
    unexpected = {item for item in observed if item[:2] not in allowed_lines}
    if not required <= observed or unexpected:
        print(f"Missing expected diagnostics: {sorted(required - observed)}")
        print(f"Unexpected diagnostic locations: {sorted(unexpected)}")
        print(f"{checker} Python {version}: type contract batch failed (exit {result.returncode})")
        print(result.stdout)
        return False
    print(f"{checker} Python {version}: positive assertions and expected rejections passed")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checker", choices=("mypy", "pyright", "both"), default="both")
    parser.add_argument(
        "--python-version", choices=("3.11", "3.12", "3.13", "3.14", "3.15"), default="3.11"
    )
    arguments = parser.parse_args()
    checkers = ("mypy", "pyright") if arguments.checker == "both" else (arguments.checker,)
    try:
        with tempfile.TemporaryDirectory(prefix="fpstreams-typing-") as cache:
            results = [check(checker, arguments.python_version, cache) for checker in checkers]
        return 0 if all(results) else 1
    except (ImportError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(f"Type contract check could not complete: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
