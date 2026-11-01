"""Shared tabular test inputs and helpers."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path


def _run_inline_python(
    script: str,
    *arguments: str,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run an isolated Python snippet with consistent text output capture."""
    return subprocess.run(
        [sys.executable, "-c", script, *arguments],
        cwd=cwd,
        env=env,
        check=check,
        capture_output=True,
        text=True,
    )
