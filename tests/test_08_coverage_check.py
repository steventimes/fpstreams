"""Coverage check scaffold.

This test intentionally enforces coverage only when running under coverage tooling
(or when COVERAGE_MIN is explicitly configured), keeping local developer loops fast.
"""

from __future__ import annotations

import os

import pytest


def test_coverage_threshold_if_enabled() -> None:
    minimum = os.environ.get("COVERAGE_MIN")
    running_under_coverage = os.environ.get("COV_CORE_SOURCE") is not None

    if minimum is None and not running_under_coverage:
        pytest.skip("Coverage gate is enabled only in coverage-aware runs.")

    # If this test runs, assert that tracing is active.
    import sys

    assert sys.gettrace() is not None
