"""Compare an installed fpstreams pipeline under the native and Python engines."""

from __future__ import annotations

import argparse
import importlib.util
import json
from collections.abc import Callable
from typing import Any

import fpstreams


def _expect_missing_extra(module: str, operation: Callable[[], Any], expected_message: str) -> str:
    """Require a genuinely absent optional dependency and its stable user-facing error."""
    if importlib.util.find_spec(module) is not None:
        raise RuntimeError(f"minimal smoke unexpectedly found optional dependency {module!r}")
    try:
        operation()
    except ImportError as error:
        message = str(error)
        if expected_message not in message:
            raise RuntimeError(f"unexpected {module} missing-extra error: {message}") from error
        return message
    raise RuntimeError(f"minimal smoke unexpectedly used absent optional dependency {module!r}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minimal", action="store_true")
    arguments = parser.parse_args()
    pipeline = fpstreams.flow(range(4)).map(fpstreams.item * 2 + 1)
    native = pipeline.with_engine("native").to_list()
    python = pipeline.with_engine("python").to_list()
    expected = [1, 3, 5, 7]
    if native != expected or python != expected:
        raise RuntimeError(
            f"release smoke produced native={native!r}, python={python!r}, expected={expected!r}"
        )
    result: dict[str, Any] = {
        "native": native,
        "python": python,
        "version": fpstreams.__version__,
    }
    if arguments.minimal:
        result["missing_extras"] = {
            "pandas": _expect_missing_extra(
                "pandas",
                lambda: fpstreams.rows([{"id": 1}]).to_pandas(),
                "to_pandas() requires the 'data' extra",
            ),
            "pyarrow": _expect_missing_extra(
                "pyarrow",
                lambda: fpstreams.rows([{"id": 1}]).to_arrow(),
                "Arrow/Parquet support requires the 'arrow' extra",
            ),
        }
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
