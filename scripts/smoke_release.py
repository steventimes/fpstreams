"""Smoke-test an installed package under the native and Python engines."""

from __future__ import annotations

import argparse
import asyncio
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


async def _async_example() -> list[int]:
    """Exercise the core asynchronous API without optional file adapters."""

    async def fetch(value: int) -> int:
        await asyncio.sleep(0)
        return value * 10

    return await (
        fpstreams.aflow([1, 2, 3, 4])
        .map_async(fetch, concurrency=2, ordered=True)
        .filter(lambda value: value >= 20)
        .to_list()
    )


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
    orders = (
        fpstreams.flow(
            [
                {"region": "eu", "status": "paid", "amount": 24},
                {"region": "us", "status": "paid", "amount": 20},
                {"region": "eu", "status": "cancelled", "amount": 99},
                {"region": "eu", "status": "paid", "amount": 24},
            ]
        )
        .filter(fpstreams.col("status") == "paid")
        .group_by("region")
        .aggregate(orders=fpstreams.agg.count(), revenue=fpstreams.agg.sum("amount"))
        .sort_by("region")
        .to_list()
    )
    expected_orders = [
        {"region": "eu", "orders": 2, "revenue": 48},
        {"region": "us", "orders": 1, "revenue": 20},
    ]
    asynchronous = asyncio.run(_async_example())
    if orders != expected_orders or asynchronous != [20, 30, 40]:
        raise RuntimeError(f"release examples produced orders={orders!r}, async={asynchronous!r}")
    result: dict[str, Any] = {
        "native": native,
        "python": python,
        "version": fpstreams.__version__,
        "orders": orders,
        "async": asynchronous,
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
