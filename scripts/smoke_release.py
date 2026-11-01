"""Smoke-test an installed package under the native and Python engines."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import importlib.util
import json
from collections.abc import Callable
from importlib.metadata import distribution
from importlib.metadata import version as distribution_version
from pathlib import Path
from typing import Any

import fpstreams


def _installed_versions() -> dict[str, str]:
    """Require the installed metadata, Python package, and native extension to agree."""
    from fpstreams import _native

    versions = {
        "distribution": distribution_version("fpstreams"),
        "python": fpstreams.__version__,
        "native": _native.version(),
    }
    if len(set(versions.values())) != 1:
        details = ", ".join(f"{name}={value}" for name, value in versions.items())
        raise RuntimeError(f"installed release version mismatch: {details}")
    return versions


def _installed_files() -> dict[str, str]:
    """Check that imported entry points belong to, and match, the installed RECORD."""
    from fpstreams import _native

    installed = distribution("fpstreams")
    direct_url = installed.read_text("direct_url.json")
    if direct_url and json.loads(direct_url).get("dir_info", {}).get("editable", False):
        raise RuntimeError("installed release is editable")
    files = installed.files
    if not files:
        raise RuntimeError("installed release has no file RECORD")
    recorded = {Path(entry.locate()).resolve(): entry for entry in files}
    checked: dict[str, str] = {}
    for name, module in (("python", fpstreams), ("native", _native)):
        origin = getattr(module, "__file__", None)
        if not isinstance(origin, str):
            raise RuntimeError(f"installed release {name} has no file origin")
        path = Path(origin).resolve()
        entry = recorded.get(path)
        if entry is None:
            raise RuntimeError(f"installed release {name} is outside its RECORD: {path}")
        if entry.hash is None:
            raise RuntimeError(f"installed release {name} has no RECORD hash: {entry}")
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, entry.hash.mode).digest()
        encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        if encoded != entry.hash.value:
            raise RuntimeError(f"installed release {name} RECORD hash mismatch: {entry}")
        checked[name] = str(entry)
    return checked


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
    parser.add_argument("--require-installed", action="store_true")
    arguments = parser.parse_args()
    versions = _installed_versions()
    installed = _installed_files() if arguments.require_installed else None
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
        "versions": versions,
        "orders": orders,
        "async": asynchronous,
    }
    if installed is not None:
        result["installed"] = installed
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
