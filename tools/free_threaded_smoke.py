"""Run threaded Flow and AsyncFlow smoke checks only on a GIL-disabled interpreter."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import sys
from concurrent.futures import ThreadPoolExecutor


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--json", dest="output", type=str)
    args = parser.parse_args()
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    if gil:
        print("expected a free-threaded interpreter", file=sys.stderr)
        return 2
    import fpstreams

    values = list(range(100))
    expected = sum(x * 2 for x in values if x % 3 == 0)

    def work(_: int) -> int:
        result = 0
        for _ in range(args.iterations):
            result += (
                fpstreams.Flow.of(*values).map(lambda x: x * 2).filter(lambda x: x % 3 == 0).sum()
            )
        return result

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        results = list(executor.map(work, range(args.threads)))
    if any(result != expected * args.iterations for result in results):
        return 1
    asyncio.run(fpstreams.AsyncFlow.of(1, 2, 3).count())
    report = {
        "threads": args.threads,
        "iterations": args.iterations,
        "python": sys.version,
        "platform": platform.platform(),
        "cache_tag": sys.implementation.cache_tag,
        "fpstreams": fpstreams.__version__,
    }
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(report, handle, sort_keys=True, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
