"""Compare an installed fpstreams pipeline under the native and Python engines."""

from __future__ import annotations

import json

import fpstreams


def main() -> None:
    pipeline = fpstreams.flow(range(4)).map(fpstreams.item * 2 + 1)
    native = pipeline.with_engine("native").to_list()
    python = pipeline.with_engine("python").to_list()
    expected = [1, 3, 5, 7]
    if native != expected or python != expected:
        raise RuntimeError(
            f"release smoke produced native={native!r}, python={python!r}, expected={expected!r}"
        )
    print(
        json.dumps(
            {
                "native": native,
                "python": python,
                "version": fpstreams.__version__,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
