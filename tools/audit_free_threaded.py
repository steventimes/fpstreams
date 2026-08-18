"""Check Rust sources for the GIL-disabled marker and two unsafe shared-state patterns."""

from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUST = ROOT / "rust" / "src"


def main() -> int:
    files = tuple(RUST.glob("*.rs"))
    text = "\n".join(path.read_text(encoding="utf-8") for path in files)
    errors: list[str] = []
    if text.count("#[pymodule(gil_used = false)]") != 1:
        errors.append("native module must declare #[pymodule(gil_used = false)] exactly once")
    for pattern, message in (
        (r"static\s+mut\b", "static mut is not free-thread safe"),
        (r"unsafe\s+impl\s+(?:Send|Sync)", "manual Send/Sync impl requires review"),
    ):
        if re.search(pattern, text):
            errors.append(message)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("free-threaded Rust audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
