"""Write a deterministic SHA-256 manifest for release artifacts."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(directory: Path, output: Path) -> None:
    artifacts = sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and path.resolve() != output.resolve()
    )
    lines = [f"{file_sha256(path)}  {path.name}\n" for path in artifacts]
    output.write_text("".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    write_manifest(arguments.directory, arguments.output)


if __name__ == "__main__":
    main()
