"""Write a portable, filename-sorted SHA-256 manifest for release artifacts."""

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
    """Hash visible files and name them relative to the manifest's directory.

    GitHub uploads ``output.parent`` as one artifact tree, so paths must remain
    verifiable after download. Build-tool metadata such as ``.gitignore`` is not
    a distributable package and is deliberately omitted.
    """
    output_parent = output.parent.resolve()
    artifacts = sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and not path.name.startswith(".") and path.resolve() != output.resolve()
    )
    try:
        names = [path.resolve().relative_to(output_parent).as_posix() for path in artifacts]
    except ValueError as error:
        raise ValueError("artifact directory must be inside the manifest directory") from error
    lines = [f"{file_sha256(path)}  {name}\n" for path, name in zip(artifacts, names, strict=True)]
    output.write_text("".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    write_manifest(arguments.directory, arguments.output)


if __name__ == "__main__":
    main()
