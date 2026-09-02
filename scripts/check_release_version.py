"""Verify that every shipped fpstreams version marker agrees."""

from __future__ import annotations

import argparse
import ast
import json
import re
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def _toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return tomllib.load(stream)


def _named_package_version(document: dict[str, Any], name: str) -> str:
    packages = [package for package in document["package"] if package.get("name") == name]
    if len(packages) != 1:
        raise ValueError(f"expected one {name!r} package in lock file, found {len(packages)}")
    return str(packages[0]["version"])


def _runtime_version(path: Path) -> str:
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for statement in module.body:
        if (
            isinstance(statement, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__version__"
                for target in statement.targets
            )
            and isinstance(statement.value, ast.Constant)
            and isinstance(statement.value.value, str)
        ):
            return statement.value.value
    raise ValueError(f"missing literal __version__ assignment in {path}")


def release_versions(root: Path = ROOT) -> dict[str, str]:
    """Return the version markers included in source and release metadata."""
    project = _toml(root / "pyproject.toml")
    cargo = _toml(root / "rust" / "Cargo.toml")
    cargo_lock = _toml(root / "rust" / "Cargo.lock")
    uv_lock = _toml(root / "uv.lock")
    api = json.loads((root / "tests" / "public_api_v2.json").read_text(encoding="utf-8"))
    changelog = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    changelog_match = re.search(r"^## ([0-9]+\.[0-9]+\.[0-9]+)(?:\s|$)", changelog, re.MULTILINE)
    if changelog_match is None:
        raise ValueError("CHANGELOG.md has no release heading")
    return {
        "pyproject": str(project["project"]["version"]),
        "cargo": str(cargo["package"]["version"]),
        "cargo-lock": _named_package_version(cargo_lock, "fpstreams-native"),
        "uv-lock": _named_package_version(uv_lock, "fpstreams"),
        "runtime": _runtime_version(root / "src" / "fpstreams" / "__init__.py"),
        "public-api": str(api["version"]),
        "changelog": changelog_match.group(1),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", help="Release version, with or without a leading v")
    arguments = parser.parse_args()
    versions = release_versions()
    expected = arguments.expected.removeprefix("v") if arguments.expected else versions["pyproject"]
    mismatches = {name: version for name, version in versions.items() if version != expected}
    if mismatches:
        details = ", ".join(f"{name}={version}" for name, version in mismatches.items())
        raise SystemExit(f"release version mismatch: expected {expected}; {details}")
    print(expected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
