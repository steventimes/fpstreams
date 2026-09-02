#!/usr/bin/env python3
"""Build the pure-Python fpstreams wheel used by the documentation playground."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import os
import re
import tempfile
import tomllib
import zipfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SOURCE_PACKAGE = ROOT / "src" / "fpstreams"
DEFAULT_OUTPUT_DIR = ROOT / "fpstreams" / "docs" / "assets" / "packages"
MANIFEST_NAME = "browser-wheel.json"
NATIVE_SUFFIXES = {".dylib", ".pyd", ".so"}
ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


def _load_project() -> Mapping[str, Any]:
    with (ROOT / "pyproject.toml").open("rb") as stream:
        project = tomllib.load(stream).get("project")
    if not isinstance(project, Mapping):
        raise ValueError("pyproject.toml does not contain a [project] table")
    return project


def _wheel_component(value: str) -> str:
    return re.sub(r"[^\w\d.]+", "_", value, flags=re.UNICODE)


def _metadata_header(value: object) -> str:
    return " ".join(str(value).splitlines())


def _extra_requirement(requirement: str, extra: str) -> str:
    dependency, separator, marker = requirement.partition(";")
    extra_marker = f'extra == "{extra}"'
    if not separator:
        return f"{dependency.strip()}; {extra_marker}"
    return f"{dependency.strip()}; ({marker.strip()}) and {extra_marker}"


def _readme(project: Mapping[str, Any]) -> tuple[str | None, str]:
    configured = project.get("readme")
    if isinstance(configured, str):
        path = ROOT / configured
        content_type = "text/markdown" if path.suffix.lower() == ".md" else "text/plain"
        return content_type, path.read_text(encoding="utf-8")
    if isinstance(configured, Mapping):
        content_type = configured.get("content-type")
        if "text" in configured:
            return str(content_type) if content_type else None, str(configured["text"])
        if "file" in configured:
            path = ROOT / str(configured["file"])
            return str(content_type) if content_type else None, path.read_text(encoding="utf-8")
    return None, ""


def _author_headers(project: Mapping[str, Any]) -> Iterable[str]:
    authors = project.get("authors", [])
    if not isinstance(authors, list):
        return
    for author in authors:
        if not isinstance(author, Mapping):
            continue
        name = _metadata_header(author.get("name", ""))
        email = _metadata_header(author.get("email", ""))
        if email:
            yield f"Author-email: {name} <{email}>" if name else f"Author-email: {email}"
        elif name:
            yield f"Author: {name}"


def _project_headers(project: Mapping[str, Any]) -> Iterable[str]:
    scalar_headers = {
        "description": "Summary",
        "requires-python": "Requires-Python",
    }
    for key, header in scalar_headers.items():
        if value := project.get(key):
            yield f"{header}: {_metadata_header(value)}"
    license_expression = project.get("license")
    if isinstance(license_expression, str):
        yield f"License-Expression: {_metadata_header(license_expression)}"
    for license_file in project.get("license-files", []):
        yield f"License-File: {_metadata_header(license_file)}"
    for classifier in project.get("classifiers", []):
        yield f"Classifier: {_metadata_header(classifier)}"
    keywords = project.get("keywords")
    if isinstance(keywords, list) and keywords:
        yield f"Keywords: {','.join(_metadata_header(keyword) for keyword in keywords)}"
    urls = project.get("urls", {})
    if isinstance(urls, Mapping):
        for label, url in urls.items():
            yield f"Project-URL: {_metadata_header(label)}, {_metadata_header(url)}"


def _dependency_headers(project: Mapping[str, Any]) -> Iterable[str]:
    for requirement in project.get("dependencies", []):
        yield f"Requires-Dist: {_metadata_header(requirement)}"
    optional = project.get("optional-dependencies", {})
    if not isinstance(optional, Mapping):
        return
    for extra, requirements in optional.items():
        normalized_extra = re.sub(r"[-_.]+", "-", str(extra)).lower()
        yield f"Provides-Extra: {normalized_extra}"
        for requirement in requirements:
            yield f"Requires-Dist: {_extra_requirement(str(requirement), normalized_extra)}"


def _metadata(project: Mapping[str, Any]) -> bytes:
    content_type, description = _readme(project)
    lines = [
        "Metadata-Version: 2.4",
        f"Name: {_metadata_header(project['name'])}",
        f"Version: {_metadata_header(project['version'])}",
        *_project_headers(project),
        *_author_headers(project),
        *_dependency_headers(project),
    ]
    if content_type:
        lines.append(f"Description-Content-Type: {_metadata_header(content_type)}")
    lines.extend(("", description.rstrip(), ""))
    return "\n".join(lines).encode("utf-8")


def _wheel_metadata() -> bytes:
    return (
        b"Wheel-Version: 1.0\n"
        b"Generator: fpstreams browser wheel builder\n"
        b"Root-Is-Purelib: true\n"
        b"Tag: py3-none-any\n"
    )


def _package_files() -> Iterable[tuple[str, bytes]]:
    if not SOURCE_PACKAGE.is_dir():
        raise FileNotFoundError(f"source package not found: {SOURCE_PACKAGE}")
    for path in sorted(SOURCE_PACKAGE.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"browser wheel sources must not be symbolic links: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(SOURCE_PACKAGE)
        if "__pycache__" in relative.parts:
            continue
        if path.suffix.lower() == ".pyc" or path.suffix.lower() in NATIVE_SUFFIXES:
            continue
        yield (Path("fpstreams", relative).as_posix(), path.read_bytes())


def _license_files(project: Mapping[str, Any], dist_info: str) -> Iterable[tuple[str, bytes]]:
    for configured in project.get("license-files", []):
        path = ROOT / str(configured)
        if not path.is_file():
            raise FileNotFoundError(f"configured license file not found: {path}")
        yield (f"{dist_info}/licenses/{path.name}", path.read_bytes())


def _record(files: Mapping[str, bytes], record_path: str) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    for path, content in files.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b"=").decode()
        writer.writerow((path, f"sha256={digest}", len(content)))
    writer.writerow((record_path, "", ""))
    return stream.getvalue().encode("utf-8")


def _write_member(archive: zipfile.ZipFile, path: str, content: bytes) -> None:
    info = zipfile.ZipInfo(path, ZIP_TIMESTAMP)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = 0o100644 << 16
    archive.writestr(info, content)


def build_browser_wheel(output_dir: Path) -> Path:
    project = _load_project()
    distribution = _wheel_component(str(project["name"]))
    version = _wheel_component(str(project["version"]))
    wheel_name = f"{distribution}-{version}-py3-none-any.whl"
    dist_info = f"{distribution}-{version}.dist-info"
    record_path = f"{dist_info}/RECORD"

    files = dict(_package_files())
    files.update(_license_files(project, dist_info))
    files[f"{dist_info}/METADATA"] = _metadata(project)
    files[f"{dist_info}/WHEEL"] = _wheel_metadata()
    files = dict(sorted(files.items()))

    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / wheel_name
    with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".tmp", delete=False) as stream:
        temporary = Path(stream.name)
    try:
        with zipfile.ZipFile(temporary, "w") as archive:
            for path, content in files.items():
                _write_member(archive, path, content)
            _write_member(archive, record_path, _record(files, record_path))
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)

    manifest = {"version": str(project["version"]), "wheel": wheel_name}
    manifest_path = output_dir / MANIFEST_NAME
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=output_dir, suffix=".tmp", delete=False
    ) as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
        temporary_manifest = Path(stream.name)
    try:
        os.replace(temporary_manifest, manifest_path)
    finally:
        temporary_manifest.unlink(missing_ok=True)
    return destination


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"wheel and manifest destination (default: {DEFAULT_OUTPUT_DIR})",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    wheel = build_browser_wheel(args.output_dir)
    print(wheel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
