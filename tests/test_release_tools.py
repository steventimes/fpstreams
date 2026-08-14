"""Release artifacts are checked by executable tools, not workflow text assertions."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PINNED_ACTION = re.compile(
    r"^\s*- uses: [^@\s]+@[0-9a-f]{40}\s+#\s+\S+.*$",
    re.MULTILINE,
)


def test_sha256_manifest_is_sorted_and_excludes_itself(tmp_path: Path) -> None:
    (tmp_path / "b.whl").write_bytes(b"wheel-b")
    (tmp_path / "a.tar.gz").write_bytes(b"source-a")
    manifest = tmp_path / "SHA256SUMS"

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "write_sha256_manifest.py"),
            str(tmp_path),
            str(manifest),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert manifest.read_text(encoding="utf-8") == (
        f"{hashlib.sha256(b'source-a').hexdigest()}  a.tar.gz\n"
        f"{hashlib.sha256(b'wheel-b').hexdigest()}  b.whl\n"
    )


def test_release_smoke_checks_native_and_python_backends() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")

    result = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "smoke_release.py")],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "native": [1, 3, 5, 7],
        "python": [1, 3, 5, 7],
        "version": "2.0.0",
    }


def test_external_github_actions_are_pinned_to_documented_commits() -> None:
    workflows = sorted((ROOT / ".github" / "workflows").glob("*.yml"))
    unpinned: list[str] = []
    for workflow in workflows:
        text = workflow.read_text(encoding="utf-8")
        uses_lines = [line for line in text.splitlines() if "- uses:" in line]
        unpinned.extend(
            f"{workflow.name}: {line.strip()}"
            for line in uses_lines
            if PINNED_ACTION.fullmatch(line) is None
        )

    assert not unpinned, "unpinned actions:\n" + "\n".join(unpinned)


def test_publish_only_receives_credentials_after_artifact_verification() -> None:
    workflow = (ROOT / ".github" / "workflows" / "publish.yml").read_text(encoding="utf-8")

    assert workflow.count("id-token: write") == 1
    assert "needs: manifest" in workflow
    assert "Smoke-test wheel" in workflow
    assert "Build and smoke-test sdist" in workflow
    assert "write_sha256_manifest.py dist/packages dist/SHA256SUMS" in workflow
