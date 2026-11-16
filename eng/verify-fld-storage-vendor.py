#!/usr/bin/env python3
"""Verify the vendored Fld1.Storage tree against its pinned Git blob manifest."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "external" / "Fld1.Storage"
MANIFEST = VENDOR / "UPSTREAM-MANIFEST.json"


def git_blob_sha1(data: bytes) -> str:
    header = f"blob {len(data)}\0".encode("ascii")
    return hashlib.sha1(header + data).hexdigest()


def main() -> int:
    if not MANIFEST.is_file():
        print(f"ERROR: missing upstream manifest: {MANIFEST}", file=sys.stderr)
        return 2

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    expected: dict[str, str] = manifest["files"]
    ignored = {"README.md", "UPSTREAM-MANIFEST.json"}
    actual_paths = {
        path.relative_to(VENDOR).as_posix()
        for path in VENDOR.rglob("*")
        if path.is_file() and path.relative_to(VENDOR).as_posix() not in ignored
    }

    errors: list[str] = []
    for path in sorted(set(expected) - actual_paths):
        errors.append(f"missing: {path}")
    for path in sorted(actual_paths - set(expected)):
        errors.append(f"unexpected: {path}")
    for path in sorted(set(expected) & actual_paths):
        # Git stores text blobs with LF; Windows checkout may materialize CRLF.\n        data = (VENDOR / path).read_bytes().replace(b"\\r\\n", b"\\n")\n        actual_sha = git_blob_sha1(data)
        if actual_sha != expected[path]:
            errors.append(f"modified: {path} (expected {expected[path]}, got {actual_sha})")

    if errors:
        print(
            "Vendored Fld1.Storage does not match "
            f"{manifest['source_repository']}@{manifest['source_commit']}:",
            file=sys.stderr,
        )
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print(
        "Verified "
        f"{len(expected)} Fld1.Storage files against "
        f"{manifest['source_repository']}@{manifest['source_commit']}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
