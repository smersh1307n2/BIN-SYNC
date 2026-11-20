#!/usr/bin/env python3
"""Verify that BIN-SYNC uses the expected FLD-DB Storage submodule commit."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUBMODULE_PATH = "external/Fld1.Storage"
STORAGE_PROJECT = ROOT / SUBMODULE_PATH / "src" / "Fld1.Storage" / "Fld1.Storage.csproj"
EXPECTED_URL = "https://github.com/smersh1307n2/FLD-DB.git"


def git(*args: str, cwd: Path = ROOT) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def main() -> int:
    try:
        configured_url = git("config", "-f", ".gitmodules", "--get", "submodule.fld1-storage.url")
        tree_entry = git("ls-tree", "HEAD", SUBMODULE_PATH)
        actual_commit = git("-C", SUBMODULE_PATH, "rev-parse", "HEAD")
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        print(f"ERROR: unable to inspect FLD Storage submodule: {exc}", file=sys.stderr)
        return 2

    errors: list[str] = []
    if configured_url != EXPECTED_URL:
        errors.append(f"unexpected submodule URL: {configured_url!r}")
    fields = tree_entry.split()
    pinned_commit: str | None = None
    if len(fields) != 4 or fields[0] != "160000" or fields[1] != "commit":
        errors.append(f"expected a gitlink at {SUBMODULE_PATH}, got: {tree_entry!r}")
    else:
        pinned_commit = fields[2]
        if actual_commit != pinned_commit:
            errors.append(
                f"checked-out submodule is {actual_commit}, but HEAD gitlink pins {pinned_commit}"
            )
    if not STORAGE_PROJECT.is_file():
        errors.append(f"Storage project not found: {STORAGE_PROJECT}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    assert pinned_commit is not None
    print(f"Verified {SUBMODULE_PATH} at FLD-DB commit {pinned_commit}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
