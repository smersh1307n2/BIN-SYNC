#!/usr/bin/env python3
"""Verify that BIN-SYNC uses the expected FLD-DB Storage submodule commit."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUBMODULE_PATH = "external/Fld1.Storage"
EXPECTED_COMMIT = "e91d2e6382079df1e258204096141c7427c6088c"
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
    if len(fields) != 4 or fields[0] != "160000" or fields[1] != "commit":
        errors.append(f"expected a gitlink at {SUBMODULE_PATH}, got: {tree_entry!r}")
    elif fields[2] != EXPECTED_COMMIT:
        errors.append(f"gitlink points to {fields[2]}, expected {EXPECTED_COMMIT}")
    if actual_commit != EXPECTED_COMMIT:
        errors.append(f"checked-out submodule is {actual_commit}, expected {EXPECTED_COMMIT}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(f"Verified {SUBMODULE_PATH} at FLD-DB commit {EXPECTED_COMMIT}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
