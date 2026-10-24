#!/usr/bin/env python3
"""Offline expand of zlib+base64 vendor blobs into Crypto/.

PRIMARY path for CI and local is:
  bash scripts/vendor-ed25519-from-ztsharp.sh

This script is a fallback when the ZtSharp zip is unavailable. It expands
whatever blobs exist under third_party/ed25519-zlib/. Ed25519.cs.zlib.b64 may
be absent — prefer the shell vendor script for a complete tree.
"""
from pathlib import Path
import base64, zlib, sys

ROOT = Path(__file__).resolve().parents[1]
BLOB_DIR = ROOT / "third_party" / "ed25519-zlib"
DEST = ROOT / "BinSync.Net" / "src" / "BinSync.Core" / "Crypto"

MAP = {
    "LICENSE.txt.zlib.b64": "LICENSE.txt",
    "IDigest.cs.zlib.b64": "Base/IDigest.cs",
    "IMemoable.cs.zlib.b64": "Utilities/IMemoable.cs",
    "Times.cs.zlib.b64": "Utilities/Times.cs",
    "Integers.cs.zlib.b64": "Utilities/Integers.cs",
    "Pack.cs.zlib.b64": "Utilities/Pack.cs",
    "Arrays.cs.zlib.b64": "Utilities/Arrays.cs",
    "Bits.cs.zlib.b64": "Raw/Bits.cs",
    "Interleave.cs.zlib.b64": "Raw/Interleave.cs",
    "Nat.cs.zlib.b64": "Raw/Nat.cs",
    "Nat256.cs.zlib.b64": "Raw/Nat256.cs",
    "Mod.cs.zlib.b64": "Raw/Mod.cs",
    "LongDigest.cs.zlib.b64": "Digests/LongDigest.cs",
    "Sha512Digest.cs.zlib.b64": "Digests/Sha512Digest.cs",
    "X25519Field.cs.zlib.b64": "X25519/X25519Field.cs",
    "Ed25519.cs.zlib.b64": "Ed25519/Ed25519.cs",
}

def main():
    if not BLOB_DIR.is_dir():
        print("No blob dir", BLOB_DIR, file=sys.stderr)
        print("Use: bash scripts/vendor-ed25519-from-ztsharp.sh", file=sys.stderr)
        return 1
    n = 0
    missing = []
    for blob_name, rel in MAP.items():
        src = BLOB_DIR / blob_name
        if not src.is_file():
            missing.append(blob_name)
            continue
        data = zlib.decompress(base64.b64decode(src.read_text().strip().encode("ascii")))
        out = DEST / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(data)
        print("wrote", out.relative_to(ROOT), len(data))
        n += 1
    digests = DEST / "Digests"
    if digests.is_dir():
        for p in digests.glob("LongDigest.Part*.cs"):
            p.unlink()
            print("removed", p.name)
    if missing:
        print("skip missing:", ", ".join(missing), file=sys.stderr)
        print("For a complete Crypto/ tree run:", file=sys.stderr)
        print("  bash scripts/vendor-ed25519-from-ztsharp.sh", file=sys.stderr)
    print("expanded", n, "files")
    return 0 if n else 1

if __name__ == "__main__":
    raise SystemExit(main())
