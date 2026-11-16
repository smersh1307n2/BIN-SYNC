#!/usr/bin/env bash
# Vendor minimal RFC 8032 Ed25519 from ZtSharp archive (Bouncy Castle MIT subset).
# Usage (from repo root):
#   bash scripts/vendor-ed25519-from-ztsharp.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/BinSync.Net/src/BinSync.Core/Crypto"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

ZIP_URL="https://codeload.github.com/smersh1307n2/ZtSharp/zip/refs/heads/main"
echo "Downloading ZtSharp archive..."
curl -fsSL --max-time 120 -o "$TMP/ztsharp.zip" "$ZIP_URL"
unzip -q "$TMP/ztsharp.zip" -d "$TMP"
SRC=$(find "$TMP" -type d -path '*/ZtSharp/Crypto/External' | head -1)
if [[ -z "$SRC" || ! -d "$SRC" ]]; then
  echo "ERROR: Crypto/External not found in archive" >&2
  exit 1
fi
echo "Source: $SRC"

FILES=(
  "LICENSE.txt"
  "Base/IDigest.cs"
  "Utilities/IMemoable.cs"
  "Utilities/Times.cs"
  "Utilities/Integers.cs"
  "Utilities/Pack.cs"
  "Utilities/Arrays.cs"
  "Raw/Bits.cs"
  "Raw/Interleave.cs"
  "Raw/Nat.cs"
  "Raw/Nat256.cs"
  "Raw/Mod.cs"
  "Digests/LongDigest.cs"
  "Digests/Sha512Digest.cs"
  "X25519/X25519Field.cs"
  "Ed25519/Ed25519.cs"
)

echo "Copying into $DEST"
mkdir -p "$DEST"
for f in "${FILES[@]}"; do
  mkdir -p "$DEST/$(dirname "$f")"
  cp "$SRC/$f" "$DEST/$f"
  echo "  $f"
done

# Drop interim partial LongDigest if present
rm -f "$DEST"/Digests/LongDigest.Part*.cs

echo "Rewriting namespaces ZeroTierNet.Crypto.External -> BinSync.Crypto ..."
find "$DEST" -name '*.cs' -print0 | xargs -0 sed -i \
  -e 's/ZeroTierNet\.Crypto\.External/BinSync.Crypto/g'

python3 - "$DEST" <<'PY'
from pathlib import Path
import sys, re
root = Path(sys.argv[1])
for p in root.rglob("*.cs"):
    t = p.read_text(encoding="utf-8")
    if t.startswith("\ufeff"):
        t = t[1:]
    t = t.replace("\ufeff", "")
    if p.name == "Arrays.cs":
        # Remove only the two BigInteger overloads (keep brace balance)
        t = re.sub(
            r"\n\s*public static BigInteger\[\] CopyOf\(BigInteger\[\] data, int newLength\)\s*\{[^}]*\}",
            "",
            t,
            flags=re.S,
        )
        t = re.sub(
            r"\n\s*public static BigInteger\[\] CopyOfRange\(BigInteger\[\] data, int from, int to\)\s*\{[^}]*\}",
            "",
            t,
            flags=re.S,
        )
        t = t.replace("using BinSync.Crypto.Raw;\n", "")
    if p.name in ("Nat.cs", "Nat256.cs"):
        for pattern in [
            r"public static uint\[\] FromBigInteger\(.*?\n        }",
            r"public static ulong\[\] FromBigInteger64\(.*?\n        }",
            r"public static BigInteger ToBigInteger\(.*?\n        }",
            r"public static BigInteger ToBigInteger64\(.*?\n        }",
        ]:
            t = re.sub(pattern, "", t, flags=re.S)
    if p.name == "Mod.cs":
        t = t.replace("using BinSync.Crypto.Security;\n", "using System.Security.Cryptography;\n")
        t = t.replace(
            "private static readonly SecureRandom RandomSource = new SecureRandom();",
            "// BinSync: BCL RNG",
        )
        t = t.replace("RandomSource.NextBytes(bytes);", "RandomNumberGenerator.Fill(bytes);")
    if p.name == "Ed25519.cs":
        t = t.replace("using BinSync.Crypto.Security;\n", "using System.Security.Cryptography;\n")
        t = t.replace(
            "public static void GeneratePrivateKey(SecureRandom random, byte[] k)\n        {\n            random.NextBytes(k);\n        }",
            "public static void GeneratePrivateKey(byte[] k)\n        {\n            RandomNumberGenerator.Fill(k);\n        }",
        )
    p.write_text(t, encoding="utf-8")

bad = []
for p in root.rglob("*.cs"):
    t = p.read_text()
    if "SecureRandom" in t or "BigInteger" in t or "ZeroTierNet" in t:
        bad.append(str(p.relative_to(root)))
if bad:
    raise SystemExit("patch incomplete: " + ", ".join(bad))
# Sanity: Arrays brace balance
arr = (root / "Utilities" / "Arrays.cs").read_text()
if arr.count("{") != arr.count("}"):
    raise SystemExit("Arrays.cs brace imbalance after patch")
print("Patched", sum(1 for _ in root.rglob("*.cs")), "cs files")
PY

echo "Done. Files:"
find "$DEST" -type f | sort
