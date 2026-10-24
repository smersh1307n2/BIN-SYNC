#!/usr/bin/env bash
# Vendor minimal RFC 8032 Ed25519 stack from ZtSharp (Bouncy Castle MIT subset).
# Usage (from repo root):
#   bash scripts/vendor-ed25519-from-ztsharp.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/BinSync.Net/src/BinSync.Core/Crypto"
BASE_URL="https://raw.githubusercontent.com/smersh1307n2/ZtSharp/main/ZtSharp/Crypto/External"

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

echo "Vendor dest: $DEST"
mkdir -p "$DEST"

for f in "${FILES[@]}"; do
  dir=$(dirname "$f")
  mkdir -p "$DEST/$dir"
  echo "  GET $f"
  curl -fsSL "$BASE_URL/$f" -o "$DEST/$f"
done

# Remove interim partial LongDigest if present (script installs monolithic LongDigest.cs)
rm -f "$DEST"/Digests/LongDigest.Part*.cs

echo "Rewriting namespaces..."
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
        t = re.sub(
            r"\n\s*public static BigInteger\[\].*?(?=\n\s*public static|\n\s*})",
            "\n",
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
            "// BinSync: BCL RNG (no SecureRandom vendor)",
        )
        t = t.replace("RandomSource.NextBytes(bytes);", "RandomNumberGenerator.Fill(bytes);")
    if p.name == "Ed25519.cs":
        t = t.replace("using BinSync.Crypto.Security;\n", "using System.Security.Cryptography;\n")
        t = t.replace(
            "public static void GeneratePrivateKey(SecureRandom random, byte[] k)\n        {\n            random.NextBytes(k);\n        }",
            "public static void GeneratePrivateKey(byte[] k)\n        {\n            RandomNumberGenerator.Fill(k);\n        }",
        )
    p.write_text(t, encoding="utf-8")
print("Patched", sum(1 for _ in root.rglob("*.cs")), "cs files under", root)
PY

echo "Done. Next:"
echo "  1) Remove PackageReference BouncyCastle.Cryptography from BinSync.Core.csproj"
echo "  2) Point BatchSignature at BinSync.Crypto.Ed25519.Ed25519 (docs/CRYPTO-NOTICE.md)"
echo "  3) dotnet test && git add BinSync.Net/src/BinSync.Core/Crypto"
