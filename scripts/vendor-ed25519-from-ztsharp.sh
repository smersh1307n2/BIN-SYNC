#!/usr/bin/env bash
# Vendor minimal RFC 8032 Ed25519 stack from ZtSharp (Bouncy Castle MIT subset).
# Usage (from repo root):
#   bash scripts/vendor-ed25519-from-ztsharp.sh
# Then remove BouncyCastle.Cryptography from BinSync.Core.csproj and switch
# BatchSignature to BinSync.Crypto.Ed25519 (see docs/CRYPTO-NOTICE.md).
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

echo "Rewriting namespaces ZeroTierNet.Crypto.External -> BinSync.Crypto ..."
find "$DEST" -name '*.cs' -print0 | xargs -0 sed -i \
  -e 's/ZeroTierNet\.Crypto\.External/BinSync.Crypto/g' \
  -e 's/\xEF\xBB\xBF//g'

# Strip UTF-8 BOM / U+FEFF if any remain
python3 - <<'PY'
from pathlib import Path
root = Path(r"'''"$DEST"'''".replace("'''",""))
# DEST injected below
PY
python3 - "$DEST" <<'PY'
from pathlib import Path
import sys, re
root = Path(sys.argv[1])
for p in root.rglob('*.cs'):
    t = p.read_text(encoding='utf-8')
    if t.startswith('\ufeff'):
        t = t[1:]
    t = t.replace('\ufeff', '')
    # Arrays: drop BigInteger helpers (unused by Ed25519; avoids vendoring BigInteger.cs)
    if p.name == 'Arrays.cs':
        t = re.sub(
            r'\n\s*public static BigInteger\[\].*?(?=\n\s*public static|\n\s*})',
            '\n',
            t,
            flags=re.S,
        )
        t = t.replace('using BinSync.Crypto.Raw;\n', '')
    # Mod: use BCL RNG instead of SecureRandom/Prng tree
    if p.name == 'Mod.cs':
        t = t.replace('using BinSync.Crypto.Security;\n', 'using System.Security.Cryptography;\n')
        t = t.replace(
            'private static readonly SecureRandom RandomSource = new SecureRandom();',
            '// BinSync: BCL RNG (no SecureRandom vendor)',
        )
        t = t.replace('RandomSource.NextBytes(bytes);', 'RandomNumberGenerator.Fill(bytes);')
    # Ed25519: GeneratePrivateKey without SecureRandom
    if p.name == 'Ed25519.cs':
        t = t.replace('using BinSync.Crypto.Security;\n', 'using System.Security.Cryptography;\n')
        t = t.replace(
            '''public static void GeneratePrivateKey(SecureRandom random, byte[] k)
        {
            random.NextBytes(k);
        }''',
            '''public static void GeneratePrivateKey(byte[] k)
        {
            RandomNumberGenerator.Fill(k);
        }''',
        )
    p.write_text(t, encoding='utf-8')
print('Patched', sum(1 for _ in root.rglob('*.cs')), 'cs files')
PY

echo "Done. Next:"
echo "  1) Remove PackageReference BouncyCastle.Cryptography from BinSync.Core.csproj"
echo "  2) Point BatchSignature at BinSync.Crypto.Ed25519.Ed25519"
echo "  3) dotnet test"
echo "  4) git add BinSync.Net/src/BinSync.Core/Crypto && git commit"
