#!/usr/bin/env bash
# Build BinSync.Core nupkg (requires .NET 10 SDK + vendored Ed25519).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Vendor Ed25519 (if missing)"
if [[ ! -f BinSync.Net/src/BinSync.Core/Crypto/Ed25519/Ed25519.cs ]]; then
  bash scripts/vendor-ed25519-from-ztsharp.sh
fi

echo "==> Pack"
dotnet pack BinSync.Net/src/BinSync.Core/BinSync.Core.csproj \
  -c Release \
  -o "$ROOT/artifacts/nupkg" \
  /p:PackageVersion=0.1.0-alpha.5

echo "==> Output"
ls -la "$ROOT/artifacts/nupkg"
echo "Done. Push with: dotnet nuget push artifacts/nupkg/*.nupkg -s https://api.nuget.org/v3/index.json -k \$NUGET_API_KEY"
