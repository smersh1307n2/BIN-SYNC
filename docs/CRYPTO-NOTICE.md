# Third-party crypto (Ed25519)

Files under `BinSync.Net/src/BinSync.Core/Crypto/` are a **minimal subset** of the
Bouncy Castle C# crypto library (MIT), as vendored by ZtSharp
(`smersh1307n2/ZtSharp`, path `ZtSharp/Crypto/External/`).

Original copyright: The Legion of the Bouncy Castle Inc. (https://www.bouncycastle.org)
License: MIT — see `Crypto/LICENSE.txt`.

## How sources are produced

```bash
bash scripts/vendor-ed25519-from-ztsharp.sh
```

This downloads the ZtSharp archive from GitHub, copies the 16 required files,
rewrites namespaces `ZeroTierNet.Crypto.External` → `BinSync.Crypto`, strips
unused BigInteger helpers from Arrays/Nat, and replaces SecureRandom with
`System.Security.Cryptography.RandomNumberGenerator`.

CI runs the same script before `dotnet build` so Crypto/ is always present
even if the expanded sources are not committed.

Optional offline path: `third_party/ed25519-zlib/*.zlib.b64` + 
`scripts/expand-ed25519-vendor.py` (zlib+base64 blobs of the already-patched sources).

Used only for RFC 8032 Ed25519 batch signatures and KEY_ROTATION proofs (§14.8–14.9).
**No BouncyCastle.Cryptography NuGet package is referenced.**
