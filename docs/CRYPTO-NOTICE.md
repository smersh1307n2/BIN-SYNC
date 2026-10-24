# Third-party crypto (Ed25519)

Files under `BinSync.Net/src/BinSync.Core/Crypto/` are a **minimal subset** of the
Bouncy Castle C# crypto library (MIT), as vendored by ZtSharp
(`smersh1307n2/ZtSharp`, path `ZtSharp/Crypto/External/`).

Original copyright: The Legion of the Bouncy Castle Inc. (https://www.bouncycastle.org)
License: MIT — see `Crypto/LICENSE.txt`.

## How sources are produced (primary)

```bash
bash scripts/vendor-ed25519-from-ztsharp.sh
```

Downloads the ZtSharp archive from GitHub, copies the 16 required files,
rewrites namespaces `ZeroTierNet.Crypto.External` → `BinSync.Crypto`, strips
unused BigInteger helpers from Arrays/Nat, and replaces SecureRandom with
`System.Security.Cryptography.RandomNumberGenerator`.

**CI runs this script before every `dotnet build`**, so Crypto/ is always complete
even if expanded sources are not committed.

## Offline fallback

`third_party/ed25519-zlib/*.zlib.b64` + `scripts/expand-ed25519-vendor.py`
hold already-patched blobs for most files. The large `Ed25519.cs` blob is not
stored; use the primary vendor script when you need a full offline tree.

Used only for RFC 8032 Ed25519 batch signatures and KEY_ROTATION proofs (§14.8–14.9).
**No BouncyCastle.Cryptography NuGet package is referenced.**
