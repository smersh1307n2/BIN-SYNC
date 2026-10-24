# Crypto / Ed25519

## Current state (`installing-.net-10-ea6be`)

### In-tree under `BinSync.Net/src/BinSync.Core/Crypto/`

| Path | Notes |
|------|--------|
| `LICENSE.txt` | Bouncy Castle MIT |
| `Base/IDigest.cs` | ✅ |
| `Utilities/{IMemoable,Times,Integers}.cs` | ✅ |
| `Raw/{Bits,Interleave}.cs` | ✅ |
| `Digests/Sha512Digest.cs` | ✅ |
| `Digests/LongDigest.Part{00,01,02}.cs` | ✅ partial class |
| `Ed25519/`, `X25519/`, `Raw/{Nat,Nat256,Mod}`, `Utilities/{Pack,Arrays}` | **via script** |

### Still on NuGet

`BouncyCastle.Cryptography` 2.6.2 in `BinSync.Core.csproj` — keeps tests green until full vendor lands.

## One-shot full vendor

```bash
bash scripts/vendor-ed25519-from-ztsharp.sh
# removes need for partial LongDigest files — delete Digests/LongDigest.Part*.cs after script
rm -f BinSync.Net/src/BinSync.Core/Crypto/Digests/LongDigest.Part*.cs
# then drop NuGet + switch BatchSignature (see below)
```

Script downloads 16 files from
[ZtSharp/Crypto/External](https://github.com/smersh1307n2/ZtSharp/tree/main/ZtSharp/Crypto/External),
rewrites `ZeroTierNet.Crypto.External` → `BinSync.Crypto`, and patches:

- `Arrays` / `Nat` / `Nat256`: strip BigInteger helpers
- `Mod` / `Ed25519.GeneratePrivateKey`: BCL `RandomNumberGenerator` (no SecureRandom tree)

**Do not copy** ZtSharp `C25519KeyPair` (96-byte ZT sigs).

## After full vendor: switch API

1. Remove `PackageReference` `BouncyCastle.Cryptography` from `BinSync.Core.csproj`.
2. `BatchSignature`:

```csharp
using BinSync.Crypto.Ed25519;
Ed25519.GeneratePublicKey(seed, 0, pub, 0);
Ed25519.Sign(seed, 0, pub, 0, msg, 0, msg.Length, sig, 0);
Ed25519.Verify(sig, 0, pub, 0, msg, 0, msg.Length);
```

3. `dotnet test` → commit `Crypto/` + csproj.
