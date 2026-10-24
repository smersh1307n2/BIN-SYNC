# Crypto / Ed25519

## Current (branch tip)

- **NuGet:** `BouncyCastle.Cryptography` 2.6.2 — used by `BatchSignature` / `KeyRotation`.
- **Partial in-tree vendor:** `BinSync.Net/src/BinSync.Core/Crypto/` already contains
  `Base/IDigest.cs`, `Utilities/{IMemoable,Times,Integers}.cs`, `Raw/Bits.cs`, `LICENSE.txt`.
- **Full vendor:** run from repo root:

```bash
bash scripts/vendor-ed25519-from-ztsharp.sh
```

This downloads the **minimal MIT subset** from
[`ZtSharp/Crypto/External`](https://github.com/smersh1307n2/ZtSharp/tree/main/ZtSharp/Crypto/External),
rewrites `ZeroTierNet.Crypto.External` → `BinSync.Crypto`, and applies BinSync patches:

| Patch | Why |
|-------|-----|
| Drop `BigInteger` helpers in `Arrays.cs` | Avoid vendoring 100KB+ BigInteger |
| `Mod` / `Ed25519.GeneratePrivateKey` use BCL `RandomNumberGenerator` | Skip SecureRandom/Prng/Sha256 tree |

**Do not copy** ZtSharp `C25519KeyPair` (96-byte ZT signatures). BIN-SYNC needs standard **64-byte** Ed25519.

## After full vendor

1. Remove `BouncyCastle.Cryptography` from `BinSync.Core.csproj`.
2. Change `BatchSignature` to call:

```csharp
using BinSync.Crypto.Ed25519;
// seed 32 bytes, pub 32, sig 64
Ed25519.GeneratePublicKey(seed, 0, pub, 0);
Ed25519.Sign(seed, 0, pub, 0, msg, 0, msg.Length, sig, 0);
Ed25519.Verify(sig, 0, pub, 0, msg, 0, msg.Length);
```

3. `dotnet test` then commit `Crypto/`.

## File list (16)

`LICENSE.txt`, `Base/IDigest.cs`, `Utilities/{IMemoable,Times,Integers,Pack,Arrays}.cs`,
`Raw/{Bits,Interleave,Nat,Nat256,Mod}.cs`, `Digests/{LongDigest,Sha512Digest}.cs`,
`X25519/X25519Field.cs`, `Ed25519/Ed25519.cs`.
