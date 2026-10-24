# Crypto / Ed25519

## Current

`BatchSignature` and `KeyRotation` use **NuGet `BouncyCastle.Cryptography` 2.6.2** for RFC 8032 Ed25519.
.NET 10 BCL still has no `Ed25519` type.

## Planned: vendor from ZtSharp (no NuGet)

ZtSharp already vendors a **minimal MIT subset** of Bouncy Castle under:

`https://github.com/smersh1307n2/ZtSharp/tree/main/ZtSharp/Crypto/External`

Relevant paths:

| Path | Role |
|------|------|
| `Ed25519/` | `Ed25519.cs`, parameters, signer |
| `X25519/X25519Field.cs` | field arithmetic used by Ed25519 |
| `Raw/` | Nat, Nat256, Mod, Interleave, **Bits** |
| `Digests/` | Sha512 (+ LongDigest) |
| `Utilities/` | Arrays, Pack, Integers, IMemoable |
| `Base/IDigest.cs` | digest interface |

**Do not copy** ZtSharp `C25519KeyPair` (96-byte ZT signature layout + hash-prefix) — BIN-SYNC needs standard **64-byte** Ed25519 over the canonical batch payload.

Namespace in ZtSharp: `ZeroTierNet.Crypto.External.*` → rewrite to `BinSync.Crypto.*`.

Local staging (agent workspace): `/home/workdir/artifacts/ed25519-vendor/` with namespaces already rewritten and `Mod.cs` using BCL `RandomNumberGenerator` (no SecureRandom/Prng tree).

After full vendor lands under `BinSync.Net/src/BinSync.Core/Crypto/`, remove the BouncyCastle PackageReference and point `BatchSignature` at `BinSync.Crypto.Ed25519.Ed25519` static `Sign`/`Verify`/`GeneratePublicKey`.
