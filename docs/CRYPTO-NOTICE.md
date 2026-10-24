# Crypto / Ed25519

## Vendor from ZtSharp (preferred)

```bash
bash scripts/vendor-ed25519-from-ztsharp.sh
```

The script:

1. Downloads **one** ZIP: `https://codeload.github.com/smersh1307n2/ZtSharp/zip/refs/heads/main`
2. Copies only the RFC 8032 subset from `ZtSharp/Crypto/External/`
3. Rewrites `ZeroTierNet.Crypto.External` → `BinSync.Crypto`
4. Patches out SecureRandom / BigInteger dependencies (BCL `RandomNumberGenerator`)

**Do not** copy ZtSharp `C25519KeyPair` (96-byte ZT signatures).

## After vendor

1. Remove `BouncyCastle.Cryptography` from `BinSync.Core.csproj`.
2. Switch `BatchSignature` to:

```csharp
using BinSync.Crypto.Ed25519;
Ed25519.GeneratePublicKey(seed, 0, pub, 0);
Ed25519.Sign(seed, 0, pub, 0, msg, 0, msg.Length, sig, 0);
Ed25519.Verify(sig, 0, pub, 0, msg, 0, msg.Length);
```

3. `dotnet test` and commit `Crypto/`.

## Already in-tree (partial)

`Base/IDigest`, `Utilities/{IMemoable,Times,Integers}`, `Raw/{Bits,Interleave}`,
`Digests/{Sha512Digest,LongDigest.Part*}`, `LICENSE.txt`.

Full tree (incl. `Ed25519.cs`, `Nat*`, `X25519Field`, `Pack`, `Arrays`, `Mod`) comes from the script.

## Current NuGet

`BouncyCastle.Cryptography` 2.6.2 remains until the script is run and `BatchSignature` is switched — keeps CI green.
