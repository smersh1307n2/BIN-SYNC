# Third-party crypto (Ed25519)

Files under `BinSync.Net/src/BinSync.Core/Crypto/` are a **minimal subset** of the
Bouncy Castle C# crypto library (**MIT**), as vendored by ZtSharp
(`smersh1307n2/ZtSharp`, path `ZtSharp/Crypto/External/`).

Original copyright: The Legion of the Bouncy Castle Inc. (https://www.bouncycastle.org)

**These files are not original BIN-SYNC code.** They remain under MIT and are
**not** covered by the AGPL-3.0-or-later / commercial dual license that applies
to the rest of BIN-SYNC. See root [`LICENSE.md`](../LICENSE.md) and
[`THIRD-PARTY-NOTICES.md`](../THIRD-PARTY-NOTICES.md).

## How sources are produced

```bash
bash scripts/vendor-ed25519-from-ztsharp.sh
```

CI runs this script before every `dotnet build`.

## Scope

Used only for RFC 8032 Ed25519 batch signatures and KEY_ROTATION proofs (§14.8–14.9).
**No `BouncyCastle.Cryptography` NuGet package is referenced.**

E2EE field sealing uses .NET / platform ChaCha20-Poly1305 APIs, not this tree.
