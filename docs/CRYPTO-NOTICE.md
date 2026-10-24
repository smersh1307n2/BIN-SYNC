# Third-party crypto (Ed25519)

Files under `BinSync.Net/src/BinSync.Core/Crypto/` are a **minimal subset** of the
Bouncy Castle C# crypto library (MIT), as vendored by ZtSharp
(`smersh1307n2/ZtSharp`, path `ZtSharp/Crypto/External/`).

Original copyright: The Legion of the Bouncy Castle Inc. (https://www.bouncycastle.org)
License: MIT — see `BinSync.Net/src/BinSync.Core/Crypto/LICENSE.txt`.

Namespace rewritten from `ZeroTierNet.Crypto.External` → `BinSync.Crypto`.
Unused BigInteger helpers were stripped from `Arrays`.

Used only for RFC 8032 Ed25519 batch signatures and KEY_ROTATION proofs (§14.8–14.9).
**No** `BouncyCastle.Cryptography` NuGet package is referenced.
