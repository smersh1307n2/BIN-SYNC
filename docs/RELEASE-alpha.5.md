# BinSync.Core v0.1.0-alpha.5

**Branch:** `installing-.net-10-ea6be`  
**Target:** `net10.0`  
**Date:** 2026-10-24

## Highlights

- No `BouncyCastle.Cryptography` NuGet — Ed25519 vendored (MIT, ZtSharp subset)
- Production hardening P0–P2 complete
- Codec §6, Merge §8 (COUNTER/SET/MANUAL), Schema §18
- Opt-in: E2EE, causal deps, KEY_ROTATION
- Lab §17 core + edge (snapshot+tail, fragments, quarantine)
- FLAG_FRAGMENT reassembly
- ~250+ tests green on CI

## Package

```
PackageId: BinSync.Core
Version:   0.1.0-alpha.5
```

Single external dependency: `Blake3.Managed` 1.10.0.

## Remaining (optional)

- SEQ_FRACTIONAL / BLOB_REF merge policies
- Trust/TOFU polish (§15)
- NuGet.org publish

See [CHANGELOG.md](CHANGELOG.md) and [PRODUCTION-STATUS.md](PRODUCTION-STATUS.md).
