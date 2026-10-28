# BinSync.Core v0.1.0-alpha.6

**Branch:** `main`  
**Target:** `net10.0`  
**Date:** 2026-10-28  
**Tip:** `661666e234bb51659ac087981aa7481e6fa9c86e`

## Highlights

- Production **comparer / allocation** pass: shared `*.Instance`, `HashCode.AddBytes`
- Codec zero-alloc varint / fixed LE write helpers
- HLC wall in **microseconds**
- Same protocol surface as alpha.5 (Merge §8, Schema, E2EE/causal opt-in, TrustStore, fragments)

## Package

```
PackageId: BinSync.Core
Version:   0.1.0-alpha.6
```

Single external dependency: `Blake3.Managed` 1.10.0.

## Remaining (optional)

- NuGet.org publish
- X.509 trust path polish + adversarial fuzz in CI
- Spec errata fold into main specification file

See [CHANGELOG.md](CHANGELOG.md) and [PRODUCTION-STATUS.md](PRODUCTION-STATUS.md).
