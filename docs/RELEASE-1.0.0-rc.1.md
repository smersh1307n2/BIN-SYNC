# BinSync.Core v1.0.0-rc.1

**Branch:** `main`  
**Target:** `net10.0`  
**Date:** 2026-10-29  
**Status:** Release candidate (API freeze for 1.x)

## Highlights

- Package **`1.0.0-rc.1`** — first RC on the path to stable **1.0.0**
- Public API documented and frozen: [PUBLIC-API.md](PUBLIC-API.md)
- Engine includes post-alpha.7 hot-path work (encode / decode / apply)

## Package

```
PackageId: BinSync.Core
Version:   1.0.0-rc.1
```

Single external dependency: `Blake3.Managed` 1.10.0.

## Compatibility

- Wire-compatible with `0.1.0-alpha.6` / `0.1.0-alpha.7`
- No intentional public API removals vs alpha.7

## After RC

1. Soak (tests + at least one external embedder if available)
2. Tag **`v1.0.0`** with the same API surface
3. Optional: nuget.org publish

See [CHANGELOG.md](CHANGELOG.md) and [PRODUCTION-STATUS.md](PRODUCTION-STATUS.md).
