# BinSync.Core v1.0.0-rc.2

**Branch:** `main`  
**Target:** `net10.0`  
**Date:** 2026-10-30  
**Status:** Release candidate (API freeze unchanged from rc.1)

## Highlights

- Package **`1.0.0-rc.2`**
- Domain sub-gates + `NodeId` alias maps (thread-safety hardening)
- Same public API as rc.1 — [PUBLIC-API.md](PUBLIC-API.md)
- 277 tests green

## Package

```
PackageId: BinSync.Core
Version:   1.0.0-rc.2
```

Single external dependency: `Blake3.Managed` 1.10.0.

## Compatibility

- Wire-compatible with `1.0.0-rc.1` and `0.1.0-alpha.6` / `0.1.0-alpha.7`
- No public API removals vs rc.1

## After this RC

1. Soak (tests + fuzz/lab CI)
2. Tag **`v1.0.0`** with the same API surface
3. Optional: nuget.org publish

See [CHANGELOG.md](CHANGELOG.md) and [PRODUCTION-STATUS.md](PRODUCTION-STATUS.md).
