# BinSync.Core v1.0.0

**Branch:** `main`  
**Target:** `net10.0`  
**Date:** 2026-10-31  
**Status:** Stable

## Highlights

- Package **`1.0.0`** — first stable release
- Public API frozen: [PUBLIC-API.md](PUBLIC-API.md)
- Domain sub-gates + `NodeId` alias maps
- Hot-path encode / decode / apply optimisations
- **277 tests** green

## Package

```
PackageId: BinSync.Core
Version:   1.0.0
```

Single external dependency: `Blake3.Managed` 1.10.0.

## Compatibility

- Wire-compatible with `1.0.0-rc.1` / `rc.2` and `0.1.0-alpha.6` / `alpha.7`
- No public API removals vs the RC line

## Optional next

- nuget.org publish (`scripts/pack-nupkg.sh`)
- Full X.509 (post-1.0)

See [CHANGELOG.md](CHANGELOG.md) and [PRODUCTION-STATUS.md](PRODUCTION-STATUS.md).
