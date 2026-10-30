# Changelog — BinSync.Core

## 1.0.0-rc.2 — 2026-10-30

**Second release candidate.** Same API freeze as rc.1; engine hardening only.

**Target:** `net10.0` · **Package:** `BinSync.Core` `1.0.0-rc.2` · **Branch:** `main`

### Engine
- Domain sub-gates (`_lockStorage` → `_lockOutbox` → `_lockMeta`) + `EnterAllGates` / `AllGateScope`
- Alias maps migrated to `NodeId` keys and `(NodeId Id, int Epoch)` values
- Persistence snapshots under `EnterAllGates`; Diag/Hardening updated
- 277 tests green; batch hot-path benches show no regression vs pre-migration tip

### Unchanged contract
- Public API same as `1.0.0-rc.1` / `PUBLIC-API.md`
- Wire-compatible with alpha.6/alpha.7 and rc.1

### Next
- Soak as `v1.0.0-rc.2` → promote to **`1.0.0`** without further API breaks

---

## 1.0.0-rc.1 — 2026-10-29

**First release candidate toward 1.0.** API surface frozen as documented in [PUBLIC-API.md](PUBLIC-API.md).

**Target:** `net10.0` · **Package:** `BinSync.Core` `1.0.0-rc.1` · **Branch:** `main`

### Product
- SemVer policy and public API freeze for the 1.x line
- Docs aligned: PRODUCTION-STATUS, PUBLIC-API, README, this changelog

### Engine (carried from post-alpha.7 main)
- Domain sub-gates (`_lockStorage` → `_lockOutbox` → `_lockMeta`) + `EnterAllGates`; alias maps on `NodeId`
- EncodeBatchPayload single-pass + Writer capacity / zero-alloc Varint write
- DecodeBatchPayload in-place records; Reader fixed-width zero-alloc reads
- ApplyBatch decode-once + O(1) change flags; ComputeRecordsRoot single buffer
- Lazy `FieldTombs`; State capacity hint; Blake3 truncated digest without extra 32-byte heap hop
- Zero-alloc FieldWrite framing probe in `Ops`

### Unchanged contract
- Protocol wire compatible with alpha.6/alpha.7
- Trust CA mode remains node-id allow-list (not full X.509)
- Causal / E2EE / schema / signature gates remain **opt-in**

### Next
- Soak as `v1.0.0-rc.1` → promote to **`1.0.0`** without further API breaks

---

## 0.1.0-alpha.7 — 2026-10-28

Packaging / release hygiene cut on `main` (engine same as alpha.6 performance pass).
**Target:** `net10.0` · **Package:** `BinSync.Core` `0.1.0-alpha.7`

### Package
- Version bump `0.1.0-alpha.6` → `0.1.0-alpha.7`
- PRODUCTION-STATUS / release notes aligned with tagged alpha.6 engine tip
- One-shot CI path for annotated tag + GitHub prerelease

### Engine
- No protocol changes vs alpha.6 (comparers, zero-alloc codec, HLC µs remain)

### Package id
```
PackageId: BinSync.Core
Version:   0.1.0-alpha.7
```
Single external dependency: `Blake3.Managed` 1.10.0.

## 0.1.0-alpha.6 — 2026-10-28

Production performance pass on `main` after alpha.5 protocol surface.
**Target:** `net10.0` · **Package:** `BinSync.Core` `0.1.0-alpha.6`

### Engine / hot paths
- Shared comparer singletons; `HashCode.AddBytes`; Codec WriteVarint / stackalloc; HLC µs

## 0.1.0-alpha.5 — 2026-10-24

Production-hardening cut. See prior release notes under `docs/RELEASE-alpha.5.md`.
