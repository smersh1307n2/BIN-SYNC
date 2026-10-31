# Production status — BinSync.Net vs BIN-SYNC CORE

**Product name:** **BIN-SYNC** — see [NAME.md](NAME.md).

**Package:** `BinSync.Core` **`1.0.1`** · **Target:** `net10.0` · **Branch:** `main`  
**Tag:** `v1.0.1` (see GitHub Releases)  
**Crypto:** vendored Ed25519 (no BouncyCastle NuGet)  
**Tests:** 274 green · **CI:** `test` + dedicated `fuzz + lab (§17)` job  

**Spec parity:** **~95%+** — reference engine production-hardened.  
**Path:** alpha.7 → **1.0.0-rc.1** → **1.0.0-rc.2** → **1.0.0** → **1.0.1** (patch).

Legend: **done** · **partial** · **missing**

---

## Post-1.0 hygiene (main)

- Node engine split into partials (`Node.Alias` / `LocalWrites` / `DeltaBatch` / `ApplyBatch` / …)
- Test warnings-as-errors; dead Diag harness and duplicate comparers removed
- ApplyBatch: COW overlays + single DecodeOp pass (no public API change)
- **CI soak:** `BinSync.Net` workflow green on recent `main` tips (`test` + `fuzz + lab`)


## Spec coverage

| Area | Spec | Code |
|------|------|------|
| Goals / consistency | §1 | done |
| Transport | §3 | done |
| Identity / alias / epoch | §4 | done |
| HLC | §5 | done (µs wall) |
| Codec | §6 | **done** (capacity Writer, zero-alloc Reader fixed-width / Varint write) |
| Data model / lifecycle | §7 | done (lazy FieldTombs) |
| Merge policies | §8 | **done** (full) |
| Digests | §9 | done (span Blake3 truncate path) |
| Outbox / inbox | §10 | done |
| Membership / GC | §11 | done |
| Wire frames | §12 | done (+ fragments) |
| Reconcile ladder | §13 | done |
| E2EE | §14 | **done** (opt-in) |
| Security | §15 | **done** (TOFU/PreShared/revoke; CA = node-id allow-list, not full X.509 PKI) |
| Causal deps | §16.19 | **done** (opt-in) |
| Lab / fuzz | §17 | **done** (in CI job) |
| Schema evolution | §18 | **done** (opt-in) |
| Errata E-1…E-13 | v2.3.7 | done |

---

## Hot-path notes (on main since post-alpha.7)

- Domain sub-gates (`Storage` → `Outbox` → `Meta`) + `EnterAllGates`; alias maps keyed by `NodeId`
- EncodeBatchPayload: single-pass records, Writer capacity
- DecodeBatchPayload: in-place records parse
- ApplyBatch: decode-once, O(1) changed flags, ComputeRecordsRoot single buffer
- Reader U8/U16/U32/U64 without per-read heap Take
- Ops FieldWrite framing probe zero-alloc

---

## Trust model (§15.6) vs “X.509”

1. **TOFU** — first seen Ed25519 pubkey for `nodeId` is pinned  
2. **PreShared** — explicit allow-list of `(nodeId → pubkey)`  
3. **CertificateAuthority** — lightweight CA allow-list (`TrustStore.AddCaRoot(nodeId)`): **node IDs**, not X.509 DER/chain  

Full X.509 path validation remains **out of scope for 1.0**.

---

## Release checklist

- [x] BouncyCastle removed / Ed25519 vendored
- [x] Merge §8 complete set
- [x] Lab / fuzz in CI
- [x] TrustStore §15.6 (TOFU / PreShared / CA-as-nodeId / revoke)
- [x] License AGPL + commercial (crypto MIT)
- [x] Branding BIN-SYNC
- [x] Production comparer / codec / apply-decode hot paths on `main`
- [x] Public API freeze doc for 1.0 (`PUBLIC-API.md`)
- [x] Package version `1.0.0` (superseded by 1.0.1 on main)
- [x] Tag `v1.0.0-rc.1` + GitHub prerelease
- [x] Tag `v1.0.0-rc.2` + GitHub prerelease (domain gates + NodeId aliases)
- [x] Soak / embedder feedback on rc.2 (274 tests + CI BinSync.Net green)
- [x] Tag `v1.0.0` (stable)
- [x] Package version `1.0.1` + tag `v1.0.1` (patch: hygiene + CI coverage + sample)
- [ ] nuget.org push (optional)
- [ ] Full X.509 (post-1.0, optional)
