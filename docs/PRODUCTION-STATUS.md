# Production status — BinSync.Net vs BIN-SYNC CORE

**Product name:** **BIN-SYNC** — see [NAME.md](NAME.md).

**Package:** `BinSync.Core` **`0.1.0-alpha.7`** · **Target:** `net10.0` · **Branch:** `main`  
**Tag:** `v0.1.0-alpha.7` (see GitHub Releases)  
**Crypto:** vendored Ed25519 (no BouncyCastle NuGet)  
**Tests:** green (user-confirmed) · **CI:** `test` + dedicated `fuzz + lab (§17)` job

**Spec parity:** **~95%+** — reference engine production-hardened.

Legend: **done** · **partial** · **missing**

---

## Spec coverage

| Area | Spec | Code |
|------|------|------|
| Goals / consistency | §1 | done |
| Transport | §3 | done |
| Identity / alias / epoch | §4 | done |
| HLC | §5 | done (µs wall) |
| Codec | §6 | **done** (zero-alloc hot paths) |
| Data model / lifecycle | §7 | done |
| Merge policies | §8 | **done** (full) |
| Digests | §9 | done |
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

## Engine notes (alpha.6 → alpha.7)

- Shared comparer singletons (`BA.Instance`, …) on all hot maps/sets
- `HashCode.AddBytes` for byte[] keys
- Codec `WriteVarint` / stackalloc fixed-width writes
- No throwaway `new BA()` on Node / Transport / Merge / Persistence paths
- alpha.7 = package version + release hygiene; protocol unchanged from alpha.6

---

## Trust model (§15.6) vs “X.509”

Spec defines **three** trust modes:

1. **TOFU** — first seen Ed25519 pubkey for `nodeId` is pinned
2. **PreShared** — explicit allow-list of `(nodeId → pubkey)`
3. **CertificateAuthority** — “центр сертификации”

In code today, mode 3 is a **lightweight CA allow-list** (`TrustStore.AddCaRoot(nodeId)`): roots are **node IDs**, not parsed X.509 certificates. There is **no** DER/PEM certificate parse, chain building, or RFC 5280 path validation.

**“Full X.509 path”** (optional next) would mean: accept a peer’s X.509 leaf (or cert chain) in the auth payload, verify it against configured CA roots (true X.509), and bind the verified identity to the Ed25519 signing key used for `BatchSignature`. That is enterprise PKI interop — not required for TOFU/PreShared deployments.

---

## Release checklist

- [x] BouncyCastle removed / Ed25519 vendored
- [x] Merge §8 complete set
- [x] Lab edge S4/S12/S13
- [x] TrustStore §15.6 (TOFU / PreShared / CA-as-nodeId-root / revoke)
- [x] License AGPL + commercial (crypto MIT)
- [x] Branding BIN-SYNC; repo `smersh1307n2/BIN-SYNC`
- [x] Production comparer / codec pass
- [x] Git tag `v0.1.0-alpha.5`
- [x] Git tag `v0.1.0-alpha.6` + GitHub prerelease
- [x] Git tag `v0.1.0-alpha.7` + GitHub prerelease
- [x] Adversarial / lab fuzz surface in CI (`fuzz` job + nightly schedule)
- [ ] nuget.org push
- [ ] Full X.509 certificate path (optional; see above)
