# Production status — BinSync.Net vs BIN-SYNC CORE

**Product name:** **BIN-SYNC** — see [NAME.md](NAME.md).

**Package:** `BinSync.Core` **`0.1.0-alpha.7`** · **Target:** `net10.0` · **Branch:** `main`  
**Tag:** `v0.1.0-alpha.7` (see GitHub Releases)  
**Crypto:** vendored Ed25519 (no BouncyCastle NuGet)  
**Tests:** green (user-confirmed)

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
| Security | §15 | **done** (TOFU/PreShared/revoke) |
| Causal deps | §16.19 | **done** (opt-in) |
| Lab / fuzz | §17 | **done** |
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

## Release checklist

- [x] BouncyCastle removed / Ed25519 vendored
- [x] Merge §8 complete set
- [x] Lab edge S4/S12/S13
- [x] TrustStore §15.6
- [x] License AGPL + commercial (crypto MIT)
- [x] Branding BIN-SYNC; repo `smersh1307n2/BIN-SYNC`
- [x] Production comparer / codec pass
- [x] Git tag `v0.1.0-alpha.5`
- [x] Git tag `v0.1.0-alpha.6` + GitHub prerelease
- [x] Git tag `v0.1.0-alpha.7` + GitHub prerelease
- [ ] nuget.org push
- [ ] Full X.509 path + adversarial fuzz in CI (optional next)
