# Production status — BinSync.Net vs BIN-SYNC CORE

**Product name:** **BIN-SYNC** — see [NAME.md](NAME.md).

**Package:** `BinSync.Core` **`0.1.0-alpha.6`** · **Target:** `net10.0` · **Branch:** `main`  
**Tip:** `661666e` (2026-10-28)  
**Crypto:** vendored Ed25519 (no BouncyCastle NuGet)  
**Tests:** green on local / CI (user-confirmed for alpha.6 cut)

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

## alpha.6 engine notes

- Shared comparer singletons (`BA.Instance`, …) on all hot maps/sets
- `HashCode.AddBytes` for byte[] keys
- Codec `WriteVarint` / stackalloc fixed-width writes
- No allocation of throwaway `new BA()` on Node / Transport / Merge / Persistence paths

---

## Release checklist

- [x] BouncyCastle removed / Ed25519 vendored
- [x] Merge §8 complete set
- [x] Lab edge S4/S12/S13
- [x] TrustStore §15.6
- [x] License AGPL + commercial (crypto MIT)
- [x] Branding BIN-SYNC; repo `smersh1307n2/BIN-SYNC`
- [x] Production comparer / codec pass (alpha.6)
- [x] Git tag `v0.1.0-alpha.5`
- [ ] Git tag `v0.1.0-alpha.6`
- [ ] nuget.org push
- [ ] Full X.509 path + adversarial fuzz in CI (optional next)
