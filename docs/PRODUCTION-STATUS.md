# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7**, and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` **`0.1.0-alpha.5`** · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Tip:** `e60e1cc` (docs) / engine tip `a12bc30` · **Crypto:** vendored Ed25519 (no BouncyCastle NuGet)  
**Tests:** **273** passed (CI)

**Spec parity:** **~95%+** — reference engine production-hardened.

Legend: **done** · **partial** · **missing**

---

## Spec coverage

| Area | Spec | Code |
|------|------|------|
| Goals / consistency | §1 | done |
| Transport | §3 | done |
| Identity / alias / epoch | §4 | done |
| HLC | §5 | done |
| Codec | §6 | **done** |
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

## Release checklist

- [x] BouncyCastle removed / Ed25519 vendored
- [x] Merge §8 complete set
- [x] Lab edge S4/S12/S13
- [x] TrustStore §15.6
- [x] README + PUBLIC-API aligned to alpha.5
- [x] 273 tests green
- [x] `scripts/pack-nupkg.sh`
- [ ] Git tag `v0.1.0-alpha.5` (GitHub Releases UI — requires write access)
- [ ] nuget.org push (requires API key)

```bash
git tag -a v0.1.0-alpha.5 a12bc30bd1747ab8c607087a8128160c8efc4665 -m "BinSync.Core 0.1.0-alpha.5"
git push origin v0.1.0-alpha.5
bash scripts/pack-nupkg.sh
```
