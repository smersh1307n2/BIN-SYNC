# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7**, and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` **`0.1.0-alpha.5`** · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Crypto:** vendored RFC 8032 Ed25519 — **no BouncyCastle.Cryptography NuGet**.  
**Tests:** **265+** (CI) + TrustStore

**Estimated remaining:** **~3–5%** (git tag, NuGet publish, CA cert path depth).

Legend: **done** · **partial** · **missing**

---

## Spec coverage

| Area | Spec | Code | Notes |
|------|------|------|-------|
| Goals / consistency | §1 | done | |
| Transport | §3 | done | |
| Identity / alias / epoch | §4 | done | |
| HLC | §5 | done | |
| Codec | §6 | **done** | |
| Data model / lifecycle | §7 | done | |
| Merge policies | §8 | **done** | full set |
| Digests | §9 | done | |
| Outbox / inbox | §10 | done | |
| Membership / GC | §11 | done | |
| Wire frames | §12 | done | |
| Reconcile ladder | §13 | done | |
| E2EE | §14 | **done** (opt-in) | |
| Security | §15 | **done** (core) | TOFU + PreShared + revoke + CA roots |
| Causal deps | §16.19 | **done** (opt-in) | |
| Lab / fuzz | §17 | **done** | |
| Schema evolution | §18 | **done** (opt-in) | |
| Errata E-1…E-13 | v2.3.7 | done | |

---

## Security §15.6

- `TrustStore` — TOFU pin, PreShared allow-list, CA root set, revoke
- `Node.TrustPeer` / `VerifyAndApplyBatch` / `RevokeTrust`
- Codes: `TRUST_REVOKED` (0x0014), `SIGNATURE_INVALID` (0x0013)

---

## Remaining backlog

1. Git tag `v0.1.0-alpha.5` (GitHub Releases UI)
2. NuGet.org publish
3. Optional: full X.509 CA path validation
