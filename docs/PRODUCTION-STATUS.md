# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7** ([ERRATA-v2.3.7.md](ERRATA-v2.3.7.md)), and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` `0.1.0-alpha.2` · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Test suite:** **159+** (prior tip) + E2ee unit tests (see CI)

Legend: **done** · **partial** · **missing**

---

## Spec coverage

| Area | Spec | Code | Notes |
|------|------|------|-------|
| Goals / consistency model | §1 | done | |
| Transport | §3 | done | |
| Identity, alias, epoch | §4 | done | |
| HLC | §5 | done | |
| Codec | §6 | partial | |
| Data model, lifecycle | §7 | done | |
| Merge policies | §8 | partial | LWW + Max/Min |
| Digests | §9 | done | |
| Outbox / inbox | §10 | done | |
| Membership / GC | §11 | done | |
| Wire frames | §12 | done | |
| Reconcile ladder | §13 | done | |
| E2EE | §14 | partial | **`E2ee`**: nonce KDF [E1] + ChaCha20-Poly1305 seal/open; blind LWW on ciphertext; **no** KEY_ROTATION frame, no auto-hook on WriteField, no Ed25519 batch sign path yet |
| Security | §15 | partial | TLS + E2EE primitives |
| Causal deps | §16.19 | partial | CausalTracker KЗ1–5 |
| Lab / fuzz | §17 | partial | |
| Schema evolution | §18 | missing | |
| Errata E-1…E-13 | v2.3.7 | done | |

---

## Delivery order

### P0 + P1 — complete

### P2 — in progress

| # | Item | Status |
|---|------|--------|
| 1–5 | CI, bilateral IBLT, WAL, merge, fuzz | **done** |
| 6 | Causal deps | **partial** |
| 7 | E2EE | **partial** (crypto foundation §14.5) |
| 8 | KEY_ROTATION / batch Ed25519 / auto-hooks | missing |

---

## Current tip

Branch **`installing-.net-10-ea6be`**. .NET **10.0.x**.
