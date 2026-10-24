# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7** ([ERRATA-v2.3.7.md](ERRATA-v2.3.7.md)), and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` **`0.1.0-alpha.3`** · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset from ZtSharp `Crypto/External`) — **no `BouncyCastle.Cryptography` NuGet**.  
**Test suite:** **180+** (CI) + KeyRotation / KeyEpochStore

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
| E2EE | §14 | partial | ChaCha20-Poly1305 + batch Ed25519 + KEY_ROTATION |
| Security | §15 | partial | TLS + E2EE + Ed25519 |
| Causal deps | §16.19 | partial | CausalTracker KЗ1–5 |
| Lab / fuzz | §17 | partial | |
| Schema evolution | §18 | missing | |
| Errata E-1…E-13 | v2.3.7 | done | |

---

## Delivery order

### P0 + P1 — complete

### P2 — nearly complete

| # | Item | Status |
|---|------|--------|
| 1–5 | CI, bilateral IBLT, WAL, merge, fuzz | **done** |
| 6 | Causal deps | **partial** |
| 7 | E2EE | **partial** (crypto foundation; no auto WriteField hook) |
| 8 | KEY_ROTATION / batch Ed25519 | **done** — `BatchSignature`, vendored Ed25519, `KeyRotation`, `KeyEpochStore`, TcpSync send/receive |

---

## Crypto (no BouncyCastle package)

- **Source:** `bash scripts/vendor-ed25519-from-ztsharp.sh` (CI runs before build)
- **API:** `BinSync.Crypto.Ed25519` · `BatchSignature` · `KeyRotation` · `KeyEpochStore`
- **Only NuGet crypto dep:** `Blake3.Managed`

---

## Current tip

Branch **`installing-.net-10-ea6be`**. .NET **10.0.x**. Package **0.1.0-alpha.3**.

### Next candidates
1. Causal deps: close remaining gaps vs §16.19
2. E2EE optional auto-seal on WriteField (opt-in)
3. Schema evolution §18
