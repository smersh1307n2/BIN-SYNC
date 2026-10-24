# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7** ([ERRATA-v2.3.7.md](ERRATA-v2.3.7.md)), and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` **`0.1.0-alpha.3`** · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset from ZtSharp) — **no `BouncyCastle.Cryptography` NuGet**.  
**Test suite:** **187+** (CI) + causal wire / KeyRotation

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
| E2EE | §14 | partial | ChaCha20-Poly1305 + Ed25519 + KEY_ROTATION |
| Security | §15 | partial | TLS + E2EE + Ed25519 |
| Causal deps | §16.19 | **done** (opt-in) | KЗ1–5, `CausalWire` trailer, `DrainAndRetryCausal` |
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
| 6 | Causal deps | **done** (opt-in `Node.Causal` + wire trailer) |
| 7 | E2EE | **partial** (no auto WriteField seal) |
| 8 | KEY_ROTATION / batch Ed25519 | **done** |

---

## Causal (§16.19)

- `CausalTracker` — KЗ1 timeout, KЗ2 cycle, KЗ3 lost, KЗ4 quarantine, KЗ5 batch all-or-nothing
- `Node.ApplyBatchCausal` / `DrainAndRetryCausal` / `NoteCausalApplied`
- `CausalWire` — optional trailer after standard batch payload (magic `CCD1`), does not change recordsRoot

---

## Current tip

Branch **`installing-.net-10-ea6be`**. .NET **10.0.x**. Package **0.1.0-alpha.3**.

### Next candidates
1. E2EE optional auto-seal on WriteField (opt-in)
2. Schema evolution §18
