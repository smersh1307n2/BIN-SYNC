# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7** ([ERRATA-v2.3.7.md](ERRATA-v2.3.7.md)), and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` **`0.1.0-alpha.4`** · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset from ZtSharp) — **no `BouncyCastle.Cryptography` NuGet**.  
**Test suite:** **224+** (CI) + CRDT merge policies

Legend: **done** · **partial** · **missing**

---

## Spec coverage

| Area | Spec | Code | Notes |
|------|------|------|-------|
| Goals / consistency model | §1 | done | |
| Transport | §3 | done | |
| Identity, alias, epoch | §4 | done | |
| HLC | §5 | done | |
| Codec | §6 | **done** | |
| Data model, lifecycle | §7 | done | |
| Merge policies | §8 | **done** | LWW, Max/Min, COUNTER_G/PN, SET_OR, MANUAL |
| Digests | §9 | done | |
| Outbox / inbox | §10 | done | |
| Membership / GC | §11 | done | |
| Wire frames | §12 | done | |
| Reconcile ladder | §13 | done | |
| E2EE | §14 | **done** (opt-in) | |
| Security | §15 | partial | TLS + E2EE + Ed25519 |
| Causal deps | §16.19 | **done** (opt-in) | |
| Lab / fuzz | §17 | partial | |
| Schema evolution | §18 | **done** (opt-in) | |
| Errata E-1…E-13 | v2.3.7 | done | |

---

## Merge (§8)

- LWW_FIELD + echo suppression [E2]
- MaxBytes / MinBytes / MaxU64 / MinU64 (engine 0x81–0x84)
- **COUNTER_G** (0x03) · **COUNTER_PN** (0x04) · **SET_OR** (0x05) · **MANUAL** (0x09)

---

## Current tip

Branch **`installing-.net-10-ea6be`**. .NET **10.0.x**. Package **0.1.0-alpha.4**.

### Next candidates
1. Lab / fuzz expansion §17
2. SEQ_FRACTIONAL / BLOB_REF if needed
