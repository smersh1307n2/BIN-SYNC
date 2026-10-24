# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7** ([ERRATA-v2.3.7.md](ERRATA-v2.3.7.md)), and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` **`0.1.0-alpha.3`** · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset from ZtSharp) — **no `BouncyCastle.Cryptography` NuGet**.  
**Test suite:** **200+** (CI) + schema evolution

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
| Wire frames | §12 | done | SCHEMA_MAP 0x04 |
| Reconcile ladder | §13 | done | |
| E2EE | §14 | **done** (opt-in) | |
| Security | §15 | partial | TLS + E2EE + Ed25519 |
| Causal deps | §16.19 | **done** (opt-in) | |
| Lab / fuzz | §17 | partial | |
| Schema evolution | §18 | **done** (opt-in) | Registry + SCHEMA_MAP + apply gate |
| Errata E-1…E-13 | v2.3.7 | done | |

---

## Delivery order

### P0 + P1 + P2 — complete (opt-in surfaces where noted)

### Schema (§18)

- `SchemaRegistry` — versioned descriptors, field-id non-reuse, policy binding
- `SchemaMap` — frame 0x04 encode/decode
- `Node.ApplyBatchWithSchema` / `RegisterSchema` / `LearnSchemaMap`
- `TcpSync.ExchangeSchemaMapAsync`

---

## Current tip

Branch **`installing-.net-10-ea6be`**. .NET **10.0.x**. Package **0.1.0-alpha.3**.

### Next candidates
1. Codec completeness (§6)
2. Package release cut (alpha.4)
