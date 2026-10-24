# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7** ([ERRATA-v2.3.7.md](ERRATA-v2.3.7.md)), and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` **`0.1.0-alpha.4`** · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset from ZtSharp) — **no `BouncyCastle.Cryptography` NuGet**.  
**Test suite:** **233+** (CI) + lab/fuzz §17

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
| Lab / fuzz | §17 | **done** (core) | S1/S2/S6–S8/S15/S18/S19/S26 + multi-node fuzz |
| Schema evolution | §18 | **done** (opt-in) | |
| Errata E-1…E-13 | v2.3.7 | done | |

---

## Lab §17 coverage (this cut)

- Field parallelism, delete-wins, clock skew, epoch regression
- Canonical digest, duplicate batch, MANUAL conflict, CRC corruption
- Flow control, CounterG/SetOr convergence
- Randomized 3-node mesh convergence (5 seeds)

---

## Current tip

Branch **`installing-.net-10-ea6be`**. .NET **10.0.x**. Package **0.1.0-alpha.4**.

### Next candidates
1. Expand remaining §17 scenarios (snapshot, fragmentation, quarantine)
2. Package release notes / alpha.5
