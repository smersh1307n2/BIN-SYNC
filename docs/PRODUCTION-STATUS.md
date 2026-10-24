# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7**, and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` **`0.1.0-alpha.5`** · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset from ZtSharp) — **no `BouncyCastle.Cryptography` NuGet**.  
**Test suite:** **249+** (CI) + lab edge §17

**Estimated remaining to full spec parity:** **~8–12%**.

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
| Merge policies | §8 | **done** | SEQ/BLOB optional |
| Digests | §9 | done | |
| Outbox / inbox | §10 | done | |
| Membership / GC | §11 | done | |
| Wire frames | §12 | done | + FLAG_FRAGMENT reassembly |
| Reconcile ladder | §13 | done | |
| E2EE | §14 | **done** (opt-in) | |
| Security | §15 | partial | |
| Causal deps | §16.19 | **done** (opt-in) | |
| Lab / fuzz | §17 | **done** (core + edge) | S4/S12/S13 covered |
| Schema evolution | §18 | **done** (opt-in) | |
| Errata E-1…E-13 | v2.3.7 | done | |

---

## Lab §17 edge (this cut)

- **S4** snapshot + delta tail for new peer
- **S12** FLAG_FRAGMENT split / out-of-order reassembly
- **S13** GC_NOTICE quarantine + purge below cutoff + OUTDATED_WATERMARK code

---

## Remaining backlog

1. SEQ_FRACTIONAL / BLOB_REF (if product needs)
2. Trust / TOFU polish (§15)
3. Git tag `v0.1.0-alpha.5` + NuGet publish
