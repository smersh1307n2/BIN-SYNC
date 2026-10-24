# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7**, and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` **`0.1.0-alpha.5`** · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset from ZtSharp) — **no `BouncyCastle.Cryptography` NuGet**.  
**Tests:** **250+** (CI)

**Estimated remaining to full spec parity:** **~5–8%** (trust UX, NuGet publish, tag).

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
| Merge policies | §8 | **done** | LWW, CRDT, SEQ_FRACTIONAL, BLOB_REF, MANUAL |
| Digests | §9 | done | |
| Outbox / inbox | §10 | done | |
| Membership / GC | §11 | done | |
| Wire frames | §12 | done | + FLAG_FRAGMENT |
| Reconcile ladder | §13 | done | |
| E2EE | §14 | **done** (opt-in) | |
| Security | §15 | partial | TLS + E2EE + Ed25519 |
| Causal deps | §16.19 | **done** (opt-in) | |
| Lab / fuzz | §17 | **done** (core + edge) | |
| Schema evolution | §18 | **done** (opt-in) | |
| Errata E-1…E-13 | v2.3.7 | done | |

---

## Merge §8 complete set

| Code | Policy |
|------|--------|
| 0x02 / 0 | LWW_FIELD |
| 0x03 | COUNTER_G |
| 0x04 | COUNTER_PN |
| 0x05 | SET_OR |
| 0x06 | SEQ_FRACTIONAL |
| 0x08 | BLOB_REF |
| 0x09 | MANUAL |
| 0x81–0x84 | Max/Min bytes/u64 |

---

## Remaining backlog

1. Trust / TOFU polish (§15)
2. Git tag `v0.1.0-alpha.5` (create via `git tag` locally or GitHub Releases UI)
3. NuGet.org publish
