# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7**, and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` **`0.1.0-alpha.5`** · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset from ZtSharp) — **no `BouncyCastle.Cryptography` NuGet**.  
**Test suite:** **249** passed (CI)

**Estimated remaining to full spec parity:** **~10–15%** (lab edge cases, SEQ/BLOB policies, trust UX, publish automation).

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
| Merge policies | §8 | **done** | LWW + CRDT set; SEQ/BLOB optional |
| Digests | §9 | done | |
| Outbox / inbox | §10 | done | |
| Membership / GC | §11 | done | |
| Wire frames | §12 | done | |
| Reconcile ladder | §13 | done | |
| E2EE | §14 | **done** (opt-in) | |
| Security | §15 | partial | TLS + E2EE + Ed25519 |
| Causal deps | §16.19 | **done** (opt-in) | |
| Lab / fuzz | §17 | **done** (core) | edge matrix open |
| Schema evolution | §18 | **done** (opt-in) | |
| Errata E-1…E-13 | v2.3.7 | done | |

---

## Release **0.1.0-alpha.5**

See [CHANGELOG.md](CHANGELOG.md).

### Remaining backlog (priority)
1. Lab §17 edge: snapshot+tail, fragmentation, quarantine deep paths
2. SEQ_FRACTIONAL / BLOB_REF if product needs them
3. Trust / TOFU polish (§15)
4. NuGet publish + tag `v0.1.0-alpha.5`
