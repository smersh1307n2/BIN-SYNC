# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7**, and **public API freeze** (`docs/PUBLIC-API.md`).

Legend: **done** · **partial** · **missing**

## Spec coverage

| Area | Spec | Code | Notes |
|---|---|---|---|
| Goals / consistency model | §1 | done | Eventual + pairwise FIFO |
| Identity, alias, epoch | §4 | done | E-1…E-5, E-9…E-11 |
| HLC | §5 | done | |
| Codec primitives | §6 | partial | Core types; schema evolution limited |
| Data model, lifecycle | §7 | done | LWW field + record tomb + field tomb |
| Merge policies | §8 | partial | LWW_FIELD + lifecycle; E-8/E-13 |
| Digests / buckets | §9 | done | Record/bucket/StateRoot + wire STATE_DIGEST |
| Outbox / inbox / cursors | §10 | done | E-3, E-4 |
| Membership | §11 | done | MEMBERSHIP_DELTA + `ExchangeMembershipAsync` |
| GC / quarantine | §11 | done | GC_NOTICE `0x0B`, `PurgeBelowCutoff` |
| Wire frames (codec) | §12 | done | DELTA + HELLO/ALIAS + ladder + GC |
| HELLO / ALIAS_MAP on wire | §12.7+ | done | SessionWire + TCP/TLS |
| Reconcile ladder | §13.1–13.3 | done | STATE_DIGEST … SNAPSHOT + IBLT |
| Auto / bilateral reconcile | §13 | done | Digest + **bilateral IBLT** |
| Apply order [S7] | §13.4 | done | |
| Relay [R3][R4] | §13.6 | done | |
| E2EE | §14 | missing | |
| Security model | §15 | partial | Transport TLS (opt-in) |
| Causal deps | §16.19 | missing | |
| Lab scenarios 1–42 | §17 | partial | + production suite |
| Schema evolution | §18 | missing | |
| Normative errata E-1…E-13 | v2.3.7 | done | |
| Public API freeze | — | done | 0.1.0-alpha.2 |

## Engine / product readiness

| Concern | Status | Notes |
|---|---|---|
| Public API surface | done (alpha freeze) | `docs/PUBLIC-API.md` |
| Network transports | done | TCP + optional TLS |
| Persistence | partial | Snapshot OK; no WAL |
| Memory bounds | partial | ResourceLimits opt-in |
| Flow control | partial | Not on every send path |
| Packaging | partial | 0.1.0-alpha.2 |
| CI | done | `.github/workflows/dotnet.yml` |
| Docs | partial | STATUS + PUBLIC-API + ERRATA |

## Delivery order

### P0 + P1 — complete

### P2 — in progress
1. ~~GitHub Actions CI~~  
2. ~~Bilateral IBLT~~ (`BilateralReconcileViaIbltAsync`)  
3. WAL / incremental persistence  
4. Extra merge policies  
5. E2EE, causalDeps, fuzz/load  

## Current tip

Branch `installing-.net-10-ea6be`. Suite: **132** tests (.NET 10). Package **0.1.0-alpha.2**.
