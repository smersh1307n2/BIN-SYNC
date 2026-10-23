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
| Merge policies | §8 | partial | LWW_FIELD + **Max/Min bytes & u64** + registry; MANUAL resolve field not wired |
| Digests / buckets | §9 | done | |
| Outbox / inbox / cursors | §10 | done | |
| Membership / GC | §11 | done | |
| Wire frames | §12 | done | ladder `0x06`…`0x0B` |
| Reconcile ladder | §13 | done | + bilateral IBLT |
| E2EE | §14 | missing | |
| Security | §15 | partial | TLS opt-in |
| Causal deps | §16.19 | missing | |
| Lab scenarios | §17 | partial | |
| Schema evolution | §18 | missing | |
| Errata E-1…E-13 | v2.3.7 | done | |

## Engine / product readiness

| Concern | Status | Notes |
|---|---|---|
| Public API | done (alpha) | |
| Network | done | TCP + TLS |
| Persistence | partial | Snapshot + WAL |
| Merge policies | partial | LWW + Max/Min; registry not hooked into Node.WriteField yet |
| Memory bounds | partial | ResourceLimits |
| CI | done | |
| Packaging | partial | 0.1.0-alpha.2 |

## Delivery order

### P0 + P1 — complete

### P2 — in progress
1. ~~CI~~  
2. ~~Bilateral IBLT~~  
3. ~~WAL~~  
4. ~~Extra merge policies (Max/Min)~~  
5. E2EE, causalDeps, fuzz/load  

## Current tip

Branch `installing-.net-10-ea6be`. Suite: **144** tests (.NET 10).
