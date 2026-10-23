# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7** ([ERRATA-v2.3.7.md](ERRATA-v2.3.7.md)), and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` `0.1.0-alpha.2` · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Test suite:** **149** passed (xUnit)

Legend: **done** · **partial** · **missing**

---

## Spec coverage

| Area | Spec | Code | Notes |
|------|------|------|-------|
| Goals / consistency model | §1 | done | Eventual consistency + pairwise FIFO |
| Transport abstraction | §3 | done | `ISyncTransport`, TCP, pipe, loopback |
| Identity, alias, epoch | §4 | done | E-1…E-5, E-9…E-11 |
| HLC | §5 | done | |
| Codec primitives | §6 | partial | Core types OK; schema evolution limited |
| Data model, lifecycle | §7 | done | LWW field + record tomb + field tomb + ghosts |
| Merge policies | §8 | partial | LWW_FIELD + Max/Min bytes & u64 + registry; MANUAL resolve not wired |
| Digests / buckets | §9 | done | |
| Outbox / inbox / cursors | §10 | done | |
| Membership / GC | §11 | done | |
| Wire frames | §12 | done | ladder `0x06`…`0x0B` |
| Reconcile ladder | §13 | done | + bilateral AutoReconcile + bilateral IBLT |
| E2EE | §14 | missing | |
| Security | §15 | partial | TLS opt-in |
| Causal deps | §16.19 | missing | |
| Lab scenarios | §17 | partial | + deterministic fuzz harness |
| Schema evolution | §18 | missing | |
| Errata E-1…E-13 | v2.3.7 | done | |

---

## Engine / product readiness

| Concern | Status | Notes |
|---------|--------|-------|
| Public API freeze | done (alpha) | |
| Network | done | TCP + TLS |
| Persistence | partial | Snapshot + WAL (manual append) |
| Merge policies | partial | LWW + Max/Min explicit API |
| Fuzz / stress | partial | Seeded PRNG tests: MaxU64/LWW/MinBytes converge, WAL replay, codec round-trip |
| Memory bounds | partial | ResourceLimits |
| CI | done | |
| Packaging | partial | 0.1.0-alpha.2 |

---

## Delivery order

### P0 + P1 — complete

### P2 — in progress

| # | Item | Status |
|---|------|--------|
| 1 | CI | **done** |
| 2 | Bilateral IBLT | **done** |
| 3 | WAL | **done** |
| 4 | Extra merge policies | **done** |
| 5 | Fuzz harness | **done** (deterministic; not external AFL) |
| 6 | E2EE | missing |
| 7 | Causal deps (§16.19) | missing |
| 8 | Auto-hook registry / WAL into WriteField | optional |

---

## Current tip

Branch **`installing-.net-10-ea6be`**. Suite: **149** tests, .NET **10.0.x**.
