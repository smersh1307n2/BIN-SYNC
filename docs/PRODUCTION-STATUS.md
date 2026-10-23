# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7** ([ERRATA-v2.3.7.md](ERRATA-v2.3.7.md)), and **public API freeze** ([PUBLIC-API.md](PUBLIC-API.md)).

**Package:** `BinSync.Core` `0.1.0-alpha.2` · **Target:** `net10.0` · **Branch:** `installing-.net-10-ea6be`  
**Test suite:** **144** passed (xUnit)

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
| Merge policies | §8 | partial | **LWW_FIELD** + **Max/Min bytes & u64** + `MergePolicyRegistry`; MANUAL resolve (`0x00FF`) not wired; registry not auto-hooked into `WriteField` |
| Digests / buckets | §9 | done | |
| Outbox / inbox / cursors | §10 | done | E-3, E-4, E-6 |
| Membership / GC | §11 | done | MEMBERSHIP + GC_NOTICE (quarantine roster + cutoff) |
| Wire frames | §12 | done | Delta + ladder `0x06`…`0x0B` |
| Reconcile ladder | §13 | done | STATE_DIGEST → NEED_RANGE → SNAPSHOT; NEED_KEYS; **bilateral AutoReconcile**; **bilateral IBLT** peel + fulfill |
| E2EE | §14 | missing | |
| Security | §15 | partial | TLS opt-in on TCP; no app-level auth |
| Causal deps | §16.19 | missing | |
| Lab scenarios | §17 | partial | Core scenarios + production tests |
| Schema evolution | §18 | missing | |
| Errata E-1…E-13 | v2.3.7 | done | See [ERRATA-v2.3.7.md](ERRATA-v2.3.7.md) |

---

## Engine / product readiness

| Concern | Status | Notes |
|---------|--------|-------|
| Public API freeze | done (alpha) | [PUBLIC-API.md](PUBLIC-API.md) |
| Network | done | TCP + optional TLS; pipe; loopback |
| Persistence | partial | Full atomic snapshot (`NodePersistence`) + **append-only WAL** (`Wal`); no auto-hook on `WriteField` |
| Merge policies | partial | LWW + Max/Min; explicit policy API; registry optional |
| Memory bounds | partial | `ResourceLimits` / `BoundedNode` |
| Flow control | partial | Present; not on every path |
| CI | done | `.github/workflows/dotnet.yml` (.NET 10) |
| Packaging | partial | `0.1.0-alpha.2` |
| Docs | partial | This file + PUBLIC-API + ERRATA; main Russian body not yet bumped to 2.3.7 |

---

## Implementation notes (normative for 0.1 alpha)

### Reconcile ladder

1. Exchange `STATE_DIGEST` (`0x06`).
2. On divergence: `NEED_RANGE` (`0x07`) → republish under prefix and/or `SNAPSHOT_CHUNK` (`0x0A`).
3. Large set-diff: exchange `IBLT` (`0x09`) → peel → `NEED_KEYS` (`0x08`) → snapshot of missing keys.
4. Bilateral helpers: `BilateralAutoReconcileAsync`, `BilateralReconcileViaIbltAsync` (both peers fill gaps).

### Persistence

- **Snapshot:** domain tag `BIN-SYNC/NODESTATE/v1`, BLAKE3 integrity, temp + fsync + rename.
- **WAL:** magic `BSWL`, version 1; records length-prefixed; `FieldWrite` uses `Bytes()` for values (variable length); identities use `B128` (16 bytes).
- Recovery: `Load(snapshot)` then `Wal.ReplayFile`.

### Merge

- Default path (`Node.WriteField`, delta apply) remains **LWW_FIELD**.
- Max/Min are opt-in via `Merge.MergeFieldWithPolicy` / `ApplyFieldWrite(..., policy)`.
- Equal values under Max/Min fall back to LWW (HLC + author).

---

## Delivery order

### P0 (transport foundation) — complete

TCP session, frame stream, HELLO/ALIAS, delta push/receive, TLS opt-in.

### P1 (reconcile ladder) — complete

STATE_DIGEST, NEED_RANGE, SNAPSHOT, NEED_KEYS, IBLT, MEMBERSHIP, GC_NOTICE, AutoReconcile, bilateral paths.

### P2 (production hardening) — in progress

| # | Item | Status |
|---|------|--------|
| 1 | CI (GitHub Actions, .NET 10) | **done** |
| 2 | Bilateral IBLT | **done** |
| 3 | WAL / incremental persistence | **done** (manual append; no auto-hook) |
| 4 | Extra merge policies (Max/Min) | **done** (explicit API; no WriteField hook) |
| 5 | E2EE | missing |
| 6 | Causal deps (§16.19) | missing |
| 7 | Fuzz / load harness | missing |
| 8 | Wire registry into `WriteField` + auto-WAL | optional follow-ups |

### Later

Schema evolution (§18), packaging 0.2, editorial merge of errata into Russian main body.

---

## Current tip

Branch **`installing-.net-10-ea6be`**.  
Suite: **144** tests, .NET **10.0.x**.
