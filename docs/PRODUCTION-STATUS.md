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
| Membership / GC / quarantine | §11 | missing | |
| Wire frames (codec) | §12 | partial | + STATE_DIGEST / NEED_* payloads |
| HELLO / ALIAS_MAP on wire | §12.7+ | partial | SessionWire + TCP/TLS |
| Reconcile ladder | §13.1–13.3 | partial | `Reconcile` build/diff/NEED_*; no IBLT/SNAPSHOT yet |
| Apply order [S7] | §13.4 | done | |
| Relay [R3][R4] | §13.6 | done | |
| E2EE | §14 | missing | |
| Security model | §15 | partial | Transport TLS |
| Causal deps | §16.19 | missing | |
| Lab scenarios 1–42 | §17 | partial | + reconcile unit tests |
| Schema evolution | §18 | missing | |
| Normative errata E-1…E-13 | v2.3.7 | done | |
| Public API freeze | — | done | 0.1.0-alpha.2 |

## Engine / product readiness

| Concern | Status | Notes |
|---|---|---|
| Public API surface | done (alpha freeze) | `docs/PUBLIC-API.md` |
| Network transports | partial | TCP + optional TLS |
| Persistence | partial | Snapshot OK; no WAL |
| Memory bounds | partial | ResourceLimits opt-in |
| Flow control | partial | |
| Packaging | partial | 0.1.0-alpha.2 |
| CI | done | |
| Docs | partial | |

## Delivery order

### P0 — complete (except editorial body-merge of errata into Russian spec)

### P1 (in progress)
1. ~~STATE_DIGEST build + encode/decode + DivergentPrefixes + NEED_* codecs~~
2. **Next:** wire STATE_DIGEST exchange on `TcpSyncSession` / `AsyncTransport`; SNAPSHOT_CHUNK; membership
3. Extra merge policies

### P2
E2EE, IBLT, causalDeps, fuzz/load.

## Current tip

Branch `installing-.net-10-ea6be`. Suite: **108** tests (.NET 10). Package **0.1.0-alpha.2**.
