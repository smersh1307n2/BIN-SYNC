# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6** and **errata v2.3.7** (implemented in code comments as `SPEC CORRECTION (errata E-n)`).

Legend: **done** · **partial** · **missing**

## Spec coverage

| Area | Spec | Code | Notes |
|---|---|---|---|
| Goals / consistency model | §1 | done | Eventual + pairwise FIFO |
| Identity, alias, epoch | §4 | done | E-1…E-5, E-9…E-11 in Node/Transport |
| HLC | §5 | done | |
| Codec primitives | §6 | partial | Core types; schema evolution limited |
| Data model, lifecycle | §7 | done | LWW field + record tomb + field tomb |
| Merge policies | §8 | partial | LWW_FIELD + lifecycle only |
| Digests / buckets | §9 | partial | Batch chain; full anti-entropy ladder missing |
| Outbox / inbox / cursors | §10 | done | E-3, E-4 stream keys |
| Membership / GC / quarantine | §11 | missing | |
| Wire frames (codec) | §12 | partial | Generic codec + session control payloads |
| HELLO / ALIAS_MAP on wire | §12.7+ | partial | `SessionWire` + TCP/TLS host |
| Reconcile ladder | §13.1–13.3 | missing | STATE_DIGEST, NEED_*, IBLT |
| Apply order [S7] | §13.4 | done | |
| Relay [R3][R4] | §13.6 | done | |
| E2EE | §14 | missing | |
| Security model | §15 | partial | Transport TLS; no app-level auth |
| Causal deps | §16.19 | missing | |
| Lab scenarios 1–42 | §17 | partial | In-memory + TCP/TLS loopback |
| Schema evolution | §18 | missing | |

## Engine / product readiness

| Concern | Status | Notes |
|---|---|---|
| Public API surface | partial | `ISyncTransport`, `SessionWire`, `TcpSyncSession`, `BoundedNode` |
| Network transports | partial | TCP + optional TLS (`SslStream`); no mutual TLS yet |
| Persistence | partial | Snapshot OK; no WAL |
| Memory bounds | partial | `ResourceLimits` / `BoundedNode` (opt-in) |
| Flow control | partial | Class exists; not fully on send path |
| Packaging | partial | NuGet metadata on `BinSync.Core` |
| CI | done | `.github/workflows/dotnet.yml` |
| Docs | partial | README + this matrix |

## Delivery order

### P0 (in progress)
1. ~~Docs / CI / status matrix~~
2. ~~ISyncTransport + session wire + stream adapter~~
3. ~~ResourceLimits (opt-in)~~
4. ~~Package metadata 0.1.0-alpha.1~~
5. ~~TCP accept/connect + handshake + one delta~~
6. ~~TLS wrapper (optional `SslStream`, lab cert helper)~~
7. **Next:** fold errata into normative spec text, freeze public API.

### P1
STATE_DIGEST / NEED_* / SNAPSHOT, membership, extra merge policies.

### P2
E2EE, IBLT, causalDeps, fuzz/load.

## Current tip

Branch `installing-.net-10-ea6be`. Suite: **103** tests (.NET 10).
