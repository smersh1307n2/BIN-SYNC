# Production status — BinSync.Net vs BIN-SYNC CORE

Tracked against **BYN-SYNC-SPECIFICATION.txt v2.3.6**, **addendum v2.3.7** ([`BYN-SYNC-v2.3.7-ADDENDUM.txt`](../BYN-SYNC-v2.3.7-ADDENDUM.txt), [`docs/ERRATA-v2.3.7.md`](ERRATA-v2.3.7.md)), and **public API freeze** ([`docs/PUBLIC-API.md`](PUBLIC-API.md)).

Legend: **done** · **partial** · **missing**

## Spec coverage

| Area | Spec | Code | Notes |
|---|---|---|---|
| Goals / consistency model | §1 | done | Eventual + pairwise FIFO |
| Identity, alias, epoch | §4 | done | E-1…E-5, E-9…E-11 |
| HLC | §5 | done | |
| Codec primitives | §6 | partial | Core types; schema evolution limited |
| Data model, lifecycle | §7 | done | LWW field + record tomb + field tomb |
| Merge policies | §8 | partial | LWW_FIELD + lifecycle; E-8/E-13 echo rules |
| Digests / buckets | §9 | partial | Batch chain; full anti-entropy ladder missing |
| Outbox / inbox / cursors | §10 | done | E-3, E-4 stream keys |
| Membership / GC / quarantine | §11 | missing | |
| Wire frames (codec) | §12 | partial | Generic codec + session control payloads |
| HELLO / ALIAS_MAP on wire | §12.7+ | partial | `SessionWire` + TCP/TLS host |
| Reconcile ladder | §13.1–13.3 | missing | STATE_DIGEST, NEED_*, IBLT |
| Apply order [S7] | §13.4 | done | |
| Relay [R3][R4] | §13.6 | done | E-6, E-7 |
| E2EE | §14 | missing | |
| Security model | §15 | partial | Transport TLS; no app-level auth |
| Causal deps | §16.19 | missing | |
| Lab scenarios 1–42 | §17 | partial | In-memory + TCP/TLS loopback |
| Schema evolution | §18 | missing | |
| Normative errata E-1…E-13 | v2.3.7 | done | Addendum + ERRATA doc |
| Public API freeze | — | done | `docs/PUBLIC-API.md`, package `0.1.0-alpha.2` |

## Engine / product readiness

| Concern | Status | Notes |
|---|---|---|
| Public API surface | done (alpha freeze) | See `docs/PUBLIC-API.md` |
| Network transports | partial | TCP + optional TLS; no mutual TLS |
| Persistence | partial | Snapshot OK; no WAL |
| Memory bounds | partial | `ResourceLimits` / `BoundedNode` (opt-in) |
| Flow control | partial | Class exists; not fully on send path |
| Packaging | partial | NuGet `0.1.0-alpha.2` |
| CI | done | `.github/workflows/dotnet.yml` |
| Docs | partial | README + status + errata + public API |

## Delivery order

### P0
1. ~~Docs / CI / status matrix~~
2. ~~ISyncTransport + session wire + stream adapter~~
3. ~~ResourceLimits (opt-in)~~
4. ~~Package metadata~~
5. ~~TCP accept/connect + handshake + one delta~~
6. ~~TLS wrapper~~
7. ~~Normative errata (E-1…E-13)~~
8. ~~Public API freeze + v2.3.7 addendum~~
9. **Next (P0 tail / P1 start):** editorial in-body merge of errata into Russian `BYN-SYNC-SPECIFICATION.txt`; then P1 reconcile ladder.

### P1
STATE_DIGEST / NEED_* / SNAPSHOT, membership, extra merge policies.

### P2
E2EE, IBLT, causalDeps, fuzz/load.

## Current tip

Branch `installing-.net-10-ea6be`. Suite: **103** tests (.NET 10). Package **0.1.0-alpha.2**.
