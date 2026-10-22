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
| Merge policies | §8 | partial | LWW_FIELD + lifecycle only; COUNTER/SET/BLOB/MANUAL missing |
| Digests / buckets | §9 | partial | Used for batch chain; full anti-entropy ladder missing |
| Outbox / inbox / cursors | §10 | done | E-3, E-4 stream keys |
| Membership / GC / quarantine | §11 | missing | |
| Wire frames (codec) | §12 | partial | Build/parse generic frames; only DELTA_BATCH/ACK fully driven |
| HELLO / ALIAS_MAP on wire | §12.7+ | partial | In-process `MakeSession`, not byte-level SM |
| Reconcile ladder | §13.1–13.3 | missing | STATE_DIGEST, NEED_*, IBLT |
| Apply order [S7] | §13.4 | done | |
| Relay [R3][R4] | §13.6 | done | E-6…E-8, E-13 |
| E2EE | §14 | missing | |
| Security model | §15 | partial | Error codes present; no auth layer |
| Causal deps | §16.19 | missing | |
| Lab scenarios 1–42 | §17 | partial | Covered in unit tests in-memory |
| Schema evolution | §18 | missing | |

## Engine / product readiness

| Concern | Status | Next work |
|---|---|---|
| Public API surface | partial | Freeze `Node`/`Transport`/`ISyncTransport`; hide test hooks |
| Network transports | missing | TCP/TLS and WebSocket adapters + session SM |
| Persistence | partial | Snapshot OK; need WAL / crash mid-write tests / format migrations |
| Memory bounds | missing | Cap outbox, SeenChanges, inbox replay cache |
| Flow control | partial | `FlowControl` class exists; not wired to send path |
| Fragmentation | missing | §12.5 |
| Packaging | missing | NuGet metadata, SemVer, SourceLink |
| CI | done (this change) | GitHub Actions `dotnet test` |
| Docs | partial | README + this matrix; API reference still TODO |
| Fuzz / load | missing | Frame/op/persistence fuzz; multi-node chaos |

## Recommended delivery order

### P0 — embeddable in a controlled environment
1. Keep errata folded into the normative spec text (single source of truth).
2. Stable public API + XML docs.
3. At least one real transport (TCP or WebSocket) + wire HELLO/ALIAS_MAP.
4. Outbox / SeenChanges bounds + simple GC horizon.
5. NuGet package + versioning.

### P1 — mesh anti-entropy
6. STATE_DIGEST + NEED_RANGE/KEYS + SNAPSHOT_CHUNK path for cold peers.
7. Membership + quarantine.
8. Additional merge policies as product needs them.

### P2 — hostile / large deployments
9. E2EE + key rotation.
10. IBLT, causalDeps, full schema evolution.
11. Fuzz, benchmarks, multi-machine CI.

## Current tip

Branch `installing-.net-10-ea6be` (backup: `backup/installing-.net-10-ea6be`). Core suite: **95** tests on .NET 10 after FieldWrite author-detection and async convergence fixes.
