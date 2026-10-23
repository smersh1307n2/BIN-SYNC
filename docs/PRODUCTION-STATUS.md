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
| Membership | §11 | done | MEMBERSHIP_DELTA wire + `MembershipRoster` / `ExchangeMembershipAsync` |
| GC / quarantine | §11 | done | GC_NOTICE `0x0B`, `GcNotice.Apply`, `PurgeBelowCutoff` |
| Wire frames (codec) | §12 | done | DELTA_BATCH + HELLO/ALIAS + reconcile ladder + GC |
| HELLO / ALIAS_MAP on wire | §12.7+ | done | SessionWire + TCP/TLS |
| Reconcile ladder | §13.1–13.3 | done | STATE_DIGEST `0x06`, NEED_RANGE `0x07`, NEED_KEYS `0x08`, IBLT `0x09`, SNAPSHOT `0x0A` |
| Auto / bilateral reconcile | §13 | done | `AutoReconcileAsync`, `BilateralReconcileAsync`, `ReconcileViaIbltAsync` |
| Apply order [S7] | §13.4 | done | |
| Relay [R3][R4] | §13.6 | done | |
| E2EE | §14 | missing | |
| Security model | §15 | partial | Transport TLS (opt-in) |
| Causal deps | §16.19 | missing | |
| Lab scenarios 1–42 | §17 | partial | + production suite (reconcile / IBLT / GC / membership) |
| Schema evolution | §18 | missing | |
| Normative errata E-1…E-13 | v2.3.7 | done | |
| Public API freeze | — | done | 0.1.0-alpha.2 (surface extended; see PUBLIC-API.md) |

## Engine / product readiness

| Concern | Status | Notes |
|---|---|---|
| Public API surface | done (alpha freeze) | `docs/PUBLIC-API.md` — reconcile/membership/GC promoted |
| Network transports | done | TCP + optional TLS (`SslStream`) |
| Persistence | partial | Snapshot OK; no WAL / incremental log |
| Memory bounds | partial | ResourceLimits opt-in |
| Flow control | partial | Present; not on every send path |
| Packaging | partial | 0.1.0-alpha.2 |
| CI | done | `.github/workflows/dotnet.yml` — restore/build/test on .NET 10, TRX artifact |
| Docs | partial | This file + PUBLIC-API + ERRATA; Russian body-merge of errata pending |

## Frame type map (implemented)

| Type | Hex | API |
|---|---|---|
| DELTA_BATCH | (core) | `PushBatchAsync` / `ReceiveBatchAsync` |
| STATE_DIGEST | `0x06` | `ExchangeStateDigestAsync` |
| NEED_RANGE | `0x07` | `SendNeedRangeAsync` / `FulfillNeedRangeAsync` |
| NEED_KEYS | `0x08` | `SendNeedKeysAsync` / `ReplyNeedKeysAsync` |
| IBLT | `0x09` | `ExchangeIbltAsync` / `ReconcileViaIbltAsync` |
| SNAPSHOT_CHUNK | `0x0A` | `SendSnapshotAsync` / `ReceiveSnapshotAsync` |
| GC_NOTICE | `0x0B` | `ExchangeGcNoticeAsync` |
| MEMBERSHIP | (SessionWire) | `ExchangeMembershipAsync` |

## Delivery order

### P0 — complete
Transport foundation, TLS loopback, HELLO/ALIAS, resource limits, public API freeze, errata E-1…E-13.

### P1 — complete
1. STATE_DIGEST + NEED_* + SNAPSHOT + Auto/Bilateral  
2. MEMBERSHIP_DELTA  
3. IBLT + ReconcileViaIblt  
4. GC_NOTICE + quarantine + PurgeBelowCutoff  

### P2 — in progress
1. ~~GitHub Actions CI on tip~~ (**done** — `dotnet.yml`)  
2. Bilateral IBLT (both peers fetch missing keys in one session)  
3. WAL / incremental persistence  
4. Extra merge policies  
5. E2EE, causalDeps, fuzz/load  

## Current tip

Branch `installing-.net-10-ea6be`. Suite: **130** tests (.NET 10). Package **0.1.0-alpha.2**. CI: `.github/workflows/dotnet.yml`.
