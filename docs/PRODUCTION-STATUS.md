# Production status — BinSync.Net vs BIN-SYNC CORE

**Package:** BinSync.Core 3.0.0 · **Target:** net10.0  
**Branch:** feature/fld-external-storage · **PR:** #16 (open, Draft; not merged)  
**CI:** GitHub Actions run [42360397431](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42360397431) successful — 637/637 tests, public samples pass, fuzz/lab hardening pass, line coverage 72.5%.  
**Audit status:** stream/fragment binding audited; Metadata Store architecture v0.7, Metadata Store semantic conformance, Data Provider semantic conformance, change-staging/retention policy and the specialized FLD synchronized-database contract are documented. Test-only volatile reference fixtures cover 20 Metadata Store and six Data Provider semantic scenarios. They do not constitute durable providers/stores or restart guarantees. External-store runtime boundary, durable PendingLocal staging/freeze and FLD Storage-only implementation are not implemented. Full normative §§4–18 audit remains in progress; no percentage-based spec-parity claim is made.

Legend: **implemented** · **partial** · **helper-only** · **not implemented**

## Spec coverage

| Area | Spec | Current implementation status |
|---|---|---|
| Goals / consistency | §1 | implemented in the core model and apply path |
| Transport | §3 | generic SyncSession, Pipe/Stream/TCP adapters, negotiated per-stream Flow Control, WINDOW_UPDATE, and typed/raw fragment reassembly implemented |
| Identity / alias / epoch | §4 | implemented; b128 API normalization and wire width audited |
| HLC | §5 | implemented in µs; overflow checked before logical increment |
| Codec | §6 | implemented; b128 writer always emits exactly 16 bytes |
| Data model / lifecycle | §7 | implemented; field tombstones are ordered against live values |
| Merge policies | §8 | **partial** — Node apply path supports LWW_FIELD only; other merge helpers are standalone |
| Digests | §9 | record digest v2 distinguishes live field values from tombstones |
| Outbox / inbox | §10 | implemented |
| Membership / GC | §11 | membership and GC_NOTICE frames implemented; destructive tombstone/ghost GC is disabled by E-25 until a safe namespace frontier and stale-peer recovery protocol exist |
| Wire frames | §12 | implemented, including fragments |
| Reconciliation ladder | §13 | implemented; CI lab/fuzz coverage |
| E2EE | §14 | opt-in |
| Security | §15 | TOFU / PreShared / revoke; CA mode is a node-id allow-list, not full X.509 PKI |
| Causal dependencies | §16.19 | opt-in |
| Lab / fuzz | §17 | in CI |
| Schema evolution | §18 | **partial** — opt-in metadata compatibility gate; not a runtime merge-policy dispatcher |
| Public examples | samples | all three build and execute in CI |
| Errata E-1…E-13 | v2.3.7 | incorporated into current specification v2.4.19 FINAL |
| Errata E-24 / E-25 | v2.4.18 / v2.4.19 | normative route-independent ChangeId identity/equivocation and fail-closed tombstone retention |

## Verified correctness fixes in this audit

- HLC logical overflow is rejected before increment; no uint wrap to zero.
- A stale field write cannot resurrect a newer field tombstone; a strictly newer write supersedes it.
- A stale field tombstone cannot replace a newer live value.
- Digest calculation does not materialize the lazy empty FieldTombs dictionary.
- Record digest v2 includes a field-kind byte, so an empty live value and a tombstone with identical metadata produce different digests.
- Long identity input is normalized to exactly 16 bytes before wire encoding.
- Flow Control is negotiated by capability intersection; payload credits are reserved atomically per stream and replenished only after receive dispatch. Oversized logical frames use bounded fragment reassembly.
- Unsupported merge policy codes are rejected by ApplyBatch; legacy policy 0 is treated as LWW_FIELD, not as a schema-gate wildcard.
- Stream binding follows §3.7: control-plane frames use stream 0; namespace frames use `0x0001_0000 | namespaceId`; ACK shares its DELTA_BATCH namespace stream; WINDOW_UPDATE targets the credited stream. Receive paths reject mismatches before applying payloads.
- IBLT response receive now requires the expected namespace. MEMBERSHIP, GC_NOTICE and SCHEMA_MAP use the control stream.
- Fragment reassembly preserves frame type/stream/base sequence/flags, supports interleaved logical messages, rejects cross-stream message-id reuse, trailing fragment bytes and conflicting duplicate parts; raw receive returns the reconstructed `Frame`.

## Release gates still open

- Complete the normative audit of remaining §§4–18 and cross-check every normative scenario against executable tests. No spec-parity percentage is claimed.
- **Snapshot retention:** current snapshot completion is not an Outbox ACK, relay-retention boundary or ChangeId-ledger GC watermark. SnapshotTail/runtime DTO still lacks the per-author cut and completion/fencing proof needed for safe historical retention.
- **E-24 implemented and normative (v2.4.18):** exact global `(namespaceId, ChangeId)` identity with canonical semantic fingerprint and equivocation rejection is implemented. Origin-sequence/range compression remains an unapproved representation alternative; it must prove mesh replay, snapshot/bootstrap and equivocation safety before replacing the exact ledger.
- **E-25 normative (v2.4.19):** tombstone/ghost GC remains disabled until durable namespace membership/safe-frontier, stale-peer fencing and mandatory authoritative full-resync semantics are implemented. TTL/HLC age and GC_NOTICE alone never authorize deletion.
- **Inbox/receipt lifecycle:** NodePersistence v12 adaptive receipt encoding and global 16-byte ChangeId ledger are implemented. Receipt expiry/GC and independent dedup retention horizons remain open; do not couple them to relay payload ACK.
- **Peer cache pruning implemented:** `ProcessAckInternal` prunes `PeerState.Ids` only after a validated contiguous ACK covers the full author cursorEnd; `LastStreamDigest` remains as the chain checkpoint. Partial ACK and lost-ACK behavior remain covered by regression tests.
- **ProtocolStateDelta:** the bounded per-batch recovery-envelope lifecycle is specified and pending-apply semantics have volatile conformance tests. Durable separate-store provider receipts, restart recovery and production metadata finalization remain unimplemented.
- **FLD APPLY/relay:** APPLY_COMMIT v3 single-encoding, relay-only ApplyJournal, per-author JournalSeq ACK sidecar and post-WAL-seal relay payload compaction are implemented on the FLD branch. The cross-system JournalSeq → ChangeId → OutboxSeq provenance map, outbound publication gate and ACK translation remain open; receipt GC and bounded/rebuildable journal indexing also remain open.
- **Two-DLL FLD boundary:** architecture is decided (`Fld1.Storage.dll` + `Fld1.Database.dll`, one-way Database → Storage); extraction into the final public assemblies and Storage-only physical conformance remain implementation work.
- **Metadata Store / Data Provider:** semantic contracts and test-only volatile fixtures now exist (20 Metadata Store tests; six Data Provider tests). Durable adapters, actual provider reopen/fault-injection, shared-transaction integration and end-to-end separate-store recovery remain open.
- **PendingLocal:** ownership by transaction mode is recorded in Metadata Storage Architecture §3.3; pre-publication local LWW coalescing and freeze remain a design contract, not implemented. Dedicated source-page replay/coalescing/freeze crash tests are still open. Separate commits to one field still create separate frozen Outbox operations; never rewrite an unacknowledged/published Outbox entry.
- **Runtime persistence refactor:** Node dictionaries plus NodePersistence/WAL remain authoritative in the current runtime. Moving them behind the neutral Metadata Store boundary is not implemented.
- **Storage-only role:** design uses `Fld1.Storage.dll` directly without loading `Fld1.Database.dll` or fabricating HLC/author/ChangeId/Outbox. Physical implementation and conformance remain open.
- Confirm final documentation/specification parity on the complete branch diff.
- Review PR #16 diff and CI once more; only then request explicit approval to merge into main.

**Merge status:** PR #16 remains Draft and unmerged. No merge is authorized by this status document.
