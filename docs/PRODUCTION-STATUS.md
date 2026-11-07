# Production status — BinSync.Net vs BIN-SYNC CORE

**Package:** BinSync.Core 3.0.0 · **Target:** net10.0  
**Branch:** feature/fld-external-storage · **PR:** #16 (open, Draft; not merged)  
**CI:** run #1276 successful — 588/588 tests, fuzz/lab 31/31, all three public samples pass, line coverage 70.7%.  
**Audit status:** stream/fragment binding audited; general Metadata Store / external Data Provider architecture documented as a design draft. The specialized FLD synchronized-database contract is aligned with the new architecture and current FLD branch APIs. External-store runtime boundary and FLD Storage-only role are not implemented. Full normative §§4–18 audit remains in progress; no percentage-based spec-parity claim is made.

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
| Membership / GC | §11 | implemented |
| Wire frames | §12 | implemented, including fragments |
| Reconciliation ladder | §13 | implemented; CI lab/fuzz coverage |
| E2EE | §14 | opt-in |
| Security | §15 | TOFU / PreShared / revoke; CA mode is a node-id allow-list, not full X.509 PKI |
| Causal dependencies | §16.19 | opt-in |
| Lab / fuzz | §17 | in CI |
| Schema evolution | §18 | **partial** — opt-in metadata compatibility gate; not a runtime merge-policy dispatcher |
| Public examples | samples | all three build and execute in CI |
| Errata E-1…E-13 | v2.3.7 | incorporated into the current specification |

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

- Complete normative audit of remaining §§4–18 and cross-check every normative scenario against executable tests.
- Complete the generic Metadata Store and Data Provider contracts from `docs/METADATA-STORAGE-ARCHITECTURE.md`, including conformance tests and the external-provider recovery protocol.
- Integrate BIN-SYNC runtime persistence behind the new boundary. Current Node dictionaries and Node WAL remain authoritative in the existing implementation; the architecture document is not yet implemented.
- Complete the FLD synchronized-database boundary: FLD `ApplyRemoteBatch`, durable apply journal, and fenced Outbox lease/high-watermark primitives now exist on the FLD branch, but BIN-SYNC integration, relay safe-ACK/retention, and consistent snapshot contract still require verification and completion. The legacy `FldDatabase.ApplyRemote(...); Commit()` path remains explicitly unsafe.
- Design and implement FLD Storage-only mode without protocol HLC/author/ChangeId/Outbox semantics; current FLD1 run format constraints must be resolved before choosing its physical representation.
- Confirm final documentation/specification parity on the complete branch diff.
- Review PR #16 diff and CI once more; only then request explicit approval to merge into main.

**Merge status:** PR #16 remains Draft and unmerged. No merge is authorized by this status document.
