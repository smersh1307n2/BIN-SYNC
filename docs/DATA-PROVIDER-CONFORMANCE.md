# BIN-SYNC Data Provider Semantic Contract and Conformance Plan

**Status:** semantic contract plus initial volatile conformance suite (six semantic tests and one durability-profile assertion); no durable provider implementation exists yet. Public C# signatures are intentionally not frozen.  
**Related architecture:** [Metadata Storage and External Data Provider Architecture](METADATA-STORAGE-ARCHITECTURE.md).  
**Related metadata contract:** [Metadata Store Semantic Contract and Conformance Plan](METADATA-STORE-CONFORMANCE.md).  
**Change lifecycle:** [Change Staging, Coalescing, and Metadata Retention](CHANGE-STAGING-AND-RETENTION.md).  
**Protocol baseline:** BIN-SYNC CORE v2.4.19 FINAL; this document does not change wire format.

## 1. Purpose and boundary

A Data Provider adapts an application-owned source of user data to the canonical BIN-SYNC record/field model. It owns user-data identity and application semantics; it is not the BIN-SYNC protocol runtime, Metadata Store or physical Storage engine.

The four roles remain separate:

- **Data Provider:** stable source-to-canonical mapping, source change capture, consistent source snapshots, provider validation and idempotent application of accepted remote changes.
- **Metadata Store:** durable BIN-SYNC receipts, ChangeId ledger, peer/author progress, frozen outbound payloads, relay retention and pending-apply recovery state.
- **BIN-SYNC Runtime:** wire/protocol interpretation, canonical validation, merge/relay decisions, ACK aggregation and generation of metadata commands.
- **Physical Storage:** generic atomic byte transactions, durability, recovery, snapshots, ordered scans and storage-native commit identifiers.

This is an observable semantic contract, not a prescribed interface, serialization format or provider schema. A provider may be SQL, file-based, array-backed, embedded, or application-specific.

## 2. Stable canonical mapping

The provider must define stable mappings:

- source entity/item identity → canonical 16-byte `RecordId`;
- source property/field identity → canonical `FieldId`;
- source value and tombstone → canonical operation payload;
- source schema/version → canonical namespace/schema/merge-policy mapping where applicable.

Mappings must survive provider reopen and schema evolution. They must not depend on process memory addresses, object hash codes, row positions, mutable array indexes or iteration order. A mapping collision or incompatible remap fails closed; it must not silently alias two source entities or fields.

The provider must document whether identifiers are deterministic derivations or durable allocated mappings, how migrations preserve them, and how deleted/retired source identities are handled.

## 3. Local source change capture

A local source change must remain recoverable until the corresponding canonical operation is durably represented in pending staging or the frozen Outbox.

Preferred mechanisms:

1. a durable ordered change feed with stable source tokens and immutable payloads/references;
2. a transactional source hook that writes source state and pending-change metadata atomically;
3. a snapshot/diff mechanism with a stable snapshot token and a matching change-feed watermark or write barrier.

A transient callback/event is notification only. It is not a durable feed and cannot be the sole record that a source change occurred.

A feed consumer must be able to:

- read bounded pages from a stable continuation token;
- repeat a page after interruption and receive the same logical changes;
- acknowledge only a contiguous prefix that has already been durably staged/frozen;
- resume after the acknowledged prefix without gaps or duplicates;
- retain payload bytes or exact-version references until the runtime confirms durable transfer.

The provider's feed token is not a BIN-SYNC author OutboxSeq, peer StreamOffset, HLC, WAL CommitSeq or Storage CommitSeq. Translation is explicit at the adapter/runtime boundary.

For state-only providers with no durable feed, snapshot comparison can preserve final-state LWW differences only. It cannot preserve intermediate events or local operations overwritten before observation. That limitation must be an explicit provider capability, not silently presented as event-history synchronization.

## 4. Snapshot and feed barrier

Opening a source snapshot must atomically bind:

- a stable source snapshot token/version;
- a change-feed watermark (or equivalent write-barrier generation);
- the namespace/schema context needed to interpret the snapshot.

The snapshot view must remain stable while later source mutations occur, for its documented lifetime. The runtime reads the snapshot and then consumes the feed strictly after the captured watermark. Together they must cover every source change exactly once logically: no gap between snapshot and tail and no duplicate change caused by an unrelated cursor sample.

A provider that cannot expose a native MVCC snapshot plus feed watermark must implement an equivalent write barrier or durable journal. A current-state snapshot and an independently sampled feed cursor are not a valid baseline.

Snapshot installation is not a peer ACK. It does not authorize Outbox/relay compaction, ChangeId-ledger GC, tombstone GC or removal of pending/frozen operations. Those require the protocol's own per-author cut and completion/fencing proof (currently not defined for these retention purposes).

## 5. Applying remote changes

The provider must apply one accepted remote batch as an atomic source-data effect, or expose an equivalent recoverable transaction.

For separate provider and metadata stores:

1. BIN-SYNC persists a pending intent with a stable apply token and sufficient canonical input or a durable payload reference.
2. The provider applies the batch and records the token plus a durable idempotency receipt atomically with the data effect.
3. Metadata Store finalizes receipt, receive cursor, dedup/provenance, relay and protocol state.
4. After both sides are durable, raw intent payload is removed; only compact receipts and materialized metadata remain.
5. Recovery retries or queries the same token. The provider returns the prior durable result without reapplying the source effect.

Required token behavior:

- same token + same canonical input → return the original durable result without repeating the effect;
- same token + different canonical input → reject as equivocation;
- invalid batch → reject atomically, with no partial source mutation and no durable success receipt;
- successful receipt → prove the provider data effect is durable, not merely queued in memory.

A process-local set of completed tokens is insufficient. Provider idempotency state must survive the provider's documented restart/recovery boundary.

## 6. Local versus remote origin

A remote apply must not be recaptured as a new local-origin change. Provider adapters must distinguish local source writes from remote protocol application even when the underlying database emits the same generic row-changed notification for both.

This may use transaction origin metadata, a durable apply token, a dedicated change-capture suppression scope, or a provider-native mechanism. It must remain correct across process restart; a transient boolean flag is not enough.

The provider does not generate or rewrite BIN-SYNC ChangeId, original author, HLC, author-stream sequence or peer cursor during remote apply. Those are supplied/owned by the protocol runtime and preserved in the canonical mutation.

## 7. Transaction modes

### 7.1 Shared transaction domain

When Data Provider and Metadata Store share one transactional engine, the source data mutation and all required metadata transitions should commit atomically. This is the preferred model.

### 7.2 Separate stores

Use the pending-apply protocol in §5. Do not claim a cross-store atomic transaction. Provider-side idempotency receipt and Metadata Store pending intent must be independently durable and recoverable.

### 7.3 Weak mode

Best-effort or at-least-once operation may be offered only as an explicitly weaker mode. It must not emit an ACK that implies durable exactly-once acceptance.

## 8. Provider-independent conformance matrix

| Area | Required test | Failure prevented |
|---|---|---|
| Mapping | Same source keys map to same canonical IDs; distinct keys do not alias; reopen/migration preserves mappings | Identity drift or record/field collision |
| Feed replay | Read the same page repeatedly before ACK | Lost or mutated source change |
| Feed paging | Bounded pages resume exactly after the last consumed token | Gaps, duplicates or unbounded reads |
| Feed ACK | Reject ACK beyond the durably exposed/consumed prefix; reject backward ACK | Source discards a change before durable staging |
| Payload ownership | Mutating caller buffers after append/read does not mutate retained change | Borrowed-buffer lifetime bug |
| Snapshot barrier | Snapshot token and feed watermark captured atomically; all later writes appear in tail | Snapshot-to-feed gap or duplicate |
| Snapshot lifetime | Snapshot remains stable while source writes/apply/checkpoint proceed; release fences future reads | Mixed-generation or use-after-reclaim |
| Remote apply | Atomic batch effect and durable receipt | Partial source mutation |
| Apply idempotency | Same token/input retry returns original result and does not repeat effect | Duplicate remote application |
| Apply equivocation | Same token/different input rejects without mutation | Ambiguous recovery identity |
| Origin separation | Remote apply does not appear in local change feed | Echo loops and false local authorship |
| Recovery | Reopen after feed append/ACK, snapshot creation and provider apply boundaries | Lost feed, mapping, snapshot or idempotency receipt |
| Bounds | Bounded page/batch/count/payload and explicit overflow behavior | Unbounded allocation or integer overflow |

Crash tests must inject deterministic failures at provider durability boundaries. For each failure, assert recovered source data, feed position, provider receipt and whether Metadata Store finalization/ACK is permitted.

## 9. Relationship to Metadata Store conformance

The Metadata Store contract owns pending-apply intent and metadata finalization. This provider contract owns the actual user-data effect, provider-side token receipt, source feed and source snapshot.

Neither test suite substitutes for the other:

- Metadata Store tests must prove pending intent remains available until provider receipt is accepted and metadata finalization completes.
- Data Provider tests must prove the source effect and provider receipt are atomic/idempotent and that local/remote change capture is separated.
- An end-to-end adapter test must inject crashes between the two stores and verify recovery using the same token.

The volatile reference provider is for semantic testing only. A durable provider must add reopen/fault-injection tests against its actual persistence engine.

## 10. Implementation gates

1. Freeze the observable provider guarantees in this document; keep public API names open.
2. Add a provider-independent test-only adapter and volatile reference provider.
3. Cover mapping, durable-feed semantics, snapshot/feed barrier, remote apply idempotency and origin separation.
4. **Default ownership is now recorded in Metadata Storage Architecture §3.3:** a separate generic Data Provider owns its durable source feed and the Metadata Store/runtime owns canonical PendingLocal staging; FLD Database, acting as both provider and metadata owner, owns PendingLocal inside its Database transaction; a shared transaction domain may co-commit the stage; Storage-only mode has no user-data PendingLocal. Keep public API names open until staging/feed-handoff conformance tests pass.
5. Only then finalize neutral Data Provider interfaces and refactor BIN-SYNC Runtime/Node persistence.
6. Validate shared-transaction and separate-store adapters independently. Keep the FLD synchronized-database provenance adapter as a separate integration gate.


## 11. Initial executable conformance status (2026-11-07)

The test-only contract and inherited suite are implemented in `BinSync.Net/tests/BinSync.MetadataStore.Conformance`; `VolatileDataProviderAdapter` is a reference model only. Six semantic tests cover stable item/field mappings, bounded and resumable local feed pages, source-feed cursor/ACK fencing, input/output payload ownership, the snapshot/feed watermark barrier, snapshot lifetime across local/remote writes, idempotent remote apply, token equivocation and atomic rejection. A separate test asserts that the reference provider does not claim restart durability.

GitHub Actions run `42359566433` passed the full BIN-SYNC Release build/tests, public samples and fuzz/lab hardening at commit `b9adbd3749e689b2019f48f8b500881293d5cc2c`: 637 tests passed, line coverage 72.5%. The follow-up feed-gap/input-ownership assertion is in the subsequent commit and is being revalidated separately.

**Still open:** a durable provider adapter and reopen/fault-injection tests; end-to-end proof that feed ACK follows durable staging; a real MVCC snapshot/feed barrier under concurrent writes; shared-transaction and separate-store integration with actual providers. The volatile reference provider is not evidence of any disk durability or process-restart guarantee.
