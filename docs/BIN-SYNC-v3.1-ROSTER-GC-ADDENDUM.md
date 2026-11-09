# BIN-SYNC v3.1 Addendum — Signed Namespace Roster and Safe GC

**Status:** mandatory v3.1 project extension; C-pass implementation is substantially present but not validated or production-complete. It is separately negotiated on the wire and does not alter v3.0-only frames. Implementing and validating this addendum is required for project completion/release. The signed roster/frontier/checkpoint/installed-ACK codecs and durable stores, FULL_REPLACE sender/receiver, coordinator APIs, author-issued/retired-origin/HLC horizons, provider epoch closure, composed host commit lease and reference-scan contracts are present. RPM3 source mapping, cutover-aware ChangeId resolution, serialized apply→page-drain→send gate, startup cursor validation, safe-ACK translation and a delegate host reference-snapshot adapter are present. The relay/ACK/GC adapter source is in `samples/FldBinSyncHost`, but the `FldDatabase`→`IDataProvider` snapshot/local-feed/full remote-apply bridge and host transport lifecycle remain open. The complete frame registry (0x01–0x1C) is source-audited; deterministic NRS2/OFR2/RCP2/FRA2 and FULL_REPLACE payload vectors plus an executable sample are present. The sample is included in the required Core GitHub Actions workflow and checks encode bytes, decode/verify/re-sign round-trips and representative negative cases; exact-head result is pending. Exact-head CI, C# vector cross-check, crash/race/rejoin/resurrection proof, complete cross-repository sample execution, metadata byte/cardinality audit and performance validation remain open. All physical GC flags remain false.

**Last green pre-v3.1 baseline:** `c56d2e206d0c8578cbfe66158d6e6f1e0824750c` (Core 676/676, Fuzz/Lab passed, Storage Integration passed on Ubuntu/Windows/macOS). This is historical, not current-head validation. The latest completed code-equivalent run on `4382c49bffe1608ebabc4a483771b83242cbc81d` built successfully: Core 672/678 (six fixture failures), Storage Integration 91/171 (80 stale provider-contract failures), Fuzz/Lab passed. Commits after that baseline have not been built/tested.

This addendum closes the missing proof needed before enabling global ChangeId GC and tombstone/Ghost GC. It deliberately does not weaken the current fail-closed behavior. It is mandatory to implement the v3.1 extension for the project; that requirement does not make v3.1 frames implicitly available to a v3.0-only session.



**Deterministic vectors:** `docs/BIN-SYNC-v3.1-GOLDEN-VECTORS.md` now records NRS2, OFR2, RCP2, FRA2 and FULL_REPLACE_BEGIN payload/signature vectors using a public-only test key. Exact C# encoder/decoder cross-check and negative tamper vectors remain Phase T; these vectors do not enable physical GC.

## 1. Problem and invariants

The current v3 implementation already has:

- one namespace-wide ChangeId identity/provenance row per operation;
- per-peer/per-author stream state for delivery and ACK;
- durable PendingApply intents and batch receipts;
- immutable origin author/epoch/sequence coordinates;
- a provider FULL_REPLACE staging/switch primitive;
- conservative transport-epoch receipt retirement.

It does **not** yet have an authoritative, durable namespace roster or a signed statement that every active member has applied an origin prefix / installed a particular authoritative snapshot. The existing `MEMBERSHIP` message is gossip and must never authorize GC.

The following remain hard rules:

1. Missing, offline, unknown, joining, disconnected or lagging active members contribute frontier zero.
2. A member is excluded from a safe minimum only after a signed roster generation and a durable transport-epoch fence have retired it.
3. A retired member that returns must install the current authoritative FULL_REPLACE generation before it can become Active again.
4. ChangeId and tombstone/Ghost GC are independent horizons. Receipt GC and outbound/relay payload GC remain separate.
5. PendingApply, local provider intent, relay payload, immutable outbound reservation, active feed lease, snapshot/apply pin and any other live reference block reclamation even when a frontier would otherwise permit it.
6. v3.0 peers continue ordinary sync, but cannot contribute a signed frontier; therefore namespace-wide GC is disabled while any required member lacks the v3.1 capability.
7. Signatures prevent spoofing and bind claims to members/authority. They do not prove a Byzantine member is honest; the initial design assumes crash-fault/non-Byzantine members. A Byzantine-resistant quorum/proof model is a separate extension.

## 2. Authority and trust model

Each namespace has one configured **Namespace Authority** (normally the namespace owner node). Its NodeId and Ed25519 public key are pinned out-of-band in `TrustStore` with `TrustMode.PreShared`; revoked, TOFU-only or unknown authority keys cannot sign GC state.

The authority signs full roster certificates and retention checkpoints. Each member signs its own applied-origin frontier and FULL_REPLACE-installed acknowledgement. The roster certificate carries each member's Ed25519 public key, so the authority signature binds the key used to verify that member's future reports.

No authority key is learned from the roster certificate itself. No automatic TOFU is permitted for namespace authority. If the authority is unavailable, ordinary synchronization continues but roster changes and GC stop safely.

## 3. Negotiation and frame allocation

The current HELLO payload layout remains unchanged. The v3.1 extension is explicitly negotiated through new capability bits; a peer sends no v3.1 frame unless the relevant capability is present in the peer's negotiated capability set.

Proposed additions to `SessionCapabilities`:

| Capability | Bit | Enables |
|---|---:|---|
| `NamespaceRosterV31` | 3 | Signed namespace roster certificate exchange |
| `OriginFrontierV31` | 4 | Signed member applied-origin frontiers and retention checkpoints |
| `FullReplaceV31` | 5 | Authoritative FULL_REPLACE transfer and installed ACK |

Frame type assignments used by the v3.1 SyncSession adapters; source audit confirmed the complete registry is unique and contiguous from 0x01 through 0x1C:

| Frame | Type | Stream |
|---|---:|---|
| `NAMESPACE_ROSTER` | `0x14` | control (stream 0) |
| `ORIGIN_FRONTIER` | `0x15` | control (stream 0) |
| `RETENTION_CHECKPOINT` | `0x16` | control (stream 0) |
| `FULL_REPLACE_BEGIN` | `0x17` | dedicated transfer stream |
| `FULL_REPLACE_CHUNK` | `0x18` | dedicated transfer stream |
| `FULL_REPLACE_COMMIT` | `0x19` | dedicated transfer stream |
| `FULL_REPLACE_ACK` | `0x1A` | control (stream 0) |
| `FULL_REPLACE_ABORT` | `0x1B` | dedicated transfer stream |
| `FULL_REPLACE_READY` | `0x1C` | dedicated transfer stream |

The existing `MEMBERSHIP` frame remains advisory gossip. It is not a signed roster and cannot advance a GC frontier. A v3.0 peer without the new bits contributes zero to every required frontier and blocks GC; no compatibility downgrade is attempted for core sync.

## 4. Canonical signed messages

All integer fields are little-endian unless explicitly stated. NodeIds and Ed25519 keys have fixed lengths. Lists are strictly sorted and unique. Every signed payload uses a domain-separation prefix and rejects trailing bytes.

### 4.1 NamespaceRosterCertificate (`NRS2`)

Canonical payload before signature:

- magic `NRS2`;
- NamespaceId u16;
- RosterGeneration u64;
- PreviousRosterDigest[32];
- AuthorityNodeId[16];
- MemberCount u16;
- Member entries sorted by NodeId:
  - MemberNodeId[16];
  - MemberPublicKey[32];
  - TransportPeerEpoch u32;
  - Status u8: Joining=0, Active=1, Retired=2;
  - RetiredThroughEpoch u32 (zero unless Retired);
- Ed25519 signature[64] by the pinned Namespace Authority over `BIN-SYNC/NAMESPACE-ROSTER/v2 || canonical-payload`.

RosterDigest is SHA-256 of the canonical payload including the domain prefix but excluding the signature.

Validation:
- namespace and authority must match local configuration;
- authority key must be pinned PreShared and not revoked;
- members must be sorted, unique, nonzero NodeIds, and have valid public keys;
- Active/Joining members require nonzero transport epoch;
- a retired entry carries a durable retired-through epoch;
- lower generations are rejected; same generation with a different digest is a fork and is rejected;
- a higher signed full roster may be accepted after offline periods, but the local highest generation/digest is persisted before it affects any safe frontier.

Joining members contribute frontier zero. They cannot become Active until the current authoritative FULL_REPLACE generation is installed and acknowledged.

### 4.2 AppliedOriginFrontier (`OFR2`)

A member signs a bounded vector of contiguous, durably applied origin prefixes:

- magic `OFR2`;
- NamespaceId u16;
- RosterGeneration u64;
- RosterDigest[32];
- MemberNodeId[16];
- MemberTransportEpoch u32;
- ReportSequence u64;
- FinalForAuthor u8 (0/1);
- UnpublishedDraftCount u64;
- MaxUnpublishedDraftHlcWall u64;
- MaxUnpublishedDraftHlcLogical u32;
- EntryCount u16;
- entries sorted by (OriginAuthorNodeId, OriginAuthorEpoch):
  - OriginAuthorNodeId[16];
  - OriginAuthorEpoch u32;
  - ContiguousAppliedOriginSequence u64;
- IssuedOriginCount u16 followed by entries sorted by OriginAuthorEpoch: OriginAuthorEpoch u32, IssuedOriginSequence u64, MaxIssuedHlcWall u64, MaxIssuedHlcLogical u32;
- Ed25519 signature[64] by the member key bound in the accepted roster over `BIN-SYNC/APPLIED-ORIGIN-FRONTIER/v2 || canonical-payload`.

A frontier is an assertion that every operation in that origin stream through the sequence has been durably applied, not merely received. The member must persist the application before signing. Missing author entries mean zero. Reports must be monotonic; regression, wrong roster generation, wrong member epoch or unknown member key is rejected. A member's IssuedOrigins must cover every locally-authored applied epoch and cannot be below its own applied frontier. A final report requires zero unpublished drafts and is terminal.

### 4.3 RetentionCheckpoint (`RCP2`)

The Namespace Authority verifies member signatures and computes a signed namespace-wide minimum:

- magic `RCP2`;
- NamespaceId u16;
- RosterGeneration u64;
- RosterDigest[32];
- CheckpointSequence u64;
- PreviousCheckpointDigest[32];
- EntryCount u16;
- entries sorted by (OriginAuthorNodeId, OriginAuthorEpoch):
  - OriginAuthorNodeId[16];
  - OriginAuthorEpoch u32;
  - SafeAppliedOriginSequence u64;
- optional fully-installed FULL_REPLACE generation[16 RFC-4122 network order], digest[32], HlcWallFloor u64 and HlcLogicalFloor u32;
- Ed25519 signature[64] by the pinned authority over `BIN-SYNC/RETENTION-CHECKPOINT/v2 || canonical-payload`.

For each origin author/epoch, SafeAppliedOriginSequence is the minimum contiguous frontier across **every Active member in the roster generation**. Joining or missing reports contribute zero. The checkpoint is persisted atomically with the accepted roster generation and cannot regress.

A node may use only an authority-signed checkpoint matching its highest accepted roster digest. A local untrusted minimum or gossip membership list is not sufficient.

## 5. FULL_REPLACE installation proof

The v3.0 provider primitive is already implemented:

- generation-scoped staging;
- bounded canonical pages;
- SHA-256 manifest verification;
- captured-base-generation fence;
- atomic active-generation switch;
- bounded abort;
- old generation retention (no premature physical GC).

The v3.1 wire transfer binds the provider operation to the accepted roster:

1. FULL_REPLACE_BEGIN carries namespace, roster generation/digest, new generation, source feed watermark, entry count, canonical digest and authority-signed snapshot manifest.
2. FULL_REPLACE_CHUNK carries generation, contiguous chunk index and canonical ProviderMutation page (bounded by the provider page limit).
3. FULL_REPLACE_COMMIT is accepted only after every page and digest verify; receiver serializes the switch against remote APPLY and verifies there is no PendingApply/provider intent/relay reservation/snapshot pin that still targets the old generation.
4. Receiver atomically commits the provider generation and returns a member-signed FULL_REPLACE_ACK containing roster generation/digest, generation, digest, member NodeId/epoch and durable commit result.
5. Authority counts a member as installed only after verifying its ACK signature and exact generation/digest.
6. A Joining member becomes Active only after the current generation ACK is durably recorded.

The source snapshot token and source feed watermark must come from one provider `OpenSnapshot` barrier. The digest algorithm is normative in v3.0 §0.13; generation ids use RFC-4122 network byte order and the entry count is appended after the sorted mutation stream.

## 6. GC algorithms

### 6.1 Receipt GC (already partially implemented in v3.0)

Receipt reclamation remains based on authenticated transport epoch, replacement initial-sync baseline, retired-through fence, PendingApply completion and exact outbound reservation resolution. The v3.1 roster does not replace this per-transport proof.

### 6.2 ChangeId ledger GC

For a row `(NamespaceId, ChangeId, OriginAuthor, OriginEpoch, OriginSequence)`, reclamation is allowed only when:

1. the latest accepted signed RetentionCheckpoint has the same roster generation/digest;
2. `OriginSequence <= SafeAppliedOriginSequence(OriginAuthor, OriginEpoch)`;
3. no PendingApply, provider intent, relay payload, immutable outbound reservation, local publication feed lease, snapshot pin, apply token or other durable object references the ChangeId;
4. the ChangeId GC checkpoint and row deletion are one crash-consistent Storage transaction.

The ChangeId ledger remains one shared row per operation. Do **not** create per-peer copies of ChangeId history. Additional knowledge state is one frontier per (roster generation, member, origin author, origin epoch), which is O(members × authors), not O(changes × peers).

### 6.3 Tombstone/Ghost and old-generation GC

A field tombstone, record tombstone or Ghost may be reclaimed only when:

1. every Active member in the accepted roster has a verified FULL_REPLACE_ACK for the same or newer authoritative generation/digest;
2. the signed RetentionCheckpoint covers the tombstone's origin author/epoch/sequence;
3. no PendingApply, relay payload, provider intent, snapshot/apply pin or other live reference can resurrect or require the tombstone;
4. the provider's active-generation switch and the metadata GC checkpoint are durably coordinated;
5. old provider generations are deleted only after all reader/apply/relay pins are released.

A member that is offline remains Active and blocks the minimum. An administrator may retire it only through a new signed roster generation plus durable epoch fence. If it returns, it must install the current FULL_REPLACE generation before reactivation. This is the anti-resurrection rule.

### 6.4 Outbound and relay payload GC

Continue using per-peer/per-author contiguous ACK cursor plus stream offset, active provider feed lease and pending reservation checks. The namespace roster does not authorize deletion of an immutable batch still needed for exact retry.

## 7. Metadata footprint and comparison with Sync Framework

The design preserves the compact model:

- one global ChangeId/provenance row per operation;
- one peer-stream delivery/ACK state per peer and author;
- one signed roster certificate per roster generation;
- one frontier row per member and origin author/epoch;
- one signed retention checkpoint per checkpoint generation;
- one FULL_REPLACE installation ACK per member/generation.

There is no per-peer duplicate ChangeId history and no separate PendingLocal journal. FLD provider feed remains the local unpublished-change source; BIN-SYNC owns delivery, receipts, causality and shared identity. This retains the useful Sync Framework-style knowledge vector while avoiding a per-change/per-peer metadata explosion.

## 8. Failure and adversarial test matrix

Before enabling any GC:

- unknown/unpinned/TOFU/revoked authority key; bad signature; wrong namespace;
- roster rollback, same-generation fork, wrong previous digest, duplicate/unsorted members;
- member key/epoch rotation, retired old epoch, stale member report;
- missing member frontier, disconnected member, Joining member, lagging author epoch;
- non-contiguous or regressing frontier; forged high sequence; wrong roster digest;
- checkpoint replay, checkpoint regression, checkpoint from stale roster generation;
- FULL_REPLACE bad manifest/signature/digest, duplicate/out-of-order page, missing page, stale base generation;
- crash/restart after each stage, commit, ACK, roster update and checkpoint write;
- member rejoins after retirement and attempts delta-only sync (must require FULL_REPLACE);
- PendingApply/relay/provider intent/active feed lease/snapshot pin blocks ChangeId and tombstone GC;
- randomized multi-peer relay model verifies no operation or tombstone resurrection;
- property tests prove safe frontier is always the minimum over the committed active roster.

GC remains disabled until these tests pass on the exact release commit and crash/power-loss limitations are documented.

## 9. Rollout

1. Keep v3.0 behavior unchanged and all ChangeId/tombstone/Ghost GC disabled.
2. Implement canonical codecs and storage rows behind feature gates.
3. Add capability negotiation; v3.0 peers continue sync but block namespace GC.
4. Add authority configuration and signed roster bootstrap.
5. Add member frontier reports and signed retention checkpoints.
6. Add FULL_REPLACE wire transfer over the existing provider primitive.
7. Enable receipt/ChangeId/tombstone GC one horizon at a time, each with independent proof and tests.
8. Release the project as v3.1-capable only after this implementation, C# golden-vector conformance, host orchestration conformance and exact-head cross-platform CI pass. Capability negotiation remains explicit; GC remains independently gated by proof.

## 10. Open design decision

This proposal uses one pinned Namespace Authority. It is simple, compact and fail-closed, but membership changes and GC stop if that authority is unavailable. A threshold/quorum authority could remove this single-authority dependency, but it increases metadata, protocol complexity and test surface. Do not add quorum until there is a concrete requirement; the crash-fault model is the initial target.

## 10. Implementation status (2026-11-08)

The first implementation slice has landed, but this addendum is not yet fully implemented:

- `IMetadataNamespaceGcStore` defines the durable roster/frontier/checkpoint/installed-ACK contract.
- `StorageNamespaceGcStore` persists those signed objects under a separate Storage key prefix and enforces initial monotonicity, roster-transition and safe-frontier checks.
- `NamespaceGcFrames` and `SyncSession.NamespaceGc` provide capability-gated send/receive adapters for roster, Origin frontier, retention checkpoint and installed ACK.
- Storage restart/fence tests and SyncSession frame-exchange/capability tests have been added. The first CI exposed only two assertions that omitted the automatically negotiated `IncrementalAliasMap`; fixed in `43efda1`, exact-head CI pending.

Still required before v3.1 conformance or any GC enablement:

1. Complete the FULL_REPLACE BEGIN/CHUNK/COMMIT/ABORT transfer protocol and durable receiver state machine.
2. Wire received signed messages to the durable namespace-GC store and implement authority/member lifecycle, periodic frontier publication and checkpoint generation.
3. Integrate forgotten-origin replay rejection with the ChangeId ledger and prove safe deletion against all live references across provider, PendingApply, reservations, relay/outbound payloads, snapshots and leases.
4. Add crash/fault injection at every persistence and generation-switch boundary, multi-peer/rejoin/resurrection and adversarial signature/rollback/fork tests.
5. Verify exact wire compatibility, frame registry uniqueness, bounded memory/CPU and exact-head CI before enabling ChangeId or tombstone/Ghost GC. Both remain disabled.


## 10.1. C-pass update — FULL_REPLACE transfer foundation (2026-11-08)

The code-first pass has added:

- Canonical BEGIN manifest envelope fields: namespace, generation, roster generation/digest, source feed watermark, expected entry count and SHA-256 digest.
- Bounded CHUNK codec: at most 4096 provider mutations and no more than the frame payload limit; exact lengths, canonical ProviderMutation decoding and trailing-byte rejection.
- COMMIT/ABORT transfer identity envelopes.
- Capability-gated SyncSession send/receive adapters on a dedicated per-namespace transfer stream.
- Receive dispatcher acceptance of v3.1 frame IDs through FULL_REPLACE_ABORT.

This is only the wire/transport foundation. It does **not** yet implement the receiver state machine, signed authoritative snapshot manifest verification, durable provider staging orchestration, sequence/page continuity across calls, crash recovery, or installed-ACK publication. The receive adapter now requires an explicit expected frame sequence and validates the dedicated transfer stream. The BEGIN manifest now carries a domain-separated Ed25519 signature and the receive adapter verifies it against the independently verified roster and pinned PreShared authority key. This authenticates the manifest, but does not itself authorize provider mutation: the receiver must still enforce the roster member/transport binding, durable staging continuity, commit guard and installed-ACK persistence. The provider's existing Begin/Stage/Commit primitives remain caller-controlled and the old generation remains retained.

The previous exact-head run at `640af0d` built successfully and ran Fuzz/Lab successfully, but 677/678 Core tests passed. The single failure was caused by the session receive dispatcher still rejecting frame types above 0x13; both remaining guards were corrected in `e012460`. The corrected head has not yet been re-tested. No v3.1 conformance or GC enablement is claimed.


## 10.2. C-pass update — receiver state machine and resumable transfer (2026-11-08)

The FULL_REPLACE transport foundation now includes a complete sender/receiver code path:

- `FULL_REPLACE_READY` (0x1C) reports the durable receiver cursor, expected count/digest, committed state and next frame sequence.
- Transfer pages are canonical 4096-entry pages except the final page. This makes restart position and frame sequence deterministic; the receiver rejects short non-final pages, gaps, overlaps and out-of-order pages.
- `StorageFullReplaceSender` holds one provider snapshot across digest and transfer passes, signs the authority manifest, resumes from READY, sends COMMIT and persists the verified signed installed ACK.
- `StorageFullReplaceReceiver` validates the pinned authority and roster, preflights its own signing key before provider mutation, resumes durable staging, detects ABORT while waiting for data, and commits only while holding an application-supplied exclusive cross-store guard. It refreshes the live Node cache and persists its signed installed ACK after the provider switch.
- `IProviderFullReplace.TryGetFullReplaceProgress` exposes the durable staging cursor; `StorageDataProvider` implements it from the persisted staging manifest.

This remains a C-pass implementation, not a T-pass proof. The commit guard is deliberately mandatory and supplied by the embedding application; it must serialize remote APPLY and local provider publication and prove there are no unresolved PendingApply intents, relay/outbound reservations, provider feed intents or reader pins that depend on the old generation. The receiver does not silently assume these cross-store facts. Old generations remain retained. No ChangeId, tombstone or ghost GC is enabled.

Exact-head build, unit/integration tests, fault injection, reconnect/resume, multi-peer/rejoin and platform durability verification remain pending.


## 10.3. C-pass update — READY/resume and current-roster commit fence (2026-11-08)

`FULL_REPLACE_READY` is now assigned frame `0x1C` on the dedicated transfer stream. It reports generation, manifest digest, expected count, durable next index, committed state and the next expected transfer sequence. The sender resumes from this cursor using canonical 4096-entry pages; the receiver detects ABORT concurrently with CHUNK/COMMIT waits and rejects ambiguous same-sequence terminal/data frames. The receiver rechecks the current signed roster while holding the required commit lease, before changing the provider's active-generation pointer.

The sender validates that the second snapshot pass still ends at the signed count. Provider progress reads use a direct single-row lookup instead of cloning a new Storage snapshot. The remaining snapshot memory issue is recorded in the roadmap: provider `OpenSnapshot` still materializes active rows into an additional array, and the Storage engine snapshot itself copies the partition. The digest builder alone does not make the whole source path memory-bounded.

No exact-head CI, fault-injection, resume/reconnect, or multi-peer proof has yet been run for this C-pass slice. The mandatory commit lease remains an embedding-application contract and must cover pending applies, local provider intents, immutable relay/outbound reservations, active leases and reader pins. No destructive GC is enabled.


## 10.4. C-pass update — origin provenance and bounded GC (2026-11-08)

Provider mutations now retain the immutable `ChangeId`, origin author epoch and origin sequence in the current provider state, local feed and FULL_REPLACE digest. This closes the prior gap where a record tombstone had no durable origin tuple to compare against the namespace retention frontier. Only the canonical 81-byte-header provider mutation encoding is accepted; older layouts are rejected without a migration decoder.

Three bounded GC paths now exist in code:

1. **Receipts:** each receipt persists the batch's maximum origin sequence. It is eligible only below the signed safe frontier for its durable author/issuer epoch, with no PendingApply reference. Receive cursors and digest chains are retained, so an old replay cannot be mistaken for a new cursor continuation.
2. **ChangeId ledger:** rows are eligible only below the signed safe frontier, with no MetadataStore relay/outbound/pending reservation reference and with a mandatory external lease for provider/outbox/reader references. The retained checkpoint rejects any operation whose ChangeId row has been forgotten but whose origin is already covered.
3. **Record tombstones:** collection requires the active provider generation to be the checkpoint's FULL_REPLACE generation, all Active members to have installed its exact digest, and the tombstone origin to be covered. Older hidden field rows are removed in bounded batches while the tombstone remains; only after no older field remains is the tombstone deleted. Interrupted cleanup is repeatable.

The external GC/commit lease is mandatory and has no permissive default. It must exclude concurrent Node mutation, PendingApply, provider feed publication, relay/outbound reservations, FULL_REPLACE staging and reader pins. The coordinator exposes attachment and compaction APIs, but the embedding application still has to supply a real lease implementation. No GC should be enabled until T-pass tests prove these conditions and crash recovery.


## 10.5. Tombstone horizon proof correction — physical GC disabled

A safety review found that the v3.1 tombstone proof is not sufficient to authorize deletion. `SafeAppliedOriginSequence` for the tombstone's own author plus all-member installation of a FULL_REPLACE snapshot does not rule out a delayed operation from another author: that operation may be above the common sequence frontier, absent from the snapshot, and still carry an HLC older than the tombstone. If the tombstone is removed, that delayed operation could resurrect the record.

Accordingly, `StorageNamespaceGcStore.HasProtocolTombstoneGcProof` now fails closed even when its existing sequence and installed-ACK checks pass. `StorageDataProvider.CompactRecordTombstones` retains its bounded crash-repeatable cleanup code but cannot physically reclaim any tombstone under v3.1. This is intentional, not an unfinished test condition.

Before enabling tombstone GC, the protocol must bind (a) each origin author's signed issued high-watermark, (b) every Active member's applied-through value reaching that watermark, (c) a global HLC floor bound to the exact FULL_REPLACE generation/digest, and (d) advancement of each installing Node's HLC before activation. Retired-origin epochs need an explicit closure proof. Until then, tombstones remain retained.


## 10.6. Provider origin metadata and v2 snapshot digest

Provider mutations now encode the immutable `ChangeId`, origin author epoch and origin sequence. The provider mutation encoding has an 81-byte fixed header; the prior 53-byte header is rejected; no fallback decoder is present. New local feed entries persist the same ChangeId in the mutation and feed envelope and reject identity mismatch. FULL_REPLACE digest domain is now `BIN-SYNC/FULL-REPLACE/v2`, so origin provenance is covered by the snapshot digest. New receipt values also persist the maximum origin sequence in the batch; old receipt encodings are rejected; no migration decoder is present.

The ChangeId and receipt compaction entry points are wired through `StorageSyncCoordinator` and require the same StorageDatabase GC guard plus an external lease. The replay fence rejects a missing ChangeId row when the signed origin frontier covers that operation. The provider tombstone cleanup code is present but physical deletion is disabled until the global causal-horizon proof is implemented; see §10.5. No build or tests have been run in this C pass.


## 10.7. Canonical encoding policy

The v3.1 implementation uses canonical encodings only: provider mutations require the 81-byte fixed header, local feed envelopes require the canonical mutation plus exactly one ChangeId, and receipt values require the origin-sequence-bearing format. Earlier in-development layouts are rejected; no hidden compatibility decoder or automatic migration path is provided. Ordinary field materialization from Node LWW state may carry zero operation provenance, but record tombstones and immutable remote/local operations must carry complete ChangeId/origin identity.


## 10.8. Receipt deletion retains the transport replay fence

`CompactReceipts` requires the corresponding durable receive cursor to cover the receipt's `CursorEnd` and the sender digest-chain row to remain present, in addition to the signed origin frontier and no PendingApply reference. It deletes only the receipt row; receive cursor and digest-chain rows are deliberately retained. This makes an old cursor continuation fail after receipt pruning rather than appearing to be a fresh batch. Exact lost-ACK and reconnect behavior remains a T-pass obligation.


## 10.9. Provider snapshot enumeration and GC enablement

`StorageDataProvider.OpenSnapshot` now retains a pinned storage snapshot and streams the two current-state prefixes in canonical merged order using persistent sorted-prefix cursors with 1024-row internal scan pages. This removes the extra full provider-row array and preserves a stable two-pass FULL_REPLACE view. The underlying `StorageDatabase.OpenSnapshot` still clones the partition, so total snapshot memory is not yet O(page).

Physical receipt, ChangeId and tombstone collection are all explicitly disabled in v3.1 (`IsReceiptGcProofEnabled`, `IsChangeIdGcProofEnabled`, `IsTombstoneGcProofEnabled` are false). Receipt eligibility code additionally requires the sender's durable retired-transport-epoch fence, retained receive cursor/digest chain, safe origin frontier and no PendingApply. These compaction methods are plumbing only until exact-head proof tests and the remaining causal-horizon/application-lease work are complete.


## 10.10. Canonical provider mutation v2 wire/storage layout

The v3.1 FULL_REPLACE/provider-state mutation encoding has an 81-byte fixed header followed by `Value[ValueLength]`. For an unpublished local draft, ChangeId and OriginAuthorEpoch are present while OriginSequence is zero; published operations and remote DELTA_BATCH apply require a non-zero OriginSequence:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 16 | RecordId |
| 16 | 2 | FieldId (LE) |
| 18 | 1 | flags (`IsTombstone`, `IsRecordTombstone`) |
| 19 | 8 | HlcWall (LE) |
| 27 | 4 | HlcLogical (LE) |
| 31 | 16 | original AuthorNodeId |
| 47 | 2 | KeyEpoch (LE) |
| 49 | 4 | ValueLength (LE) |
| 53 | 16 | ChangeId |
| 69 | 4 | OriginAuthorEpoch (LE) |
| 73 | 8 | OriginSequence (LE) |
| 81 | ValueLength | Value |

The 81-byte canonical encoding is required; the earlier 53-byte layout is rejected. A local provider feed envelope is exactly this canonical mutation followed by one additional `ChangeId[16]`, which MUST equal the embedded ChangeId. Record tombstones and immutable local/remote operations require complete ChangeId/origin identity. Ordinary Node LWW field materialization may have no operation identity, but it does not create a record tombstone or an immutable PublishedOperation. FULL_REPLACE digest domain is `BIN-SYNC/FULL-REPLACE/v2` and hashes the canonical mutation encoding, including provenance.

Receipt values persist the batch maximum `OriginSequence`. Canonical lengths are 76/80 bytes without a header fingerprint and 92/96 bytes with it; the 4-byte transport-peer epoch is present in the 80/96-byte variants. Earlier receipt layouts are rejected.

All physical compaction gates are currently false. In addition to the signed origin frontier and retained receive cursor/digest-chain fence, receipt eligibility requires the issuer to be durably Retired and the stored transport-peer epoch to be at or below `RetiredThroughEpoch`. These are implemented guards, not permission to enable receipt GC before the T-pass lost-ACK/retirement proof.


## 10.11. Member applied-origin frontier and authority checkpoint production

`StorageSyncCoordinator.PublishAppliedOriginFrontier(memberSeed)` produces the local Active member's signed OFR2 report from immutable origin identities in the durable Metadata Store ChangeId ledger only. Unpublished provider-feed drafts are excluded: they have no OriginSequence yet. For each `(OriginAuthorNodeId, OriginAuthorEpoch)`, the report advances only through the contiguous prefix after the prior durable report; gaps stop advancement. The report is signed with the member key and persisted by `StorageNamespaceGcStore.AcceptOriginFrontier`.

`StorageSyncCoordinator.PublishRetentionCheckpoint(authoritySeed, fullReplaceGeneration?, fullReplaceDigest?)` reads the latest durable reports and builds RCP2 using the minimum contiguous applied sequence across every Active member. A missing Active-member report conservatively prevents a safe frontier for that origin. Optional FULL_REPLACE binding is accepted only after every Active member's signed installed ACK is durable. The checkpoint is authority-signed and persisted with the existing monotonic sequence/digest-chain checks.

## 10.12. FULL_REPLACE HLC floor must be installed before ACK

`StorageFullReplaceReceiver.Commit` computes the maximum `(HlcWall,HlcLogical)` in the committed provider generation and invokes its mandatory refresh callback before creating/persisting the signed installed ACK. The intended callback is `StorageSyncCoordinator.RefreshAfterFullReplace(persistNode, wall, logical)`: it advances `Node` HLC to the floor, rehydrates the Node cache from the committed provider generation, replays durable PendingApply intents, and persists the refreshed Node state. `Node.AdvanceClockToFloor` is a restore operation and deliberately does not apply remote clock-skew rejection. The caller must provide this callback; ACK must not be emitted if refresh or persistence fails.

These are C-pass production APIs. Exact-head tests for gaps, report/checkpoint rollback, crash/retry, callback ordering and all-member installation remain T-pass obligations. Physical receipt, ChangeId and tombstone/ghost GC remain disabled.


## 10.13. OriginSequence is allocated at immutable freeze, not provider-feed append

The local provider feed has its own monotonically increasing feed cursor. That cursor is not an operation-origin sequence. `AppendLocalChange` assigns a stable ChangeId and author epoch but leaves OriginSequence zero while the change is still unpublished/coalescible. This draft identity is valid in local provider state and FULL_REPLACE state images; `ProviderMutationCodec.ValidatePublished` requires a complete non-zero origin tuple before remote operation apply.

`StorageMetadataStore.FreezeOutbound` allocates OriginSequence atomically with the outbound operation and global ChangeId ledger row. The allocator is durable and keyed by `(NamespaceId, AuthorNodeId, OriginAuthorEpoch)` (storage key kind 22), separate from the transport/outbound cursor. Coalesced-away drafts consume no origin sequence, so published operations remain contiguous. After freeze, `StorageSyncCoordinator` calls `IProviderPublishedOriginStore.CommitPublishedOrigins` before acknowledging the provider feed; `StorageDataProvider` stamps only the matching current-state winner with the assigned sequence. If stamping or feed ACK fails, retry reads the same frozen identity from the ChangeId ledger and stamping is idempotent. A later FULL_REPLACE state-image replay may carry the draft form but cannot erase an already-published current-row origin.


## 10.14. FULL_REPLACE HLC floor is signed by every installed member and bound by RCP2

FRA2 extends the signed installed ACK with `HlcWallFloor[LE u64]` and `HlcLogicalFloor[LE u32]`. The receiver computes the maximum HLC in the committed provider generation, calls its mandatory refresh callback, and only then signs/persists FRA2. The callback can be created with `StorageSyncCoordinator.CreateFullReplaceRefreshCallback(persistNode)`; it advances the local Node HLC to the supplied floor, rehydrates the provider generation, replays PendingApply and persists the refreshed Node state. Failure prevents installed ACK publication.

RCP2 binds `FullReplaceGeneration`, `FullReplaceDigest`, `FullReplaceHlcWallFloor` and `FullReplaceHlcLogicalFloor` in the authority-signed checkpoint payload. `StorageSyncCoordinator.PublishRetentionCheckpoint` requires every Active member's durable installed ACK for that exact generation/digest and requires all ACKs to report the same HLC floor before creating the checkpoint. Checkpoints without a FULL_REPLACE binding must carry zero floor fields. RCP1/FRA1 are not accepted as fallback encodings.

This closes the FULL_REPLACE HLC-floor transport/persistence gap. The subsequent OFR2/NRS2 extension now carries author-issued high-watermarks and retired-origin closure; physical tombstone/ghost GC remains disabled until cross-store live-reference scans and adversarial T-pass proof are complete.


## 10.15. OFR2/NRS2 author closure and complete-issued horizon

OFR2 now carries, in the member-signed report:

- `IssuedOrigins[]`: each local author epoch's durable final issued OriginSequence plus the maximum HLC among all operations frozen in that epoch;
- `UnpublishedDraftCount` and maximum draft HLC, computed from the provider feed without treating feed cursors as origin sequences;
- `FinalForAuthor`, a terminal declaration that the member has stopped issuing local changes.

The origin allocator row (storage key kind 22) persists `(IssuedOriginSequence, MaxIssuedHlcWall, MaxIssuedHlcLogical)` per namespace/author/epoch. Coalesced drafts do not advance it. `StorageSyncCoordinator.PublishAppliedOriginFrontier(..., finalForAuthor:true, finalAuthorEpoch:...)` holds the application commit guard, closes every known local epoch plus the supplied current epoch in the provider, rejects any remaining feed drafts, and publishes the final signed report. The provider persists per-epoch closure markers plus a terminal author-identity marker, removes the active-epoch marker on retirement, and rejects all later local appends. After the member is Active in a later accepted roster, `StorageSyncCoordinator.ActivateLocalAuthorEpoch(newEpoch)` clears the terminal marker and installs a fresh epoch; previously closed epochs cannot be reused. The authority rejects reports after a final report.

NRS2 carries `RetiredOriginEpochs[]`, each with author, epoch, final issued sequence and maximum issued HLC. The authority may retire an Active member only after receiving its final signed OFR2 report and copying every issued epoch exactly into the new roster. Closed epochs are immutable and survive member rejoin; a rejoined member uses a new author epoch.

Retention checkpoint derivation is conservative: for an Active author it uses that author's signed issued high-watermark; for a Retired author it uses the NRS2 closure. An origin enters the safe frontier only when every Active member's signed applied frontier covers the entire final issued sequence. A FULL_REPLACE-bound RCP2 checkpoint additionally requires a report from every Active member with zero unpublished drafts and an installed FRA2 ACK from every Active member. The shared HLC floor must cover the maximum HLC of every issued author epoch. This closes the author-issued/retired-origin and pending-draft gaps in the causal horizon.

`StorageSyncCoordinator.AcquireApplicationCommitGuard()` acquires locks in coordinator→Node Storage→Outbox→Meta→provider/shared-Storage order, and rejects a durable Metadata Store/Data Provider pair that do not share one StorageDatabase. Final-author report creation holds this lease; the FULL_REPLACE receiver must receive `coordinator.AcquireApplicationCommitGuard` as its commit-guard factory. The receiver checks that the local provider feed is empty while the lease is held, then switches generation and runs the HLC refresh callback before ACK. Use the same lease together with the host's WAL/FLD lock for local GC calls. The receiver refuses to switch generations while local provider feed drafts remain. The physical GC feature flags remain false until the remaining cross-store live-reference scan and adversarial T-pass proof are complete.


## 10.16. OFR2 decoder and Node-inclusive application barrier (2026-11-09)

The OFR2 decoder reads and preserves the final-author flag, unpublished-draft count and maximum draft HLC before the applied-entry vector. These fields are part of the signed payload and are not advisory metadata; dropping or misaligning them changes the interpretation of all following lengths and can turn a valid report into trailing-byte rejection. A final report cannot carry pending drafts, and an empty draft set has a zero maximum HLC.

The application commit barrier now holds the coordinator gate, all Node gates in their documented Storage→Outbox→Meta order, and the provider's shared Storage transaction gate. The durable Metadata Store and Data Provider must use the same StorageDatabase or guard acquisition fails closed. `StorageNodePersistence.Save` follows the same Node-before-Storage lock order to avoid inversion. A host must still compose its WAL/FLD transaction lock if it is outside this shared database. These changes provide exclusion, not a cross-store atomic transaction; physical GC remains disabled until host-owned live references are included in the lease.


## 10.17. Coordinator-owned v3.1 control-plane exchange (2026-11-09)

`StorageSyncCoordinator` now provides durable orchestration around the negotiated `SyncSession` control frames:

- `SendAcceptedNamespaceRosterAsync` / `ReceiveAndAcceptNamespaceRosterAsync`;
- `PublishAndSendAppliedOriginFrontierAsync` / `ReceiveAndAcceptOriginFrontierAsync`;
- `PublishAndSendRetentionCheckpointAsync` / `ReceiveAndAcceptRetentionCheckpointAsync`;
- `ReceiveAndAcceptFullReplaceInstalledAckAsync`;
- `SendDurableFullReplaceInstalledAckAsync`, which refuses to send unless the exact local member ACK is already persisted and its roster epoch, generation, digest and HLC floor match.

Receive helpers first decode and authenticate through the session against the accepted roster, then preserve the original signature while submitting the object to `StorageNamespaceGcStore.Accept*` for monotonicity, current-roster and durable-persistence checks. Outgoing OFR2/RCP2 state is persisted before network send. A lost send is recoverable by sending the next monotonic report/checkpoint. The APIs exchange one control object per session; the host remains responsible for peer selection, periodic scheduling and retry policy.


## 10.18. Rejoined author reports preserve prior closed epochs (2026-11-09)

OFR2 verification is roster-aware: when the accepted NRS2 roster contains retired-origin closures for the reporting member, its report must include each closed epoch and reproduce the exact final issued sequence and maximum HLC. A rejoined member may report a fresh epoch, but cannot omit, advance or rewrite any previously closed epoch. This check runs for both wire decoding and in-memory report verification.


## 10.19. Provider terminal retirement validates the active epoch (2026-11-09)

The provider refuses to set the terminal author-identity marker while its persisted active epoch lacks the corresponding durable per-epoch closure marker. Rejoin also fails closed if a retired identity still contains an active-epoch marker. This prevents a mismatched `finalAuthorEpoch` from silently retiring the identity while leaving the real active epoch reusable.


## 10.20. Bounded linear report/checkpoint validation (2026-11-09)

OFR2 validation indexes local applied sequences and issued epochs once per report; immutable retired-epoch closure matching is also indexed. RCP2 derivation indexes each member's applied `(author, epoch)` vector once, then computes the all-Active-member minimum without rescanning report arrays for each origin. The canonical 4096-entry bounds remain unchanged. Performance acceptance and equivalence tests remain T-pass work.


## 10.21. Storage snapshot payload sharing (2026-11-09)

`StorageDatabase.OpenSnapshot` still copies the sorted key index for point-in-time isolation, but it now shares the immutable stored value arrays rather than cloning every payload. This is safe because commits copy caller-owned buffers on publication and replace value references instead of mutating arrays; snapshot APIs return owned copies to callers. Total snapshot index memory remains O(number of keys), not O(page), until the engine adopts a persistent immutable index/MVCC root.


## 10.22. Snapshot prefix cursors seek by sorted range (2026-11-09)

`StorageSnapshot` uses a sorted index with a lower-bound binary search for prefix and `afterKey` cursor startup. Cursor reads only matching key pages rather than scanning unrelated partition keys. Snapshot payload arrays are shared as immutable references; each returned `StorageEntry` owns its key/value copies. Empty prefixes and all-`0xFF` prefixes remain supported without special range sentinels. Page boundaries are unchanged.


## 10.23. Immutable freeze rejects author-epoch HLC regression (2026-11-09)

For each `(NamespaceId, AuthorNodeId)`, newly frozen operations must have HLC greater than or equal to the persisted maximum HLC across all of that author's epochs, including previously retired/rejoined epochs. A lower wall value or a lower logical value at the same wall is rejected before allocating the next OriginSequence. Equal HLC is permitted for operations from the same transaction; the stored maximum advances only when the new HLC is greater. This ensures the signed issued-HLC floor cannot be bypassed by publishing an older operation after a tombstone has been proven reclaimable.


## 10.24. Tombstone HLC must belong to its issued author epoch (2026-11-09)

The protocol tombstone proof rejects a candidate whose HLC exceeds the signed maximum HLC for its exact `(author, epoch)`, even if the global FULL_REPLACE floor is higher. It separately requires both the candidate tombstone HLC and the entire epoch's maximum issued HLC to be covered by the common installed FULL_REPLACE floor. This prevents a corrupted or mismatched row from borrowing another epoch's higher HLC floor.


## 10.25. Physical GC requires explicit host live-reference scanners (2026-11-09)

`IApplicationGcReferenceScanner.OpenScan(scopeId, localReplica, namespaceId)` is a mandatory host contract for physical receipt, ChangeId and record-tombstone reclamation. It returns one stable `IApplicationGcReferenceScan` whose receipt queries are keyed by sender/author epoch/transport epoch/batch id, in addition to ChangeId and record references. One scan object is reused for all candidates in the pass; host adapters should index source records once rather than rescan the WAL/Outbox/ApplyJournal per candidate. It checks references outside BIN-SYNC's Storage database, including host WAL/FLD outbox and pending application work, while the caller's composed application commit lease is held. `CompactChangeLedger` checks internal relay/outbound/PendingApply/reservation references and this external scanner. `CompactRecordTombstones` additionally requires the durable Metadata Store, checks internal pending apply/relay/outbound payloads by RecordId (malformed payloads fail closed), blocks on pending immutable reservations, and checks host ChangeId/record references. Provider, Metadata Store and namespace-GC store must share one StorageDatabase. These API gates do not enable physical GC; the host adapter and adversarial T-pass remain mandatory.


## 10.26. Host draft horizon and canonical cross-store lock order (2026-11-09)

\`StorageSyncCoordinator.AcquireApplicationCommitGuard(hostLeaseFactory)\` now acquires locks in the required order: coordinator → all Node gates → host WAL/FLD gate → shared BIN-SYNC Storage transaction gate. The returned lease releases in reverse order. The no-argument overload remains suitable only when no external host store participates. FULL_REPLACE and physical GC integrations must use the composed overload whenever FLD/WAL state is separate.

\`IApplicationGcReferenceScanner\` now also exposes \`ReadUnpublishedLocalDraftHorizon(scopeId, localReplica, namespaceId)\`. OFR2 publication requires both the scanner and a host lease factory; it combines host WAL/Outbox drafts with provider-feed drafts and carries the maximum HLC. A final-author report is rejected while host drafts remain, before any provider epoch is closed. The count is conservative; zero is the only authorization signal. The host adapter must account for WAL commits beyond the Outbox materialization watermark and unacknowledged Outbox entries without mapping source-local cursors into OriginSequence.

This closes the protocol-side gap where a zero provider-feed count could miss a local WAL commit not yet materialized into the provider. It does not claim that the FLD-specific adapter already exists. Update the host implementation and crash/race tests before any physical GC flag is enabled.


## 10.27. Final-author retirement binds the exact active epoch (2026-11-09)

The provider terminal-retirement operation now takes the declared final author epoch. It compares that value with the persisted active-epoch marker, requires the same epoch's durable closure marker, and persists the exact final epoch alongside the terminal identity marker. A retry after a crash must repeat the same final epoch; a different historical closed epoch cannot be substituted. Rejoin still requires a monotonically fresh epoch and an accepted Active roster entry.


## 10.28. One host reference snapshot per GC/frontier operation (2026-11-09)

The host scanner now opens a partition-scoped \`IApplicationGcReferenceScan\` while the composed application commit lease is held. The scan object supplies the unpublished draft horizon and indexed receipt/ChangeId/RecordId queries. OFR2, receipt GC, ChangeId GC and tombstone GC each open one scan and reuse it for the complete operation, avoiding candidate-count × host-history rescans.

FLD exposes the neutral \`FldDatabase.OpenReferenceSnapshot()\` / \`FldReferenceSnapshot\` primitive. It indexes dirty local state, unacknowledged Outbox entries and retained ApplyJournal field/relay payloads once; any unsealed WAL prefix conservatively makes every destructive-reference query return live and keeps the unpublished-draft horizon non-zero. This is required because restart recovery may reconstruct previously acknowledged/compacted Outbox rows from an unsealed WAL; final author closure must wait for source checkpoint/seal so those old writes cannot be republished under a fresh author epoch. The thin adapter still belongs outside FLD so no BIN-SYNC dependency is introduced into the database engine.


## 10.29. FULL_REPLACE requires an empty host draft horizon (2026-11-09)

\`StorageFullReplaceReceiver\` now requires the partition-scoped host reference scanner and scope id. While holding the composed application commit lease, it opens one host scan and refuses the generation switch if either the host unpublished draft horizon or provider local feed is non-zero. Because the FLD horizon remains non-zero while WAL is unsealed, callers must checkpoint/seal source WAL before FULL_REPLACE can commit. The same host scan/lease contract is used for OFR2 and physical GC.


## 10.30. Recover immutable provider origin after freeze-before-stamp interruption (2026-11-09)

The provider publication boundary now treats the durable ChangeLedger identity as sufficient to finish a provider stamp even if the outbound payload row has already been acknowledged/compacted. \`TryGetOutboundChangeIdentity\` returns fingerprint and immutable \`OperationOrigin\` in one ledger lookup. \`PublishLocalChangesCore\` collects already-frozen feed rows and replays their origin stamps before Node persistence and provider-feed ACK. \`ProviderPublishedOrigin\` carries the origin tuple directly; it no longer requires retaining a \`MetadataOutboundItem\`.

ChangeId compaction now builds its live set from the provider's pending local feed as well as relay/outbound/PendingApply state and the host \`IApplicationGcReferenceScan\`. Thus a crash after freeze but before provider stamp cannot lose the origin mapping to GC while the provider feed remains pending. A dedicated crash-window test belongs in Phase T.


## Implementation status update — shared FULL_REPLACE source generation

The host implementation now prepares one immutable provider snapshot and one signed manifest for recipient fanout. `PreparedFullReplaceSnapshot` is an in-process lease; it can be retained while retrying failed recipients and is validated against the accepted roster and authority signature before any retry network send. Convenience single-recipient and all-member methods also perform snapshot/manifest preflight before roster broadcast. This improves source-generation consistency but does not yet provide a durable snapshot token across sender process restart. The exact-head build/retry matrix remains a T-pass requirement; physical GC remains disabled.
