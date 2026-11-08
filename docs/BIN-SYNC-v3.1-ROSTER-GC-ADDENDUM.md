# BIN-SYNC v3.1 Addendum — Signed Namespace Roster and Safe GC

**Status:** Proposed protocol extension; design checkpoint only. Not implemented and not part of v3.0 wire conformance.  
**Baseline:** BIN-SYNC v3.0.0 clean cut. Exact verified implementation checkpoint: `2f75b266c7c2d720d87b106573182393d5d15ea1` (Core 662/662, Fuzz/Lab 31/31, Storage Integration 168/168 on Ubuntu/Windows/macOS).

This addendum closes the missing proof needed before enabling global ChangeId GC and tombstone/Ghost GC. It deliberately does not weaken the current fail-closed behavior.

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

Proposed currently-unused frame type assignments (confirm against the final frame registry before implementation):

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

The existing `MEMBERSHIP` frame remains advisory gossip. It is not a signed roster and cannot advance a GC frontier. A v3.0 peer without the new bits contributes zero to every required frontier and blocks GC; no compatibility downgrade is attempted for core sync.

## 4. Canonical signed messages

All integer fields are little-endian unless explicitly stated. NodeIds and Ed25519 keys have fixed lengths. Lists are strictly sorted and unique. Every signed payload uses a domain-separation prefix and rejects trailing bytes.

### 4.1 NamespaceRosterCertificate (`NRS1`)

Canonical payload before signature:

- magic `NRS1`;
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
- Ed25519 signature[64] by the pinned Namespace Authority over `BIN-SYNC/NAMESPACE-ROSTER/v1 || canonical-payload`.

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

### 4.2 AppliedOriginFrontier (`OFR1`)

A member signs a bounded vector of contiguous, durably applied origin prefixes:

- magic `OFR1`;
- NamespaceId u16;
- RosterGeneration u64;
- RosterDigest[32];
- MemberNodeId[16];
- MemberTransportEpoch u32;
- ReportSequence u64;
- EntryCount u16;
- entries sorted by (OriginAuthorNodeId, OriginAuthorEpoch):
  - OriginAuthorNodeId[16];
  - OriginAuthorEpoch u32;
  - ContiguousAppliedOriginSequence u64;
- Ed25519 signature[64] by the member key bound in the accepted roster over `BIN-SYNC/APPLIED-ORIGIN-FRONTIER/v1 || canonical-payload`.

A frontier is an assertion that every operation in that origin stream through the sequence has been durably applied, not merely received. The member must persist the application before signing. Missing author entries mean zero. Reports must be monotonic; regression, wrong roster generation, wrong member epoch or unknown member key is rejected.

### 4.3 RetentionCheckpoint (`RCP1`)

The Namespace Authority verifies member signatures and computes a signed namespace-wide minimum:

- magic `RCP1`;
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
- optional fully-installed FULL_REPLACE generation[16 RFC-4122 network order] and digest[32];
- Ed25519 signature[64] by the pinned authority over `BIN-SYNC/RETENTION-CHECKPOINT/v1 || canonical-payload`.

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
8. Only then consider release of the v3.1 extension.

## 10. Open design decision

This proposal uses one pinned Namespace Authority. It is simple, compact and fail-closed, but membership changes and GC stop if that authority is unavailable. A threshold/quorum authority could remove this single-authority dependency, but it increases metadata, protocol complexity and test surface. Do not add quorum until there is a concrete requirement; the crash-fault model is the initial target.
