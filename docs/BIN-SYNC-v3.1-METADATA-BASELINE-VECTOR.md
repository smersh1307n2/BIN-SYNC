# BIN-SYNC v3.1 — Proof-bound Metadata FULL_REPLACE baseline

**Status:** design contract for the next implementation slice. This document does not enable FULL_REPLACE baseline retirement or physical GC. All GC feature switches remain false.

## 1. Why a scalar snapshot watermark is insufficient

The provider snapshot watermark and BIN-SYNC stream coordinates are independent sequence domains. A valid FULL_REPLACE cut must bind the immutable provider snapshot to the Metadata state that existed at the same namespace commit cut. Never derive an outbound or relay offset from a provider mutation count, origin sequence, HLC, or the provider-local change-feed watermark.

The durable state has distinct coordinates:

- provider-local change-feed watermark;
- immutable origin (author, issuer epoch, origin sequence) frontier;
- local outbound feed (author, issuer epoch, outbound sequence);
- local relay feed (author, issuer epoch, relay sequence);
- per-peer send stream (peer, transport epoch, namespace, author stream) including issuer epoch, stream offset, ACK cursor and digest chain;
- per-sender receive stream (sender, transport epoch, namespace, author stream, issuer epoch) including receive cursor and digest-chain anchor.

Do not collapse these into one "author cursor". Outbound and relay are mutually exclusive source kinds for one (namespace, author) under the current invariant, but remain distinct coordinate domains in the proof.

## 2. Snapshot-bound baseline evidence

A baseline evidence package is versioned and canonical. Its signed envelope binds at minimum:

- namespace ID;
- accepted roster generation and digest;
- FULL_REPLACE generation GUID;
- provider snapshot digest and entry count;
- provider-local change-feed watermark;
- HLC wall/logical floor;
- the evidence sequence and issuer identity.

The feed section is a sorted, unique vector of rows:

    (source replica, target peer, target transport epoch, author stream, feed kind, issuer epoch, absolute feed offset, digest anchor)

Where:
- feed kind is exactly Outbound or Relay;
- source replica identifies the Metadata store whose feed is being described;
- target peer + transport epoch identify the peer stream whose send state will be initialized;
- author stream + issuer epoch identify the author stream carried by that feed;
- absolute feed offset is the source feed sequence captured at the snapshot cut;
- digest anchor is the 16-byte per-peer/per-author chain value agreed for the post-baseline stream. A snapshot reset uses the canonical all-zero anchor; a non-zero anchor is permitted only when both sides prove the exact same prior chain state.

The receive section is a sorted, unique vector of rows:

    (receiver replica, sender peer, sender transport epoch, author stream, issuer epoch, receive cursor, last batch digest)

It describes the receiver's already-accepted stream state, not the source feed offset. These rows are required to preserve streams that advanced beyond the snapshot while FULL_REPLACE was being transferred. A missing row is not interpreted as cursor zero unless the signed package explicitly declares that stream newly established at this generation.

The evidence is not a provider manifest extension by implication. It must use an explicitly versioned control-plane contract and a domain-separated signature. Existing FRA1/FRA2 and FULL_REPLACE frame layouts must not be silently reinterpreted. The implementation must first settle whether each member signs its own feed/receive section or the namespace authority signs a canonical aggregate of independently authenticated member reports. Authority signature alone must not fabricate another member's local Metadata cursor state.

## 3. Capture and admission

The source host must expose one composition operation, not a raw sender overload that callers can mistake for GC authorization. Under one typed namespace commit lease it must:

1. exclude local publication, remote APPLY/finalization, outbound reservation and ACK mutation, relay reservation and ACK mutation, roster/frontier acceptance, FULL_REPLACE commit and every direct Metadata writer;
2. open and pin the immutable provider snapshot;
3. capture the provider watermark and exact Metadata feed/peer/receive vectors from the same exclusion interval;
4. validate that no PendingApply, immutable peer reservation, unresolved provider intent, legacy ambiguous receive digest, or snapshot/reference pin makes the cut incomplete;
5. construct and sign the versioned evidence;
6. persist the provider token, manifest, evidence bytes and generation identity as one recoverable prepared-generation record before releasing the lease.

A per-coordinator semaphore is not a namespace-wide writer barrier. The production host must own the writer capability and prevent bypass through direct public Metadata mutation APIs. No thread-affine lock may cross an await.

## 4. Atomic receiver-side application

After validating the accepted roster/checkpoint, snapshot manifest, all Active-member FRA acknowledgements and the signed evidence, each host applies the baseline as one Metadata transaction. The transaction must cover:

- all affected outgoing PeerStreamState rows, including source kind, peer transport epoch, issuer epoch, absolute stream offset, ACK/pushed cursor and digest anchor;
- all affected incoming ReceiveState rows and per-author receive digest-chain rows;
- the generation/evidence digest idempotency marker;
- the completed-initial-sync marker for each affected peer/epoch.

Reject the transaction if any relevant peer stream has a pending immutable reservation, any PendingApply exists for the namespace, the roster or transport epoch changed, an existing stream is ambiguous, or the evidence does not cover the exact required stream set.

Do not reset a stream that advanced beyond the snapshot cut while transfer was in progress. Preserve its newer cursor and digest chain only when the persisted stream identity and epoch match the signed evidence. A stale or incomparable stream must fail closed and require a newer FULL_REPLACE generation; it must not be guessed from a scalar cursor.

The transaction must be idempotent for the same (namespace, roster generation/digest, FULL_REPLACE generation, evidence digest). Replaying identical evidence is a no-op. Reusing a generation with different evidence is a fork and must be rejected. A newer roster/transport epoch cannot inherit a baseline authorized under an older roster.

## 5. Compaction ordering

The durable baseline marker and both send/receive sides must commit before payload retirement begins. Payload compaction is a separate bounded, retryable operation:

1. commit the complete two-sided baseline transaction;
2. reopen/revalidate the exact durable baseline marker;
3. compact only contiguous outbound/relay payload prefixes proven by that marker and existing peer ACK requirements;
4. persist progress and retry idempotently after interruption.

A peer ACK alone is not a baseline. A FULL_REPLACE installed ACK alone is not a Metadata compaction marker. Provider snapshot installation alone is not proof that Metadata payload references can be dropped.

## 6. Required tests

- Canonical encode/decode/sign/verify vectors; sorting, duplicates, unknown enum, trailing bytes, wrong domain, wrong roster, wrong snapshot digest and tampered digest anchor.
- Distinct outbound and relay sequence domains; no derivation from provider watermark or origin sequence.
- Multiple peers with different transport epochs and different per-author digest chains.
- Interleaved authors and independent receive digest chains.
- Stream advanced during FULL_REPLACE transfer: preserve newer cursor/digest without regression.
- PendingApply, immutable reservation, legacy sender-wide digest ambiguity, stale roster and stale transport epoch all block baseline commit.
- Atomicity: crash before commit leaves no marker or partial stream rows; crash after commit leaves all send/receive rows and marker durable.
- Crash between baseline commit and payload compaction; reopen and retry compacts only the same contiguous prefixes.
- Lost FULL_REPLACE ACK, member rejoin, roster update, peer epoch replacement and conflicting generation/evidence.
- Two independent FLD Database Hosts: both apply the same proof-bound generation, both Metadata reference scans become empty, and both reclaim the same tombstone only after their own host-side reference revalidation.

## 7. Release rule

This contract is an implementation prerequisite, not a GC authorization. Production receipt, ChangeId, record-tombstone and field-tombstone GC remain disabled until the writer barrier, two-sided baseline transaction, two-independent-Host proof, async Host reference-snapshot/revalidation, crash/rejoin/resurrection matrix, physical-compaction crash tests, byte/cardinality audit, performance validation and exact-head platform CI are complete.


## 8. Durable member-report acceptance

The namespace GC metadata store persists one latest MBR1 report per
(namespace, accepted roster generation, member). Acceptance is serialized by
the same Storage transaction gate as roster, frontier, FULL_REPLACE ACK and
retention-checkpoint updates.

A report is accepted only when the exact member has a durable installed FRA2
ACK for the same FULL_REPLACE generation and snapshot digest, and the report's
transport epoch and HLC floor equal that ACK. The MBR1 signature still proves
ownership of the member-local cursor claims; the ACK proves that this member
installed the exact snapshot to which those claims are bound.

For a previously stored report, the store enforces:

- lower report sequence: reject;
- same report sequence and byte-identical signed envelope: idempotent success;
- same report sequence with different bytes: reject as a member fork;
- higher sequence: provider watermark and HLC floor cannot regress;
- every previously reported feed/receive stream must remain present;
- feed kind cannot change for an existing peer/author stream;
- feed offset and receive cursor cannot regress;
- an increased feed issuer epoch must reset its digest anchor to all zeroes;
- feed digest anchors may advance while the snapshot feed offset remains fixed, but the complete baseline apply must match the target member's signed receive digest for the same sender/transport epoch/author/issuer stream;
- an equal receive cursor must retain the exact digest anchor.

The row is committed before the acceptance API returns. Reopen revalidates
the stored signature, roster binding, key/member identity and requested
FULL_REPLACE generation/digest. Reports for superseded FULL_REPLACE generations
are not returned when collecting the current generation's vector.

This is still an evidence-collection step. It does not update
PeerStreamState, ReceiveState, digest-chain rows or initial-sync markers;
it does not compact payloads and does not authorize physical GC.


## 9. Two-sided digest reconciliation semantics

The feed offset is the source sequence cut covered by the FULL_REPLACE snapshot.
The receive cursor is the relative post-baseline DELTA_BATCH cursor. They are
different coordinates and must never be compared directly. For each active
source-to-target author stream, the aggregate report set pairs the source's
feed row with the target's receive row by source member, source transport
epoch, target member/transport epoch, author stream and issuer epoch.

The feed digest anchor and paired receive last-batch digest must match. The
receiver cursor is the source peer stream's post-baseline ACK cursor; the
source peer stream's absolute position is therefore
`feed.AbsoluteFeedOffset + receive.ReceiveCursor`. The source must prove
that this position does not exceed the corresponding local outbound/relay
high-watermark. Relay feed offset remains zero because FULL_REPLACE does not
replace relay history; relay cursor progress is represented by the paired
receive cursor.

A feed digest may be non-zero even when the snapshot feed offset is zero:
the peer may have accepted relay batches after the snapshot cut. A zero
receive cursor still requires a zero receive digest. A fixed snapshot offset
does not freeze the post-baseline digest chain; digest progress is accepted
only when the target member's signed receive report carries the same digest.
