# BIN-SYNC v3.1 host orchestration

This document is the application-owned orchestration pattern for the explicit v3.1 coordinator APIs. BIN-SYNC core does not start timers, own background workers or decide transport cadence. The host owns session lifetime, cancellation, one receive loop per session, frame sequence allocation and retry policy.

## 1. Authority round

The authority sends the accepted signed roster to each negotiated member, receives one signed OFR2 frontier from each Active member, publishes exactly one durable RCP2 checkpoint for the round, then broadcasts that same checkpoint to all Active members.

```csharp
// One independent, monotonically increasing control-frame sequence per SyncSession.
foreach (SyncSession memberSession in activeMemberSessions)
{
    await coordinator.SendAcceptedNamespaceRosterAsync(
        memberSession, authoritySeed, NextFrameSequence(memberSession), cancellationToken);
}

// Do not run two concurrent receive operations on the same SyncSession.
foreach (SyncSession memberSession in activeMemberSessions)
{
    await coordinator.ReceiveAndAcceptOriginFrontierAsync(
        memberSession, cancellationToken);
}

// Publish once. Do not call PublishAndSendRetentionCheckpointAsync once per member:
// that creates a new checkpoint sequence for every destination.
NamespaceRetentionCheckpoint checkpoint =
    coordinator.PublishRetentionCheckpoint(authoritySeed);

foreach (SyncSession memberSession in activeMemberSessions)
{
    await coordinator.SendAcceptedRetentionCheckpointAsync(
        memberSession, checkpoint, authoritySeed,
        NextFrameSequence(memberSession), cancellationToken);
}
```

A host may pipeline the per-session operations across different sessions, but must keep one reader per session and preserve the publish-before-send ordering.

## 2. Active member round

After accepting the authority's current roster, an Active member publishes its signed applied-origin frontier under the composed application commit barrier, sends it, then receives and durably accepts the authority checkpoint.

```csharp
_ = await coordinator.ReceiveAndAcceptNamespaceRosterAsync(session, cancellationToken);

// The scanner must cover FLD/WAL/ApplyJournal plus any other external live references.
// The lease callback composes the FLD commit guard in coordinator → Node → FLD → Storage order.
await coordinator.PublishAndSendAppliedOriginFrontierAsync(
    session, memberSeed, externalReferenceScanner, acquireFldCommitLease,
    pageSize: 1024, frameSequence: NextFrameSequence(session),
    cancellationToken: cancellationToken);

NamespaceRetentionCheckpoint checkpoint =
    await coordinator.ReceiveAndAcceptRetentionCheckpointAsync(session, cancellationToken);
```

The host should schedule another round only after the prior per-session control exchange has completed or been explicitly cancelled/retried.

## 3. Joining member and FULL_REPLACE activation

1. Authority publishes a roster with the new member in Joining state and sends it.
2. The host performs the FULL_REPLACE snapshot transfer and atomic provider-generation install. The receiver must call `StorageSyncCoordinator.CreateFullReplaceRefreshCallback` so HLC floor, provider snapshot, PendingApply recovery and Node persistence complete before the installed ACK.
3. The member sends only its already-durable ACK with `SendDurableFullReplaceInstalledAckAsync`.
4. The authority accepts it with `ReceiveAndAcceptFullReplaceInstalledAckAsync` and persists the ACK.
5. Only after all required Active-member installation ACKs are durable may the authority publish an RCP2 checkpoint bound to that FULL_REPLACE generation/digest.
6. The authority promotes the member to Active in a new signed roster and sends it. The member accepts the Active roster, then calls `ActivateLocalAuthorEpoch` before producing local operations under the new epoch.

A Joining member is not treated as an Active retention participant. A Retired member cannot silently rejoin under the old author epoch.

## 4. Host scheduling and safety rules

- Use one receive owner per `SyncSession`; do not let a periodic task and an interactive task race to read the same session.
- Keep frame sequence counters per session and persist/recover them according to the transport contract.
- The authority must gather the round's member reports before publishing the checkpoint. Missing or stale Active-member reports remain conservative.
- Broadcast one accepted checkpoint object to every member in a round with `SendAcceptedRetentionCheckpointAsync`; do not mint a new checkpoint per peer.
- Compose `AcquireApplicationCommitGuard(acquireHostCommitLease)` for FULL_REPLACE and physical GC. The host lease must cover the actual FLD/WAL/ApplyJournal source snapshot.
- Do not enable receipt, ChangeId or tombstone/Ghost physical GC. All three proof flags remain false until the composed scanner, membership fencing and adversarial T-pass are complete.
- The synchronized-FLD JournalSeq provenance adapter is a later FLD integration phase; do not bridge directly to the current pre-extraction `FldDatabase` project graph. See `FLD-DB-INTEGRATION-CONTRACT.md` §9.6.

This is an orchestration recipe, not a background scheduler implementation. The application chooses cadence and transport policy while protocol/storage code remains deterministic and explicitly invoked.
