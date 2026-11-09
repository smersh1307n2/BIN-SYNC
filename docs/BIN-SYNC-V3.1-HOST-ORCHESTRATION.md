# BIN-SYNC v3.1 host orchestration

This document is the application-owned orchestration pattern for the explicit v3.1 coordinator APIs. The optional `samples/FldBinSyncHost/V31NamespaceGcHostOrchestrator` now provides a reusable one-shot implementation of the authority/member round ordering below; it does not replace application-owned scheduling or transport ownership. BIN-SYNC core does not start timers, own background workers or decide transport cadence. The host owns session lifetime, cancellation, one receive loop per session, frame sequence allocation and retry policy. Implementing the v3.1 APIs and adopting this safe host ordering are mandatory for project completion; this guide itself remains non-normative and does not override the addendum's wire/storage rules.

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

A host may pipeline the per-session operations across different sessions, but must keep one reader per session and preserve the publish-before-send ordering. Serialize authority rounds at the host: do not publish a newer checkpoint until the current accepted checkpoint has been sent to every intended member. The send helper revalidates the currently accepted sequence/digest on every destination and fails closed if another round supersedes it.

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

## 3.1 One FULL_REPLACE generation across all recipients

When the authority must install one generation on every non-retired remote roster member, use `V31NamespaceGcHostOrchestrator.TransferFullReplaceToMembersAsync`. It validates the complete session set, sends the accepted roster, captures one immutable provider snapshot, computes/signs one manifest and streams that exact source generation to each recipient. Do not call the old one-recipient `TransferFullReplaceToMemberAsync` repeatedly when the recipients must share one digest: each standalone call opens its own source snapshot.

For a large fanout with partial failures, split preparation from transfer:

```csharp
using PreparedFullReplaceSnapshot prepared =
    orchestrator.PrepareFullReplaceSnapshot(
        authorityProvider, gcStore, trustStore, authoritySeed, generation);

foreach (SyncSession missingRecipient in recipientsStillNeedingInstall)
{
    await orchestrator.TransferPreparedFullReplaceToMemberAsync(
        prepared, missingRecipient, gcStore, trustStore, authoritySeed,
        cancellationToken);
}

// Bind the exact shared generation and digest into the next RCP2 only
// after all required installed acknowledgements have been durably accepted.
NamespaceRetentionCheckpoint checkpoint = coordinator.PublishRetentionCheckpoint(
    authoritySeed, prepared.Generation, prepared.Digest);
```

Keep `prepared` alive until the intended recipient set is complete or the generation is abandoned. The prepared token is an in-process immutable snapshot lease; it is not a durable restart token. After process loss, do not reconstruct the same generation from a newer live provider state. A crash-resumable source generation requires a durable provider snapshot capability and remains a C-pass item.
## 3. Joining member and FULL_REPLACE activation

1. Authority publishes a roster with the new member in Joining state and sends it.
2. The host performs the FULL_REPLACE snapshot transfer and atomic provider-generation install. The receiver must call `StorageSyncCoordinator.CreateFullReplaceRefreshCallback` so HLC floor, provider snapshot, PendingApply recovery and Node persistence complete before the installed ACK.
3. The member sends only its already-durable ACK with `SendDurableFullReplaceInstalledAckAsync`.
4. The authority accepts it with `ReceiveAndAcceptFullReplaceInstalledAckAsync` and persists the ACK.
5. Only after all required Active-member installation ACKs are durable may the authority publish an RCP2 checkpoint bound to that FULL_REPLACE generation/digest.
6. The authority promotes the member to Active in a new signed roster and sends it. The member accepts the Active roster, then calls `ActivateLocalAuthorEpoch` before producing local operations under the new epoch.

A Joining member is not treated as an Active retention participant. A Retired member cannot silently rejoin under the old author epoch.

## 4.1 Implemented host helper

The optional `V31NamespaceGcHostOrchestrator` in `samples/FldBinSyncHost` exposes:

- `RunAuthorityRoundAsync`: send the accepted roster to the supplied sessions, receive/accept one OFR2 per supplied session, verify the signed member identity and transport epoch match that session, publish one durable RCP2 and broadcast the same object.
- `BroadcastAcceptedCheckpointAsync`: retry a partial broadcast using the exact already-accepted checkpoint; the coordinator rejects a superseded checkpoint and never allocates a new checkpoint sequence here.
- `RunActiveMemberRoundAsync`: accept the authority roster, require the local member to be Active, publish/send OFR2 under the composed application barrier, then receive/accept RCP2.
- `ResendAcceptedFrontierAsync`: resend the persisted local OFR2 without creating a new ReportSequence.

The constructor takes a host-owned `Func<SyncSession, ulong>` frame-sequence allocator. The authority must call `PublishAuthorityMemberFrontier` (or supply `authorityMemberSeed` to the authority round) when it is itself an Active member. Missing Active members are not silently excluded: absent reports contribute zero in `PublishRetentionCheckpoint`. Joining members are rejected by the Active-member helper and must complete the FULL_REPLACE/installed-ACK/Active-roster sequence first.

## 4. Host scheduling and safety rules

- Use one receive owner per `SyncSession`; do not let a periodic task and an interactive task race to read the same session.
- Keep frame sequence counters per session and persist/recover them according to the transport contract.
- The authority must gather the round's member reports before publishing the checkpoint. Missing or stale Active-member reports remain conservative.
- Broadcast one accepted checkpoint object to every member in a round with `SendAcceptedRetentionCheckpointAsync`; do not mint a new checkpoint per peer.
- Compose `AcquireApplicationCommitGuard(acquireHostCommitLease)` for FULL_REPLACE and physical GC. The host lease must cover the actual FLD/WAL/ApplyJournal source snapshot.
- Do not enable receipt, ChangeId or tombstone/Ghost physical GC. All three proof flags remain false until the composed scanner, membership fencing and adversarial T-pass are complete.
- The optional `samples/FldBinSyncHost` now implements the FLD relay-page/RPM3/publication/ACK/reference-scan adapter around a preconstructed `StorageSyncCoordinator`. The full `FldDatabase`→`IDataProvider` snapshot/local-feed/remote-apply bridge is still an open C-pass item; do not claim the relay sample alone is a complete synchronized-FLD application. See `FLD-DB-INTEGRATION-CONTRACT.md` §§9.6–9.8.

This is an orchestration recipe, not a background scheduler implementation. The application chooses cadence and transport policy while protocol/storage code remains deterministic and explicitly invoked.


## FULL_REPLACE sender HLC confirmation

The authority computes the source snapshot HLC floor during the same digest pass, retains it in `PreparedFullReplaceSnapshot`, and checks every receiver FRA2 wall/logical pair against that exact source floor before persisting the ACK. A signature alone proves which member made the claim; this equality check proves that the claim matches the authority's source generation. Do not accept an ACK that reports a higher/lower floor or only compare recipients with each other.


### Prepared source snapshot lifetime

A host may retain one `PreparedFullReplaceSnapshot` for multi-recipient fanout/retry. `SendPreparedSnapshotAsync` takes an internal transfer lease before preflight and holds it until the asynchronous send completes. The owner may call `Dispose` from a cancellation/failure cleanup path without invalidating an in-flight read; it prevents new sends and defers the provider token release until the last transfer exits. Still keep ownership explicit and dispose after intended fanout; this lease is process-local and is not a restart token.
