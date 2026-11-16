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

Keep `prepared` alive until the intended recipient set is complete or the generation is abandoned. The prepared token can be made restart-durable with `StorageFullReplaceSender.PrepareSnapshotAndPersist`, provided the provider implements `IProviderDurableSnapshotStore`. Persist the signed manifest and provider snapshot token before network transfer. After process restart, reopen the same durable provider and Metadata Store, call `RestorePreparedSnapshot` with the currently accepted roster/authority, then resume transfers using the restored handle. Do not reconstruct the same generation from a newer live provider state. After every intended recipient has completed (or the generation is explicitly abandoned), call `ReleasePreparedSnapshot`; call `PruneUnreferencedPreparedSnapshots` during startup recovery to reconcile interrupted release and orphaned provider snapshot tokens. The optional `FldDatabaseDataProvider` implements durable snapshot presence/pruning through FLD's persisted provider-snapshot generations. This sender recovery path is implemented in the C-pass and still requires exact-head restart/crash tests.
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
- Compose `AcquireApplicationCommitGuard(acquireHostCommitLease)` for physical GC/frontier work. FULL_REPLACE must use `AcquireFullReplaceCommitGuard(acquireHostCommitLease)`, which additionally holds the durable Metadata Store gate, proves no target-namespace PendingApply or unresolved outbound/relay reservation remains, and returns a lease bound to the exact scope/local replica/namespace and namespace-GC/trust-store instances. The host lease must cover the actual FLD/WAL/ApplyJournal source snapshot; the reference scanner and provider snapshot-GC capability must only open immutable views under that already-held lease and must not reacquire the host gate.
- `StorageMetadataStore` and `StorageNamespaceGcStore` also share the per-`StorageDatabase` writer-admission gate. Receipt/ChangeId reference-scan GC opens the Host scan first, then acquires Metadata admission before reading proof/candidate state; both remain held through the final bounded delete. Tombstone GC holds Metadata admission across provider snapshot enumeration and the final provider-delete call. Keep the FLD Host IPC lease as the cross-process provider/WAL/ApplyJournal fence, and never write adapter-owned keyspaces through raw `StorageDatabase` batches.
- Do not enable receipt, ChangeId or tombstone/Ghost physical GC. All three proof flags remain false until the composed scanner, membership fencing and adversarial T-pass are complete.
- The optional `samples/FldBinSyncHost` now implements both the `FldDatabase`→`IDataProvider` snapshot/local-feed/author-epoch/remote-apply/FULL_REPLACE bridge (`FldDatabaseDataProvider`) and the relay-page/RPM3/publication/ACK/startup-recovery/reference-scan adapter (`FldBinSyncHostAdapter`) around one exact `StorageSyncCoordinator` composition. It is a complete source/metadata host integration sample, but not a transport executable or separately published integration package; cross-repository build and end-to-end T-pass proof remain. See `FLD-DB-INTEGRATION-CONTRACT.md` §§9.6–9.8.

This is an orchestration recipe, not a background scheduler implementation. The application chooses cadence and transport policy while protocol/storage code remains deterministic and explicitly invoked.


## FULL_REPLACE sender HLC confirmation

The authority computes the source snapshot HLC floor during the same digest pass, retains it in `PreparedFullReplaceSnapshot`, and checks every receiver FRA2 wall/logical pair against that exact source floor before persisting the ACK. A signature alone proves which member made the claim; this equality check proves that the claim matches the authority's source generation. Do not accept an ACK that reports a higher/lower floor or only compare recipients with each other.


### Prepared source snapshot lifetime

A host may retain one `PreparedFullReplaceSnapshot` for multi-recipient fanout/retry. `SendPreparedSnapshotAsync` takes an internal transfer lease before preflight and holds it until the asynchronous send completes. The owner may call `Dispose` from a cancellation/failure cleanup path without invalidating an in-flight read; it prevents new sends and defers the provider token release until the last transfer exits. Still keep ownership explicit and dispose after intended fanout; this lease is process-local and is not a restart token.


### Joining promotion and HLC floor

After a Joining member has completed FULL_REPLACE and its signed FRA2 is durably stored, promotion checks that both HLC floor components in that ACK exactly match the prior accepted FULL_REPLACE-bound RCP2, in addition to generation/digest, roster generation and transport epoch. A mismatch leaves the member Joining; do not promote using only a matching snapshot digest.


### v3.1 capability dependencies

The session enforces the same dependency masks in both directions: NRS2 requires `NamespaceRosterV31`; OFR2 and RCP2 require both `NamespaceRosterV31` and `OriginFrontierV31`; all FULL_REPLACE frames and FRA2 require both `NamespaceRosterV31` and `FullReplaceV31`. A partial capability intersection can still establish a v3.0-capable session, but the dependent v3.1 operation throws before transport I/O. The host should treat that as a capability incompatibility and must not retry the same v3.1 operation through a v3.0 frame fallback.


### HELLO capability preflight

The session bootstrap validates the v3.1 dependency mask on both encode and decode: OFR2/RCP2 and FULL_REPLACE capabilities require NRS2/namespace-roster support. An invalid local advertisement fails before HELLO/HELLO_ACK is emitted; an invalid remote advertisement is rejected before the session is established. The host should treat this as protocol incompatibility, not retry the same feature with a v3.0 fallback.

## 5. Physical-GC readiness and bounded async maintenance

The process-isolated sample adapter `FldBinSyncAsyncHostAdapter` exposes:

- `GetGcReadinessAsync(gcStore, trustStore)`: reports the accepted signed roster/checkpoint state, FULL_REPLACE installation state, unpublished local draft horizon, provider capabilities, and independent readiness for Receipt, ChangeId and Record/Field Tombstone GC. It opens and disposes one read-only Host reference scan; it does not delete rows or change feature flags.
- The read-only report includes `HasSnapshotPin`. Any active immutable provider snapshot pin appears as a global blocked reason for all three domains. The async Metadata compactors independently recheck this veto before candidate deletion.
- The report also exposes PendingApply count, outstanding peer-reservation count, and retained relay/outbound payload counts from `StorageMetadataStore.GetGcReservationDiagnostics`. Outstanding peer reservations block ChangeId and Tombstone readiness; PendingApply and retained payload counts are visible while exact candidate checks preserve candidate-level correctness.
- `RunGcMaintenanceAsync(gcStore, trustStore, limit, cancellationToken)`: executes at most one bounded page per independently enabled and globally ready domain, sequentially. Cancellation is checked between domains. Durable compaction cursors make later invocations resume safely.
- Each result carries per-domain enablement, prerequisite status and explicit global blocked reasons, plus reclaimed counts.

Readiness is deliberately weaker than candidate authorization. The compactor still validates each Receipt's retired transport epoch and receive-chain fence; each ChangeId's signed origin frontier and all Metadata/Host references; and each field/record tombstone's signed origin proof, installed FULL_REPLACE generation, exact provider identity and Host reference/pin state. A readiness report must never be used to turn on a feature flag.

All three `StorageNamespaceGcFeatureOptions` switches remain false by default. The embedding application owns cadence, scheduling, logging, retention policy and the operator kill switch. The sample performs one bounded round only; it does not create a background timer or silently enable GC.


### Stable blocker codes and durable operator policy contract

`FldBinSyncGcDomainReadiness.Blockers` provides stable `FldBinSyncGcBlockerCode` identifiers paired with the human-readable `BlockedReasons`. Existing string consumers remain supported. Unknown messages map to `Unclassified`; consumers must preserve the message and must not interpret the code as candidate authorization.

The optional Host sample defines `IFldBinSyncGcOperatorPolicyStore`, `IFldBinSyncGcOperatorAuthorizer` and `FldBinSyncGcOperatorPolicyCoordinator`. A host implementation must atomically compare-and-swap the policy generation and append its audit entry in one durable transaction. The policy is bound to Metadata scope, local replica and namespace; generations and audit sequences are monotonic; actor, action, reason and independently selected domains are recorded. The authorizer is application supplied. Call `RestoreAsync` before scheduling maintenance. Missing/corrupt/unavailable state fails closed. Policy commits before runtime gates open and can never override false startup GC feature options.

`EmergencyStop()` remains an unconditional process-local action and does not depend on the durable policy/audit store. This sample deliberately does not provide a production store adapter, identity provider or remote admin API. Multi-process CAS, concrete durable reopen/corruption/backup tests and Windows power-loss evidence remain open. All physical-GC flags remain false by default; Ghost GC remains excluded.

The durable policy store also exposes `TryAcquireMaintenanceLeaseAsync`. The embedding host must acquire the lease for the exact domain before each bounded page and hold it until completion. Acquisition atomically validates the current generation and domain enablement; updates that disable/change a domain must wait for leases to drain. A null lease means skip that domain's page and disable only that local domain; other independently enabled domains remain eligible. Store read/corruption failures throw and fail closed by stopping the entire local runtime control. The sample coordinator exposes `TryAcquireMaintenanceLeaseAsync`, but the current adapter does not yet wire this cross-process lease internally; until that integration lands, callers must wrap every GC entry point themselves. The process-local emergency Stop remains immediate and independent of the lease store. RestoreAsync does not clear the emergency latch; only a new explicit, authorized policy update can reopen domains.

For production composition, construct `FldBinSyncAsyncHostAdapter` with the durable policy coordinator and `requireDurableOperatorPolicy: true`. This makes the coordinator mandatory: readiness reports `DurableOperatorPolicyUnavailable` when absent, and all combined/legacy GC entry points refuse maintenance without it. When supplied, the adapter acquires and holds the per-domain durable lease around each bounded compaction operation, releasing it only after the page completes. The constructor's optional non-required mode exists for compatibility/tests and must not be used as production policy enforcement.





### FLD Tombstone GC sweep contract (first implementation block landed)

FLD physical Tombstone GC is one logical namespace sweep, not a bounded deletion quota. The adapter limit argument is the provider metadata page size (1..256); the coordinator reads the entire immutable snapshot to exhaustion and commits all currently proof-approved candidates in one Host operation. Receipt and ChangeId Metadata GC keep their independent bounded-page/cursor semantics.

The implemented process-isolated flow is:

1. BIN-SYNC acquires the Tombstone policy lease and Metadata writer-admission lease for the complete logical operation.
2. The FLD provider opens one immutable candidate snapshot. The coordinator advances one monotonic page cursor, checks the existing signed proof and Metadata references page by page, and streams only approved exact identities to the Host-owned approval spool in bounded batches.
3. The candidate snapshot is released before the Host-wide GC lease is acquired.
4. The provider opens a Tombstone-specific Host reference scan. The Host seals committed WAL before capturing that final reference snapshot. A failed seal, outstanding provider snapshot pin or unpublished draft fails closed.
5. The Host commit requires the lease owner and active reference scan, reuses that same post-seal snapshot for exact ChangeId/RecordId reference checks, consumes the ordered approval spool during one FLD compaction, and publishes the replacement Runs through one Manifest update.
6. The Host lease fence and database commit guard remain held through Manifest publication. The final synchronous commit is not interrupted by caller cancellation once it starts; cancellation before that boundary aborts the spool and releases admission.

The Host spool is temporary scratch, not a journal or resume source. It validates strict RecordId/FieldId order and uniqueness, seals count + SHA-256, is bound to one IPC client/session, is capped at 2M entries and four active sessions, and is deleted on commit, abort, disconnect, lease cleanup and Host startup. No candidate-array compatibility loop remains in the provider interface or FLD IPC client/dispatcher.

The two-Host regression uses page size 1 with two safe tombstones and verifies that one logical pass reclaims both on each Host, then restarts both Hosts and verifies the complete sweep remains durable.

The FLD provider snapshot no longer constructs a namespace-wide RecordId array or deduplication HashSet. It merges the sorted RecordId slot cursors from immutable Runs with a min-heap whose retained state is proportional to Run count; duplicate RecordIds across Runs are yielded once. The snapshot's existing RecordCount is preserved through a bounded-memory count pass at open, followed by the sequential candidate-page cursor. Exact FLD HEAD `99e8175368dc3b9641c2e3133ac6d04b02ac2a18` passed Ubuntu/Windows/macOS .NET CI (run [43835665775](https://github.com/smersh1307n2/FLD-DB/actions/runs/43835665775)).

Metadata reference checks are page-bounded and do not materialize the namespace-wide live-record HashSet, but each page currently scans retained PendingApply/Relay/Outbound payloads again. This is bounded-memory behavior with potentially repeated CPU work, not yet an end-to-end performance proof.

**Remaining gates:** Metadata reference payloads are currently rescanned once per candidate page, so CPU may grow with page count; overlay snapshot residency and peak managed heap/RSS are not yet measured; no composed 1M Tombstone candidate benchmark has run. Direct Metadata writer-bypass fencing, concrete durable operator policy/audit storage, Windows NTFS power-loss evidence, retention/rollback policy and staged canary remain open. All physical-GC flags remain default-off; Ghost GC remains excluded.
