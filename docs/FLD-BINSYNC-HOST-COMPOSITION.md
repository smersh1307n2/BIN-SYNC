# FLD ↔ BIN-SYNC host composition

This is the application-layer binding between the protocol-neutral FLD database and BIN-SYNC. It belongs outside both runtime assemblies: FLD must not reference BIN-SYNC, and the FLD runtime remains exactly two assemblies (`Fld1.Storage` + `Fld1.Database`).

## Shared BIN-SYNC storage and partition

The BIN-SYNC `StorageMetadataStore`, `StorageNodePersistence`, namespace GC store and RPM3 `StorageRelayProvenanceMap` must share one `StorageDatabase`. FLD owns its separate FLD1 database directory and WAL/ApplyJournal.

```csharp
int namespaceId = checked((int)fld.NamespaceId);
var metadataStore = new StorageMetadataStore(syncStorage);
var map = new StorageRelayProvenanceMap(syncStorage, scopeId, localReplica);

// TODO C-pass: provide the generation-pinned FldDatabase -> IDataProvider adapter.
// Do not replace this with an in-memory mirror or a second independent database.
IDataProvider fldDataProvider = CreateFldDataProvider(fld);

var fldApply = new FldRemoteBatchApplyStore(fld);
var coordinator = new StorageSyncCoordinator(
    node, fldDataProvider, namespaceId, metadataStore, scopeId,
    remoteBatchCommitStore: fldApply);
var adapter = new FldBinSyncHostAdapter(
    fld, coordinator, map, scopeId, fldApply);
var relayGate = StorageRelayPublicationGate.For(coordinator);

IDisposable AcquireFldLease() => fld.AcquireCommitGuard();
```

Keep one `FldDatabase`, one `StorageSyncCoordinator` and one shared publication gate per local replica/scope/namespace. The RPM3 map is required only for legacy Node-owned publication. Never send directly through a session outside the adapter/gate. Before cutover, the gate captures the local Node Outbox ceiling plus every relay-author RPM3 ceiling under the composed commit guard. After provider-authoritative Metadata Store cutover, it captures the local outbound high watermark and every relay-author high watermark directly from the same Metadata Store; it does not read FLD relay pages or consult RPM3. The authority mode is captured with the watermarks so a concurrent cutover cannot mix sequence domains.

## Legacy Node-owned relay provenance (pre-cutover only)

This FLD JournalSeq→RPM3 projection is used only while Node Outbox remains the publication authority. After Metadata Store cutover, `FldRemoteBatchApplyStore` intentionally passes an empty `RelayWrites` set to FLD ApplyJournal; Metadata Store finalization owns relay payload, sequence allocation and delivery retention. Do not run the source-page callback or append RPM3 rows in that mode.
The gate invokes the source-page callback while holding the composed coordinator → Node → FLD → shared Storage commit barrier. The callback must not perform network I/O or acquire a second independent FLD lease. The FLD monitor-based guard is reentrant for the same thread, so public database read methods can be called inside the callback.

```csharp
IReadOnlyList<NodeId> ReadRelayAuthors() =>
    fld.ReadUnacknowledgedRelayAuthors()
       .Select(static bytes => NodeId.From(bytes))
       .ToArray();

(StorageRelaySourceChange[] Changes,
 ulong ScannedThroughJournalSeq,
 ulong SourceHighWatermark) ReadRelayPage(
    NodeId author, StorageRelayProvenanceCursor cursor)
{
    RelayBatchPage page = fld.ReadRelayBatchPage(
        author.ToArray(), cursor.ScannedThroughJournalSeq, limit: 256);

    var changes = new List<StorageRelaySourceChange>();
    foreach (RelayJournalBatch batch in page.Batches)
    {
        // RelayWrites is the exact relay subset selected during durable APPLY.
        // Preserve duplicate JournalSeq values: all operations from one ApplyJournal
        // record form an indivisible source batch and must not be split across pages.
        foreach (RemoteFieldWrite write in batch.Batch.RelayWrites)
            changes.Add(new StorageRelaySourceChange(
                batch.JournalSeq, BatchId.From(write.ChangeId)));
    }

    // Both values are read under the same FLD commit guard acquired by the gate.
    return (changes.ToArray(), page.ScannedThroughJournalSeq,
        fld.GetRelayJournalHighWatermark());
}
```

The gate persists only `(JournalSeq, OutboxSeq)` in RPM3. It resolves each transient ChangeId through the active BIN-SYNC outbound owner (Node Outbox before metadata cutover, Metadata Store relay rows after cutover). Do not persist another ChangeId index or copy operation payloads into RPM3.

## Startup recovery and send

Use the `RecoverAndPublishAllAsync(persistNode, ...)` adapter overload before opening any sender. It recovers PendingApply, checkpoints Node and freezes the local provider feed under the composed coordinator → Node → FLD → Storage barrier.

- **Before Metadata Store cutover:** startup recovery unions FLD unacknowledged relay authors with RPM3-only authors, validates source JournalSeq cursors and drains complete source pages into RPM3 before sending.
- **After Metadata Store cutover:** startup recovery switches to `RecoverMetadataOwnedAndPublishAsync`. It captures the local outbound plus every relay-author sequence directly from Metadata Store. No FLD JournalSeq scan or RPM3 mapping is performed. This is the required mode for the contextual FLD receive path because FLD ApplyJournal deliberately stores no duplicate relay payload.

The host send callback remains the same ceiling-enforcing entry point:

```csharp
ValueTask<bool> RecoverRelayAsync(CancellationToken cancellationToken) =>
    adapter.RecoverAndPublishAllAsync(
        persistNode,
        async (ceilings, ct) =>
            await adapter.PublishReadyOutboxAsync(
                session, node, peerStateStore, ceilings,
                cancellationToken: ct),
        cancellationToken: cancellationToken);
```

For each newly accepted remote batch, use `ReceiveAndPublishAllAsync(inboundSession, persistNode, ...)`. It routes through `ReceiveBatchBeforeAckAsync`: PendingApply → Node checkpoint → contextual FLD ApplyJournal receipt → Metadata receipt finalization → incoming ACK. Once cutover is active, that Metadata finalization atomically creates the relay payload and author sequence; the gate then captures Metadata Store ceilings and sends it without RPM3. If APPLY/finalization fails after a durable mutation may have happened, the corresponding gate remains closed until startup/PendingApply recovery. Network retries reuse immutable durable outbound reservations and never create a new ChangeId.

## Safe ACK and relay compaction

Two mutually exclusive retention paths exist:

**Metadata Store-owned mode (after cutover).** Each peer's ACK and immutable reservation are committed by `SyncSession` into `IMetadataPeerStateStore`. There is no FLD JournalSeq ACK and no RPM3 prune. Once all peer ACKs advance, call:

```csharp
ValueTask<IReadOnlyList<(NodeId Author, ulong SafeThroughSequence)>>
    CompactRelayAsync(CancellationToken cancellationToken) =>
    adapter.CompactAcknowledgedRelayPayloadsAsync(cancellationToken);
```

The coordinator computes the minimum durable per-peer safe cursor for each relay author and calls `IMetadataStore.CompactRelayThrough` under the composed application commit guard. An absent/incomplete peer remains at zero and therefore blocks compaction.

**Legacy Node-owned mode (before cutover).** The safe Node Outbox sequence is translated through RPM3 to a complete FLD JournalSeq; the host durably writes `FldDatabase.AcknowledgeRelayBatches` first, then RPM3 mapping rows are pruned. The old `AcknowledgeAllAsync` API is reserved for this mode and fails closed after Metadata Store cutover.

Do not run both paths or mirror relay payload/ACK state in FLD. A crash before Metadata Store ACK leaves the peer reservation retryable; a crash after ACK but before payload compaction is harmless because compaction is idempotently recomputed from durable peer cursors.

## GC reference snapshot binding

The host scanner acquires the FLD commit lease before building one immutable `FldReferenceSnapshot`. The BIN-SYNC wrapper owns both and releases the snapshot before the lease. Every callback is partition-scoped; the namespace is checked/cast to FLD's 16-bit receipt key domain.

```csharp
var scanner = new DelegateApplicationGcReferenceScanner((scope, replica, ns) =>
{
    IDisposable lease = fld.AcquireCommitGuard();
    try
    {
        FldReferenceSnapshot snapshot = fld.OpenReferenceSnapshot();
        FldDraftHorizon drafts = snapshot.UnpublishedLocalDraftHorizon;
        return new DelegateApplicationGcReferenceScan(
            lease,
            snapshot,
            new ApplicationDraftHorizon(
                drafts.Count, drafts.MaxHlcWall, drafts.MaxHlcLogical),
            (sender, authorEpoch, transportEpoch, batch) =>
                snapshot.HasReceiptReference(
                    sender.ToArray(), authorEpoch, transportEpoch,
                    checked((ushort)ns), batch.ToArray()),
            changeId => snapshot.HasChangeIdReference(changeId.ToArray()),
            (recordId, tombstoneChangeId) =>
                snapshot.HasRecordReference(
                    recordId.ToArray(), tombstoneChangeId.ToArray()));
    }
    catch
    {
        lease.Dispose();
        throw;
    }
});
```

Pass this scanner and `AcquireFldLease` into every BIN-SYNC physical-GC/frontier/FULL_REPLACE path that requires host state. Do not enable physical GC yet: first prove composed lock ordering and safe horizons for receipts, ChangeId and record tombstones/ghosts with crash/reopen and adversarial tests.

## Operational invariants

1. One shared publication gate serializes remote APPLY, outbound sends and the mode-specific ACK/compaction path for the namespace.
2. No send before startup PendingApply recovery and authority-specific publication reconciliation complete.
3. Before cutover, complete FLD JournalSeq pages are mapped atomically into RPM3; after cutover, Metadata Store owns relay payload and sequence directly and FLD ApplyJournal carries no duplicate RelayWrites.
4. RPM3 stores only sequence pairs and is used only by legacy Node-owned mode; ChangeId remains in the global BIN-SYNC ledger.
5. After cutover, peer ACKs are durable in Metadata Store and relay payload compacts only through the minimum durable peer cursor. Before cutover: safe Node ACK → durable FLD JournalSeq ACK → RPM3 prune.
6. A failed or ambiguous durable operation closes the corresponding publication gate until recovery; a network retry reuses immutable outbound state.
7. FLD1 WAL, ApplyJournal, Outbox, Run and Manifest formats remain unchanged by this adapter.
8. Physical GC remains disabled until the host reference snapshot and composed lock proof pass T/O gates.


## Optional source implementation

The optional cross-repository sample in BIN-SYNC at [`samples/FldBinSyncHost`](https://github.com/smersh1307n2/BIN-SYNC/tree/feature/fld-external-storage/samples/FldBinSyncHost) implements both the legacy FLD/RPM3 relay adapter and the direct Metadata Store-owned relay publication/compaction path, plus the contextual FLD ApplyJournal receipt adapter and reference scanner. The full `FldDatabase`→`IDataProvider` snapshot/local-feed/key-resolver bridge remains open. The sample stays outside the default BIN-SYNC solution because FLD-DB is a separate checkout; its cross-repository build is still a T-pass requirement. Physical GC remains disabled.


### Local feed freeze and ceiling-aware sender API

The adapter provides `RecoverAndPublishAllAsync(persistNode, ...)`, `ApplyAndPublishAllAsync(persistNode, ...)` and `RunOutboundAsync(persistNode, ...)` overloads. Each freezes the provider's durable local feed into the active authority before entering the gate; the captured local ceiling then includes only rows actually frozen. A provider edit that arrives after the freeze is deferred to the next gate pass and cannot leak above the current ceiling. The lower-level overloads remain available when the host deliberately manages the freeze separately.



The optional host adapter's `PublishReadyOutboxAsync` converts the captured watermark set into a per-author ceiling map and drains the peer session using the matching ceiling-aware Core overload. Before cutover it uses Node-owned `SyncSession.PushBatchAsync`; after cutover it uses the Metadata Store overload. Both paths filter local-outbound and relay pages before batch construction. The Metadata Store path also rejects any immutable pending reservation whose absolute sequence (`CursorEnd + StreamOffset`) exceeds the captured ceiling. Authors absent from the set and zero-ceiling authors are not eligible. The captured authority-mode bit prevents a cutover race. Do not use a legacy ceiling-free overload inside this FLD publication gate.


### Remaining provider bridge (still C-pass)

The optional adapter is intentionally only the relay/ACK/GC integration layer. It accepts a preconstructed `StorageSyncCoordinator` and a host-supplied remote-apply callback. It does not yet implement `IDataProvider` over `FldDatabase`: FLD needs a generation-pinned, paged full-data snapshot API, and the neutral provider apply contract must carry the complete remote batch context (sender peer/epoch, issuer, cursor, batch identity/digests and relay writes) so FLD can persist the correct `RemoteBatchCommit` without synthetic identities. This is a separate remaining C-pass item, not a test-only task.


### Full remote APPLY context

The Core optional capability `IProviderRemoteBatchCommitStore` and sample `FldRemoteBatchApplyStore` preserve the entire v3 batch header across PendingApply and recovery. The FLD adapter constructs `RemoteBatchCommit` with authenticated SenderPeer/transport epoch kept separate from wire IssuerNodeId/issuer epoch; it carries BatchId, cursor, previous/current digest and RecordsRoot unchanged. The v3 provider-state delta is empty because the current `Batch` wire DTO has no such member. The FLD `RelayWrites` list remains empty by design: BIN-SYNC Metadata Store owns relay copies. Record tombstones map to FLD lifecycle field 0 DeadValue, preserving original HLC/author/ChangeId.

Do not mistake this capability for a full `IDataProvider` implementation. The generation-pinned full-data snapshot API, Outbox-backed local feed with an epoch/ChangeId contract, identifier resolver and crash/reopen conformance are still open C/T work.


### Separate remote-apply capability injection

The `StorageSyncCoordinator` constructor's optional `remoteBatchCommitStore` argument lets the host supply `FldRemoteBatchApplyStore` independently of the `IDataProvider` implementation. This avoids forcing a composite/decorator object to implement both contracts. The two objects must target the same FLD database and the same composed FLD commit lease; mismatched storage targets would split field state and apply receipts and are invalid. The base provider fallback remains unchanged when no separate capability is supplied.


`FldRemoteBatchApplyStore` is an external host adapter; it preserves the full incoming batch header for `FldDatabase.ApplyRemoteBatch` and is not `IDataProvider`. Pass the exact same instance as `remoteBatchCommitStore` to `StorageSyncCoordinator` and to `FldBinSyncHostAdapter`; the latter rejects a mismatched instance/database. For normal inbound traffic use `ReceiveAndPublishAllAsync`, which routes through `ReceiveBatchBeforeAckAsync` so contextual FLD apply and receipt finalization complete before the sender is ACKed.
