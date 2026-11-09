# FLD ↔ BIN-SYNC host composition

This is the application-layer binding between the protocol-neutral FLD database and BIN-SYNC. It belongs outside both runtime assemblies: FLD must not reference BIN-SYNC, and the FLD runtime remains exactly two assemblies (`Fld1.Storage` + `Fld1.Database`).

## Shared BIN-SYNC storage and partition

The BIN-SYNC `StorageMetadataStore`, `StorageNodePersistence`, namespace GC store and RPM3 `StorageRelayProvenanceMap` must share one `StorageDatabase`. FLD owns its separate FLD1 database directory and WAL/ApplyJournal.

```csharp
int namespaceId = checked((int)fld.NamespaceId);
var metadata = new StorageMetadataStore(syncStorage);
var map = new StorageRelayProvenanceMap(syncStorage, scopeId, localReplica);
var coordinator = new StorageSyncCoordinator(node, provider, namespaceId, metadata, scopeId);
var relayGate = StorageRelayPublicationGate.For(coordinator);
var adapter = new FldBinSyncHostAdapter(fld, coordinator, map, scopeId);

IDisposable? AcquireFldLease() => fld.AcquireCommitGuard();
```

Keep one `FldDatabase`, one `StorageSyncCoordinator`, one RPM3 map and one shared publication gate per local replica/scope/namespace. Never send directly through a session outside this gate. The gate captures the local author's publication high watermark together with all relay-author RPM3 ceilings under the same composed commit guard. Before provider cutover, the local watermark comes from Node Outbox; after cutover, it comes from Metadata Store. The captured mode is carried with every watermark so the host cannot race the cutover between capture and send.

## Relay page projection

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

Keep every namespace sender closed on process startup until all source authors and RPM3-only authors have been reconciled. `RecoverAndPublishAllAsync` unions FLD's `ReadUnacknowledgedRelayAuthors()` with `map.ReadAuthors(namespaceId)`, validates every saved cursor against `GetRelayJournalHighWatermark()`, drains all pages and only then invokes the sender callback.

```csharp
ValueTask<bool> RecoverRelayAsync(CancellationToken cancellationToken) =>
    relayGate.RecoverAndPublishAllAsync(
        coordinator,
        map,
        ReadRelayAuthors,
        AcquireFldLease,
        fld.GetRelayJournalHighWatermark,
        ReadRelayPage,
        async (ceilings, ct) =>
            await adapter.PublishReadyOutboxAsync(
                session, node, peerStateStore, ceilings, cancellationToken: ct),
        cancellationToken);
```

For each newly accepted remote batch, the host must use `ApplyAndPublishAllAsync` so durable APPLY, relay-page mapping and the sender callback are serialized by the same gate. If APPLY or mapping fails after the remote commit may have become durable, the gate closes publication and requires full recovery before another sender callback. A failed network send is retried from immutable durable outbound state; it must not create a new ChangeId or source mapping.

## Safe ACK order

BIN-SYNC computes the downstream-safe ACK across every interested peer. The host callback durably ACKs the FLD source journal first; RPM3 rows are pruned only after that callback returns successfully.

```csharp
ValueTask<IReadOnlyList<(NodeId Author, ulong SafeJournalSeq)>> AckRelayAsync(
    CancellationToken cancellationToken) =>
    relayGate.AcknowledgeAllAsync(
        coordinator,
        map,
        ReadRelayAuthors,
        AcquireFldLease,
        (author, safeJournalSeq) =>
            fld.AcknowledgeRelayBatches(author.ToArray(), safeJournalSeq),
        cancellationToken);
```

The callback must be idempotent. A crash after FLD ACK but before RPM3 prune is recovered because RPM3 cursor authors are unioned with FLD's remaining unacknowledged authors. A partial multi-author ACK is retried from the remaining map cursors.

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

1. One shared publication gate serializes remote APPLY, all author relay mappings, outbound sends and ACKs for the namespace.
2. No send before startup reconciliation has validated every author and drained every source page.
3. Every page ends on a complete JournalSeq boundary; one JournalSeq may map to several operations within the page.
4. RPM3 stores sequence coordinates only; ChangeId remains in the global BIN-SYNC ledger.
5. Safe ACK order is downstream-safe BIN-SYNC watermark → durable FLD ACK → RPM3 prune.
6. A failed or ambiguous durable operation closes publication until full recovery; a network retry reuses immutable outbound state.
7. FLD1 WAL, ApplyJournal, Outbox, Run and Manifest formats remain unchanged by this adapter.
8. Physical GC remains disabled until the host reference snapshot and composed lock proof pass T/O gates.


## Optional source implementation

The guide is now implemented by the optional cross-repository sample in BIN-SYNC at [`samples/FldBinSyncHost`](https://github.com/smersh1307n2/BIN-SYNC/tree/feature/fld-external-storage/samples/FldBinSyncHost). It contains the concrete adapter for relay paging, startup reconciliation, publication ceilings, safe ACK ordering and the FLD reference scanner. The sample remains outside the default BIN-SYNC solution because FLD-DB is a separate checkout; see its README for the canonical shared-Storage project build command. It does not provide a transport implementation or enable physical GC.


### Local feed freeze and ceiling-aware sender API

The adapter provides `RecoverAndPublishAllAsync(persistNode, ...)`, `ApplyAndPublishAllAsync(persistNode, ...)` and `RunOutboundAsync(persistNode, ...)` overloads. Each freezes the provider's durable local feed into the active authority before entering the gate; the captured local ceiling then includes only rows actually frozen. A provider edit that arrives after the freeze is deferred to the next gate pass and cannot leak above the current ceiling. The lower-level overloads remain available when the host deliberately manages the freeze separately.



The optional host adapter's `PublishReadyOutboxAsync` converts the captured watermark set into a per-author ceiling map and drains the peer session using the matching ceiling-aware Core overload. Before cutover it uses Node-owned `SyncSession.PushBatchAsync`; after cutover it uses the Metadata Store overload. Both paths filter local-outbound and relay pages before batch construction. The Metadata Store path also rejects any immutable pending reservation whose absolute sequence (`CursorEnd + StreamOffset`) exceeds the captured ceiling. Authors absent from the set and zero-ceiling authors are not eligible. The captured authority-mode bit prevents a cutover race. Do not use a legacy ceiling-free overload inside this FLD publication gate.


### Remaining provider bridge (still C-pass)

The optional adapter is intentionally only the relay/ACK/GC integration layer. It accepts a preconstructed `StorageSyncCoordinator` and a host-supplied remote-apply callback. It does not yet implement `IDataProvider` over `FldDatabase`: FLD needs a generation-pinned, paged full-data snapshot API, and the neutral provider apply contract must carry the complete remote batch context (sender peer/epoch, issuer, cursor, batch identity/digests and relay writes) so FLD can persist the correct `RemoteBatchCommit` without synthetic identities. This is a separate remaining C-pass item, not a test-only task.
