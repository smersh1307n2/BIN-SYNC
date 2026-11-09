using BinSync;
using BinSync.Storage;
using Fld1.Engine.Db;
using Fld1.Engine.Sync;

namespace BinSync.FldHost;

/// <summary>
/// Concrete application-layer binding between one FLD database and the durable
/// BIN-SYNC relay coordinator. This sample is deliberately outside both database
/// runtimes: FLD remains protocol-neutral and the two FLD runtime assemblies stay
/// Fld1.Storage.dll + Fld1.Database.dll.
/// </summary>
public sealed class FldBinSyncHostAdapter
{
    private const int RelayBatchPageLimit = 256;
    private const int MaximumSourceChangesPerPage = 4096;

    private readonly FldDatabase _database;
    private readonly StorageSyncCoordinator _coordinator;
    private readonly StorageRelayProvenanceMap _map;
    private readonly StorageRelayPublicationGate _publicationGate;
    private readonly string _scopeId;

    public FldBinSyncHostAdapter(
        FldDatabase database,
        StorageSyncCoordinator coordinator,
        StorageRelayProvenanceMap map,
        string scopeId)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _map = map ?? throw new ArgumentNullException(nameof(map));
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (coordinator.NamespaceId != checked((int)database.NamespaceId))
            throw new ArgumentException("The coordinator and FLD database must target the same namespace.", nameof(coordinator));

        _scopeId = scopeId;
        _publicationGate = StorageRelayPublicationGate.For(coordinator);
    }

    /// <summary>
    /// Reconcile FLD source authors and RPM3-only authors before the host enables
    /// any namespace sender. The callback must enforce every supplied per-author
    /// PublishedThroughSequence ceiling when reading durable outbound payload.
    /// </summary>
    public ValueTask<bool> RecoverAndPublishAllAsync(
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
        => _publicationGate.RecoverAndPublishAllAsync(
            _coordinator,
            _map,
            ReadRelayAuthors,
            _database.AcquireCommitGuard,
            _database.GetRelayJournalHighWatermark,
            ReadRelayPage,
            publishReadyOutbox,
            cancellationToken);

    /// <summary>
    /// Serialize a durable remote APPLY with relay mapping and the following
    /// namespace-wide send. The apply callback must finish its durable FLD commit
    /// before returning; the sender receives only committed per-author ceilings.
    /// </summary>
    public ValueTask<bool> ApplyAndPublishAllAsync(
        Func<CancellationToken, ValueTask> applyRemoteBatch,
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publishReadyOutbox,
        CancellationToken cancellationToken = default)
        => _publicationGate.ApplyAndPublishAllAsync(
            _coordinator,
            _map,
            ReadRelayAuthors,
            _database.AcquireCommitGuard,
            applyRemoteBatch,
            ReadRelayPage,
            publishReadyOutbox,
            cancellationToken);

    /// <summary>Run a later outbound send through the same reconciled namespace gate.</summary>
    public ValueTask RunOutboundAsync(
        Func<IReadOnlyList<StorageRelayAuthorPublicationWatermark>,
            CancellationToken, ValueTask> publish,
        CancellationToken cancellationToken = default)
        => _publicationGate.RunOutboundAsync(
            _coordinator,
            _map,
            ReadRelayAuthors,
            _database.AcquireCommitGuard,
            publish,
            cancellationToken);

    /// <summary>
    /// Persist the safe source ACK in FLD first. The publication gate prunes RPM3
    /// only after this idempotent durable callback returns successfully.
    /// </summary>
    public ValueTask<IReadOnlyList<(NodeId Author, ulong SafeJournalSeq)>> AcknowledgeAllAsync(
        CancellationToken cancellationToken = default)
        => _publicationGate.AcknowledgeAllAsync(
            _coordinator,
            _map,
            ReadRelayAuthors,
            _database.AcquireCommitGuard,
            (author, safeJournalSeq) =>
                _database.AcknowledgeRelayBatches(author.ToArray(), safeJournalSeq),
            cancellationToken);

    /// <summary>
    /// Create the host reference scanner used by BIN-SYNC's ChangeId, receipt,
    /// record-tombstone and FULL_REPLACE proof paths. Each scan owns one immutable
    /// FLD index and its commit lease; disposal releases snapshot before lease.
    /// </summary>
    public IApplicationGcReferenceScanner CreateReferenceScanner()
        => new DelegateApplicationGcReferenceScanner((scope, replica, namespaceId) =>
        {
            if (!StringComparer.Ordinal.Equals(scope, _scopeId) ||
                replica != _coordinator.LocalReplicaId ||
                namespaceId != _coordinator.NamespaceId)
                throw new InvalidOperationException(
                    "The FLD reference scanner is bound to a different scope, replica or namespace.");

            IDisposable lease = _database.AcquireReferenceSnapshotGuard(
                out FldReferenceSnapshot snapshot);
            FldDraftHorizon drafts = snapshot.UnpublishedLocalDraftHorizon;
            return new DelegateApplicationGcReferenceScan(
                lease,
                snapshot,
                new ApplicationDraftHorizon(
                    drafts.Count, drafts.MaxHlcWall, drafts.MaxHlcLogical),
                (sender, authorEpoch, transportEpoch, batch) =>
                    snapshot.HasReceiptReference(
                        sender.ToArray(),
                        authorEpoch,
                        transportEpoch,
                        checked((ushort)namespaceId),
                        batch.ToArray()),
                changeId => snapshot.HasChangeIdReference(changeId.ToArray()),
                (recordId, tombstoneChangeId) =>
                    snapshot.HasRecordReference(
                        recordId.ToArray(), tombstoneChangeId.ToArray()));
        });

    private IReadOnlyList<NodeId> ReadRelayAuthors()
    {
        IReadOnlyList<byte[]> source = _database.ReadUnacknowledgedRelayAuthors();
        var authors = new NodeId[source.Count];
        for (int i = 0; i < source.Count; i++)
            authors[i] = NodeId.From(source[i]);
        return authors;
    }

    private (StorageRelaySourceChange[] Changes,
        ulong ScannedThroughJournalSeq, ulong SourceHighWatermark) ReadRelayPage(
        NodeId author, StorageRelayProvenanceCursor cursor)
    {
        RelayBatchPage page = _database.ReadRelayBatchPage(
            author.ToArray(), cursor.ScannedThroughJournalSeq, RelayBatchPageLimit);

        int changeCount = 0;
        foreach (RelayJournalBatch batch in page.Batches)
        {
            changeCount = checked(changeCount + batch.Batch.RelayWrites.Count);
            if (changeCount > MaximumSourceChangesPerPage)
                throw new InvalidDataException(
                    "One FLD relay page exceeds the bounded RPM3 source-change limit.");
        }

        var changes = new StorageRelaySourceChange[changeCount];
        int index = 0;
        foreach (RelayJournalBatch batch in page.Batches)
        {
            foreach (RemoteFieldWrite write in batch.Batch.RelayWrites)
            {
                changes[index++] = new StorageRelaySourceChange(
                    batch.JournalSeq, BatchId.From(write.ChangeId));
            }
        }

        return (changes, page.ScannedThroughJournalSeq,
            _database.GetRelayJournalHighWatermark());
    }
}
