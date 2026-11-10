using System.Buffers.Binary;
using System.Security.Cryptography;

namespace BinSync.Storage;

public sealed record StorageRemoteApplyPlan(
    Guid ApplyToken, MetadataIncomingBatch Batch, bool FinalizeMetadata, bool IsDuplicate,
    IReadOnlyList<bool> ApplyOperations);

/// <summary>
/// Coordinates BIN-SYNC with an application-owned durable Data Provider and optional
/// Metadata Store. Before provider-authoritative cutover, local feed entries are
/// mirrored through Node Outbox; after cutover, Metadata Store owns outbound sequencing
/// and Node is rehydrated from the provider snapshot as an application-data cache.
/// </summary>
public sealed class StorageSyncCoordinator
{
    private readonly Node _node;
    private readonly IDataProvider _provider;
    private readonly IProviderRemoteBatchCommitStore? _remoteBatchCommitStore;
    private readonly int _namespaceId;
    private readonly IMetadataStore? _metadataStore;
    private readonly string? _scopeId;
    private StorageNamespaceGcStore? _namespaceGcStore;
    private TrustStore? _namespaceGcTrustStore;
    private readonly object _gate = new();

    /// <summary>The immutable namespace coordinated by this instance.</summary>
    public int NamespaceId => _namespaceId;

    /// <summary>The local replica identity whose outbound streams this coordinator owns.</summary>
    public NodeId LocalReplicaId => _node.NodeId;

    /// <summary>
    /// True only when this coordinator will invoke the supplied contextual remote-apply
    /// store instance for metadata-owned incoming batches. Hosts use this to prevent a
    /// separately-created FLD ApplyJournal adapter from being silently bypassed.
    /// </summary>
    public bool UsesRemoteBatchCommitStore(IProviderRemoteBatchCommitStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        return ReferenceEquals(_remoteBatchCommitStore, store) ||
            ReferenceEquals(_provider, store);
    }

    /// <summary>True only when this coordinator uses the exact supplied application data provider.</summary>
    public bool UsesDataProvider(IDataProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        return ReferenceEquals(_provider, provider);
    }

    /// <summary>True only when this coordinator uses the exact supplied Metadata Store instance.</summary>
    public bool UsesMetadataPeerStateStore(IMetadataPeerStateStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        return ReferenceEquals(_metadataStore, store);
    }

    /// <summary>True after the provider-authoritative Metadata Store publication cutover.</summary>
    public bool IsMetadataStoreOwnedPublication =>
        _provider is IProviderPublicationMode publicationMode &&
        publicationMode.TryGetMetadataStoreOwnedPublicationCutover(out _);

    /// <summary>
    /// Capture the current local-author publication ceiling from the durable Metadata
    /// Store. Local provider-feed operations use the Metadata Store's outbound sequence
    /// directly; they do not have an FLD relay JournalSeq/RPM3 mapping.
    /// Call while the composed application commit guard is held.
    /// </summary>
    public StorageRelayAuthorPublicationWatermark ReadLocalPublicationWatermark()
        => ReadLocalPublicationWatermark(IsMetadataStoreOwnedPublication);

    internal StorageRelayAuthorPublicationWatermark ReadLocalPublicationWatermark(
        bool metadataStoreOwnedPublication)
    {
        if (_metadataStore is null || string.IsNullOrWhiteSpace(_scopeId))
            throw new NotSupportedException(
                "The local publication ceiling requires the durable Metadata Store and a stable scope.");
        ulong publishedThrough;
        if (metadataStoreOwnedPublication)
        {
            publishedThrough = _metadataStore.GetOutboundHighWatermark(
                _scopeId, _node.NodeId, _namespaceId, _node.NodeId);
        }
        else
        {
            long nodeOutboxHighWatermark = _node.GetOutboxHighWatermark(
                _namespaceId, _node.NodeId);
            if (nodeOutboxHighWatermark < 0)
                throw new InvalidDataException("Node Outbox returned a negative local publication watermark.");
            publishedThrough = checked((ulong)nodeOutboxHighWatermark);
        }

        return new StorageRelayAuthorPublicationWatermark(
            _node.NodeId, publishedThrough, ScannedThroughJournalSeq: 0,
            IsMetadataStoreOwnedPublication: metadataStoreOwnedPublication);
    }

    /// <summary>
    /// Capture the complete Metadata-Store-owned publication cut: local outbound
    /// watermark plus every original-author relay watermark. This path deliberately
    /// does not consult FLD JournalSeq or RPM3; the Metadata Store owns both relay
    /// payload and per-author sequence after the provider-authoritative cutover.
    /// The caller's FLD commit lease is composed with Node/Storage gates here.
    /// </summary>
    public IReadOnlyList<StorageRelayAuthorPublicationWatermark>
        CaptureMetadataOwnedPublicationWatermarks(
            Func<IDisposable?> acquireHostCommitLease)
    {
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        if (!IsMetadataStoreOwnedPublication)
            throw new InvalidOperationException(
                "Metadata-owned relay ceilings are available only after the durable provider cutover.");
        if (_metadataStore is not IMetadataPeerStateStore peerStore ||
            string.IsNullOrWhiteSpace(_scopeId))
            throw new NotSupportedException(
                "Metadata-owned relay publication requires a durable peer-state store and stable scope.");

        using IDisposable commitGuard = AcquireApplicationCommitGuard(acquireHostCommitLease);
        var result = new List<StorageRelayAuthorPublicationWatermark>
        {
            ReadLocalPublicationWatermark(metadataStoreOwnedPublication: true)
        };
        IReadOnlyList<NodeId> authors = peerStore.ReadRelayAuthors(
            _scopeId!, _node.NodeId, _namespaceId);
        var seen = new HashSet<NodeId> { _node.NodeId };
        foreach (NodeId author in authors)
        {
            if (author.IsZero || !seen.Add(author))
                throw new InvalidDataException(
                    "Metadata Store relay author set contains a zero or local/duplicate identity.");
            ulong highWatermark = _metadataStore!.GetRelayHighWatermark(
                _scopeId!, _node.NodeId, _namespaceId, author);
            result.Add(new StorageRelayAuthorPublicationWatermark(
                author,
                highWatermark,
                ScannedThroughJournalSeq: 0,
                IsMetadataStoreOwnedPublication: true));
        }
        return result;
    }

    /// <summary>
    /// Compact only Metadata Store relay payload through the minimum durable ACK
    /// across all interested peers. Per-peer ACKs are committed by SyncSession; this
    /// method never writes FLD's legacy ApplyJournal relay-ACK sidecar.
    /// </summary>
    public IReadOnlyList<(NodeId Author, ulong SafeThroughSequence)>
        CompactAcknowledgedMetadataRelayPayloads(
            Func<IDisposable?> acquireHostCommitLease)
    {
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        if (!IsMetadataStoreOwnedPublication)
            throw new InvalidOperationException(
                "Metadata relay compaction requires the provider-authoritative cutover.");
        if (_metadataStore is not IMetadataPeerStateStore peerStore ||
            string.IsNullOrWhiteSpace(_scopeId))
            throw new NotSupportedException(
                "Metadata relay compaction requires a durable peer-state store and stable scope.");

        using IDisposable commitGuard = AcquireApplicationCommitGuard(acquireHostCommitLease);
        IReadOnlyList<NodeId> authors = peerStore.ReadRelayAuthors(
            _scopeId!, _node.NodeId, _namespaceId);
        var result = new List<(NodeId Author, ulong SafeThroughSequence)>(authors.Count);
        foreach (NodeId author in authors)
        {
            if (author.IsZero || author == _node.NodeId)
                throw new InvalidDataException(
                    "Metadata Store relay author set contains a zero or local identity.");
            ulong safeThrough = peerStore.GetSafePeerStreamCompactionCursor(
                _scopeId!, _node.NodeId, _namespaceId, author);
            ulong highWatermark = _metadataStore!.GetRelayHighWatermark(
                _scopeId!, _node.NodeId, _namespaceId, author);
            if (safeThrough > highWatermark)
                throw new InvalidDataException(
                    "Metadata Store peer ACK exceeds its durable relay high watermark.");
            if (safeThrough != 0)
                _metadataStore.CompactRelayThrough(
                    _scopeId!, _node.NodeId, _namespaceId, author, safeThrough);
            result.Add((author, safeThrough));
        }
        return result;
    }

    internal void ValidateRelayProvenanceMap(StorageRelayProvenanceMap map)
    {
        ArgumentNullException.ThrowIfNull(map);
        if (_metadataStore is not StorageMetadataStore metadataStore ||
            string.IsNullOrWhiteSpace(_scopeId))
            throw new NotSupportedException(
                "Relay publication requires the durable StorageMetadataStore and a stable metadata scope.");
        if (!map.IsBoundTo(_scopeId, _node.NodeId) || !map.IsBoundTo(metadataStore))
            throw new InvalidOperationException(
                "Relay provenance map must share this coordinator's scope, local replica and StorageDatabase.");
    }

    public StorageSyncCoordinator(Node node, IDataProvider provider, int namespaceId,
        IMetadataStore? metadataStore = null, string? scopeId = null,
        IProviderRemoteBatchCommitStore? remoteBatchCommitStore = null)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _remoteBatchCommitStore = remoteBatchCommitStore;
        if (namespaceId < 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (metadataStore is not null && string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("A stable metadata scope is required when a Metadata Store is attached.", nameof(scopeId));
        if (metadataStore is null && scopeId is not null)
            throw new ArgumentException("A metadata scope cannot be supplied without a Metadata Store.", nameof(scopeId));
        _namespaceId = namespaceId;
        _metadataStore = metadataStore;
        _scopeId = scopeId;
        if (_metadataStore is not null && _provider is IProviderPublicationMode publicationMode &&
            publicationMode.TryGetMetadataStoreOwnedPublicationCutover(out _))
            _node.EnableMetadataStoreOwnedPublicationFence(_namespaceId);
    }

    /// <summary>Attach the same-partition signed namespace-GC replay fence to durable metadata.</summary>
    public void AttachNamespaceGcGuard(StorageNamespaceGcStore gcStore, TrustStore trustStore)
    {
        if (_metadataStore is not StorageMetadataStore storageMetadata)
            throw new NotSupportedException("Namespace-GC replay fencing requires the durable StorageMetadataStore implementation.");
        lock (_gate)
        {
            storageMetadata.AttachNamespaceGcGuard(gcStore, trustStore);
            _namespaceGcStore = gcStore;
            _namespaceGcTrustStore = trustStore;
        }
    }

    /// <summary>True when this coordinator is attached to the exact supplied namespace-GC store.</summary>
    public bool UsesNamespaceGcStore(IMetadataNamespaceGcStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        lock (_gate) return ReferenceEquals(_namespaceGcStore, store);
    }

    /// <summary>True when this coordinator trusts the exact supplied trust-store instance.</summary>
    public bool UsesNamespaceGcTrustStore(TrustStore trustStore)
    {
        ArgumentNullException.ThrowIfNull(trustStore);
        lock (_gate) return ReferenceEquals(_namespaceGcTrustStore, trustStore);
    }

    /// <summary>Activate a fresh author epoch after this member has rejoined as Active.</summary>
    public void ActivateLocalAuthorEpoch(uint authorEpoch)
    {
        if (authorEpoch == 0) throw new ArgumentOutOfRangeException(nameof(authorEpoch));
        lock (_gate)
        {
            if (_namespaceGcStore is null || _namespaceGcTrustStore is null)
                throw new InvalidOperationException("Attach the trusted namespace roster before activating a rejoined author epoch.");
            if (!_namespaceGcStore.TryReadNamespaceRoster(
                    _namespaceId, _namespaceGcTrustStore, out NamespaceRosterCertificate? roster) ||
                roster is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");
            NamespaceRosterMember? localMember = roster.Members.FirstOrDefault(member =>
                member.NodeId == _node.NodeId);
            if (localMember is null || localMember.Status != NamespaceMemberStatus.Active)
                throw new InvalidOperationException("A local author epoch can be reactivated only after the member is Active in the accepted roster.");
            if (_provider is not IProviderAuthorEpochClosureStore closureStore)
                throw new NotSupportedException("The attached provider does not support durable author-epoch activation.");
            closureStore.ActivateLocalAuthorEpoch(authorEpoch);
        }
    }

    /// <summary>
    /// Acquire the application commit barrier for coordinated work and physical-GC
    /// callbacks in lock order (coordinator, Node gates, host gate when supplied,
    /// provider/shared-Storage transaction gate). FULL_REPLACE must use the stronger
    /// AcquireFullReplaceCommitGuard overload so Metadata pending state is also fenced.
    /// Dispose on the acquiring thread.
    /// </summary>
    public IDisposable AcquireApplicationCommitGuard()
        => AcquireApplicationCommitGuardCore(null, requireFullReplaceMetadataProof: false);

    /// <summary>
    /// Acquire the cross-store FULL_REPLACE barrier. In addition to the normal
    /// coordinator/Node/host/provider gates, this holds the durable Metadata Store
    /// transaction gate and proves that no target-namespace PendingApply or immutable
    /// outbound/relay reservation remains unresolved. The returned typed lease is bound
    /// to this coordinator's exact scope, local replica, namespace, namespace-GC store
    /// and trust-store instance.
    /// </summary>
    public IFullReplaceCommitLease AcquireFullReplaceCommitGuard(Func<IDisposable?> acquireHostCommitLease)
    {
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        if (_namespaceId <= 0 || _namespaceId > ushort.MaxValue ||
            _node.NodeId.IsZero || string.IsNullOrWhiteSpace(_scopeId))
            throw new InvalidOperationException(
                "FULL_REPLACE requires a non-zero local replica, namespace and stable metadata scope.");
        Monitor.Enter(_gate);
        try
        {
            IMetadataNamespaceGcStore namespaceGcStore = _namespaceGcStore
                ?? throw new NotSupportedException("FULL_REPLACE requires the coordinator to be attached to the exact namespace-GC store.");
            TrustStore namespaceGcTrustStore = _namespaceGcTrustStore
                ?? throw new NotSupportedException("FULL_REPLACE requires the coordinator to be attached to the exact namespace-GC trust store.");
            IDisposable lease = AcquireApplicationCommitGuardCore(
                acquireHostCommitLease, requireFullReplaceMetadataProof: true);
            try
            {
                return new FullReplaceApplicationCommitLease(
                    lease, _scopeId, _node.NodeId, _namespaceId,
                    namespaceGcStore, namespaceGcTrustStore);
            }
            catch
            {
                lease.Dispose();
                throw;
            }
        }
        finally
        {
            Monitor.Exit(_gate);
        }
    }

    /// <summary>
    /// Acquire the application barrier in the required lock order:
    /// coordinator → Node gates → host WAL/FLD gate → shared BIN-SYNC Storage gate.
    /// Use this overload for physical GC and other coordinated work when the host
    /// has durable state outside the shared Storage database. FULL_REPLACE must use
    /// AcquireFullReplaceCommitGuard so unresolved Metadata state is rejected.
    /// </summary>
    public IDisposable AcquireApplicationCommitGuard(Func<IDisposable?> acquireHostCommitLease)
    {
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        return AcquireApplicationCommitGuardCore(
            acquireHostCommitLease, requireFullReplaceMetadataProof: false);
    }

    private IDisposable AcquireApplicationCommitGuardCore(
        Func<IDisposable?>? acquireHostCommitLease,
        bool requireFullReplaceMetadataProof)
    {
        Monitor.Enter(_gate);
        IDisposable? nodeLease = null;
        IDisposable? hostLease = null;
        IDisposable? metadataLease = null;
        IDisposable? providerLease = null;
        try
        {
            nodeLease = _node.EnterAllGates();
            if (_metadataStore is StorageMetadataStore metadataStore &&
                _provider is StorageDataProvider storageProvider &&
                !storageProvider.SharesStorageWith(metadataStore))
                throw new InvalidOperationException("The durable Metadata Store and Data Provider must share the same StorageDatabase for the application commit barrier.");
            if (_provider is not IProviderCommitGuard providerGuard)
                throw new NotSupportedException("The attached provider does not expose an exclusive commit guard.");
            if (acquireHostCommitLease is not null)
                hostLease = acquireHostCommitLease()
                    ?? throw new InvalidOperationException("The host WAL/FLD commit lease is unavailable; the application barrier cannot be acquired.");

            if (requireFullReplaceMetadataProof)
            {
                if (_metadataStore is not IMetadataFullReplaceCommitGuard metadataGuard ||
                    string.IsNullOrWhiteSpace(_scopeId))
                    throw new NotSupportedException(
                        "FULL_REPLACE requires a Metadata Store full-replace guard for the exact scope and local replica.");
                metadataLease = metadataGuard.AcquireFullReplaceCommitGuard(
                    _scopeId, _node.NodeId, _namespaceId);
            }

            providerLease = providerGuard.AcquireCommitGuard();
            var lease = new ApplicationCommitLease(
                providerLease, metadataLease, hostLease, nodeLease, _gate);
            providerLease = null;
            metadataLease = null;
            hostLease = null;
            nodeLease = null;
            return lease;
        }
        catch
        {
            try { providerLease?.Dispose(); }
            finally
            {
                try { metadataLease?.Dispose(); }
                finally
                {
                    try { hostLease?.Dispose(); }
                    finally
                    {
                        try { nodeLease?.Dispose(); }
                        finally { Monitor.Exit(_gate); }
                    }
                }
            }
            throw;
        }
    }

    /// <summary>
    /// Read one complete host relay page and append its RPM3 provenance while the
    /// composed application commit guard is held. The host callback must only read
    /// and flatten durable source data; it must not perform network I/O.
    /// </summary>
    public bool ReadAndAppendRelayProvenancePage(
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        Func<IDisposable?> acquireHostCommitLease,
        Func<(StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq)> readSourcePage)
    {
        ArgumentNullException.ThrowIfNull(map);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(readSourcePage);
        using IDisposable commitGuard = AcquireApplicationCommitGuard(acquireHostCommitLease);
        (StorageRelaySourceChange[] Changes, ulong ScannedThroughJournalSeq) page = readSourcePage();
        if (page.Changes is null)
            throw new InvalidDataException("Host relay page callback returned a null change array.");
        return AppendRelayProvenancePage(
            map, originalAuthor, page.Changes, page.ScannedThroughJournalSeq);
    }

    /// <summary>
    /// Validate the persisted RPM3 scan/ACK cursors against the durable FLD journal before
    /// a host opens its namespace sender after restart. This is fail-closed: a map cursor
    /// beyond the source journal high watermark indicates lost/replaced source history.
    /// </summary>
    public StorageRelayProvenanceCursor ValidateRelayProvenanceRecovery(
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        Func<IDisposable?> acquireHostCommitLease,
        Func<ulong> readSourceJournalHighWatermark)
    {
        ArgumentNullException.ThrowIfNull(map);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(readSourceJournalHighWatermark);
        if (originalAuthor.IsZero)
            throw new ArgumentException("Original relay author must be non-zero.", nameof(originalAuthor));

        using IDisposable commitGuard = AcquireApplicationCommitGuard(acquireHostCommitLease);
        if (_metadataStore is not StorageMetadataStore metadataStore ||
            string.IsNullOrWhiteSpace(_scopeId))
            throw new NotSupportedException(
                "Relay provenance recovery requires the durable StorageMetadataStore.");
        if (!map.IsBoundTo(_scopeId, _node.NodeId) || !map.IsBoundTo(metadataStore))
            throw new InvalidOperationException(
                "Relay provenance map must share this coordinator's scope, local replica and StorageDatabase.");

        ulong sourceHighWatermark = readSourceJournalHighWatermark();
        StorageRelayProvenanceCursor cursor = map.ReadCursor(_namespaceId, originalAuthor);
        if (cursor.ScannedThroughJournalSeq > sourceHighWatermark ||
            cursor.AckedThroughJournalSeq > sourceHighWatermark)
            throw new InvalidDataException(
                "Persisted RPM3 cursor is ahead of the durable FLD relay journal; keep namespace publication closed and recover the source history.");
        return cursor;
    }

    /// <summary>
    /// Resolve transient FLD ChangeIds against retained Node Outbox entries and atomically
    /// persist their compact JournalSeq↔OutboxSeq map. The global ChangeId ledger remains
    /// the identity authority; this method obtains the current OutboxSeq from the retained
    /// Outbox projection. It fails closed if that projection has already been compacted.
    /// Call inside <c>AcquireApplicationCommitGuard</c> while the FLD commit lease used to
    /// read the source page is still held.
    /// </summary>
    /// <returns>
    /// False when any ChangeId is no longer retained in the Node Outbox; in that case
    /// no mapping row or scan cursor is written and the caller must recover/catch up.
    /// </returns>
    public bool AppendRelayProvenancePage(
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        ReadOnlySpan<StorageRelaySourceChange> sourceChanges,
        ulong scannedThroughJournalSeq)
    {
        ArgumentNullException.ThrowIfNull(map);
        if (!Monitor.IsEntered(_gate))
            throw new InvalidOperationException(
                "Relay provenance pages must be appended inside the application commit guard.");
        if (_metadataStore is not StorageMetadataStore metadataStore ||
            string.IsNullOrWhiteSpace(_scopeId))
            throw new NotSupportedException(
                "Relay provenance mapping requires the durable StorageMetadataStore.");
        if (!map.IsBoundTo(_scopeId, _node.NodeId) || !map.IsBoundTo(metadataStore))
            throw new InvalidOperationException(
                "Relay provenance map must share this coordinator's scope, local replica and StorageDatabase.");
        if (originalAuthor.IsZero)
            throw new ArgumentException("Original relay author must be non-zero.", nameof(originalAuthor));
        if (sourceChanges.Length > 4096)
            throw new ArgumentOutOfRangeException(nameof(sourceChanges), "Relay source page cannot exceed 4,096 changes.");

        StorageRelayProvenanceCursor priorCursor = map.ReadCursor(_namespaceId, originalAuthor);
        if (scannedThroughJournalSeq < priorCursor.ScannedThroughJournalSeq)
            throw new InvalidDataException("FLD relay scan cursor cannot regress.");
        if (sourceChanges.IsEmpty)
        {
            map.AppendPage(_namespaceId, originalAuthor,
                Array.Empty<StorageRelayProvenanceEntry>(), scannedThroughJournalSeq);
            return true;
        }
        if (scannedThroughJournalSeq == priorCursor.ScannedThroughJournalSeq)
            throw new InvalidDataException("A committed FLD scan cursor cannot be reused for new relay mappings.");

        ulong previousJournalSeq = priorCursor.ScannedThroughJournalSeq;
        for (int i = 0; i < sourceChanges.Length; i++)
        {
            ulong journalSeq = sourceChanges[i].JournalSeq;
            if (journalSeq == 0 ||
                (i == 0 ? journalSeq <= previousJournalSeq : journalSeq < previousJournalSeq) ||
                journalSeq > scannedThroughJournalSeq)
                throw new InvalidDataException("Relay source JournalSeq values must start after the prior cursor, remain non-decreasing within the page, and not exceed the scanned cursor.");
            if (sourceChanges[i].ChangeId.IsZero)
                throw new InvalidDataException("Relay source ChangeId must be non-zero before Outbox identity resolution.");
            previousJournalSeq = journalSeq;
        }

        var mappedEntries = new StorageRelayProvenanceEntry[sourceChanges.Length];
        bool metadataOwnsOutbound = _provider is IProviderPublicationMode publicationMode &&
            publicationMode.TryGetMetadataStoreOwnedPublicationCutover(out _);
        if (metadataOwnsOutbound)
        {
            var changeIds = new BatchId[sourceChanges.Length];
            var relaySequences = new ulong[sourceChanges.Length];
            for (int i = 0; i < sourceChanges.Length; i++)
                changeIds[i] = sourceChanges[i].ChangeId;

            if (!metadataStore.TryResolveRelaySequences(
                _scopeId!, _node.NodeId, _namespaceId, originalAuthor, changeIds, relaySequences))
                return false;

            for (int i = 0; i < sourceChanges.Length; i++)
            {
                ulong relaySequence = relaySequences[i];
                if (relaySequence == 0)
                    throw new InvalidDataException("Resolved metadata relay sequence must be positive.");
                mappedEntries[i] = new StorageRelayProvenanceEntry(
                    sourceChanges[i].JournalSeq, relaySequence);
            }
        }
        else
        {
            var identities = new OutboxChangeIdentity[sourceChanges.Length];
            var outboxSequences = new long[sourceChanges.Length];
            for (int i = 0; i < sourceChanges.Length; i++)
                identities[i] = new OutboxChangeIdentity(originalAuthor, sourceChanges[i].ChangeId);

            if (!_node.TryResolveOutboxSequences(_namespaceId, identities, outboxSequences))
                return false;

            for (int i = 0; i < sourceChanges.Length; i++)
            {
                long outboxSequence = outboxSequences[i];
                if (outboxSequence <= 0)
                    throw new InvalidDataException("Resolved relay OutboxSeq must be positive.");
                mappedEntries[i] = new StorageRelayProvenanceEntry(
                    sourceChanges[i].JournalSeq, checked((ulong)outboxSequence));
            }
        }

        map.AppendPage(_namespaceId, originalAuthor, mappedEntries, scannedThroughJournalSeq);
        return true;
    }

    /// <summary>
    /// Translate the aggregate downstream-safe Node Outbox watermark to a whole FLD
    /// JournalSeq, durably acknowledge that FLD prefix, then prune RPM3 rows. The
    /// host callback executes while the composed FLD/Node/Storage commit guard is held.
    /// If the callback throws, the provenance map is untouched and the FLD ACK can be
    /// retried. If map pruning fails after the FLD ACK, replaying the idempotent FLD ACK
    /// and then this method is safe.
    /// </summary>
    public ulong AcknowledgeRelayProvenance(
        StorageRelayProvenanceMap map,
        NodeId originalAuthor,
        Func<IDisposable?> acquireHostCommitLease,
        Action<ulong> acknowledgeSource)
    {
        ArgumentNullException.ThrowIfNull(map);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        ArgumentNullException.ThrowIfNull(acknowledgeSource);
        if (originalAuthor.IsZero)
            throw new ArgumentException("Original relay author must be non-zero.", nameof(originalAuthor));

        using IDisposable commitGuard = AcquireApplicationCommitGuard(acquireHostCommitLease);
        if (_metadataStore is not StorageMetadataStore metadataStore ||
            string.IsNullOrWhiteSpace(_scopeId))
            throw new NotSupportedException(
                "Relay provenance acknowledgement requires the durable StorageMetadataStore.");
        if (!map.IsBoundTo(_scopeId, _node.NodeId) || !map.IsBoundTo(metadataStore))
            throw new InvalidOperationException(
                "Relay provenance map must share this coordinator's scope, local replica and StorageDatabase.");

        StorageRelayProvenanceCursor priorCursor = map.ReadCursor(_namespaceId, originalAuthor);
        ulong currentSafeOutbox;
        bool metadataOwnsOutbound = _provider is IProviderPublicationMode publicationMode &&
            publicationMode.TryGetMetadataStoreOwnedPublicationCutover(out _);
        if (metadataOwnsOutbound)
        {
            currentSafeOutbox = metadataStore.GetSafePeerStreamCompactionCursor(
                _scopeId!, _node.NodeId, _namespaceId, originalAuthor);
        }
        else
        {
            long safeOutboxSequence = _node.GetSafeOutboxAckedThrough(_namespaceId, originalAuthor);
            if (safeOutboxSequence < 0)
                throw new InvalidDataException("Node returned a negative safe Outbox watermark.");
            currentSafeOutbox = checked((ulong)safeOutboxSequence);
        }

        // The durable map ACK is itself a previously proven safe lower bound.
        // Peer registration/retirement can temporarily make the current aggregate
        // report zero (for example while a replacement peer completes its baseline);
        // it must never regress an already committed source ACK.
        ulong safeOutbox = Math.Max(priorCursor.AckedThroughOutboxSeq, currentSafeOutbox);
        ulong safeJournal = map.TranslateSafeOutboxSequence(
            _namespaceId, originalAuthor, safeOutbox);

        // This callback must durably acknowledge the source JournalSeq prefix.
        // Do not move the map cursor until it returns successfully.
        acknowledgeSource(safeJournal);
        map.CommitAfterFldRelayAck(
            _namespaceId, originalAuthor, safeOutbox, safeJournal);
        return safeJournal;
    }

    private sealed class FullReplaceApplicationCommitLease : IFullReplaceCommitLease
    {
        private IDisposable? _inner;
        public string ScopeId { get; }
        public NodeId LocalReplica { get; }
        public int NamespaceId { get; }
        public IMetadataNamespaceGcStore NamespaceGcStore { get; }
        public TrustStore NamespaceGcTrustStore { get; }

        public FullReplaceApplicationCommitLease(
            IDisposable inner, string scopeId, NodeId localReplica, int namespaceId,
            IMetadataNamespaceGcStore namespaceGcStore, TrustStore namespaceGcTrustStore)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            ScopeId = scopeId ?? throw new ArgumentNullException(nameof(scopeId));
            LocalReplica = localReplica;
            NamespaceId = namespaceId;
            NamespaceGcStore = namespaceGcStore ?? throw new ArgumentNullException(nameof(namespaceGcStore));
            NamespaceGcTrustStore = namespaceGcTrustStore ?? throw new ArgumentNullException(nameof(namespaceGcTrustStore));
        }

        public void Dispose()
            => Interlocked.Exchange(ref _inner, null)?.Dispose();
    }

    private sealed class ApplicationCommitLease : IDisposable
    {
        private IDisposable? _providerLease;
        private IDisposable? _metadataLease;
        private IDisposable? _hostLease;
        private IDisposable? _nodeLease;
        private object? _coordinatorGate;
        public ApplicationCommitLease(
            IDisposable providerLease, IDisposable? metadataLease,
            IDisposable? hostLease, IDisposable nodeLease, object coordinatorGate)
        {
            _providerLease = providerLease;
            _metadataLease = metadataLease;
            _hostLease = hostLease;
            _nodeLease = nodeLease;
            _coordinatorGate = coordinatorGate;
        }
        public void Dispose()
        {
            IDisposable? providerLease = Interlocked.Exchange(ref _providerLease, null);
            IDisposable? metadataLease = Interlocked.Exchange(ref _metadataLease, null);
            IDisposable? hostLease = Interlocked.Exchange(ref _hostLease, null);
            IDisposable? nodeLease = Interlocked.Exchange(ref _nodeLease, null);
            object? coordinatorGate = Interlocked.Exchange(ref _coordinatorGate, null);
            try { providerLease?.Dispose(); }
            finally
            {
                try { metadataLease?.Dispose(); }
                finally
                {
                    try { hostLease?.Dispose(); }
                    finally
                    {
                        try { nodeLease?.Dispose(); }
                        finally
                        {
                            if (coordinatorGate is not null) Monitor.Exit(coordinatorGate);
                        }
                    }
                }
            }
        }
    }

    /// <summary>
    /// Publish this member's signed contiguous applied-origin frontier. The host scanner
    /// reports WAL/FLD drafts not yet frozen into BIN-SYNC; the provider scan reports
    /// durable provider-feed drafts. Both are sampled under coordinator→Node→host
    /// WAL/FLD→shared-Storage locks. Their counts are conservative (overlap may be
    /// counted twice); only zero authorizes a FULL_REPLACE retention barrier. Neither
    /// FLD JournalSeq, CommitSeq nor OutboxSeq is used as OriginSequence.
    /// </summary>
    public AppliedOriginFrontierReport PublishAppliedOriginFrontier(
        byte[] memberSeed, IApplicationGcReferenceScanner externalState,
        Func<IDisposable?> acquireHostCommitLease,
        int pageSize = 1024, bool finalForAuthor = false, uint finalAuthorEpoch = 0)
    {
        ArgumentNullException.ThrowIfNull(memberSeed);
        ArgumentNullException.ThrowIfNull(externalState);
        ArgumentNullException.ThrowIfNull(acquireHostCommitLease);
        if (memberSeed.Length != 32)
            throw new ArgumentException("Ed25519 member seed must be 32 bytes.", nameof(memberSeed));

        // The draft count and applied frontier must describe one application
        // commit boundary even for non-final reports; otherwise a concurrent local
        // write could race a zero-draft report used by a FULL_REPLACE checkpoint.
        using IDisposable commitGuard = AcquireApplicationCommitGuard(acquireHostCommitLease);
        lock (_gate)
        {
            if (_metadataStore is not StorageMetadataStore metadataStore ||
                string.IsNullOrWhiteSpace(_scopeId))
                throw new NotSupportedException("Applied-origin frontier publication requires the durable StorageMetadataStore.");
            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the signed namespace-GC store before publishing frontiers.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.TryReadNamespaceRoster(_namespaceId, trustStore, out NamespaceRosterCertificate? roster) ||
                roster is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");
            NamespaceRosterProtocol.VerifyOrThrow(roster, trustStore);

            if (pageSize <= 0 || pageSize > 4096)
                throw new ArgumentOutOfRangeException(nameof(pageSize));
            using IApplicationGcReferenceScan externalScan =
                externalState.OpenScan(_scopeId, _node.NodeId, _namespaceId);
            ApplicationDraftHorizon hostDrafts = externalScan.UnpublishedLocalDraftHorizon;
            if (hostDrafts.Count == 0 &&
                (hostDrafts.MaxHlcWall != 0 || hostDrafts.MaxHlcLogical != 0))
                throw new InvalidDataException("Host draft scanner returned an HLC maximum for an empty draft set.");

            if (finalForAuthor && hostDrafts.Count != 0)
                throw new InvalidOperationException("Cannot close the local author identity while host WAL/FLD drafts remain unpublished.");
            if (finalForAuthor)
            {
                if (finalAuthorEpoch == 0 ||
                    _provider is not IProviderAuthorEpochClosureStore closureStore)
                    throw new InvalidOperationException("Final author report requires the durable provider author-epoch closure capability and a non-zero current epoch.");
                foreach (uint epoch in metadataStore.ReadLocalOriginEpochs(
                    _scopeId, _node.NodeId, _namespaceId, _node.NodeId).Append(finalAuthorEpoch).Distinct().OrderBy(value => value))
                {
                    if (!closureStore.TryCloseLocalAuthorEpoch(epoch))
                        throw new InvalidOperationException("Final author report requires every issued epoch to have no remaining local feed drafts; the provider closes each epoch atomically before signing.");
                }
            }
            else if (finalAuthorEpoch != 0)
            {
                throw new ArgumentException("finalAuthorEpoch is only valid when finalForAuthor is true.", nameof(finalAuthorEpoch));
            }

            ulong unpublishedDraftCount = hostDrafts.Count;
            ulong maxDraftHlcWall = hostDrafts.MaxHlcWall;
            uint maxDraftHlcLogical = hostDrafts.MaxHlcLogical;
            ulong providerHighWatermark = _provider.LocalChangeHighWatermark;
            ulong afterSequence = 0;
            while (afterSequence < providerHighWatermark)
            {
                ProviderChangePage page = _provider.ReadLocalChanges(afterSequence, pageSize);
                if (page.ScannedThroughSequence <= afterSequence ||
                    page.ScannedThroughSequence > providerHighWatermark ||
                    page.Changes.Count > pageSize)
                    throw new InvalidDataException("Provider feed returned an invalid frontier scan page.");

                ulong previousChangeSequence = afterSequence;
                foreach (ProviderChange change in page.Changes)
                {
                    if (change.Sequence <= previousChangeSequence ||
                        change.Sequence > page.ScannedThroughSequence)
                        throw new InvalidDataException("Provider feed draft sequences are not ordered within the scanned prefix.");
                    previousChangeSequence = change.Sequence;

                    ProviderMutation mutation = change.Mutation
                        ?? throw new InvalidDataException("Provider feed contains a null draft mutation.");
                    unpublishedDraftCount = checked(unpublishedDraftCount + 1);
                    if (mutation.HlcWall > maxDraftHlcWall ||
                        (mutation.HlcWall == maxDraftHlcWall && mutation.HlcLogical > maxDraftHlcLogical))
                    {
                        maxDraftHlcWall = mutation.HlcWall;
                        maxDraftHlcLogical = mutation.HlcLogical;
                    }
                }

                // A short page is not an end-of-feed signal for arbitrary providers.
                // Advance by the provider's explicit scanned cursor until the captured
                // high-watermark is covered; otherwise a false zero-draft report could
                // authorize an unsafe FULL_REPLACE retention checkpoint.
                afterSequence = page.ScannedThroughSequence;
            }

            if (finalForAuthor && unpublishedDraftCount != 0)
                throw new InvalidOperationException("Cannot close a local author epoch while unpublished provider drafts remain.");
            if (finalForAuthor)
                ((IProviderAuthorEpochClosureStore)_provider).CloseLocalAuthorIdentity(finalAuthorEpoch);
            return metadataStore.PublishAppliedOriginFrontier(
                _scopeId, _node.NodeId, roster, unpublishedDraftCount,
                maxDraftHlcWall, maxDraftHlcLogical, finalForAuthor, memberSeed);
        }
    }

    /// <summary>
    /// Publish the authority-signed retention checkpoint from the latest durable
    /// member reports. The checkpoint remains conservative when any Active member
    /// has not reported; a FULL_REPLACE binding is accepted only after every Active
    /// member's signed installation ACK has been persisted.
    /// </summary>
    public NamespaceRetentionCheckpoint PublishRetentionCheckpoint(
        byte[] authoritySeed,
        Guid? fullReplaceGeneration = null,
        byte[]? fullReplaceDigest = null)
    {
        ArgumentNullException.ThrowIfNull(authoritySeed);
        if (authoritySeed.Length != 32)
            throw new ArgumentException("Ed25519 namespace authority seed must be 32 bytes.", nameof(authoritySeed));
        if (fullReplaceGeneration.HasValue &&
            (fullReplaceGeneration.Value == Guid.Empty ||
             fullReplaceDigest is null || fullReplaceDigest.Length != NamespaceRosterProtocol.DigestSize))
            throw new ArgumentException("A FULL_REPLACE checkpoint binding requires a non-empty generation and 32-byte digest.");
        if (!fullReplaceGeneration.HasValue && fullReplaceDigest is { Length: > 0 })
            throw new ArgumentException("FULL_REPLACE digest cannot be supplied without a generation.", nameof(fullReplaceDigest));

        using IDisposable commitGuard = AcquireApplicationCommitGuard();
        lock (_gate)
        {
            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the signed namespace-GC store before publishing retention checkpoints.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.TryReadNamespaceRoster(_namespaceId, trustStore, out NamespaceRosterCertificate? roster) ||
                roster is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");
            NamespaceRosterProtocol.VerifyOrThrow(roster, trustStore);
            if (roster.AuthorityNodeId != _node.NodeId)
                throw new InvalidOperationException("Only the pinned namespace authority may publish a retention checkpoint.");

            gcStore.TryReadRetentionCheckpoint(roster, trustStore, out NamespaceRetentionCheckpoint? prior);
            ulong sequence = checked((prior?.CheckpointSequence ?? 0) + 1);
            byte[] previousDigest = prior is null
                ? new byte[NamespaceRosterProtocol.DigestSize]
                : NamespaceRetentionCheckpointProtocol.ComputeDigest(prior);
            ulong hlcWallFloor = 0;
            uint hlcLogicalFloor = 0;
            if (fullReplaceGeneration.HasValue)
            {
                if (!gcStore.AreAllActiveMembersInstalled(
                        roster, fullReplaceGeneration.Value, fullReplaceDigest!))
                    throw new InvalidOperationException("Cannot bind a retention checkpoint before every Active member has durably installed the FULL_REPLACE generation.");

                IReadOnlyList<FullReplaceInstalledAck> acknowledgements =
                    gcStore.ReadFullReplaceInstalledAcks(roster, fullReplaceGeneration.Value);
                NamespaceRosterMember[] activeMembers = roster.Members
                    .Where(member => member.Status == NamespaceMemberStatus.Active)
                    .ToArray();
                if (activeMembers.Length == 0)
                    throw new InvalidOperationException("A FULL_REPLACE retention checkpoint requires at least one Active member.");

                bool firstFloor = true;
                foreach (NamespaceRosterMember member in activeMembers)
                {
                    FullReplaceInstalledAck acknowledgement = acknowledgements.FirstOrDefault(item =>
                        item.MemberNodeId == member.NodeId)
                        ?? throw new InvalidDataException("An Active member's installed ACK is missing.");
                    if (!acknowledgement.Installed ||
                        acknowledgement.Generation != fullReplaceGeneration.Value ||
                        !acknowledgement.Digest.AsSpan().SequenceEqual(fullReplaceDigest))
                        throw new InvalidDataException("An installed ACK does not match the checkpoint FULL_REPLACE generation/digest.");

                    if (firstFloor)
                    {
                        hlcWallFloor = acknowledgement.HlcWallFloor;
                        hlcLogicalFloor = acknowledgement.HlcLogicalFloor;
                        firstFloor = false;
                    }
                    else if (acknowledgement.HlcWallFloor != hlcWallFloor ||
                        acknowledgement.HlcLogicalFloor != hlcLogicalFloor)
                    {
                        throw new InvalidDataException("Active members reported different HLC floors for the same FULL_REPLACE digest.");
                    }
                }
            }

            IReadOnlyList<AppliedOriginFrontierReport> reports = gcStore.ReadOriginFrontiers(roster);
            NamespaceRetentionCheckpoint checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, sequence, previousDigest, reports,
                fullReplaceGeneration, fullReplaceDigest ?? Array.Empty<byte>(),
                hlcWallFloor, hlcLogicalFloor);
            byte[] signed = NamespaceRetentionCheckpointProtocol.Sign(checkpoint, authoritySeed);
            return gcStore.AcceptRetentionCheckpoint(signed, roster, trustStore);
        }
    }

    /// <summary>Send the locally accepted, authority-signed roster to one negotiated peer.</summary>
    public async Task SendAcceptedNamespaceRosterAsync(
        SyncSession session, byte[] authoritySeed, ulong frameSequence = 1,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(authoritySeed);
        StorageNamespaceGcStore gcStore;
        TrustStore trustStore;
        lock (_gate)
        {
            gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the namespace-GC store before roster exchange.");
            trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
        }
        if (!gcStore.TryReadNamespaceRoster(_namespaceId, trustStore, out NamespaceRosterCertificate? roster) ||
            roster is null)
            throw new InvalidOperationException("No accepted namespace roster is available to send.");
        if (roster.AuthorityNodeId != _node.NodeId)
            throw new InvalidOperationException("Only the namespace authority may send the accepted roster.");
        byte[] signed = NamespaceRosterProtocol.Sign(roster, authoritySeed);
        _ = NamespaceRosterProtocol.DecodeAndVerify(signed, trustStore);
        await session.SendNamespaceRosterAsync(roster, authoritySeed, frameSequence, ct).ConfigureAwait(false);
    }

    /// <summary>Receive, verify and durably accept the peer's signed namespace roster.</summary>
    public async Task<NamespaceRosterCertificate> ReceiveAndAcceptNamespaceRosterAsync(
        SyncSession session, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        StorageNamespaceGcStore gcStore;
        TrustStore trustStore;
        lock (_gate)
        {
            gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the namespace-GC store before roster exchange.");
            trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
        }
        NamespaceRosterCertificate received =
            await session.ReceiveNamespaceRosterAsync(trustStore, ct).ConfigureAwait(false);
        if (received.NamespaceId != _namespaceId ||
            received.AuthorityNodeId != session.PeerNodeId)
            throw new InvalidDataException(
                "The signed namespace roster does not match the configured namespace and authenticated authority session.");
        NamespaceRosterMember? authorityMember = received.Members
            .FirstOrDefault(member => member.NodeId == received.AuthorityNodeId);
        if (authorityMember is null ||
            authorityMember.TransportPeerEpoch != checked((uint)session.PeerEpoch))
            throw new InvalidDataException(
                "The roster authority transport epoch does not match the authenticated session.");
        return gcStore.AcceptNamespaceRoster(
            EncodeSignedPayload(NamespaceRosterProtocol.EncodePayload(received), received.Signature),
            trustStore);
    }

    /// <summary>
    /// Resend the latest locally accepted durable OFR2 report without minting a new
    /// report sequence. Use after transport failure so retries preserve the same signed
    /// report identity and do not create artificial frontier history.
    /// </summary>
    public async Task<AppliedOriginFrontierReport> SendAcceptedAppliedOriginFrontierAsync(
        SyncSession session, byte[] memberSeed, ulong frameSequence = 1,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(memberSeed);
        if (memberSeed.Length != 32)
            throw new ArgumentException("Ed25519 member seed must be 32 bytes.", nameof(memberSeed));

        StorageNamespaceGcStore gcStore;
        TrustStore trustStore;
        NamespaceRosterCertificate roster;
        lock (_gate)
        {
            gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the namespace-GC store before frontier resend.");
            trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.TryReadNamespaceRoster(_namespaceId, trustStore, out NamespaceRosterCertificate? accepted) ||
                accepted is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");
            roster = accepted;
        }

        AppliedOriginFrontierReport? report = gcStore.ReadOriginFrontiers(roster)
            .Where(item => item.MemberNodeId == _node.NodeId &&
                item.NamespaceId == _namespaceId &&
                item.RosterGeneration == roster.RosterGeneration)
            .OrderByDescending(item => item.MemberTransportEpoch)
            .ThenByDescending(item => item.ReportSequence)
            .FirstOrDefault();
        if (report is null)
            throw new InvalidOperationException("No durable local applied-origin frontier report is available to resend.");

        byte[] encoded = EncodeSignedPayload(
            OriginFrontierProtocol.EncodePayload(report), report.Signature);
        AppliedOriginFrontierReport verified =
            OriginFrontierProtocol.DecodeAndVerify(encoded, roster);
        byte[] locallySigned = OriginFrontierProtocol.Sign(verified, memberSeed);
        if (!locallySigned.AsSpan(locallySigned.Length - BatchSignature.SignatureSize)
                .SequenceEqual(verified.Signature))
            throw new CryptographicException("The supplied member seed does not match the persisted frontier signature.");
        await session.SendOriginFrontierAsync(verified, memberSeed, frameSequence, ct)
            .ConfigureAwait(false);
        return verified;
    }

    /// <summary>Publish and persist the local OFR2 report before sending it to the authority peer.</summary>
    public async Task<AppliedOriginFrontierReport> PublishAndSendAppliedOriginFrontierAsync(
        SyncSession session, byte[] memberSeed,
        IApplicationGcReferenceScanner externalState,
        Func<IDisposable?> acquireHostCommitLease,
        int pageSize = 1024, ulong frameSequence = 1,
        bool finalForAuthor = false, uint finalAuthorEpoch = 0,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        AppliedOriginFrontierReport report = PublishAppliedOriginFrontier(
            memberSeed, externalState, acquireHostCommitLease,
            pageSize, finalForAuthor, finalAuthorEpoch);
        await session.SendOriginFrontierAsync(report, memberSeed, frameSequence, ct).ConfigureAwait(false);
        return report;
    }

    /// <summary>Receive, verify and durably accept an Active member's OFR2 report.</summary>
    public async Task<AppliedOriginFrontierReport> ReceiveAndAcceptOriginFrontierAsync(
        SyncSession session, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        StorageNamespaceGcStore gcStore;
        NamespaceRosterCertificate roster;
        lock (_gate)
        {
            gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the namespace-GC store before frontier exchange.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.TryReadNamespaceRoster(_namespaceId, trustStore, out NamespaceRosterCertificate? accepted) ||
                accepted is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");
            roster = accepted;
        }
        AppliedOriginFrontierReport report =
            await session.ReceiveOriginFrontierAsync(roster, ct).ConfigureAwait(false);
        if (report.NamespaceId != _namespaceId ||
            report.MemberNodeId != session.PeerNodeId ||
            report.MemberTransportEpoch != checked((uint)session.PeerEpoch))
            throw new InvalidDataException(
                "The signed OFR2 member identity/epoch does not match the authenticated session.");
        return gcStore.AcceptOriginFrontier(
            EncodeSignedPayload(OriginFrontierProtocol.EncodePayload(report), report.Signature), roster);
    }

    /// <summary>Publish and persist the authority checkpoint before sending it to one member.</summary>
    public async Task<NamespaceRetentionCheckpoint> PublishAndSendRetentionCheckpointAsync(
        SyncSession session, byte[] authoritySeed, Guid? fullReplaceGeneration = null,
        byte[]? fullReplaceDigest = null, ulong frameSequence = 1,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        NamespaceRetentionCheckpoint checkpoint = PublishRetentionCheckpoint(
            authoritySeed, fullReplaceGeneration, fullReplaceDigest);
        await SendAcceptedRetentionCheckpointAsync(
            session, checkpoint, authoritySeed, frameSequence, ct).ConfigureAwait(false);
        return checkpoint;
    }

    /// <summary>
    /// Broadcast an already-published checkpoint to another member without minting a
    /// new checkpoint sequence. This lets the authority publish one durable checkpoint
    /// per control-plane round and send the same signed object to every Active member.
    /// </summary>
    public async Task SendAcceptedRetentionCheckpointAsync(
        SyncSession session, NamespaceRetentionCheckpoint checkpoint, byte[] authoritySeed,
        ulong frameSequence = 1, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(checkpoint);
        ArgumentNullException.ThrowIfNull(authoritySeed);
        if (authoritySeed.Length != 32)
            throw new ArgumentException("Ed25519 namespace authority seed must be 32 bytes.", nameof(authoritySeed));

        NamespaceRetentionCheckpoint verified;
        lock (_gate)
        {
            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the signed namespace-GC store before checkpoint broadcast.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.TryReadNamespaceRoster(_namespaceId, trustStore, out NamespaceRosterCertificate? roster) ||
                roster is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");

            if (roster.AuthorityNodeId != _node.NodeId ||
                checkpoint.NamespaceId != _namespaceId ||
                checkpoint.RosterGeneration != roster.RosterGeneration)
                throw new InvalidOperationException("Only this namespace authority may broadcast its accepted checkpoint.");

            if (!gcStore.TryReadRetentionCheckpoint(roster, trustStore, out NamespaceRetentionCheckpoint? accepted) ||
                accepted is null ||
                accepted.CheckpointSequence != checkpoint.CheckpointSequence ||
                !NamespaceRetentionCheckpointProtocol.ComputeDigest(accepted).AsSpan()
                    .SequenceEqual(NamespaceRetentionCheckpointProtocol.ComputeDigest(checkpoint)))
                throw new InvalidDataException("Only the currently durable accepted checkpoint can be broadcast.");

            byte[] signed = NamespaceRetentionCheckpointProtocol.Sign(accepted, authoritySeed);
            verified = gcStore.AcceptRetentionCheckpoint(signed, roster, trustStore);
            if (!NamespaceRetentionCheckpointProtocol.ComputeDigest(verified).AsSpan()
                    .SequenceEqual(NamespaceRetentionCheckpointProtocol.ComputeDigest(accepted)))
                throw new InvalidDataException("Checkpoint signing did not preserve the accepted durable checkpoint.");
        }

        await session.SendRetentionCheckpointAsync(verified, authoritySeed, frameSequence, ct)
            .ConfigureAwait(false);
    }

    /// <summary>Receive, verify and durably accept the authority's RCP2 checkpoint.</summary>
    public async Task<NamespaceRetentionCheckpoint> ReceiveAndAcceptRetentionCheckpointAsync(
        SyncSession session, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        StorageNamespaceGcStore gcStore;
        TrustStore trustStore;
        NamespaceRosterCertificate roster;
        lock (_gate)
        {
            gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the namespace-GC store before checkpoint exchange.");
            trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.TryReadNamespaceRoster(_namespaceId, trustStore, out NamespaceRosterCertificate? accepted) ||
                accepted is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");
            roster = accepted;
        }
        NamespaceRetentionCheckpoint checkpoint =
            await session.ReceiveRetentionCheckpointAsync(roster, trustStore, ct).ConfigureAwait(false);
        if (session.PeerNodeId != roster.AuthorityNodeId ||
            checkpoint.NamespaceId != _namespaceId ||
            checkpoint.RosterGeneration != roster.RosterGeneration)
            throw new InvalidDataException(
                "The signed RCP2 checkpoint does not match the authenticated namespace authority and accepted roster.");
        return gcStore.AcceptRetentionCheckpoint(
            EncodeSignedPayload(NamespaceRetentionCheckpointProtocol.EncodePayload(checkpoint), checkpoint.Signature),
            roster, trustStore);
    }

    /// <summary>Send only an installed ACK that is already durable in the local GC store.</summary>
    public async Task SendDurableFullReplaceInstalledAckAsync(
        SyncSession session, FullReplaceInstalledAck acknowledgement, byte[] memberSeed,
        ulong frameSequence = 1, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(acknowledgement);
        ArgumentNullException.ThrowIfNull(memberSeed);
        if (acknowledgement.NamespaceId != _namespaceId ||
            acknowledgement.MemberNodeId != _node.NodeId ||
            !acknowledgement.Installed || acknowledgement.Generation == Guid.Empty)
            throw new InvalidDataException("Only this node's installed FULL_REPLACE ACK can be sent.");
        StorageNamespaceGcStore gcStore;
        TrustStore trustStore;
        NamespaceRosterCertificate roster;
        lock (_gate)
        {
            gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the namespace-GC store before installed-ACK exchange.");
            trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.TryReadNamespaceRoster(_namespaceId, trustStore, out NamespaceRosterCertificate? accepted) ||
                accepted is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");
            roster = accepted;
        }
        FullReplaceInstalledAck durable = gcStore.ReadFullReplaceInstalledAcks(roster, acknowledgement.Generation)
            .FirstOrDefault(item => item.MemberNodeId == _node.NodeId)
            ?? throw new InvalidOperationException("The installed ACK is not durable; refusing to send it.");
        if (durable.MemberTransportEpoch != acknowledgement.MemberTransportEpoch ||
            durable.RosterGeneration != acknowledgement.RosterGeneration ||
            !durable.Digest.AsSpan().SequenceEqual(acknowledgement.Digest) ||
            durable.HlcWallFloor != acknowledgement.HlcWallFloor ||
            durable.HlcLogicalFloor != acknowledgement.HlcLogicalFloor)
            throw new InvalidDataException("The supplied installed ACK differs from the durable accepted ACK.");
        byte[] signed = FullReplaceInstalledAckProtocol.Sign(durable, memberSeed);
        _ = FullReplaceInstalledAckProtocol.DecodeAndVerify(signed, roster);
        await session.SendFullReplaceInstalledAckAsync(durable, memberSeed, frameSequence, ct)
            .ConfigureAwait(false);
    }

    /// <summary>Receive, verify and durably accept a member's FULL_REPLACE installed ACK.</summary>
    public async Task<FullReplaceInstalledAck> ReceiveAndAcceptFullReplaceInstalledAckAsync(
        SyncSession session, Guid generation, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        if (generation == Guid.Empty) throw new ArgumentException("Generation must be non-empty.", nameof(generation));
        StorageNamespaceGcStore gcStore;
        NamespaceRosterCertificate roster;
        lock (_gate)
        {
            gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the namespace-GC store before installed-ACK exchange.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.TryReadNamespaceRoster(_namespaceId, trustStore, out NamespaceRosterCertificate? accepted) ||
                accepted is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");
            roster = accepted;
        }
        FullReplaceInstalledAck acknowledgement =
            await session.ReceiveFullReplaceInstalledAckAsync(roster, ct).ConfigureAwait(false);
        if (acknowledgement.Generation != generation)
            throw new InvalidDataException("Installed ACK belongs to another FULL_REPLACE generation.");
        if (acknowledgement.NamespaceId != _namespaceId ||
            acknowledgement.MemberNodeId != session.PeerNodeId ||
            acknowledgement.MemberTransportEpoch != checked((uint)session.PeerEpoch) ||
            acknowledgement.RosterGeneration != roster.RosterGeneration)
            throw new InvalidDataException(
                "The signed FRA2 member identity/epoch does not match the authenticated session and accepted roster.");
        return gcStore.AcceptFullReplaceInstalledAck(
            EncodeSignedPayload(FullReplaceInstalledAckProtocol.EncodePayload(acknowledgement),
                acknowledgement.Signature), roster);
    }

    private static byte[] EncodeSignedPayload(byte[] payload, byte[] signature)
    {
        if (signature is null || signature.Length != BatchSignature.SignatureSize)
            throw new InvalidDataException("A verified signed control message must retain its 64-byte signature.");
        byte[] encoded = new byte[checked(payload.Length + signature.Length)];
        payload.CopyTo(encoded, 0);
        signature.CopyTo(encoded, payload.Length);
        return encoded;
    }

    /// <summary>
    /// Complete the receiver's mandatory post-FULL_REPLACE local refresh before it
    /// publishes the installed ACK: advance HLC to the signed snapshot floor, rebuild
    /// the Node cache from the newly committed provider generation, replay durable
    /// PendingApply intents, and checkpoint the resulting Node state.
    /// Pass this method as the receiver's (wall, logical) refresh callback.
    /// </summary>
    public void RefreshAfterFullReplace(
        Action<Node> persistNode, ulong hlcWallFloor, uint hlcLogicalFloor)
    {
        ArgumentNullException.ThrowIfNull(persistNode);
        using IDisposable commitGuard = AcquireApplicationCommitGuard();
        lock (_gate)
        {
            _node.AdvanceClockToFloor(hlcWallFloor, hlcLogicalFloor);
            InstallProviderSnapshot(persistNode);
            HlcClockSnapshot clock = _node.ClockState;
            if (clock.LastWall < hlcWallFloor ||
                (clock.LastWall == hlcWallFloor && clock.LastLogical < hlcLogicalFloor))
                throw new InvalidOperationException("Node HLC did not retain the installed FULL_REPLACE floor.");
        }
    }

    /// <summary>Create the mandatory receiver callback that installs HLC floor and refreshes this Node.</summary>
    public Action<ulong, uint> CreateFullReplaceRefreshCallback(Action<Node> persistNode)
    {
        ArgumentNullException.ThrowIfNull(persistNode);
        return (wall, logical) => RefreshAfterFullReplace(persistNode, wall, logical);
    }

    /// <summary>Compact ChangeId fingerprints below the signed origin frontier. The supplied lease must be the composed application barrier (coordinator, Node, host and provider gates); the reference scanner opens snapshots under that already-held lease and must not reacquire the host gate.</summary>
    public int CompactChangeLedger(
        int namespaceId, int limit, Func<IDisposable?> acquireGcLease,
        IApplicationGcReferenceScanner externalReferences)
    {
        if (_metadataStore is not StorageMetadataStore storageMetadata)
            throw new NotSupportedException("ChangeId GC requires the durable StorageMetadataStore implementation.");
        return storageMetadata.CompactChangeLedger(_scopeId
            ?? throw new InvalidOperationException("A durable metadata scope is required."),
            _node.NodeId, namespaceId, _provider, limit, acquireGcLease, externalReferences);
    }

    /// <summary>Attempt fail-closed record-tombstone compaction. The supplied lease must be the composed application barrier (coordinator, Node, host and provider gates); the reference scanner opens snapshots under that already-held lease and must not reacquire the host gate.</summary>
    public int CompactRecordTombstones(
        int namespaceId, int limit, Func<IDisposable?> acquireGcLease,
        IApplicationGcReferenceScanner externalReferences)
    {
        if (_provider is not StorageDataProvider storageProvider)
            throw new NotSupportedException("Record-tombstone GC requires the durable StorageDataProvider implementation.");
        if (_metadataStore is not StorageMetadataStore storageMetadata)
            throw new NotSupportedException("Record-tombstone GC requires the durable StorageMetadataStore implementation.");
        StorageNamespaceGcStore gcStore = _namespaceGcStore
            ?? throw new InvalidOperationException("Attach the namespace-GC proof store before tombstone GC.");
        TrustStore trustStore = _namespaceGcTrustStore
            ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
        return storageProvider.CompactRecordTombstones(
            _scopeId ?? throw new InvalidOperationException("A durable metadata scope is required."),
            _node.NodeId, namespaceId, storageMetadata, gcStore, trustStore,
            limit, acquireGcLease, externalReferences);
    }

    /// <summary>Compact incoming batch receipts below the signed origin frontier under the composed application barrier, including host/provider gates; the reference scanner must not reacquire the host gate.</summary>
    public int CompactReceipts(
        int namespaceId, int limit, Func<IDisposable?> acquireGcLease,
        IApplicationGcReferenceScanner externalReferences)
    {
        if (_metadataStore is not StorageMetadataStore storageMetadata)
            throw new NotSupportedException("Receipt GC requires the durable StorageMetadataStore implementation.");
        return storageMetadata.CompactReceipts(_scopeId
            ?? throw new InvalidOperationException("A durable metadata scope is required."),
            _node.NodeId, namespaceId, limit, acquireGcLease, externalReferences);
    }

    /// <summary>
    /// Performs the one-time, fail-closed transition from Node-Outbox mirroring to
    /// Metadata-Store-owned outbound sequencing. The provider feed must already be
    /// drained through the legacy path; every retained Node Outbox payload is verified
    /// against its durable Metadata ChangeId identity before the cutover marker
    /// or any duplicate-payload cleanup is allowed.
    /// </summary>
    public void EnableMetadataStoreOwnedPublication(Action<Node> persistNode)
    {
        ArgumentNullException.ThrowIfNull(persistNode);
        using IDisposable commitGuard = AcquireApplicationCommitGuard();
        if (_metadataStore is null || string.IsNullOrWhiteSpace(_scopeId))
            throw new InvalidOperationException("Metadata-Store-owned publication requires a durable Metadata Store scope.");
        if (_metadataStore is not IMetadataOutboundChangeIdentityStore ||
            _metadataStore is not IMetadataChangeIdentityStore)
            throw new NotSupportedException("Metadata-Store-owned publication requires durable outbound and complete ChangeId identity capabilities.");
        if (_provider is not IProviderPublicationMode publicationMode)
            throw new NotSupportedException("The attached provider does not support a durable outbound authority cutover.");
        if (_provider is not IProviderPublishedOriginStore)
            throw new NotSupportedException("The attached provider cannot durably stamp immutable publication origins.");

        lock (_gate)
        {
            if (publicationMode.TryGetMetadataStoreOwnedPublicationCutover(out ulong existingCutover))
            {
                // Older/interrupted checkpoints may still contain Node-owned identity
                // history. Prove its ChangeIds exist in Metadata Store before discarding
                // any retained Node Outbox payload projection.
                ValidateRetainedNodeOutboxOwnership();
                _node.EnableMetadataStoreOwnedPublicationFence(_namespaceId);
                long currentNodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
                if (currentNodeHigh < 0 || (ulong)currentNodeHigh != existingCutover)
                    throw new InvalidOperationException("Node Outbox advanced after Metadata-Store-owned publication was enabled.");
                // Idempotent enable also completes a cutover interrupted after the
                // provider marker commit but before the duplicate Node payload cache
                // was checkpointed away.
                InstallProviderSnapshot(persistNode);
                return;
            }

            ProviderChangePage pending = _provider.ReadLocalChanges(0, 1);
            if (pending.Changes.Count != 0)
                throw new InvalidOperationException(
                    "Drain and acknowledge the provider local feed through the Node-Outbox transition path before enabling Metadata-Store-owned publication.");

            long nodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
            if (nodeHigh < 0)
                throw new InvalidDataException("Node Outbox high watermark is invalid.");
            ulong metadataHigh = _metadataStore.GetOutboundHighWatermark(
                _scopeId!, _node.NodeId, _namespaceId, _node.NodeId);
            if (metadataHigh != (ulong)nodeHigh)
                throw new InvalidOperationException(
                    "Node and Metadata Store outbound watermarks must already match. Drain the legacy provider feed and freeze every Node Outbox row before cutover; Node-only writes must first be migrated into the provider.");

            // Prove every retained Node payload is already represented by the
            // durable Metadata Store before the irreversible provider-authority marker.
            // This includes local ChangeId fingerprints and all foreign-author relay rows.
            ValidateRetainedNodeOutboxOwnership();

            // Fence direct Node local writes before replacing the cache. The durable
            // provider marker is the restart authority; a failed cutover remains
            // fail-closed in this live Node instance.
            _node.EnableMetadataStoreOwnedPublicationFence(_namespaceId);
            // Rebuild the Node cache from the provider snapshot only after the
            // existing Node Outbox range is known to be fully frozen. The Node
            // sequence watermark then becomes an immutable fence.
            InstallProviderSnapshot(persistNode);

            nodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
            metadataHigh = _metadataStore.GetOutboundHighWatermark(
                _scopeId!, _node.NodeId, _namespaceId, _node.NodeId);
            if (nodeHigh < 0 || metadataHigh != (ulong)nodeHigh)
                throw new InvalidOperationException("Node and Metadata Store outbound watermarks are not aligned at cutover.");

            publicationMode.EnableMetadataStoreOwnedPublication((ulong)nodeHigh);
            // The provider snapshot is the authoritative namespace state and the
            // Metadata Store is the only delivery source after cutover. Remove every
            // retained Node payload projection (local Outbox and legacy relay rows),
            // while preserving each author stream's sequence watermark and replay IDs.
            // Ownership and relay coverage were proved before the durable marker. The
            // provider snapshot may remove pending remote IDs from Node's legacy ledger,
            // so do not repeat that preflight after the snapshot has intentionally changed it.
            DiscardNodeOutboxPayloadsAfterProviderCutover(ownershipAlreadyValidated: true);
            persistNode(_node);
        }
    }

    /// <summary>
    /// Publishes the provider's pending local feed. Before cutover, winners are imported
    /// through Node Outbox and frozen to Metadata Store; after cutover, stable provider
    /// ChangeIds are frozen directly and Node is updated incrementally from the feed.
    /// </summary>
    public int PublishLocalChanges(Action<Node> persistNode, int limit = 1024)
    {
        ArgumentNullException.ThrowIfNull(persistNode);
        if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
        lock (_gate)
            return PublishLocalChangesCore(persistNode, limit);
    }

    private int PublishLocalChangesCore(
        Action<Node> persistNode, int limit)
    {
        var winners = new Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange>();
        var recoveredPublishedOrigins = new List<ProviderPublishedOrigin>();
        ulong afterSequence = 0;
        ulong providerHighWatermark = _provider.LocalChangeHighWatermark;
        ulong scannedThroughSequence = 0;
        ulong lastFrozenFeedSequence = 0;
        ulong cutoverNodeOutboxHigh = 0;
        int totalChanges = 0;
        bool sawStableChangeId = false;
        bool sawLegacyChangeId = false;
        bool metadataStoreOwnsOutbound = _provider is IProviderPublicationMode initialPublicationMode &&
            initialPublicationMode.TryGetMetadataStoreOwnedPublicationCutover(out cutoverNodeOutboxHigh);
        Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange>? nodeWinners = null;
        IMetadataOutboundChangeIdentityStore? outboundIdentityStore =
            _metadataStore as IMetadataOutboundChangeIdentityStore;
        if (metadataStoreOwnsOutbound && (_metadataStore is null || string.IsNullOrWhiteSpace(_scopeId)))
            throw new InvalidOperationException("Provider publication cutover exists but the durable Metadata Store scope is not attached.");
        if (metadataStoreOwnsOutbound && outboundIdentityStore is null)
            throw new NotSupportedException("Provider publication cutover requires the outbound ChangeId identity capability.");

        // Drain the complete provider-feed prefix visible at method entry, not just
        // one page. Later concurrent appends remain pending for the next pass instead
        // of extending this scan indefinitely. The winner map is bounded by distinct
        // record/field identities rather than historical edits within that prefix.
        while (afterSequence < providerHighWatermark)
        {
            var page = _provider.ReadLocalChanges(afterSequence, limit);
            if (page.Changes.Count > limit || page.ScannedThroughSequence < afterSequence)
                throw new InvalidDataException("Provider local feed returned an invalid scan page.");
            if (page.ScannedThroughSequence == afterSequence)
                throw new InvalidDataException("Provider local feed cursor did not advance toward its captured high-watermark.");

            int pageChangeCount = 0;
            ulong previousChangeSequence = afterSequence;
            foreach (var change in page.Changes)
            {
                if (change.Sequence <= previousChangeSequence ||
                    change.Sequence > page.ScannedThroughSequence)
                    throw new InvalidDataException("Provider local feed changes are not ordered within the scanned prefix.");
                previousChangeSequence = change.Sequence;
                // The provider may append while this scan is in progress. Its page can
                // therefore extend beyond the high-watermark captured at entry; leave
                // that suffix for the next publication pass rather than chasing writers.
                if (change.Sequence > providerHighWatermark)
                    continue;
                pageChangeCount++;
                _node.ValidateProviderMutation(change.Mutation);
                if (change.Mutation.KeyEpoch != 0)
                    throw new NotSupportedException("The current Node field-write API accepts key epoch zero only.");
                sawStableChangeId |= !change.ChangeId.IsZero;
                sawLegacyChangeId |= change.ChangeId.IsZero;
                bool sharedNodeAndOutboundWinners = false;
                if (metadataStoreOwnsOutbound)
                {
                    if (change.ChangeId.IsZero)
                        throw new InvalidDataException("Metadata-Store-owned publication requires stable provider ChangeIds.");

                    // The Node cache only needs the HLC winner per record/field. Keep
                    // the same compact winner map for both purposes until a previously
                    // frozen ChangeId is encountered; then fork it once so the Node
                    // cache retains the full scanned prefix while outbound coalescing
                    // starts at the unfrozen suffix.
                    nodeWinners ??= winners;
                    sharedNodeAndOutboundWinners = ReferenceEquals(nodeWinners, winners);
                    if (sharedNodeAndOutboundWinners)
                        AddCoalescedPendingLocalChange(winners, change);
                    else
                        AddCoalescedPendingLocalChange(nodeWinners, change);

                    if (outboundIdentityStore!.TryGetOutboundChangeIdentity(
                        _scopeId!, _node.NodeId, _namespaceId, change.ChangeId,
                        out ReadOnlyMemory<byte> frozenFingerprint, out OperationOrigin frozenOrigin))
                    {
                        Digest16 actualFingerprint = ComputeMetadataFingerprint(change);
                        if (frozenFingerprint.Length != 16 || Digest16.From(frozenFingerprint.Span) != actualFingerprint)
                            throw new InvalidDataException("Provider ChangeId conflicts with its previously frozen outbound operation.");
                        recoveredPublishedOrigins.Add(new ProviderPublishedOrigin(change, frozenOrigin));
                        lastFrozenFeedSequence = change.Sequence;
                        // Preserve all scanned winners for Node recovery, but discard
                        // the outbound suffix because it was frozen by an earlier pass.
                        if (sharedNodeAndOutboundWinners)
                            nodeWinners = new Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange>(winners);
                        winners.Clear();
                        continue;
                    }
                    if (change.Sequence <= lastFrozenFeedSequence)
                        throw new InvalidDataException("Provider local feed is not in strictly increasing source-sequence order.");
                    if (!sharedNodeAndOutboundWinners)
                        AddCoalescedPendingLocalChange(winners, change);
                }
                else
                {
                    AddCoalescedPendingLocalChange(winners, change);
                }
            }

            totalChanges = checked(totalChanges + pageChangeCount);
            scannedThroughSequence = Math.Min(page.ScannedThroughSequence, providerHighWatermark);
            afterSequence = scannedThroughSequence;
        }

        if (metadataStoreOwnsOutbound)
        {
            long currentNodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
            if (currentNodeHigh < 0 || (ulong)currentNodeHigh != cutoverNodeOutboxHigh)
                throw new InvalidOperationException("Node Outbox advanced after Metadata-Store-owned publication was enabled.");
            ulong currentMetadataHigh = _metadataStore!.GetOutboundHighWatermark(
                _scopeId!, _node.NodeId, _namespaceId, _node.NodeId);
            if (currentMetadataHigh < cutoverNodeOutboxHigh)
                throw new InvalidDataException("Metadata Store outbound watermark regressed below the provider cutover fence.");
        }
        else if (_metadataStore is not null && !string.IsNullOrWhiteSpace(_scopeId))
        {
            // If the provider-side cutover marker is missing after Metadata Store
            // has advanced beyond the Node sequence, never silently fall back to the
            // legacy mirror path: that could publish an already-frozen feed row twice.
            long currentNodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
            if (currentNodeHigh < 0)
                throw new InvalidDataException("Node Outbox high watermark is invalid.");
            ulong currentMetadataHigh = _metadataStore.GetOutboundHighWatermark(
                _scopeId!, _node.NodeId, _namespaceId, _node.NodeId);
            if (currentMetadataHigh > (ulong)currentNodeHigh)
                throw new InvalidOperationException(
                    "Metadata Store is ahead of Node Outbox but the provider cutover marker is missing; refusing legacy publication.");
        }

        if (totalChanges == 0)
        {
            // A provider may advance the scanned cursor over local-only lifecycle
            // rows without returning a syncable ProviderChange. Commit that empty
            // prefix ACK so it is not rescanned forever on every publication pass.
            if (scannedThroughSequence > 0)
                _provider.AcknowledgeLocalChanges(scannedThroughSequence);
            return 0;
        }

        if (metadataStoreOwnsOutbound)
        {
            if (_metadataStore is null || !sawStableChangeId || sawLegacyChangeId)
                throw new InvalidDataException("Metadata-Store-owned publication requires non-zero stable ChangeIds for every provider feed entry.");
            var nodeChanges = (nodeWinners ?? winners).Values
                .OrderBy(change => change.Sequence).ToArray();
            return PublishLocalChangesToMetadataStore(
                winners, nodeChanges, scannedThroughSequence, totalChanges,
                recoveredPublishedOrigins, persistNode);
        }

        // Legacy/volatile providers without durable source identities retain the
        // transition path through Node Outbox. Durable StorageDataProvider feeds use
        // the Metadata-Store-owned path above and do not allocate duplicate Node rows.
        foreach (var change in winners.Values.OrderBy(change => change.Sequence))
        {
            ProviderMutation mutation = change.Mutation;
            var hlc = new Hlc(mutation.HlcWall, mutation.HlcLogical);
            if (mutation.IsRecordTombstone)
                _node.DeleteRecord(_namespaceId, mutation.RecordId, hlc);
            else if (mutation.IsTombstone)
                _node.TombField(_namespaceId, mutation.RecordId, mutation.FieldId, hlc);
            else
                _node.WriteField(_namespaceId, mutation.RecordId, mutation.FieldId, mutation.Value, hlc);
        }

        // Do not ACK if persistence or metadata freeze fails. The provider feed
        // remains the durable PendingLocal journal and is replayed/coalesced on restart.
        persistNode(_node);
        FreezeLocalOutboxToMetadata();
        if (scannedThroughSequence > 0)
            _provider.AcknowledgeLocalChanges(scannedThroughSequence);
        return totalChanges;
    }

    private int PublishLocalChangesToMetadataStore(
        Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange> winners,
        IReadOnlyList<ProviderChange> nodeChanges,
        ulong scannedThroughSequence,
        int totalChanges,
        IReadOnlyList<ProviderPublishedOrigin> recoveredPublishedOrigins,
        Action<Node> persistNode)
    {
        if (nodeChanges.Count > totalChanges)
            throw new InvalidDataException("Coalesced Node cache winners exceed the scanned provider feed prefix.");
        if (nodeChanges.Count != 0 && nodeChanges[nodeChanges.Count - 1].Sequence > scannedThroughSequence)
            throw new InvalidDataException("Provider cache update extends beyond the scanned ACK prefix.");
        if (_metadataStore is null || string.IsNullOrWhiteSpace(_scopeId))
            throw new InvalidOperationException("Stable provider ChangeIds require a durable Metadata Store scope.");

        if (_provider is not IProviderPublicationMode publicationMode ||
            !publicationMode.TryGetMetadataStoreOwnedPublicationCutover(out ulong cutoverNodeOutboxHigh))
            throw new InvalidOperationException("Provider-authoritative outbound publication has not been enabled.");
        long nodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
        if (nodeHigh < 0 || (ulong)nodeHigh != cutoverNodeOutboxHigh)
            throw new InvalidOperationException("Node Outbox advanced after Metadata-Store-owned publication was enabled.");
        ulong metadataHigh = _metadataStore.GetOutboundHighWatermark(
            _scopeId!, _node.NodeId, _namespaceId, _node.NodeId);
        if (metadataHigh < cutoverNodeOutboxHigh)
            throw new InvalidDataException("Metadata Store outbound watermark regressed below the cutover fence.");

        ProviderChange[] orderedWinners = winners.Values.OrderBy(change => change.Sequence).ToArray();
        var frozenOperations = new List<MetadataFrozenOperation>(orderedWinners.Length);
        foreach (var change in orderedWinners)
        {
            if (change.ChangeId.IsZero)
                throw new InvalidDataException("Metadata-Store-owned publication requires a stable provider ChangeId.");
            frozenOperations.Add(CreateMetadataFrozenOperation(change));
        }

        var published = _metadataStore.FreezeOutbound(
            _scopeId!, _node.NodeId, _namespaceId, _node.NodeId, frozenOperations);
        if (published.Count != frozenOperations.Count)
            throw new InvalidDataException("Metadata Store freeze returned an unexpected operation count.");
        for (int i = 0; i < published.Count; i++)
        {
            if (published[i].ChangeId != frozenOperations[i].ChangeId)
                throw new InvalidDataException("Metadata Store returned a different provider ChangeId.");
            if (i > 0 && published[i].Sequence <= published[i - 1].Sequence)
                throw new InvalidDataException("Metadata Store returned outbound sequences out of order.");
        }

        if (_provider is not IProviderPublishedOriginStore originStore)
            throw new NotSupportedException("Metadata-Store-owned publication requires durable provider origin stamping.");
        var publishedOrigins = new List<ProviderPublishedOrigin>(
            checked(recoveredPublishedOrigins.Count + published.Count));
        publishedOrigins.AddRange(recoveredPublishedOrigins);
        for (int i = 0; i < published.Count; i++)
            publishedOrigins.Add(new ProviderPublishedOrigin(orderedWinners[i], published[i].Provenance));
        originStore.CommitPublishedOrigins(publishedOrigins);

        // Keep Node as an application-data cache, but update it from the exact feed
        // prefix instead of scanning and rebuilding the entire provider snapshot.
        // Persist the cache before ACK: a checkpoint failure leaves the feed replayable.
        // If ACK fails, reapplying the same HLC mutations is idempotent and the
        // immutable Metadata Store freeze remains available for retry.
        _node.ApplyProviderChanges(_namespaceId, nodeChanges);
        persistNode(_node);
        if (scannedThroughSequence > 0)
            _provider.AcknowledgeLocalChanges(scannedThroughSequence);
        return totalChanges;
    }

    /// <summary>
    /// Prove that every retained Node Outbox payload already has a durable identity
    /// in the Metadata Store before the duplicate Node projection can be discarded.
    /// Every row is checked against its semantic fingerprint, immutable Origin,
    /// ownership mode and legacy Node digest; foreign-author rows must also resolve
    /// to retained Metadata Relay payloads.
    /// </summary>
    private void ValidateRetainedNodeOutboxOwnership()
    {
        if (_metadataStore is not IMetadataChangeIdentityStore changeIdentityStore ||
            _metadataStore is not IMetadataLegacyNodeLedgerStore legacyLedgerStore)
            throw new NotSupportedException(
                "Provider-authoritative cutover requires the durable all-route ChangeId identity index and legacy Node digest migration capability.");

        // Node's legacy ledger retains compact origin-bound identities even after
        // Outbox payload ACK. Move each digest into the same Metadata ChangeLedger row
        // before clearing the Node projection. Retained payloads are checked exactly
        // below; compacted history cannot be re-hashed, so its original digest is
        // preserved as a versioned alias rather than compared across hash domains.
        foreach (var pair in _node.GlobalChangeLedger.Where(pair => pair.Key.Ns == _namespaceId))
        {
            if (!pair.Value.IsKnown ||
                !changeIdentityStore.TryGetChangeIdentity(
                    _scopeId!, _node.NodeId, _namespaceId, pair.Key.ChangeId,
                    out ReadOnlyMemory<byte> fingerprint, out OperationOrigin origin,
                    out bool isLocalOutbound) ||
                fingerprint.Length != 16 || origin.AuthorNodeId.IsZero ||
                origin.AuthorEpoch == 0 || origin.Sequence == 0 ||
                isLocalOutbound != (origin.AuthorNodeId == _node.NodeId))
                throw new InvalidOperationException(
                    "A retained Node GlobalChangeLedger identity is not durably represented by the Metadata Store; provider cutover cannot discard the Node Outbox payload projection.");
        }

        const int pageSize = 1024;
        foreach (NodeId author in _node.ReadOutboxAuthors(_namespaceId))
        {
            long afterSequence = 0;
            while (true)
            {
                IReadOnlyList<OutboxEntry> entries =
                    _node.ReadOutboxEntries(_namespaceId, author, afterSequence, pageSize);
                if (entries.Count == 0) break;

                long previousSequence = afterSequence;
                var relayChangeIds = author == _node.NodeId
                    ? null
                    : new BatchId[entries.Count];

                for (int i = 0; i < entries.Count; i++)
                {
                    OutboxEntry entry = entries[i];
                    if (entry.NamespaceId != _namespaceId || entry.AuthorNodeId != author ||
                        entry.Seq <= previousSequence || entry.ChangeId.IsZero ||
                        entry.Payload.IsEmpty || entry.OriginAuthorEpoch == 0 ||
                        entry.OriginSequence == 0)
                        throw new InvalidDataException(
                            "Retained Node Outbox row has an invalid namespace, author, sequence, ChangeId, origin or payload during provider cutover.");
                    previousSequence = entry.Seq;

                    object operation = Ops.DecodeOp(entry.Payload).Op;
                    Digest16 actualFingerprint = ChangeIdentity.Fingerprint(
                        _namespaceId, entry.RecordId, entry.OpKind, operation,
                        entry.AuthorNodeId, entry.CausalDeps);
                    Digest16 expectedLegacyLedgerFingerprint = ChangeIdentity.LedgerFingerprint(
                        _namespaceId, entry.RecordId, entry.OpKind, operation,
                        new OperationOrigin(author, entry.OriginAuthorEpoch, entry.OriginSequence),
                        entry.CausalDepsOrNull);
                    if (!_node.GlobalChangeLedger.TryGetValue(
                            (_namespaceId, entry.ChangeId), out ChangeLedgerValue nodeLedger) ||
                        !nodeLedger.IsKnown ||
                        nodeLedger.Fingerprint != expectedLegacyLedgerFingerprint)
                        throw new InvalidOperationException(
                            "A retained Node Outbox payload does not match its compact origin-bound Node GlobalChangeLedger fingerprint.");

                    if (!changeIdentityStore.TryGetChangeIdentity(
                            _scopeId!, _node.NodeId, _namespaceId, entry.ChangeId,
                            out ReadOnlyMemory<byte> storedFingerprint, out OperationOrigin storedOrigin,
                            out bool storedIsLocalOutbound) ||
                        storedFingerprint.Length != 16 ||
                        Digest16.From(storedFingerprint.Span) != actualFingerprint ||
                        storedOrigin.AuthorNodeId != author ||
                        storedOrigin.AuthorEpoch != entry.OriginAuthorEpoch ||
                        storedOrigin.Sequence != entry.OriginSequence ||
                        storedIsLocalOutbound != (author == _node.NodeId))
                        throw new InvalidOperationException(
                            "A retained Node Outbox payload is not represented by the same immutable Metadata Store ChangeId fingerprint, origin and ownership mode.");

                    if (relayChangeIds is not null)
                        relayChangeIds[i] = entry.ChangeId;
                }

                if (relayChangeIds is not null)
                {
                    if (_metadataStore is not StorageMetadataStore metadataStore)
                        throw new NotSupportedException(
                            "Foreign-author Node relay payloads require StorageMetadataStore relay identity resolution before cutover.");
                    var relaySequences = new ulong[relayChangeIds.Length];
                    if (!metadataStore.TryResolveRelaySequences(
                            _scopeId!, _node.NodeId, _namespaceId, author,
                            relayChangeIds, relaySequences) ||
                        relaySequences.Any(sequence => sequence == 0))
                        throw new InvalidOperationException(
                            "A retained foreign-author Node Outbox payload is not backed by a retained Metadata Store relay payload; drain or recover relay publication before cutover.");
                }

                afterSequence = previousSequence;
                if (entries.Count < pageSize) break;
            }
        }

        // Persist aliases only after every retained payload and relay route has passed
        // exact validation. Bounded atomic batches avoid one Storage commit per ChangeId;
        // a crash midway is idempotent because Node retains its complete ledger.
        const int aliasBatchSize = 4096;
        var aliasBatch = new List<MetadataLegacyNodeLedgerFingerprint>(aliasBatchSize);
        foreach (var pair in _node.GlobalChangeLedger.Where(pair => pair.Key.Ns == _namespaceId))
        {
            aliasBatch.Add(new MetadataLegacyNodeLedgerFingerprint(
                pair.Key.ChangeId, pair.Value.Fingerprint.ToArray()));
            if (aliasBatch.Count == aliasBatchSize)
            {
                legacyLedgerStore.StoreLegacyNodeLedgerFingerprints(
                    _scopeId!, _node.NodeId, _namespaceId, aliasBatch);
                aliasBatch.Clear();
            }
        }
        if (aliasBatch.Count != 0)
            legacyLedgerStore.StoreLegacyNodeLedgerFingerprints(
                _scopeId!, _node.NodeId, _namespaceId, aliasBatch);
    }

    private void DiscardNodeOutboxPayloadsAfterProviderCutover(bool ownershipAlreadyValidated = false)
    {
        if (!ownershipAlreadyValidated)
            ValidateRetainedNodeOutboxOwnership();
        // Every namespace ledger entry now has its legacy digest durably co-located
        // with the semantic identity in Metadata Store. Only now may the duplicate
        // Node projection be removed; the caller checkpoints this state immediately.
        _node.DiscardMetadataStoreOwnedChangeLedgerShadow(_namespaceId);
        foreach (NodeId author in _node.ReadOutboxAuthors(_namespaceId))
        {
            long highWatermark = _node.GetOutboxHighWatermark(_namespaceId, author);
            if (highWatermark < 0)
                throw new InvalidDataException("Node Outbox author watermark is invalid during provider cutover.");
            _node.DiscardOutboxPayloadsThrough(_namespaceId, author, highWatermark);
            if (_node.ReadOutboxEntries(_namespaceId, author, 0, 1).Count != 0)
                throw new InvalidDataException("Node Outbox payload rows remain after provider-authoritative cutover cleanup.");
        }
    }

    private Digest16 ComputeMetadataFingerprint(ProviderChange change) =>
        ChangeIdentity.Fingerprint(_namespaceId, change.Mutation, _node.NodeId);

    private MetadataFrozenOperation CreateMetadataFrozenOperation(ProviderChange change)
    {
        ProviderMutation mutation = change.Mutation;
        if (change.ChangeId.IsZero || mutation.ChangeId != change.ChangeId ||
            mutation.OriginAuthorEpoch == 0 || mutation.OriginSequence != 0 ||
            mutation.AuthorNodeId != _node.NodeId)
            throw new InvalidDataException("Provider local feed entry must retain a stable ChangeId/author epoch and defer OriginSequence until immutable freeze.");
        object op;
        byte opKind;
        if (mutation.IsRecordTombstone)
        {
            op = new OpRecordTombstone
            {
                RecordId = mutation.RecordId,
                Wall = mutation.HlcWall,
                Logical = mutation.HlcLogical,
                Origin = 1
            };
            opKind = OpsConst.TagRecordTomb;
        }
        else if (mutation.IsTombstone)
        {
            op = new OpFieldTomb
            {
                FieldId = mutation.FieldId,
                KeyEpoch = mutation.KeyEpoch,
                Wall = mutation.HlcWall,
                Logical = mutation.HlcLogical,
                Origin = 1
            };
            opKind = OpsConst.TagFieldTomb;
        }
        else
        {
            op = new OpFieldWrite
            {
                FieldId = mutation.FieldId,
                KeyEpoch = mutation.KeyEpoch,
                Wall = mutation.HlcWall,
                Logical = mutation.HlcLogical,
                Origin = 1,
                Value = mutation.Value.ToArray()
            };
            opKind = OpsConst.TagFieldWrite;
        }

        byte[] encoded = Ops.EncodeOp(op, fullAuthor: false);
        byte[] envelope = new byte[16 + encoded.Length];
        mutation.RecordId.WriteTo(envelope);
        encoded.CopyTo(envelope, 16);
        Digest16 fingerprint = ChangeIdentity.Fingerprint(
            _namespaceId, mutation.RecordId, opKind, op, _node.NodeId, Array.Empty<BatchId>());
        return new MetadataFrozenOperation
        {
            ChangeId = change.ChangeId,
            Fingerprint = fingerprint.ToArray(),
            Payload = envelope,
            OriginAuthorEpoch = mutation.OriginAuthorEpoch,
            OriginSequence = 0,
            HlcWall = mutation.HlcWall,
            HlcLogical = mutation.HlcLogical
        };
    }

    private static void AddCoalescedPendingLocalChange(
        Dictionary<(RecordId RecordId, ushort FieldId, bool IsRecordTombstone), ProviderChange> winners,
        ProviderChange change)
    {
        ProviderMutation mutation = change.Mutation;
        var key = (mutation.RecordId, mutation.FieldId, mutation.IsRecordTombstone);
        if (!winners.TryGetValue(key, out var prior))
        {
            winners.Add(key, change);
            return;
        }

        ProviderMutation previous = prior.Mutation;
        int comparison = HlcOps.CompareHlc(
            new Hlc(mutation.HlcWall, mutation.HlcLogical), mutation.AuthorNodeId,
            new Hlc(previous.HlcWall, previous.HlcLogical), previous.AuthorNodeId);
        if (comparison == 0)
        {
            if (!SameMutation(mutation, previous))
                throw new InvalidDataException("Pending local feed contains conflicting mutations at the same HLC and author.");
            return;
        }
        if (comparison > 0)
            winners[key] = change;
    }

    /// <summary>Compares complete local mutations when equal HLC/author keys are encountered.</summary>
    private static bool SameMutation(ProviderMutation left, ProviderMutation right)
        => left.RecordId == right.RecordId
            && left.FieldId == right.FieldId
            && left.IsTombstone == right.IsTombstone
            && left.IsRecordTombstone == right.IsRecordTombstone
            && left.HlcWall == right.HlcWall
            && left.HlcLogical == right.HlcLogical
            && left.AuthorNodeId == right.AuthorNodeId
            && left.KeyEpoch == right.KeyEpoch
            && left.Value.AsSpan().SequenceEqual(right.Value);

    /// <summary>
    /// Freezes newly appended local Node Outbox entries into the attached durable
    /// Metadata Store. Node sequence numbers are preserved exactly; a gap between the
    /// metadata high-watermark and the first retained Node entry fails closed.
    /// </summary>
    private void FreezeLocalOutboxToMetadata()
    {
        if (_metadataStore is null)
            return;

        ulong high = _metadataStore.GetOutboundHighWatermark(
            _scopeId!, _node.NodeId, _namespaceId, _node.NodeId);
        if (high > long.MaxValue)
            throw new InvalidDataException("Metadata outbound high-watermark exceeds Node Outbox sequence range.");

        long nodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
        if (high > (ulong)nodeHigh)
            throw new InvalidDataException("Metadata outbound high-watermark is ahead of the restored Node Outbox.");
        long after = (long)high;
        if (after == nodeHigh)
            return;

        const int pageSize = 1024;
        while (true)
        {
            var entries = _node.ReadOutboxEntries(_namespaceId, _node.NodeId, after, pageSize);
            if (entries.Count == 0)
                throw new InvalidDataException("Node Outbox entries required by the Metadata Store high-watermark have been compacted or lost.");

            var frozen = new List<MetadataFrozenOperation>(entries.Count);
            long expected = checked(after + 1);
            foreach (var entry in entries)
            {
                if (entry.Seq != expected)
                    throw new InvalidDataException("Node Outbox sequence gap prevents safe Metadata Store freeze.");

                object op = Ops.DecodeOp(entry.Payload).Op;
                Digest16 fingerprint = ChangeIdentity.Fingerprint(
                    _namespaceId, entry.RecordId, entry.OpKind, op,
                    entry.AuthorNodeId, entry.CausalDeps);

                byte[] envelope = new byte[16 + entry.Payload.Length];
                entry.RecordId.WriteTo(envelope);
                entry.Payload.Span.CopyTo(envelope.AsSpan(16));
                frozen.Add(new MetadataFrozenOperation
                {
                    ChangeId = entry.ChangeId,
                    Fingerprint = fingerprint.ToArray(),
                    Payload = envelope,
                    OriginAuthorEpoch = entry.OriginAuthorEpoch,
                    OriginSequence = entry.OriginSequence
                });
                expected = checked(expected + 1);
            }

            var published = _metadataStore.FreezeOutbound(
                _scopeId!, _node.NodeId, _namespaceId, _node.NodeId, frozen);
            if (published.Count != entries.Count)
                throw new InvalidDataException("Metadata Store freeze returned an unexpected operation count.");
            for (int i = 0; i < published.Count; i++)
                if (published[i].Sequence != checked((ulong)entries[i].Seq) ||
                    published[i].ChangeId != entries[i].ChangeId)
                    throw new InvalidDataException("Metadata Store sequence diverged from the Node Outbox.");

            after = entries[^1].Seq;
            if (after == nodeHigh)
                return;
            if (entries.Count < pageSize)
                throw new InvalidDataException("Node Outbox page ended before its allocated high-watermark.");
        }
    }

    /// <summary>
    /// Materializes the Node's current LWW field states into the application provider.
    /// Remote materialization never enters the provider's local change feed.
    /// The deterministic apply token makes an interrupted retry idempotent.
    /// </summary>
    public ProviderApplyReceipt? ApplyRemoteFields(IEnumerable<(RecordId RecordId, ushort FieldId)> fields)
        => ApplyRemoteFieldsCore(fields, applyToken: null);

    /// <summary>
    /// Materializes exact immutable FieldWrite, FieldTomb and RecordTombstone operations
    /// carried by an accepted BIN-SYNC batch. The apply token is derived from the
    /// immutable batch identity, not the receiver's current winners, so replay remains
    /// idempotent even if later batches have advanced one or more of the same fields.
    /// </summary>
    public ProviderApplyReceipt? ApplyRemoteBatch(Batch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.NamespaceId != _namespaceId)
            throw new ArgumentException("Batch namespace does not match this coordinator.", nameof(batch));

        var mutations = new List<ProviderMutation>(batch.Entries.Count);
        foreach (var entry in batch.Entries)
        {
            if (entry.NamespaceId != _namespaceId || entry.Payload.IsEmpty)
                throw new InvalidDataException("Batch entry has an invalid namespace or empty operation payload.");

            object op = Ops.DecodeOp(entry.Payload).Op;
            switch (op)
            {
                case OpFieldWrite write:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = entry.RecordId,
                        FieldId = write.FieldId,
                        HlcWall = write.Wall,
                        HlcLogical = write.Logical,
                        AuthorNodeId = batch.DurableAuthor,
                        KeyEpoch = write.KeyEpoch,
                        Value = write.Value.ToArray(),
                        ChangeId = entry.ChangeId,
                        OriginAuthorEpoch = entry.OriginAuthorEpoch,
                        OriginSequence = entry.OriginSequence
                    });
                    break;
                case OpFieldTomb tomb:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = entry.RecordId,
                        FieldId = tomb.FieldId,
                        IsTombstone = true,
                        HlcWall = tomb.Wall,
                        HlcLogical = tomb.Logical,
                        AuthorNodeId = batch.DurableAuthor,
                        KeyEpoch = tomb.KeyEpoch,
                        Value = Array.Empty<byte>(),
                        ChangeId = entry.ChangeId,
                        OriginAuthorEpoch = entry.OriginAuthorEpoch,
                        OriginSequence = entry.OriginSequence
                    });
                    break;
                case OpRecordTombstone recordTombstone:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = recordTombstone.RecordId,
                        FieldId = Model.LifecycleFieldId,
                        IsRecordTombstone = true,
                        HlcWall = recordTombstone.Wall,
                        HlcLogical = recordTombstone.Logical,
                        AuthorNodeId = batch.DurableAuthor,
                        KeyEpoch = 0,
                        Value = Array.Empty<byte>(),
                        ChangeId = entry.ChangeId,
                        OriginAuthorEpoch = entry.OriginAuthorEpoch,
                        OriginSequence = entry.OriginSequence
                    });
                    break;
                default:
                    throw new InvalidDataException("Batch contains an unsupported operation type.");
            }
        }

        if (mutations.Count == 0) return null;
        // Preserve wire order in the fingerprint. Multiple immutable operations
        // for the same field can legitimately occur in one batch; Storage applies
        // their LWW order while retaining the complete batch receipt. Do not sort or
        // reconstruct payload from current Node winners.
        Guid token = CreateBatchApplyToken(batch);
        lock (_gate)
        {
            if (_metadataStore is null)
                return _provider.ApplyRemoteBatch(token, mutations);

            MetadataIncomingBatch metadataBatch = CreateMetadataBatch(batch);
            bool finalizeMetadata = true;
            try
            {
                _metadataStore.BeginPendingApply(metadataBatch, token);
            }
            catch (InvalidOperationException)
            {
                // A finalized token is resolved through the durable receive receipt.
                // If it is not an exact duplicate, fail closed.
                var prior = _metadataStore.AcceptIncomingBatch(metadataBatch);
                if (!prior.IsDuplicate || (!prior.DurableAuthor.IsZero && prior.DurableAuthor != batch.DurableAuthor))
                    throw new InvalidDataException("Completed metadata receipt does not match the replayed batch author.");
                finalizeMetadata = false;
            }

            var providerReceipt = _provider.ApplyRemoteBatch(token, mutations);
            if (finalizeMetadata)
            {
                _metadataStore.FinalizePendingApply(_scopeId!, _node.NodeId,
                    new MetadataProviderApplyReceipt(token, metadataBatch.BatchDigest.ToArray()));
            }
            return providerReceipt;
        }
    }

    /// <summary>
    /// Registers a session peer as interested in this namespace. Existing Node-only
    /// progress is migrated first; an unacknowledged legacy send without a durable
    /// reservation fails closed instead of being silently skipped.
    /// </summary>
    public void RegisterInterestedPeer(SyncSession session, int namespaceId)
    {
        ArgumentNullException.ThrowIfNull(session);
        if (_metadataStore is not IMetadataPeerStateStore peerStore)
            throw new InvalidOperationException("The configured Metadata Store does not support durable peer state.");
        if (string.IsNullOrWhiteSpace(_scopeId))
            throw new InvalidOperationException("A stable Metadata Store scope is required for peer registration.");
        if ((uint)namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));

        lock (_gate)
        {
            NodeId peer = session.PeerNodeId;
            uint peerEpoch = checked((uint)session.PeerEpoch);
            var priorPeerEpochs = peerStore.ReadInterestedPeers(_scopeId!, _node.NodeId, namespaceId)
                .Where(item => item.Peer == peer && item.PeerEpoch < peerEpoch)
                .Select(item => item.PeerEpoch).Distinct().ToArray();
            bool hasPriorIncarnation = priorPeerEpochs.Length != 0 ||
                peerStore.GetRetiredPeerEpoch(_scopeId!, _node.NodeId, peer, namespaceId) is not null;
            // Register the replacement first, but keep prior epochs active until
            // AutoReconcileWithMetadataAsync commits this epoch's complete baseline.
            // Retiring earlier would reclaim receipts before the replacement can
            // prove its initial state and would fence a still-needed retry path.
            peerStore.RegisterInterestedPeer(_scopeId!, _node.NodeId, peer, peerEpoch, namespaceId);

            var known = peerStore.ReadPeerStreamStates(_scopeId!, _node.NodeId, peer, peerEpoch, namespaceId)
                .ToDictionary(stream => stream.AuthorStream, stream => stream.State);
            var authors = peerStore.ReadOutboundAuthors(_scopeId!, _node.NodeId, namespaceId)
                .Concat(peerStore.ReadRelayAuthors(_scopeId!, _node.NodeId, namespaceId))
                .Distinct()
                .ToArray();
            foreach (NodeId author in authors)
            {
                if (author == peer) continue;
                var saved = known.TryGetValue(author, out var existing)
                    ? existing
                    : peerStore.GetPeerStreamState(_scopeId!, _node.NodeId, peer, peerEpoch, namespaceId, author);
                bool pristine = saved.AckedCursor == 0 && saved.PushedCursor == 0 &&
                    saved.StreamOffset == 0 && saved.IssuerEpoch == 0 &&
                    saved.PendingBatch is null && saved.LastBatchDigest.All(value => value == 0);
                if (!pristine || hasPriorIncarnation) continue;
                var legacy = _node.ReadPeerStreamProgress(peer, namespaceId, author);
                if (legacy.PushedCursor > legacy.AckedCursor)
                    throw new InvalidDataException("Legacy Node peer state has an unacknowledged batch without a durable replay reservation.");
                peerStore.RegisterInterestedPeerStream(_scopeId!, _node.NodeId, peer,
                    peerEpoch, namespaceId, author, legacy.AckedCursor,
                    legacy.StreamOffset, legacy.IssuerEpoch, legacy.LastBatchDigest);
            }
        }
    }

    /// <summary>
    /// Rebuilds the Node's namespace data cache from the authoritative provider snapshot,
    /// then replays any durable PendingApply intents before the caller starts or resumes
    /// transport work. The caller must use this only when all local mutations for this
    /// namespace are captured by the provider feed; Node-originated writes that bypass
    /// the provider are intentionally not part of the restored data snapshot.
    /// </summary>
    /// <returns>The number of PendingApply intents recovered after installing the snapshot.</returns>
    public int RehydrateNodeFromProviderSnapshot(Action<Node> persistNode)
    {
        ArgumentNullException.ThrowIfNull(persistNode);
        using IDisposable commitGuard = AcquireApplicationCommitGuard();
        if (_metadataStore is null)
            throw new InvalidOperationException("Provider-authoritative rehydration requires a durable Metadata Store.");
        if (string.IsNullOrWhiteSpace(_scopeId))
            throw new InvalidOperationException("Provider-authoritative rehydration requires a stable Metadata Store scope.");

        lock (_gate)
        {
            // Drain pending provider-feed changes through the incremental cache path;
            // the explicit full snapshot below remains the startup reconciliation.
            PublishLocalChangesCore(persistNode, 1024);
            return InstallProviderSnapshot(persistNode);
        }
    }

    private int InstallProviderSnapshot(Action<Node> persistNode)
    {
        // One Metadata Store scope may contain multiple namespaces. This coordinator
        // owns only its namespace; leave other coordinators' intents untouched.
        var intents = _metadataStore!.ReadPendingApplyIntents(_scopeId!, _node.NodeId)
            .Where(intent => intent.Batch.NamespaceId == _namespaceId)
            .ToArray();
        if (intents.Any(intent => intent.Batch.WirePayload.Length == 0))
            throw new InvalidOperationException(
                "Provider-authoritative rehydration requires replayable PendingApply wire payloads; recover legacy intents before replacing the Node cache.");

        BatchId[] pendingRemoteChangeIds = intents
            .SelectMany(intent => intent.Batch.Operations)
            .Select(operation => operation.ChangeId)
            .Where(changeId => !changeId.IsZero)
            .Distinct()
            .ToArray();

        ProviderSnapshotHandle snapshot = _provider.OpenSnapshot();
        try
        {
            var mutations = new List<ProviderMutation>();
            int afterIndex = 0;
            while (true)
            {
                ProviderSnapshotPage page = _provider.ReadSnapshot(snapshot.Token, afterIndex, 512);
                if (page.Entries.Count > 512)
                    throw new InvalidDataException("Provider snapshot exceeded the requested page size.");
                mutations.AddRange(page.Entries);

                if (page.IsComplete) break;
                if (page.NextIndex <= afterIndex)
                    throw new InvalidDataException("Provider snapshot page did not advance.");
                afterIndex = page.NextIndex;
            }

            if (_provider is IProviderPublicationMode publicationMode &&
                publicationMode.TryGetMetadataStoreOwnedPublicationCutover(out ulong cutoverNodeHigh))
            {
                long currentNodeHigh = _node.GetOutboxHighWatermark(_namespaceId, _node.NodeId);
                if (currentNodeHigh < 0 || (ulong)currentNodeHigh != cutoverNodeHigh)
                    throw new InvalidOperationException("Node Outbox advanced after Metadata-Store-owned publication was enabled.");

                // A prior-version cutover marker may be recovered with a Node snapshot
                // that predates the in-memory fence. Re-establish it before migrating
                // and clearing the complete legacy ledger; RestoreProviderSnapshot
                // below removes pending remote IDs and must not erase their old digests
                // before the Metadata aliases have been persisted.
                _node.EnableMetadataStoreOwnedPublicationFence(_namespaceId);
                // The provider snapshot is authoritative after cutover. No Node
                // Outbox payload (local or legacy relay) may be revived from a stale
                // checkpoint; Metadata Store owns every retained delivery stream.
                DiscardNodeOutboxPayloadsAfterProviderCutover();
            }

            _node.RestoreProviderSnapshot(_namespaceId, mutations, pendingRemoteChangeIds);
            // Persist the clean provider-derived base before replaying pending intents.
            // If recovery is interrupted, the same durable intents can safely replay.
            persistNode(_node);
        }
        finally
        {
            _provider.ReleaseSnapshot(snapshot.Token);
        }

        return RecoverPendingApplies(persistNode);
    }

    /// <summary>
    /// Runs bidirectional anti-entropy and commits the matching durable peer snapshot/feed
    /// baseline. The feed watermarks are captured before reconciliation: changes after
    /// that barrier remain eligible for DELTA_BATCH, even if a later snapshot happens
    /// to include them. Relay streams always retain offset zero because snapshots do not
    /// replace relay/event history.
    /// </summary>
    public async Task<(int Prefixes, int FieldsApplied)> AutoReconcileWithMetadataAsync(
        SyncSession session, Action<Node> persistNode, bool initiate,
        int maxPrefixes = 16, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(persistNode);
        if (_metadataStore is not IMetadataPeerStateStore peerStore)
            throw new InvalidOperationException("The configured Metadata Store does not support durable peer state.");
        if (string.IsNullOrWhiteSpace(_scopeId))
            throw new InvalidOperationException("A stable Metadata Store scope is required for initial-sync baseline.");

        bool providerAuthoritative = _provider is IProviderPublicationMode publicationMode &&
            publicationMode.TryGetMetadataStoreOwnedPublicationCutover(out _);
        if (providerAuthoritative)
        {
            // The durable cutover marker proves that all local namespace mutations
            // are captured by the provider feed. Rebuild rather than merge so stale
            // Node checkpoint rows cannot survive provider-authoritative recovery.
            RehydrateNodeFromProviderSnapshot(persistNode);
        }
        else
        {
            ProviderSnapshotHandle providerSnapshot = _provider.OpenSnapshot();
            try
            {
                RecoverPendingApplies(persistNode);
                PublishLocalChanges(persistNode);
                MergeProviderSnapshotIntoNode(providerSnapshot, persistNode);
            }
            finally
            {
                _provider.ReleaseSnapshot(providerSnapshot.Token);
            }
        }

        RegisterInterestedPeer(session, _namespaceId);
        persistNode(_node);

        NodeId peer = session.PeerNodeId;
        uint peerEpoch = checked((uint)session.PeerEpoch);
        var relayAuthors = peerStore.ReadRelayAuthors(_scopeId!, _node.NodeId, _namespaceId).ToHashSet();
        var authors = peerStore.ReadOutboundAuthors(_scopeId!, _node.NodeId, _namespaceId)
            .Concat(relayAuthors)
            .Where(author => author != peer)
            .Distinct()
            .ToArray();
        var baselines = new List<MetadataPeerStreamBaseline>(authors.Length);
        foreach (NodeId author in authors)
        {
            ulong offset = relayAuthors.Contains(author)
                ? 0
                : _metadataStore.GetOutboundHighWatermark(_scopeId!, _node.NodeId, _namespaceId, author);
            uint issuerEpoch = _node.ReadPeerStreamProgress(peer, _namespaceId, author).IssuerEpoch;
            baselines.Add(new MetadataPeerStreamBaseline(author, offset, issuerEpoch));
        }

        var result = await session.AutoReconcileAsync(
            _node, _namespaceId, initiate, maxPrefixes, ct,
            afterSnapshotApplied: (chunks, _) =>
            {
                persistNode(_node);
                ApplyRemoteSnapshotToProvider(session.PeerNodeId, peerEpoch, chunks);
            }).ConfigureAwait(false);
        if (!_node.IsInitialSyncComplete(peer, _namespaceId))
            throw new SyncException("SNAPSHOT_REQUIRED");
        persistNode(_node);
        peerStore.CompletePeerInitialSync(
            _scopeId!, _node.NodeId, peer, peerEpoch, _namespaceId, baselines);

        // Receipt reclamation is permitted only after the replacement epoch's
        // complete baseline is durable. Until this point the prior incarnation
        // remains registered and can finish any in-flight PendingApply.
        foreach (uint oldEpoch in peerStore.ReadInterestedPeers(_scopeId!, _node.NodeId, _namespaceId)
                     .Where(item => item.Peer == peer && item.PeerEpoch < peerEpoch)
                     .Select(item => item.PeerEpoch).Distinct())
            peerStore.RetireInterestedPeer(
                _scopeId!, _node.NodeId, peer, oldEpoch, peerEpoch, _namespaceId);

        return result;
    }

    private void MergeProviderSnapshotIntoNode(
        ProviderSnapshotHandle snapshot, Action<Node> persistNode)
    {
        var recordsById = new Dictionary<RecordId, SnapshotRecord>();
        int afterIndex = 0;
        while (true)
        {
            ProviderSnapshotPage page = _provider.ReadSnapshot(snapshot.Token, afterIndex, 512);
            foreach (ProviderMutation mutation in page.Entries)
            {
                if (!recordsById.TryGetValue(mutation.RecordId, out var record))
                {
                    record = new SnapshotRecord { RecordId = mutation.RecordId };
                    recordsById.Add(mutation.RecordId, record);
                }

                if (mutation.IsRecordTombstone)
                {
                    record.HasLifecycle = true;
                    record.LifecycleValue = Model.Dead;
                    record.LifecycleWall = mutation.HlcWall;
                    record.LifecycleLogical = mutation.HlcLogical;
                    record.LifecycleAuthor = mutation.AuthorNodeId;
                }
                else
                {
                    record.Fields.Add(new SnapshotField
                    {
                        FieldId = mutation.FieldId,
                        IsTombstone = mutation.IsTombstone,
                        Wall = mutation.HlcWall,
                        Logical = mutation.HlcLogical,
                        AuthorNodeId = mutation.AuthorNodeId,
                        KeyEpoch = mutation.KeyEpoch,
                        Value = mutation.Value.ToArray()
                    });
                }
            }

            if (page.IsComplete) break;
            if (page.NextIndex <= afterIndex)
                throw new InvalidDataException("Provider snapshot page did not advance.");
            afterIndex = page.NextIndex;
        }

        if (recordsById.Count == 0) return;
        var records = recordsById.Values.ToList();
        foreach (var record in records)
            record.Fields.Sort(static (left, right) => left.FieldId.CompareTo(right.FieldId));

        var partitions = new List<List<SnapshotRecord>>();
        var current = new List<SnapshotRecord>();
        void AddPartition(List<SnapshotRecord> candidate)
        {
            var probe = new SnapshotChunkMessage
            {
                NamespaceId = _namespaceId,
                ChunkIndex = 0,
                ChunkCount = 1,
                Records = candidate
            };
            if (Reconcile.EncodeSnapshotChunk(probe).Length <= Frames.MAX_PAYLOAD_SIZE)
            {
                partitions.Add(candidate);
                if (partitions.Count > 4096) throw new SyncException("PAYLOAD_TOO_LARGE");
                return;
            }
            if (candidate.Count <= 1) throw new SyncException("PAYLOAD_TOO_LARGE");
            int middle = candidate.Count / 2;
            AddPartition(candidate.GetRange(0, middle));
            AddPartition(candidate.GetRange(middle, candidate.Count - middle));
        }

        foreach (var record in records)
        {
            current.Add(record);
            if (current.Count == 64)
            {
                AddPartition(current);
                current = new List<SnapshotRecord>();
            }
        }
        if (current.Count > 0) AddPartition(current);

        uint chunkCount = checked((uint)partitions.Count);
        var chunks = new List<SnapshotChunkMessage>(partitions.Count);
        for (int i = 0; i < partitions.Count; i++)
        {
            chunks.Add(new SnapshotChunkMessage
            {
                NamespaceId = _namespaceId,
                ChunkIndex = checked((uint)i),
                ChunkCount = chunkCount,
                Records = partitions[i]
            });
        }

        Reconcile.ApplySnapshotChunks(_node, chunks);
        persistNode(_node);
    }

    private void ApplyRemoteSnapshotToProvider(
        NodeId senderPeer, uint senderEpoch, IReadOnlyList<SnapshotChunkMessage> chunks)
    {
        var mutations = new List<ProviderMutation>();
        foreach (var chunk in chunks)
        {
            foreach (var record in chunk.Records)
            {
                if (record.HasLifecycle && record.LifecycleValue == Model.Dead)
                {
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = record.RecordId,
                        FieldId = Model.LifecycleFieldId,
                        IsRecordTombstone = true,
                        HlcWall = record.LifecycleWall,
                        HlcLogical = record.LifecycleLogical,
                        AuthorNodeId = record.LifecycleAuthor,
                        KeyEpoch = 0,
                        Value = Array.Empty<byte>()
                    });
                }

                foreach (var field in record.Fields)
                {
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = record.RecordId,
                        FieldId = field.FieldId,
                        IsTombstone = field.IsTombstone,
                        HlcWall = field.Wall,
                        HlcLogical = field.Logical,
                        AuthorNodeId = field.AuthorNodeId,
                        KeyEpoch = field.KeyEpoch,
                        Value = field.Value.ToArray()
                    });
                }
            }
        }
        if (mutations.Count == 0) return;

        using var hash = System.Security.Cryptography.IncrementalHash.CreateHash(
            System.Security.Cryptography.HashAlgorithmName.SHA256);
        hash.AppendData(System.Text.Encoding.UTF8.GetBytes(_scopeId!));
        hash.AppendData(senderPeer.ToArray());
        Span<byte> fixedPart = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(fixedPart, senderEpoch);
        BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(4), checked((uint)_namespaceId));
        hash.AppendData(fixedPart);
        Span<byte> length = stackalloc byte[4];
        foreach (var chunk in chunks)
        {
            byte[] encoded = Reconcile.EncodeSnapshotChunk(chunk);
            BinaryPrimitives.WriteInt32LittleEndian(length, encoded.Length);
            hash.AppendData(length);
            hash.AppendData(encoded);
        }
        byte[] digest = hash.GetHashAndReset();
        Guid applyToken = new(digest.AsSpan(0, 16));
        if (applyToken == Guid.Empty)
        {
            digest[0] = 1;
            applyToken = new Guid(digest.AsSpan(0, 16));
        }
        _provider.ApplyRemoteBatch(applyToken, mutations);
    }

    /// <summary>
    /// Receive through SyncSession with a durable PendingApply intent committed before
    /// Node mutation. The ACK is emitted only after Node checkpoint, provider apply and
    /// metadata receipt finalization all succeed.
    /// </summary>
    public async Task<BatchResult> ReceiveBatchBeforeAckAsync(
        SyncSession session, Action<Node> persistNode, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(persistNode);
        RegisterInterestedPeer(session, _namespaceId);
        StorageRemoteApplyPlan? plan = null;
        BatchResult result = await session.ReceiveBatchAsync(_node,
            beforeApply: null,
            beforeApplyWithOperationDecisions: (batch, payload, senderPeer) =>
            {
                plan = BeginRemoteBatchBeforeNodeApply(batch, payload.ToArray(), senderPeer, checked((uint)session.PeerEpoch));
                return plan.ApplyOperations;
            },
            beforeAck: (batch, _) =>
            {
                var accepted = plan ?? throw new InvalidOperationException("Receive intent was not durably staged.");
                ApplyRemoteBatchAfterNodeApply(batch, accepted, persistNode);
            },
            ct: ct,
            receiveStateOwnedByMetadataStore: true).ConfigureAwait(false);
        if (plan?.IsDuplicate == true)
            result.Duplicate = true;
        return result;
    }

    /// <summary>
    /// Persist the exact incoming batch as a recoverable intent before mutating Node.
    /// If the process stops at any point after this call, recovery can replay the same
    /// wire payload into Node and the provider before the sender is acknowledged.
    /// </summary>
    public StorageRemoteApplyPlan BeginRemoteBatchBeforeNodeApply(Batch batch, byte[] wirePayload,
        NodeId senderPeer, uint transportPeerEpoch = 0)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(wirePayload);
        if (wirePayload.Length == 0 || wirePayload.Length > 16 * 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(wirePayload));
        if (_metadataStore is null)
            throw new InvalidOperationException("A Metadata Store is required for durable receive staging.");
        if (batch.NamespaceId != _namespaceId)
            throw new ArgumentException("Batch namespace does not match this coordinator.", nameof(batch));

        lock (_gate)
        {
            MetadataIncomingBatch metadataBatch = CreateMetadataBatch(batch, wirePayload, senderPeer, transportPeerEpoch);
            Guid token = CreateBatchApplyToken(batch);
            bool finalizeMetadata = true;
            bool isDuplicate;
            IReadOnlyList<bool> applyOperations;
            try
            {
                var intent = _metadataStore.BeginPendingApply(metadataBatch, token);
                isDuplicate = intent.IsDuplicate;
                applyOperations = intent.ApplyOperations;
                if (applyOperations.Count != metadataBatch.Operations.Count)
                    throw new InvalidDataException("Metadata Store returned an incomplete operation decision mask.");
            }
            catch (MetadataPendingApplyAlreadyFinalizedException)
            {
                var prior = _metadataStore.AcceptIncomingBatch(metadataBatch);
                if (!prior.IsDuplicate)
                    throw;
                finalizeMetadata = false;
                isDuplicate = true;
                applyOperations = Array.AsReadOnly(new bool[metadataBatch.Operations.Count]);
            }
            return new StorageRemoteApplyPlan(token, metadataBatch, finalizeMetadata, isDuplicate, applyOperations);
        }
    }

    /// <summary>
    /// Complete a pre-staged receive after Node has applied the decoded batch:
    /// persist Node, apply the exact immutable provider mutations, then finalize receipt.
    /// </summary>
    public ProviderApplyReceipt ApplyRemoteBatchAfterNodeApply(
        Batch batch, StorageRemoteApplyPlan plan, Action<Node> persistNode)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(persistNode);
        if (_metadataStore is null)
            throw new InvalidOperationException("A Metadata Store is required for durable receive staging.");
        if (batch.NamespaceId != _namespaceId || plan.ApplyToken != CreateBatchApplyToken(batch) ||
            plan.Batch.BatchId != batch.BatchId ||
            !plan.Batch.BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest.ToArray()))
            throw new InvalidDataException("Pre-staged receive intent does not match the applied Node batch.");

        lock (_gate)
        {
            persistNode(_node);
            var mutations = CreateProviderMutations(plan.Batch.Operations);
            if (mutations.Count == 0)
                throw new InvalidDataException("Pre-staged receive intent contains no provider mutations.");
            var receipt = ApplyProviderRemoteBatch(batch, plan.Batch, plan.ApplyToken, mutations);
            if (plan.FinalizeMetadata)
            {
                var metadataReceipt = _metadataStore.FinalizePendingApply(_scopeId!, _node.NodeId,
                    new MetadataProviderApplyReceipt(plan.ApplyToken, plan.Batch.BatchDigest.ToArray()));
                if (metadataReceipt.DurableAuthor != batch.DurableAuthor)
                    throw new InvalidDataException("Finalized metadata receipt author does not match the applied Node batch.");
                byte[] expectedHeaderFingerprint = InboxReceiptIdentity.Compute(batch).ToArray();
                if (!metadataReceipt.HeaderFingerprint.AsSpan().SequenceEqual(expectedHeaderFingerprint))
                    throw new InvalidDataException("Finalized metadata receipt header fingerprint does not match the applied Node batch.");
            }
            return receipt;
        }
    }

    /// <summary>
    /// Complete the receiver-side durability sequence for a pre-ACK callback:
    /// checkpoint Node, commit provider state and finalize Metadata Store PendingApply.
    /// The caller may ACK the sender only after this method returns successfully.
    /// </summary>
    public ProviderApplyReceipt? ApplyRemoteBatchBeforeAck(
        Batch batch, Action<Node> persistNode)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(persistNode);
        if (batch.NamespaceId != _namespaceId)
            throw new ArgumentException("Batch namespace does not match this coordinator.", nameof(batch));
        lock (_gate)
        {
            persistNode(_node);
            return ApplyRemoteBatch(batch);
        }
    }

    /// <summary>Recover pending provider commits after restart before accepting new sync work.</summary>
    public int RecoverPendingApplies(Action<Node>? persistNode = null)
    {
        if (_metadataStore is null)
            throw new InvalidOperationException("No Metadata Store is attached to this coordinator.");

        lock (_gate)
        {
            var intents = _metadataStore.ReadPendingApplyIntents(_scopeId!, _node.NodeId);
            int recovered = 0;
            foreach (var intent in intents)
            {
                if (intent.Batch.NamespaceId != _namespaceId)
                    continue;

                Batch? recoveredBatch = null;
                if (intent.Batch.WirePayload.Length > 0)
                {
                    if (persistNode is null)
                        throw new InvalidOperationException("Recovering a pre-Node PendingApply intent requires a Node checkpoint callback.");
                    if (intent.Batch.ReceiveStateOwnedByMetadataStore)
                        recoveredBatch = _node.ApplyBatchPayloadWithMetadataAuthority(
                            intent.Batch.WirePayload, intent.Batch.SenderPeer, intent.ApplyOperations).Batch;
                    else
                        _node.ApplyBatchPayload(intent.Batch.WirePayload, intent.Batch.SenderPeer);
                    persistNode(_node);
                }

                var mutations = CreateProviderMutations(intent.Batch.Operations);
                if (mutations.Count == 0)
                    throw new InvalidDataException("Pending apply intent contains no provider mutations.");

                if (recoveredBatch is not null)
                    _ = ApplyProviderRemoteBatch(
                        recoveredBatch, intent.Batch, intent.ApplyToken, mutations);
                else
                    _provider.ApplyRemoteBatch(intent.ApplyToken, mutations);
                var metadataReceipt = _metadataStore.FinalizePendingApply(_scopeId!, _node.NodeId,
                    new MetadataProviderApplyReceipt(intent.ApplyToken, intent.Batch.BatchDigest.ToArray()));
                NodeId expectedAuthor = !intent.Batch.AuthorStreamNodeId.IsZero
                    ? intent.Batch.AuthorStreamNodeId
                    : intent.Batch.Operations.Count == 0 ? default : intent.Batch.Operations[0].AuthorNodeId;
                if (!metadataReceipt.DurableAuthor.IsZero && metadataReceipt.DurableAuthor != expectedAuthor)
                    throw new InvalidDataException("Recovered metadata receipt author does not match its PendingApply intent.");
                if (intent.Batch.ReceiveStateOwnedByMetadataStore &&
                    !metadataReceipt.HeaderFingerprint.AsSpan().SequenceEqual(intent.Batch.HeaderFingerprint))
                    throw new InvalidDataException("Recovered metadata receipt header fingerprint does not match its PendingApply intent.");
                recovered++;
            }

            return recovered;
        }
    }

    private ProviderApplyReceipt ApplyProviderRemoteBatch(
        Batch wireBatch,
        MetadataIncomingBatch pending,
        Guid applyToken,
        IReadOnlyList<ProviderMutation> mutations)
    {
        ArgumentNullException.ThrowIfNull(wireBatch);
        ArgumentNullException.ThrowIfNull(pending);
        IProviderRemoteBatchCommitStore? contextualProvider =
            _remoteBatchCommitStore ?? _provider as IProviderRemoteBatchCommitStore;
        if (contextualProvider is null)
            return _provider.ApplyRemoteBatch(applyToken, mutations);

        if (!pending.ReceiveStateOwnedByMetadataStore ||
            pending.WirePayload.Length == 0 ||
            wireBatch.NamespaceId != pending.NamespaceId ||
            wireBatch.BatchId != pending.BatchId ||
            checked((ulong)wireBatch.CursorStart) != pending.CursorStart ||
            checked((ulong)wireBatch.CursorEnd) != pending.CursorEnd ||
            wireBatch.IssuerNodeId.IsZero ||
            wireBatch.IssuerEpoch <= 0 ||
            checked((uint)wireBatch.IssuerEpoch) != pending.SenderEpoch ||
            !wireBatch.PrevBatchDigest.ToArray().AsSpan().SequenceEqual(pending.PreviousBatchDigest) ||
            !wireBatch.BatchDigest.ToArray().AsSpan().SequenceEqual(pending.BatchDigest) ||
            pending.HeaderFingerprint.Length != 16 ||
            !InboxReceiptIdentity.Compute(wireBatch).ToArray().AsSpan()
                .SequenceEqual(pending.HeaderFingerprint))
            throw new InvalidDataException(
                "Full provider remote-batch context does not match its durable PendingApply header.");

        var context = new ProviderRemoteBatchCommit(
            applyToken,
            pending.SenderPeer,
            pending.TransportPeerEpoch,
            pending.NamespaceId,
            wireBatch.IssuerNodeId,
            checked((uint)wireBatch.IssuerEpoch),
            wireBatch.BatchId,
            checked((ulong)wireBatch.CursorStart),
            checked((ulong)wireBatch.CursorEnd),
            pending.PreviousBatchDigest,
            pending.BatchDigest,
            wireBatch.RecordsRoot.ToArray(),
            ReadOnlySpan<byte>.Empty,
            mutations);
        return contextualProvider.ApplyRemoteBatch(context);
    }

    private MetadataIncomingBatch CreateMetadataBatch(Batch batch, byte[]? wirePayload = null,
        NodeId? senderPeer = null, uint transportPeerEpoch = 0)
    {
        var operations = new List<MetadataOperation>(batch.Entries.Count);
        foreach (var entry in batch.Entries)
        {
            object op = Ops.DecodeOp(entry.Payload).Op;
            Digest16 fingerprint = ChangeIdentity.Fingerprint(
                batch.NamespaceId, entry.RecordId, entry.OpKind, op,
                batch.DurableAuthor, entry.CausalDeps);

            // MetadataStore payloads are opaque relay records. Prefix the 16-byte
            // RecordId so relay/recovery can reconstruct the exact provider mutation.
            byte[] envelope = new byte[16 + entry.Payload.Length];
            entry.RecordId.WriteTo(envelope);
            entry.Payload.Span.CopyTo(envelope.AsSpan(16));
            operations.Add(new MetadataOperation
            {
                AuthorNodeId = batch.DurableAuthor,
                ChangeId = entry.ChangeId,
                Fingerprint = fingerprint.ToArray(),
                OriginAuthorEpoch = entry.OriginAuthorEpoch,
                OriginSequence = entry.OriginSequence,
                Payload = envelope,
                RelayEligible = true
            });
        }

        return new MetadataIncomingBatch
        {
            ScopeId = _scopeId!,
            LocalReplica = _node.NodeId,
            SenderPeer = senderPeer ?? batch.IssuerNodeId,
            SenderEpoch = checked((uint)batch.IssuerEpoch),
            TransportPeerEpoch = transportPeerEpoch,
            AuthorStreamNodeId = batch.DurableAuthor,
            NamespaceId = batch.NamespaceId,
            BatchId = batch.BatchId,
            CursorStart = checked((ulong)batch.CursorStart),
            CursorEnd = checked((ulong)batch.CursorEnd),
            PreviousBatchDigest = batch.PrevBatchDigest.ToArray(),
            BatchDigest = batch.BatchDigest.ToArray(),
            HeaderFingerprint = InboxReceiptIdentity.Compute(batch).ToArray(),
            WirePayload = wirePayload?.ToArray() ?? Array.Empty<byte>(),
            ReceiveStateOwnedByMetadataStore = true,
            Operations = operations
        };
    }

    private static List<ProviderMutation> CreateProviderMutations(
        IReadOnlyList<MetadataOperation> operations)
    {
        var mutations = new List<ProviderMutation>(operations.Count);
        foreach (var operation in operations)
        {
            if (operation.Payload.Length <= 16)
                throw new InvalidDataException("Pending metadata operation has no RecordId envelope.");

            RecordId recordId = RecordId.From(operation.Payload.AsSpan(0, 16));
            object op = Ops.DecodeOp(operation.Payload.AsMemory(16)).Op;
            switch (op)
            {
                case OpFieldWrite write:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = recordId,
                        FieldId = write.FieldId,
                        HlcWall = write.Wall,
                        HlcLogical = write.Logical,
                        AuthorNodeId = operation.AuthorNodeId,
                        KeyEpoch = write.KeyEpoch,
                        Value = write.Value.ToArray(),
                        ChangeId = operation.ChangeId,
                        OriginAuthorEpoch = operation.OriginAuthorEpoch,
                        OriginSequence = operation.OriginSequence
                    });
                    break;
                case OpFieldTomb tomb:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = recordId,
                        FieldId = tomb.FieldId,
                        IsTombstone = true,
                        HlcWall = tomb.Wall,
                        HlcLogical = tomb.Logical,
                        AuthorNodeId = operation.AuthorNodeId,
                        KeyEpoch = tomb.KeyEpoch,
                        Value = Array.Empty<byte>(),
                        ChangeId = operation.ChangeId,
                        OriginAuthorEpoch = operation.OriginAuthorEpoch,
                        OriginSequence = operation.OriginSequence
                    });
                    break;
                case OpRecordTombstone recordTombstone:
                    mutations.Add(new ProviderMutation
                    {
                        RecordId = recordTombstone.RecordId,
                        FieldId = Model.LifecycleFieldId,
                        IsRecordTombstone = true,
                        HlcWall = recordTombstone.Wall,
                        HlcLogical = recordTombstone.Logical,
                        AuthorNodeId = operation.AuthorNodeId,
                        KeyEpoch = 0,
                        Value = Array.Empty<byte>(),
                        ChangeId = operation.ChangeId,
                        OriginAuthorEpoch = operation.OriginAuthorEpoch,
                        OriginSequence = operation.OriginSequence
                    });
                    break;
                default:
                    throw new InvalidDataException("Pending metadata operation has an unsupported operation type.");
            }
        }
        return mutations;
    }

    private ProviderApplyReceipt? ApplyRemoteFieldsCore(
        IEnumerable<(RecordId RecordId, ushort FieldId)> fields, Guid? applyToken)
    {
        ArgumentNullException.ThrowIfNull(fields);
        lock (_gate)
        {
            var mutations = new List<ProviderMutation>();
            foreach (var item in fields.Distinct())
            {
                if (!_node.TryGetRecord(_namespaceId, item.RecordId, out var record) || record is null)
                    continue;

                bool tombstone;
                FieldState? field;
                if (record.State.TryGetValue(item.FieldId, out field))
                    tombstone = false;
                else if (record.FieldTombs.TryGetValue(item.FieldId, out field))
                    tombstone = true;
                else
                    continue;
                if (field is null) continue;

                // This compatibility bridge materializes Node state into the provider.
                // It must carry the immutable identity from the exact local/relay Outbox
                // operation that produced the visible FieldState; reconstructing a new
                // ChangeId or Origin from the value would break route-independent dedupe.
                OutboxEntry? sourceEntry = null;
                foreach (OutboxEntry candidate in _node.ReadOutboxEntries(
                    _namespaceId, field.AuthorNodeId, 0, int.MaxValue))
                {
                    if (candidate.RecordId != item.RecordId ||
                        candidate.AuthorNodeId != field.AuthorNodeId)
                        continue;

                    object sourceOperation = Ops.DecodeOp(candidate.Payload).Op;
                    bool matches = sourceOperation switch
                    {
                        OpFieldWrite write =>
                            !tombstone && write.FieldId == item.FieldId &&
                            write.Wall == field.Timestamp.Wall &&
                            write.Logical == field.Timestamp.Logical,
                        OpFieldTomb fieldTomb =>
                            tombstone && fieldTomb.FieldId == item.FieldId &&
                            fieldTomb.Wall == field.Timestamp.Wall &&
                            fieldTomb.Logical == field.Timestamp.Logical,
                        _ => false
                    };
                    if (matches)
                        sourceEntry = candidate;
                }

                if (sourceEntry is null || sourceEntry.ChangeId.IsZero ||
                    sourceEntry.OriginAuthorEpoch == 0 || sourceEntry.OriginSequence == 0)
                    throw new InvalidDataException(
                        "Node field materialization has no matching immutable Outbox ChangeId/Origin.");

                mutations.Add(new ProviderMutation
                {
                    RecordId = item.RecordId,
                    FieldId = item.FieldId,
                    IsTombstone = tombstone,
                    HlcWall = field.Timestamp.Wall,
                    HlcLogical = field.Timestamp.Logical,
                    AuthorNodeId = field.AuthorNodeId,
                    KeyEpoch = field.KeyEpoch,
                    Value = tombstone ? Array.Empty<byte>() : field.Value.ToArray(),
                    ChangeId = sourceEntry.ChangeId,
                    OriginAuthorEpoch = sourceEntry.OriginAuthorEpoch,
                    OriginSequence = sourceEntry.OriginSequence
                });
            }

            if (mutations.Count == 0) return null;
            mutations.Sort(static (a, b) =>
            {
                int c = CompareRecordIds(a.RecordId, b.RecordId);
                return c != 0 ? c : a.FieldId.CompareTo(b.FieldId);
            });

            Guid token = applyToken ?? CreateApplyToken(_namespaceId, mutations);
            return _provider.ApplyRemoteBatch(token, mutations);
        }
    }

    private static Guid CreateBatchApplyToken(Batch batch)
    {
        Span<byte> identity = stackalloc byte[34];
        BinaryPrimitives.WriteUInt16LittleEndian(identity, checked((ushort)batch.NamespaceId));
        batch.IssuerNodeId.WriteTo(identity.Slice(2, 16));
        batch.BatchId.WriteTo(identity.Slice(18, 16));
        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(identity, digest);
        return new Guid(digest.Slice(0, 16));
    }

    private static int CompareRecordIds(RecordId left, RecordId right)
    {
        ulong leftLo = left.Lo, rightLo = right.Lo;
        for (int i = 0; i < sizeof(ulong); i++)
        {
            int c = ((byte)(leftLo >> (i * 8))).CompareTo((byte)(rightLo >> (i * 8)));
            if (c != 0) return c;
        }

        ulong leftHi = left.Hi, rightHi = right.Hi;
        for (int i = 0; i < sizeof(ulong); i++)
        {
            int c = ((byte)(leftHi >> (i * 8))).CompareTo((byte)(rightHi >> (i * 8)));
            if (c != 0) return c;
        }

        return 0;
    }

    private static Guid CreateApplyToken(int namespaceId, IReadOnlyList<ProviderMutation> mutations)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> header = stackalloc byte[6];
        BinaryPrimitives.WriteUInt16LittleEndian(header, checked((ushort)namespaceId));
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(2), mutations.Count);
        hash.AppendData(header);

        Span<byte> fixedPart = stackalloc byte[53];
        foreach (var mutation in mutations)
        {
            mutation.RecordId.WriteTo(fixedPart);
            BinaryPrimitives.WriteUInt16LittleEndian(fixedPart.Slice(16), mutation.FieldId);
            fixedPart[18] = mutation.IsTombstone ? (byte)1 : (byte)0;
            BinaryPrimitives.WriteUInt64LittleEndian(fixedPart.Slice(19), mutation.HlcWall);
            BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(27), mutation.HlcLogical);
            mutation.AuthorNodeId.WriteTo(fixedPart.Slice(31));
            BinaryPrimitives.WriteUInt16LittleEndian(fixedPart.Slice(47), mutation.KeyEpoch);
            BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(49), checked((uint)mutation.Value.Length));
            hash.AppendData(fixedPart);
            hash.AppendData(mutation.Value);
        }

        byte[] digest = hash.GetHashAndReset();
        return new Guid(digest.AsSpan(0, 16));
    }
}
