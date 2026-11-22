using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using BinSync;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

public sealed record MetadataGcReservationDiagnostics(
    int PendingApplyCount,
    int OutstandingPeerReservationCount,
    int RelayPayloadCount,
    int OutboundPayloadCount)
{
    public bool HasOutstandingPeerReservation => OutstandingPeerReservationCount != 0;
}

/// <summary>
/// Durable BIN-SYNC Metadata Store backed by Fld1.Storage.
/// Each logical metadata entry is stored under its own key; transactions persist only diffs.
/// </summary>
public class StorageMetadataStore : IMetadataStore, IMetadataPeerStateStore, IMetadataOutboundChangeIdentityStore, IMetadataProviderReceiptStreamStore, IMetadataChangeIdentityStore, IMetadataLegacyNodeLedgerStore, IMetadataFullReplaceCommitGuard, IMetadataBaselineCommitStore, IMetadataBaselinePayloadCompactionStore
{
    private readonly object _gate;
    private readonly StorageMetadataWriterAdmission _writerAdmission;
    // Keep decoded Metadata partitions as a small LRU. The durable FLD rows are
    // authoritative; cold partitions are cheap to rehydrate compared with retaining
    // every scope/replica's payload-bearing state for the lifetime of this store.
    private const int MaxCachedPartitions = 4;
    private readonly Dictionary<PartitionKey, PartitionState> _partitions = new();
    private readonly LinkedList<PartitionKey> _partitionLru = new();
    private readonly Dictionary<PartitionKey, LinkedListNode<PartitionKey>> _partitionLruNodes = new();
    private readonly StorageDatabase _storage;
    private readonly string? _directory;
    private readonly bool _deleteOnDispose;
    private readonly bool _ownsStorage;
    private StorageNamespaceGcStore? _namespaceGcStore;
    private TrustStore? _namespaceGcTrustStore;

    // Optional test-only observer; null in production keeps this diagnostic path inert.
    internal Action<string, long>? TombstoneGcAllocationObserver { get; set; }
    // Optional test-only phase observer; null in production keeps this path inert.
    internal Action<string, long>? ChangeIdGcAllocationObserver { get; set; }
    // Test-only signal proving whether a cold partition load fell back to the legacy full scan.
    internal Action? PartitionColdLoadValidationObserver { get; set; }
    private bool _disposed;

    public StorageMetadataStore(string directory, ulong partitionId = 301, bool deleteOnDispose = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        _directory = directory;
        _deleteOnDispose = deleteOnDispose;
        _ownsStorage = true;
        _storage = StorageDatabase.Open(directory, partitionId);
        _gate = StorageDatabaseTransactionGate.For(_storage);
        _writerAdmission = StorageMetadataWriterAdmission.For(_storage);
    }

    /// <summary>Attach to a caller-owned shared Storage database.</summary>
    public StorageMetadataStore(StorageDatabase storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _gate = StorageDatabaseTransactionGate.For(_storage);
        _writerAdmission = StorageMetadataWriterAdmission.For(_storage);
    }

    /// <summary>
    /// Attach the signed namespace retention checkpoint guard to the exact local
    /// Metadata scope and replica used by this coordinator.
    /// </summary>
    public void AttachNamespaceGcGuard(
        StorageNamespaceGcStore gcStore, TrustStore trustStore,
        string scopeId, NodeId localReplica)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        ArgumentNullException.ThrowIfNull(gcStore);
        if (localReplica.IsZero || !gcStore.IsBoundTo(_storage, scopeId, localReplica))
            throw new ArgumentException(
                "Namespace GC state must be bound to this Metadata Store, scope and local replica.",
                nameof(gcStore));
        AttachNamespaceGcGuard(gcStore, trustStore);
    }

    /// <summary>
    /// Attach the signed namespace retention checkpoint guard. The GC store must share
    /// this exact StorageDatabase so replay checks and metadata commits serialize on
    /// the same transaction gate.
    /// </summary>
    public void AttachNamespaceGcGuard(StorageNamespaceGcStore gcStore, TrustStore trustStore)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentNullException.ThrowIfNull(gcStore);
        ArgumentNullException.ThrowIfNull(trustStore);
        if (!gcStore.IsBoundTo(_storage))
            throw new ArgumentException("Namespace GC state must share the Metadata Store StorageDatabase.", nameof(gcStore));
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_namespaceGcStore is not null &&
                (!ReferenceEquals(_namespaceGcStore, gcStore) ||
                 !ReferenceEquals(_namespaceGcTrustStore, trustStore)))
                throw new InvalidOperationException("Namespace GC replay guard cannot be replaced after attachment.");
            _namespaceGcStore = gcStore;
            _namespaceGcTrustStore = trustStore;
        }
    }

    /// <summary>
    /// Acquires the shared Metadata writer boundary for a durable host-owned
    /// transaction that must serialize with Metadata mutations and namespace-GC commits.
    /// Dispose the returned lease after the transaction completes.
    /// </summary>
    public ValueTask<IDisposable> AcquireWriterAdmissionAsync(CancellationToken cancellationToken = default)
        => _writerAdmission.EnterAsync(cancellationToken);

    /// <summary>
    /// Acquire the shared StorageDatabase transaction gate for a host-owned synchronous
    /// write batch. Call after writer admission and do not await while this lease is held.
    /// This serializes the batch with Node checkpoints and all other adapters using the
    /// same StorageDatabase transaction gate.
    /// </summary>
    public IDisposable AcquireStorageTransactionGate()
    {
        Monitor.Enter(_gate);
        try
        {
            ThrowIfDisposed();
            return new StorageTransactionGateLease(_gate);
        }
        catch
        {
            Monitor.Exit(_gate);
            throw;
        }
    }

    internal ValueTask<IDisposable> AcquireGcWriterAdmissionAsync(CancellationToken cancellationToken)
        => AcquireWriterAdmissionAsync(cancellationToken);

    /// <summary>Returns true only when this adapter is attached to the exact StorageDatabase instance.</summary>
    public bool IsBoundTo(StorageDatabase storage) => ReferenceEquals(_storage, storage);
    internal bool IsBoundTo(StorageNamespaceGcStore gcStore, string scopeId, NodeId localReplica)
        => gcStore is not null && gcStore.IsBoundTo(_storage, scopeId, localReplica);

    /// <summary>
    /// Reads the immutable per-namespace publication cutover marker for an FLD-backed
    /// provider. This keyspace is separate from decoded metadata partition rows so
    /// normal Metadata Store rewrites cannot discard the marker.
    /// </summary>
    public bool TryGetProviderPublicationCutover(
        string scopeId, NodeId localReplica, int namespaceId, out ulong nodeOutboxHighWatermark)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica must be non-zero.", nameof(localReplica));
        ushort ns = checked((ushort)namespaceId);
        lock (_gate)
        {
            ThrowIfDisposed();
            var partition = new PartitionKey(scopeId, localReplica);
            byte[] root = PublicationModeRoot(partition);
            using var snapshot = _storage.OpenSnapshot();
            byte[] identityKey = StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
            byte[] suffix = new byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, ns);
            byte[] markerKey = StorageKey(root, 1, suffix);
            bool hasIdentity = snapshot.TryGet(identityKey, out byte[]? identity);
            bool hasMarker = snapshot.TryGet(markerKey, out byte[]? value);
            if (!hasIdentity)
            {
                if (hasMarker)
                    throw new InvalidDataException("Provider publication cutover marker has no partition identity row.");
                nodeOutboxHighWatermark = 0;
                return false;
            }
            ValidatePartitionIdentity(partition, identity!);
            if (!hasMarker)
            {
                nodeOutboxHighWatermark = 0;
                return false;
            }
            nodeOutboxHighWatermark = DecodeProviderPublicationCutover(value!);
            return true;
        }
    }

    /// <summary>Persists an immutable per-namespace Node→Metadata Store publication cutover.</summary>
    public void EnableProviderPublicationCutover(
        string scopeId, NodeId localReplica, int namespaceId, ulong nodeOutboxHighWatermark)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica must be non-zero.", nameof(localReplica));
        ushort ns = checked((ushort)namespaceId);
        lock (_gate)
        {
            ThrowIfDisposed();
            var partition = new PartitionKey(scopeId, localReplica);
            byte[] root = PublicationModeRoot(partition);
            byte[] identityKey = StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
            byte[] suffix = new byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, ns);
            byte[] markerKey = StorageKey(root, 1, suffix);
            using var snapshot = _storage.OpenSnapshot();

            bool hasIdentity = snapshot.TryGet(identityKey, out byte[]? identity);
            if (hasIdentity)
                ValidatePartitionIdentity(partition, identity!);
            if (snapshot.TryGet(markerKey, out byte[]? existing))
            {
                if (!hasIdentity)
                    throw new InvalidDataException("Provider publication cutover marker has no partition identity row.");
                if (DecodeProviderPublicationCutover(existing!) != nodeOutboxHighWatermark)
                    throw new InvalidOperationException("Provider publication cutover watermark is immutable.");
                return;
            }

            using var batch = _storage.BeginWriteBatch();
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);
            if (!hasIdentity)
                batch.Put(identityKey, EncodePartitionIdentity(partition));
            batch.Put(markerKey, EncodeProviderPublicationCutover(nodeOutboxHighWatermark));
            batch.Commit();
        }
    }

    /// <summary>
    /// Durably binds one namespace to one stable FLD ClientId and prevents that ClientId's
    /// ClientSequence stream from being reused by another namespace of the same replica.
    /// The two indexes are committed atomically in the publication-mode keyspace.
    /// </summary>
    public Guid BindProviderReceiptStream(
        string scopeId, NodeId localReplica, int namespaceId, Guid receiptStreamId)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica must be non-zero.", nameof(localReplica));
        if (receiptStreamId == Guid.Empty)
            throw new ArgumentException("Provider receipt stream identity must be non-empty.", nameof(receiptStreamId));
        ushort ns = checked((ushort)namespaceId);

        lock (_gate)
        {
            ThrowIfDisposed();
            var partition = new PartitionKey(scopeId, localReplica);
            byte[] root = PublicationModeRoot(partition);
            byte[] identityKey = StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
            byte[] namespaceSuffix = new byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(namespaceSuffix, ns);
            byte[] namespaceKey = StorageKey(root, 2, namespaceSuffix);
            byte[] clientKey = StorageKey(root, 3, receiptStreamId.ToByteArray());

            using var snapshot = _storage.OpenSnapshot();
            bool hasIdentity = snapshot.TryGet(identityKey, out byte[]? identity);
            bool hasNamespaceBinding = snapshot.TryGet(namespaceKey, out byte[]? namespaceValue);
            bool hasClientBinding = snapshot.TryGet(clientKey, out byte[]? clientValue);
            if (!hasIdentity && (hasNamespaceBinding || hasClientBinding))
                throw new InvalidDataException("Provider receipt stream binding has no partition identity row.");
            if (hasIdentity)
                ValidatePartitionIdentity(partition, identity!);

            if (hasNamespaceBinding)
            {
                if (namespaceValue!.Length != 16)
                    throw new InvalidDataException("Provider receipt namespace binding has an invalid length.");
                if (new Guid(namespaceValue) != receiptStreamId)
                    throw new InvalidOperationException(
                        "BIN-SYNC namespace is already bound to a different FLD ClientId/receipt stream.");
            }
            if (hasClientBinding)
            {
                if (clientValue!.Length != 2)
                    throw new InvalidDataException("Provider receipt ClientId binding has an invalid namespace value.");
                if (BinaryPrimitives.ReadUInt16LittleEndian(clientValue) != ns)
                    throw new InvalidOperationException(
                        "FLD ClientId is already bound to a different BIN-SYNC namespace.");
            }
            if (hasNamespaceBinding != hasClientBinding)
                throw new InvalidDataException("Provider receipt stream indexes are incomplete.");
            if (hasNamespaceBinding)
                return receiptStreamId;

            using var batch = _storage.BeginWriteBatch();
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);
            if (!hasIdentity)
                batch.Put(identityKey, EncodePartitionIdentity(partition));
            batch.Put(namespaceKey, receiptStreamId.ToByteArray());
            batch.Put(clientKey, namespaceSuffix);
            batch.Commit();
            return receiptStreamId;
        }
    }

    private static byte[] EncodeProviderPublicationCutover(ulong nodeOutboxHighWatermark)
    {
        byte[] value = new byte[16];
        BinaryPrimitives.WriteUInt32LittleEndian(value, ProviderPublicationCutoverMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(value.AsSpan(4), ProviderPublicationCutoverVersion);
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), nodeOutboxHighWatermark);
        return value;
    }

    private static ulong DecodeProviderPublicationCutover(byte[] value)
    {
        if (value.Length != 16 ||
            BinaryPrimitives.ReadUInt32LittleEndian(value) != ProviderPublicationCutoverMagic ||
            BinaryPrimitives.ReadUInt16LittleEndian(value.AsSpan(4)) != ProviderPublicationCutoverVersion ||
            BinaryPrimitives.ReadUInt16LittleEndian(value.AsSpan(6)) != 0)
            throw new InvalidDataException("Provider publication cutover marker is not in canonical MCP1 format.");
        return BinaryPrimitives.ReadUInt64LittleEndian(value.AsSpan(8));
    }

    private static byte[] PublicationModeRoot(PartitionKey partition)
    {
        byte[] root = PartitionRoot(partition);
        root[0] = 0x71;
        return root;
    }

    public void SavePreparedFullReplaceSnapshot(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        PreparedFullReplaceSnapshotState state)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        ArgumentNullException.ThrowIfNull(state);
        if (localReplica.IsZero || state.Generation == Guid.Empty || state.SnapshotToken == Guid.Empty ||
            state.ManifestBytes is null || state.ManifestBytes.Length == 0)
            throw new ArgumentException("Prepared FULL_REPLACE snapshot identity or manifest is invalid.", nameof(state));
        ushort ns = checked((ushort)namespaceId);
        byte[] encoded = EncodePreparedFullReplaceSnapshot(state);

        lock (_gate)
        {
            ThrowIfDisposed();
            var partition = new PartitionKey(scopeId, localReplica);
            byte[] root = PreparedSnapshotRoot(partition);
            byte[] identityKey = StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
            byte[] indexKey = StorageKey(root, 1, EncodeNamespaceSuffix(ns));
            byte[] stateKey = StorageKey(root, 2, EncodePreparedSnapshotSuffix(ns, state.Generation));
            using var snapshot = _storage.OpenSnapshot();

            bool hasIdentity = snapshot.TryGet(identityKey, out byte[]? identity);
            if (hasIdentity)
                ValidatePartitionIdentity(partition, identity!);
            Guid[] generations = snapshot.TryGet(indexKey, out byte[]? indexBytes)
                ? DecodePreparedSnapshotIndex(indexBytes!)
                : Array.Empty<Guid>();
            if (snapshot.TryGet(stateKey, out byte[]? existing))
            {
                if (!generations.Contains(state.Generation))
                    throw new InvalidDataException("Prepared FULL_REPLACE state exists outside its generation index.");
                if (!existing!.AsSpan().SequenceEqual(encoded))
                    throw new InvalidOperationException("Prepared FULL_REPLACE snapshot generation is immutable.");
                return;
            }
            if (generations.Contains(state.Generation))
                throw new InvalidDataException("Prepared FULL_REPLACE snapshot index references a missing generation.");
            Guid[] updated = generations.Append(state.Generation).OrderBy(value => value).ToArray();
            if (updated.Length > 4096)
                throw new InvalidOperationException("Prepared FULL_REPLACE generation index exceeds its bounded capacity.");
            using var batch = _storage.BeginWriteBatch();
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);
            if (!hasIdentity)
                batch.Put(identityKey, EncodePartitionIdentity(partition));
            batch.Put(stateKey, encoded);
            batch.Put(indexKey, EncodePreparedSnapshotIndex(updated));
            batch.Commit();
        }
    }

    public bool TryGetPreparedFullReplaceSnapshot(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        Guid generation,
        out PreparedFullReplaceSnapshotState? state)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica must be non-zero.", nameof(localReplica));
        if (generation == Guid.Empty) throw new ArgumentException("Generation is required.", nameof(generation));
        ushort ns = checked((ushort)namespaceId);
        lock (_gate)
        {
            ThrowIfDisposed();
            var partition = new PartitionKey(scopeId, localReplica);
            byte[] root = PreparedSnapshotRoot(partition);
            byte[] identityKey = StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
            byte[] stateKey = StorageKey(root, 2, EncodePreparedSnapshotSuffix(ns, generation));
            using var snapshot = _storage.OpenSnapshot();
            if (!snapshot.TryGet(identityKey, out byte[]? identity))
            {
                state = null;
                return false;
            }
            ValidatePartitionIdentity(partition, identity!);
            byte[] indexKey = StorageKey(root, 1, EncodeNamespaceSuffix(ns));
            Guid[] generations = snapshot.TryGet(indexKey, out byte[]? index)
                ? DecodePreparedSnapshotIndex(index!)
                : Array.Empty<Guid>();
            if (!generations.Contains(generation))
            {
                // A normal miss (including after the last generation was deleted)
                // is represented by false. If an unindexed state row remains, that
                // is an orphan/corruption and must still fail closed.
                if (snapshot.TryGet(stateKey, out _))
                    throw new InvalidDataException("Prepared FULL_REPLACE state row is absent from its durable generation index.");
                state = null;
                return false;
            }
            if (!snapshot.TryGet(stateKey, out byte[]? encoded))
                throw new InvalidDataException("Prepared FULL_REPLACE generation index references a missing state row.");
            state = DecodePreparedFullReplaceSnapshot(encoded!);
            if (state.Generation != generation)
                throw new InvalidDataException("Prepared FULL_REPLACE state key and value generation differ.");
            return true;
        }
    }

    public IReadOnlyList<Guid> ListPreparedFullReplaceSnapshotGenerations(
        string scopeId, NodeId localReplica, int namespaceId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica must be non-zero.", nameof(localReplica));
        ushort ns = checked((ushort)namespaceId);
        lock (_gate)
        {
            ThrowIfDisposed();
            var partition = new PartitionKey(scopeId, localReplica);
            byte[] root = PreparedSnapshotRoot(partition);
            byte[] identityKey = StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
            byte[] indexKey = StorageKey(root, 1, EncodeNamespaceSuffix(ns));
            using var snapshot = _storage.OpenSnapshot();
            if (!snapshot.TryGet(identityKey, out byte[]? identity))
                return Array.Empty<Guid>();
            ValidatePartitionIdentity(partition, identity!);
            Guid[] generations = snapshot.TryGet(indexKey, out byte[]? index)
                ? DecodePreparedSnapshotIndex(index!)
                : Array.Empty<Guid>();
            foreach (Guid generation in generations)
            {
                if (!snapshot.TryGet(StorageKey(root, 2, EncodePreparedSnapshotSuffix(ns, generation)), out _))
                    throw new InvalidDataException("Prepared FULL_REPLACE index references a missing state row.");
            }
            return generations;
        }
    }

    public void DeletePreparedFullReplaceSnapshot(
        string scopeId, NodeId localReplica, int namespaceId, Guid generation)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica must be non-zero.", nameof(localReplica));
        if (generation == Guid.Empty) return;
        ushort ns = checked((ushort)namespaceId);
        lock (_gate)
        {
            ThrowIfDisposed();
            var partition = new PartitionKey(scopeId, localReplica);
            byte[] root = PreparedSnapshotRoot(partition);
            byte[] identityKey = StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
            byte[] indexKey = StorageKey(root, 1, EncodeNamespaceSuffix(ns));
            byte[] stateKey = StorageKey(root, 2, EncodePreparedSnapshotSuffix(ns, generation));
            using var snapshot = _storage.OpenSnapshot();
            if (!snapshot.TryGet(identityKey, out byte[]? identity))
                return;
            ValidatePartitionIdentity(partition, identity!);
            if (!snapshot.TryGet(stateKey, out _))
                return;
            Guid[] generations = snapshot.TryGet(indexKey, out byte[]? index)
                ? DecodePreparedSnapshotIndex(index!)
                : Array.Empty<Guid>();
            Guid[] updated = generations.Where(value => value != generation).ToArray();
            using var batch = _storage.BeginWriteBatch();
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);
            batch.Delete(stateKey);
            if (updated.Length == 0)
                batch.Delete(indexKey);
            else
                batch.Put(indexKey, EncodePreparedSnapshotIndex(updated));
            batch.Commit();
        }
    }

    private static byte[] PreparedSnapshotRoot(PartitionKey partition)
    {
        byte[] root = PartitionRoot(partition);
        root[0] = 0x72;
        return root;
    }

    private static byte[] EncodeNamespaceSuffix(ushort namespaceId)
    {
        byte[] suffix = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, namespaceId);
        return suffix;
    }

    private static byte[] EncodePreparedSnapshotSuffix(ushort namespaceId, Guid generation)
    {
        byte[] suffix = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, namespaceId);
        generation.TryWriteBytes(suffix.AsSpan(2));
        return suffix;
    }

    private static byte[] EncodePreparedFullReplaceSnapshot(PreparedFullReplaceSnapshotState state)
    {
        int length = checked(64 + state.ManifestBytes.Length + 32);
        byte[] bytes = new byte[length];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, PreparedFullReplaceMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4), PreparedFullReplaceVersion);
        state.Generation.TryWriteBytes(bytes.AsSpan(8, 16));
        state.SnapshotToken.TryWriteBytes(bytes.AsSpan(24, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(40), state.SourceFeedWatermark);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(48), state.HlcWallFloor);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(56), state.HlcLogicalFloor);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(60), checked((uint)state.ManifestBytes.Length));
        state.ManifestBytes.CopyTo(bytes, 64);
        SHA256.HashData(bytes.AsSpan(0, length - 32)).CopyTo(bytes, length - 32);
        return bytes;
    }

    private static PreparedFullReplaceSnapshotState DecodePreparedFullReplaceSnapshot(byte[] bytes)
    {
        if (bytes.Length < 96 ||
            BinaryPrimitives.ReadUInt32LittleEndian(bytes) != PreparedFullReplaceMagic ||
            BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4)) != PreparedFullReplaceVersion ||
            BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(6)) != 0)
            throw new InvalidDataException("Prepared FULL_REPLACE state header is invalid.");
        Guid generation = new(bytes.AsSpan(8, 16));
        uint manifestLength = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(60));
        if (generation == Guid.Empty || manifestLength == 0 || manifestLength > 4096 || 64L + manifestLength + 32 != bytes.Length)
            throw new InvalidDataException("Prepared FULL_REPLACE state length is invalid.");
        byte[] actualHash = SHA256.HashData(bytes.AsSpan(0, bytes.Length - 32));
        if (!CryptographicOperations.FixedTimeEquals(actualHash, bytes.AsSpan(bytes.Length - 32)))
            throw new InvalidDataException("Prepared FULL_REPLACE state checksum mismatch.");
        Guid snapshotToken = new(bytes.AsSpan(24, 16));
        if (snapshotToken == Guid.Empty)
            throw new InvalidDataException("Prepared FULL_REPLACE state has an empty snapshot token.");
        return new PreparedFullReplaceSnapshotState(
            generation,
            snapshotToken,
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(40)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(48)),
            BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(56)),
            bytes.AsSpan(64, (int)manifestLength).ToArray());
    }

    private static byte[] EncodePreparedSnapshotIndex(Guid[] generations)
    {
        byte[] bytes = new byte[checked(12 + generations.Length * 16)];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, PreparedFullReplaceIndexMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4), PreparedFullReplaceVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8), checked((uint)generations.Length));
        int offset = 12;
        foreach (Guid generation in generations)
        {
            generation.TryWriteBytes(bytes.AsSpan(offset, 16));
            offset += 16;
        }
        return bytes;
    }

    private static Guid[] DecodePreparedSnapshotIndex(byte[] bytes)
    {
        if (bytes.Length < 12 ||
            BinaryPrimitives.ReadUInt32LittleEndian(bytes) != PreparedFullReplaceIndexMagic ||
            BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4)) != PreparedFullReplaceVersion ||
            BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(6)) != 0)
            throw new InvalidDataException("Prepared FULL_REPLACE snapshot index header is invalid.");
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(8));
        if (count > 4096 || 12L + (long)count * 16 != bytes.Length)
            throw new InvalidDataException("Prepared FULL_REPLACE snapshot index length is invalid.");
        var result = new Guid[(int)count];
        var unique = new HashSet<Guid>();
        int offset = 12;
        for (int i = 0; i < result.Length; i++)
        {
            Guid generation = new(bytes.AsSpan(offset, 16));
            if (generation == Guid.Empty || !unique.Add(generation))
                throw new InvalidDataException("Prepared FULL_REPLACE snapshot index contains a zero or duplicate generation.");
            result[i] = generation;
            offset += 16;
        }
        return result;
    }

    private void RejectForgottenOrigins(PartitionState state, MetadataIncomingBatch batch)
    {
        StorageNamespaceGcStore? gcStore = _namespaceGcStore;
        TrustStore? trustStore = _namespaceGcTrustStore;
        if (gcStore is null || trustStore is null)
            return;

        var receiptKey = new ReceiptKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, batch.BatchId);
        bool hasReceipt = TryGetReceipt(new PartitionKey(batch.ScopeId, batch.LocalReplica), state, receiptKey, out ReceiptValue? receipt);
        if (hasReceipt && !receipt!.Matches(batch))
            throw new InvalidDataException("Incoming batch receipt identity was reused with conflicting metadata.");

        if (!gcStore.TryReadNamespaceRoster(batch.NamespaceId, trustStore, out NamespaceRosterCertificate? roster) ||
            roster is null)
            return;

        foreach (MetadataOperation operation in batch.Operations)
        {
            if (TryGetChangeLedger(new PartitionKey(batch.ScopeId, batch.LocalReplica), state, new LedgerKey(batch.NamespaceId, operation.ChangeId), out _))
                continue;
            if (operation.AuthorNodeId.IsZero || operation.OriginAuthorEpoch == 0 ||
                operation.OriginSequence == 0 || operation.ChangeId.IsZero)
                throw new InvalidDataException("Incoming v3 operation is missing immutable ChangeId/origin provenance.");
            if (gcStore.IsOriginCoveredByRetentionCheckpoint(
                    roster, trustStore, operation.AuthorNodeId,
                    operation.OriginAuthorEpoch, operation.OriginSequence))
                throw new InvalidDataException(
                    "Incoming operation has no retained ChangeId receipt but its origin is below the signed retention frontier; stale replay is rejected.");
            if (hasReceipt)
                throw new InvalidDataException("Stored batch receipt exists but its ChangeId ledger row is missing without a signed retention proof.");
        }
    }

    public IReadOnlyList<uint> ReadLocalOriginEpochs(
        string scopeId, NodeId localReplica, int namespaceId, NodeId localAuthor)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out PartitionState state))
                return Array.Empty<uint>();
            return state.OriginHighWatermarks.Keys
                .Where(key => key.NamespaceId == namespaceId && key.Author == localAuthor)
                .Select(key => key.AuthorEpoch)
                .Distinct()
                .OrderBy(epoch => epoch)
                .ToArray();
        }
    }

    /// <summary>
    /// Build and durably publish this member's contiguous applied-origin frontier
    /// from immutable ChangeLedger rows only. Unpublished provider drafts are supplied
    /// separately as a checkpoint barrier; their feed cursor is never an OriginSequence.
    /// </summary>
    public AppliedOriginFrontierReport PublishAppliedOriginFrontier(
        string scopeId,
        NodeId localMember,
        NamespaceRosterCertificate acceptedRoster,
        ulong unpublishedDraftCount,
        ulong maxUnpublishedDraftHlcWall,
        uint maxUnpublishedDraftHlcLogical,
        bool finalForAuthor,
        ReadOnlySpan<byte> memberSeed)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localMember.IsZero) throw new ArgumentException("Local member identity must be non-zero.", nameof(localMember));
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        if (memberSeed.Length != 32) throw new ArgumentException("Ed25519 member seed must be 32 bytes.", nameof(memberSeed));

        lock (_gate)
        {
            ThrowIfDisposed();
            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the durable namespace-GC store before publishing origin frontiers.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.TryReadNamespaceRoster(acceptedRoster.NamespaceId, trustStore,
                    out NamespaceRosterCertificate? currentRoster) || currentRoster is null ||
                currentRoster.RosterGeneration != acceptedRoster.RosterGeneration ||
                !NamespaceRosterProtocol.ComputeDigest(currentRoster).AsSpan()
                    .SequenceEqual(NamespaceRosterProtocol.ComputeDigest(acceptedRoster)))
                throw new InvalidOperationException("Applied-origin frontier roster is not the current accepted roster.");

            NamespaceRosterMember member = acceptedRoster.Members.FirstOrDefault(candidate => candidate.NodeId == localMember)
                ?? throw new InvalidDataException("Local member is not present in the accepted namespace roster.");
            if (member.Status != NamespaceMemberStatus.Active || member.TransportPeerEpoch == 0)
                throw new InvalidOperationException("Only an Active roster member may publish an applied-origin frontier.");

            var partitionKey = new PartitionKey(scopeId, localMember);
            PartitionState? current = TryGetPartition(partitionKey, out PartitionState loaded)
                ? loaded : null;
            AppliedOriginFrontierReport? prior = gcStore.ReadOriginFrontiers(acceptedRoster)
                .FirstOrDefault(report => report.MemberNodeId == localMember);
            ulong reportSequence = checked((prior?.ReportSequence ?? 0) + 1);

            var sequences = new Dictionary<(NodeId Author, uint Epoch), List<ulong>>();
            if (current is not null)
            {
                VisitChangeLedger(partitionKey, acceptedRoster.NamespaceId,
                    (key, origin) =>
                    {
                        if (!origin.AuthorNodeId.IsZero && origin.AuthorEpoch != 0 && origin.Sequence != 0)
                            AddOriginSequence(sequences, origin.AuthorNodeId, origin.AuthorEpoch, origin.Sequence);
                        return true;
                    });
            }


            var frontier = new Dictionary<(NodeId Author, uint Epoch), ulong>();
            if (prior is not null)
            {
                foreach (AppliedOriginFrontierEntry entry in prior.Entries)
                    frontier.Add((entry.OriginAuthorNodeId, entry.OriginAuthorEpoch),
                        entry.ContiguousAppliedOriginSequence);
            }

            foreach (var pair in sequences)
            {
                frontier.TryGetValue(pair.Key, out ulong contiguous);
                pair.Value.Sort();
                foreach (ulong sequence in pair.Value)
                {
                    if (sequence <= contiguous) continue;
                    if (sequence != contiguous + 1) break;
                    contiguous = sequence;
                }
                if (contiguous != 0) frontier[pair.Key] = contiguous;
            }

            var entries = frontier
                .Where(pair => pair.Value != 0)
                .Select(pair => new AppliedOriginFrontierEntry
                {
                    OriginAuthorNodeId = pair.Key.Author,
                    OriginAuthorEpoch = pair.Key.Epoch,
                    ContiguousAppliedOriginSequence = pair.Value
                })
                .OrderBy(entry => entry.OriginAuthorNodeId, Comparer<NodeId>.Create((left, right) =>
                {
                    Span<byte> leftBytes = stackalloc byte[16];
                    Span<byte> rightBytes = stackalloc byte[16];
                    left.WriteTo(leftBytes);
                    right.WriteTo(rightBytes);
                    return leftBytes.SequenceCompareTo(rightBytes);
                }))
                .ThenBy(entry => entry.OriginAuthorEpoch)
                .ToArray();

            var report = new AppliedOriginFrontierReport
            {
                NamespaceId = acceptedRoster.NamespaceId,
                RosterGeneration = acceptedRoster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(acceptedRoster),
                MemberNodeId = localMember,
                MemberTransportEpoch = member.TransportPeerEpoch,
                ReportSequence = reportSequence,
                FinalForAuthor = finalForAuthor,
                UnpublishedDraftCount = unpublishedDraftCount,
                MaxUnpublishedDraftHlcWall = maxUnpublishedDraftHlcWall,
                MaxUnpublishedDraftHlcLogical = maxUnpublishedDraftHlcLogical,
                Entries = entries,
                IssuedOrigins = current is null
                    ? Array.Empty<IssuedOriginFrontierEntry>()
                    : current.OriginHighWatermarks
                        .Where(pair => pair.Key.NamespaceId == acceptedRoster.NamespaceId &&
                            pair.Key.Author == localMember && pair.Value.Sequence != 0)
                        .OrderBy(pair => pair.Key.AuthorEpoch)
                        .Select(pair => new IssuedOriginFrontierEntry
                        {
                            OriginAuthorEpoch = pair.Key.AuthorEpoch,
                            IssuedOriginSequence = pair.Value.Sequence,
                            MaxIssuedHlcWall = pair.Value.MaxHlcWall,
                            MaxIssuedHlcLogical = pair.Value.MaxHlcLogical
                        })
                        .ToArray()
            };
            byte[] signed = OriginFrontierProtocol.Sign(report, memberSeed);
            return gcStore.AcceptOriginFrontier(signed, acceptedRoster);
        }
    }

    private static void AddOriginSequence(
        Dictionary<(NodeId Author, uint Epoch), List<ulong>> sequences,
        NodeId author, uint epoch, ulong sequence)
    {
        var key = (author, epoch);
        if (!sequences.TryGetValue(key, out List<ulong>? values))
            sequences.Add(key, values = new List<ulong>());
        values.Add(sequence);
    }

    /// <summary>
    /// Reclaim ChangeId ledger rows only below the signed all-active-member origin
    /// frontier and while the caller holds an exclusive cross-store live-reference
    /// lease. The retained checkpoint becomes the durable replay fence.
    /// </summary>
    internal static bool IsPeerStreamGcReservationOutstanding(
        bool hasPendingBatch, ulong pushedCursor, ulong ackedCursor)
        => hasPendingBatch || pushedCursor != ackedCursor;

    internal static bool TryCollectProviderChangeIdsForGc(
        IDataProvider provider, ulong providerHighWatermark, out MetadataStreamChangeIdBloomFilter liveChangeIds)
    {
        ArgumentNullException.ThrowIfNull(provider);

        // The feed sequence high-watermark is an upper bound on retained identities.
        // Size from that bound in one pass; the filter is capped independently of feed
        // history. A positive is only a conservative retain decision, never delete proof.
        const int minBytes = 256;
        const int maxBytes = 8 * 1024 * 1024;
        ulong maxItemsAtCap = (ulong)maxBytes * 8 / 10;
        int byteCount = providerHighWatermark >= maxItemsAtCap
            ? maxBytes
            : (int)Math.Max((ulong)minBytes, (providerHighWatermark * 10 + 7) / 8);
        liveChangeIds = new MetadataStreamChangeIdBloomFilter(byteCount);

        ulong providerCursor = 0;
        const int providerPageSize = 1024;
        while (providerCursor < providerHighWatermark)
        {
            ProviderChangePage providerPage = provider.ReadLocalChanges(providerCursor, providerPageSize);
            if (providerPage.ScannedThroughSequence <= providerCursor ||
                providerPage.ScannedThroughSequence > providerHighWatermark ||
                providerPage.Changes.Count > providerPageSize)
                throw new InvalidDataException("Provider local feed returned an invalid ChangeId GC scan page.");

            ulong previousProviderSequence = providerCursor;
            foreach (ProviderChange change in providerPage.Changes)
            {
                if (change.Sequence <= previousProviderSequence ||
                    change.Sequence > providerPage.ScannedThroughSequence)
                    throw new InvalidDataException("Provider local feed changes are not ordered within the ChangeId GC scan prefix.");
                previousProviderSequence = change.Sequence;
                if (change.ChangeId.IsZero)
                {
                    liveChangeIds = new MetadataStreamChangeIdBloomFilter(minBytes);
                    return false; // Legacy/unknown feed identity blocks ChangeId reclamation.
                }
                liveChangeIds.Add(change.ChangeId);
            }
            providerCursor = providerPage.ScannedThroughSequence;
        }
        return true;
    }

    public int CompactChangeLedger(
        string scopeId, NodeId localReplica, int namespaceId, IDataProvider provider, int limit,
        Func<IDisposable?> acquireGcLease, IApplicationGcReferenceScanner externalReferences)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (limit <= 0 || limit > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(acquireGcLease);
        ArgumentNullException.ThrowIfNull(externalReferences);

        IDisposable? lease = acquireGcLease();
        if (lease is null)
            throw new InvalidOperationException("Cross-store ChangeId GC proof is unavailable; no ledger rows were reclaimed.");
        using IDisposable writerAdmission = _writerAdmission.Enter();
        using (lease)
        using (IDisposable? providerSnapshotLease =
            provider is IProviderSnapshotGcGuard snapshotGuard
                ? snapshotGuard.TryAcquireSnapshotGcGuard()
                : null)
        lock (_gate)
        {
            if (provider is IProviderSnapshotGcGuard && providerSnapshotLease is null)
                return 0; // An active immutable provider snapshot still references its captured ChangeIds.
            ThrowIfDisposed();
            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the durable namespace-GC replay guard before ChangeId GC.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.IsChangeIdGcProofEnabled)
                return 0;
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out PartitionState? current))
                return 0;
            if (!gcStore.TryReadNamespaceRoster(namespaceId, trustStore, out NamespaceRosterCertificate? roster) ||
                roster is null)
                throw new InvalidOperationException("No accepted namespace roster is available for ChangeId GC.");
            if (!gcStore.TryReadRetentionCheckpoint(roster, trustStore,
                    out NamespaceRetentionCheckpoint? retentionCheckpoint) ||
                retentionCheckpoint is null)
                return 0;
            Dictionary<(NodeId Author, uint Epoch), ulong> safeOriginFrontier =
                BuildSafeOriginFrontier(retentionCheckpoint);
            using IApplicationGcReferenceScan externalScan =
                externalReferences.OpenScan(scopeId, localReplica, namespaceId);

            // Provider, PendingApply and durable stream identities use bounded Bloom
            // prefilters. A positive only retains a ledger row; exact Host references,
            // signed frontiers and writer/GC leases remain the deletion proof.
            if (current.PeerStreams.Any(pair =>
                pair.Key.NamespaceId == namespaceId &&
                IsPeerStreamGcReservationOutstanding(
                    pair.Value.PendingBatch is not null,
                    pair.Value.PushedCursor,
                    pair.Value.AckedCursor)))
                return 0;
            ulong providerHighWatermark = provider.LocalChangeHighWatermark;
            if (!TryCollectProviderChangeIdsForGc(provider, providerHighWatermark, out MetadataStreamChangeIdBloomFilter liveChangeIds))
                return 0;
            // PendingApply can contain a large unresolved receive backlog. Keep its
            // live-identity guard bounded just like stream history; false positives
            // only retain ledger rows and can never authorize deletion.
            MetadataStreamChangeIdBloomFilter pendingApplyLiveIds =
                BuildPendingApplyChangeIdBloomFilter(current, namespaceId);
            MetadataStreamChangeIdBloomFilter streamLiveIds =
                BuildMetadataStreamChangeIdBloomFilter(partitionKey, current, namespaceId);
            var candidates = new List<(LedgerKey Key, byte[] StorageKey)>(limit);
            byte[] partitionRoot = PartitionRoot(partitionKey);
            VisitChangeLedger(partitionKey, namespaceId, (key, origin) =>
            {
                if (origin.AuthorNodeId.IsZero || origin.AuthorEpoch == 0 || origin.Sequence == 0 ||
                    !IsOriginCoveredBySafeFrontier(safeOriginFrontier, origin) ||
                    liveChangeIds.MightContain(key.ChangeId) ||
                    pendingApplyLiveIds.MightContain(key.ChangeId) ||
                    streamLiveIds.MightContain(key.ChangeId) ||
                    externalScan.HasExternalChangeIdReference(key.ChangeId))
                    return true;

                candidates.Add((key, ChangeLedgerStorageKey(partitionRoot, namespaceId, key.ChangeId)));
                return candidates.Count < limit;
            });

            if (candidates.Count == 0)
                return 0;
            IReadOnlyList<byte[]> deletes = new ChangeIdTupleStorageKeyView(candidates);
            CommitPeerMetadataDelta(partitionKey, current,
                Array.Empty<(byte[] Key, byte[] Value)>(), deletes,
                static () => { });
            return candidates.Count;
        }
    }

    /// <summary>
    /// Async counterpart for process-isolated providers. The Host scan is opened once
    /// and queried outside the Metadata monitor; candidates are then revalidated under
    /// the monitor before one bounded durable deletion transaction.
    /// </summary>
    public async Task<int> CompactChangeLedgerAsync(
        string scopeId, NodeId localReplica, int namespaceId, int limit,
        IAsyncApplicationGcReferenceScanner externalReferences,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (limit <= 0 || limit > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        ArgumentNullException.ThrowIfNull(externalReferences);

        Action<string, long>? allocationObserver = ChangeIdGcAllocationObserver;
        long allocationCheckpoint = allocationObserver is null ? 0 : GC.GetTotalAllocatedBytes(precise: true);
        void ReportAllocation(string phase)
        {
            if (allocationObserver is null) return;
            long allocated = GC.GetTotalAllocatedBytes(precise: true);
            allocationObserver(phase, allocated - allocationCheckpoint);
            allocationCheckpoint = allocated;
        }

        await using IAsyncApplicationGcReferenceScan externalScan =
            await externalReferences.OpenScanAsync(scopeId, localReplica, namespaceId, cancellationToken)
                .ConfigureAwait(false);
        ReportAllocation("host-scan-open");
        using IDisposable writerAdmission = await _writerAdmission.EnterAsync(cancellationToken).ConfigureAwait(false);
        ApplicationDraftHorizon draftHorizon =
            await externalScan.GetUnpublishedLocalDraftHorizonAsync(cancellationToken).ConfigureAwait(false);
        if (draftHorizon.Count == 0 &&
            (draftHorizon.MaxHlcWall != 0 || draftHorizon.MaxHlcLogical != 0))
            throw new InvalidDataException("Async Host GC scanner returned an HLC maximum for an empty draft horizon.");
        ReportAllocation("writer-admission-and-draft-horizon");

        if (externalScan.HasSnapshotPin)
            return 0; // An immutable provider snapshot may still retain identities from the captured generation.

        StorageNamespaceGcStore gcStore = _namespaceGcStore
            ?? throw new InvalidOperationException("Attach the durable namespace-GC replay guard before ChangeId GC.");
        TrustStore trustStore = _namespaceGcTrustStore
            ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
        if (!gcStore.IsChangeIdGcProofEnabled)
            return 0;

        var partitionKey = new PartitionKey(scopeId, localReplica);
        List<ChangeIdGcCandidate> candidates;
        MetadataStreamChangeIdBloomFilter streamLiveIds;
        MetadataStreamChangeIdBloomFilter pendingApplyLiveIds;
        ulong initialMetadataRevision;
        Dictionary<(NodeId Author, uint Epoch), ulong> safeOriginFrontier;
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!TryGetPartition(partitionKey, out PartitionState? current))
                return 0;
            if (!gcStore.TryReadNamespaceRoster(namespaceId, trustStore, out NamespaceRosterCertificate? roster) ||
                roster is null)
                throw new InvalidOperationException("No accepted namespace roster is available for ChangeId GC.");
            if (!gcStore.TryReadRetentionCheckpoint(roster, trustStore,
                    out NamespaceRetentionCheckpoint? retentionCheckpoint) ||
                retentionCheckpoint is null)
                return 0;
            safeOriginFrontier = BuildSafeOriginFrontier(retentionCheckpoint);
            if (current.PeerStreams.Any(pair =>
                pair.Key.NamespaceId == namespaceId &&
                IsPeerStreamGcReservationOutstanding(
                    pair.Value.PendingBatch is not null,
                    pair.Value.PushedCursor,
                    pair.Value.AckedCursor)))
                return 0;

            // Every Metadata writer shares writerAdmission. Capture the partition
            // revision so the post-Host check can revalidate the entire candidate set
            // with one point read instead of one B+ tree lookup per ChangeId.
            initialMetadataRevision = current.MetadataRevision;
            pendingApplyLiveIds = BuildPendingApplyChangeIdBloomFilter(current, namespaceId);
            streamLiveIds = BuildMetadataStreamChangeIdBloomFilter(partitionKey, current, namespaceId);
            candidates = new List<ChangeIdGcCandidate>(limit);
            VisitChangeLedger(partitionKey, namespaceId, (key, origin) =>
            {
                if (origin.AuthorNodeId.IsZero || origin.AuthorEpoch == 0 || origin.Sequence == 0 ||
                    pendingApplyLiveIds.MightContain(key.ChangeId) ||
                    streamLiveIds.MightContain(key.ChangeId) ||
                    !IsOriginCoveredBySafeFrontier(safeOriginFrontier, origin))
                    return true;
                candidates.Add(new ChangeIdGcCandidate(key, origin, null));
                return candidates.Count < limit;
            });
        }
        ReportAllocation("metadata-candidate-discovery");

        if (candidates.Count == 0)
            return 0;

        // ChangeLedger is keyed by (namespace, ChangeId), and this candidate
        // scan is already restricted to one namespace, so these IDs are unique.
        // Expose them through a zero-copy IReadOnlyList view instead of allocating
        // a second 16-byte-per-entry BatchId array.
        var candidateChangeIds = new ChangeIdCandidateView(candidates);
        ReportAllocation("candidate-id-materialization");
        IReadOnlySet<BatchId> hostReferenced =
            await externalScan.FindExternalChangeIdReferencesAsync(
                candidateChangeIds, cancellationToken).ConfigureAwait(false);
        ReportAllocation("host-reference-ipc-scan");
        if (hostReferenced.Count >= candidateChangeIds.Count)
            return 0;

        long revalidationStart = allocationObserver is null
            ? 0
            : GC.GetTotalAllocatedBytes(precise: true);
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!TryGetPartition(partitionKey, out PartitionState? current))
                return 0;
            if (current.MetadataRevision != initialMetadataRevision)
                return 0;
            if (current.PeerStreams.Any(pair =>
                pair.Key.NamespaceId == namespaceId &&
                IsPeerStreamGcReservationOutstanding(
                    pair.Value.PendingBatch is not null,
                    pair.Value.PushedCursor,
                    pair.Value.AckedCursor)))
                return 0;
            // The Host lease and shared Metadata writer admission have remained held
            // since candidate discovery. Every partition mutation advances this
            // revision atomically with its rows, so one revision check revalidates
            // the candidate page; per-ChangeId Storage reads would repeat B+ tree work.
            byte[] partitionRoot = PartitionRoot(partitionKey);
            // Compact approved entries in the candidate list itself. A second
            // List<(LedgerKey, byte[])> duplicates a large backing array for every
            // GC page; the candidate list is no longer needed after Host revalidation.
            int approvedCount = 0;
            for (int i = 0; i < candidates.Count; i++)
            {
                ChangeIdGcCandidate candidate = candidates[i];
                LedgerKey key = candidate.Key;
                OperationOrigin snapshotOrigin = candidate.Provenance;
                if (hostReferenced.Contains(key.ChangeId) ||
                    pendingApplyLiveIds.MightContain(key.ChangeId) ||
                    streamLiveIds.MightContain(key.ChangeId) ||
                    !IsOriginCoveredBySafeFrontier(safeOriginFrontier, snapshotOrigin))
                    continue;

                candidates[approvedCount++] = candidate with
                {
                    StorageKey = ChangeLedgerStorageKey(partitionRoot, namespaceId, key.ChangeId)
                };
            }
            ReportAllocation("metadata-revalidation-approved-candidates");
            if (approvedCount == 0)
                return 0;

            IReadOnlyList<byte[]> deletes = new ChangeIdStorageKeyView(candidates, approvedCount);
            ReportAllocation("metadata-revalidation-delete-view");
            CommitPeerMetadataDelta(partitionKey, current,
                Array.Empty<(byte[] Key, byte[] Value)>(), deletes,
                static () => { });
            ReportAllocation("metadata-revalidation-storage-commit");
            int reclaimed = approvedCount;
            if (allocationObserver is not null)
            {
                allocationObserver("metadata-revalidation-total",
                    GC.GetTotalAllocatedBytes(precise: true) - revalidationStart);
                allocationCheckpoint = GC.GetTotalAllocatedBytes(precise: true);
            }
            return reclaimed;
        }
    }

    private static Dictionary<(NodeId Author, uint Epoch), ulong> BuildSafeOriginFrontier(
        NamespaceRetentionCheckpoint checkpoint)
    {
        var frontier = new Dictionary<(NodeId Author, uint Epoch), ulong>(
            checkpoint.SafeFrontier.Count);
        foreach (RetentionFrontierEntry entry in checkpoint.SafeFrontier)
        {
            if (!frontier.TryAdd(
                    (entry.OriginAuthorNodeId, entry.OriginAuthorEpoch),
                    entry.SafeAppliedOriginSequence))
                throw new InvalidDataException("Retention checkpoint contains a duplicate origin frontier.");
        }
        return frontier;
    }

    private static bool IsOriginCoveredBySafeFrontier(
        IReadOnlyDictionary<(NodeId Author, uint Epoch), ulong> frontier,
        OperationOrigin origin)
        => IsOriginCoveredBySafeFrontier(
            frontier, origin.AuthorNodeId, origin.AuthorEpoch, origin.Sequence);

    private static bool IsOriginCoveredBySafeFrontier(
        IReadOnlyDictionary<(NodeId Author, uint Epoch), ulong> frontier,
        NodeId author, uint epoch, ulong sequence)
        => frontier.TryGetValue((author, epoch), out ulong safeSequence) &&
           sequence <= safeSequence;

    private MetadataStreamChangeIdBloomFilter BuildPendingApplyChangeIdBloomFilter(
        PartitionState current, int namespaceId)
    {
        // Count first, then populate a capped filter. PendingApply rows are disk-primary;
        // enumerating twice trades bounded CPU/I/O for avoiding an unbounded HashSet.
        ulong expectedItems = 0;
        foreach (PendingApplyValue pending in current.PendingApplies.Values)
        {
            if (pending.Batch.NamespaceId != namespaceId)
                continue;
            ulong count = (ulong)pending.Batch.Operations.Count;
            expectedItems = ulong.MaxValue - expectedItems < count
                ? ulong.MaxValue
                : expectedItems + count;
        }

        const int minBytes = 64 * 1024;
        const int maxBytes = 8 * 1024 * 1024;
        const ulong maxItemsAtCap = (ulong)maxBytes * 8 / 10;
        int byteCount = expectedItems >= maxItemsAtCap
            ? maxBytes
            : (int)Math.Max((ulong)minBytes, (expectedItems * 10 + 7) / 8);
        var filter = new MetadataStreamChangeIdBloomFilter(byteCount);
        foreach (PendingApplyValue pending in current.PendingApplies.Values)
        {
            if (pending.Batch.NamespaceId != namespaceId)
                continue;
            foreach (MetadataOperation operation in pending.Batch.Operations)
                filter.Add(operation.ChangeId);
        }
        return filter;
    }

    private MetadataStreamChangeIdBloomFilter BuildMetadataStreamChangeIdBloomFilter(
        PartitionKey partitionKey, PartitionState state, int namespaceId)
    {
        const int minBytes = 64 * 1024;
        const int maxBytes = 8 * 1024 * 1024;
        ulong expectedItems = 0;
        static ulong SaturatingAdd(ulong current, ulong value) =>
            ulong.MaxValue - current < value ? ulong.MaxValue : current + value;

        foreach (var pair in state.RelayHighWatermarks)
            if (pair.Key.NamespaceId == namespaceId)
                expectedItems = SaturatingAdd(expectedItems, pair.Value);
        foreach (var pair in state.OutboundHighWatermarks)
            if (pair.Key.NamespaceId == namespaceId)
                expectedItems = SaturatingAdd(expectedItems, pair.Value);

        const ulong maxItemsAtCap = (ulong)maxBytes * 8 / 10;
        int byteCount = expectedItems >= maxItemsAtCap
            ? maxBytes
            : (int)Math.Max((ulong)minBytes, (expectedItems * 10 + 7) / 8);
        var filter = new MetadataStreamChangeIdBloomFilter(byteCount);

        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        byte[] root = PartitionRoot(partitionKey);
        if (!VisitRelayStorageRows(snapshot, root, state, namespaceId,
            (key, sequence, payload) =>
            {
                filter.Add(DecodePayloadHeader(payload));
                return true;
            }))
            throw new InvalidDataException("Relay ChangeId Bloom scan was interrupted.");
        if (!VisitOutboundStorageRows(snapshot, root, state, namespaceId,
            (key, sequence, payload) =>
            {
                filter.Add(DecodePayloadHeader(payload));
                return true;
            }))
            throw new InvalidDataException("Outbound ChangeId Bloom scan was interrupted.");
        return filter;
    }

    // Conservative prefilter only: positives retain metadata; false positives never delete data.
    // A conservative, capped prefilter for pending receipt identities. False
    // positives only retain receipts; this filter never proves that a receipt is dead.
    private sealed class PendingReceiptBloomFilter
    {
        private const int HashCount = 7;
        private readonly byte[] _bits;
        private readonly ulong _bitCount;

        public PendingReceiptBloomFilter(int byteCount)
        {
            _bits = new byte[byteCount];
            _bitCount = checked((ulong)byteCount * 8);
        }

        public void Add(ReceiptKey key) => VisitBits(key, add: true);
        public bool MightContain(ReceiptKey key) => VisitBits(key, add: false);

        private bool VisitBits(ReceiptKey key, bool add)
        {
            ulong seed = unchecked((uint)key.GetHashCode());
            ulong first = Mix(seed);
            ulong step = Mix(seed ^ 0x9E3779B97F4A7C15UL) | 1UL;
            for (ulong i = 0; i < HashCount; i++)
            {
                ulong bit = unchecked(first + i * step) % _bitCount;
                int byteIndex = (int)(bit >> 3);
                byte mask = (byte)(1 << (int)(bit & 7));
                if (add)
                    _bits[byteIndex] |= mask;
                else if ((_bits[byteIndex] & mask) == 0)
                    return false;
            }
            return true;
        }

        private static ulong Mix(ulong value)
        {
            unchecked
            {
                value ^= value >> 30;
                value *= 0xBF58476D1CE4E5B9UL;
                value ^= value >> 27;
                value *= 0x94D049BB133111EBUL;
                return value ^ (value >> 31);
            }
        }
    }

    internal sealed class MetadataStreamChangeIdBloomFilter
    {
        private const int HashCount = 7;
        private readonly byte[] _bits;
        private readonly ulong _bitCount;

        public MetadataStreamChangeIdBloomFilter(int byteCount)
        {
            _bits = new byte[byteCount];
            _bitCount = checked((ulong)byteCount * 8);
        }

        internal int StorageBytes => _bits.Length;

        public void Add(BatchId changeId) => VisitBits(changeId, add: true);

        public bool MightContain(BatchId changeId) => VisitBits(changeId, add: false);

        private bool VisitBits(BatchId changeId, bool add)
        {
            Span<byte> bytes = stackalloc byte[16];
            changeId.WriteTo(bytes);
            ulong first = Mix(BinaryPrimitives.ReadUInt64LittleEndian(bytes));
            ulong step = Mix(BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8))) | 1UL;
            for (ulong i = 0; i < HashCount; i++)
            {
                ulong bit = unchecked(first + i * step) % _bitCount;
                int byteIndex = (int)(bit >> 3);
                byte mask = (byte)(1 << (int)(bit & 7));
                if (add)
                    _bits[byteIndex] |= mask;
                else if ((_bits[byteIndex] & mask) == 0)
                    return false;
            }
            return true;
        }

        private static ulong Mix(ulong value)
        {
            unchecked
            {
                value ^= value >> 30;
                value *= 0xBF58476D1CE4E5B9UL;
                value ^= value >> 27;
                value *= 0x94D049BB133111EBUL;
                return value ^ (value >> 31);
            }
        }
    }

    /// <summary>Async Host-scanner counterpart for bounded retired-epoch receipt compaction.</summary>
    public async Task<int> CompactReceiptsAsync(
        string scopeId, NodeId localReplica, int namespaceId, int limit,
        IAsyncApplicationGcReferenceScanner externalReferences,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (limit <= 0 || limit > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        ArgumentNullException.ThrowIfNull(externalReferences);

        await using IAsyncApplicationGcReferenceScan externalScan =
            await externalReferences.OpenReceiptGcScanAsync(scopeId, localReplica, namespaceId, cancellationToken)
                .ConfigureAwait(false);
        using IDisposable writerAdmission = await _writerAdmission.EnterAsync(cancellationToken).ConfigureAwait(false);
        ApplicationDraftHorizon draftHorizon =
            await externalScan.GetUnpublishedLocalDraftHorizonAsync(cancellationToken).ConfigureAwait(false);
        if (draftHorizon.Count == 0 &&
            (draftHorizon.MaxHlcWall != 0 || draftHorizon.MaxHlcLogical != 0))
            throw new InvalidDataException("Async Host GC scanner returned an HLC maximum for an empty draft horizon.");

        if (externalScan.HasSnapshotPin)
            return 0; // An immutable provider snapshot may still retain receipt-era state.

        StorageNamespaceGcStore gcStore = _namespaceGcStore
            ?? throw new InvalidOperationException("Attach the durable namespace-GC replay guard before receipt GC.");
        TrustStore trustStore = _namespaceGcTrustStore
            ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
        if (!gcStore.IsReceiptGcProofEnabled)
            return 0;

        var partitionKey = new PartitionKey(scopeId, localReplica);
        NamespaceRosterCertificate roster;
        Dictionary<(NodeId Author, uint Epoch), ulong> safeOriginFrontier;
        List<(ReceiptKey Key, ReceiptValue Value)> candidates;
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!TryGetPartition(partitionKey, out PartitionState? current))
                return 0;
            if (!gcStore.TryReadNamespaceRoster(namespaceId, trustStore, out NamespaceRosterCertificate? acceptedRoster) ||
                acceptedRoster is null)
                throw new InvalidOperationException("No accepted namespace roster is available for receipt GC.");
            roster = acceptedRoster;
            if (!gcStore.TryReadRetentionCheckpoint(roster, trustStore,
                    out NamespaceRetentionCheckpoint? retentionCheckpoint) ||
                retentionCheckpoint is null)
                return 0;
            safeOriginFrontier = BuildSafeOriginFrontier(retentionCheckpoint);
            candidates = BuildReceiptGcCandidates(
                partitionKey, current, namespaceId, roster, safeOriginFrontier, limit);
        }

        if (candidates.Count == 0)
            return 0;

        var hostUnreferenced = new HashSet<ReceiptKey>();
        foreach ((ReceiptKey key, ReceiptValue receipt) in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!await externalScan.HasExternalReceiptReferenceAsync(
                    key.Sender, key.Epoch, receipt.TransportPeerEpoch, key.BatchId,
                    cancellationToken).ConfigureAwait(false))
                hostUnreferenced.Add(key);
        }
        if (hostUnreferenced.Count == 0)
            return 0;

        lock (_gate)
        {
            ThrowIfDisposed();
            if (!TryGetPartition(partitionKey, out PartitionState? current))
                return 0;
            using StorageSnapshot revalidationSnapshot = _storage.OpenSnapshot();
            List<(ReceiptKey Key, byte[] StorageKey)> approved = new(Math.Min(limit, hostUnreferenced.Count));
            PendingReceiptBloomFilter pendingReceiptFilter = BuildPendingReceiptBloomFilter(current);
            var rosterMembers = roster.Members.ToDictionary(member => member.NodeId);
            foreach ((ReceiptKey key, ReceiptValue observed) in candidates)
            {
                if (!hostUnreferenced.Contains(key) ||
                    !TryGetReceipt(revalidationSnapshot, partitionKey, current, key, out ReceiptValue? receipt) ||
                    !ReceiptValuesEqual(receipt!, observed) ||
                    key.NamespaceId != namespaceId || receipt.DurableAuthor.IsZero ||
                    key.Epoch == 0 || receipt.OriginSequence == 0 ||
                    pendingReceiptFilter.MightContain(key) ||
                    !rosterMembers.TryGetValue(key.Sender, out NamespaceRosterMember? senderMember) ||
                    !current.RetiredPeerEpochs.TryGetValue(
                        (namespaceId, key.Sender), out uint retiredThrough) ||
                    !IsReceiptTransportEpochFenced(senderMember, receipt.TransportPeerEpoch, retiredThrough))
                    continue;

                var receiveKey = new ReceiveKey(
                    key.Sender, key.Epoch, key.NamespaceId, receipt.DurableAuthor);
                if (!current.Receives.TryGetValue(receiveKey, out ReceiveState receive) ||
                    receive.CursorEnd < receipt.CursorEnd ||
                    !HasReceiveChainForAuthor(current, receiveKey) ||
                    !IsOriginCoveredBySafeFrontier(
                        safeOriginFrontier, receipt.DurableAuthor, key.Epoch, receipt.OriginSequence))
                    continue;
                approved.Add((key, EncodeReceiptKey(partitionKey, key)));
                if (approved.Count == limit)
                    break;
            }

            if (approved.Count == 0)
                return 0;
            byte[][] deletes = approved.Select(item => item.StorageKey).ToArray();
            CommitPeerMetadataDelta(partitionKey, current,
                Array.Empty<(byte[] Key, byte[] Value)>(), deletes,
                static () => { });
            return approved.Count;
        }
    }

    private List<(ReceiptKey Key, ReceiptValue Value)> BuildReceiptGcCandidates(
        PartitionKey partitionKey, PartitionState current, int namespaceId, NamespaceRosterCertificate roster,
        IReadOnlyDictionary<(NodeId Author, uint Epoch), ulong> safeOriginFrontier, int limit)
    {
        var rosterMembers = roster.Members.ToDictionary(member => member.NodeId);
        PendingReceiptBloomFilter pendingReceiptFilter = BuildPendingReceiptBloomFilter(current);
        var candidates = new List<(ReceiptKey, ReceiptValue)>(limit);
        foreach (var pair in EnumerateReceipts(partitionKey, current, namespaceId))
        {
            ReceiptKey key = pair.Key;
            ReceiptValue receipt = pair.Value;
            if (key.NamespaceId != namespaceId || receipt.DurableAuthor.IsZero ||
                key.Epoch == 0 || receipt.OriginSequence == 0 ||
                !rosterMembers.TryGetValue(key.Sender, out NamespaceRosterMember? senderMember) ||
                !current.RetiredPeerEpochs.TryGetValue(
                    (namespaceId, key.Sender), out uint retiredThrough) ||
                !IsReceiptTransportEpochFenced(senderMember, receipt.TransportPeerEpoch, retiredThrough))
                continue;
            var receiveKey = new ReceiveKey(
                key.Sender, key.Epoch, key.NamespaceId, receipt.DurableAuthor);
            if (!current.Receives.TryGetValue(receiveKey, out ReceiveState receive) ||
                receive.CursorEnd < receipt.CursorEnd ||
                !HasReceiveChainForAuthor(current, receiveKey) ||
                pendingReceiptFilter.MightContain(key) ||
                !IsOriginCoveredBySafeFrontier(
                    safeOriginFrontier, receipt.DurableAuthor, key.Epoch, receipt.OriginSequence))
                continue;
            candidates.Add((key, receipt));
            if (candidates.Count == limit)
                break;
        }
        return candidates;
    }

    private static PendingReceiptBloomFilter BuildPendingReceiptBloomFilter(PartitionState current)
    {
        ulong expectedItems = 0;
        foreach (PendingApplyValue intent in current.PendingApplies.Values)
            if (expectedItems != ulong.MaxValue)
                expectedItems++;

        const int minBytes = 64 * 1024;
        const int maxBytes = 8 * 1024 * 1024;
        ulong maxItemsAtCap = (ulong)maxBytes * 8 / 10;
        int byteCount = expectedItems >= maxItemsAtCap
            ? maxBytes
            : (int)Math.Max((ulong)minBytes, (expectedItems * 10 + 7) / 8);
        var filter = new PendingReceiptBloomFilter(byteCount);
        foreach (PendingApplyValue intent in current.PendingApplies.Values)
        {
            MetadataIncomingBatch batch = intent.Batch;
            filter.Add(new ReceiptKey(
                batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId, batch.BatchId));
        }
        return filter;
    }

    internal static bool IsReceiptTransportEpochFenced(
        NamespaceRosterMember senderMember, uint receiptTransportPeerEpoch, uint retiredThroughEpoch)
    {
        ArgumentNullException.ThrowIfNull(senderMember);
        if (receiptTransportPeerEpoch == 0 || receiptTransportPeerEpoch > retiredThroughEpoch)
            return false;
        return senderMember.Status == NamespaceMemberStatus.Retired
            ? receiptTransportPeerEpoch <= senderMember.RetiredThroughEpoch
            : receiptTransportPeerEpoch < senderMember.TransportPeerEpoch;
    }

    /// <summary>
    /// Reclaim transport receipts only when the durable retired-epoch fence and
    /// signed retention frontier cover the receipt, and neither PendingApply nor the
    /// host reference snapshot retains it. Receive cursors/digest chains remain as
    /// the replay fence; peer retirement itself never eagerly deletes receipt rows.
    /// </summary>
    public int CompactReceipts(
        string scopeId, NodeId localReplica, int namespaceId, int limit,
        Func<IDisposable?> acquireGcLease, IApplicationGcReferenceScanner externalReferences)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (limit <= 0 || limit > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        ArgumentNullException.ThrowIfNull(acquireGcLease);
        ArgumentNullException.ThrowIfNull(externalReferences);

        IDisposable? lease = acquireGcLease();
        if (lease is null)
            throw new InvalidOperationException("Cross-store receipt GC proof is unavailable; no receipts were reclaimed.");
        using IDisposable writerAdmission = _writerAdmission.Enter();
        using (lease)
        lock (_gate)
        {
            ThrowIfDisposed();
            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the durable namespace-GC replay guard before receipt GC.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.IsReceiptGcProofEnabled)
                return 0;
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out PartitionState? current))
                return 0;
            if (!gcStore.TryReadNamespaceRoster(namespaceId, trustStore, out NamespaceRosterCertificate? roster) ||
                roster is null)
                throw new InvalidOperationException("No accepted namespace roster is available for receipt GC.");
            if (!gcStore.TryReadRetentionCheckpoint(roster, trustStore,
                    out NamespaceRetentionCheckpoint? retentionCheckpoint) ||
                retentionCheckpoint is null)
                return 0;
            Dictionary<(NodeId Author, uint Epoch), ulong> safeOriginFrontier =
                BuildSafeOriginFrontier(retentionCheckpoint);
            using IApplicationGcReferenceScan externalScan =
                externalReferences.OpenScan(scopeId, localReplica, namespaceId);
            var rosterMembers = roster.Members.ToDictionary(member => member.NodeId);

            var candidates = new List<ReceiptKey>(limit);
            foreach (var pair in EnumerateReceipts(partitionKey, current, namespaceId))
            {
                ReceiptKey key = pair.Key;
                ReceiptValue receipt = pair.Value;
                if (key.NamespaceId != namespaceId || receipt.DurableAuthor.IsZero ||
                    key.Epoch == 0 || receipt.OriginSequence == 0)
                    continue;
                if (!rosterMembers.TryGetValue(key.Sender, out NamespaceRosterMember? senderMember) ||
                    !current.RetiredPeerEpochs.TryGetValue(
                        (namespaceId, key.Sender), out uint retiredThrough) ||
                    !IsReceiptTransportEpochFenced(senderMember, receipt.TransportPeerEpoch, retiredThrough))
                    continue;
                var receiveKey = new ReceiveKey(
                    key.Sender, key.Epoch, key.NamespaceId, receipt.DurableAuthor);
                if (!current.Receives.TryGetValue(receiveKey, out ReceiveState receive) ||
                    receive.CursorEnd < receipt.CursorEnd ||
                    !HasReceiveChainForAuthor(current, receiveKey))
                    continue;
                if (externalScan.HasExternalReceiptReference(
                    key.Sender, key.Epoch, receipt.TransportPeerEpoch, key.BatchId))
                    continue;
                if (!IsOriginCoveredBySafeFrontier(
                        safeOriginFrontier, receipt.DurableAuthor, key.Epoch, receipt.OriginSequence))
                    continue;
                candidates.Add(key);
                if (candidates.Count == limit)
                    break;
            }

            if (candidates.Count == 0)
                return 0;

            // PendingApply history is disk-primary and may be arbitrarily large.
            // Intersect it with this bounded candidate page instead of building a
            // HashSet containing every pending receipt in the namespace.
            var eligibleCandidates = new HashSet<ReceiptKey>(candidates);
            foreach (PendingApplyValue pending in current.PendingApplies.Values)
            {
                MetadataIncomingBatch batch = pending.Batch;
                eligibleCandidates.Remove(new ReceiptKey(
                    batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId, batch.BatchId));
                if (eligibleCandidates.Count == 0)
                    return 0;
            }
            candidates.RemoveAll(key => !eligibleCandidates.Contains(key));

            byte[][] deletes = candidates
                .Select(key => EncodeReceiptKey(partitionKey, key))
                .ToArray();
            CommitPeerMetadataDelta(partitionKey, current,
                Array.Empty<(byte[] Key, byte[] Value)>(), deletes,
                static () => { });
            return candidates.Count;
        }
    }

    /// <summary>
    /// Build a bounded Bloom prefilter from every retained namespace payload.
    /// The caller must hold GC writer admission until candidate approval is committed.
    /// A malformed payload or outstanding immutable peer reservation blocks GC.
    /// </summary>
    internal bool TryCreateLiveRecordReferenceFilter(
        string scopeId, NodeId localReplica, int namespaceId,
        out LiveRecordReferenceFilter filter)
    {
        lock (_gate)
        {
            Action<string, long>? observer = TombstoneGcAllocationObserver;
            long checkpoint = observer is null ? 0 : GC.GetTotalAllocatedBytes(precise: true);
            void Report(string phase)
            {
                if (observer is null) return;
                long current = GC.GetTotalAllocatedBytes(precise: true);
                observer(phase, current - checkpoint);
                checkpoint = current;
            }

            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out PartitionState? state))
            {
                Report("partition-state-load");
                filter = new LiveRecordReferenceFilter(0);
                return true;
            }

            Report("partition-state-load");
            if (state.PeerStreams.Any(pair =>
                pair.Key.NamespaceId == namespaceId &&
                IsPeerStreamGcReservationOutstanding(
                    pair.Value.PendingBatch is not null,
                    pair.Value.PushedCursor,
                    pair.Value.AckedCursor)))
            {
                filter = new LiveRecordReferenceFilter(0);
                return false;
            }

            Report("peer-reservation-check");
            // Monotonic stream high-watermarks upper-bound retained row counts. Use
            // them to size the bounded filter without a separate payload-history scan.
            long expectedItems = 0;
            foreach (PendingApplyValue pending in state.PendingApplies.Values)
                if (pending.Batch.NamespaceId == namespaceId)
                    expectedItems = AddLiveRecordReferenceExpectedItems(
                        expectedItems, (ulong)pending.Batch.Operations.Count);
            foreach (var pair in state.RelayHighWatermarks)
                if (pair.Key.NamespaceId == namespaceId)
                    expectedItems = AddLiveRecordReferenceExpectedItems(expectedItems, pair.Value);
            foreach (var pair in state.OutboundHighWatermarks)
                if (pair.Key.NamespaceId == namespaceId)
                    expectedItems = AddLiveRecordReferenceExpectedItems(expectedItems, pair.Value);

            Report("payload-count-estimate");
            var builtFilter = new LiveRecordReferenceFilter(expectedItems);
            filter = builtFilter;
            bool AddPayload(ReadOnlySpan<byte> payload)
            {
                try
                {
                    builtFilter.Add(ProviderMutationCodec.DecodeRecordId(payload));
                    return true;
                }
                catch (Exception exception) when (exception is InvalidDataException or
                    ArgumentException or EndOfStreamException or IndexOutOfRangeException)
                {
                    return false;
                }
            }

            foreach (PendingApplyValue pending in state.PendingApplies.Values)
            {
                if (pending.Batch.NamespaceId != namespaceId) continue;
                foreach (MetadataOperation operation in pending.Batch.Operations)
                    if (!AddPayload(operation.Payload)) return false;
            }
            var partitionKey = new PartitionKey(scopeId, localReplica);
            byte[] partitionRoot = PartitionRoot(partitionKey);
            using StorageSnapshot payloadSnapshot = _storage.OpenSnapshot();
            if (!VisitRelayStorageRows(payloadSnapshot, partitionRoot, state, namespaceId,
                (key, sequence, encodedPayload) =>
                {
                    _ = DecodePayloadHeader(encodedPayload);
                    return AddPayload(encodedPayload.Slice(20));
                }))
                return false;
            if (!VisitOutboundStorageRows(payloadSnapshot, partitionRoot, state, namespaceId,
                (key, sequence, encodedPayload) =>
                {
                    _ = DecodePayloadHeader(encodedPayload);
                    return AddPayload(encodedPayload.Slice(20));
                }))
                return false;
            Report("bloom-population");
            return true;
        }
    }

    /// <summary>
    /// Find live RecordIds only within the current bounded tombstone candidate page.
    /// Exact retained-payload scans are used only when the Bloom prefilter has a possible hit.
    /// </summary>
    internal bool TryFindLiveRecordReferences(
        string scopeId, NodeId localReplica, int namespaceId,
        IReadOnlyList<ProviderMutation> candidates,
        LiveRecordReferenceFilter referenceFilter,
        out HashSet<RecordId> liveRecords)
    {
        ArgumentNullException.ThrowIfNull(candidates);
        ArgumentNullException.ThrowIfNull(referenceFilter);
        var candidateRecords = new HashSet<RecordId>(candidates.Count);
        foreach (ProviderMutation candidate in candidates)
        {
            ArgumentNullException.ThrowIfNull(candidate);
            if (!candidate.RecordId.IsZero)
                candidateRecords.Add(candidate.RecordId);
        }

        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            var collectedRecords = new HashSet<RecordId>();
            liveRecords = collectedRecords;
            if (candidateRecords.Count == 0 ||
                !TryGetPartition(new PartitionKey(scopeId, localReplica), out PartitionState? state))
                return true;

            if (state.PeerStreams.Any(pair =>
                pair.Key.NamespaceId == namespaceId &&
                IsPeerStreamGcReservationOutstanding(
                    pair.Value.PendingBatch is not null,
                    pair.Value.PushedCursor,
                    pair.Value.AckedCursor)))
                return false;

            bool pageMayContainReference = false;
            foreach (RecordId candidate in candidateRecords)
            {
                if (!referenceFilter.MightContain(candidate)) continue;
                pageMayContainReference = true;
                break;
            }
            if (!pageMayContainReference)
                return true;

            bool ScanPayload(ReadOnlySpan<byte> payload)
            {
                try
                {
                    RecordId recordId = ProviderMutationCodec.DecodeRecordId(payload);
                    if (candidateRecords.Contains(recordId))
                        collectedRecords.Add(recordId);
                    return true;
                }
                catch (Exception exception) when (exception is InvalidDataException or
                    ArgumentException or EndOfStreamException or IndexOutOfRangeException)
                {
                    return false;
                }
            }

            foreach (PendingApplyValue pending in state.PendingApplies.Values)
            {
                if (pending.Batch.NamespaceId != namespaceId) continue;
                foreach (MetadataOperation operation in pending.Batch.Operations)
                    if (!ScanPayload(operation.Payload)) return false;
            }
            var partitionKey = new PartitionKey(scopeId, localReplica);
            byte[] partitionRoot = PartitionRoot(partitionKey);
            using StorageSnapshot payloadSnapshot = _storage.OpenSnapshot();
            if (!VisitRelayStorageRows(payloadSnapshot, partitionRoot, state, namespaceId,
                (key, sequence, encodedPayload) =>
                {
                    _ = DecodePayloadHeader(encodedPayload);
                    return ScanPayload(encodedPayload.Slice(20));
                }))
                return false;
            if (!VisitOutboundStorageRows(payloadSnapshot, partitionRoot, state, namespaceId,
                (key, sequence, encodedPayload) =>
                {
                    _ = DecodePayloadHeader(encodedPayload);
                    return ScanPayload(encodedPayload.Slice(20));
                }))
                return false;
            return true;
        }
    }

    /// <summary>
    /// Read-only namespace-local diagnostics for durable Metadata reservations and
    /// retained relay/outbound payloads. These counts are advisory; compaction still
    /// revalidates exact candidate references under the Metadata transaction gate.
    /// </summary>
    public MetadataGcReservationDiagnostics GetGcReservationDiagnostics(
        string scopeId, NodeId localReplica, int namespaceId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));

        lock (_gate)
        {
            ThrowIfDisposed();
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out PartitionState? state))
                return new MetadataGcReservationDiagnostics(0, 0, 0, 0);

            int pendingApplies = state.PendingApplies.Values.Count(
                pending => pending.Batch.NamespaceId == namespaceId);
            int outstandingPeerReservations = state.PeerStreams.Count(pair =>
                pair.Key.NamespaceId == namespaceId &&
                IsPeerStreamGcReservationOutstanding(
                    pair.Value.PendingBatch is not null,
                    pair.Value.PushedCursor,
                    pair.Value.AckedCursor));
            int relayPayloads = CountRelayRows(
                new PartitionKey(scopeId, localReplica), state, namespaceId);
            int outboundPayloads = CountOutboundRows(
                new PartitionKey(scopeId, localReplica), state, namespaceId);

            return new MetadataGcReservationDiagnostics(
                pendingApplies, outstandingPeerReservations, relayPayloads, outboundPayloads);
        }
    }

    public bool HasLiveRecordReference(
        string scopeId, NodeId localReplica, int namespaceId, RecordId recordId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out PartitionState? state))
                return false;

            if (state.PeerStreams.Any(pair =>
                pair.Key.NamespaceId == namespaceId &&
                IsPeerStreamGcReservationOutstanding(
                    pair.Value.PendingBatch is not null,
                    pair.Value.PushedCursor,
                    pair.Value.AckedCursor)))
                return true;
            if (state.PendingApplies.Values.Any(pending =>
                pending.Batch.NamespaceId == namespaceId &&
                pending.Batch.Operations.Any(operation => PayloadReferencesRecord(operation.Payload, recordId))))
                return true;
            var partitionKey = new PartitionKey(scopeId, localReplica);
            byte[] partitionRoot = PartitionRoot(partitionKey);
            using StorageSnapshot payloadSnapshot = _storage.OpenSnapshot();
            bool foundRelayReference = false;
            VisitRelayStorageRows(payloadSnapshot, partitionRoot, state, namespaceId,
                (key, sequence, encodedPayload) =>
                {
                    _ = DecodePayloadHeader(encodedPayload);
                    foundRelayReference = PayloadReferencesRecord(encodedPayload.Slice(20), recordId);
                    return !foundRelayReference;
                });
            if (foundRelayReference) return true;
            bool foundOutboundReference = false;
            VisitOutboundStorageRows(payloadSnapshot, partitionRoot, state, namespaceId,
                (key, sequence, encodedPayload) =>
                {
                    _ = DecodePayloadHeader(encodedPayload);
                    foundOutboundReference = PayloadReferencesRecord(
                        encodedPayload.Slice(20), recordId);
                    return !foundOutboundReference;
                });
            return foundOutboundReference;
        }
    }

    /// <summary>
    /// Collects every RecordId retained by unresolved Metadata payloads in one namespace.
    /// Returns false when a peer batch reservation is outstanding or a retained payload
    /// cannot be decoded; callers must then refuse physical tombstone reclamation.
    /// </summary>
    public bool TryCollectLiveRecordReferences(
        string scopeId, NodeId localReplica, int namespaceId,
        out HashSet<RecordId> liveRecordReferences)
    {
        liveRecordReferences = new HashSet<RecordId>();
        HashSet<RecordId> collectedReferences = liveRecordReferences;
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out PartitionState? state))
                return true;

            if (state.PeerStreams.Any(pair =>
                pair.Key.NamespaceId == namespaceId &&
                IsPeerStreamGcReservationOutstanding(
                    pair.Value.PendingBatch is not null,
                    pair.Value.PushedCursor,
                    pair.Value.AckedCursor)))
                return false;

            foreach (var pending in state.PendingApplies.Values)
            {
                if (pending.Batch.NamespaceId != namespaceId)
                    continue;
                foreach (var operation in pending.Batch.Operations)
                {
                    if (!TryAddLiveRecordReference(liveRecordReferences, operation.Payload))
                    {
                        liveRecordReferences.Clear();
                        return false;
                    }
                }
            }

            var partitionKey = new PartitionKey(scopeId, localReplica);
            byte[] partitionRoot = PartitionRoot(partitionKey);
            using StorageSnapshot payloadSnapshot = _storage.OpenSnapshot();
            if (!VisitRelayStorageRows(payloadSnapshot, partitionRoot, state, namespaceId,
                (key, sequence, encodedPayload) =>
                {
                    _ = DecodePayloadHeader(encodedPayload);
                    return TryAddLiveRecordReference(collectedReferences, encodedPayload.Slice(20));
                }))
            {
                liveRecordReferences.Clear();
                return false;
            }

            if (!VisitOutboundStorageRows(payloadSnapshot, partitionRoot, state, namespaceId,
                (key, sequence, encodedPayload) =>
                {
                    _ = DecodePayloadHeader(encodedPayload);
                    return TryAddLiveRecordReference(
                        collectedReferences, encodedPayload.Slice(20));
                }))
            {
                liveRecordReferences.Clear();
                return false;
            }

            return true;
        }
    }

    private static bool TryAddLiveRecordReference(
        HashSet<RecordId> liveRecordReferences, ReadOnlySpan<byte> payload)
    {
        try
        {
            liveRecordReferences.Add(ProviderMutationCodec.DecodeRecordId(payload));
            return true;
        }
        catch (Exception exception) when (exception is InvalidDataException or
            ArgumentException or EndOfStreamException or IndexOutOfRangeException)
        {
            // An undecodable retained payload may reference any record; fail closed.
            return false;
        }
    }

    private static long AddLiveRecordReferenceExpectedItems(long current, ulong count)
    {
        const long maximumItems = (16L * 1024 * 1024 * 8) / 32;
        if (current >= maximumItems)
            return maximumItems;
        ulong remaining = (ulong)(maximumItems - current);
        return count >= remaining ? maximumItems : current + (long)count;
    }

    private static bool PayloadReferencesRecord(ReadOnlySpan<byte> payload, RecordId recordId)
    {
        try
        {
            return ProviderMutationCodec.DecodeRecordId(payload) == recordId;
        }
        catch (Exception exception) when (exception is InvalidDataException or
            ArgumentException or EndOfStreamException or IndexOutOfRangeException)
        {
            // Unknown/corrupt retained payloads are live until repaired or explicitly
            // acknowledged; never let a decoder failure authorize physical GC.
            return true;
        }
    }


    public bool SupportsRestartDurability => true;

    public MetadataBatchReceipt AcceptIncomingBatch(MetadataIncomingBatch batch)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(batch);
        ValidateBatch(batch);

        var partitionKey = new PartitionKey(batch.ScopeId, batch.LocalReplica);
        var current = GetOrCreatePartition(partitionKey);
        ValidateIncomingTransportPeerEpoch(current, batch);
        RejectForgottenOrigins(current, batch);
        _ = BuildPendingApplyDecisions(partitionKey, current, batch, Guid.Empty);
        return AcceptIncomingBatchDelta(partitionKey, current, batch);
    
        }
    }

    public MetadataPendingApplyIntent BeginPendingApply(MetadataIncomingBatch batch, Guid applyToken)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
        ArgumentNullException.ThrowIfNull(batch);
        ValidateBatch(batch);
        if (applyToken == Guid.Empty)
            throw new ArgumentException("ApplyToken must be non-empty.", nameof(applyToken));

        var partitionKey = new PartitionKey(batch.ScopeId, batch.LocalReplica);
        var current = GetOrCreatePartition(partitionKey);
        ValidateIncomingTransportPeerEpoch(current, batch);
        var receiptKey = new ReceiptKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, batch.BatchId);
        bool hasPriorReceipt = TryGetReceipt(partitionKey, current, receiptKey, out var priorReceipt);
        if (hasPriorReceipt && !priorReceipt!.Matches(batch))
            throw new InvalidDataException("Incoming batch receipt identity was reused with conflicting metadata.");
        RejectForgottenOrigins(current, batch);

        if (TryGetCompletedPendingApply(partitionKey, current, applyToken, out var completed))
        {
            if (!completed.BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest))
                throw new InvalidDataException("Completed apply token was reused for different input.");
            throw new MetadataPendingApplyAlreadyFinalizedException();
        }

        if (current.PendingApplies.TryGetValue(applyToken, out var pending))
        {
            if (!SamePendingBatch(pending.Batch, batch))
                throw new InvalidDataException("Pending apply token was reused for different input.");
            return new MetadataPendingApplyIntent(applyToken, CloneBatch(pending.Batch))
            {
                IsDuplicate = hasPriorReceipt,
                ApplyOperations = BuildPendingApplyDecisions(partitionKey, current, pending.Batch, applyToken)
            };
        }

        bool isDuplicate = ValidateIncomingBatchForStaging(partitionKey, current, batch, applyToken);
        IReadOnlyList<bool> applyOperations = BuildPendingApplyDecisions(partitionKey, current, batch, applyToken);
        var pendingValue = new PendingApplyValue(CloneBatch(batch));
        PersistPendingApply(partitionKey, current, applyToken, pendingValue);
        return new MetadataPendingApplyIntent(applyToken, CloneBatch(batch))
        {
            IsDuplicate = isDuplicate,
            ApplyOperations = applyOperations
        };
    
        }
    }

    public IReadOnlyList<MetadataPendingApplyIntent> ReadPendingApplyIntents(
        string scopeId,
        NodeId localReplica)
        => EnumeratePendingApplyIntents(scopeId, localReplica).ToArray();

    /// <summary>
    /// Stream durable PendingApply recovery intents one at a time. The storage cursor
    /// pages the underlying rows; only one decoded batch and its operation mask are
    /// materialized for the caller at a time.
    /// </summary>
    public IEnumerable<MetadataPendingApplyIntent> EnumeratePendingApplyIntents(
        string scopeId,
        NodeId localReplica)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));

        var partitionKey = new PartitionKey(scopeId, localReplica);
        foreach (KeyValuePair<Guid, PendingApplyValue> row in EnumeratePendingApplies(partitionKey))
        {
            MetadataPendingApplyIntent intent;
            lock (_gate)
            {
                ThrowIfDisposed();
                if (!TryGetPartition(partitionKey, out PartitionState? state) ||
                    !state.PendingApplies.TryGetValue(row.Key, out PendingApplyValue pending))
                    continue;

                MetadataIncomingBatch batch = pending.Batch;
                intent = new MetadataPendingApplyIntent(row.Key, CloneBatch(batch))
                {
                    IsDuplicate = TryGetReceipt(partitionKey, state, new ReceiptKey(
                        batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId, batch.BatchId), out _),
                    ApplyOperations = BuildPendingApplyDecisions(
                        partitionKey, state, batch, row.Key)
                };
            }
            yield return intent;
        }
    }

    /// <summary>
    /// Hold the durable Metadata transaction gate across provider tombstone reclamation.
    /// The provider must capture live PendingApply/relay/outbound references and finish
    /// its physical delete while this lease is held; otherwise a direct Metadata writer
    /// could add a new reference after the scan but before the provider transaction.
    /// Lock order is coordinator → Node → host → Metadata → provider.
    /// </summary>
    public IDisposable AcquireTombstoneGcCommitGuard(
        string scopeId, NodeId localReplica, int namespaceId,
        StorageNamespaceGcStore gcStore, TrustStore trustStore)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if ((uint)namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        ArgumentNullException.ThrowIfNull(gcStore);
        ArgumentNullException.ThrowIfNull(trustStore);

        IDisposable writerAdmission = _writerAdmission.Enter();
        Monitor.Enter(_gate);
        try
        {
            ThrowIfDisposed();
            if (!ReferenceEquals(_namespaceGcStore, gcStore) ||
                !ReferenceEquals(_namespaceGcTrustStore, trustStore))
                throw new InvalidOperationException(
                    "Tombstone GC requires the exact namespace-GC store and TrustStore attached to this Metadata Store.");
            if (!gcStore.IsBoundTo(_storage, scopeId, localReplica))
                throw new InvalidOperationException(
                    "Tombstone GC namespace state is not bound to the exact Metadata Store scope and local replica.");

            // Exact candidate lookups are performed under this held transaction and
            // writer-admission lease. Do not materialize every live RecordId in the
            // namespace merely to authorize a bounded/streamed Tombstone sweep.
            return new FullReplaceCommitLease(_gate, writerAdmission);
        }
        catch
        {
            Monitor.Exit(_gate);
            writerAdmission.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Hold the durable metadata transaction gate across FULL_REPLACE and fail closed
    /// while the namespace has an unresolved receive apply or outbound/relay reservation.
    /// Completed receipts are deliberately retained: they are idempotency history, not
    /// pending provider work and do not pin the replaced provider generation.
    /// </summary>
    public IDisposable AcquireFullReplaceCommitGuard(
        string scopeId, NodeId localReplica, int namespaceId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if ((uint)namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));

        IDisposable writerAdmission = _writerAdmission.Enter();
        Monitor.Enter(_gate);
        try
        {
            ThrowIfDisposed();
            if (_namespaceGcStore is null || _namespaceGcTrustStore is null)
                throw new InvalidOperationException(
                    "FULL_REPLACE requires the Metadata Store to be attached to its shared durable namespace-GC store.");
            if (!_namespaceGcStore.IsBoundTo(_storage, scopeId, localReplica))
                throw new InvalidOperationException(
                    "FULL_REPLACE namespace-GC store is not bound to the exact Metadata Store scope and local replica.");

            if (TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
            {
                int pendingApplies = state.PendingApplies.Values.Count(
                    pending => pending.Batch.NamespaceId == namespaceId);
                int pendingReservations = state.PeerStreams.Count(pair =>
                    pair.Key.NamespaceId == namespaceId &&
                    (pair.Value.PendingBatch is not null ||
                     pair.Value.PushedCursor != pair.Value.AckedCursor));

                if (pendingApplies != 0)
                    throw new InvalidOperationException(
                        $"FULL_REPLACE is blocked by {pendingApplies} unresolved Metadata PendingApply intent(s) in the target namespace.");
                if (pendingReservations != 0)
                    throw new InvalidOperationException(
                        $"FULL_REPLACE is blocked by {pendingReservations} unresolved outbound/relay reservation(s) in the target namespace.");
            }

            return new FullReplaceCommitLease(_gate, writerAdmission);
        }
        catch
        {
            Monitor.Exit(_gate);
            writerAdmission.Dispose();
            throw;
        }
    }

    public MetadataBatchReceipt FinalizePendingApply(
        string scopeId,
        NodeId localReplica,
        MetadataProviderApplyReceipt providerReceipt)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
            ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(providerReceipt);
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (providerReceipt.ApplyToken == Guid.Empty)
            throw new ArgumentException("ApplyToken must be non-empty.", nameof(providerReceipt));
        ValidateDigest(providerReceipt.BatchDigest, nameof(providerReceipt.BatchDigest));

        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!TryGetPartition(partitionKey, out var current))
            throw new InvalidOperationException("Pending apply intent does not exist.");

        if (TryGetCompletedPendingApply(partitionKey, current, providerReceipt.ApplyToken, out var completed))
        {
            if (!completed.BatchDigest.AsSpan().SequenceEqual(providerReceipt.BatchDigest))
                throw new InvalidDataException("Provider receipt conflicts with the completed apply token.");
            return CloneReceipt(completed.Receipt, isDuplicate: true);
        }

        if (!current.PendingApplies.TryGetValue(providerReceipt.ApplyToken, out var pending))
            throw new InvalidOperationException("Pending apply intent does not exist.");
        if (!pending.Batch.BatchDigest.AsSpan().SequenceEqual(providerReceipt.BatchDigest))
            throw new InvalidDataException("Provider receipt does not match the pending apply input.");
        RejectForgottenOrigins(current, pending.Batch);

        // Finalization is a hot durability boundary. Do not clone and re-encode the
        // entire partition here: a single incoming batch must write only its receipt,
        // cursor/digest, new ledger/relay rows and PendingApply transition.
        return FinalizePendingApplyDelta(partitionKey, current,
            providerReceipt.ApplyToken, pending.Batch, providerReceipt.BatchDigest);
    
        }
    }

    public IMetadataStoreSnapshot OpenSnapshot(string scopeId, NodeId localReplica)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));

        var partitionKey = new PartitionKey(scopeId, localReplica);
        // Pin the immutable Storage generation; outbound rows are read from it
        // page-by-page rather than copied into the volatile snapshot.
        StorageSnapshot outboundSnapshot = _storage.OpenSnapshot();
        try
        {
            var state = GetOrCreatePartition(partitionKey).CloneForSnapshot();
                return new VolatileSnapshot(this, partitionKey, state, outboundSnapshot);
        }
        catch
        {
            outboundSnapshot.Dispose();
            throw;
        }
    
        }
    }


    private MetadataBatchReceipt FinalizePendingApplyDelta(
        PartitionKey partitionKey, PartitionState current, Guid applyToken,
        MetadataIncomingBatch incoming, byte[] providerDigest)
    {
        var receiptKey = new ReceiptKey(incoming.SenderPeer, incoming.SenderEpoch,
            incoming.NamespaceId, incoming.BatchId);
        MetadataBatchReceipt receipt;
        bool isNewReceipt = !TryGetReceipt(partitionKey, current, receiptKey, out var priorReceipt);
        ReceiptValue? refreshedReceipt = null;
        var newLedger = new List<(LedgerKey Key, ChangeLedgerValue Value)>();
        var newRelay = new List<(RelayKey Key, MetadataRelayItem Item)>();
        var relayHigh = new Dictionary<RelayKey, ulong>();
        var newPeerStreams = new Dictionary<PeerStreamKey, PeerStreamStateValue>();
        var newRequiredStreams = new HashSet<PeerStreamKey>();
        ReceiveChainKey chainKey = default;
        LegacyReceiveChainKey? legacyChainKey = null;
        ReceiveChainKey? legacyTargetKey = null;
        ReceiveChainState? legacyTargetState = null;

        if (!isNewReceipt)
        {
            if (!priorReceipt!.Matches(incoming))
                throw new InvalidDataException("Incoming batch receipt identity was reused with conflicting metadata.");
            receipt = new MetadataBatchReceipt(priorReceipt.CursorStart, priorReceipt.CursorEnd,
                priorReceipt.BatchDigest.ToArray(), priorReceipt.NewRelayOperationCount, IsDuplicate: true)
            { DurableAuthor = priorReceipt.DurableAuthor, HeaderFingerprint = priorReceipt.HeaderFingerprint.ToArray() };
            if (incoming.TransportPeerEpoch > priorReceipt.TransportPeerEpoch)
                refreshedReceipt = new ReceiptValue(priorReceipt.CursorStart, priorReceipt.CursorEnd,
                    priorReceipt.PreviousBatchDigest.ToArray(), priorReceipt.BatchDigest.ToArray(),
                    priorReceipt.NewRelayOperationCount, priorReceipt.DurableAuthor,
                    priorReceipt.HeaderFingerprint.ToArray(), incoming.TransportPeerEpoch, priorReceipt.OriginSequence);
        }
        else
        {
            var receiveKey = new ReceiveKey(incoming.SenderPeer, incoming.SenderEpoch,
                incoming.NamespaceId, ResolveAuthorStream(incoming));
            var receive = current.Receives.TryGetValue(receiveKey, out var priorReceive)
                ? priorReceive : new ReceiveState(0);
            chainKey = new ReceiveChainKey(incoming.SenderPeer, incoming.SenderEpoch,
                incoming.NamespaceId, receiveKey.AuthorStream);
            var chain = GetReceiveChainStateForAuthor(current, receiveKey,
                out legacyChainKey, out legacyTargetKey, out legacyTargetState);
            if (incoming.CursorStart != checked(receive.CursorEnd + 1) ||
                !chain.LastBatchDigest.AsSpan().SequenceEqual(incoming.PreviousBatchDigest))
                throw new InvalidDataException("Incoming batch does not continue its author cursor and author-stream digest chain.");

            int relayAdded = 0;
            var interestedPeers = current.InterestedPeers
                .Where(peer => peer.NamespaceId == incoming.NamespaceId)
                .Select(peer => (peer.Peer, peer.PeerEpoch))
                .Concat(current.RequiredPeerStreams
                    .Where(peer => peer.NamespaceId == incoming.NamespaceId)
                    .Select(peer => (peer.Peer, peer.PeerEpoch)))
                .Distinct()
                .ToArray();
            foreach (var operation in incoming.Operations)
            {
                var ledgerKey = new LedgerKey(incoming.NamespaceId, operation.ChangeId);
                if (TryGetChangeLedger(partitionKey, current, ledgerKey, out var priorLedger))
                {
                    if (!LedgerMatches(priorLedger, operation))
                        throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint.");
                    continue;
                }
                if (current.OutboundHighWatermarks.ContainsKey(
                    new OutboundKey(incoming.NamespaceId, operation.AuthorNodeId)))
                    throw new InvalidDataException("An author stream cannot be populated by both local outbound and incoming relay operations.");
                if (!operation.RelayEligible)
                {
                    newLedger.Add((ledgerKey, new ChangeLedgerValue(operation.Fingerprint.ToArray(), false,
                        operation.AuthorNodeId, operation.OriginAuthorEpoch, operation.OriginSequence)));
                    continue;
                }

                var relayKey = new RelayKey(incoming.NamespaceId, operation.AuthorNodeId);
                ulong high = relayHigh.TryGetValue(relayKey, out var stagedHigh)
                    ? stagedHigh
                    : current.RelayHighWatermarks.TryGetValue(relayKey, out var savedHigh) ? savedHigh : 0;
                ulong sequence = checked(high + 1);
                relayHigh[relayKey] = sequence;
                newLedger.Add((ledgerKey, new ChangeLedgerValue(operation.Fingerprint.ToArray(), false,
                    operation.AuthorNodeId, operation.OriginAuthorEpoch, operation.OriginSequence,
                    StreamSequence: sequence)));
                newRelay.Add((relayKey, new MetadataRelayItem(
                    sequence, operation.AuthorNodeId, operation.ChangeId, operation.Payload.ToArray())
                { Provenance = operation.Provenance }));
                relayAdded++;

                foreach (var peer in interestedPeers)
                {
                    if (peer.Peer == operation.AuthorNodeId) continue;
                    var streamKey = new PeerStreamKey(peer.Peer, peer.PeerEpoch,
                        incoming.NamespaceId, operation.AuthorNodeId);
                    if (!current.PeerStreams.ContainsKey(streamKey) &&
                        !newPeerStreams.ContainsKey(streamKey))
                        newPeerStreams.Add(streamKey, new PeerStreamStateValue(
                            0, 0, 0, incoming.SenderEpoch, new byte[16], null));
                    if (!current.RequiredPeerStreams.Contains(streamKey))
                        newRequiredStreams.Add(streamKey);
                }
            }

            receipt = new MetadataBatchReceipt(incoming.CursorStart, incoming.CursorEnd,
                incoming.BatchDigest.ToArray(), relayAdded, IsDuplicate: false)
            { DurableAuthor = ResolveAuthorStream(incoming), HeaderFingerprint = incoming.HeaderFingerprint.ToArray() };
        }

        byte[] root = PartitionRoot(partitionKey);
        byte[] pendingKey = StorageKey(root, 12, applyToken.ToByteArray());
        byte[] completedKey = StorageKey(root, 13, applyToken.ToByteArray());
        byte[] completedValue = EncodeCompleted(new CompletedPendingApplyValue(
            providerDigest.ToArray(), CloneReceipt(receipt, receipt.IsDuplicate)));
        byte[]? identityKey = current.IsPersisted ? null : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null ? null : EncodePartitionIdentity(partitionKey);

        var writes = new List<(byte[] Key, byte[] Value)>();
        var deletes = new List<byte[]> { pendingKey };
        if (identityKey is not null) writes.Add((identityKey, identityValue!));
        writes.Add((completedKey, completedValue));

        if (isNewReceipt || refreshedReceipt is not null)
        {
            byte[] receiptSuffix = new byte[38];
            incoming.SenderPeer.WriteTo(receiptSuffix);
            BinaryPrimitives.WriteUInt32LittleEndian(receiptSuffix.AsSpan(16), incoming.SenderEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(receiptSuffix.AsSpan(20), checked((ushort)incoming.NamespaceId));
            incoming.BatchId.WriteTo(receiptSuffix.AsSpan(22));
            var receiptValueState = refreshedReceipt ?? new ReceiptValue(incoming.CursorStart, incoming.CursorEnd,
                incoming.PreviousBatchDigest.ToArray(), incoming.BatchDigest.ToArray(),
                receipt.NewRelayOperationCount, receipt.DurableAuthor,
                incoming.HeaderFingerprint.ToArray(), incoming.TransportPeerEpoch, GetMaxOriginSequence(incoming));
            writes.Add((StorageKey(root, 1, receiptSuffix), EncodeReceiptValue(receiptValueState)));

            if (isNewReceipt)
            {
            byte[] receiveSuffix = new byte[38];
            incoming.SenderPeer.WriteTo(receiveSuffix);
            BinaryPrimitives.WriteUInt32LittleEndian(receiveSuffix.AsSpan(16), incoming.SenderEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(receiveSuffix.AsSpan(20), checked((ushort)incoming.NamespaceId));
            ResolveAuthorStream(incoming).WriteTo(receiveSuffix.AsSpan(22));
            byte[] receiveValue = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(receiveValue, incoming.CursorEnd);
            writes.Add((StorageKey(root, 2, receiveSuffix), receiveValue));

            writes.Add((EncodeReceiveChainKey(partitionKey, chainKey), incoming.BatchDigest.ToArray()));
            if (legacyChainKey is { } legacyToDelete)
            {
                deletes.Add(EncodeLegacyReceiveChainKey(partitionKey, legacyToDelete));
                if (legacyTargetKey is { } legacyTarget && legacyTarget != chainKey &&
                    legacyTargetState is { } legacyState)
                    writes.Add((EncodeReceiveChainKey(partitionKey, legacyTarget),
                        legacyState.LastBatchDigest.ToArray()));
            }
            }
        }

        foreach (var item in newLedger)
        {
            byte[] suffix = new byte[18];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)item.Key.NamespaceId));
            item.Key.ChangeId.WriteTo(suffix.AsSpan(2));
            byte[] value = EncodeChangeLedgerValue(item.Value);
            writes.Add((StorageKey(root, 3, suffix), value));
        }

        foreach (var item in newRelay)
        {
            byte[] suffix = new byte[26];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)item.Key.NamespaceId));
            item.Key.Author.WriteTo(suffix.AsSpan(2));
            BinaryPrimitives.WriteUInt64BigEndian(suffix.AsSpan(18), item.Item.Sequence);
            writes.Add((StorageKey(root, 20, suffix), EncodePayloadItem(item.Item.ChangeId, item.Item.Payload)));
        }

        foreach (var pair in relayHigh)
        {
            byte[] value = new byte[16];
            if (current.RelayAck.TryGetValue(pair.Key, out ulong ack))
                BinaryPrimitives.WriteUInt64LittleEndian(value, ack);
            BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), pair.Value);
            writes.Add((StorageKey(root, 5, KeyNamespaceAuthor(pair.Key.NamespaceId, pair.Key.Author)), value));
        }

        foreach (var pair in newPeerStreams)
        {
            byte[] suffix = new byte[38];
            pair.Key.Peer.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), pair.Key.PeerEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)pair.Key.NamespaceId));
            pair.Key.AuthorStream.WriteTo(suffix.AsSpan(22));
            writes.Add((StorageKey(root, 15, suffix), EncodePeerStreamState(pair.Value)));
        }
        foreach (var key in newRequiredStreams)
        {
            byte[] suffix = new byte[38];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)key.NamespaceId));
            key.AuthorStream.WriteTo(suffix.AsSpan(2));
            key.Peer.WriteTo(suffix.AsSpan(18));
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(34), key.PeerEpoch);
            writes.Add((StorageKey(root, 16, suffix), Array.Empty<byte>()));
        }

        using (var storageBatch = _storage.BeginWriteBatch())
        {
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != current.StorageCommitSeq)
                throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
            foreach (var write in writes) storageBatch.Put(write.Key, write.Value);
            foreach (var delete in deletes) storageBatch.Delete(delete);
            CommitPartitionBatch(storageBatch, partitionKey, current);
        }

        // PendingApply deletion and the completed-apply marker are already durable
        // in this batch. CommitPartitionBatch has published the new revision; no
        // resident dictionary mutation or duplicate commit-sequence update is needed.
        return receipt;
    }

    private static void ValidateIncomingTransportPeerEpoch(PartitionState state, MetadataIncomingBatch batch)
    {
        bool hasRetiredFence = state.RetiredPeerEpochs.TryGetValue(
            (batch.NamespaceId, batch.SenderPeer), out uint retiredThrough);
        if (batch.TransportPeerEpoch == 0)
        {
            if (hasRetiredFence)
                throw new InvalidDataException(
                    "Incoming batch has no verifiable transport epoch after a peer retirement fence.");
            return;
        }

        if (hasRetiredFence && batch.TransportPeerEpoch <= retiredThrough)
            throw new InvalidDataException("Incoming batch belongs to a retired transport peer epoch.");

        var registeredPeer = new InterestedPeerKey(
            batch.NamespaceId, batch.SenderPeer, batch.TransportPeerEpoch);
        if (!state.InterestedPeers.Contains(registeredPeer))
            throw new InvalidDataException(
                "Incoming batch transport epoch was not durably registered for this namespace.");
    }

    private IReadOnlyList<bool> BuildPendingApplyDecisions(
        PartitionKey partitionKey, PartitionState state, MetadataIncomingBatch batch, Guid applyToken)
    {
        var decisions = new bool[batch.Operations.Count];
        var receiptKey = new ReceiptKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, batch.BatchId);
        if (TryGetReceipt(partitionKey, state, receiptKey, out _))
            return Array.AsReadOnly(decisions);

        // Index only the incoming batch (bounded by the transport batch limit),
        // then stream pending intents once. Do not allocate by retained history size.
        var incomingIdentities = new Dictionary<LedgerKey, MetadataOperation>(batch.Operations.Count);
        foreach (MetadataOperation operation in batch.Operations)
            incomingIdentities.TryAdd(new LedgerKey(batch.NamespaceId, operation.ChangeId), operation);

        foreach (var otherPending in state.PendingApplies)
        {
            if (otherPending.Key == applyToken ||
                otherPending.Value.Batch.NamespaceId != batch.NamespaceId)
                continue;
            foreach (MetadataOperation pendingOperation in otherPending.Value.Batch.Operations)
            {
                var key = new LedgerKey(batch.NamespaceId, pendingOperation.ChangeId);
                if (!incomingIdentities.TryGetValue(key, out MetadataOperation? operation))
                    continue;
                if (!SameOperationIdentity(operation, pendingOperation))
                    throw new InvalidDataException("Pending ChangeId was reused with different fingerprint or Origin provenance.");
                throw new InvalidOperationException("ChangeId is reserved by another pending apply and must be retried after it finalizes.");
            }
        }

        for (int i = 0; i < batch.Operations.Count; i++)
        {
            var operation = batch.Operations[i];
            var key = new LedgerKey(batch.NamespaceId, operation.ChangeId);
            if (TryGetChangeLedger(partitionKey, state, key, out var prior))
            {
                if (!LedgerMatches(prior, operation))
                    throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint.");
                decisions[i] = false;
            }
            else
            {
                decisions[i] = true;
            }
        }
        return Array.AsReadOnly(decisions);
    }

        private static bool SameOperationIdentity(MetadataOperation left, MetadataOperation right)
        => left.Provenance == right.Provenance
            && left.Fingerprint.AsSpan().SequenceEqual(right.Fingerprint);

private bool ValidateIncomingBatchForStaging(
        PartitionKey partitionKey, PartitionState state, MetadataIncomingBatch batch, Guid applyToken)
    {
        var receiptKey = new ReceiptKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, batch.BatchId);
        if (TryGetReceipt(partitionKey, state, receiptKey, out var receipt))
        {
            if (!receipt.Matches(batch))
                throw new InvalidDataException("Incoming batch receipt identity was reused with conflicting metadata.");
            return true;
        }

        var receiveKey = new ReceiveKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, ResolveAuthorStream(batch));
        foreach (var pending in state.PendingApplies)
        {
            if (pending.Key == applyToken) continue;
            var other = pending.Value.Batch;
            if (other.SenderPeer == receiveKey.Sender &&
                other.SenderEpoch == receiveKey.Epoch &&
                other.NamespaceId == receiveKey.NamespaceId)
                throw new InvalidOperationException("An earlier batch for this sender session namespace is still pending apply.");
        }

        var receive = state.Receives.TryGetValue(receiveKey, out var priorReceive)
            ? priorReceive
            : new ReceiveState(0);
        var chain = GetReceiveChainStateForAuthor(state, receiveKey, out _, out _, out _);
        if (batch.CursorStart != checked(receive.CursorEnd + 1) ||
            !chain.LastBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest))
            throw new InvalidDataException("Incoming batch does not continue its author cursor and author-stream digest chain.");

        // Index the bounded incoming batch, then stream retained pending operations.
        // The previous pendingIdentities map grew with the complete unresolved history.
        var batchIdentities = new Dictionary<LedgerKey, MetadataOperation>(batch.Operations.Count);
        foreach (var operation in batch.Operations)
        {
            var key = new LedgerKey(batch.NamespaceId, operation.ChangeId);
            if (batchIdentities.TryGetValue(key, out var inBatchOperation))
            {
                if (!SameOperationIdentity(inBatchOperation, operation))
                    throw new InvalidDataException("ChangeId was reused with different fingerprint or Origin provenance within one incoming batch.");
            }
            else
            {
                batchIdentities.Add(key, operation);
            }

            bool hasLedgerIdentity = TryGetChangeLedger(partitionKey, state, key, out var priorLedger);
            if (hasLedgerIdentity && !LedgerMatches(priorLedger!, operation))
                throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint.");
            if (!hasLedgerIdentity &&
                state.OutboundHighWatermarks.ContainsKey(new OutboundKey(batch.NamespaceId, operation.AuthorNodeId)))
                throw new InvalidDataException("An author stream cannot be populated by both local outbound and incoming relay operations.");
        }

        // Keep exact conflict checks, but allocate only for the incoming batch.
        foreach (PendingApplyValue otherPending in state.PendingApplies.Values)
        {
            if (otherPending.Batch.NamespaceId != batch.NamespaceId)
                continue;
            foreach (MetadataOperation pendingOperation in otherPending.Batch.Operations)
            {
                var key = new LedgerKey(batch.NamespaceId, pendingOperation.ChangeId);
                if (!batchIdentities.TryGetValue(key, out MetadataOperation? operation))
                    continue;
                if (!SameOperationIdentity(operation, pendingOperation))
                    throw new InvalidDataException("Pending ChangeId was reused with different fingerprint or Origin provenance.");
                throw new InvalidOperationException("ChangeId is reserved by another pending apply and must be retried after it finalizes.");
            }
        }
        return false;
    }

    private MetadataBatchReceipt AcceptIncomingBatchDelta(
        PartitionKey partitionKey, PartitionState current, MetadataIncomingBatch batch)
    {
        var receiptKey = new ReceiptKey(batch.SenderPeer, batch.SenderEpoch, batch.NamespaceId, batch.BatchId);
        if (TryGetReceipt(partitionKey, current, receiptKey, out var priorReceipt))
        {
            if (!priorReceipt.Matches(batch))
                throw new InvalidDataException("Incoming batch receipt identity was reused with conflicting metadata.");
            RefreshReceiptTransportEpoch(partitionKey, current, receiptKey, priorReceipt,
                batch.TransportPeerEpoch);
            return new MetadataBatchReceipt(priorReceipt.CursorStart, priorReceipt.CursorEnd,
                priorReceipt.BatchDigest.ToArray(), priorReceipt.NewRelayOperationCount, IsDuplicate: true)
            {
                DurableAuthor = priorReceipt.DurableAuthor,
                HeaderFingerprint = priorReceipt.HeaderFingerprint.ToArray()
            };
        }

        var receiveKey = new ReceiveKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, ResolveAuthorStream(batch));
        var receive = current.Receives.TryGetValue(receiveKey, out var priorReceive)
            ? priorReceive
            : new ReceiveState(0);
        var chainKey = new ReceiveChainKey(batch.SenderPeer, batch.SenderEpoch,
            batch.NamespaceId, receiveKey.AuthorStream);
        var chain = GetReceiveChainStateForAuthor(current, receiveKey,
            out var legacyChainKey, out var legacyTargetKey, out var legacyTargetState);
        if (batch.CursorStart != checked(receive.CursorEnd + 1) ||
            !chain.LastBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest))
            throw new InvalidDataException("Incoming batch does not continue its author cursor and author-stream digest chain.");

        var newLedger = new Dictionary<LedgerKey, ChangeLedgerValue>();
        var newRelay = new Dictionary<RelayKey, List<MetadataRelayItem>>();
        var relayHigh = new Dictionary<RelayKey, ulong>();
        var peerStreamStates = new Dictionary<PeerStreamKey, PeerStreamStateValue>();
        var requiredStreams = new HashSet<PeerStreamKey>();
        var registeredAuthors = new HashSet<NodeId>();
        int relayAdded = 0;

        foreach (var operation in batch.Operations)
        {
            var ledgerKey = new LedgerKey(batch.NamespaceId, operation.ChangeId);
            if (TryGetChangeLedger(partitionKey, current, ledgerKey, out var priorLedger))
            {
                if (!LedgerMatches(priorLedger, operation))
                    throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint.");
                continue;
            }
            if (newLedger.TryGetValue(ledgerKey, out var stagedLedger))
            {
                if (!LedgerMatches(stagedLedger, operation))
                    throw new InvalidDataException("ChangeId was reused with a different canonical fingerprint within one incoming batch.");
                continue;
            }
            if (current.OutboundHighWatermarks.ContainsKey(
                new OutboundKey(batch.NamespaceId, operation.AuthorNodeId)))
                throw new InvalidDataException("An author stream cannot be populated by both local outbound and incoming relay operations.");

            if (!operation.RelayEligible)
            {
                newLedger.Add(ledgerKey, new ChangeLedgerValue(operation.Fingerprint.ToArray(), false,
                    operation.AuthorNodeId, operation.OriginAuthorEpoch, operation.OriginSequence));
                continue;
            }

            var relayKey = new RelayKey(batch.NamespaceId, operation.AuthorNodeId);
            ulong previousHigh = relayHigh.TryGetValue(relayKey, out ulong stagedHigh)
                ? stagedHigh
                : current.RelayHighWatermarks.TryGetValue(relayKey, out ulong currentHigh) ? currentHigh : 0;
            ulong sequence = checked(previousHigh + 1);
            if (!newRelay.TryGetValue(relayKey, out var relayItems))
                newRelay.Add(relayKey, relayItems = new List<MetadataRelayItem>());
            newLedger.Add(ledgerKey, new ChangeLedgerValue(operation.Fingerprint.ToArray(), false,
                operation.AuthorNodeId, operation.OriginAuthorEpoch, operation.OriginSequence,
                StreamSequence: sequence));
            relayItems.Add(new MetadataRelayItem(
                sequence, operation.AuthorNodeId, operation.ChangeId, operation.Payload.ToArray())
            { Provenance = operation.Provenance });
            relayHigh[relayKey] = sequence;
            relayAdded++;

            if (registeredAuthors.Add(operation.AuthorNodeId))
            {
                var peers = current.InterestedPeers.Where(peer => peer.NamespaceId == batch.NamespaceId)
                    .Select(peer => (peer.Peer, peer.PeerEpoch))
                    .Concat(current.RequiredPeerStreams.Where(peer => peer.NamespaceId == batch.NamespaceId)
                        .Select(peer => (peer.Peer, peer.PeerEpoch)))
                    .Distinct();
                foreach (var peer in peers.Where(peer => peer.Peer != operation.AuthorNodeId))
                {
                    var streamKey = new PeerStreamKey(peer.Peer, peer.PeerEpoch,
                        batch.NamespaceId, operation.AuthorNodeId);
                    if (!current.PeerStreams.ContainsKey(streamKey) && !peerStreamStates.ContainsKey(streamKey))
                        peerStreamStates.Add(streamKey,
                            EmptyPeerStreamValue() with { IssuerEpoch = batch.SenderEpoch });
                    if (!current.RequiredPeerStreams.Contains(streamKey))
                        requiredStreams.Add(streamKey);
                }
            }
        }

        var receipt = new MetadataBatchReceipt(batch.CursorStart, batch.CursorEnd,
            batch.BatchDigest.ToArray(), relayAdded, IsDuplicate: false)
        { DurableAuthor = ResolveAuthorStream(batch), HeaderFingerprint = batch.HeaderFingerprint.ToArray() };
        byte[] root = PartitionRoot(partitionKey);
        byte[] receiptSuffix = new byte[38];
        batch.SenderPeer.WriteTo(receiptSuffix);
        BinaryPrimitives.WriteUInt32LittleEndian(receiptSuffix.AsSpan(16), batch.SenderEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(receiptSuffix.AsSpan(20), checked((ushort)batch.NamespaceId));
        batch.BatchId.WriteTo(receiptSuffix.AsSpan(22));
        var receiptValueState = new ReceiptValue(batch.CursorStart, batch.CursorEnd,
            batch.PreviousBatchDigest.ToArray(), batch.BatchDigest.ToArray(),
            relayAdded, receipt.DurableAuthor, batch.HeaderFingerprint.ToArray(),
            batch.TransportPeerEpoch, GetMaxOriginSequence(batch));
        byte[] receiptValue = EncodeReceiptValue(receiptValueState);

        byte[] receiveSuffix = new byte[38];
        batch.SenderPeer.WriteTo(receiveSuffix);
        BinaryPrimitives.WriteUInt32LittleEndian(receiveSuffix.AsSpan(16), batch.SenderEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(receiveSuffix.AsSpan(20), checked((ushort)batch.NamespaceId));
        receiveKey.AuthorStream.WriteTo(receiveSuffix.AsSpan(22));
        byte[] receiveValue = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(receiveValue, batch.CursorEnd);

        var chainStorageKey = EncodeReceiveChainKey(partitionKey, chainKey);
        byte[]? legacyChainDeleteKey = legacyChainKey is { } legacyToDelete
            ? EncodeLegacyReceiveChainKey(partitionKey, legacyToDelete) : null;

        var writes = new List<(byte[] Key, byte[] Value)>(
            3 + newLedger.Count + newRelay.Values.Sum(items => items.Count) + relayHigh.Count + peerStreamStates.Count + requiredStreams.Count);
        writes.Add((StorageKey(root, 1, receiptSuffix), receiptValue));
        writes.Add((StorageKey(root, 2, receiveSuffix), receiveValue));
        writes.Add((chainStorageKey, batch.BatchDigest.ToArray()));
        if (legacyTargetKey is { } legacyTarget && legacyTarget != chainKey &&
            legacyTargetState is { } legacyState)
            writes.Add((EncodeReceiveChainKey(partitionKey, legacyTarget),
                legacyState.LastBatchDigest.ToArray()));

        foreach (var pair in newLedger)
        {
            byte[] suffix = new byte[18];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)pair.Key.NamespaceId));
            pair.Key.ChangeId.WriteTo(suffix.AsSpan(2));
            byte[] value = EncodeChangeLedgerValue(pair.Value);
            writes.Add((StorageKey(root, 3, suffix), value));
        }
        foreach (var pair in newRelay)
        {
            foreach (var item in pair.Value)
            {
                byte[] suffix = new byte[26];
                BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)pair.Key.NamespaceId));
                pair.Key.Author.WriteTo(suffix.AsSpan(2));
                BinaryPrimitives.WriteUInt64BigEndian(suffix.AsSpan(18), item.Sequence);
                writes.Add((StorageKey(root, 20, suffix), EncodePayloadItem(item.ChangeId, item.Payload)));
            }
        }
        foreach (var pair in relayHigh)
        {
            byte[] value = new byte[16];
            if (current.RelayAck.TryGetValue(pair.Key, out ulong ack))
                BinaryPrimitives.WriteUInt64LittleEndian(value, ack);
            BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), pair.Value);
            writes.Add((StorageKey(root, 5, KeyNamespaceAuthor(pair.Key.NamespaceId, pair.Key.Author)), value));
        }
        foreach (var pair in peerStreamStates)
        {
            byte[] suffix = new byte[38];
            pair.Key.Peer.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), pair.Key.PeerEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)pair.Key.NamespaceId));
            pair.Key.AuthorStream.WriteTo(suffix.AsSpan(22));
            writes.Add((StorageKey(root, 15, suffix), EncodePeerStreamState(pair.Value)));
        }
        foreach (var key in requiredStreams)
        {
            byte[] suffix = new byte[38];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)key.NamespaceId));
            key.AuthorStream.WriteTo(suffix.AsSpan(2));
            key.Peer.WriteTo(suffix.AsSpan(18));
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(34), key.PeerEpoch);
            writes.Add((StorageKey(root, 16, suffix), Array.Empty<byte>()));
        }

        byte[]? identityKey = current.IsPersisted
            ? null
            : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null
            ? null
            : EncodePartitionIdentity(partitionKey);
        using var storageBatch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            storageBatch.Put(identityKey, identityValue!);
        foreach (var write in writes)
            storageBatch.Put(write.Key, write.Value);
        if (legacyChainDeleteKey is not null)
            storageBatch.Delete(legacyChainDeleteKey);
        CommitPartitionBatch(storageBatch, partitionKey, current);


        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
        return receipt;
    }

    private static bool SamePendingBatch(MetadataIncomingBatch left, MetadataIncomingBatch right)
        => left.ScopeId == right.ScopeId
            && left.LocalReplica == right.LocalReplica
            && left.SenderPeer == right.SenderPeer
            && left.SenderEpoch == right.SenderEpoch
            && left.TransportPeerEpoch == right.TransportPeerEpoch
            && left.AuthorStreamNodeId == right.AuthorStreamNodeId
            && left.NamespaceId == right.NamespaceId
            && left.BatchId == right.BatchId
            && left.CursorStart == right.CursorStart
            && left.CursorEnd == right.CursorEnd
            && left.PreviousBatchDigest.AsSpan().SequenceEqual(right.PreviousBatchDigest)
            && left.BatchDigest.AsSpan().SequenceEqual(right.BatchDigest)
            && left.WirePayload.AsSpan().SequenceEqual(right.WirePayload)
            && left.HeaderFingerprint.AsSpan().SequenceEqual(right.HeaderFingerprint)
            && left.ReceiveStateOwnedByMetadataStore == right.ReceiveStateOwnedByMetadataStore;

    private static MetadataIncomingBatch CloneBatch(MetadataIncomingBatch batch)
        => new()
        {
            ScopeId = batch.ScopeId,
            LocalReplica = batch.LocalReplica,
            SenderPeer = batch.SenderPeer,
            SenderEpoch = batch.SenderEpoch,
            TransportPeerEpoch = batch.TransportPeerEpoch,
            AuthorStreamNodeId = batch.AuthorStreamNodeId,
            NamespaceId = batch.NamespaceId,
            BatchId = batch.BatchId,
            CursorStart = batch.CursorStart,
            CursorEnd = batch.CursorEnd,
            PreviousBatchDigest = batch.PreviousBatchDigest.ToArray(),
            BatchDigest = batch.BatchDigest.ToArray(),
            WirePayload = batch.WirePayload.ToArray(),
            HeaderFingerprint = batch.HeaderFingerprint.ToArray(),
            ReceiveStateOwnedByMetadataStore = batch.ReceiveStateOwnedByMetadataStore,
            Operations = batch.Operations.Select(operation => new MetadataOperation
            {
                AuthorNodeId = operation.AuthorNodeId,
                ChangeId = operation.ChangeId,
                Fingerprint = operation.Fingerprint.ToArray(),
                Payload = operation.Payload.ToArray(),
                RelayEligible = operation.RelayEligible,
                Provenance = operation.Provenance
            }).ToArray()
        };

    private static byte[] EncodeReceiptValue(ReceiptValue receipt)
    {
        int baseLength = receipt.HeaderFingerprint.Length == 16 ? 92 : 76;
        byte[] value = new byte[baseLength + (receipt.TransportPeerEpoch == 0 ? 0 : 4)];
        BinaryPrimitives.WriteUInt64LittleEndian(value, receipt.CursorStart);
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), receipt.CursorEnd);
        receipt.PreviousBatchDigest.CopyTo(value, 16);
        receipt.BatchDigest.CopyTo(value, 32);
        BinaryPrimitives.WriteInt32LittleEndian(value.AsSpan(48), receipt.NewRelayOperationCount);
        receipt.DurableAuthor.WriteTo(value.AsSpan(52));
        if (receipt.HeaderFingerprint.Length == 16)
            receipt.HeaderFingerprint.CopyTo(value, 68);
        int originSequenceOffset = receipt.HeaderFingerprint.Length == 16 ? 84 : 68;
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(originSequenceOffset), receipt.OriginSequence);
        if (receipt.TransportPeerEpoch != 0)
            BinaryPrimitives.WriteUInt32LittleEndian(value.AsSpan(baseLength), receipt.TransportPeerEpoch);
        return value;
    }

    private static MetadataBatchReceipt CloneReceipt(MetadataBatchReceipt receipt, bool isDuplicate)
        => new MetadataBatchReceipt(receipt.CursorStart, receipt.CursorEnd, receipt.BatchDigest.ToArray(),
            receipt.NewRelayOperationCount, isDuplicate)
        {
            DurableAuthor = receipt.DurableAuthor,
            HeaderFingerprint = receipt.HeaderFingerprint.ToArray()
        };

    public IReadOnlyList<MetadataOutboundItem> FreezeOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        IReadOnlyList<MetadataFrozenOperation> operations)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        ArgumentNullException.ThrowIfNull(operations);

        var seen = new HashSet<BatchId>();
        foreach (var operation in operations)
        {
            if (operation.ChangeId.IsZero)
                throw new ArgumentException("Outbound ChangeId must be non-zero.", nameof(operations));
            if (operation.OriginAuthorEpoch == 0)
                throw new InvalidDataException("Outbound freeze requires a non-zero local author epoch.");
            ValidateDigest(operation.Fingerprint, nameof(operation.Fingerprint));
            ArgumentNullException.ThrowIfNull(operation.Payload);
            if (!seen.Add(operation.ChangeId))
                throw new ArgumentException("Outbound ChangeId values must be unique within a freeze operation.", nameof(operations));
        }

        var partitionKey = new PartitionKey(scopeId, localReplica);
        var current = GetOrCreatePartition(partitionKey);
        byte[] partitionRoot = PartitionRoot(partitionKey);
        if (operations.Count == 0)
            return Array.Empty<MetadataOutboundItem>();

        var key = new OutboundKey(namespaceId, localAuthor);
        if (current.RelayHighWatermarks.TryGetValue(new RelayKey(namespaceId, localAuthor), out ulong existingRelayHigh) &&
            existingRelayHigh != 0)
            throw new InvalidDataException("An author stream cannot be populated by both local outbound and incoming relay operations.");
        if (current.PendingApplies.Values.Any(pending =>
            pending.Batch.NamespaceId == namespaceId &&
            pending.Batch.Operations.Any(operation =>
                operation.AuthorNodeId == localAuthor && operation.RelayEligible)))
            throw new InvalidOperationException(
                "Outbound publication is fenced while an incoming relay apply for the same author stream is unresolved.");
        // Existing retries are required to form a prefix, so keep their resolved
        // items directly in the result slots instead of building two ChangeId maps.
        var result = new MetadataOutboundItem[operations.Count];
        var newOperations = new MetadataFrozenOperation[operations.Count];
        int newOperationCount = 0;
        var indexedRetryPayloads = new (ulong Sequence, MetadataFrozenOperation Operation)[operations.Count];
        int indexedRetryPayloadCount = 0;
        using StorageSnapshot payloadSnapshot = _storage.OpenSnapshot();
        bool sawNewOperation = false;
        int existingCount = 0;
        ulong previousExistingSequence = 0;
        for (int operationIndex = 0; operationIndex < operations.Count; operationIndex++)
        {
            MetadataFrozenOperation operation = operations[operationIndex];
            var ledgerKey = new LedgerKey(namespaceId, operation.ChangeId);
            bool alreadyFrozen = TryGetChangeLedger(payloadSnapshot, partitionRoot, current, ledgerKey, out var priorLedger)
                && priorLedger.IsLocalOutbound;
            if (priorLedger is not null)
            {
                if (!priorLedger.Fingerprint.AsSpan().SequenceEqual(operation.Fingerprint) ||
                    priorLedger.AuthorNodeId != localAuthor ||
                    priorLedger.OriginAuthorEpoch != operation.OriginAuthorEpoch ||
                    (operation.OriginSequence != 0 && priorLedger.OriginSequence != operation.OriginSequence))
                    throw new InvalidDataException("Outbound ChangeId conflicts with the global ChangeId ledger or immutable Origin.");
                if (!alreadyFrozen)
                    throw new InvalidDataException("ChangeId is already known from an incoming route and cannot be published as a new local operation.");
            }

            if (!alreadyFrozen)
            {
                sawNewOperation = true;
                newOperations[newOperationCount++] = operation;
                continue;
            }
            if (sawNewOperation)
                throw new InvalidDataException("Previously frozen outbound operations must form a prefix of an incremental retry.");

            ChangeLedgerValue frozenLedger = priorLedger
                ?? throw new InvalidDataException("Frozen outbound ChangeId has no durable ledger identity.");
            MetadataOutboundItem? prior = null;
            if (frozenLedger.StreamSequence != 0)
            {
                // The durable ChangeId ledger is also the outbound identity index.
                // Replays use a point lookup, independent of the historical stream length.
                ulong sequence = frozenLedger.StreamSequence;
                // Validate dense retry payloads through one bounded stream cursor below.
                // Reuse caller bytes until the public result is cloned; avoid one random
                // Storage point-read and payload-body allocation per indexed ChangeId.
                prior = new MetadataOutboundItem(sequence, localAuthor, operation.ChangeId, operation.Payload)
                {
                    Provenance = frozenLedger.Provenance
                };
                indexedRetryPayloads[indexedRetryPayloadCount++] = (sequence, operation);
            }
            else
            {
                // Compatibility for legacy ledger rows written before the sequence
                // was stored. Scan the B+ tree with a bounded cursor; no header cache.
                ulong legacySequence = FindStreamSequenceByChangeId(
                    payloadSnapshot, partitionRoot, namespaceId, localAuthor, operation.ChangeId);
                if (legacySequence == 0)
                    throw new InvalidDataException("Outbound ChangeId was retried with conflicting author or payload.");
                byte[] priorPayload = LoadStreamPayloadBody(
                    payloadSnapshot, partitionRoot, 21, namespaceId, localAuthor, legacySequence, operation.ChangeId);
                if (!priorPayload.AsSpan().SequenceEqual(operation.Payload))
                    throw new InvalidDataException("Outbound ChangeId was retried with conflicting author or payload.");
                prior = new MetadataOutboundItem(
                    legacySequence, localAuthor, operation.ChangeId, priorPayload)
                {
                    Provenance = frozenLedger.Provenance
                };
            }
            if (prior.Sequence <= previousExistingSequence)
                throw new InvalidDataException("Previously frozen outbound operations are not in author-sequence order.");
            previousExistingSequence = prior.Sequence;
            result[operationIndex] = prior;
            existingCount++;
        }

        ValidateIndexedOutboundRetryPayloads(
            payloadSnapshot, partitionRoot, namespaceId, localAuthor,
            new ArraySegment<(ulong Sequence, MetadataFrozenOperation Operation)>(
                indexedRetryPayloads, 0, indexedRetryPayloadCount));

        // Each ChangeId is an independent immutable operation. A replay may contain
        // already-frozen feed entries plus newer entries appended before the provider
        // ACK completed; preserve the old sequences and allocate only the new ones.

        if (newOperationCount > 0)
        {
            ulong highWatermark = current.OutboundHighWatermarks.TryGetValue(key, out var priorHighWatermark)
                ? priorHighWatermark
                : 0;
            var originHighWatermarks = new Dictionary<OriginStreamKey, OriginIssuedState>();
            int frozenStart = existingCount;
            ulong authorMaxHlcWall = 0;
            uint authorMaxHlcLogical = 0;
            foreach (var issued in current.OriginHighWatermarks)
            {
                if (issued.Key.NamespaceId != namespaceId || issued.Key.Author != localAuthor)
                    continue;
                if (issued.Value.MaxHlcWall > authorMaxHlcWall ||
                    (issued.Value.MaxHlcWall == authorMaxHlcWall &&
                     issued.Value.MaxHlcLogical > authorMaxHlcLogical))
                {
                    authorMaxHlcWall = issued.Value.MaxHlcWall;
                    authorMaxHlcLogical = issued.Value.MaxHlcLogical;
                }
            }
            // Missing allocator rows are checked against the durable ChangeLedger
            // in one pass, using only the epochs present in this bounded publication
            // batch. Avoid rescanning the complete ledger once per operation/epoch.
            var epochsMissingAllocator = new HashSet<uint>();
            foreach (MetadataFrozenOperation operation in newOperations.AsSpan(0, newOperationCount))
            {
                var originKey = new OriginStreamKey(namespaceId, localAuthor, operation.OriginAuthorEpoch);
                if (!current.OriginHighWatermarks.ContainsKey(originKey))
                    epochsMissingAllocator.Add(operation.OriginAuthorEpoch);
            }
            var epochsWithLedger = new HashSet<uint>();
            if (epochsMissingAllocator.Count != 0)
            {
                VisitChangeLedger(partitionKey, namespaceId, (key, provenance) =>
                {
                    if (provenance.AuthorNodeId == localAuthor &&
                        epochsMissingAllocator.Contains(provenance.AuthorEpoch))
                        epochsWithLedger.Add(provenance.AuthorEpoch);
                    return true;
                });
            }

            for (int newOperationIndex = 0; newOperationIndex < newOperationCount; newOperationIndex++)
            {
                MetadataFrozenOperation operation = newOperations[newOperationIndex];
                ulong sequence = checked(++highWatermark);
                var originKey = new OriginStreamKey(namespaceId, localAuthor, operation.OriginAuthorEpoch);
                if (!originHighWatermarks.TryGetValue(originKey, out OriginIssuedState issuedState))
                {
                    if (!current.OriginHighWatermarks.TryGetValue(originKey, out issuedState) &&
                        epochsWithLedger.Contains(operation.OriginAuthorEpoch))
                        throw new InvalidDataException("Durable origin-sequence allocator row is missing for an existing local author epoch.");
                }
                ulong originSequence = checked(issuedState.Sequence + 1);
                if (operation.OriginSequence != 0 && operation.OriginSequence != originSequence)
                    throw new InvalidDataException("New outbound operation supplied a non-contiguous immutable OriginSequence.");
                ulong maxIssuedHlcWall = issuedState.MaxHlcWall;
                uint maxIssuedHlcLogical = issuedState.MaxHlcLogical;
                if (operation.HlcWall < authorMaxHlcWall ||
                    (operation.HlcWall == authorMaxHlcWall && operation.HlcLogical < authorMaxHlcLogical))
                    throw new InvalidDataException("Local HLC regressed across immutable author epochs; publishing it could violate the signed HLC retention floor.");
                if (operation.HlcWall > authorMaxHlcWall ||
                    (operation.HlcWall == authorMaxHlcWall && operation.HlcLogical > authorMaxHlcLogical))
                {
                    authorMaxHlcWall = operation.HlcWall;
                    authorMaxHlcLogical = operation.HlcLogical;
                }
                if (operation.HlcWall > maxIssuedHlcWall ||
                    (operation.HlcWall == maxIssuedHlcWall && operation.HlcLogical > maxIssuedHlcLogical))
                {
                    maxIssuedHlcWall = operation.HlcWall;
                    maxIssuedHlcLogical = operation.HlcLogical;
                }
                originHighWatermarks[originKey] = new OriginIssuedState(
                    originSequence, maxIssuedHlcWall, maxIssuedHlcLogical);
                var item = new MetadataOutboundItem(sequence, localAuthor, operation.ChangeId, operation.Payload.ToArray())
                {
                    Provenance = new OperationOrigin(localAuthor, operation.OriginAuthorEpoch, originSequence)
                };
                result[frozenStart + newOperationIndex] = item;
            }

            PersistFrozenOutboundDelta(partitionKey, current, key, namespaceId, localAuthor,
                new ArraySegment<MetadataFrozenOperation>(newOperations, 0, newOperationCount),
                new ArraySegment<MetadataOutboundItem>(result, frozenStart, newOperationCount),
                highWatermark, originHighWatermarks);
        }

        for (int i = 0; i < result.Length; i++)
            result[i] = CloneOutboundItem(result[i]
                ?? throw new InvalidDataException("Outbound freeze did not resolve every input operation."));
        return result;
        }
    }

    public bool TryGetOutboundChangeFingerprint(
        string scopeId, NodeId localReplica, int namespaceId, BatchId changeId, out ReadOnlyMemory<byte> fingerprint)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (changeId.IsZero)
                throw new ArgumentException("Outbound ChangeId must be non-zero.", nameof(changeId));
            fingerprint = ReadOnlyMemory<byte>.Empty;
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out var state))
                return false;

            var key = new LedgerKey(namespaceId, changeId);
            if (!TryGetChangeLedger(partitionKey, state, key, out var prior))
                return false;
            if (!prior.IsLocalOutbound)
                throw new InvalidDataException("Provider ChangeId is already owned by an incoming route and cannot be published locally.");
            fingerprint = prior.Fingerprint;
            return true;
        }
    }

    public bool TryGetOutboundChangeIdentity(
        string scopeId, NodeId localReplica, int namespaceId,
        BatchId changeId, out ReadOnlyMemory<byte> fingerprint,
        out OperationOrigin origin)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (changeId.IsZero)
                throw new ArgumentException("Outbound ChangeId must be non-zero.", nameof(changeId));
            fingerprint = ReadOnlyMemory<byte>.Empty;
            origin = default;
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out PartitionState? state))
                return false;
            if (!TryGetChangeLedger(partitionKey, state, new LedgerKey(namespaceId, changeId), out ChangeLedgerValue? ledger))
                return false;
            if (!ledger!.IsLocalOutbound)
                throw new InvalidDataException("Provider ChangeId is already owned by an incoming route and cannot be published locally.");
            if (ledger.AuthorNodeId.IsZero || ledger.OriginAuthorEpoch == 0 || ledger.OriginSequence == 0)
                throw new InvalidDataException("Frozen local ChangeId is missing its immutable origin provenance.");
            fingerprint = ledger.Fingerprint;
            origin = new OperationOrigin(ledger.AuthorNodeId, ledger.OriginAuthorEpoch, ledger.OriginSequence);
            return true;
        }
    }

    /// <summary>
    /// Resolve any durable ChangeId identity, including incoming routes. This is used
    /// only for a one-time authority cutover proof; ordinary receive hot paths retain
    /// their existing partition transaction and do not call this per operation.
    /// </summary>
    public bool TryGetChangeIdentity(
        string scopeId, NodeId localReplica, int namespaceId, BatchId changeId,
        out ReadOnlyMemory<byte> fingerprint, out OperationOrigin origin,
        out bool isLocalOutbound)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (changeId.IsZero)
                throw new ArgumentException("ChangeId must be non-zero.", nameof(changeId));
            fingerprint = ReadOnlyMemory<byte>.Empty;
            origin = default;
            isLocalOutbound = false;
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out PartitionState? state) ||
                !TryGetChangeLedger(partitionKey, state, new LedgerKey(namespaceId, changeId), out ChangeLedgerValue? ledger))
                return false;

            fingerprint = ledger!.Fingerprint;
            origin = ledger.Provenance;
            isLocalOutbound = ledger.IsLocalOutbound;
            return true;
        }
    }

    public void StoreLegacyNodeLedgerFingerprints(
        string scopeId, NodeId localReplica, int namespaceId,
        IReadOnlyList<MetadataLegacyNodeLedgerFingerprint> fingerprints)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentNullException.ThrowIfNull(fingerprints);
        if (fingerprints.Count == 0)
            return;
        if (fingerprints.Count > 4096)
            throw new ArgumentOutOfRangeException(nameof(fingerprints), "Legacy Node digest commits are bounded to 4096 ChangeIds.");

        var seen = new HashSet<BatchId>(fingerprints.Count);
        foreach (MetadataLegacyNodeLedgerFingerprint item in fingerprints)
        {
            if (item.ChangeId.IsZero || !seen.Add(item.ChangeId))
                throw new ArgumentException("Legacy Node digest batch contains a zero or duplicate ChangeId.", nameof(fingerprints));
            if (item.Fingerprint.Length != 16)
                throw new ArgumentException("Legacy Node ledger fingerprint must be exactly 16 bytes.", nameof(fingerprints));
            bool nonZero = false;
            foreach (byte value in item.Fingerprint.Span) nonZero |= value != 0;
            if (!nonZero)
                throw new ArgumentException("Legacy Node ledger fingerprint cannot be zero.", nameof(fingerprints));
        }

        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out PartitionState? state))
                throw new InvalidOperationException("Cannot attach legacy Node digests to a missing Metadata Store partition.");

            var updates = new List<(LedgerKey LedgerKey, ChangeLedgerValue Value, byte[] StorageKey, byte[] Encoded)>(
                fingerprints.Count);
            byte[] root = PartitionRoot(partitionKey);
            foreach (MetadataLegacyNodeLedgerFingerprint item in fingerprints)
            {
                var ledgerKey = new LedgerKey(namespaceId, item.ChangeId);
                if (!TryGetChangeLedger(partitionKey, state, ledgerKey, out ChangeLedgerValue? ledger))
                    throw new InvalidOperationException("Cannot attach a legacy Node digest to a missing durable ChangeId identity.");

                if (ledger.LegacyNodeLedgerFingerprint is not null)
                {
                    if (!ledger.LegacyNodeLedgerFingerprint.AsSpan().SequenceEqual(item.Fingerprint.Span))
                        throw new InvalidDataException("A ChangeId already has a different legacy Node ledger digest.");
                    continue;
                }

                var updated = ledger with { LegacyNodeLedgerFingerprint = item.Fingerprint.ToArray() };
                byte[] suffix = new byte[18];
                BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
                item.ChangeId.WriteTo(suffix.AsSpan(2));
                byte[] storageKey = StorageKey(root, 3, suffix);
                updates.Add((ledgerKey, updated, storageKey, EncodeChangeLedgerValue(updated)));
            }

            if (updates.Count == 0)
                return;

            using var batch = _storage.BeginWriteBatch();
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != state.StorageCommitSeq)
                throw new StorageConflictException(state.StorageCommitSeq, actualBaseCommitSeq);
            foreach (var update in updates)
                batch.Put(update.StorageKey, update.Encoded);
            CommitPartitionBatch(batch, partitionKey, state);

            foreach (var update in updates)
            {
                state.ChangeLedger[update.LedgerKey] = update.Value;
            }
            state.StorageCommitSeq = _storage.LastCommitSeq;
            state.IsPersisted = true;
        }
    }

    public ulong AcquireOutboundLease(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        Guid consumerId)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        if (consumerId == Guid.Empty)
            throw new ArgumentException("ConsumerId must be non-empty.", nameof(consumerId));

        var partitionKey = new PartitionKey(scopeId, localReplica);
        var current = GetOrCreatePartition(partitionKey);
        var key = new OutboundKey(namespaceId, localAuthor);
        ulong previous = current.OutboundLeases.TryGetValue(key, out var prior) ? prior.Generation : 0;
        ulong generation = checked(previous + 1);

        PersistOutboundLease(partitionKey, current, key, new LeaseState(generation, consumerId));
        return generation;
    
        }
    }

    public MetadataOutboundPage ReadOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        ulong leaseGeneration,
        ulong afterSequence,
        int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
            throw new InvalidOperationException("Outbound lease is not active.");

        var key = new OutboundKey(namespaceId, localAuthor);
        ValidateOutboundLease(state, key, leaseGeneration);
        ulong acknowledged = state.OutboundAck.TryGetValue(key, out var ack) ? ack : 0;
        ulong highWatermark = state.OutboundHighWatermarks.TryGetValue(key, out var outboundHighWatermark) ? outboundHighWatermark : 0;
        if (afterSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(afterSequence));

        ulong cursor = Math.Max(afterSequence, acknowledged);
        if (limit <= 0 || cursor >= highWatermark)
            return new MetadataOutboundPage(Array.Empty<MetadataOutboundItem>(), cursor);

        var partitionKey = new PartitionKey(scopeId, localReplica);
        byte[] partitionRoot = PartitionRoot(partitionKey);
        using StorageSnapshot payloadSnapshot = _storage.OpenSnapshot();
        MetadataOutboundItem[] page = ReadOutboundStoragePage(
            payloadSnapshot, partitionRoot, state, namespaceId, localAuthor, cursor, limit, out _);
        ulong scannedThrough = page.Length == 0 ? cursor : page[^1].Sequence;
        return new MetadataOutboundPage(page, scannedThrough);
    
        }
    }

    public void AcknowledgeOutbound(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        ulong leaseGeneration,
        ulong safeThroughSequence)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!TryGetPartition(partitionKey, out var current))
            throw new InvalidOperationException("Outbound lease is not active.");

        var key = new OutboundKey(namespaceId, localAuthor);
        ValidateOutboundLease(current, key, leaseGeneration);
        ulong previous = current.OutboundAck.TryGetValue(key, out var ack) ? ack : 0;
        ulong highWatermark = current.OutboundHighWatermarks.TryGetValue(key, out var outboundHighWatermark) ? outboundHighWatermark : 0;
        if (safeThroughSequence < previous || safeThroughSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        PersistStreamWatermark(partitionKey, current, 8,
            namespaceId, localAuthor, safeThroughSequence, highWatermark);
    
        }
    }

    public void CompactOutboundThrough(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor,
        ulong safeThroughSequence)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!TryGetPartition(partitionKey, out var current))
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var key = new OutboundKey(namespaceId, localAuthor);
        ulong acknowledged = current.OutboundAck.TryGetValue(key, out var ack) ? ack : 0;
        ulong peerSafe = GetSafePeerCursor(current, namespaceId, localAuthor);
        bool hasPeerRequirements = current.InterestedPeers.Any(peer => peer.NamespaceId == namespaceId && peer.Peer != localAuthor) ||
            current.RequiredPeerStreams.Any(peer => peer.NamespaceId == namespaceId && peer.AuthorStream == localAuthor && peer.Peer != localAuthor);
        bool hasLocalConsumer = current.OutboundLeases.ContainsKey(key);
        if ((hasLocalConsumer && safeThroughSequence > acknowledged) ||
            (hasPeerRequirements && safeThroughSequence > peerSafe) ||
            (!hasLocalConsumer && !hasPeerRequirements && safeThroughSequence > 0))
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        _ = CompactStreamPayload(partitionKey, current, 21,
            namespaceId, localAuthor, safeThroughSequence, relay: false, maxEntries: int.MaxValue);
    
        }
    }

    /// <summary>Remove at most one bounded page of already-safe outbound payload rows.</summary>
    public int CompactOutboundThroughBounded(
        string scopeId, NodeId localReplica, int namespaceId, NodeId localAuthor,
        ulong safeThroughSequence, int maxEntries = 512)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        if (maxEntries <= 0 || maxEntries > 4096)
            throw new ArgumentOutOfRangeException(nameof(maxEntries));
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localAuthor);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out var current))
                throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
            var key = new OutboundKey(namespaceId, localAuthor);
            ulong acknowledged = current.OutboundAck.TryGetValue(key, out var ack) ? ack : 0;
            ulong peerSafe = GetSafePeerCursor(current, namespaceId, localAuthor);
            bool hasPeerRequirements = current.InterestedPeers.Any(peer => peer.NamespaceId == namespaceId && peer.Peer != localAuthor) ||
                current.RequiredPeerStreams.Any(peer => peer.NamespaceId == namespaceId && peer.AuthorStream == localAuthor && peer.Peer != localAuthor);
            bool hasLocalConsumer = current.OutboundLeases.ContainsKey(key);
            if ((hasLocalConsumer && safeThroughSequence > acknowledged) ||
                (hasPeerRequirements && safeThroughSequence > peerSafe) ||
                (!hasLocalConsumer && !hasPeerRequirements && safeThroughSequence > 0))
                throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
            return CompactStreamPayload(partitionKey, current, 21,
                namespaceId, localAuthor, safeThroughSequence, relay: false, maxEntries);
        }
    }

    /// <summary>Remove at most one bounded page of already-safe relay payload rows.</summary>
    public int CompactRelayThroughBounded(
        string scopeId, NodeId localReplica, int namespaceId, NodeId originalAuthor,
        ulong safeThroughSequence, int maxEntries = 512)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        if (maxEntries <= 0 || maxEntries > 4096)
            throw new ArgumentOutOfRangeException(nameof(maxEntries));
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out var current))
                throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
            var key = new RelayKey(namespaceId, originalAuthor);
            ulong acknowledged = current.RelayAck.TryGetValue(key, out var ack) ? ack : 0;
            ulong peerSafe = GetSafePeerCursor(current, namespaceId, originalAuthor);
            bool hasPeerRequirements = current.InterestedPeers.Any(peer =>
                peer.NamespaceId == namespaceId && peer.Peer != originalAuthor) ||
                current.RequiredPeerStreams.Any(peer =>
                peer.NamespaceId == namespaceId && peer.AuthorStream == originalAuthor && peer.Peer != originalAuthor);
            bool hasLocalConsumer = current.RelayAck.ContainsKey(key);
            if ((hasLocalConsumer && safeThroughSequence > acknowledged) ||
                (hasPeerRequirements && safeThroughSequence > peerSafe) ||
                (!hasLocalConsumer && !hasPeerRequirements && safeThroughSequence > 0))
                throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
            return CompactStreamPayload(partitionKey, current, 20,
                namespaceId, originalAuthor, safeThroughSequence, relay: true, maxEntries);
        }
    }

    /// <summary>
    /// Removes one bounded page per baseline-covered outbound/relay stream. The exact
    /// marker and accepted MBR1 evidence are revalidated before every page, so a caller
    /// can retry the same generation after process death or Storage reopen. The safe
    /// prefix is the minimum paired source-feed/receiver cursor across Active peers,
    /// capped by current peer and local-consumer acknowledgements.
    /// </summary>
    public int CompactFullReplaceBaselinePayloadsBounded(
        string scopeId, NodeId localReplica, NamespaceRosterCertificate acceptedRoster,
        Guid generation, ReadOnlySpan<byte> snapshotDigest, int maxEntriesPerStream = 512)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        if (generation == Guid.Empty || snapshotDigest.Length != MetadataBaselineReportProtocol.DigestSize)
            throw new ArgumentException("A FULL_REPLACE generation and 32-byte snapshot digest are required.");
        if (maxEntriesPerStream <= 0 || maxEntriesPerStream > 4096)
            throw new ArgumentOutOfRangeException(nameof(maxEntriesPerStream));

        lock (_gate)
        {
            ThrowIfDisposed();
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!IsFullReplaceBaselineApplied(
                    scopeId, localReplica, acceptedRoster, generation, snapshotDigest))
                throw new InvalidOperationException(
                    "Baseline payload compaction requires the exact durable FULL_REPLACE apply marker.");

            // Re-run the transaction's evidence validation. With an exact marker this
            // must be a no-op; a changed MBR1 evidence set fails closed before deletion.
            if (ApplyFullReplaceBaseline(
                    scopeId, localReplica, acceptedRoster, generation, snapshotDigest))
                throw new InvalidOperationException(
                    "FULL_REPLACE baseline marker changed while preparing payload compaction.");

            if (!TryGetPartition(partitionKey, out var current))
                throw new InvalidOperationException("FULL_REPLACE baseline Metadata partition is unavailable.");

            int pendingApplies = current.PendingApplies.Values.Count(
                pending => pending.Batch.NamespaceId == acceptedRoster.NamespaceId);
            if (pendingApplies != 0)
                throw new InvalidOperationException(
                    $"Baseline payload compaction is blocked by {pendingApplies} unresolved PendingApply intent(s).");
            int pendingReservations = current.PeerStreams.Count(pair =>
                pair.Key.NamespaceId == acceptedRoster.NamespaceId &&
                (pair.Value.PendingBatch is not null || pair.Value.PushedCursor != pair.Value.AckedCursor));
            if (pendingReservations != 0)
                throw new InvalidOperationException(
                    $"Baseline payload compaction is blocked by {pendingReservations} immutable peer reservation(s).");

            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the durable namespace-GC store before baseline payload compaction.");
            IReadOnlyList<MetadataBaselineReport> reports =
                gcStore.ReadMetadataBaselineReports(acceptedRoster, generation, snapshotDigest);
            NamespaceRosterMember[] activeMembers = acceptedRoster.Members
                .Where(member => member.Status == NamespaceMemberStatus.Active)
                .ToArray();
            if (reports.Count != activeMembers.Length)
                throw new InvalidDataException("Baseline payload compaction requires one current MBR1 report per Active member.");

            var reportsByMember = reports.ToDictionary(report => report.MemberNodeId);
            if (activeMembers.Any(member => !reportsByMember.ContainsKey(member.NodeId)))
                throw new InvalidDataException("Baseline payload compaction found an incomplete Active-member report set.");
            NamespaceRosterMember localMember = activeMembers.FirstOrDefault(member => member.NodeId == localReplica)
                ?? throw new InvalidOperationException("Local Metadata replica is not Active in the accepted roster.");
            MetadataBaselineReport localReport = reportsByMember[localReplica];
            if (localReport.MemberTransportEpoch != localMember.TransportPeerEpoch)
                throw new InvalidDataException("Local MBR1 report transport epoch does not match the accepted roster.");

            var safePrefixes = new Dictionary<(MetadataBaselineFeedKind Kind, NodeId Author), ulong>();
            var issuerEpochs = new Dictionary<(MetadataBaselineFeedKind Kind, NodeId Author), uint>();
            foreach (MetadataBaselineFeedEntry feed in localReport.FeedEntries)
            {
                NamespaceRosterMember targetMember = activeMembers.FirstOrDefault(member =>
                    member.NodeId == feed.TargetPeer)
                    ?? throw new InvalidDataException("Baseline feed row targets a non-Active member.");
                if (feed.TargetTransportEpoch != targetMember.TransportPeerEpoch)
                    throw new InvalidDataException("Baseline feed row targets a stale peer transport epoch.");
                MetadataBaselineReport targetReport = reportsByMember[feed.TargetPeer];
                MetadataBaselineReceiveEntry? receive = targetReport.ReceiveEntries.FirstOrDefault(entry =>
                    entry.SenderPeer == localReplica &&
                    entry.SenderTransportEpoch == localMember.TransportPeerEpoch &&
                    entry.AuthorStream == feed.AuthorStream &&
                    entry.IssuerEpoch == feed.IssuerEpoch);
                byte[] receiveDigest = receive?.LastBatchDigest ?? new byte[16];
                if (!receiveDigest.AsSpan().SequenceEqual(feed.DigestAnchor))
                    throw new InvalidDataException("Baseline source feed digest does not match its target's signed receive chain.");

                var stream = (feed.FeedKind, feed.AuthorStream);
                if (issuerEpochs.TryGetValue(stream, out uint priorIssuerEpoch) &&
                    priorIssuerEpoch != feed.IssuerEpoch)
                    throw new InvalidDataException(
                        "Active peers disagree on the source issuer epoch for one baseline feed stream.");
                issuerEpochs[stream] = feed.IssuerEpoch;

                ulong absoluteSafe = checked(feed.AbsoluteFeedOffset + (receive?.ReceiveCursor ?? 0));
                if (safePrefixes.TryGetValue(stream, out ulong prior))
                    safePrefixes[stream] = Math.Min(prior, absoluteSafe);
                else
                    safePrefixes.Add(stream, absoluteSafe);
            }

            int removedTotal = 0;
            foreach (var pair in safePrefixes)
            {
                bool relay = pair.Key.Kind == MetadataBaselineFeedKind.Relay;
                if (!relay && pair.Key.Kind != MetadataBaselineFeedKind.Outbound)
                    throw new InvalidDataException("Baseline feed contains an unsupported source kind.");
                NodeId author = pair.Key.Author;
                ulong safeThrough = pair.Value;
                bool hasPeerRequirements = current.InterestedPeers.Any(peer =>
                        peer.NamespaceId == acceptedRoster.NamespaceId && peer.Peer != author) ||
                    current.RequiredPeerStreams.Any(peer =>
                        peer.NamespaceId == acceptedRoster.NamespaceId &&
                        peer.AuthorStream == author && peer.Peer != author);
                if (hasPeerRequirements)
                    safeThrough = Math.Min(safeThrough,
                        GetSafePeerCursor(current, acceptedRoster.NamespaceId, author));

                if (relay)
                {
                    var key = new RelayKey(acceptedRoster.NamespaceId, author);
                    if (current.RelayAck.ContainsKey(key))
                        safeThrough = Math.Min(safeThrough,
                            current.RelayAck.TryGetValue(key, out ulong ack) ? ack : 0);
                    else if (!hasPeerRequirements)
                        safeThrough = 0;

                    removedTotal = checked(removedTotal + CompactRelayThroughBounded(
                        scopeId, localReplica, acceptedRoster.NamespaceId, author,
                        safeThrough, maxEntriesPerStream));
                }
                else
                {
                    var key = new OutboundKey(acceptedRoster.NamespaceId, author);
                    if (current.OutboundLeases.ContainsKey(key))
                        safeThrough = Math.Min(safeThrough,
                            current.OutboundAck.TryGetValue(key, out ulong ack) ? ack : 0);
                    else if (!hasPeerRequirements)
                        safeThrough = 0;

                    removedTotal = checked(removedTotal + CompactOutboundThroughBounded(
                        scopeId, localReplica, acceptedRoster.NamespaceId, author,
                        safeThrough, maxEntriesPerStream));
                }
            }
            return removedTotal;
        }
    }

    /// <summary>
    /// Returns true only when the exact FULL_REPLACE generation, roster generation and
    /// snapshot digest have a durable atomic MBR1 baseline-apply marker.
    /// </summary>
    public bool IsFullReplaceBaselineApplied(
        string scopeId, NodeId localReplica, NamespaceRosterCertificate acceptedRoster,
        Guid generation, ReadOnlySpan<byte> snapshotDigest)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        if (localReplica.IsZero || generation == Guid.Empty ||
            snapshotDigest.Length != MetadataBaselineReportProtocol.DigestSize)
            throw new ArgumentException("Baseline marker identity is invalid.");
        lock (_gate)
        {
            ThrowIfDisposed();
            var partitionKey = new PartitionKey(scopeId, localReplica);
            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the durable namespace-GC store before checking a FULL_REPLACE baseline marker.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.TryReadNamespaceRoster(acceptedRoster.NamespaceId, trustStore,
                    out NamespaceRosterCertificate? currentRoster) || currentRoster is null ||
                currentRoster.RosterGeneration != acceptedRoster.RosterGeneration ||
                !NamespaceRosterProtocol.ComputeDigest(currentRoster).AsSpan()
                    .SequenceEqual(NamespaceRosterProtocol.ComputeDigest(acceptedRoster)))
                throw new InvalidDataException("FULL_REPLACE baseline marker cannot authorize compaction for a stale or unaccepted roster.");
            if (!TryGetPartition(partitionKey, out var current))
                return false;
            byte[] key = EncodeBaselineApplyMarkerKey(
                partitionKey, acceptedRoster.NamespaceId, acceptedRoster.RosterGeneration, generation);
            using var snapshot = _storage.OpenSnapshot();
            if (!snapshot.TryGet(key, out byte[]? value))
                return false;
            byte[] expectedRosterDigest = NamespaceRosterProtocol.ComputeDigest(acceptedRoster);
            if (value!.Length != 109 || !value.AsSpan(0, 4).SequenceEqual("MBAC"u8) ||
                value[4] != 1 || !value.AsSpan(5, 32).SequenceEqual(snapshotDigest) ||
                !value.AsSpan(69, 32).SequenceEqual(expectedRosterDigest) ||
                BinaryPrimitives.ReadInt32LittleEndian(value.AsSpan(101)) !=
                    acceptedRoster.Members.Count(member => member.Status == NamespaceMemberStatus.Active) ||
                BinaryPrimitives.ReadInt32LittleEndian(value.AsSpan(105)) < 0)
                throw new InvalidDataException("Durable FULL_REPLACE baseline marker is malformed or does not match the requested cut.");
            return true;
        }
    }

    /// <summary>
    /// Durably records this member's installed FULL_REPLACE ACK and its MBR1 report
    /// from the same caller-held Host commit interval. Repeated calls reuse the exact
    /// persisted report when its sequence matches, so crash recovery cannot recapture a
    /// different Metadata cut under the same report sequence.
    /// </summary>
    public MetadataBaselineReport AcceptLocalFullReplaceAndCreateBaselineReport(
        string scopeId,
        NodeId localReplica,
        NamespaceRosterCertificate acceptedRoster,
        FullReplaceInstalledAck installedAck,
        int snapshotEntryCount,
        ulong providerChangeFeedWatermark,
        ulong reportSequence,
        ReadOnlySpan<byte> memberSeed)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentNullException.ThrowIfNull(installedAck);
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        if (installedAck.MemberNodeId != localReplica ||
            installedAck.NamespaceId != acceptedRoster.NamespaceId ||
            installedAck.RosterGeneration != acceptedRoster.RosterGeneration ||
            !installedAck.Installed || installedAck.Generation == Guid.Empty ||
            installedAck.Digest.Length != MetadataBaselineReportProtocol.DigestSize)
            throw new InvalidDataException("Local FULL_REPLACE installation ACK identity is invalid for MBR1 capture.");

        lock (_gate)
        {
            ThrowIfDisposed();
            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the durable namespace-GC store before recording local MBR1 evidence.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.IsBoundTo(_storage, scopeId, localReplica))
                throw new InvalidOperationException("Namespace-GC store is not bound to this Metadata scope and local replica.");

            byte[] signedAck = FullReplaceInstalledAckProtocol.Sign(installedAck, memberSeed);
            _ = gcStore.AcceptFullReplaceInstalledAck(signedAck, acceptedRoster);

            MetadataBaselineReport? prior = gcStore.ReadMetadataBaselineReports(
                    acceptedRoster, installedAck.Generation, installedAck.Digest)
                .FirstOrDefault(report => report.MemberNodeId == localReplica);
            if (prior is not null)
            {
                if (prior.ReportSequence > reportSequence)
                    throw new InvalidDataException("Requested local MBR1 report sequence regressed behind the durable report.");
                if (prior.ReportSequence == reportSequence)
                    return prior;
            }

            MetadataBaselineReport report = CreateMetadataBaselineReport(
                scopeId, localReplica, acceptedRoster, installedAck.Generation,
                installedAck.Digest, snapshotEntryCount, providerChangeFeedWatermark,
                reportSequence, memberSeed);
            _ = gcStore.AcceptMetadataBaselineReport(
                MetadataBaselineReportProtocol.EncodeEnvelope(report),
                acceptedRoster, installedAck.Generation, installedAck.Digest);
            return report;
        }
    }

    /// <summary>
    /// Captures and signs this replica's own MBR1 vector from one durable Metadata
    /// partition cut. The caller must hold the Host/provider commit lease while invoking
    /// this method so the provider snapshot identity and Metadata vectors share one
    /// admission boundary.
    /// </summary>
    public MetadataBaselineReport CreateMetadataBaselineReport(
        string scopeId,
        NodeId localReplica,
        NamespaceRosterCertificate acceptedRoster,
        Guid generation,
        ReadOnlySpan<byte> snapshotDigest,
        int snapshotEntryCount,
        ulong providerChangeFeedWatermark,
        ulong reportSequence,
        ReadOnlySpan<byte> memberSeed)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        if (generation == Guid.Empty ||
            snapshotDigest.Length != MetadataBaselineReportProtocol.DigestSize ||
            snapshotEntryCount < 0 || reportSequence == 0 || memberSeed.Length != 32)
            throw new ArgumentException("MBR1 snapshot, report sequence or signing identity is invalid.");

        lock (_gate)
        {
            ThrowIfDisposed();
            var partitionKey = new PartitionKey(scopeId, localReplica);
            PartitionState current = GetOrCreatePartition(partitionKey);
            int pendingApplies = current.PendingApplies.Values.Count(
                pending => pending.Batch.NamespaceId == acceptedRoster.NamespaceId);
            if (pendingApplies != 0)
                throw new InvalidOperationException($"MBR1 capture is blocked by {pendingApplies} unresolved PendingApply intent(s).");
            int pendingReservations = current.PeerStreams.Count(pair =>
                pair.Key.NamespaceId == acceptedRoster.NamespaceId &&
                (pair.Value.PendingBatch is not null || pair.Value.PushedCursor != pair.Value.AckedCursor));
            if (pendingReservations != 0)
                throw new InvalidOperationException($"MBR1 capture is blocked by {pendingReservations} immutable peer reservation(s).");

            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the durable namespace-GC store before creating an MBR1 report.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.IsBoundTo(_storage, scopeId, localReplica) ||
                !gcStore.TryReadNamespaceRoster(acceptedRoster.NamespaceId, trustStore,
                    out NamespaceRosterCertificate? currentRoster) || currentRoster is null ||
                currentRoster.RosterGeneration != acceptedRoster.RosterGeneration ||
                !NamespaceRosterProtocol.ComputeDigest(currentRoster).AsSpan()
                    .SequenceEqual(NamespaceRosterProtocol.ComputeDigest(acceptedRoster)))
                throw new InvalidDataException("MBR1 capture requires the exact currently accepted namespace roster and Metadata binding.");

            NamespaceRosterMember localMember = acceptedRoster.Members.FirstOrDefault(
                member => member.NodeId == localReplica)
                ?? throw new InvalidOperationException("Local replica is absent from the accepted namespace roster.");
            if (localMember.Status != NamespaceMemberStatus.Active)
                throw new InvalidOperationException("Only an Active member may publish an MBR1 report.");

            FullReplaceInstalledAck localAck = gcStore.ReadFullReplaceInstalledAcks(
                    acceptedRoster, generation)
                .FirstOrDefault(ack => ack.MemberNodeId == localReplica)
                ?? throw new InvalidOperationException("Local member has not durably installed the requested FULL_REPLACE generation.");
            if (!localAck.Installed ||
                localAck.MemberTransportEpoch != localMember.TransportPeerEpoch ||
                !localAck.Digest.AsSpan().SequenceEqual(snapshotDigest))
                throw new InvalidDataException("Local FRA2 installation ACK does not match the accepted roster and requested snapshot.");

            int namespaceId = acceptedRoster.NamespaceId;
            NamespaceRosterMember[] activePeers = acceptedRoster.Members
                .Where(member => member.Status == NamespaceMemberStatus.Active && member.NodeId != localReplica)
                .OrderBy(member => Convert.ToHexString(member.NodeId.ToArray()), StringComparer.Ordinal)
                .ToArray();

            var authorKinds = new Dictionary<NodeId, MetadataBaselineFeedKind>();
            foreach (OutboundKey key in current.OutboundHighWatermarks.Keys.Where(key => key.NamespaceId == namespaceId))
                authorKinds.Add(key.Author, MetadataBaselineFeedKind.Outbound);
            foreach (RelayKey key in current.RelayHighWatermarks.Keys.Where(key => key.NamespaceId == namespaceId))
                if (!authorKinds.TryAdd(key.Author, MetadataBaselineFeedKind.Relay))
                    throw new InvalidDataException("One author stream exists in both outbound and relay feed domains.");

            var feeds = new List<MetadataBaselineFeedEntry>(
                checked(activePeers.Length * authorKinds.Count));
            foreach (NamespaceRosterMember peer in activePeers)
            {
                foreach (var author in authorKinds.OrderBy(pair =>
                             Convert.ToHexString(pair.Key.ToArray()), StringComparer.Ordinal))
                {
                    if (author.Key == peer.NodeId)
                        continue;
                    var streamKey = new PeerStreamKey(peer.NodeId, peer.TransportPeerEpoch,
                        namespaceId, author.Key);
                    if (!current.PeerStreams.TryGetValue(streamKey, out PeerStreamStateValue? state))
                        throw new InvalidOperationException("MBR1 cannot omit an uninitialized Active-peer feed stream.");
                    if (state.IssuerEpoch == 0 || state.LastBatchDigest.Length != MetadataBaselineReportProtocol.ChainDigestSize ||
                        state.PendingBatch is not null || state.PushedCursor != state.AckedCursor)
                        throw new InvalidOperationException("MBR1 capture is blocked by an invalid or in-flight peer stream state.");
                    if (author.Value == MetadataBaselineFeedKind.Relay && state.StreamOffset != 0)
                        throw new InvalidDataException("Relay feed baseline offset must remain zero.");
                    feeds.Add(new MetadataBaselineFeedEntry
                    {
                        TargetPeer = peer.NodeId,
                        TargetTransportEpoch = peer.TransportPeerEpoch,
                        AuthorStream = author.Key,
                        FeedKind = author.Value,
                        IssuerEpoch = state.IssuerEpoch,
                        AbsoluteFeedOffset = state.StreamOffset,
                        DigestAnchor = state.LastBatchDigest.ToArray()
                    });
                }
            }

            var receiveKeys = new HashSet<ReceiveKey>(
                current.Receives.Keys.Where(key => key.NamespaceId == namespaceId));
            foreach (ReceiveChainKey key in current.ReceiveChains.Keys.Where(key => key.NamespaceId == namespaceId))
                receiveKeys.Add(new ReceiveKey(key.Sender, key.Epoch, key.NamespaceId, key.AuthorStream));
            foreach (LegacyReceiveChainKey legacy in current.LegacyReceiveChains.Keys.Where(key => key.NamespaceId == namespaceId))
            {
                if (!TryGetSingleEstablishedAuthor(current, legacy, out NodeId establishedAuthor))
                    throw new InvalidDataException("Ambiguous legacy sender-wide receive digest chain blocks MBR1 capture.");
                receiveKeys.Add(new ReceiveKey(legacy.Sender, legacy.Epoch, namespaceId, establishedAuthor));
            }

            var receives = new List<MetadataBaselineReceiveEntry>(receiveKeys.Count);
            foreach (ReceiveKey key in receiveKeys)
            {
                NamespaceRosterMember sender = acceptedRoster.Members.FirstOrDefault(
                    member => member.NodeId == key.Sender)
                    ?? throw new InvalidDataException("A persisted receive stream sender is absent from the accepted roster.");
                uint senderTransportEpoch = sender.Status == NamespaceMemberStatus.Retired
                    ? sender.RetiredThroughEpoch
                    : sender.TransportPeerEpoch;
                if (senderTransportEpoch == 0)
                    throw new InvalidDataException("A persisted receive stream has no roster-covered sender transport epoch.");

                ulong cursor = current.Receives.TryGetValue(key, out ReceiveState receiveState)
                    ? receiveState.CursorEnd : 0;
                ReceiveChainState chain = GetReceiveChainStateForAuthor(
                    current, key, out _, out _, out _);
                if (chain.LastBatchDigest.Length != MetadataBaselineReportProtocol.ChainDigestSize ||
                    (cursor == 0 && chain.LastBatchDigest.Any(value => value != 0)))
                    throw new InvalidDataException("Persisted receive cursor and digest anchor are inconsistent.");
                receives.Add(new MetadataBaselineReceiveEntry
                {
                    SenderPeer = key.Sender,
                    SenderTransportEpoch = senderTransportEpoch,
                    AuthorStream = key.AuthorStream,
                    IssuerEpoch = key.Epoch,
                    ReceiveCursor = cursor,
                    LastBatchDigest = chain.LastBatchDigest.ToArray()
                });
            }

            var report = new MetadataBaselineReport
            {
                NamespaceId = namespaceId,
                RosterGeneration = acceptedRoster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(acceptedRoster),
                FullReplaceGeneration = generation,
                SnapshotDigest = snapshotDigest.ToArray(),
                SnapshotEntryCount = snapshotEntryCount,
                ProviderChangeFeedWatermark = providerChangeFeedWatermark,
                HlcWallFloor = localAck.HlcWallFloor,
                HlcLogicalFloor = localAck.HlcLogicalFloor,
                MemberNodeId = localReplica,
                MemberTransportEpoch = localMember.TransportPeerEpoch,
                ReportSequence = reportSequence,
                FeedEntries = feeds.OrderBy(entry => Convert.ToHexString(entry.TargetPeer.ToArray()), StringComparer.Ordinal)
                    .ThenBy(entry => entry.TargetTransportEpoch)
                    .ThenBy(entry => Convert.ToHexString(entry.AuthorStream.ToArray()), StringComparer.Ordinal)
                    .ToArray(),
                ReceiveEntries = receives.OrderBy(entry => Convert.ToHexString(entry.SenderPeer.ToArray()), StringComparer.Ordinal)
                    .ThenBy(entry => entry.SenderTransportEpoch)
                    .ThenBy(entry => Convert.ToHexString(entry.AuthorStream.ToArray()), StringComparer.Ordinal)
                    .ThenBy(entry => entry.IssuerEpoch)
                    .ToArray()
            };
            byte[] signed = MetadataBaselineReportProtocol.Sign(report, memberSeed);
            return MetadataBaselineReportProtocol.DecodeAndVerify(
                signed, acceptedRoster, generation, snapshotDigest);
        }
    }

    public IReadOnlyList<MetadataPeerStreamSnapshot> ReadPeerStreamStates(
        string scopeId, NodeId localReplica, NodeId peer, uint peerEpoch, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (string.IsNullOrWhiteSpace(scopeId)) throw new ArgumentException("ScopeId is required.", nameof(scopeId));
            if (localReplica.IsZero || peer.IsZero) throw new ArgumentException("Replica and peer identities must be non-zero.");
            if ((uint)namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return Array.Empty<MetadataPeerStreamSnapshot>();
            return state.PeerStreams
                .Where(pair => pair.Key.Peer == peer && pair.Key.PeerEpoch == peerEpoch && pair.Key.NamespaceId == namespaceId)
                .OrderBy(pair => Convert.ToHexString(pair.Key.AuthorStream.ToArray()), StringComparer.Ordinal)
                .Select(pair => new MetadataPeerStreamSnapshot(pair.Key.AuthorStream, ToPublicPeerStreamState(pair.Value)))
                .ToArray();
        }
    }

    public IReadOnlyList<NodeId> ReadOutboundAuthors(string scopeId, NodeId localReplica, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return Array.Empty<NodeId>();
            return state.OutboundHighWatermarks.Keys.Where(key => key.NamespaceId == namespaceId)
                .Select(key => key.Author).Distinct()
                .OrderBy(author => Convert.ToHexString(author.ToArray()), StringComparer.Ordinal)
                .ToArray();
        }
    }

    public IReadOnlyList<NodeId> ReadRelayAuthors(string scopeId, NodeId localReplica, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return Array.Empty<NodeId>();
            return state.RelayHighWatermarks.Keys.Where(key => key.NamespaceId == namespaceId)
                .Select(key => key.Author).Distinct()
                .OrderBy(author => Convert.ToHexString(author.ToArray()), StringComparer.Ordinal)
                .ToArray();
        }
    }

    public MetadataOutboundPage ReadOutboundForPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream, int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            if (limit < 0) throw new ArgumentOutOfRangeException(nameof(limit));
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return new MetadataOutboundPage(Array.Empty<MetadataOutboundItem>(), 0);
            ValidatePeerEpochForAccess(state, namespaceId, peer, peerEpoch);
            if (!state.CompletedPeerBaselines.Contains(new InterestedPeerKey(namespaceId, peer, peerEpoch)))
                throw new SyncException("SNAPSHOT_REQUIRED");
            var streamKey = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            var progress = state.PeerStreams.TryGetValue(streamKey, out var saved)
                ? saved : EmptyPeerStreamValue();
            ulong cursor = checked(progress.AckedCursor + progress.StreamOffset);
            var key = new OutboundKey(namespaceId, authorStream);
            ulong high = state.OutboundHighWatermarks.TryGetValue(key, out var watermark) ? watermark : 0;
            if (cursor >= high || limit == 0)
                return new MetadataOutboundPage(Array.Empty<MetadataOutboundItem>(), Math.Min(cursor, high));
            var partitionKey = new PartitionKey(scopeId, localReplica);
            byte[] partitionRoot = PartitionRoot(partitionKey);
            using StorageSnapshot payloadSnapshot = _storage.OpenSnapshot();
            MetadataOutboundItem[] page = ReadOutboundStoragePage(
                payloadSnapshot, partitionRoot, state, namespaceId, authorStream, cursor, limit,
                out bool byteBudgetLimited);
            if (page.Length == 0)
                throw new InvalidDataException("Outbound high-watermark has no recoverable payload rows.");
            if (progress.AckedCursor == 0 && progress.StreamOffset == 0 &&
                page.Length > 0 && page[0].Sequence > 1)
                throw new SyncException("SNAPSHOT_REQUIRED");
            int validationLimit = byteBudgetLimited ? page.Length : Math.Min(limit, MaxOutboundPageItems);
            ValidatePeerPageContinuity(page.Select(item => item.Sequence), cursor, high, page.Length, validationLimit, "outbound");
            return new MetadataOutboundPage(page, page.Length == 0 ? cursor : page[^1].Sequence);
        }
    }

    public MetadataRelayPage ReadRelayForPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream, int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            if (limit < 0) throw new ArgumentOutOfRangeException(nameof(limit));
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), 0);
            ValidatePeerEpochForAccess(state, namespaceId, peer, peerEpoch);
            if (!state.CompletedPeerBaselines.Contains(new InterestedPeerKey(namespaceId, peer, peerEpoch)))
                throw new SyncException("SNAPSHOT_REQUIRED");
            var streamKey = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            var progress = state.PeerStreams.TryGetValue(streamKey, out var saved)
                ? saved : EmptyPeerStreamValue();
            ulong cursor = checked(progress.AckedCursor + progress.StreamOffset);
            var key = new RelayKey(namespaceId, authorStream);
            ulong high = state.RelayHighWatermarks.TryGetValue(key, out var watermark) ? watermark : 0;
            if (cursor >= high || limit == 0)
                return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), Math.Min(cursor, high));
            var partitionKey = new PartitionKey(scopeId, localReplica);
            byte[] partitionRoot = PartitionRoot(partitionKey);
            using StorageSnapshot payloadSnapshot = _storage.OpenSnapshot();
            MetadataRelayItem[] page = ReadRelayStoragePage(
                payloadSnapshot, partitionRoot, state, namespaceId, authorStream, cursor, limit,
                out bool byteBudgetLimited);
            if (page.Length == 0)
                throw new InvalidDataException("Relay high-watermark has no recoverable payload rows.");
            if (progress.AckedCursor == 0 && progress.StreamOffset == 0 &&
                page.Length > 0 && page[0].Sequence > 1)
                throw new SyncException("SNAPSHOT_REQUIRED");
            int validationLimit = byteBudgetLimited ? page.Length : Math.Min(limit, MaxOutboundPageItems);
            ValidatePeerPageContinuity(page.Select(item => item.Sequence), cursor, high, page.Length, validationLimit, "relay");
            return new MetadataRelayPage(page, page.Length == 0 ? cursor : page[^1].Sequence);
        }
    }

    private static void ValidatePeerPageContinuity(IEnumerable<ulong> sequences,
        ulong cursor, ulong highWatermark, int count, int limit, string streamName)
    {
        ulong expected = checked(cursor + 1);
        foreach (ulong sequence in sequences)
        {
            if (sequence != expected)
                throw new InvalidDataException($"Peer {streamName} page contains a gap after its durable cursor.");
            expected = checked(expected + 1);
        }
        ulong scanned = count == 0 ? cursor : expected - 1;
        if (count < limit && scanned < highWatermark)
            throw new InvalidDataException($"Peer {streamName} payload is missing below its durable high-watermark.");
    }

    public void RegisterInterestedPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, localReplica);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            var current = GetOrCreatePartition(partitionKey);
            ValidatePeerEpochForAccess(current, namespaceId, peer, peerEpoch);
            var rosterKey = new InterestedPeerKey(namespaceId, peer, peerEpoch);
            if (current.InterestedPeers.Contains(rosterKey)) return;

            var candidateStreams = current.OutboundHighWatermarks.Keys
                .Where(key => key.NamespaceId == namespaceId && key.Author != peer)
                .Select(key => new PeerStreamKey(peer, peerEpoch, namespaceId, key.Author))
                .Concat(current.RelayHighWatermarks.Keys
                    .Where(key => key.NamespaceId == namespaceId && key.Author != peer)
                    .Select(key => new PeerStreamKey(peer, peerEpoch, namespaceId, key.Author)))
                .Distinct()
                .ToArray();
            var streamKeys = candidateStreams.Where(key => !current.PeerStreams.ContainsKey(key)).ToArray();
            var requiredKeys = candidateStreams.Where(key => !current.RequiredPeerStreams.Contains(key)).ToArray();
            var writes = new List<(byte[] Key, byte[] Value)>(1 + streamKeys.Length + requiredKeys.Length);
            writes.Add((EncodeInterestedPeerKey(partitionKey, rosterKey), Array.Empty<byte>()));
            foreach (var key in streamKeys)
                writes.Add((EncodePeerStreamKey(partitionKey, key), EncodePeerStreamState(EmptyPeerStreamValue())));
            foreach (var key in requiredKeys)
                writes.Add((EncodeRequiredPeerStreamKey(partitionKey, key), Array.Empty<byte>()));

            CommitPeerMetadataDelta(partitionKey, current, writes, Array.Empty<byte[]>(), static () => { });
        }
    }

    public IReadOnlyList<MetadataInterestedPeer> ReadInterestedPeers(
        string scopeId, NodeId localReplica, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return Array.Empty<MetadataInterestedPeer>();
            return state.InterestedPeers.Where(key => key.NamespaceId == namespaceId)
                .Select(key => new MetadataInterestedPeer(key.Peer, key.PeerEpoch, key.NamespaceId))
                .OrderBy(peer => Convert.ToHexString(peer.Peer.ToArray()), StringComparer.Ordinal)
                .ThenBy(peer => peer.PeerEpoch)
                .ToArray();
        }
    }

    public bool IsPeerInitialSyncComplete(
        string scopeId, NodeId localReplica, NodeId peer, uint peerEpoch, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return false;
            ValidatePeerEpochForAccess(state, namespaceId, peer, peerEpoch);
            return state.CompletedPeerBaselines.Contains(new InterestedPeerKey(namespaceId, peer, peerEpoch));
        }
    }

    public void CompletePeerInitialSync(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId,
        IReadOnlyList<MetadataPeerStreamBaseline> streamBaselines)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, localReplica);
            ArgumentNullException.ThrowIfNull(streamBaselines);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            var current = GetOrCreatePartition(partitionKey);
            ValidatePeerEpochForAccess(current, namespaceId, peer, peerEpoch);
            var rosterKey = new InterestedPeerKey(namespaceId, peer, peerEpoch);
            if (!current.InterestedPeers.Contains(rosterKey))
                throw new InvalidOperationException("Peer must be durably registered before initial sync completion.");

            var seen = new HashSet<NodeId>();
            foreach (var baseline in streamBaselines)
            {
                if (baseline is null || baseline.AuthorStream.IsZero ||
                    baseline.AuthorStream == peer || baseline.StreamOffset > long.MaxValue)
                    throw new ArgumentException("Initial-sync author baseline is invalid.", nameof(streamBaselines));
                if (!seen.Add(baseline.AuthorStream))
                    throw new ArgumentException("Initial-sync author baselines must be unique.", nameof(streamBaselines));

                var outboundKey = new OutboundKey(namespaceId, baseline.AuthorStream);
                ulong outboundHigh = current.OutboundHighWatermarks.TryGetValue(outboundKey, out var outHigh)
                    ? outHigh : 0;
                bool hasRelayHistory = current.RelayHighWatermarks.ContainsKey(
                    new RelayKey(namespaceId, baseline.AuthorStream));
                if (baseline.StreamOffset > outboundHigh || (hasRelayHistory && baseline.StreamOffset != 0))
                    throw new InvalidDataException("Snapshot baseline cannot skip relay history or exceed the outbound feed watermark.");
            }

            if (!current.CompletedPeerBaselines.Contains(rosterKey))
            {
                var requiredAuthors = current.PeerStreams.Keys
                    .Where(key => key.Peer == peer && key.PeerEpoch == peerEpoch && key.NamespaceId == namespaceId)
                    .Select(key => key.AuthorStream)
                    .Concat(current.RequiredPeerStreams
                        .Where(key => key.Peer == peer && key.PeerEpoch == peerEpoch && key.NamespaceId == namespaceId)
                        .Select(key => key.AuthorStream))
                    .Distinct();
                if (requiredAuthors.Any(author => !seen.Contains(author)))
                    throw new InvalidDataException("Initial-sync baseline must cover every currently registered peer author stream.");
            }

            if (current.CompletedPeerBaselines.Contains(rosterKey))
            {
                foreach (var baseline in streamBaselines)
                {
                    var key = new PeerStreamKey(peer, peerEpoch, namespaceId, baseline.AuthorStream);
                    if (!current.PeerStreams.TryGetValue(key, out var saved) ||
                        saved.StreamOffset != baseline.StreamOffset || saved.IssuerEpoch != baseline.IssuerEpoch)
                        throw new InvalidDataException("Repeated initial-sync completion conflicts with its durable feed baseline.");
                }
                return;
            }

            var baselineWrites = new List<(byte[] Key, byte[] Value)>(streamBaselines.Count * 2 + 1);
            var baselineStates = new List<(PeerStreamKey Key, PeerStreamStateValue Value)>(streamBaselines.Count);
            var newRequired = new List<PeerStreamKey>(streamBaselines.Count);
            foreach (var baseline in streamBaselines)
            {
                var key = new PeerStreamKey(peer, peerEpoch, namespaceId, baseline.AuthorStream);
                if (current.PeerStreams.TryGetValue(key, out var saved) &&
                    (saved.AckedCursor != 0 || saved.PushedCursor != 0 || saved.PendingBatch is not null))
                    throw new InvalidOperationException("Initial snapshot baseline cannot reset an active peer author stream.");
                var value = new PeerStreamStateValue(
                    0, 0, baseline.StreamOffset, baseline.IssuerEpoch, new byte[16], null);
                baselineStates.Add((key, value));
                baselineWrites.Add((EncodePeerStreamKey(partitionKey, key), EncodePeerStreamState(value)));
                if (!current.RequiredPeerStreams.Contains(key))
                {
                    newRequired.Add(key);
                    baselineWrites.Add((EncodeRequiredPeerStreamKey(partitionKey, key), Array.Empty<byte>()));
                }
            }
            baselineWrites.Add((EncodeCompletedPeerBaselineKey(partitionKey, rosterKey), Array.Empty<byte>()));
            CommitPeerMetadataDelta(partitionKey, current, baselineWrites, Array.Empty<byte[]>(), static () => { });
        }
    }


    /// <summary>
    /// Atomically installs the two-sided MBR1 send/receive baseline for this local
    /// replica. The exact accepted roster, all active-member FRA2 ACKs and all
    /// member-signed reports are revalidated while the shared Storage transaction
    /// gate is held. Payload rows are deliberately not compacted here.
    /// </summary>
    public bool ApplyFullReplaceBaseline(
        string scopeId, NodeId localReplica,
        NamespaceRosterCertificate acceptedRoster, Guid generation,
        ReadOnlySpan<byte> snapshotDigest)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        if (generation == Guid.Empty || snapshotDigest.Length != MetadataBaselineReportProtocol.DigestSize)
            throw new ArgumentException("A FULL_REPLACE generation and 32-byte snapshot digest are required.");

        lock (_gate)
        {
            ThrowIfDisposed();
            StorageNamespaceGcStore gcStore = _namespaceGcStore
                ?? throw new InvalidOperationException("Attach the durable namespace-GC store before applying a FULL_REPLACE baseline.");
            TrustStore trustStore = _namespaceGcTrustStore
                ?? throw new InvalidOperationException("Namespace-GC trust store is not attached.");
            if (!gcStore.IsBoundTo(_storage, scopeId, localReplica))
                throw new InvalidOperationException("Namespace-GC store is not bound to this exact Metadata scope and local replica.");
            if (!gcStore.TryReadNamespaceRoster(acceptedRoster.NamespaceId, trustStore,
                    out NamespaceRosterCertificate? currentRoster) || currentRoster is null ||
                currentRoster.RosterGeneration != acceptedRoster.RosterGeneration ||
                !NamespaceRosterProtocol.ComputeDigest(currentRoster).AsSpan()
                    .SequenceEqual(NamespaceRosterProtocol.ComputeDigest(acceptedRoster)))
                throw new InvalidDataException("FULL_REPLACE baseline roster is not the current accepted namespace roster.");
            if (!gcStore.AreAllActiveMembersInstalled(acceptedRoster, generation, snapshotDigest))
                throw new InvalidOperationException("FULL_REPLACE baseline requires an installed FRA2 ACK from every Active roster member.");
            if (!gcStore.TryReadRetentionCheckpoint(acceptedRoster, trustStore,
                    out NamespaceRetentionCheckpoint? retentionCheckpoint) || retentionCheckpoint is null ||
                retentionCheckpoint.FullReplaceGeneration != generation ||
                !retentionCheckpoint.FullReplaceDigest.AsSpan().SequenceEqual(snapshotDigest))
                throw new InvalidOperationException("FULL_REPLACE baseline requires the current signed RCP2 checkpoint bound to the exact generation and snapshot digest.");

            IReadOnlyList<MetadataBaselineReport> reports =
                gcStore.ReadMetadataBaselineReports(acceptedRoster, generation, snapshotDigest);
            NamespaceRosterMember[] activeMembers = acceptedRoster.Members
                .Where(member => member.Status == NamespaceMemberStatus.Active)
                .OrderBy(member => Convert.ToHexString(member.NodeId.ToArray()), StringComparer.Ordinal)
                .ToArray();
            if (reports.Count != activeMembers.Length)
                throw new InvalidOperationException("FULL_REPLACE baseline requires one accepted MBR1 report from every Active roster member.");

            var reportsByMember = new Dictionary<NodeId, MetadataBaselineReport>();
            int? snapshotEntryCount = null;
            foreach (MetadataBaselineReport report in reports)
            {
                if (report.NamespaceId != acceptedRoster.NamespaceId ||
                    report.RosterGeneration != acceptedRoster.RosterGeneration ||
                    report.FullReplaceGeneration != generation ||
                    !report.SnapshotDigest.AsSpan().SequenceEqual(snapshotDigest) ||
                    !report.RosterDigest.AsSpan().SequenceEqual(NamespaceRosterProtocol.ComputeDigest(acceptedRoster)) ||
                    !reportsByMember.TryAdd(report.MemberNodeId, report))
                    throw new InvalidDataException("FULL_REPLACE baseline report set contains a duplicate or mismatched member envelope.");
                snapshotEntryCount ??= report.SnapshotEntryCount;
                if (snapshotEntryCount.Value != report.SnapshotEntryCount)
                    throw new InvalidDataException("Active members disagree on the installed FULL_REPLACE snapshot entry count.");
                if (retentionCheckpoint!.FullReplaceHlcWallFloor < report.HlcWallFloor ||
                    (retentionCheckpoint.FullReplaceHlcWallFloor == report.HlcWallFloor &&
                     retentionCheckpoint.FullReplaceHlcLogicalFloor < report.HlcLogicalFloor))
                    throw new InvalidDataException("RCP2 FULL_REPLACE HLC floor is behind a member's installed snapshot floor.");
            }
            foreach (NamespaceRosterMember member in activeMembers)
                if (!reportsByMember.ContainsKey(member.NodeId))
                    throw new InvalidOperationException("An Active roster member has not supplied its signed MBR1 report.");

            NamespaceRosterMember localMember = activeMembers.FirstOrDefault(member => member.NodeId == localReplica)
                ?? throw new InvalidOperationException("The local Metadata replica is not an Active member of the accepted roster.");
            MetadataBaselineReport localReport = reportsByMember[localReplica];
            if (localReport.MemberTransportEpoch != localMember.TransportPeerEpoch)
                throw new InvalidDataException("Local MBR1 report transport epoch does not match the accepted roster.");

            byte[] evidenceDigest = ComputeBaselineEvidenceDigest(reports);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            PartitionState current = GetOrCreatePartition(partitionKey);
            byte[] markerKey = EncodeBaselineApplyMarkerKey(
                partitionKey, acceptedRoster.NamespaceId, acceptedRoster.RosterGeneration, generation);
            byte[] markerValue = EncodeBaselineApplyMarker(
                acceptedRoster, generation, snapshotDigest, evidenceDigest,
                reports.Count, snapshotEntryCount ?? 0);
            using (var snapshot = _storage.OpenSnapshot())
            {
                if (snapshot.TryGet(markerKey, out byte[]? priorMarker))
                {
                    if (priorMarker!.AsSpan().SequenceEqual(markerValue))
                        return false;
                    throw new InvalidDataException("FULL_REPLACE generation was already applied with different MBR1 evidence.");
                }
            }

            int pendingApplies = current.PendingApplies.Values.Count(
                pending => pending.Batch.NamespaceId == acceptedRoster.NamespaceId);
            if (pendingApplies != 0)
                throw new InvalidOperationException($"FULL_REPLACE baseline is blocked by {pendingApplies} unresolved PendingApply intent(s).");
            int pendingReservations = current.PeerStreams.Count(pair =>
                pair.Key.NamespaceId == acceptedRoster.NamespaceId &&
                (pair.Value.PendingBatch is not null || pair.Value.PushedCursor != pair.Value.AckedCursor));
            if (pendingReservations != 0)
                throw new InvalidOperationException($"FULL_REPLACE baseline is blocked by {pendingReservations} immutable peer reservation(s).");

            int namespaceId = acceptedRoster.NamespaceId;
            var activePeerEpochs = activeMembers
                .Where(member => member.NodeId != localReplica)
                .ToDictionary(member => member.NodeId, member => member.TransportPeerEpoch);

            foreach (var activePeer in activePeerEpochs)
            {
                if (current.RetiredPeerEpochs.TryGetValue((namespaceId, activePeer.Key), out uint retiredThrough) &&
                    activePeer.Value <= retiredThrough)
                    throw new InvalidDataException("An Active peer transport epoch is already covered by a local retirement fence.");
                foreach (InterestedPeerKey existingPeer in current.InterestedPeers.Where(key =>
                             key.NamespaceId == namespaceId && key.Peer == activePeer.Key))
                {
                    if (existingPeer.PeerEpoch == activePeer.Value)
                        continue;
                    if (existingPeer.PeerEpoch > activePeer.Value ||
                        !current.RetiredPeerEpochs.TryGetValue((namespaceId, activePeer.Key), out uint fence) ||
                        existingPeer.PeerEpoch > fence)
                        throw new InvalidDataException("Local interested-peer epoch state is not fenced by the accepted roster.");
                }
            }

            var feedByKey = localReport.FeedEntries.ToDictionary(
                entry => (entry.TargetPeer, entry.TargetTransportEpoch, entry.AuthorStream));
            var outboundAuthors = current.OutboundHighWatermarks.Keys
                .Where(key => key.NamespaceId == namespaceId)
                .ToDictionary(key => key.Author, _ => MetadataBaselineFeedKind.Outbound);
            var relayAuthors = current.RelayHighWatermarks.Keys
                .Where(key => key.NamespaceId == namespaceId)
                .ToArray();
            foreach (var relay in relayAuthors)
            {
                if (!outboundAuthors.TryAdd(relay.Author, MetadataBaselineFeedKind.Relay))
                    throw new InvalidDataException("One author stream is present in both local outbound and relay feed domains.");
            }

            foreach (var activePeer in activePeerEpochs)
            {
                foreach (NodeId author in outboundAuthors.Keys)
                {
                    if (author != activePeer.Key &&
                        !feedByKey.ContainsKey((activePeer.Key, activePeer.Value, author)))
                        throw new InvalidDataException("MBR1 feed vector omits a required outbound/relay stream for an Active peer.");
                }
                foreach (PeerStreamKey required in current.RequiredPeerStreams.Where(key =>
                             key.NamespaceId == namespaceId && key.Peer == activePeer.Key &&
                             key.PeerEpoch == activePeer.Value))
                    if (!feedByKey.ContainsKey((required.Peer, required.PeerEpoch, required.AuthorStream)))
                        throw new InvalidDataException("MBR1 feed vector omits a durably required peer stream.");
                foreach (PeerStreamKey existing in current.PeerStreams.Keys.Where(key =>
                             key.NamespaceId == namespaceId && key.Peer == activePeer.Key &&
                             key.PeerEpoch == activePeer.Value))
                    if (!feedByKey.ContainsKey((existing.Peer, existing.PeerEpoch, existing.AuthorStream)))
                        throw new InvalidDataException("MBR1 feed vector omits an existing Active peer stream.");
            }

            var writes = new List<(byte[] Key, byte[] Value)>();
            var deletes = new List<byte[]>();
            var peerUpdates = new List<(PeerStreamKey Key, PeerStreamStateValue Value)>();
            var requiredAdds = new HashSet<PeerStreamKey>();
            var interestedAdds = new HashSet<InterestedPeerKey>();
            var completedAdds = new HashSet<InterestedPeerKey>();

            foreach (var activePeer in activePeerEpochs)
            {
                var interestedKey = new InterestedPeerKey(namespaceId, activePeer.Key, activePeer.Value);
                if (!current.InterestedPeers.Contains(interestedKey))
                {
                    interestedAdds.Add(interestedKey);
                    writes.Add((EncodeInterestedPeerKey(partitionKey, interestedKey), Array.Empty<byte>()));
                }
                if (!current.CompletedPeerBaselines.Contains(interestedKey))
                {
                    completedAdds.Add(interestedKey);
                    writes.Add((EncodeCompletedPeerBaselineKey(partitionKey, interestedKey), Array.Empty<byte>()));
                }
            }

            foreach (MetadataBaselineFeedEntry feed in localReport.FeedEntries)
            {
                if (!activePeerEpochs.TryGetValue(feed.TargetPeer, out uint targetEpoch) ||
                    targetEpoch != feed.TargetTransportEpoch)
                    throw new InvalidDataException("MBR1 feed row targets a peer outside the exact Active roster epoch.");
                if (!outboundAuthors.TryGetValue(feed.AuthorStream, out MetadataBaselineFeedKind sourceKind) ||
                    sourceKind != feed.FeedKind)
                    throw new InvalidDataException("MBR1 feed kind does not match the local author's durable outbound/relay domain.");

                ulong highWatermark = feed.FeedKind == MetadataBaselineFeedKind.Outbound
                    ? current.OutboundHighWatermarks[new OutboundKey(namespaceId, feed.AuthorStream)]
                    : current.RelayHighWatermarks[new RelayKey(namespaceId, feed.AuthorStream)];
                if (feed.AbsoluteFeedOffset > highWatermark)
                    throw new InvalidDataException("MBR1 snapshot feed offset exceeds the local durable feed high-watermark.");
                if (feed.FeedKind == MetadataBaselineFeedKind.Relay && feed.AbsoluteFeedOffset != 0)
                    throw new InvalidDataException("FULL_REPLACE cannot skip relay history; relay baseline offset must remain zero.");

                MetadataBaselineReport targetReport = reportsByMember[feed.TargetPeer];
                MetadataBaselineReceiveEntry? targetReceive = targetReport.ReceiveEntries.FirstOrDefault(entry =>
                    entry.SenderPeer == localReplica &&
                    entry.SenderTransportEpoch == localMember.TransportPeerEpoch &&
                    entry.AuthorStream == feed.AuthorStream &&
                    entry.IssuerEpoch == feed.IssuerEpoch);
                ulong receiverCursor = targetReceive?.ReceiveCursor ?? 0;
                byte[] receiverDigest = targetReceive?.LastBatchDigest ?? new byte[16];
                if (!receiverDigest.AsSpan().SequenceEqual(feed.DigestAnchor))
                    throw new InvalidDataException("Source feed digest does not match the target member's signed receive digest chain.");
                ulong absolutePeerCursor = checked(feed.AbsoluteFeedOffset + receiverCursor);
                if (absolutePeerCursor > highWatermark)
                    throw new InvalidDataException("Paired send/receive baseline exceeds the local durable feed high-watermark.");

                var streamKey = new PeerStreamKey(
                    feed.TargetPeer, feed.TargetTransportEpoch, namespaceId, feed.AuthorStream);
                if (current.PeerStreams.TryGetValue(streamKey, out PeerStreamStateValue? existing))
                {
                    if (existing.PendingBatch is not null || existing.PushedCursor != existing.AckedCursor)
                        throw new InvalidOperationException("An immutable outbound reservation blocks baseline application.");
                    bool hasProgress = existing.AckedCursor != 0 || existing.PushedCursor != 0 ||
                        existing.StreamOffset != 0 || existing.LastBatchDigest.Any(value => value != 0);
                    if (hasProgress)
                    {
                        if (existing.IssuerEpoch != feed.IssuerEpoch)
                            throw new InvalidDataException("Existing peer stream issuer epoch is incomparable with the signed baseline.");
                        ulong existingAbsolute = checked(existing.StreamOffset + existing.AckedCursor);
                        if (existingAbsolute > absolutePeerCursor ||
                            (existingAbsolute == absolutePeerCursor &&
                             !existing.LastBatchDigest.AsSpan().SequenceEqual(feed.DigestAnchor)))
                            throw new InvalidDataException("Signed baseline would regress or fork an already advanced peer stream.");
                    }
                }

                var value = new PeerStreamStateValue(
                    receiverCursor, receiverCursor, feed.AbsoluteFeedOffset,
                    feed.IssuerEpoch, feed.DigestAnchor.ToArray(), null);
                peerUpdates.Add((streamKey, value));
                writes.Add((EncodePeerStreamKey(partitionKey, streamKey), EncodePeerStreamState(value)));
                if (!current.RequiredPeerStreams.Contains(streamKey))
                {
                    requiredAdds.Add(streamKey);
                    writes.Add((EncodeRequiredPeerStreamKey(partitionKey, streamKey), Array.Empty<byte>()));
                }
            }

            var reportReceiveKeys = new HashSet<(NodeId Sender, uint IssuerEpoch, NodeId Author)>();
            foreach (MetadataBaselineReceiveEntry entry in localReport.ReceiveEntries)
                reportReceiveKeys.Add((entry.SenderPeer, entry.IssuerEpoch, entry.AuthorStream));

            foreach (var existing in current.Receives.Keys.Where(key => key.NamespaceId == namespaceId))
                if (!reportReceiveKeys.Contains((existing.Sender, existing.Epoch, existing.AuthorStream)))
                    throw new InvalidDataException("MBR1 receive vector omits an existing per-author receive cursor.");
            foreach (var existing in current.ReceiveChains.Keys.Where(key => key.NamespaceId == namespaceId))
                if (!reportReceiveKeys.Contains((existing.Sender, existing.Epoch, existing.AuthorStream)))
                    throw new InvalidDataException("MBR1 receive vector omits an existing per-author digest chain.");

            foreach (var legacy in current.LegacyReceiveChains.Keys.Where(key => key.NamespaceId == namespaceId))
            {
                if (!TryGetSingleEstablishedAuthor(current, legacy, out NodeId establishedAuthor) ||
                    !reportReceiveKeys.Contains((legacy.Sender, legacy.Epoch, establishedAuthor)) ||
                    current.ReceiveChains.ContainsKey(new ReceiveChainKey(
                        legacy.Sender, legacy.Epoch, namespaceId, establishedAuthor)))
                    throw new InvalidDataException("Ambiguous legacy sender-wide receive digest chain blocks FULL_REPLACE baseline.");
            }

            var receiveUpdates = new List<(ReceiveKey Key, ReceiveState Value, ReceiveChainKey ChainKey, ReceiveChainState ChainValue)>();
            var legacyDeletes = new HashSet<LegacyReceiveChainKey>();
            foreach (MetadataBaselineReceiveEntry entry in localReport.ReceiveEntries)
            {
                var receiveKey = new ReceiveKey(entry.SenderPeer, entry.IssuerEpoch, namespaceId, entry.AuthorStream);
                var chainKey = new ReceiveChainKey(entry.SenderPeer, entry.IssuerEpoch, namespaceId, entry.AuthorStream);
                bool hasCurrent = current.Receives.TryGetValue(receiveKey, out ReceiveState currentReceive);
                ulong currentCursor = hasCurrent ? currentReceive.CursorEnd : 0;
                byte[] currentDigest = new byte[16];
                if (current.ReceiveChains.TryGetValue(chainKey, out ReceiveChainState currentChain))
                    currentDigest = currentChain.LastBatchDigest.ToArray();
                else if (current.LegacyReceiveChains.TryGetValue(
                             new LegacyReceiveChainKey(entry.SenderPeer, entry.IssuerEpoch, namespaceId),
                             out ReceiveChainState legacyChain))
                    currentDigest = legacyChain.LastBatchDigest.ToArray();
                else if (currentCursor != 0)
                    throw new InvalidDataException("Existing receive cursor has no unambiguous per-author digest chain.");

                if (current.ReceiveChains.ContainsKey(chainKey) &&
                    current.LegacyReceiveChains.ContainsKey(
                        new LegacyReceiveChainKey(entry.SenderPeer, entry.IssuerEpoch, namespaceId)))
                    throw new InvalidDataException("Legacy and per-author receive digest rows coexist.");

                NamespaceRosterMember senderMember = acceptedRoster.Members.First(member => member.NodeId == entry.SenderPeer);
                MetadataBaselineFeedEntry? senderFeed = null;
                if (senderMember.Status == NamespaceMemberStatus.Active)
                {
                    MetadataBaselineReport senderReport = reportsByMember[entry.SenderPeer];
                    senderFeed = senderReport.FeedEntries.FirstOrDefault(feed =>
                        feed.TargetPeer == localReplica &&
                        feed.TargetTransportEpoch == localMember.TransportPeerEpoch &&
                        feed.AuthorStream == entry.AuthorStream &&
                        feed.IssuerEpoch == entry.IssuerEpoch);
                    if (senderFeed is null)
                    {
                        MetadataBaselineFeedEntry? newerEpoch = senderReport.FeedEntries.FirstOrDefault(feed =>
                            feed.TargetPeer == localReplica &&
                            feed.TargetTransportEpoch == localMember.TransportPeerEpoch &&
                            feed.AuthorStream == entry.AuthorStream &&
                            feed.IssuerEpoch > entry.IssuerEpoch);
                        if (newerEpoch is null && (entry.ReceiveCursor != 0 || currentCursor != 0))
                            throw new InvalidDataException("Active sender receive stream has no matching signed source feed.");
                    }
                    else if (!senderFeed.DigestAnchor.AsSpan().SequenceEqual(entry.LastBatchDigest) &&
                             currentCursor <= entry.ReceiveCursor)
                        throw new InvalidDataException("Receiver digest does not match the active sender's signed feed digest.");
                }

                ulong selectedCursor;
                byte[] selectedDigest;
                if (currentCursor > entry.ReceiveCursor)
                {
                    if (senderMember.Status == NamespaceMemberStatus.Active &&
                        (senderFeed is null || !senderFeed.DigestAnchor.AsSpan().SequenceEqual(currentDigest)))
                        throw new InvalidDataException("A locally advanced receive stream is not confirmed by the sender's signed feed digest.");
                    selectedCursor = currentCursor;
                    selectedDigest = currentDigest;
                }
                else
                {
                    if (currentCursor == entry.ReceiveCursor &&
                        !currentDigest.AsSpan().SequenceEqual(entry.LastBatchDigest))
                        throw new InvalidDataException("Equal receive cursors have conflicting digest anchors.");
                    selectedCursor = entry.ReceiveCursor;
                    selectedDigest = entry.LastBatchDigest.ToArray();
                }

                var legacyKey = new LegacyReceiveChainKey(entry.SenderPeer, entry.IssuerEpoch, namespaceId);
                if (current.LegacyReceiveChains.ContainsKey(legacyKey))
                {
                    legacyDeletes.Add(legacyKey);
                    deletes.Add(EncodeLegacyReceiveChainKey(partitionKey, legacyKey));
                }

                byte[] receiveSuffix = new byte[38];
                entry.SenderPeer.WriteTo(receiveSuffix);
                BinaryPrimitives.WriteUInt32LittleEndian(receiveSuffix.AsSpan(16), entry.IssuerEpoch);
                BinaryPrimitives.WriteUInt16LittleEndian(receiveSuffix.AsSpan(20), checked((ushort)namespaceId));
                entry.AuthorStream.WriteTo(receiveSuffix.AsSpan(22));
                writes.Add((StorageKey(PartitionRoot(partitionKey), 2, receiveSuffix), EncodeUInt64(selectedCursor)));
                writes.Add((EncodeReceiveChainKey(partitionKey, chainKey), selectedDigest));
                receiveUpdates.Add((receiveKey, new ReceiveState(selectedCursor),
                    chainKey, new ReceiveChainState(selectedDigest)));
            }

            foreach (NamespaceRosterMember retired in acceptedRoster.Members.Where(member =>
                         member.Status == NamespaceMemberStatus.Retired && member.RetiredThroughEpoch != 0))
            {
                if (!current.RetiredPeerEpochs.TryGetValue((namespaceId, retired.NodeId), out uint priorFence) ||
                    priorFence < retired.RetiredThroughEpoch)
                {
                    byte[] suffix = new byte[18];
                    BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
                    retired.NodeId.WriteTo(suffix.AsSpan(2));
                    byte[] value = new byte[4];
                    BinaryPrimitives.WriteUInt32LittleEndian(value, retired.RetiredThroughEpoch);
                    writes.Add((StorageKey(PartitionRoot(partitionKey), 18, suffix), value));
                }
            }

            writes.Add((markerKey, markerValue));
            CommitPeerMetadataDelta(partitionKey, current, writes, deletes, static () => { });
            return true;
        }
    }

    private static byte[] ComputeBaselineEvidenceDigest(IReadOnlyList<MetadataBaselineReport> reports)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(Encoding.UTF8.GetBytes("BIN-SYNC/METADATA-FULL-REPLACE-BASELINE-SET/v1"));
        Span<byte> length = stackalloc byte[4];
        foreach (MetadataBaselineReport report in reports.OrderBy(
                     item => Convert.ToHexString(item.MemberNodeId.ToArray()), StringComparer.Ordinal))
        {
            byte[] envelope = MetadataBaselineReportProtocol.EncodeEnvelope(report);
            BinaryPrimitives.WriteInt32LittleEndian(length, envelope.Length);
            hash.AppendData(length);
            hash.AppendData(envelope);
        }
        return hash.GetHashAndReset();
    }

    private static byte[] EncodeBaselineApplyMarkerKey(
        PartitionKey partitionKey, int namespaceId, ulong rosterGeneration, Guid generation)
    {
        byte[] suffix = new byte[26];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        BinaryPrimitives.WriteUInt64LittleEndian(suffix.AsSpan(2), rosterGeneration);
        generation.TryWriteBytes(suffix.AsSpan(10), bigEndian: true, out _);
        return StorageKey(PartitionRoot(partitionKey), 26, suffix);
    }

    private static byte[] EncodeBaselineApplyMarker(
        NamespaceRosterCertificate roster, Guid generation, ReadOnlySpan<byte> snapshotDigest,
        ReadOnlySpan<byte> evidenceDigest, int reportCount, int snapshotEntryCount)
    {
        byte[] value = new byte[109];
        "MBAC"u8.CopyTo(value);
        value[4] = 1;
        snapshotDigest.CopyTo(value.AsSpan(5, 32));
        evidenceDigest.CopyTo(value.AsSpan(37, 32));
        NamespaceRosterProtocol.ComputeDigest(roster).CopyTo(value, 69);
        BinaryPrimitives.WriteInt32LittleEndian(value.AsSpan(101), reportCount);
        BinaryPrimitives.WriteInt32LittleEndian(value.AsSpan(105), snapshotEntryCount);
        return value;
    }

    private static byte[] EncodeReceiveStateKey(PartitionKey partitionKey, ReceiveKey receive)
    {
        byte[] suffix = new byte[38];
        receive.Sender.WriteTo(suffix);
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), receive.Epoch);
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)receive.NamespaceId));
        receive.AuthorStream.WriteTo(suffix.AsSpan(22));
        return StorageKey(PartitionRoot(partitionKey), 2, suffix);
    }

    public uint? GetRetiredPeerEpoch(string scopeId, NodeId localReplica, NodeId peer, int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, localReplica);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return null;
            return state.RetiredPeerEpochs.TryGetValue((namespaceId, peer), out uint retiredThrough)
                ? retiredThrough : null;
        }
    }

    public void RetireInterestedPeer(string scopeId, NodeId localReplica,
        NodeId peer, uint retiredPeerEpoch, uint replacementPeerEpoch, int namespaceId)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, localReplica);
            if (replacementPeerEpoch <= retiredPeerEpoch)
                throw new ArgumentOutOfRangeException(nameof(replacementPeerEpoch),
                    "Replacement peer epoch must be newer than the retired incarnation.");

            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out var current))
                throw new InvalidOperationException("The replacement peer epoch has not been registered.");

            var replacement = new InterestedPeerKey(namespaceId, peer, replacementPeerEpoch);
            if (!current.InterestedPeers.Contains(replacement))
                throw new InvalidOperationException("The replacement peer epoch must be durably registered before retirement.");
            if (!current.CompletedPeerBaselines.Contains(replacement))
                throw new InvalidOperationException(
                    "The replacement peer epoch must complete its durable initial-sync baseline before retirement.");
            uint latestActiveEpoch = current.InterestedPeers
                .Where(key => key.NamespaceId == namespaceId && key.Peer == peer)
                .Select(key => key.PeerEpoch).DefaultIfEmpty(0U).Max();
            if (latestActiveEpoch != replacementPeerEpoch)
                throw new InvalidOperationException("Only the newest registered peer epoch can retire an older incarnation.");

            var retiredKey = (NamespaceId: namespaceId, Peer: peer);
            bool hasRetiredMarker = current.RetiredPeerEpochs.TryGetValue(retiredKey, out uint retiredThrough);
            bool alreadyRetired = hasRetiredMarker && retiredThrough >= retiredPeerEpoch;
            uint nextRetiredThrough = Math.Max(hasRetiredMarker ? retiredThrough : 0, retiredPeerEpoch);
            var oldRosterKey = new InterestedPeerKey(namespaceId, peer, retiredPeerEpoch);
            if (current.PendingApplies.Values.Any(intent =>
                    intent.Batch.SenderPeer == peer && intent.Batch.NamespaceId == namespaceId &&
                    (intent.Batch.TransportPeerEpoch == 0 ||
                     intent.Batch.TransportPeerEpoch <= nextRetiredThrough)))
                throw new InvalidOperationException(
                    "Cannot retire a transport peer epoch while one of its PendingApply intents is unresolved; recover or finalize it first.");

            // An old immutable reservation is resolved either by its exact ACK or by
            // the replacement incarnation's completed authoritative snapshot for the
            // same author stream. The replacement baseline must cover the reservation's
            // absolute end cursor (old StreamOffset + CursorEnd); a missing/short baseline
            // keeps retirement fenced. Epoch-zero reservations have unknown provenance
            // and can never be discharged by a newer baseline.
            foreach (var pair in current.PeerStreams.Where(pair =>
                         pair.Key.NamespaceId == namespaceId &&
                         pair.Key.Peer == peer &&
                         pair.Key.PeerEpoch <= nextRetiredThrough &&
                         pair.Value.PendingBatch is not null))
            {
                if (pair.Key.PeerEpoch == 0)
                    throw new InvalidOperationException(
                        "Cannot retire a transport peer epoch while an epoch-unknown outbound reservation remains unresolved.");
                var replacementStreamKey = new PeerStreamKey(
                    peer, replacementPeerEpoch, namespaceId, pair.Key.AuthorStream);
                if (!current.PeerStreams.TryGetValue(replacementStreamKey, out var replacementProgress))
                    throw new InvalidOperationException(
                        "Cannot retire a transport peer epoch while its replacement snapshot omits an author stream with a pending reservation.");
                if (replacementProgress.IssuerEpoch != pair.Value.PendingBatch!.IssuerEpoch ||
                    pair.Value.IssuerEpoch != pair.Value.PendingBatch.IssuerEpoch)
                    throw new InvalidOperationException(
                        "Cannot compare a pending reservation cursor with a replacement baseline from a different author epoch.");
                ulong oldAbsoluteEnd;
                ulong replacementBaseline;
                try
                {
                    oldAbsoluteEnd = checked(pair.Value.StreamOffset + pair.Value.PendingBatch!.CursorEnd);
                    replacementBaseline = checked(replacementProgress.AckedCursor + replacementProgress.StreamOffset);
                }
                catch (OverflowException ex)
                {
                    throw new InvalidDataException("Peer stream retirement cursor overflowed.", ex);
                }
                if (replacementBaseline < oldAbsoluteEnd)
                    throw new InvalidOperationException(
                        "Cannot retire a transport peer epoch while its replacement snapshot baseline is behind an immutable outbound reservation.");
            }

            // Keep receipts until the separate receipt-GC path proves that no
            // host-side reference remains. The durable retired-epoch fence makes
            // old transport replays impossible without eagerly deleting receipts.
            bool hasOldRows =
                current.InterestedPeers.Contains(oldRosterKey) ||
                current.PeerStreams.Keys.Any(key => key.NamespaceId == namespaceId &&
                    key.Peer == peer && key.PeerEpoch == retiredPeerEpoch) ||
                current.RequiredPeerStreams.Any(key => key.NamespaceId == namespaceId &&
                    key.Peer == peer && key.PeerEpoch == retiredPeerEpoch) ||
                current.CompletedPeerBaselines.Contains(oldRosterKey);
            if (alreadyRetired && !hasOldRows)
                return;

            var oldStreams = current.PeerStreams.Keys
                .Where(key => key.NamespaceId == namespaceId && key.Peer == peer &&
                    key.PeerEpoch == retiredPeerEpoch).ToArray();
            var oldRequired = current.RequiredPeerStreams
                .Where(key => key.NamespaceId == namespaceId && key.Peer == peer &&
                    key.PeerEpoch == retiredPeerEpoch).ToArray();
            var deletes = new List<byte[]>(oldStreams.Length + oldRequired.Length + 2)
            {
                EncodeInterestedPeerKey(partitionKey, oldRosterKey),
                EncodeCompletedPeerBaselineKey(partitionKey, oldRosterKey)
            };
            deletes.AddRange(oldStreams.Select(key => EncodePeerStreamKey(partitionKey, key)));
            deletes.AddRange(oldRequired.Select(key => EncodeRequiredPeerStreamKey(partitionKey, key)));
            byte[] retiredValue = new byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(retiredValue, nextRetiredThrough);
            var writes = new[]
            {
                (EncodeRetiredPeerEpochKey(partitionKey, namespaceId, peer), retiredValue)
            };
            CommitPeerMetadataDelta(partitionKey, current, writes, deletes, static () => { });
        }
    }

    public void RegisterInterestedPeerStream(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream,
        ulong baselineAckedCursor, ulong streamOffset, uint issuerEpoch, byte[] lastBatchDigest)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            if (baselineAckedCursor > long.MaxValue || streamOffset > long.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(baselineAckedCursor));
            ValidateDigest(lastBatchDigest, nameof(lastBatchDigest));
            var partitionKey = new PartitionKey(scopeId, localReplica);
            var current = GetOrCreatePartition(partitionKey);
            ValidatePeerEpochForAccess(current, namespaceId, peer, peerEpoch);
            var key = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            var rosterKey = new InterestedPeerKey(namespaceId, peer, peerEpoch);
            bool hasExisting = current.PeerStreams.TryGetValue(key, out var existing);
            if (hasExisting && IsPristinePeerStream(existing!))
            {
                bool sameBaseline = existing!.AckedCursor == baselineAckedCursor &&
                    existing.StreamOffset == streamOffset &&
                    existing.IssuerEpoch == issuerEpoch &&
                    existing.LastBatchDigest.AsSpan().SequenceEqual(lastBatchDigest);
                if (sameBaseline && current.RequiredPeerStreams.Contains(key))
                {
                    if (baselineAckedCursor > 0)
                        PersistPeerBaselineMarker(partitionKey, current, rosterKey);
                    return;
                }
                PersistPeerStreamState(partitionKey, current, key,
                    new PeerStreamStateValue(baselineAckedCursor, baselineAckedCursor,
                        streamOffset, issuerEpoch, lastBatchDigest.ToArray(), null),
                    ensureRequired: true);
                if (baselineAckedCursor > 0)
                    PersistPeerBaselineMarker(partitionKey, current, rosterKey);
                return;
            }
            if (hasExisting)
            {
                // A live or already-advanced stream is never reset by a repeated
                // membership/snapshot registration call.
                if (current.RequiredPeerStreams.Contains(key))
                {
                    if (baselineAckedCursor > 0)
                        PersistPeerBaselineMarker(partitionKey, current, rosterKey);
                    return;
                }
                throw new InvalidDataException("Interested peer registration conflicts with existing stream progress.");
            }

            var baseline = new PeerStreamStateValue(baselineAckedCursor, baselineAckedCursor,
                streamOffset, issuerEpoch, lastBatchDigest.ToArray(), null);
            PersistPeerStreamState(partitionKey, current, key, baseline, ensureRequired: true);
            if (baselineAckedCursor > 0)
                PersistPeerBaselineMarker(partitionKey, current, rosterKey);
        }
    }

    public ulong GetSafePeerStreamCompactionCursor(
        string scopeId, NodeId localReplica, int namespaceId, NodeId authorStream)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (string.IsNullOrWhiteSpace(scopeId)) throw new ArgumentException("ScopeId is required.", nameof(scopeId));
            if (localReplica.IsZero || authorStream.IsZero) throw new ArgumentException("Replica and author identities must be non-zero.");
            if ((uint)namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return 0;
            return GetSafePeerCursor(state, namespaceId, authorStream);
        }
    }

    public MetadataPeerStreamState GetPeerStreamState(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state) ||
                !state.PeerStreams.TryGetValue(new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream), out var value))
                return EmptyPeerStreamState();
            return ToPublicPeerStreamState(value);
        }
    }

    public MetadataOutboundBatchReservation? GetPendingOutboundBatch(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream)
        => GetPeerStreamState(scopeId, localReplica, peer, peerEpoch, namespaceId, authorStream).PendingBatch;

    public MetadataOutboundBatchReservation ReserveOutboundBatch(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream,
        MetadataOutboundBatchReservation reservation)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            ValidateReservation(reservation);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            var current = GetOrCreatePartition(partitionKey);
            var key = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            if (current.PeerStreams.TryGetValue(key, out var existing) && existing.PendingBatch is not null)
            {
                ValidatePendingReservationEpoch(current, namespaceId, peer, peerEpoch);
                return CloneReservation(existing.PendingBatch);
            }
            ValidatePeerEpochForAccess(current, namespaceId, peer, peerEpoch);
            if (!current.CompletedPeerBaselines.Contains(new InterestedPeerKey(namespaceId, peer, peerEpoch)))
                throw new SyncException("SNAPSHOT_REQUIRED");

            if (!current.PeerStreams.TryGetValue(key, out existing))
                throw new SyncException("SNAPSHOT_REQUIRED");
            var prior = existing;
            if (prior.StreamOffset != reservation.StreamOffset)
                throw new InvalidDataException("Outbound reservation changed the durable snapshot stream offset.");
            if (reservation.CursorStart != checked(prior.AckedCursor + 1))
                throw new InvalidDataException("Outbound batch reservation is not contiguous with the durable peer ACK cursor.");
            byte[] expectedPreviousDigest = prior.IssuerEpoch == reservation.IssuerEpoch
                ? prior.LastBatchDigest
                : new byte[16];
            if (!reservation.PreviousBatchDigest.AsSpan().SequenceEqual(expectedPreviousDigest))
                throw new InvalidDataException("Outbound batch reservation does not extend the durable peer digest chain.");

            PersistPeerStreamState(partitionKey, current, key, prior with
            {
                IssuerEpoch = reservation.IssuerEpoch,
                LastBatchDigest = expectedPreviousDigest.ToArray(),
                PendingBatch = CloneReservation(reservation)
            }, ensureRequired: true);
            return CloneReservation(reservation);
        }
    }

    public void MarkOutboundBatchPushed(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream, BatchId batchId)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out var current))
                throw new InvalidOperationException("No durable outbound reservation exists.");
            var key = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            if (!current.PeerStreams.TryGetValue(key, out var prior) ||
                prior.PendingBatch is null || prior.PendingBatch.BatchId != batchId)
                throw new InvalidOperationException("Outbound batch is not the active durable reservation.");
            ValidatePendingReservationEpoch(current, namespaceId, peer, peerEpoch);
            if (prior.PushedCursor >= prior.PendingBatch.CursorEnd) return;
            PersistPeerStreamState(partitionKey, current, key,
                prior with { PushedCursor = prior.PendingBatch.CursorEnd }, ensureRequired: true);
        }
    }

    public void AcknowledgePeerBatch(string scopeId, NodeId localReplica,
        NodeId peer, uint peerEpoch, int namespaceId, NodeId authorStream,
        BatchId batchId, ulong cursorEnd, byte[] batchDigest)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePeerStreamArguments(scopeId, localReplica, peer, namespaceId, authorStream);
            ValidateDigest(batchDigest, nameof(batchDigest));
            var partitionKey = new PartitionKey(scopeId, localReplica);
            if (!TryGetPartition(partitionKey, out var current))
                throw new InvalidOperationException("No durable outbound reservation exists.");
            var key = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
            if (!current.PeerStreams.TryGetValue(key, out var prior) || prior.PendingBatch is null)
                throw new InvalidOperationException("ACK has no matching durable outbound reservation.");
            ValidatePendingReservationEpoch(current, namespaceId, peer, peerEpoch);
            var pending = prior.PendingBatch;
            if (pending.BatchId != batchId || pending.CursorEnd != cursorEnd ||
                !pending.BatchDigest.AsSpan().SequenceEqual(batchDigest) || prior.PushedCursor < cursorEnd)
                throw new InvalidDataException("ACK does not match the pushed durable outbound reservation.");

            PersistPeerStreamState(partitionKey, current, key, prior with
            {
                AckedCursor = cursorEnd,
                LastBatchDigest = batchDigest.ToArray(),
                PendingBatch = null
            }, ensureRequired: true);
        }
    }

    private static bool IsPristinePeerStream(PeerStreamStateValue value)
        => value.AckedCursor == 0 && value.PushedCursor == 0 && value.StreamOffset == 0 &&
           value.IssuerEpoch == 0 && value.PendingBatch is null &&
           value.LastBatchDigest.AsSpan().IndexOfAnyExcept((byte)0) < 0;

    private static ulong GetSafePeerCursor(PartitionState state, int namespaceId, NodeId authorStream)
    {
        ulong safe = ulong.MaxValue;
        bool found = false;

        // The two durable peer indexes can contain the same peer. Duplicates do not
        // change a minimum, so process both streams directly without Distinct/ToArray.
        foreach (InterestedPeerKey peer in state.InterestedPeers)
        {
            if (peer.NamespaceId != namespaceId || peer.Peer == authorStream) continue;
            if (!TryGetPeerSafeCursor(state, namespaceId, authorStream, peer.Peer, peer.PeerEpoch, out ulong cursor))
                return 0;
            safe = Math.Min(safe, cursor);
            found = true;
        }
        foreach (PeerStreamKey required in state.RequiredPeerStreams)
        {
            if (required.NamespaceId != namespaceId || required.AuthorStream != authorStream ||
                required.Peer == authorStream) continue;
            if (!TryGetPeerSafeCursor(state, namespaceId, authorStream, required.Peer, required.PeerEpoch, out ulong cursor))
                return 0;
            safe = Math.Min(safe, cursor);
            found = true;
        }
        return found ? safe : 0;
    }

    private static bool TryGetPeerSafeCursor(
        PartitionState state, int namespaceId, NodeId authorStream,
        NodeId peer, uint peerEpoch, out ulong cursor)
    {
        var rosterKey = new InterestedPeerKey(namespaceId, peer, peerEpoch);
        var streamKey = new PeerStreamKey(peer, peerEpoch, namespaceId, authorStream);
        if (!state.CompletedPeerBaselines.Contains(rosterKey) ||
            !state.PeerStreams.TryGetValue(streamKey, out var progress))
        {
            cursor = 0;
            return false;
        }
        cursor = checked(progress.AckedCursor + progress.StreamOffset);
        return true;
    }

    private static void ValidatePendingReservationEpoch(
        PartitionState state, int namespaceId, NodeId peer, uint peerEpoch)
    {
        if (state.RetiredPeerEpochs.TryGetValue((namespaceId, peer), out uint retiredThrough) &&
            peerEpoch <= retiredThrough)
            throw new InvalidDataException("Peer session epoch has been durably retired.");
    }

    private static void ValidatePeerEpochForAccess(
        PartitionState state, int namespaceId, NodeId peer, uint peerEpoch)
    {
        if (state.RetiredPeerEpochs.TryGetValue((namespaceId, peer), out uint retiredThrough) &&
            peerEpoch <= retiredThrough)
            throw new InvalidDataException("Peer session epoch has been durably retired.");

        uint latestActiveEpoch = state.InterestedPeers
            .Where(key => key.NamespaceId == namespaceId && key.Peer == peer)
            .Select(key => key.PeerEpoch).DefaultIfEmpty(0U).Max();
        if (peerEpoch < latestActiveEpoch)
            throw new InvalidDataException("Peer session epoch is stale relative to the durable namespace roster.");
    }

    private static void ValidatePeerStreamArguments(string scopeId, NodeId localReplica,
        NodeId peer, int namespaceId, NodeId authorStream)
    {
        if (string.IsNullOrWhiteSpace(scopeId)) throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero || peer.IsZero || authorStream.IsZero)
            throw new ArgumentException("Replica, peer and author identities must be non-zero.");
        if (peer == localReplica || peer == authorStream)
            throw new ArgumentException("A replica cannot register itself or the author as a downstream interested peer.");
        if ((uint)namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
    }

    private static void ValidateReservation(MetadataOutboundBatchReservation reservation)
    {
        ArgumentNullException.ThrowIfNull(reservation);
        if (reservation.BatchId.IsZero) throw new ArgumentException("BatchId must be non-zero.", nameof(reservation));
        if (reservation.CursorStart == 0 || reservation.CursorEnd < reservation.CursorStart)
            throw new ArgumentOutOfRangeException(nameof(reservation.CursorStart));
        ValidateDigest(reservation.PreviousBatchDigest, nameof(reservation.PreviousBatchDigest));
        ValidateDigest(reservation.BatchDigest, nameof(reservation.BatchDigest));
        ArgumentNullException.ThrowIfNull(reservation.Payload);
        if (reservation.Payload.Length == 0) throw new ArgumentException("Reserved batch payload cannot be empty.", nameof(reservation));
    }

    private static MetadataPeerStreamState EmptyPeerStreamState()
        => new(0, 0, 0, 0, new byte[16], null);
    private static PeerStreamStateValue EmptyPeerStreamValue()
        => new(0, 0, 0, 0, new byte[16], null);
    private static MetadataPeerStreamState ToPublicPeerStreamState(PeerStreamStateValue value)
        => new(value.AckedCursor, value.PushedCursor, value.StreamOffset, value.IssuerEpoch,
            value.LastBatchDigest.ToArray(),
            value.PendingBatch is null ? null : CloneReservation(value.PendingBatch));
    private static MetadataOutboundBatchReservation CloneReservation(MetadataOutboundBatchReservation value)
        => new()
        {
            BatchId = value.BatchId, IssuerEpoch = value.IssuerEpoch, StreamOffset = value.StreamOffset,
            CursorStart = value.CursorStart, CursorEnd = value.CursorEnd,
            PreviousBatchDigest = value.PreviousBatchDigest.ToArray(),
            BatchDigest = value.BatchDigest.ToArray(), Payload = value.Payload.ToArray()
        };

    public ulong GetOutboundHighWatermark(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId localAuthor)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
            return 0;

        return state.OutboundHighWatermarks.TryGetValue(new OutboundKey(namespaceId, localAuthor), out var highWatermark)
            ? highWatermark
            : 0;
    
        }
    }

    public MetadataRelayPage ReadRelay(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong afterSequence,
        int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
        {
            if (afterSequence != 0)
                throw new ArgumentOutOfRangeException(nameof(afterSequence));
            return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), 0);
        }

        var key = new RelayKey(namespaceId, originalAuthor);
        ulong acknowledged = state.RelayAck.TryGetValue(key, out var ack) ? ack : 0;
        ulong highWatermark = state.RelayHighWatermarks.TryGetValue(key, out var relayHighWatermark) ? relayHighWatermark : 0;
        if (afterSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(afterSequence));

        ulong cursor = Math.Max(afterSequence, acknowledged);
        if (limit <= 0)
            return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), cursor);

        var partitionKey = new PartitionKey(scopeId, localReplica);
        byte[] partitionRoot = PartitionRoot(partitionKey);
        using StorageSnapshot payloadSnapshot = _storage.OpenSnapshot();
        MetadataRelayItem[] page = ReadRelayStoragePage(
            payloadSnapshot, partitionRoot, state, namespaceId, originalAuthor, cursor, limit,
            out _);
        if (page.Length == 0 && cursor < highWatermark &&
            !IsRelayHistoryFullyCompacted(state, key, highWatermark, acknowledged))
            throw new InvalidDataException("Relay high-watermark has no recoverable payload rows.");

        ulong scannedThrough = page.Length == 0 ? cursor : page[^1].Sequence;
        return new MetadataRelayPage(page, scannedThrough);
    
        }
    }

    public void AcknowledgeRelay(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong safeThroughSequence)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!TryGetPartition(partitionKey, out var current))
        {
            if (safeThroughSequence == 0) return;
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
        }

        var key = new RelayKey(namespaceId, originalAuthor);
        ulong previous = current.RelayAck.TryGetValue(key, out var ack) ? ack : 0;
        ulong highWatermark = current.RelayHighWatermarks.TryGetValue(key, out var relayHighWatermark) ? relayHighWatermark : 0;

        if (safeThroughSequence < previous || safeThroughSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        PersistStreamWatermark(partitionKey, current, 5,
            namespaceId, originalAuthor, safeThroughSequence, highWatermark);
    
        }
    }

    public ulong GetReceiveCursor(
        string scopeId,
        NodeId localReplica,
        NodeId sender,
        uint senderEpoch,
        int namespaceId)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
            return 0;

        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return GetMaxReceiveCursor(snapshot, new PartitionKey(scopeId, localReplica),
            sender, senderEpoch, namespaceId);
    
        }
    }

    public ulong GetReceiveCursor(
        string scopeId, NodeId localReplica, NodeId sender, uint senderEpoch,
        int namespaceId, NodeId authorStream)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return 0;
            return state.Receives.TryGetValue(
                new ReceiveKey(sender, senderEpoch, namespaceId, authorStream), out var receive)
                ? receive.CursorEnd : 0;
        }
    }

    public void CompactRelayThrough(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ulong safeThroughSequence)
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
            ThrowIfDisposed();
        ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
        var partitionKey = new PartitionKey(scopeId, localReplica);
        if (!TryGetPartition(partitionKey, out var current))
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        var key = new RelayKey(namespaceId, originalAuthor);
        ulong acknowledged = current.RelayAck.TryGetValue(key, out var ack) ? ack : 0;
        ulong peerSafe = GetSafePeerCursor(current, namespaceId, originalAuthor);
        bool hasPeerRequirements = current.InterestedPeers.Any(peer =>
            peer.NamespaceId == namespaceId && peer.Peer != originalAuthor) ||
            current.RequiredPeerStreams.Any(peer =>
                peer.NamespaceId == namespaceId && peer.AuthorStream == originalAuthor && peer.Peer != originalAuthor);
        bool hasLocalConsumer = current.RelayAck.ContainsKey(key);
        if ((hasLocalConsumer && safeThroughSequence > acknowledged) ||
            (hasPeerRequirements && safeThroughSequence > peerSafe) ||
            (!hasLocalConsumer && !hasPeerRequirements && safeThroughSequence > 0))
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        _ = CompactStreamPayload(partitionKey, current, 20,
            namespaceId, originalAuthor, safeThroughSequence, relay: true, maxEntries: int.MaxValue);
    
        }
    }

    /// <summary>
    /// Resolve relay ChangeIds to the durable per-author sequence assigned by the
    /// Metadata Store. This is the authoritative resolver after metadata-owned
    /// publication cutover; Node.Outbox is intentionally empty in that mode.
    /// Returns false and clears the output if any relay payload has already been
    /// compacted or is otherwise not retained.
    /// </summary>
    public bool TryResolveRelaySequences(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor,
        ReadOnlySpan<BatchId> changeIds,
        Span<ulong> sequences)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidatePartitionArguments(scopeId, localReplica, namespaceId, originalAuthor);
            if (sequences.Length != changeIds.Length)
                throw new ArgumentException("Output length must match the requested ChangeIds.", nameof(sequences));
            sequences.Clear();
            if (changeIds.IsEmpty) return true;

            var slots = new Dictionary<BatchId, int>(changeIds.Length);
            for (int i = 0; i < changeIds.Length; i++)
            {
                if (changeIds[i].IsZero)
                    throw new ArgumentException("Relay ChangeIds must be non-zero.", nameof(changeIds));
                if (!slots.TryAdd(changeIds[i], i))
                    throw new ArgumentException("Relay ChangeIds must be unique within one source page.", nameof(changeIds));
            }

            if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
                return false;
            byte[] root = PartitionRoot(new PartitionKey(scopeId, localReplica));
            using StorageSnapshot snapshot = _storage.OpenSnapshot();
            int found = 0;
            for (int i = 0; i < changeIds.Length; i++)
            {
                if (!TryGetChangeLedger(snapshot, root, state,
                        new LedgerKey(namespaceId, changeIds[i]), out ChangeLedgerValue ledger) ||
                    ledger.IsLocalOutbound || ledger.AuthorNodeId != originalAuthor)
                    continue;

                ulong sequence = ledger.StreamSequence;
                if (sequence == 0)
                    sequence = FindRelaySequenceByChangeId(snapshot, root, namespaceId, originalAuthor, changeIds[i]);
                if (sequence == 0)
                    continue;
                if (sequences[i] != 0)
                    throw new InvalidDataException("Metadata relay identity resolves to a duplicate sequence.");
                if (!snapshot.TryGet(StreamPayloadStorageKey(root, 20, namespaceId, originalAuthor, sequence),
                        out byte[]? encoded) || DecodePayloadHeader(encoded!) != changeIds[i])
                    throw new InvalidDataException("Relay ChangeId index does not reference its immutable stream row.");
                sequences[i] = sequence;
                found++;
            }

            if (found == changeIds.Length)
                return true;
            sequences.Clear();
            return false;
        }
    }

    public ulong GetRelayHighWatermark(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        if (!TryGetPartition(new PartitionKey(scopeId, localReplica), out var state))
            return 0;

        return state.RelayHighWatermarks.TryGetValue(new RelayKey(namespaceId, originalAuthor), out var highWatermark)
            ? highWatermark
            : 0;
    
        }
    }

    public void Dispose()
    {
        using IDisposable writerAdmission = _writerAdmission.Enter();
        lock (_gate)
        {
        if (_disposed) return;
        _disposed = true;
        if (_ownsStorage) _storage.Dispose();
        _partitions.Clear();
        _partitionLru.Clear();
        _partitionLruNodes.Clear();
        if (_deleteOnDispose && _directory is { } directory)
        {
            try { Directory.Delete(directory, recursive: true); }
            catch { /* isolated test directory cleanup */ }
        }
    
        }
    }


    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private PartitionState GetOrCreatePartition(PartitionKey key)
    {
        if (TryGetPartition(key, out var state)) return state;
        state = new PartitionState(CreatePendingApplyMap(key), CreatePeerStreamMap(key),
            CreateRequiredPeerStreamSet(key), CreateInterestedPeerSet(key), CreateRetiredPeerEpochMap(key),
            CreateReceiveMap(key), CreateReceiveChainMap(key), CreateLegacyReceiveChainMap(key),
            CreateReceiptMap(key), CreateChangeLedgerMap(key), CreateCompletedPendingApplyMap(key),
            CreateRelayAckMap(key), CreateRelayHighWatermarkMap(key), CreateOutboundHighWatermarkMap(key),
            CreateOriginHighWatermarkMap(key), CreateOutboundAckMap(key), CreateOutboundLeaseMap(key),
            CreateCompletedPeerBaselineSet(key))
            { StorageCommitSeq = _storage.LastCommitSeq };
        CachePartition(key, state);
        return state;
    }

    private void CachePartition(PartitionKey key, PartitionState state)
    {
        if (_partitions.ContainsKey(key))
        {
            _partitions[key] = state;
            TouchCachedPartition(key);
            return;
        }

        _partitions.Add(key, state);
        _partitionLruNodes.Add(key, _partitionLru.AddLast(key));
        while (_partitions.Count > MaxCachedPartitions)
        {
            LinkedListNode<PartitionKey> oldest = _partitionLru.First!;
            _partitionLru.RemoveFirst();
            _partitionLruNodes.Remove(oldest.Value);
            _partitions.Remove(oldest.Value);
        }
    }

    private void TouchCachedPartition(PartitionKey key)
    {
        if (!_partitionLruNodes.TryGetValue(key, out LinkedListNode<PartitionKey>? node) ||
            ReferenceEquals(node, _partitionLru.Last))
            return;
        _partitionLru.Remove(node);
        _partitionLru.AddLast(node);
    }

    private void RemoveCachedPartition(PartitionKey key)
    {
        _partitions.Remove(key);
        if (_partitionLruNodes.TryGetValue(key, out LinkedListNode<PartitionKey>? node))
        {
            _partitionLru.Remove(node);
            _partitionLruNodes.Remove(key);
        }
    }

    internal int CachedPartitionCount => _partitions.Count;
    internal int CachedRelayHeaderCount => 0;
    internal int CachedPendingApplyCount => 0;
    internal int CachedPeerStreamCount => 0;
    internal int CachedPeerRosterIndexCount => 0;
    internal int CachedRetiredPeerEpochCount => 0;
    internal int CachedReceiveCursorCount => 0;
    internal int CachedReceiveChainCount => 0;
    internal int CachedLegacyReceiveChainCount => 0;
    internal int CachedReceiptCount => 0;
    internal int CachedChangeLedgerCount => 0;
    internal int CachedCompletedPendingApplyCount => 0;
    internal int CachedStreamWatermarkCount => 0;
    internal int CachedOutboundLeaseCount => 0;
    internal int CachedOriginHighWatermarkCount => 0;
    internal int CachedCompletedPeerBaselineCount => 0;

    private static byte[] PartitionFormatKey(PartitionKey partitionKey)
    {
        byte[] key = PartitionRoot(partitionKey);
        // Separate from the Metadata row prefix and the revision key.
        key[0] = 0x74;
        return key;
    }

    private static byte[] EncodePartitionFormatMarker()
        => new byte[] { 0x42, 0x53, 0x50, 0x31, 1, 0, 0, 0 }; // BSP1, version 1

    private static void ValidatePartitionFormatMarker(byte[] value)
    {
        ReadOnlySpan<byte> expected = stackalloc byte[] { 0x42, 0x53, 0x50, 0x31, 1, 0, 0, 0 };
        if (!value.AsSpan().SequenceEqual(expected))
            throw new InvalidDataException("Metadata partition disk-primary format marker is invalid.");
    }

    private static byte[] PartitionRevisionKey(PartitionKey partitionKey)
    {
        byte[] key = PartitionRoot(partitionKey);
        // Keep the cache-version stamp outside the v3 Metadata row namespace so
        // older v3 readers can continue decoding every Metadata row kind unchanged.
        key[0] = 0x73;
        return key;
    }

    private static byte[] EncodePartitionRevision(ulong revision)
    {
        if (revision == 0) throw new ArgumentOutOfRangeException(nameof(revision));
        byte[] value = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(value, revision);
        return value;
    }

    private static ulong DecodePartitionRevision(byte[] value)
    {
        if (value.Length != 8)
            throw new InvalidDataException("Metadata partition revision marker has an invalid length.");
        ulong revision = BinaryPrimitives.ReadUInt64LittleEndian(value);
        if (revision == 0)
            throw new InvalidDataException("Metadata partition revision marker cannot be zero.");
        return revision;
    }

    private ulong CommitPartitionBatch(
        StorageWriteBatch batch, PartitionKey partitionKey, PartitionState current)
    {
        ulong nextRevision = checked(current.MetadataRevision + 1);
        byte[] key = PartitionRevisionKey(partitionKey);
        byte[] value = EncodePartitionRevision(nextRevision);
        batch.Put(key, value);
        if (!current.HasDiskPrimaryFormatMarker)
            batch.Put(PartitionFormatKey(partitionKey), EncodePartitionFormatMarker());
        ulong commitSeq = batch.Commit();

        current.HasDiskPrimaryFormatMarker = true;
        current.MetadataRevision = nextRevision;
        current.StorageCommitSeq = commitSeq;
        current.IsPersisted = true;
        // The partition cache contains only bounded adapter state and disk-backed
        // facades. No per-key metadata map, payload header or ChangeId index is retained.
        return commitSeq;
    }

    private bool TryGetPartition(PartitionKey key, out PartitionState state)
    {
        ulong currentCommitSeq = _storage.LastCommitSeq;
        if (_partitions.TryGetValue(key, out state!))
        {
            if (state.StorageCommitSeq == currentCommitSeq)
            {
                TouchCachedPartition(key);
                return true;
            }
        }

        byte[] root = PartitionRoot(key);
        using var snapshot = _storage.OpenSnapshot();
        byte[] formatKey = PartitionFormatKey(key);
        if (snapshot.TryGet(formatKey, out byte[]? formatMarker))
        {
            // New-format partitions have already passed the one-time legacy validator.
            // Reopen by validating only the identity and revision rows; never walk the
            // receipt/ledger/stream history merely to construct disk-backed facades.
            ValidatePartitionFormatMarker(formatMarker!);
            if (!snapshot.TryGet(PartitionRevisionKey(key), out byte[]? revisionValue))
                throw new InvalidDataException("Disk-primary Metadata partition is missing its revision marker.");
            ulong revision = DecodePartitionRevision(revisionValue!);
            byte[] identityKey = StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
            if (!snapshot.TryGet(identityKey, out byte[]? identity))
                throw new InvalidDataException("Disk-primary Metadata partition is missing its scope identity record.");
            ValidatePartitionIdentity(key, identity!);

            state = new PartitionState(CreatePendingApplyMap(key), CreatePeerStreamMap(key),
                CreateRequiredPeerStreamSet(key), CreateInterestedPeerSet(key), CreateRetiredPeerEpochMap(key),
                CreateReceiveMap(key), CreateReceiveChainMap(key), CreateLegacyReceiveChainMap(key),
                CreateReceiptMap(key), CreateChangeLedgerMap(key), CreateCompletedPendingApplyMap(key),
                CreateRelayAckMap(key), CreateRelayHighWatermarkMap(key), CreateOutboundHighWatermarkMap(key),
                CreateOriginHighWatermarkMap(key), CreateOutboundAckMap(key), CreateOutboundLeaseMap(key),
                CreateCompletedPeerBaselineSet(key))
            {
                StorageCommitSeq = snapshot.CommitSeq,
                MetadataRevision = revision,
                IsPersisted = true,
                HasDiskPrimaryFormatMarker = true
            };
            CachePartition(key, state);
            return true;
        }

        // Legacy v3 partitions have no format marker. Validate their existing rows
        // once, then durably mark the partition so subsequent cold loads are O(1)
        // in metadata cardinality (apart from point reads of identity/revision).
        PartitionColdLoadValidationObserver?.Invoke();
        PartitionState? decoded = DecodePartition(
            key, root, EnumeratePartitionRows(snapshot, root), snapshot.CommitSeq);
        if (decoded is null)
        {
            RemoveCachedPartition(key);
            state = null!;
            return false;
        }

        if (snapshot.TryGet(PartitionRevisionKey(key), out byte[]? decodedRevision))
            decoded.MetadataRevision = DecodePartitionRevision(decodedRevision!);

        using (var migrationBatch = _storage.BeginWriteBatch())
        {
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);
            CommitPartitionBatch(migrationBatch, key, decoded);
        }

        state = decoded;
        CachePartition(key, state);
        return true;
    }

    private void RefreshReceiptTransportEpoch(
        PartitionKey partitionKey, PartitionState current, ReceiptKey key,
        ReceiptValue prior, uint transportPeerEpoch)
    {
        if (transportPeerEpoch == 0 || transportPeerEpoch <= prior.TransportPeerEpoch)
            return;
        var refreshed = new ReceiptValue(prior.CursorStart, prior.CursorEnd,
            prior.PreviousBatchDigest.ToArray(), prior.BatchDigest.ToArray(),
            prior.NewRelayOperationCount, prior.DurableAuthor,
            prior.HeaderFingerprint.ToArray(), transportPeerEpoch, prior.OriginSequence);
        byte[] storageKey = EncodeReceiptKey(partitionKey, key);
        byte[] value = EncodeReceiptValue(refreshed);
        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        batch.Put(storageKey, value);
        CommitPartitionBatch(batch, partitionKey, current);
        // The refreshed receipt is already durable; the disk-primary map has no shadow entry.
    }

    private void PersistStreamWatermark(
        PartitionKey partitionKey, PartitionState current, byte storageKind,
        int namespaceId, NodeId author, ulong acknowledged, ulong highWatermark)
    {
        if (storageKind is not (5 or 8))
            throw new ArgumentOutOfRangeException(nameof(storageKind));
        byte[] root = PartitionRoot(partitionKey);
        byte[] key = StorageKey(root, storageKind, KeyNamespaceAuthor(namespaceId, author));
        byte[] value = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(value, acknowledged);
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(8), highWatermark);
        bool exists;
        ulong previousAcknowledged;
        ulong previousHighWatermark;
        if (storageKind == 5)
        {
            var streamKey = new RelayKey(namespaceId, author);
            // Kind 5 combines ACK and high-watermark. A new relay stream starts
            // with ACK=0; only a non-zero ACK represents a local consumer here.
            exists = current.RelayAck.TryGetValue(streamKey, out previousAcknowledged);
            current.RelayHighWatermarks.TryGetValue(streamKey, out previousHighWatermark);
        }
        else
        {
            var streamKey = new OutboundKey(namespaceId, author);
            exists = current.OutboundAck.TryGetValue(streamKey, out previousAcknowledged);
            current.OutboundHighWatermarks.TryGetValue(streamKey, out previousHighWatermark);
        }

        if (exists && previousAcknowledged == acknowledged && previousHighWatermark == highWatermark)
            return;
        if (!exists && acknowledged == 0 && highWatermark == 0)
            return;

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        batch.Put(key, value);
        CommitPartitionBatch(batch, partitionKey, current);

        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
    }

    private int CompactStreamPayload(
        PartitionKey partitionKey, PartitionState current, byte storageKind,
        int namespaceId, NodeId author, ulong safeThroughSequence, bool relay, int maxEntries)
    {
        if (maxEntries <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxEntries));
        if ((relay && storageKind != 20) || (!relay && storageKind != 21))
            throw new ArgumentOutOfRangeException(nameof(storageKind));
        byte[] root = PartitionRoot(partitionKey);
        byte[] prefix = StorageKey(root, storageKind, KeyNamespaceAuthor(namespaceId, author));
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        var removedSequences = new List<ulong>(Math.Min(maxEntries, 512));
        while (removedSequences.Count < maxEntries)
        {
            int request = Math.Min(512, maxEntries - removedSequences.Count);
            bool reachedSafeBoundary = false;
            int visited = cursor.VisitPage(request, (in StorageEntryView row) =>
            {
                ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(
                    row.Key.Slice(root.Length + 1 + 18, 8));
                if (sequence > safeThroughSequence)
                {
                    reachedSafeBoundary = true;
                    return false;
                }
                removedSequences.Add(sequence);
                return true;
            });
            if (reachedSafeBoundary || visited < request) break;
        }
        if (removedSequences.Count == 0) return 0;

        var keys = new List<byte[]>(removedSequences.Count);
        foreach (ulong sequence in removedSequences)
        {
            byte[] suffix = new byte[26];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
            author.WriteTo(suffix.AsSpan(2));
            BinaryPrimitives.WriteUInt64BigEndian(suffix.AsSpan(18), sequence);
            keys.Add(StorageKey(root, storageKind, suffix));
        }

        using (var batch = _storage.BeginWriteBatch())
        {
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != current.StorageCommitSeq)
                throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
            foreach (var key in keys) batch.Delete(key);
            CommitPartitionBatch(batch, partitionKey, current);
        }

        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
        return removedSequences.Count;
    }

    private void PersistFrozenOutboundDelta(
        PartitionKey partitionKey,
        PartitionState current,
        OutboundKey outboundKey,
        int namespaceId,
        NodeId localAuthor,
        IReadOnlyList<MetadataFrozenOperation> operations,
        IReadOnlyList<MetadataOutboundItem> frozen,
        ulong highWatermark,
        IReadOnlyDictionary<OriginStreamKey, OriginIssuedState> originHighWatermarks)
    {
        byte[] root = PartitionRoot(partitionKey);
        var newPeerStreams = new List<(PeerStreamKey Key, PeerStreamStateValue Value)>();
        var newRequiredStreams = new List<PeerStreamKey>();
        var peers = current.InterestedPeers.Where(peer => peer.NamespaceId == namespaceId)
            .Select(peer => (peer.Peer, peer.PeerEpoch))
            .Concat(current.RequiredPeerStreams.Where(peer => peer.NamespaceId == namespaceId)
                .Select(peer => (peer.Peer, peer.PeerEpoch)))
            .Distinct();
        foreach (var peer in peers.Where(peer => peer.Peer != localAuthor))
        {
            var streamKey = new PeerStreamKey(peer.Peer, peer.PeerEpoch, namespaceId, localAuthor);
            if (!current.PeerStreams.ContainsKey(streamKey))
                newPeerStreams.Add((streamKey, EmptyPeerStreamValue() with { IssuerEpoch = 0 }));
            if (!current.RequiredPeerStreams.Contains(streamKey))
                newRequiredStreams.Add(streamKey);
        }

        var writes = new List<(byte[] Key, byte[] Value)>(operations.Count * 2 + newPeerStreams.Count + newRequiredStreams.Count + originHighWatermarks.Count + 2);
        for (int i = 0; i < operations.Count; i++)
        {
            var operation = operations[i];
            var item = frozen[i];
            byte[] outboundSuffix = new byte[26];
            BinaryPrimitives.WriteUInt16LittleEndian(outboundSuffix, checked((ushort)namespaceId));
            localAuthor.WriteTo(outboundSuffix.AsSpan(2));
            BinaryPrimitives.WriteUInt64BigEndian(outboundSuffix.AsSpan(18), item.Sequence);
            writes.Add((StorageKey(root, 21, outboundSuffix), EncodePayloadItem(item.ChangeId, item.Payload)));

            byte[] ledgerSuffix = new byte[18];
            BinaryPrimitives.WriteUInt16LittleEndian(ledgerSuffix, checked((ushort)namespaceId));
            operation.ChangeId.WriteTo(ledgerSuffix.AsSpan(2));
            byte[] ledgerValue = EncodeChangeLedgerValue(new ChangeLedgerValue(
                operation.Fingerprint.ToArray(), true, localAuthor,
                item.OriginAuthorEpoch, item.OriginSequence, StreamSequence: item.Sequence));
            writes.Add((StorageKey(root, 3, ledgerSuffix), ledgerValue));
        }

        foreach (var originWatermark in originHighWatermarks)
        {
            byte[] suffix = new byte[22];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)originWatermark.Key.NamespaceId));
            originWatermark.Key.Author.WriteTo(suffix.AsSpan(2));
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(18), originWatermark.Key.AuthorEpoch);
            byte[] originValue = new byte[20];
            BinaryPrimitives.WriteUInt64LittleEndian(originValue, originWatermark.Value.Sequence);
            BinaryPrimitives.WriteUInt64LittleEndian(originValue.AsSpan(8), originWatermark.Value.MaxHlcWall);
            BinaryPrimitives.WriteUInt32LittleEndian(originValue.AsSpan(16), originWatermark.Value.MaxHlcLogical);
            writes.Add((StorageKey(root, 22, suffix), originValue));
        }

        byte[] watermarkValue = new byte[16];
        if (current.OutboundAck.TryGetValue(outboundKey, out ulong acknowledged))
            BinaryPrimitives.WriteUInt64LittleEndian(watermarkValue, acknowledged);
        BinaryPrimitives.WriteUInt64LittleEndian(watermarkValue.AsSpan(8), highWatermark);
        writes.Add((StorageKey(root, 8, KeyNamespaceAuthor(namespaceId, localAuthor)), watermarkValue));

        foreach (var peerStream in newPeerStreams)
        {
            byte[] suffix = new byte[38];
            peerStream.Key.Peer.WriteTo(suffix);
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), peerStream.Key.PeerEpoch);
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)namespaceId));
            localAuthor.WriteTo(suffix.AsSpan(22));
            writes.Add((StorageKey(root, 15, suffix), EncodePeerStreamState(peerStream.Value)));
        }
        foreach (var required in newRequiredStreams)
        {
            byte[] suffix = new byte[38];
            BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
            localAuthor.WriteTo(suffix.AsSpan(2));
            required.Peer.WriteTo(suffix.AsSpan(18));
            BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(34), required.PeerEpoch);
            writes.Add((StorageKey(root, 16, suffix), Array.Empty<byte>()));
        }

        byte[]? identityKey = current.IsPersisted
            ? null
            : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null
            ? null
            : EncodePartitionIdentity(partitionKey);
        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            batch.Put(identityKey, identityValue!);
        foreach (var write in writes)
            batch.Put(write.Key, write.Value);
        CommitPartitionBatch(batch, partitionKey, current);


        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
    }

    private void PersistOutboundLease(
        PartitionKey partitionKey, PartitionState current, OutboundKey key, LeaseState lease)
    {
        byte[] root = PartitionRoot(partitionKey);
        byte[] storageKey = StorageKey(root, 10, KeyNamespaceAuthor(key.NamespaceId, key.Author));
        byte[] value = new byte[24];
        BinaryPrimitives.WriteUInt64LittleEndian(value, lease.Generation);
        lease.ConsumerId.TryWriteBytes(value.AsSpan(8));
        byte[]? identityKey = current.IsPersisted
            ? null
            : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null
            ? null
            : EncodePartitionIdentity(partitionKey);

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            batch.Put(identityKey, identityValue!);
        batch.Put(storageKey, value);
        CommitPartitionBatch(batch, partitionKey, current);
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
    }

    private void PersistPeerBaselineMarker(
        PartitionKey partitionKey, PartitionState current, InterestedPeerKey peer)
    {
        if (current.CompletedPeerBaselines.Contains(peer)) return;
        byte[] root = PartitionRoot(partitionKey);
        byte[] suffix = new byte[22];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
        peer.Peer.WriteTo(suffix.AsSpan(2));
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(18), peer.PeerEpoch);
        byte[] key = StorageKey(root, 19, suffix);
        byte[] value = Array.Empty<byte>();

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        batch.Put(key, value);
        CommitPartitionBatch(batch, partitionKey, current);

        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
    }

    private void PersistPendingApply(
        PartitionKey partitionKey, PartitionState current, Guid applyToken, PendingApplyValue value)
    {
        byte[] root = PartitionRoot(partitionKey);
        byte[] key = StorageKey(root, 12, applyToken.ToByteArray());
        byte[] encoded = EncodeIncomingBatch(value.Batch);
        byte[]? identityKey = current.IsPersisted
            ? null
            : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null
            ? null
            : EncodePartitionIdentity(partitionKey);

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            batch.Put(identityKey, identityValue!);
        batch.Put(key, encoded);
        CommitPartitionBatch(batch, partitionKey, current);

        // The disk-backed PendingApply view reads the just-committed row on demand.
        current.StorageCommitSeq = _storage.LastCommitSeq;
        current.IsPersisted = true;
    }

    private void CommitPeerMetadataDelta(
        PartitionKey partitionKey,
        PartitionState current,
        IReadOnlyList<(byte[] Key, byte[] Value)> writes,
        IReadOnlyList<byte[]> deletes,
        Action publish)
    {
        Action<string, long>? allocationObserver = ChangeIdGcAllocationObserver;
        long allocationCheckpoint = allocationObserver is null ? 0 : GC.GetTotalAllocatedBytes(precise: true);
        void ReportAllocation(string phase)
        {
            if (allocationObserver is null) return;
            long allocated = GC.GetTotalAllocatedBytes(precise: true);
            allocationObserver(phase, allocated - allocationCheckpoint);
            allocationCheckpoint = allocated;
        }

        byte[] root = PartitionRoot(partitionKey);
        byte[]? identityKey = current.IsPersisted
            ? null
            : StorageKey(root, 0, ReadOnlySpan<byte>.Empty);
        byte[]? identityValue = identityKey is null
            ? null
            : EncodePartitionIdentity(partitionKey);

        using var batch = _storage.BeginWriteBatch();
        batch.ReserveCapacity(checked(writes.Count + deletes.Count + (identityKey is null ? 0 : 1)));
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            batch.PutShared(identityKey, identityValue!);
        for (int i = 0; i < deletes.Count; i++)
            batch.DeleteShared(deletes[i]);
        foreach (var write in writes)
            batch.PutShared(write.Key, write.Value);
        ReportAllocation("metadata-batch-staging");
        Action<string, long>? previousStorageObserver = _storage.AllocationObserver;
        _storage.AllocationObserver = allocationObserver;
        try
        {
            CommitPartitionBatch(batch, partitionKey, current);
        }
        finally
        {
            _storage.AllocationObserver = previousStorageObserver;
        }
        ReportAllocation("fld-storage-commit");

        publish();
        ReportAllocation("metadata-commit-complete");
    }

    private static byte[] EncodeReceiptKey(PartitionKey partitionKey, ReceiptKey receipt)
    {
        byte[] suffix = new byte[38];
        receipt.Sender.WriteTo(suffix);
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), receipt.Epoch);
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)receipt.NamespaceId));
        receipt.BatchId.WriteTo(suffix.AsSpan(22));
        return StorageKey(PartitionRoot(partitionKey), 1, suffix);
    }

    private static byte[] EncodeInterestedPeerKey(PartitionKey partitionKey, InterestedPeerKey peer)
    {
        byte[] suffix = new byte[22];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
        peer.Peer.WriteTo(suffix.AsSpan(2));
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(18), peer.PeerEpoch);
        return StorageKey(PartitionRoot(partitionKey), 17, suffix);
    }

    private static byte[] EncodeCompletedPeerBaselineKey(PartitionKey partitionKey, InterestedPeerKey peer)
    {
        byte[] suffix = new byte[22];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
        peer.Peer.WriteTo(suffix.AsSpan(2));
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(18), peer.PeerEpoch);
        return StorageKey(PartitionRoot(partitionKey), 19, suffix);
    }

    private static byte[] EncodePeerStreamKey(PartitionKey partitionKey, PeerStreamKey peer)
    {
        byte[] suffix = new byte[38];
        peer.Peer.WriteTo(suffix);
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), peer.PeerEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)peer.NamespaceId));
        peer.AuthorStream.WriteTo(suffix.AsSpan(22));
        return StorageKey(PartitionRoot(partitionKey), 15, suffix);
    }

    private static byte[] EncodeRequiredPeerStreamKey(PartitionKey partitionKey, PeerStreamKey peer)
    {
        byte[] suffix = new byte[38];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)peer.NamespaceId));
        peer.AuthorStream.WriteTo(suffix.AsSpan(2));
        peer.Peer.WriteTo(suffix.AsSpan(18));
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(34), peer.PeerEpoch);
        return StorageKey(PartitionRoot(partitionKey), 16, suffix);
    }

    private static byte[] EncodeRetiredPeerEpochKey(PartitionKey partitionKey, int namespaceId, NodeId peer)
    {
        byte[] suffix = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        peer.WriteTo(suffix.AsSpan(2));
        return StorageKey(PartitionRoot(partitionKey), 18, suffix);
    }

    private void PersistPeerStreamState(
        PartitionKey partitionKey, PartitionState current, PeerStreamKey key,
        PeerStreamStateValue value, bool ensureRequired)
    {
        byte[] root = PartitionRoot(partitionKey);
        byte[] streamSuffix = new byte[38];
        key.Peer.WriteTo(streamSuffix);
        BinaryPrimitives.WriteUInt32LittleEndian(streamSuffix.AsSpan(16), key.PeerEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(streamSuffix.AsSpan(20), checked((ushort)key.NamespaceId));
        key.AuthorStream.WriteTo(streamSuffix.AsSpan(22));
        byte[] streamKey = StorageKey(root, 15, streamSuffix);
        byte[] streamValue = EncodePeerStreamState(value);

        bool addRequired = ensureRequired && !current.RequiredPeerStreams.Contains(key);
        var rosterKey = new InterestedPeerKey(key.NamespaceId, key.Peer, key.PeerEpoch);
        bool addRoster = ensureRequired && !current.InterestedPeers.Contains(rosterKey);
        byte[]? requiredKey = null;
        byte[]? rosterRowKey = null;
        if (addRequired)
        {
            byte[] requiredSuffix = new byte[38];
            BinaryPrimitives.WriteUInt16LittleEndian(requiredSuffix, checked((ushort)key.NamespaceId));
            key.AuthorStream.WriteTo(requiredSuffix.AsSpan(2));
            key.Peer.WriteTo(requiredSuffix.AsSpan(18));
            BinaryPrimitives.WriteUInt32LittleEndian(requiredSuffix.AsSpan(34), key.PeerEpoch);
            requiredKey = StorageKey(root, 16, requiredSuffix);
        }
        if (addRoster)
        {
            byte[] rosterSuffix = new byte[22];
            BinaryPrimitives.WriteUInt16LittleEndian(rosterSuffix, checked((ushort)key.NamespaceId));
            key.Peer.WriteTo(rosterSuffix.AsSpan(2));
            BinaryPrimitives.WriteUInt32LittleEndian(rosterSuffix.AsSpan(18), key.PeerEpoch);
            rosterRowKey = StorageKey(root, 17, rosterSuffix);
        }

        bool addPartitionIdentity = !current.IsPersisted;
        byte[]? identityKey = addPartitionIdentity
            ? StorageKey(root, 0, ReadOnlySpan<byte>.Empty)
            : null;
        byte[]? identityValue = addPartitionIdentity
            ? EncodePartitionIdentity(partitionKey)
            : null;

        using var batch = _storage.BeginWriteBatch();
        ulong actualBaseCommitSeq = _storage.LastCommitSeq;
        if (actualBaseCommitSeq != current.StorageCommitSeq)
            throw new StorageConflictException(current.StorageCommitSeq, actualBaseCommitSeq);
        if (identityKey is not null)
            batch.Put(identityKey, identityValue!);
        batch.Put(streamKey, streamValue);
        if (requiredKey is not null)
            batch.Put(requiredKey, Array.Empty<byte>());
        if (rosterRowKey is not null)
            batch.Put(rosterRowKey, Array.Empty<byte>());
        CommitPartitionBatch(batch, partitionKey, current);

        // Storage is authoritative; no in-memory peer-stream/roster overlay is published.
    }


    private const uint MetadataPartitionMagicV3 = 0x33534D42; // "BMS3" little-endian
    private const uint ProviderPublicationCutoverMagic = 0x3150434D; // "MCP1" little-endian
    private const ushort ProviderPublicationCutoverVersion = 1;
    private const uint PreparedFullReplaceMagic = 0x31524650; // "PFR1"
    private const uint PreparedFullReplaceIndexMagic = 0x31584650; // "PFX1"
    private const ushort PreparedFullReplaceVersion = 1;

    private static byte[] EncodePartitionIdentity(PartitionKey partition)
    {
        byte[] scope = Encoding.UTF8.GetBytes(partition.ScopeId);
        byte[] value = new byte[8 + scope.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(value, MetadataPartitionMagicV3);
        BinaryPrimitives.WriteInt32LittleEndian(value.AsSpan(4), scope.Length);
        scope.CopyTo(value, 8);
        return value;
    }

    private static void ValidatePartitionIdentity(PartitionKey partition, byte[] value)
    {
        if (value.Length < 8 ||
            BinaryPrimitives.ReadUInt32LittleEndian(value) != MetadataPartitionMagicV3)
            throw new InvalidDataException("Metadata partition is not in canonical BIN-SYNC v3 format.");
        int length = BinaryPrimitives.ReadInt32LittleEndian(value.AsSpan(4));
        if (length < 0 || value.Length != 8 + length ||
            !Encoding.UTF8.GetString(value, 8, length).Equals(partition.ScopeId, StringComparison.Ordinal))
            throw new InvalidDataException("Metadata partition identity is invalid or its scope hash collides.");
    }

    private static byte[] PartitionRoot(PartitionKey key)
    {
        byte[] scope = Encoding.UTF8.GetBytes(key.ScopeId);
        byte[] hash = SHA256.HashData(scope);
        byte[] root = new byte[49];
        root[0] = 0x70;
        hash.CopyTo(root, 1);
        key.LocalReplica.WriteTo(root.AsSpan(33));
        return root;
    }

    private static byte[] StorageKey(byte[] root, byte kind, ReadOnlySpan<byte> suffix)
    {
        byte[] key = new byte[root.Length + 1 + suffix.Length];
        root.CopyTo(key, 0);
        key[root.Length] = kind;
        suffix.CopyTo(key.AsSpan(root.Length + 1));
        return key;
    }

    private static byte[] ChangeLedgerStorageKey(byte[] partitionRoot, int namespaceId, BatchId changeId)
    {
        byte[] key = new byte[partitionRoot.Length + 1 + 18];
        partitionRoot.CopyTo(key, 0);
        key[partitionRoot.Length] = 3;
        BinaryPrimitives.WriteUInt16LittleEndian(
            key.AsSpan(partitionRoot.Length + 1), checked((ushort)namespaceId));
        changeId.WriteTo(key.AsSpan(partitionRoot.Length + 3));
        return key;
    }

    private static IEnumerable<StorageEntry> EnumeratePartitionRows(
        StorageSnapshot snapshot, byte[] root)
    {
        // All cardinality-bearing metadata keyspaces are disk-primary and skipped here:
        // receipts (1), receive cursors (2), ChangeId ledger (3), stream watermarks (5/8),
        // leases (10), PendingApply intents/completions (12/13), receive digest chains (14/25),
        // peer state/rosters (15/16/17/18/19), relay/outbound payloads (20/21),
        // origin allocators (22), and FULL_REPLACE commit markers (26). Their readers use
        // exact-key lookups or bounded prefix pages; cold hydration only sees the partition
        // identity and any genuinely bounded control rows.
        const int pageSize = 512;
        for (byte kind = 0; kind <= 26; kind++)
        {
            if (kind is 1 or 2 or 3 or 5 or 8 or 10 or 12 or 13 or 14 or 15 or 16 or 17 or 18 or 19 or 20 or 21 or 22 or 25 or 26) continue;
            byte[] prefix = new byte[root.Length + 1];
            root.CopyTo(prefix, 0);
            prefix[^1] = kind;
            using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
            while (true)
            {
                IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
                foreach (StorageEntry row in page)
                    yield return row;
                if (page.Count < pageSize)
                    break;
            }
        }
    }

    private PartitionState? DecodePartition(
        PartitionKey partition,
        byte[] root,
        IEnumerable<StorageEntry> rows,
        ulong commitSeq)
    {
        var state = new PartitionState(CreatePendingApplyMap(partition), CreatePeerStreamMap(partition),
            CreateRequiredPeerStreamSet(partition), CreateInterestedPeerSet(partition), CreateRetiredPeerEpochMap(partition),
            CreateReceiveMap(partition), CreateReceiveChainMap(partition), CreateLegacyReceiveChainMap(partition),
            CreateReceiptMap(partition), CreateChangeLedgerMap(partition), CreateCompletedPendingApplyMap(partition),
            CreateRelayAckMap(partition), CreateRelayHighWatermarkMap(partition), CreateOutboundHighWatermarkMap(partition),
            CreateOriginHighWatermarkMap(partition), CreateOutboundAckMap(partition), CreateOutboundLeaseMap(partition),
            CreateCompletedPeerBaselineSet(partition))
        { StorageCommitSeq = commitSeq, IsPersisted = true };
        bool identitySeen = false;
        bool rowSeen = false;
        foreach (var row in rows)
        {
            rowSeen = true;
            ReadOnlySpan<byte> suffix = row.Key.AsSpan(root.Length);
            if (suffix.Length < 1) throw new InvalidDataException("Metadata storage key is truncated.");
            byte kind = suffix[0];
            if (kind is 4 or 6 or 7 or 9 or 11 ||
                (kind == 3 && row.Value.Length is not (45 or 53 or 61 or 69)) ||
                ((kind is 5 or 8) && row.Value.Length != 16))
                throw new InvalidDataException("Legacy metadata row encoding is not supported by BIN-SYNC v3.");
            ReadOnlySpan<byte> key = suffix.Slice(1);
            switch (kind)
            {
                case 0:
                    ValidatePartitionIdentity(partition, row.Value);
                    identitySeen = true;
                    break;
                case 1:
                {
                    // Defensive validation for scans that explicitly include receipts.
                    // Normal partition hydration skips this disk-primary keyspace; point reads decode on demand.
                    RequireLength(key, 38);
                    _ = DecodeReceiptValue(row.Value);
                    break;
                }
                case 2:
                {
                    RequireLength(key, 38);
                    RequireLength(row.Value, 8);
                    var sender = NodeId.From(key.Slice(0, 16));
                    uint epoch = BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(16));
                    int namespaceId = BinaryPrimitives.ReadUInt16LittleEndian(key.Slice(20));
                    _ = sender;
                    _ = epoch;
                    _ = namespaceId;
                    _ = NodeId.From(key.Slice(22, 16));
                    _ = BinaryPrimitives.ReadUInt64LittleEndian(row.Value);
                    break;
                }
                case 14:
                {
                    RequireLength(key, 22);
                    RequireLength(row.Value, 16);
                    _ = NodeId.From(key.Slice(0, 16));
                    _ = BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(16));
                    _ = BinaryPrimitives.ReadUInt16LittleEndian(key.Slice(20));
                    break;
                }
                case 26:
                {
                    RequireLength(key, 26);
                    if (row.Value.Length != 109 || !row.Value.AsSpan(0, 4).SequenceEqual("MBAC"u8) ||
                        row.Value[4] != 1 || BinaryPrimitives.ReadUInt16LittleEndian(key) == 0 ||
                        BinaryPrimitives.ReadUInt64LittleEndian(key.Slice(2)) == 0 ||
                        new Guid(key.Slice(10, 16), bigEndian: true) == Guid.Empty ||
                        BinaryPrimitives.ReadInt32LittleEndian(row.Value.AsSpan(101)) <= 0 ||
                        BinaryPrimitives.ReadInt32LittleEndian(row.Value.AsSpan(105)) < 0)
                        throw new InvalidDataException("Stored FULL_REPLACE baseline commit marker is malformed.");
                    break;
                }
                case 25:
                {
                    RequireLength(key, 38);
                    RequireLength(row.Value, 16);
                    _ = NodeId.From(key.Slice(0, 16));
                    _ = BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(16));
                    _ = BinaryPrimitives.ReadUInt16LittleEndian(key.Slice(20));
                    _ = NodeId.From(key.Slice(22, 16));
                    break;
                }
                case 3:
                {
                    // Defensive validation for scans that explicitly include ledger rows.
                    // Normal hydration skips the disk-primary ledger; point reads and bounded GC scans decode on demand.
                    RequireLength(key, 18);
                    _ = DecodeChangeLedgerValue(row.Value);
                    break;
                }
                case 20:
                case 21:
                {
                    // Payload rows are normally skipped during partition hydration;
                    // the page readers validate their headers while streaming from Storage.
                    RequireLength(key, 26);
                    _ = DecodePayloadHeader(row.Value);
                    break;
                }
                case 5:
                    RequireLength(key, 18);
                    RequireLength(row.Value, 16);
                    _ = new RelayKey(BinaryPrimitives.ReadUInt16LittleEndian(key), NodeId.From(key.Slice(2, 16)));
                    _ = BinaryPrimitives.ReadUInt64LittleEndian(row.Value);
                    _ = BinaryPrimitives.ReadUInt64LittleEndian(row.Value.AsSpan(8));
                    break;
                case 8:
                    RequireLength(key, 18);
                    RequireLength(row.Value, 16);
                    _ = new OutboundKey(BinaryPrimitives.ReadUInt16LittleEndian(key), NodeId.From(key.Slice(2, 16)));
                    _ = BinaryPrimitives.ReadUInt64LittleEndian(row.Value);
                    _ = BinaryPrimitives.ReadUInt64LittleEndian(row.Value.AsSpan(8));
                    break;
                case 10:
                    RequireLength(key, 18);
                    RequireLength(row.Value, 24);
                    _ = new OutboundKey(BinaryPrimitives.ReadUInt16LittleEndian(key), NodeId.From(key.Slice(2, 16)));
                    _ = BinaryPrimitives.ReadUInt64LittleEndian(row.Value);
                    _ = new Guid(row.Value.AsSpan(8, 16));
                    break;
                case 18:
                    // Retired-peer fences are point-read durable state, not partition cache.
                    RequireLength(key, 18);
                    RequireLength(row.Value, 4);
                    if (BinaryPrimitives.ReadUInt32LittleEndian(row.Value) == 0)
                        throw new InvalidDataException("Stored retired-peer epoch fence is invalid.");
                    break;
                case 19:
                    RequireLength(key, 22);
                    RequireLength(row.Value, 0);
                    _ = new InterestedPeerKey(BinaryPrimitives.ReadUInt16LittleEndian(key),
                        NodeId.From(key.Slice(2, 16)), BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(18)));
                    break;
                case 22:
                {
                    RequireLength(key, 22);
                    RequireLength(row.Value, 20);
                    var originKey = new OriginStreamKey(
                        BinaryPrimitives.ReadUInt16LittleEndian(key),
                        NodeId.From(key.Slice(2, 16)),
                        BinaryPrimitives.ReadUInt32LittleEndian(key.Slice(18)));
                    if (originKey.Author.IsZero || originKey.AuthorEpoch == 0)
                        throw new InvalidDataException("Stored origin-sequence allocator identity is invalid.");
                    _ = new OriginIssuedState(
                        BinaryPrimitives.ReadUInt64LittleEndian(row.Value),
                        BinaryPrimitives.ReadUInt64LittleEndian(row.Value.AsSpan(8)),
                        BinaryPrimitives.ReadUInt32LittleEndian(row.Value.AsSpan(16)));
                    break;
                }
                case 17:
                    // Interested-peer roster rows are exact-key / paged disk state.
                    RequireLength(key, 22);
                    RequireLength(row.Value, 0);
                    break;
                case 16:
                    // Required peer-stream membership is exact-key / paged disk state.
                    RequireLength(key, 38);
                    RequireLength(row.Value, 0);
                    break;
                case 15:
                    // Peer-stream progress is disk-primary. The facade performs exact
                    // key reads and bounded scans only for operations that need it.
                    RequireLength(key, 38);
                    break;
                case 12:
                    // PendingApply payloads are decoded only by exact-token lookup or
                    // bounded recovery scans, never retained by the partition cache.
                    RequireLength(key, 16);
                    _ = DecodeIncomingBatch(row.Value);
                    break;
                case 13:
                    // Completed-apply markers stay on Storage; validate only on cold load.
                    RequireLength(key, 16);
                    _ = DecodeCompleted(row.Value);
                    break;
                default:
                    throw new InvalidDataException($"Unknown metadata storage key kind {kind}.");
            }
        }

        if (!rowSeen)
            return null;
        if (!identitySeen)
            throw new InvalidDataException("Metadata partition is missing its scope identity record.");

        // Provenance hydration runs after DecodePartition returns so the caller can
        // use instance Storage lookups against the durable ChangeId ledger.
        return state;
    }

    private static byte[] KeyNamespaceAuthor(int namespaceId, NodeId author)
    {
        byte[] key = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(key, checked((ushort)namespaceId));
        author.WriteTo(key.AsSpan(2));
        return key;
    }

    private static byte[] EncodeUInt64(ulong value)
    {
        byte[] bytes = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
        return bytes;
    }

    private static byte[] EncodePayloadItem(BatchId changeId, byte[] payload)
    {
        // Origin is stored once in the global ChangeId ledger, not duplicated in
        // both the identity row and every relay/outbound payload row.
        byte[] bytes = new byte[20 + payload.Length];
        changeId.WriteTo(bytes);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(16), payload.Length);
        payload.CopyTo(bytes, 20);
        return bytes;
    }

    private static ReceiptValue DecodeReceiptValue(ReadOnlySpan<byte> value)
    {
        if (value.Length is not (76 or 80 or 92 or 96))
            throw new InvalidDataException("Stored v3 inbox receipt value has an invalid length.");
        return new ReceiptValue(
            BinaryPrimitives.ReadUInt64LittleEndian(value),
            BinaryPrimitives.ReadUInt64LittleEndian(value.Slice(8)),
            value.Slice(16, 16).ToArray(),
            value.Slice(32, 16).ToArray(),
            BinaryPrimitives.ReadInt32LittleEndian(value.Slice(48)),
            NodeId.From(value.Slice(52, 16)),
            value.Length is 92 or 96 ? value.Slice(68, 16).ToArray() : Array.Empty<byte>(),
            value.Length is 80 or 96
                ? BinaryPrimitives.ReadUInt32LittleEndian(value.Slice(value.Length - 4))
                : 0,
            value.Length is 76 or 80 or 92 or 96
                ? BinaryPrimitives.ReadUInt64LittleEndian(value.Slice(value.Length is 92 or 96 ? 84 : 68))
                : 0);
    }

    private static OperationOrigin DecodeChangeLedgerProvenance(ReadOnlySpan<byte> value)
    {
        if (value.Length is not (45 or 53 or 61 or 69) || value[44] > 1)
            throw new InvalidDataException("Stored v3 ChangeId ledger value has an invalid length or origin flag.");
        if (value.Length is 61 or 69)
        {
            bool hasLegacyFingerprint = false;
            for (int i = 45; i < 61; i++)
                hasLegacyFingerprint |= value[i] != 0;
            if (!hasLegacyFingerprint)
                throw new InvalidDataException("Stored legacy Node ChangeId fingerprint cannot be zero.");
        }

        return new OperationOrigin(
            NodeId.From(value.Slice(16, 16)),
            BinaryPrimitives.ReadUInt32LittleEndian(value.Slice(32)),
            BinaryPrimitives.ReadUInt64LittleEndian(value.Slice(36)));
    }

    private static ChangeLedgerValue DecodeChangeLedgerValue(ReadOnlySpan<byte> value)
    {
        if (value.Length is not (45 or 53 or 61 or 69) || value[44] > 1)
            throw new InvalidDataException("Stored v3 ChangeId ledger value has an invalid length or origin flag.");
        byte[]? legacyNodeFingerprint = value.Length is 61 or 69
            ? value.Slice(45, 16).ToArray()
            : null;
        if (legacyNodeFingerprint is not null && legacyNodeFingerprint.All(b => b == 0))
            throw new InvalidDataException("Stored legacy Node ChangeId fingerprint cannot be zero.");
        bool hasStreamSequence = value.Length is 53 or 69;
        ulong outboundSequence = hasStreamSequence
            ? BinaryPrimitives.ReadUInt64LittleEndian(value.Slice(value.Length == 69 ? 61 : 45, 8))
            : 0;
        return new ChangeLedgerValue(
            value.Slice(0, 16).ToArray(),
            value[44] == 1,
            NodeId.From(value.Slice(16, 16)),
            BinaryPrimitives.ReadUInt32LittleEndian(value.Slice(32)),
            BinaryPrimitives.ReadUInt64LittleEndian(value.Slice(36)),
            legacyNodeFingerprint,
            outboundSequence);
    }

    private static bool ReceiptValuesEqual(ReceiptValue left, ReceiptValue right)
        => left.CursorStart == right.CursorStart &&
            left.CursorEnd == right.CursorEnd &&
            left.NewRelayOperationCount == right.NewRelayOperationCount &&
            left.DurableAuthor == right.DurableAuthor &&
            left.TransportPeerEpoch == right.TransportPeerEpoch &&
            left.OriginSequence == right.OriginSequence &&
            left.PreviousBatchDigest.AsSpan().SequenceEqual(right.PreviousBatchDigest) &&
            left.BatchDigest.AsSpan().SequenceEqual(right.BatchDigest) &&
            left.HeaderFingerprint.AsSpan().SequenceEqual(right.HeaderFingerprint);

    private bool TryGetChangeLedger(
        PartitionKey partitionKey, PartitionState state, LedgerKey key, out ChangeLedgerValue value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return TryGetChangeLedger(snapshot, partitionKey, state, key, out value);
    }

    private bool TryGetChangeLedger(
        StorageSnapshot snapshot, PartitionKey partitionKey, PartitionState state,
        LedgerKey key, out ChangeLedgerValue value)
        => TryGetChangeLedger(snapshot, PartitionRoot(partitionKey), state, key, out value);

    private bool TryGetChangeLedger(
        StorageSnapshot snapshot, byte[] partitionRoot, PartitionState state,
        LedgerKey key, out ChangeLedgerValue value)
    {
        Span<byte> storageKey = stackalloc byte[68];
        partitionRoot.AsSpan().CopyTo(storageKey);
        storageKey[49] = 3;
        BinaryPrimitives.WriteUInt16LittleEndian(storageKey.Slice(50, 2), checked((ushort)key.NamespaceId));
        key.ChangeId.WriteTo(storageKey.Slice(52, 16));
        if (!snapshot.TryGet(storageKey, out byte[]? bytes) || bytes is null)
        {
            value = null!;
            return false;
        }
        value = DecodeChangeLedgerValue(bytes);
        return true;
    }

    private bool TryGetReceipt(
        PartitionKey partitionKey, PartitionState state, ReceiptKey key, out ReceiptValue value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return TryGetReceipt(snapshot, partitionKey, state, key, out value);
    }

    private bool TryGetReceipt(
        StorageSnapshot snapshot, PartitionKey partitionKey, PartitionState state,
        ReceiptKey key, out ReceiptValue value)
    {
        byte[] storageKey = EncodeReceiptKey(partitionKey, key);
        if (!snapshot.TryGet(storageKey, out byte[]? bytes) || bytes is null)
        {
            value = null!;
            return false;
        }
        value = DecodeReceiptValue(bytes);
        return true;
    }

    private bool TryGetCompletedPendingApply(
        PartitionKey partitionKey, PartitionState state, Guid applyToken,
        out CompletedPendingApplyValue value)
    {
        byte[] storageKey = StorageKey(PartitionRoot(partitionKey), 13, applyToken.ToByteArray());
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        if (!snapshot.TryGet(storageKey, out byte[]? bytes) || bytes is null)
        {
            value = null!;
            return false;
        }
        value = DecodeCompleted(bytes);
        return true;
    }

    private delegate bool ChangeLedgerRowVisitor(LedgerKey key, OperationOrigin provenance);

    private void VisitChangeLedger(
        PartitionKey partitionKey, int namespaceId, ChangeLedgerRowVisitor visitor)
    {
        byte[] root = PartitionRoot(partitionKey);
        Span<byte> namespaceSuffix = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(namespaceSuffix, checked((ushort)namespaceId));
        byte[] prefix = StorageKey(root, 3, namespaceSuffix);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 256;
        bool stopped = false;
        while (true)
        {
            int visited = cursor.VisitPage(pageSize, VisitRow);
            if (stopped || visited < pageSize)
                return;
        }

        bool VisitRow(in StorageEntryView row)
        {
            ReadOnlySpan<byte> keyBytes = row.Key;
            int offset = prefix.Length;
            if (keyBytes.Length - offset != 16)
                throw new InvalidDataException("ChangeLedger storage key is malformed.");
            var ledgerKey = new LedgerKey(namespaceId, BatchId.From(keyBytes.Slice(offset, 16)));
            if (!visitor(ledgerKey, DecodeChangeLedgerProvenance(row.Value)))
            {
                stopped = true;
                return false;
            }
            return true;
        }
    }

    private IEnumerable<(ReceiptKey Key, ReceiptValue Value)> EnumerateReceipts(
        PartitionKey partitionKey, PartitionState state, int namespaceId)
    {
        byte[] root = PartitionRoot(partitionKey);
        byte[] prefix = StorageKey(root, 1, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] keyBytes = row.Key;
                int offset = root.Length + 1;
                if (keyBytes.Length - offset != 38)
                    throw new InvalidDataException("Receipt storage key is malformed.");
                int rowNamespace = BinaryPrimitives.ReadUInt16LittleEndian(keyBytes.AsSpan(offset + 20));
                if (rowNamespace != namespaceId) continue;
                var receiptKey = new ReceiptKey(
                    NodeId.From(keyBytes.AsSpan(offset, 16)),
                    BinaryPrimitives.ReadUInt32LittleEndian(keyBytes.AsSpan(offset + 16)),
                    rowNamespace,
                    BatchId.From(keyBytes.AsSpan(offset + 22, 16)));
                yield return (receiptKey, DecodeReceiptValue(row.Value));
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private delegate bool OutboundStorageRowVisitor(
        OutboundKey key, ulong sequence, ReadOnlySpan<byte> encodedPayload);

    private bool IsRelayHistoryFullyCompacted(
        PartitionState state, RelayKey key, ulong highWatermark, ulong acknowledged)
    {
        if (acknowledged >= highWatermark) return true;
        bool hasPeerRequirements =
            state.InterestedPeers.Any(peer => peer.NamespaceId == key.NamespaceId && peer.Peer != key.Author) ||
            state.RequiredPeerStreams.Any(peer => peer.NamespaceId == key.NamespaceId &&
                peer.AuthorStream == key.Author && peer.Peer != key.Author);
        // With no local consumer and no active downstream requirement, old relay
        // rows may already have been compacted before peers were retired.
        if (!hasPeerRequirements && !state.RelayAck.ContainsKey(key))
            return true;
        return hasPeerRequirements && GetSafePeerCursor(state, key.NamespaceId, key.Author) >= highWatermark;
    }

    private bool IsRelayHistoryFullyCompacted(
        StorageSnapshot snapshot, PartitionKey partitionKey, RelayKey key,
        ulong highWatermark, ulong acknowledged)
    {
        if (acknowledged >= highWatermark) return true;
        byte[] root = PartitionRoot(partitionKey);
        bool hasPeerRequirements = HasPeerRequirements(snapshot, root, key.NamespaceId, key.Author);
        if (!hasPeerRequirements &&
            !TryReadStreamWatermark(snapshot, partitionKey, 5, key.NamespaceId, key.Author, 0, out _))
            return true;
        return hasPeerRequirements &&
            GetSafePeerCursor(snapshot, root, key.NamespaceId, key.Author) >= highWatermark;
    }

    private static bool HasPeerRequirements(
        StorageSnapshot snapshot, byte[] root, int namespaceId, NodeId author)
    {
        byte[] interestedPrefix = StorageKey(root, 17, EncodeNamespaceSuffix(checked((ushort)namespaceId)));
        using (StorageSnapshot.PrefixCursor interested = snapshot.OpenPrefixCursor(interestedPrefix))
        {
            const int pageSize = 64;
            while (true)
            {
                IReadOnlyList<StorageEntry> page = interested.ReadPage(pageSize);
                foreach (StorageEntry row in page)
                {
                    int offset = interestedPrefix.Length;
                    if (row.Key.Length != offset + 22)
                        throw new InvalidDataException("Interested-peer storage row is malformed.");
                    if (NodeId.From(row.Key.AsSpan(offset + 2, 16)) != author) return true;
                }
                if (page.Count < pageSize) break;
            }
        }
        byte[] requiredPrefix = StorageKey(root, 16, EncodeRequiredPeerStreamPrefix(namespaceId, author));
        using StorageSnapshot.PrefixCursor required = snapshot.OpenPrefixCursor(requiredPrefix);
        const int requiredPageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = required.ReadPage(requiredPageSize);
            foreach (StorageEntry row in page)
            {
                int offset = requiredPrefix.Length;
                if (row.Key.Length != offset + 20)
                    throw new InvalidDataException("Required peer-stream storage row is malformed.");
                if (NodeId.From(row.Key.AsSpan(offset, 16)) != author) return true;
            }
            if (page.Count < requiredPageSize) break;
        }
        return false;
    }

    private static byte[] EncodeRequiredPeerStreamPrefix(int namespaceId, NodeId author)
    {
        byte[] suffix = new byte[18];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        author.WriteTo(suffix.AsSpan(2));
        return suffix;
    }

    private static ulong GetSafePeerCursor(
        StorageSnapshot snapshot, byte[] root, int namespaceId, NodeId author)
    {
        ulong safe = ulong.MaxValue;
        bool found = false;
        byte[] interestedPrefix = StorageKey(root, 17, EncodeNamespaceSuffix(checked((ushort)namespaceId)));
        using (StorageSnapshot.PrefixCursor interested = snapshot.OpenPrefixCursor(interestedPrefix))
        {
            const int pageSize = 64;
            while (true)
            {
                IReadOnlyList<StorageEntry> page = interested.ReadPage(pageSize);
                foreach (StorageEntry row in page)
                {
                    byte[] key = row.Key;
                    int offset = interestedPrefix.Length;
                    NodeId peer = NodeId.From(key.AsSpan(offset + 2, 16));
                    uint epoch = BinaryPrimitives.ReadUInt32LittleEndian(key.AsSpan(offset + 18));
                    if (peer == author) continue;
                    if (!TryReadSnapshotPeerSafeCursor(snapshot, root, namespaceId, author, peer, epoch, out ulong cursor))
                        return 0;
                    safe = Math.Min(safe, cursor);
                    found = true;
                }
                if (page.Count < pageSize) break;
            }
        }
        byte[] requiredPrefix = StorageKey(root, 16, EncodeRequiredPeerStreamPrefix(namespaceId, author));
        using (StorageSnapshot.PrefixCursor required = snapshot.OpenPrefixCursor(requiredPrefix))
        {
            const int pageSize = 64;
            while (true)
            {
                IReadOnlyList<StorageEntry> page = required.ReadPage(pageSize);
                foreach (StorageEntry row in page)
                {
                    byte[] key = row.Key;
                    int offset = requiredPrefix.Length;
                    NodeId peer = NodeId.From(key.AsSpan(offset, 16));
                    uint epoch = BinaryPrimitives.ReadUInt32LittleEndian(key.AsSpan(offset + 16));
                    if (peer == author) continue;
                    if (!TryReadSnapshotPeerSafeCursor(snapshot, root, namespaceId, author, peer, epoch, out ulong cursor))
                        return 0;
                    safe = Math.Min(safe, cursor);
                    found = true;
                }
                if (page.Count < pageSize) break;
            }
        }
        return found ? safe : 0;
    }

    private static bool TryReadSnapshotPeerSafeCursor(
        StorageSnapshot snapshot, byte[] root, int namespaceId, NodeId author,
        NodeId peer, uint peerEpoch, out ulong cursor)
    {
        byte[] baselineSuffix = new byte[22];
        BinaryPrimitives.WriteUInt16LittleEndian(baselineSuffix, checked((ushort)namespaceId));
        peer.WriteTo(baselineSuffix.AsSpan(2));
        BinaryPrimitives.WriteUInt32LittleEndian(baselineSuffix.AsSpan(18), peerEpoch);
        if (!snapshot.TryGet(StorageKey(root, 19, baselineSuffix), out _))
        {
            cursor = 0;
            return false;
        }
        byte[] streamSuffix = new byte[38];
        peer.WriteTo(streamSuffix);
        BinaryPrimitives.WriteUInt32LittleEndian(streamSuffix.AsSpan(16), peerEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(streamSuffix.AsSpan(20), checked((ushort)namespaceId));
        author.WriteTo(streamSuffix.AsSpan(22));
        if (!snapshot.TryGet(StorageKey(root, 15, streamSuffix), out byte[]? bytes))
        {
            cursor = 0;
            return false;
        }
        PeerStreamStateValue state = DecodePeerStreamState(bytes!);
        cursor = checked(state.AckedCursor + state.StreamOffset);
        return true;
    }

    private delegate bool RelayStorageRowVisitor(
        RelayKey key, ulong sequence, ReadOnlySpan<byte> encodedPayload);

    private bool VisitRelayStorageRows(
        StorageSnapshot snapshot, byte[] partitionRoot, PartitionState state,
        int namespaceId, RelayStorageRowVisitor visitor)
    {
        foreach (RelayKey key in state.RelayHighWatermarks.Keys)
        {
            if (key.NamespaceId != namespaceId) continue;
            ulong high = state.RelayHighWatermarks[key];
            ulong acknowledged = state.RelayAck.TryGetValue(key, out ulong ack) ? ack : 0;
            if (acknowledged > high)
                throw new InvalidDataException("Relay acknowledgement exceeds its durable high-watermark.");

            byte[] prefix = StorageKey(partitionRoot, 20, KeyNamespaceAuthor(key.NamespaceId, key.Author));
            using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
            ulong previousSequence = 0;
            while (true)
            {
                bool keepGoing = true;
                int visited = cursor.VisitPage(512, (in StorageEntryView row) =>
                {
                    ReadOnlySpan<byte> rowKey = row.Key;
                    if (rowKey.Length != partitionRoot.Length + 1 + 26 || !rowKey.StartsWith(prefix))
                        throw new InvalidDataException("Relay storage scan escaped its stream prefix.");
                    ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(
                        rowKey.Slice(partitionRoot.Length + 1 + 18, 8));
                    if ((previousSequence != 0 && sequence != checked(previousSequence + 1)) ||
                        sequence == 0 || sequence > high)
                        throw new InvalidDataException("Relay storage contains a sequence gap or exceeds its high-watermark.");
                    _ = DecodePayloadHeader(row.Value);
                    previousSequence = sequence;
                    if (!visitor(key, sequence, row.Value))
                    {
                        keepGoing = false;
                        return false;
                    }
                    return true;
                });
                if (!keepGoing) return false;
                if (visited < 512) break;
            }
            if (previousSequence == 0 && high != 0 &&
                !IsRelayHistoryFullyCompacted(state, key, high, acknowledged))
                throw new InvalidDataException("Relay high-watermark has no recoverable payload rows.");
            if (previousSequence != 0 && previousSequence != high)
                throw new InvalidDataException("Relay payload history ends before its durable high-watermark.");
        }
        return true;
    }

    private int CountRelayRows(PartitionKey partitionKey, PartitionState state, int namespaceId)
    {
        int count = 0;
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        byte[] root = PartitionRoot(partitionKey);
        if (!VisitRelayStorageRows(snapshot, root, state, namespaceId,
            (key, sequence, payload) =>
            {
                count = checked(count + 1);
                return true;
            }))
            throw new InvalidDataException("Relay row count scan was interrupted.");
        return count;
    }

    private static ulong FindRelaySequenceByChangeId(
        StorageSnapshot snapshot, byte[] partitionRoot,
        int namespaceId, NodeId author, BatchId changeId)
    {
        byte[] prefix = StorageKey(partitionRoot, 20, KeyNamespaceAuthor(namespaceId, author));
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        ulong found = 0;
        while (true)
        {
            int visited = cursor.VisitPage(512, (in StorageEntryView row) =>
            {
                if (DecodePayloadHeader(row.Value) != changeId) return true;
                ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(
                    row.Key.Slice(partitionRoot.Length + 1 + 18, 8));
                if (found != 0)
                    throw new InvalidDataException("Relay ChangeId occurs more than once in its author stream.");
                found = sequence;
                return true;
            });
            if (visited < 512) break;
        }
        return found;
    }

    private bool VisitOutboundStorageRows(
        StorageSnapshot snapshot, byte[] partitionRoot, PartitionState state,
        int namespaceId, OutboundStorageRowVisitor visitor)
    {
        foreach (OutboundKey key in state.OutboundHighWatermarks.Keys)
        {
            if (key.NamespaceId != namespaceId) continue;
            byte[] prefix = StorageKey(partitionRoot, 21, KeyNamespaceAuthor(key.NamespaceId, key.Author));
            using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
            while (true)
            {
                bool keepGoing = true;
                int visited = cursor.VisitPage(512, (in StorageEntryView row) =>
                {
                    ReadOnlySpan<byte> rowKey = row.Key;
                    if (rowKey.Length != partitionRoot.Length + 1 + 26 ||
                        !rowKey.StartsWith(prefix))
                        throw new InvalidDataException("Outbound storage scan escaped its stream prefix.");
                    ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(
                        rowKey.Slice(partitionRoot.Length + 1 + 18, 8));
                    if (!visitor(key, sequence, row.Value))
                    {
                        keepGoing = false;
                        return false;
                    }
                    return true;
                });
                if (!keepGoing) return false;
                if (visited < 512) break;
            }
        }
        return true;
    }

    private int CountOutboundRows(
        PartitionKey partitionKey, PartitionState state, int namespaceId)
    {
        int count = 0;
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        byte[] root = PartitionRoot(partitionKey);
        VisitOutboundStorageRows(snapshot, root, state, namespaceId, (key, sequence, payload) =>
        {
            count = checked(count + 1);
            return true;
        });
        return count;
    }

    private static ulong FindStreamSequenceByChangeId(
        StorageSnapshot snapshot, byte[] partitionRoot,
        int namespaceId, NodeId author, BatchId changeId)
    {
        byte[] prefix = StorageKey(partitionRoot, 21, KeyNamespaceAuthor(namespaceId, author));
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        while (true)
        {
            ulong found = 0;
            int visited = cursor.VisitPage(512, (in StorageEntryView row) =>
            {
                if (DecodePayloadHeader(row.Value) != changeId) return true;
                found = BinaryPrimitives.ReadUInt64BigEndian(
                    row.Key.Slice(partitionRoot.Length + 1 + 18, 8));
                return false;
            });
            if (found != 0) return found;
            if (visited < 512) return 0;
        }
    }

    private const int MaxOutboundPageItems = 256;
    private const int MaxOutboundPageBytes = 1024 * 1024;
    private const int MaxStreamPayloadBytes = 16 * 1024 * 1024 - 25;

    private MetadataOutboundItem[] ReadOutboundStoragePage(
        StorageSnapshot snapshot, byte[] partitionRoot, PartitionState state,
        int namespaceId, NodeId author, ulong afterSequence, int limit, out bool byteBudgetLimited)
    {
        byteBudgetLimited = false;
        if (limit <= 0) return Array.Empty<MetadataOutboundItem>();
        bool budgetLimited = false;
        int pageLimit = Math.Min(limit, MaxOutboundPageItems);
        byte[] prefix = StorageKey(partitionRoot, 21, KeyNamespaceAuthor(namespaceId, author));
        byte[] afterKey = afterSequence == 0
            ? Array.Empty<byte>()
            : StreamPayloadStorageKey(partitionRoot, 21, namespaceId, author, afterSequence);
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix, afterKey);
        var page = new MetadataOutboundItem[pageLimit];
        int pageCount = 0;
        int encodedPageBytes = 0;
        ulong previousSequence = afterSequence;
        cursor.VisitPage(pageLimit, (in StorageEntryView row) =>
        {
            ReadOnlySpan<byte> rowKey = row.Key;
            ReadOnlySpan<byte> rowValue = row.Value;
            if (rowValue.Length > MaxStreamPayloadBytes + 20)
                throw new InvalidDataException("Outbound payload exceeds the protocol maximum.");
            bool oversizedFirstRow = pageCount == 0 && rowValue.Length > MaxOutboundPageBytes;
            if (pageCount > 0 && rowValue.Length > MaxOutboundPageBytes - encodedPageBytes)
            {
                budgetLimited = true;
                return false;
            }
            encodedPageBytes = checked(encodedPageBytes + rowValue.Length);
            if (rowKey.Length != partitionRoot.Length + 1 + 26 ||
                !rowKey.StartsWith(prefix))
                throw new InvalidDataException("Outbound storage cursor returned a row outside its stream prefix.");
            ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(
                rowKey.Slice(partitionRoot.Length + 1 + 18, 8));
            if (sequence <= previousSequence ||
                (pageCount > 0 && sequence != checked(previousSequence + 1)))
                throw new InvalidDataException("Outbound storage cursor returned a gap or non-monotonic sequence.");
            previousSequence = sequence;

            BatchId changeId = DecodePayloadHeader(rowValue);
            if (!TryGetChangeLedger(snapshot, partitionRoot, state,
                    new LedgerKey(namespaceId, changeId), out ChangeLedgerValue ledger) ||
                !ledger.IsLocalOutbound || ledger.AuthorNodeId != author ||
                (ledger.StreamSequence != 0 && ledger.StreamSequence != sequence))
                throw new InvalidDataException("Outbound payload row has no matching durable ChangeId ledger identity.");
            page[pageCount++] = new MetadataOutboundItem(
                sequence, author, changeId, rowValue.Slice(20).ToArray())
            {
                Provenance = ledger.Provenance
            };
            if (oversizedFirstRow)
            {
                budgetLimited = true;
                return false;
            }
            return true;
        });
        byteBudgetLimited = budgetLimited;
        if (pageCount != page.Length) Array.Resize(ref page, pageCount);
        return page;
    }

    private MetadataRelayItem[] ReadRelayStoragePage(
        StorageSnapshot snapshot, byte[] partitionRoot, PartitionState state,
        int namespaceId, NodeId author, ulong afterSequence, int limit, out bool byteBudgetLimited)
    {
        byteBudgetLimited = false;
        if (limit <= 0) return Array.Empty<MetadataRelayItem>();

        int pageLimit = Math.Min(limit, MaxOutboundPageItems);
        byte[] prefix = StorageKey(partitionRoot, 20, KeyNamespaceAuthor(namespaceId, author));
        byte[] afterKey = afterSequence == 0
            ? Array.Empty<byte>()
            : StreamPayloadStorageKey(partitionRoot, 20, namespaceId, author, afterSequence);
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix, afterKey);
        var page = new MetadataRelayItem[pageLimit];
        int count = 0;
        int encodedPageBytes = 0;
        ulong previousSequence = afterSequence;
        bool budgetLimited = false;

        cursor.VisitPage(pageLimit, (in StorageEntryView row) =>
        {
            ReadOnlySpan<byte> rowKey = row.Key;
            ReadOnlySpan<byte> rowValue = row.Value;
            if (rowValue.Length > MaxStreamPayloadBytes + 20)
                throw new InvalidDataException("Relay payload exceeds the protocol maximum.");
            bool oversizedFirstRow = count == 0 && rowValue.Length > MaxOutboundPageBytes;
            if (count > 0 && rowValue.Length > MaxOutboundPageBytes - encodedPageBytes)
            {
                budgetLimited = true;
                return false;
            }
            encodedPageBytes = checked(encodedPageBytes + rowValue.Length);
            if (rowKey.Length != partitionRoot.Length + 1 + 26 || !rowKey.StartsWith(prefix))
                throw new InvalidDataException("Relay storage cursor returned a row outside its stream prefix.");

            ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(
                rowKey.Slice(partitionRoot.Length + 1 + 18, 8));
            if (sequence <= previousSequence ||
                (count > 0 && sequence != checked(previousSequence + 1)))
                throw new InvalidDataException("Relay storage cursor returned a gap or non-monotonic sequence.");
            previousSequence = sequence;

            BatchId changeId = DecodePayloadHeader(rowValue);
            if (!TryGetChangeLedger(snapshot, partitionRoot, state,
                    new LedgerKey(namespaceId, changeId), out ChangeLedgerValue ledger) ||
                ledger.IsLocalOutbound || ledger.AuthorNodeId != author ||
                (ledger.StreamSequence != 0 && ledger.StreamSequence != sequence))
                throw new InvalidDataException("Relay payload row has no matching durable ChangeId identity.");

            page[count++] = new MetadataRelayItem(
                sequence, author, changeId, rowValue.Slice(20).ToArray())
            { Provenance = ledger.Provenance };
            if (oversizedFirstRow)
            {
                budgetLimited = true;
                return false;
            }
            return true;
        });

        byteBudgetLimited = budgetLimited;
        if (count != page.Length) Array.Resize(ref page, count);
        return page;
    }

    private static BatchId DecodePayloadHeader(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 20) throw new InvalidDataException("Stored stream payload is truncated.");
        int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(16));
        if (length < 0 || length > MaxStreamPayloadBytes || bytes.Length != 20 + length)
            throw new InvalidDataException("Stored compact stream payload length is invalid.");
        return BatchId.From(bytes.Slice(0, 16));
    }

    private static MetadataOutboundItem DecodePayloadItem(ulong sequence, NodeId author, byte[] bytes)
    {
        BatchId changeId = DecodePayloadHeader(bytes);
        return new MetadataOutboundItem(sequence, author, changeId, bytes.AsSpan(20).ToArray());
    }

    private static byte[] StreamPayloadStorageKey(
        byte[] root, byte kind, int namespaceId, NodeId author, ulong sequence)
    {
        byte[] suffix = new byte[26];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        author.WriteTo(suffix.AsSpan(2));
        BinaryPrimitives.WriteUInt64BigEndian(suffix.AsSpan(18), sequence);
        return StorageKey(root, kind, suffix);
    }

    private byte[] LoadStreamPayloadBody(
        PartitionKey partitionKey, byte kind, int namespaceId, NodeId author, ulong sequence)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return LoadStreamPayloadBody(snapshot, partitionKey, kind, namespaceId, author, sequence);
    }

    private byte[] LoadStreamPayloadBody(
        StorageSnapshot snapshot, PartitionKey partitionKey, byte kind,
        int namespaceId, NodeId author, ulong sequence)
        => LoadStreamPayloadBody(snapshot, PartitionRoot(partitionKey), kind, namespaceId, author, sequence);

    private byte[] LoadStreamPayloadBody(
        StorageSnapshot snapshot, byte[] partitionRoot, byte kind,
        int namespaceId, NodeId author, ulong sequence, BatchId? expectedChangeId = null)
    {
        if (kind is not (20 or 21))
            throw new ArgumentOutOfRangeException(nameof(kind));
        Span<byte> key = stackalloc byte[76];
        partitionRoot.AsSpan().CopyTo(key);
        key[49] = kind;
        BinaryPrimitives.WriteUInt16LittleEndian(key.Slice(50, 2), checked((ushort)namespaceId));
        author.WriteTo(key.Slice(52, 16));
        BinaryPrimitives.WriteUInt64BigEndian(key.Slice(68, 8), sequence);
        if (!snapshot.TryGet(key, out byte[]? value) || value is null)
            throw new InvalidDataException("Persisted stream payload row is missing.");
        BatchId storedChangeId = DecodePayloadHeader(value);
        if (expectedChangeId is BatchId expected && storedChangeId != expected)
            throw new InvalidDataException("Outbound ChangeId sequence index points to a different stream payload row.");
        return value.AsSpan(20).ToArray();
    }

    private void ValidateIndexedOutboundRetryPayloads(
        StorageSnapshot snapshot,
        byte[] partitionRoot,
        int namespaceId,
        NodeId author,
        IReadOnlyList<(ulong Sequence, MetadataFrozenOperation Operation)> expected)
    {
        if (expected.Count == 0)
            return;

        ulong firstSequence = expected[0].Sequence;
        ulong lastSequence = expected[^1].Sequence;
        ulong sequenceSpan = checked(lastSequence - firstSequence + 1);

        // Sparse retries are cheaper as exact point reads. Dense retries (the normal
        // post-freeze/provider-ACK retry) are validated with one forward-only B+ tree
        // cursor, avoiding one random Storage lookup and payload copy per operation.
        if (expected.Count < 8 || sequenceSpan > (ulong)expected.Count * 2)
        {
            foreach ((ulong sequence, MetadataFrozenOperation operation) in expected)
            {
                byte[] payload = LoadStreamPayloadBody(
                    snapshot, partitionRoot, 21, namespaceId, author, sequence, operation.ChangeId);
                if (!payload.AsSpan().SequenceEqual(operation.Payload))
                    throw new InvalidDataException(
                        "Outbound ChangeId was retried with conflicting author or payload.");
            }
            return;
        }

        byte[] prefix = StorageKey(partitionRoot, 21, KeyNamespaceAuthor(namespaceId, author));
        byte[] afterKey = firstSequence == 1
            ? Array.Empty<byte>()
            : StreamPayloadStorageKey(
                partitionRoot, 21, namespaceId, author, firstSequence - 1);
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix, afterKey);

        int expectedIndex = 0;
        ulong previousSequence = firstSequence - 1;
        while (expectedIndex < expected.Count &&
            cursor.TryReadNext(out StorageEntryView row))
        {
            ReadOnlySpan<byte> rowKey = row.Key;
            ReadOnlySpan<byte> rowValue = row.Value;
            if (rowKey.Length != partitionRoot.Length + 1 + 26 ||
                !rowKey.StartsWith(prefix))
                throw new InvalidDataException(
                    "Outbound retry cursor returned a row outside its stream prefix.");

            ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(
                rowKey.Slice(partitionRoot.Length + 1 + 18, 8));
            if (sequence <= previousSequence)
                throw new InvalidDataException(
                    "Outbound retry cursor returned non-monotonic sequences.");
            previousSequence = sequence;

            if (sequence > lastSequence)
                break;

            (ulong expectedSequence, MetadataFrozenOperation operation) = expected[expectedIndex];
            if (sequence < expectedSequence)
                continue;
            if (sequence > expectedSequence)
                throw new InvalidDataException(
                    "Persisted outbound payload row is missing for a frozen ChangeId.");

            if (rowValue.Length < 20 || rowValue.Length > MaxStreamPayloadBytes + 20)
                throw new InvalidDataException("Stored outbound payload has an invalid length.");
            BatchId storedChangeId = DecodePayloadHeader(rowValue);
            if (storedChangeId != operation.ChangeId)
                throw new InvalidDataException(
                    "Outbound ChangeId sequence index points to a different stream payload row.");
            if (!rowValue.Slice(20).SequenceEqual(operation.Payload))
                throw new InvalidDataException(
                    "Outbound ChangeId was retried with conflicting author or payload.");

            expectedIndex++;
        }

        if (expectedIndex != expected.Count)
            throw new InvalidDataException(
                "Persisted outbound payload row is missing for a frozen ChangeId.");
    }

    private byte[] ResolveStreamPayload(
        PartitionKey partitionKey, byte kind, int namespaceId, NodeId author, ulong sequence, byte[] cached)
    {
        if (cached.Length > 0) return cached;
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return ResolveStreamPayload(snapshot, partitionKey, kind, namespaceId, author, sequence, cached);
    }

    private byte[] ResolveStreamPayload(
        StorageSnapshot snapshot, PartitionKey partitionKey, byte kind,
        int namespaceId, NodeId author, ulong sequence, byte[] cached)
        => ResolveStreamPayload(snapshot, PartitionRoot(partitionKey), kind,
            namespaceId, author, sequence, cached);

    private byte[] ResolveStreamPayload(
        StorageSnapshot snapshot, byte[] partitionRoot, byte kind,
        int namespaceId, NodeId author, ulong sequence, byte[] cached)
        => cached.Length > 0
            ? cached
            : LoadStreamPayloadBody(snapshot, partitionRoot, kind, namespaceId, author, sequence);

    private OperationOrigin ResolvePayloadProvenance(PartitionKey partitionKey, PartitionState state, int namespaceId,
        NodeId author, BatchId changeId, bool isLocalOutbound)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return ResolvePayloadProvenance(snapshot, PartitionRoot(partitionKey), state,
            namespaceId, author, changeId, isLocalOutbound);
    }

    private OperationOrigin ResolvePayloadProvenance(
        StorageSnapshot snapshot, byte[] partitionRoot, PartitionState state, int namespaceId,
        NodeId author, BatchId changeId, bool isLocalOutbound)
    {
        if (!TryGetChangeLedger(snapshot, partitionRoot, state,
                new LedgerKey(namespaceId, changeId), out var ledger))
            throw new InvalidDataException("Stream payload row has no global ChangeId identity row.");
        var provenance = ledger.Provenance.EnsureValid();
        if (provenance.AuthorNodeId != author || ledger.IsLocalOutbound != isLocalOutbound)
            throw new InvalidDataException("Stream payload row conflicts with its global ChangeId identity row.");
        return provenance;
    }

    private static void RequireLength(ReadOnlySpan<byte> bytes, int length)
    {
        if (bytes.Length != length) throw new InvalidDataException("Metadata storage key/value has an invalid length.");
    }

    private static byte[] EncodePeerStreamState(PeerStreamStateValue state)
    {
        int payloadLength = state.PendingBatch?.Payload.Length ?? 0;
        byte[] bytes = new byte[46 + (state.PendingBatch is null ? 0 : 80 + payloadLength)];
        bytes[0] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(1), state.AckedCursor);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(9), state.PushedCursor);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(17), state.StreamOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(25), state.IssuerEpoch);
        state.LastBatchDigest.CopyTo(bytes, 29);
        bytes[45] = state.PendingBatch is null ? (byte)0 : (byte)1;
        if (state.PendingBatch is not null)
        {
            var pending = state.PendingBatch;
            int offset = 46;
            pending.BatchId.WriteTo(bytes.AsSpan(offset)); offset += 16;
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(offset), pending.IssuerEpoch); offset += 4;
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), pending.StreamOffset); offset += 8;
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), pending.CursorStart); offset += 8;
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(offset), pending.CursorEnd); offset += 8;
            pending.PreviousBatchDigest.CopyTo(bytes, offset); offset += 16;
            pending.BatchDigest.CopyTo(bytes, offset); offset += 16;
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset), payloadLength); offset += 4;
            pending.Payload.CopyTo(bytes, offset);
        }
        return bytes;
    }

    private static PeerStreamStateValue DecodePeerStreamState(byte[] bytes)
    {
        if (bytes.Length < 46 || bytes[0] != 1 || bytes[45] > 1)
            throw new InvalidDataException("Stored peer stream state has an invalid header.");
        ulong acked = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(1));
        ulong pushed = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(9));
        ulong streamOffset = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(17));
        uint stateIssuerEpoch = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(25));
        byte[] digest = bytes.AsSpan(29, 16).ToArray();
        if (pushed < acked) throw new InvalidDataException("Stored peer pushed cursor is behind its ACK cursor.");
        if (bytes[45] == 0)
        {
            RequireLength(bytes, 46);
            return new PeerStreamStateValue(acked, pushed, streamOffset, stateIssuerEpoch, digest, null);
        }
        if (bytes.Length < 126) throw new InvalidDataException("Stored peer batch reservation is truncated.");
        int offset = 46;
        var batchId = BatchId.From(bytes.AsSpan(offset, 16)); offset += 16;
        uint reservationIssuerEpoch = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
        ulong reservationStreamOffset = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
        ulong cursorStart = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
        ulong cursorEnd = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(offset)); offset += 8;
        byte[] previous = bytes.AsSpan(offset, 16).ToArray(); offset += 16;
        byte[] batchDigest = bytes.AsSpan(offset, 16).ToArray(); offset += 16;
        int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)); offset += 4;
        if (payloadLength <= 0 || bytes.Length != offset + payloadLength ||
            batchId.IsZero || cursorStart == 0 || cursorEnd < cursorStart ||
            cursorStart != checked(acked + 1) || reservationIssuerEpoch != stateIssuerEpoch || reservationStreamOffset != streamOffset ||
            !previous.AsSpan().SequenceEqual(digest))
            throw new InvalidDataException("Stored peer batch reservation is inconsistent.");
        var pending = new MetadataOutboundBatchReservation
        {
            BatchId = batchId, IssuerEpoch = reservationIssuerEpoch, StreamOffset = reservationStreamOffset, CursorStart = cursorStart, CursorEnd = cursorEnd,
            PreviousBatchDigest = previous, BatchDigest = batchDigest,
            Payload = bytes.AsSpan(offset, payloadLength).ToArray()
        };
        return new PeerStreamStateValue(acked, pushed, streamOffset, stateIssuerEpoch, digest, pending);
    }

    private static byte[] EncodeCompleted(CompletedPendingApplyValue completed)
    {
        byte[] bytes = completed.Receipt.HeaderFingerprint.Length == 16 ? new byte[85] : new byte[69];
        completed.BatchDigest.CopyTo(bytes, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(16), completed.Receipt.CursorStart);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(24), completed.Receipt.CursorEnd);
        completed.Receipt.BatchDigest.CopyTo(bytes, 32);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(48), completed.Receipt.NewRelayOperationCount);
        bytes[52] = completed.Receipt.IsDuplicate ? (byte)1 : (byte)0;
        completed.Receipt.DurableAuthor.WriteTo(bytes.AsSpan(53));
        if (completed.Receipt.HeaderFingerprint.Length == 16)
            completed.Receipt.HeaderFingerprint.CopyTo(bytes, 69);
        return bytes;
    }

    private static CompletedPendingApplyValue DecodeCompleted(byte[] bytes)
    {
        if (bytes.Length is not (69 or 85))
            throw new InvalidDataException("Completed v3 apply receipt has an invalid length.");
        if (bytes[52] > 1) throw new InvalidDataException("Completed apply receipt flags are invalid.");
        byte[] digest = bytes.AsSpan(0, 16).ToArray();
        var receipt = new MetadataBatchReceipt(
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(16)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(24)),
            bytes.AsSpan(32, 16).ToArray(),
            BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(48)),
            bytes[52] != 0)
        {
            DurableAuthor = NodeId.From(bytes.AsSpan(53, 16)),
            HeaderFingerprint = bytes.Length == 85 ? bytes.AsSpan(69, 16).ToArray() : Array.Empty<byte>()
        };
        return new CompletedPendingApplyValue(digest, receipt);
    }

    private static byte[] EncodeIncomingBatch(MetadataIncomingBatch batch)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        byte[] scope = Encoding.UTF8.GetBytes(batch.ScopeId);
        writer.Write(-7); // v3: mandatory immutable Origin on every PendingApply operation
        writer.Write(scope.Length); writer.Write(scope);
        writer.Write(batch.LocalReplica.ToArray());
        writer.Write(batch.SenderPeer.ToArray());
        writer.Write(batch.SenderEpoch);
        writer.Write(batch.TransportPeerEpoch);
        writer.Write(batch.AuthorStreamNodeId.ToArray());
        writer.Write(checked((ushort)batch.NamespaceId));
        writer.Write(batch.BatchId.ToArray());
        writer.Write(batch.CursorStart); writer.Write(batch.CursorEnd);
        writer.Write(batch.PreviousBatchDigest); writer.Write(batch.BatchDigest);
        writer.Write(batch.WirePayload.Length); writer.Write(batch.WirePayload);
        writer.Write(batch.Operations.Count);
        foreach (var operation in batch.Operations)
        {
            var provenance = operation.Provenance.EnsureValid();
            writer.Write(provenance.AuthorNodeId.ToArray());
            writer.Write(operation.ChangeId.ToArray());
            writer.Write(provenance.AuthorEpoch);
            writer.Write(provenance.Sequence);
            writer.Write(operation.Fingerprint);
            writer.Write(operation.RelayEligible);
            writer.Write(operation.Payload.Length);
            writer.Write(operation.Payload);
        }
        writer.Write(batch.ReceiveStateOwnedByMetadataStore);
        writer.Write(batch.HeaderFingerprint.Length == 16);
        if (batch.HeaderFingerprint.Length == 16)
            writer.Write(batch.HeaderFingerprint);
        writer.Flush();
        return stream.ToArray();
    }

    private static MetadataIncomingBatch DecodeIncomingBatch(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes, writable: false);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: false);
        int versionOrScopeLength = reader.ReadInt32();
        if (versionOrScopeLength != -7)
            throw new InvalidDataException("Unsupported PendingApply encoding; BIN-SYNC v3 requires format 7.");
        const bool hasAuthorStream = true;
        const bool hasWirePayload = true;
        const bool hasReceiveStateAuthority = true;
        const bool hasHeaderFingerprint = true;
        int scopeLength = reader.ReadInt32();
        string scope = Encoding.UTF8.GetString(ReadExactly(reader, scopeLength));
        NodeId local = NodeId.From(ReadExactly(reader, 16));
        NodeId sender = NodeId.From(ReadExactly(reader, 16));
        uint epoch = reader.ReadUInt32();
        uint transportPeerEpoch = reader.ReadUInt32();
        NodeId authorStream = hasAuthorStream ? NodeId.From(ReadExactly(reader, 16)) : default;
        int ns = reader.ReadUInt16();
        BatchId batchId = BatchId.From(ReadExactly(reader, 16));
        ulong cursorStart = reader.ReadUInt64();
        ulong cursorEnd = reader.ReadUInt64();
        byte[] previous = ReadExactly(reader, 16);
        byte[] digest = ReadExactly(reader, 16);
        byte[] wirePayload = hasWirePayload ? ReadExactly(reader, reader.ReadInt32()) : Array.Empty<byte>();
        int count = reader.ReadInt32();
        if (count < 0 || count > 100_000) throw new InvalidDataException("Pending batch operation count is invalid.");
        var operations = new MetadataOperation[count];
        for (int i = 0; i < count; i++)
        {
            var author = NodeId.From(ReadExactly(reader, 16));
            var changeId = BatchId.From(ReadExactly(reader, 16));
            uint originAuthorEpoch = reader.ReadUInt32();
            ulong originSequence = reader.ReadUInt64();
            if (originAuthorEpoch == 0 || originSequence == 0)
                throw new InvalidDataException("PendingApply operation has invalid immutable Origin provenance.");
            byte[] fingerprint = ReadExactly(reader, 16);
            bool relay = reader.ReadBoolean();
            int payloadLength = reader.ReadInt32();
            operations[i] = new MetadataOperation
            {
                ChangeId = changeId,
                Provenance = new OperationOrigin(author, originAuthorEpoch, originSequence),
                Fingerprint = fingerprint,
                RelayEligible = relay,
                Payload = ReadExactly(reader, payloadLength)
            };
        }
        bool receiveStateOwnedByMetadataStore = hasReceiveStateAuthority && reader.ReadBoolean();
        bool headerFingerprintPresent = hasHeaderFingerprint && reader.ReadBoolean();
        byte[] headerFingerprint = headerFingerprintPresent ? reader.ReadBytes(16) : Array.Empty<byte>();
        if (headerFingerprintPresent && headerFingerprint.Length != 16)
            throw new InvalidDataException("Pending batch header fingerprint is truncated.");
        if (receiveStateOwnedByMetadataStore && headerFingerprint.Length != 16)
            throw new InvalidDataException("Metadata-authoritative PendingApply requires a full v3 header fingerprint.");
        if (stream.Position != stream.Length) throw new InvalidDataException("Pending batch has trailing bytes.");
        return new MetadataIncomingBatch
        {
            ScopeId = scope,
            LocalReplica = local,
            SenderPeer = sender,
            SenderEpoch = epoch,
            TransportPeerEpoch = transportPeerEpoch,
            AuthorStreamNodeId = authorStream,
            NamespaceId = ns,
            BatchId = batchId,
            CursorStart = cursorStart,
            CursorEnd = cursorEnd,
            PreviousBatchDigest = previous,
            BatchDigest = digest,
            WirePayload = wirePayload,
            HeaderFingerprint = headerFingerprint,
            ReceiveStateOwnedByMetadataStore = receiveStateOwnedByMetadataStore,
            Operations = operations
        };
    }

    private static byte[] ReadExactly(BinaryReader reader, int length)
    {
        if (length < 0 || length > 16 * 1024 * 1024) throw new InvalidDataException("Metadata field length is invalid.");
        byte[] bytes = reader.ReadBytes(length);
        if (bytes.Length != length) throw new InvalidDataException("Metadata payload is truncated.");
        return bytes;
    }

    private static NodeId ResolveAuthorStream(MetadataIncomingBatch batch)
        => batch.AuthorStreamNodeId;

    private static ulong GetMaxOriginSequence(MetadataIncomingBatch batch)
        => batch.Operations.Count == 0 ? 0 : batch.Operations[^1].OriginSequence;

    private static void ValidateBatch(MetadataIncomingBatch batch)
    {
        if (string.IsNullOrWhiteSpace(batch.ScopeId))
            throw new ArgumentException("ScopeId is required.", nameof(batch));
        if (batch.LocalReplica.IsZero || batch.SenderPeer.IsZero || batch.BatchId.IsZero)
            throw new ArgumentException("Replica, sender and batch identities must be non-zero.", nameof(batch));
        if ((uint)batch.NamespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(batch.NamespaceId));
        if (batch.CursorStart == 0 || batch.CursorEnd < batch.CursorStart)
            throw new ArgumentOutOfRangeException(nameof(batch.CursorStart));
        ValidateDigest(batch.PreviousBatchDigest, nameof(batch.PreviousBatchDigest));
        ValidateDigest(batch.BatchDigest, nameof(batch.BatchDigest));
        if (batch.HeaderFingerprint.Length is not (0 or 16))
            throw new ArgumentException("Header fingerprint must be empty or 16 bytes.", nameof(batch));
        if (batch.HeaderFingerprint.Length != 0)
            ValidateDigest(batch.HeaderFingerprint, nameof(batch.HeaderFingerprint));
        if (batch.ReceiveStateOwnedByMetadataStore && batch.HeaderFingerprint.Length != 16)
            throw new ArgumentException("Metadata-authoritative receive requires a full header fingerprint.", nameof(batch));
        ArgumentNullException.ThrowIfNull(batch.Operations);
        if (!batch.AuthorStreamNodeId.IsZero &&
            batch.Operations.Any(operation => operation.AuthorNodeId != batch.AuthorStreamNodeId))
            throw new ArgumentException("Incoming batch operations must belong to one author stream.", nameof(batch));

        var seen = new HashSet<BatchId>();
        foreach (var operation in batch.Operations)
        {
            if (operation.AuthorNodeId.IsZero || operation.ChangeId.IsZero)
                throw new ArgumentException("Operation author and ChangeId must be non-zero.", nameof(batch));
            ValidateDigest(operation.Fingerprint, nameof(operation.Fingerprint));
            ArgumentNullException.ThrowIfNull(operation.Payload);
            if (!seen.Add(operation.ChangeId))
                throw new ArgumentException("ChangeId values must be unique within one incoming batch.", nameof(batch));
        }
        if (batch.AuthorStreamNodeId.IsZero)
            throw new InvalidDataException("v3 batch has no resolved author stream.");
        ulong previousOriginSequence = 0;
        foreach (var operation in batch.Operations)
        {
            var provenance = operation.Provenance;
            if (!provenance.IsValid || provenance.AuthorNodeId != batch.AuthorStreamNodeId ||
                provenance.AuthorEpoch != batch.SenderEpoch ||
                provenance.Sequence <= previousOriginSequence)
                throw new InvalidDataException("v3 operation has missing, mismatched or non-monotonic immutable Origin provenance.");
            previousOriginSequence = provenance.Sequence;
        }
    }

    private static void ValidateDigest(byte[]? value, string name)
    {
        ArgumentNullException.ThrowIfNull(value, name);
        if (value.Length != 16)
            throw new ArgumentException("Digest/fingerprint must be 16 bytes.", name);
    }

    private static void ValidatePartitionArguments(
        string scopeId,
        NodeId localReplica,
        int namespaceId,
        NodeId originalAuthor)
    {
        if (string.IsNullOrWhiteSpace(scopeId))
            throw new ArgumentException("ScopeId is required.", nameof(scopeId));
        if (localReplica.IsZero || originalAuthor.IsZero)
            throw new ArgumentException("Replica and author identities must be non-zero.");
        if ((uint)namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
    }

    private static void AddRelaySequenceIndex(
        Dictionary<BatchId, ulong> index, MetadataRelayItem item)
    {
        if (index.TryGetValue(item.ChangeId, out ulong prior))
        {
            if (prior != item.Sequence)
                throw new InvalidDataException("A relay ChangeId was assigned conflicting author-stream sequences.");
            return;
        }
        index.Add(item.ChangeId, item.Sequence);
    }

    private static int FindFirstSequenceAfter<T>(
        IReadOnlyList<T> entries, ulong sequence, Func<T, ulong> sequenceSelector)
    {
        int low = 0;
        int high = entries.Count;
        while (low < high)
        {
            int middle = low + ((high - low) >> 1);
            if (sequenceSelector(entries[middle]) <= sequence)
                low = middle + 1;
            else
                high = middle;
        }
        return low;
    }

    private static MetadataRelayItem CloneRelayItem(MetadataRelayItem item)
        => new(item.Sequence, item.OriginalAuthor, item.ChangeId, item.Payload.ToArray())
        { Provenance = item.Provenance };

    private static MetadataOutboundItem CloneOutboundItem(MetadataOutboundItem item)
        => new(item.Sequence, item.AuthorNodeId, item.ChangeId, item.Payload.ToArray())
        { Provenance = item.Provenance };

    private static void ValidateOutboundLease(PartitionState state, OutboundKey key, ulong generation)
    {
        if (!state.OutboundLeases.TryGetValue(key, out var lease) || lease.Generation != generation)
            throw new InvalidOperationException("Outbound lease was superseded by a newer consumer.");
    }

    private sealed class VolatileSnapshot : IMetadataStoreSnapshot
    {
        private readonly StorageMetadataStore _owner;
        private readonly PartitionKey _partitionKey;
        private PartitionState? _state;
        private StorageSnapshot? _storageSnapshot;

        public VolatileSnapshot(
            StorageMetadataStore owner, PartitionKey partitionKey,
            PartitionState state, StorageSnapshot storageSnapshot)
        {
            _owner = owner;
            _partitionKey = partitionKey;
            _state = state;
            _storageSnapshot = storageSnapshot;
        }

        public ulong GetReceiveCursor(NodeId sender, uint senderEpoch, int namespaceId)
        {
            _ = GetState();
            return _owner.GetMaxReceiveCursor(GetStorageSnapshot(), _partitionKey,
                sender, senderEpoch, namespaceId);
        }

        public ulong GetReceiveCursor(NodeId sender, uint senderEpoch, int namespaceId, NodeId authorStream)
        {
            var state = GetState();
            return _owner.TryReadReceive(GetStorageSnapshot(), _partitionKey,
                new ReceiveKey(sender, senderEpoch, namespaceId, authorStream), out var receive)
                ? receive.CursorEnd : 0;
        }

        public ulong GetRelayHighWatermark(int namespaceId, NodeId originalAuthor)
        {
            var state = GetState();
            return _owner.TryReadStreamWatermark(GetStorageSnapshot(), _partitionKey, 5,
                namespaceId, originalAuthor, 8, out ulong high) ? high : 0;
        }

        public ulong GetOutboundHighWatermark(int namespaceId, NodeId localAuthor)
        {
            var state = GetState();
            return _owner.TryReadStreamWatermark(GetStorageSnapshot(), _partitionKey, 8,
                namespaceId, localAuthor, 8, out ulong high) ? high : 0;
        }

        public MetadataRelayPage ReadRelay(int namespaceId, NodeId originalAuthor, ulong afterSequence, int limit)
        {
            var state = GetState();
            var key = new RelayKey(namespaceId, originalAuthor);
            StorageSnapshot storageSnapshot = GetStorageSnapshot();
            ulong acknowledged = _owner.TryReadStreamWatermark(storageSnapshot, _partitionKey, 5,
                namespaceId, originalAuthor, 0, out ulong ack) ? ack : 0;
            ulong high = _owner.TryReadStreamWatermark(storageSnapshot, _partitionKey, 5,
                namespaceId, originalAuthor, 8, out ulong watermark) ? watermark : 0;
            if (afterSequence > high)
                throw new ArgumentOutOfRangeException(nameof(afterSequence));
            ulong cursor = Math.Max(afterSequence, acknowledged);
            if (limit <= 0 || cursor >= high)
                return new MetadataRelayPage(Array.Empty<MetadataRelayItem>(), cursor);
            MetadataRelayItem[] page = _owner.ReadRelayStoragePage(
                GetStorageSnapshot(), PartitionRoot(_partitionKey), state,
                namespaceId, originalAuthor, cursor, limit, out _);
            if (page.Length == 0 && cursor < high &&
                !_owner.IsRelayHistoryFullyCompacted(storageSnapshot, _partitionKey, key, high, acknowledged))
                throw new InvalidDataException("Relay snapshot high-watermark has no recoverable payload rows.");
            return new MetadataRelayPage(page, page.Length == 0 ? cursor : page[^1].Sequence);
        }

        public MetadataOutboundPage ReadOutbound(int namespaceId, NodeId localAuthor, ulong afterSequence, int limit)
        {
            var state = GetState();
            var key = new OutboundKey(namespaceId, localAuthor);
            StorageSnapshot storageSnapshot = GetStorageSnapshot();
            ulong acknowledged = _owner.TryReadStreamWatermark(storageSnapshot, _partitionKey, 8,
                namespaceId, localAuthor, 0, out ulong ack) ? ack : 0;
            ulong high = _owner.TryReadStreamWatermark(storageSnapshot, _partitionKey, 8,
                namespaceId, localAuthor, 8, out ulong watermark) ? watermark : 0;
            if (afterSequence > high)
                throw new ArgumentOutOfRangeException(nameof(afterSequence));
            ulong cursor = Math.Max(afterSequence, acknowledged);
            if (limit <= 0 || cursor >= high)
                return new MetadataOutboundPage(Array.Empty<MetadataOutboundItem>(), cursor);
            MetadataOutboundItem[] page = _owner.ReadOutboundStoragePage(
                GetStorageSnapshot(), PartitionRoot(_partitionKey), state,
                namespaceId, localAuthor, cursor, limit, out _);
            return new MetadataOutboundPage(page, page.Length == 0 ? cursor : page[^1].Sequence);
        }

        public void Dispose()
        {
            _state = null;
            Interlocked.Exchange(ref _storageSnapshot, null)?.Dispose();
        }

        private StorageSnapshot GetStorageSnapshot()
            => _storageSnapshot ?? throw new ObjectDisposedException(nameof(VolatileSnapshot));

        private PartitionState GetState()
            => _state ?? throw new ObjectDisposedException(nameof(VolatileSnapshot));
    }

    private readonly record struct PartitionKey(string ScopeId, NodeId LocalReplica);
    private readonly record struct ReceiptKey(NodeId Sender, uint Epoch, int NamespaceId, BatchId BatchId);
    private readonly record struct ReceiveKey(NodeId Sender, uint Epoch, int NamespaceId, NodeId AuthorStream);
    private readonly record struct ReceiveChainKey(NodeId Sender, uint Epoch, int NamespaceId, NodeId AuthorStream);
    private readonly record struct LegacyReceiveChainKey(NodeId Sender, uint Epoch, int NamespaceId);
    private readonly record struct LedgerKey(int NamespaceId, BatchId ChangeId);
    // GC only needs immutable Origin for post-Host revalidation. Keep this value
    // lightweight instead of materializing and retaining each ledger fingerprint.
    private readonly record struct ChangeIdGcCandidate(
        LedgerKey Key, OperationOrigin Provenance, byte[]? StorageKey);

    private sealed class ChangeIdTupleStorageKeyView : IReadOnlyList<byte[]>
    {
        private readonly IReadOnlyList<(LedgerKey Key, byte[] StorageKey)> _candidates;

        public ChangeIdTupleStorageKeyView(
            IReadOnlyList<(LedgerKey Key, byte[] StorageKey)> candidates)
            => _candidates = candidates;

        public int Count => _candidates.Count;
        public byte[] this[int index] => _candidates[index].StorageKey;

        public IEnumerator<byte[]> GetEnumerator()
        {
            for (int i = 0; i < _candidates.Count; i++)
                yield return _candidates[i].StorageKey;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            => GetEnumerator();
    }

    private sealed class ChangeIdStorageKeyView : IReadOnlyList<byte[]>
    {
        private readonly IReadOnlyList<ChangeIdGcCandidate> _candidates;
        private readonly int _count;

        public ChangeIdStorageKeyView(
            IReadOnlyList<ChangeIdGcCandidate> candidates,
            int count)
        {
            if ((uint)count > (uint)candidates.Count)
                throw new ArgumentOutOfRangeException(nameof(count));
            _candidates = candidates;
            _count = count;
        }

        public int Count => _count;
        public byte[] this[int index] =>
            _candidates[index].StorageKey
            ?? throw new InvalidOperationException("An approved ChangeId GC candidate has no storage key.");

        public IEnumerator<byte[]> GetEnumerator()
        {
            for (int i = 0; i < _count; i++)
                yield return this[i];
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            => GetEnumerator();
    }

    private sealed class ChangeIdCandidateView : IReadOnlyList<BatchId>
    {
        private readonly IReadOnlyList<ChangeIdGcCandidate> _candidates;

        public ChangeIdCandidateView(IReadOnlyList<ChangeIdGcCandidate> candidates)
            => _candidates = candidates;

        public int Count => _candidates.Count;
        public BatchId this[int index] => _candidates[index].Key.ChangeId;

        public IEnumerator<BatchId> GetEnumerator()
        {
            for (int i = 0; i < _candidates.Count; i++)
                yield return _candidates[i].Key.ChangeId;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
    private readonly record struct RelayKey(int NamespaceId, NodeId Author);
    private readonly record struct OutboundKey(int NamespaceId, NodeId Author);
    private readonly record struct OriginStreamKey(int NamespaceId, NodeId Author, uint AuthorEpoch);
    private readonly record struct OriginIssuedState(ulong Sequence, ulong MaxHlcWall, uint MaxHlcLogical);
    private readonly record struct LeaseState(ulong Generation, Guid ConsumerId);
    private readonly record struct PeerStreamKey(NodeId Peer, uint PeerEpoch, int NamespaceId, NodeId AuthorStream);
    private readonly record struct InterestedPeerKey(int NamespaceId, NodeId Peer, uint PeerEpoch);
    private sealed record PeerStreamStateValue(ulong AckedCursor, ulong PushedCursor, ulong StreamOffset,
        uint IssuerEpoch, byte[] LastBatchDigest, MetadataOutboundBatchReservation? PendingBatch);
    private readonly record struct ReceiveState(ulong CursorEnd);
    private readonly record struct ReceiveChainState(byte[] LastBatchDigest);

    private static bool TryGetSingleEstablishedAuthor(
        PartitionState state, LegacyReceiveChainKey legacy, out NodeId author)
    {
        bool found = false;
        author = default;
        foreach (var pair in state.Receives)
        {
            if (pair.Key.Sender != legacy.Sender || pair.Key.Epoch != legacy.Epoch ||
                pair.Key.NamespaceId != legacy.NamespaceId || pair.Value.CursorEnd == 0)
                continue;
            if (found && pair.Key.AuthorStream != author)
            {
                author = default;
                return false;
            }
            author = pair.Key.AuthorStream;
            found = true;
        }
        return found;
    }

    private static ReceiveChainState GetReceiveChainStateForAuthor(
        PartitionState state, ReceiveKey receiveKey,
        out LegacyReceiveChainKey? legacyKey,
        out ReceiveChainKey? legacyTargetKey,
        out ReceiveChainState? legacyTargetState)
    {
        var key = new ReceiveChainKey(receiveKey.Sender, receiveKey.Epoch,
            receiveKey.NamespaceId, receiveKey.AuthorStream);
        var legacy = new LegacyReceiveChainKey(receiveKey.Sender, receiveKey.Epoch, receiveKey.NamespaceId);
        if (state.ReceiveChains.TryGetValue(key, out var current))
        {
            if (state.LegacyReceiveChains.ContainsKey(legacy))
                throw new InvalidDataException(
                    "Legacy and per-author receive digest rows coexist; a FULL_REPLACE baseline is required.");
            legacyKey = null;
            legacyTargetKey = null;
            legacyTargetState = null;
            return current;
        }

        if (!state.LegacyReceiveChains.TryGetValue(legacy, out var legacyState))
        {
            legacyKey = null;
            legacyTargetKey = null;
            legacyTargetState = null;
            return new ReceiveChainState(new byte[16]);
        }

        if (!TryGetSingleEstablishedAuthor(state, legacy, out NodeId establishedAuthor))
            throw new InvalidDataException(
                "Legacy sender-wide digest chain is ambiguous across author streams; a FULL_REPLACE baseline is required.");

        legacyKey = legacy;
        legacyTargetKey = new ReceiveChainKey(receiveKey.Sender, receiveKey.Epoch,
            receiveKey.NamespaceId, establishedAuthor);
        legacyTargetState = legacyState;
        return establishedAuthor == receiveKey.AuthorStream
            ? legacyState
            : new ReceiveChainState(new byte[16]);
    }

    private static bool HasReceiveChainForAuthor(PartitionState state, ReceiveKey receiveKey)
    {
        var key = new ReceiveChainKey(receiveKey.Sender, receiveKey.Epoch,
            receiveKey.NamespaceId, receiveKey.AuthorStream);
        var legacy = new LegacyReceiveChainKey(receiveKey.Sender, receiveKey.Epoch, receiveKey.NamespaceId);
        if (state.ReceiveChains.ContainsKey(key))
            return !state.LegacyReceiveChains.ContainsKey(legacy);
        if (!state.LegacyReceiveChains.ContainsKey(legacy)) return false;
        return TryGetSingleEstablishedAuthor(state, legacy, out NodeId establishedAuthor) &&
            establishedAuthor == receiveKey.AuthorStream;
    }

    private static byte[] EncodeReceiveChainKey(PartitionKey partitionKey, ReceiveChainKey chain)
    {
        byte[] suffix = new byte[38];
        chain.Sender.WriteTo(suffix);
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), chain.Epoch);
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)chain.NamespaceId));
        chain.AuthorStream.WriteTo(suffix.AsSpan(22));
        return StorageKey(PartitionRoot(partitionKey), 25, suffix);
    }

    private static byte[] EncodeLegacyReceiveChainKey(PartitionKey partitionKey, LegacyReceiveChainKey chain)
    {
        byte[] suffix = new byte[22];
        chain.Sender.WriteTo(suffix);
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), chain.Epoch);
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)chain.NamespaceId));
        return StorageKey(PartitionRoot(partitionKey), 14, suffix);
    }

    private sealed class ReceiptValue(
        ulong cursorStart,
        ulong cursorEnd,
        byte[] previousBatchDigest,
        byte[] batchDigest,
        int newRelayOperationCount,
        NodeId durableAuthor = default,
        byte[]? headerFingerprint = null,
        uint transportPeerEpoch = 0,
        ulong originSequence = 0)
    {
        public ulong CursorStart { get; } = cursorStart;
        public ulong CursorEnd { get; } = cursorEnd;
        public byte[] PreviousBatchDigest { get; } = previousBatchDigest;
        public byte[] BatchDigest { get; } = batchDigest;
        public int NewRelayOperationCount { get; } = newRelayOperationCount;
        public NodeId DurableAuthor { get; } = durableAuthor;
        public byte[] HeaderFingerprint { get; } = headerFingerprint ?? Array.Empty<byte>();
        public uint TransportPeerEpoch { get; } = transportPeerEpoch;
        public ulong OriginSequence { get; } = originSequence;

        public bool Matches(MetadataIncomingBatch batch)
            => CursorStart == batch.CursorStart
                && CursorEnd == batch.CursorEnd
                && PreviousBatchDigest.AsSpan().SequenceEqual(batch.PreviousBatchDigest)
                && BatchDigest.AsSpan().SequenceEqual(batch.BatchDigest)
                && HeaderFingerprint.AsSpan().SequenceEqual(batch.HeaderFingerprint);
    }

    private sealed record PendingApplyValue(MetadataIncomingBatch Batch);
    private sealed record CompletedPendingApplyValue(byte[] BatchDigest, MetadataBatchReceipt Receipt);

        private static bool LedgerMatches(ChangeLedgerValue ledger, MetadataOperation operation)
        => ledger.Fingerprint.AsSpan().SequenceEqual(operation.Fingerprint)
            && ledger.Provenance == operation.Provenance;

private sealed record ChangeLedgerValue(byte[] Fingerprint, bool IsLocalOutbound,
    NodeId AuthorNodeId, uint OriginAuthorEpoch, ulong OriginSequence,
    byte[]? LegacyNodeLedgerFingerprint = null, ulong StreamSequence = 0)
{
    public OperationOrigin Provenance => AuthorNodeId.IsZero || OriginAuthorEpoch == 0 || OriginSequence == 0
        ? default : new OperationOrigin(AuthorNodeId, OriginAuthorEpoch, OriginSequence);
}

private static byte[] EncodeChangeLedgerValue(ChangeLedgerValue ledger)
{
    var provenance = ledger.Provenance.EnsureValid();
    byte[]? legacy = ledger.LegacyNodeLedgerFingerprint;
    if (legacy is not null && (legacy.Length != 16 || legacy.All(value => value == 0)))
        throw new InvalidDataException("Legacy Node ChangeId fingerprint must be a non-zero 16-byte digest.");

    byte[] value = new byte[legacy is null
        ? (ledger.StreamSequence == 0 ? 45 : 53)
        : (ledger.StreamSequence == 0 ? 61 : 69)];
    ledger.Fingerprint.CopyTo(value, 0);
    provenance.AuthorNodeId.WriteTo(value.AsSpan(16));
    BinaryPrimitives.WriteUInt32LittleEndian(value.AsSpan(32), provenance.AuthorEpoch);
    BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(36), provenance.Sequence);
    value[44] = ledger.IsLocalOutbound ? (byte)1 : (byte)0;
    legacy?.CopyTo(value, 45);
    if (ledger.StreamSequence != 0)
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(legacy is null ? 45 : 61), ledger.StreamSequence);
    return value;
}

    private sealed class StorageTransactionGateLease : IDisposable
    {
        private object? _gate;

        public StorageTransactionGateLease(object gate) => _gate = gate;

        public void Dispose()
        {
            object? gate = Interlocked.Exchange(ref _gate, null);
            if (gate is not null)
                Monitor.Exit(gate);
        }
    }

    private sealed class FullReplaceCommitLease : IDisposable
    {
        private object? _gate;
        private IDisposable? _writerAdmission;

        public FullReplaceCommitLease(object gate, IDisposable writerAdmission)
        {
            _gate = gate;
            _writerAdmission = writerAdmission;
        }

        public void Dispose()
        {
            object? gate = Interlocked.Exchange(ref _gate, null);
            if (gate is null) return;
            Monitor.Exit(gate);
            Interlocked.Exchange(ref _writerAdmission, null)?.Dispose();
        }
    }

    private PendingApplyMap CreatePendingApplyMap(PartitionKey partitionKey)
        => new(
            () => EnumeratePendingApplies(partitionKey),
            token => TryReadPendingApply(partitionKey, token));

    private PeerStreamMap CreatePeerStreamMap(PartitionKey partitionKey)
        => new(
            () => EnumeratePeerStreams(partitionKey),
            key => TryReadPeerStream(partitionKey, key));

    private DiskKeySet<PeerStreamKey> CreateRequiredPeerStreamSet(PartitionKey partitionKey)
        => new(
            () => EnumerateRequiredPeerStreams(partitionKey),
            key => ContainsStorageKey(EncodeRequiredPeerStreamKey(partitionKey, key)));

    private DiskKeySet<InterestedPeerKey> CreateInterestedPeerSet(PartitionKey partitionKey)
        => new(
            () => EnumerateInterestedPeers(partitionKey),
            key => ContainsStorageKey(EncodeInterestedPeerKey(partitionKey, key)));

    private RetiredPeerEpochMap CreateRetiredPeerEpochMap(PartitionKey partitionKey)
        => new(
            () => EnumerateRetiredPeerEpochs(partitionKey),
            key => TryReadRetiredPeerEpoch(partitionKey, key));

    private DiskMap<ReceiveKey, ReceiveState> CreateReceiveMap(PartitionKey partitionKey)
        => new(() => EnumerateReceives(partitionKey), key =>
        {
            bool found = TryReadReceive(partitionKey, key, out ReceiveState value);
            return (found, value);
        });

    private DiskMap<ReceiveChainKey, ReceiveChainState> CreateReceiveChainMap(PartitionKey partitionKey)
        => new(() => EnumerateReceiveChains(partitionKey), key =>
        {
            bool found = TryReadReceiveChain(partitionKey, key, out ReceiveChainState value);
            return (found, value);
        });

    private DiskMap<LegacyReceiveChainKey, ReceiveChainState> CreateLegacyReceiveChainMap(PartitionKey partitionKey)
        => new(() => EnumerateLegacyReceiveChains(partitionKey), key =>
        {
            bool found = TryReadLegacyReceiveChain(partitionKey, key, out ReceiveChainState value);
            return (found, value);
        });

    private DiskMap<ReceiptKey, ReceiptValue> CreateReceiptMap(PartitionKey partitionKey)
        => new(() => EnumerateReceiptRows(partitionKey), key =>
        {
            bool found = TryReadReceipt(partitionKey, key, out ReceiptValue value);
            return (found, value);
        });

    private DiskMap<LedgerKey, ChangeLedgerValue> CreateChangeLedgerMap(PartitionKey partitionKey)
        => new(() => EnumerateChangeLedgerRows(partitionKey), key =>
        {
            bool found = TryReadChangeLedger(partitionKey, key, out ChangeLedgerValue value);
            return (found, value);
        });

    private DiskMap<Guid, CompletedPendingApplyValue> CreateCompletedPendingApplyMap(PartitionKey partitionKey)
        => new(() => EnumerateCompletedPendingApplies(partitionKey), token =>
        {
            bool found = TryReadCompletedPendingApply(partitionKey, token, out CompletedPendingApplyValue value);
            return (found, value);
        });

    private ulong GetMaxReceiveCursor(
        StorageSnapshot snapshot, PartitionKey partitionKey,
        NodeId sender, uint senderEpoch, int namespaceId)
    {
        byte[] suffix = new byte[22];
        sender.WriteTo(suffix);
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), senderEpoch);
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)namespaceId));
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), 2, suffix);
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        ulong maximum = 0;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                if (row.Key.Length != prefix.Length + 16 || row.Value.Length != 8)
                    throw new InvalidDataException("Receive cursor storage row is malformed.");
                ulong value = BinaryPrimitives.ReadUInt64LittleEndian(row.Value);
                if (value > maximum) maximum = value;
            }
            if (page.Count < pageSize) return maximum;
        }
    }

    private static byte[] EncodeReceiveKey(PartitionKey partitionKey, ReceiveKey receive)
    {
        byte[] suffix = new byte[38];
        receive.Sender.WriteTo(suffix);
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(16), receive.Epoch);
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(20), checked((ushort)receive.NamespaceId));
        receive.AuthorStream.WriteTo(suffix.AsSpan(22));
        return StorageKey(PartitionRoot(partitionKey), 2, suffix);
    }

    private bool TryReadReceive(PartitionKey partitionKey, ReceiveKey key, out ReceiveState value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return TryReadReceive(snapshot, partitionKey, key, out value);
    }

    private bool TryReadReceive(StorageSnapshot snapshot, PartitionKey partitionKey, ReceiveKey key, out ReceiveState value)
    {
        if (!snapshot.TryGet(EncodeReceiveKey(partitionKey, key), out byte[]? bytes))
        {
            value = default;
            return false;
        }
        if (bytes!.Length != 8)
            throw new InvalidDataException("Receive cursor storage value has an invalid length.");
        value = new ReceiveState(BinaryPrimitives.ReadUInt64LittleEndian(bytes));
        return true;
    }

    private IEnumerable<KeyValuePair<ReceiveKey, ReceiveState>> EnumerateReceives(PartitionKey partitionKey)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        foreach (var pair in EnumerateReceives(partitionKey, snapshot))
            yield return pair;
    }

    private IEnumerable<KeyValuePair<ReceiveKey, ReceiveState>> EnumerateReceives(
        PartitionKey partitionKey, StorageSnapshot snapshot)
    {
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), 2, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] key = row.Key;
                int offset = prefix.Length;
                if (key.Length - offset != 38 || row.Value.Length != 8)
                    throw new InvalidDataException("Receive cursor storage row is malformed.");
                var receiveKey = new ReceiveKey(NodeId.From(key.AsSpan(offset, 16)),
                    BinaryPrimitives.ReadUInt32LittleEndian(key.AsSpan(offset + 16)),
                    BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset + 20)),
                    NodeId.From(key.AsSpan(offset + 22, 16)));
                yield return new KeyValuePair<ReceiveKey, ReceiveState>(receiveKey,
                    new ReceiveState(BinaryPrimitives.ReadUInt64LittleEndian(row.Value)));
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private bool TryReadReceiveChain(PartitionKey partitionKey, ReceiveChainKey key, out ReceiveChainState value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        if (!snapshot.TryGet(EncodeReceiveChainKey(partitionKey, key), out byte[]? bytes))
        {
            value = default;
            return false;
        }
        if (bytes!.Length != 16)
            throw new InvalidDataException("Receive digest-chain storage value has an invalid length.");
        value = new ReceiveChainState(bytes);
        return true;
    }

    private IEnumerable<KeyValuePair<ReceiveChainKey, ReceiveChainState>> EnumerateReceiveChains(PartitionKey partitionKey)
    {
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), 25, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] key = row.Key;
                int offset = prefix.Length;
                if (key.Length - offset != 38 || row.Value.Length != 16)
                    throw new InvalidDataException("Receive digest-chain storage row is malformed.");
                var chainKey = new ReceiveChainKey(NodeId.From(key.AsSpan(offset, 16)),
                    BinaryPrimitives.ReadUInt32LittleEndian(key.AsSpan(offset + 16)),
                    BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset + 20)),
                    NodeId.From(key.AsSpan(offset + 22, 16)));
                yield return new KeyValuePair<ReceiveChainKey, ReceiveChainState>(
                    chainKey, new ReceiveChainState(row.Value));
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private bool TryReadLegacyReceiveChain(
        PartitionKey partitionKey, LegacyReceiveChainKey key, out ReceiveChainState value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        if (!snapshot.TryGet(EncodeLegacyReceiveChainKey(partitionKey, key), out byte[]? bytes))
        {
            value = default;
            return false;
        }
        if (bytes!.Length != 16)
            throw new InvalidDataException("Legacy receive digest-chain storage value has an invalid length.");
        value = new ReceiveChainState(bytes);
        return true;
    }

    private IEnumerable<KeyValuePair<LegacyReceiveChainKey, ReceiveChainState>> EnumerateLegacyReceiveChains(
        PartitionKey partitionKey)
    {
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), 14, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] key = row.Key;
                int offset = prefix.Length;
                if (key.Length - offset != 22 || row.Value.Length != 16)
                    throw new InvalidDataException("Legacy receive digest-chain storage row is malformed.");
                var chainKey = new LegacyReceiveChainKey(NodeId.From(key.AsSpan(offset, 16)),
                    BinaryPrimitives.ReadUInt32LittleEndian(key.AsSpan(offset + 16)),
                    BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset + 20)));
                yield return new KeyValuePair<LegacyReceiveChainKey, ReceiveChainState>(
                    chainKey, new ReceiveChainState(row.Value));
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private bool TryReadReceipt(PartitionKey partitionKey, ReceiptKey key, out ReceiptValue value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        if (!snapshot.TryGet(EncodeReceiptKey(partitionKey, key), out byte[]? bytes))
        {
            value = null!;
            return false;
        }
        value = DecodeReceiptValue(bytes!);
        return true;
    }

    private IEnumerable<KeyValuePair<ReceiptKey, ReceiptValue>> EnumerateReceiptRows(PartitionKey partitionKey)
    {
        byte[] root = PartitionRoot(partitionKey);
        byte[] prefix = StorageKey(root, 1, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] key = row.Key;
                int offset = root.Length + 1;
                if (key.Length - offset != 38)
                    throw new InvalidDataException("Receipt storage key is malformed.");
                var receiptKey = new ReceiptKey(NodeId.From(key.AsSpan(offset, 16)),
                    BinaryPrimitives.ReadUInt32LittleEndian(key.AsSpan(offset + 16)),
                    BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset + 20)),
                    BatchId.From(key.AsSpan(offset + 22, 16)));
                yield return new KeyValuePair<ReceiptKey, ReceiptValue>(receiptKey, DecodeReceiptValue(row.Value));
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private bool TryReadChangeLedger(PartitionKey partitionKey, LedgerKey key, out ChangeLedgerValue value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        Span<byte> storageKey = stackalloc byte[68];
        PartitionRoot(partitionKey).AsSpan().CopyTo(storageKey);
        storageKey[49] = 3;
        BinaryPrimitives.WriteUInt16LittleEndian(storageKey.Slice(50, 2), checked((ushort)key.NamespaceId));
        key.ChangeId.WriteTo(storageKey.Slice(52, 16));
        if (!snapshot.TryGet(storageKey, out byte[]? bytes) || bytes is null)
        {
            value = null!;
            return false;
        }
        value = DecodeChangeLedgerValue(bytes);
        return true;
    }

    private IEnumerable<KeyValuePair<LedgerKey, ChangeLedgerValue>> EnumerateChangeLedgerRows(PartitionKey partitionKey)
    {
        byte[] root = PartitionRoot(partitionKey);
        byte[] prefix = StorageKey(root, 3, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] key = row.Key;
                int offset = root.Length + 1;
                if (key.Length - offset != 18)
                    throw new InvalidDataException("ChangeLedger storage key is malformed.");
                var ledgerKey = new LedgerKey(
                    BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                    BatchId.From(key.AsSpan(offset + 2, 16)));
                yield return new KeyValuePair<LedgerKey, ChangeLedgerValue>(
                    ledgerKey, DecodeChangeLedgerValue(row.Value));
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private bool TryReadCompletedPendingApply(
        PartitionKey partitionKey, Guid token, out CompletedPendingApplyValue value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        byte[] key = StorageKey(PartitionRoot(partitionKey), 13, token.ToByteArray());
        if (!snapshot.TryGet(key, out byte[]? bytes) || bytes is null)
        {
            value = null!;
            return false;
        }
        value = DecodeCompleted(bytes);
        return true;
    }

    private IEnumerable<KeyValuePair<Guid, CompletedPendingApplyValue>> EnumerateCompletedPendingApplies(
        PartitionKey partitionKey)
    {
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), 13, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 32;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                if (row.Key.Length != prefix.Length + 16)
                    throw new InvalidDataException("Completed PendingApply storage key is malformed.");
                yield return new KeyValuePair<Guid, CompletedPendingApplyValue>(
                    new Guid(row.Key.AsSpan(prefix.Length)), DecodeCompleted(row.Value));
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private DiskMap<RelayKey, ulong> CreateRelayAckMap(PartitionKey partitionKey)
        => new(() => EnumerateKeyspace(partitionKey, 5, 18, 16,
                (key, offset) => new RelayKey(BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                    NodeId.From(key.AsSpan(offset + 2, 16))),
                value => BinaryPrimitives.ReadUInt64LittleEndian(value))
                .Where(pair => pair.Value != 0),
            key =>
            {
                bool found = TryReadStreamWatermark(partitionKey, 5, key.NamespaceId, key.Author, 0, out ulong value);
                // Kind 5 combines ACK and high-watermark. A relay stream's first
                // payload creates the row with ACK=0; that is not evidence of a
                // registered local consumer and must not veto peer-safe compaction.
                return (found && value != 0, value);
            });

    private DiskMap<RelayKey, ulong> CreateRelayHighWatermarkMap(PartitionKey partitionKey)
        => new(() => EnumerateKeyspace(partitionKey, 5, 18, 16,
                (key, offset) => new RelayKey(BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                    NodeId.From(key.AsSpan(offset + 2, 16))),
                value => BinaryPrimitives.ReadUInt64LittleEndian(value.AsSpan(8))),
            key =>
            {
                bool found = TryReadStreamWatermark(partitionKey, 5, key.NamespaceId, key.Author, 8, out ulong value);
                return (found, value);
            });

    private DiskMap<OutboundKey, ulong> CreateOutboundAckMap(PartitionKey partitionKey)
        => new(() => EnumerateKeyspace(partitionKey, 8, 18, 16,
                (key, offset) => new OutboundKey(BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                    NodeId.From(key.AsSpan(offset + 2, 16))),
                value => BinaryPrimitives.ReadUInt64LittleEndian(value)),
            key =>
            {
                bool found = TryReadStreamWatermark(partitionKey, 8, key.NamespaceId, key.Author, 0, out ulong value);
                return (found, value);
            });

    private DiskMap<OutboundKey, ulong> CreateOutboundHighWatermarkMap(PartitionKey partitionKey)
        => new(() => EnumerateKeyspace(partitionKey, 8, 18, 16,
                (key, offset) => new OutboundKey(BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                    NodeId.From(key.AsSpan(offset + 2, 16))),
                value => BinaryPrimitives.ReadUInt64LittleEndian(value.AsSpan(8))),
            key =>
            {
                bool found = TryReadStreamWatermark(partitionKey, 8, key.NamespaceId, key.Author, 8, out ulong value);
                return (found, value);
            });

    private DiskMap<OutboundKey, LeaseState> CreateOutboundLeaseMap(PartitionKey partitionKey)
        => new(() => EnumerateKeyspace(partitionKey, 10, 18, 24,
                (key, offset) => new OutboundKey(BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                    NodeId.From(key.AsSpan(offset + 2, 16))),
                value => new LeaseState(BinaryPrimitives.ReadUInt64LittleEndian(value),
                    new Guid(value.AsSpan(8, 16)))),
            key =>
            {
                bool found = TryReadOutboundLease(partitionKey, key, out LeaseState value);
                return (found, value);
            });

    private DiskMap<OriginStreamKey, OriginIssuedState> CreateOriginHighWatermarkMap(PartitionKey partitionKey)
        => new(() => EnumerateKeyspace(partitionKey, 22, 22, 20,
                (key, offset) => new OriginStreamKey(BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                    NodeId.From(key.AsSpan(offset + 2, 16)),
                    BinaryPrimitives.ReadUInt32LittleEndian(key.AsSpan(offset + 18))),
                value => new OriginIssuedState(BinaryPrimitives.ReadUInt64LittleEndian(value),
                    BinaryPrimitives.ReadUInt64LittleEndian(value.AsSpan(8)),
                    BinaryPrimitives.ReadUInt32LittleEndian(value.AsSpan(16)))),
            key =>
            {
                bool found = TryReadOriginHighWatermark(partitionKey, key, out OriginIssuedState value);
                return (found, value);
            });

    private DiskKeySet<InterestedPeerKey> CreateCompletedPeerBaselineSet(PartitionKey partitionKey)
        => new(() => EnumerateKeyspace<InterestedPeerKey, byte>(partitionKey, 19, 22, 0,
                (key, offset) => new InterestedPeerKey(BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                    NodeId.From(key.AsSpan(offset + 2, 16)),
                    BinaryPrimitives.ReadUInt32LittleEndian(key.AsSpan(offset + 18))),
                _ => 0).Select(pair => pair.Key),
            key => ContainsStorageKey(EncodeCompletedPeerBaselineKey(partitionKey, key)));

    private bool TryReadStreamWatermark(PartitionKey partitionKey, byte kind,
        int namespaceId, NodeId author, int valueOffset, out ulong value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return TryReadStreamWatermark(snapshot, partitionKey, kind, namespaceId, author, valueOffset, out value);
    }

    private bool TryReadStreamWatermark(StorageSnapshot snapshot, PartitionKey partitionKey, byte kind,
        int namespaceId, NodeId author, int valueOffset, out ulong value)
    {
        byte[] key = StorageKey(PartitionRoot(partitionKey), kind, KeyNamespaceAuthor(namespaceId, author));
        if (!snapshot.TryGet(key, out byte[]? bytes))
        {
            value = 0;
            return false;
        }
        if (bytes!.Length != 16 || (valueOffset != 0 && valueOffset != 8))
            throw new InvalidDataException("Stream watermark storage row is malformed.");
        value = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(valueOffset));
        return true;
    }

    private bool TryReadOutboundLease(PartitionKey partitionKey, OutboundKey key, out LeaseState value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        byte[] storageKey = StorageKey(PartitionRoot(partitionKey), 10, KeyNamespaceAuthor(key.NamespaceId, key.Author));
        if (!snapshot.TryGet(storageKey, out byte[]? bytes))
        {
            value = default;
            return false;
        }
        if (bytes!.Length != 24)
            throw new InvalidDataException("Outbound lease storage row is malformed.");
        value = new LeaseState(BinaryPrimitives.ReadUInt64LittleEndian(bytes), new Guid(bytes.AsSpan(8, 16)));
        return true;
    }

    private bool TryReadOriginHighWatermark(PartitionKey partitionKey, OriginStreamKey key, out OriginIssuedState value)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        byte[] suffix = new byte[22];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)key.NamespaceId));
        key.Author.WriteTo(suffix.AsSpan(2));
        BinaryPrimitives.WriteUInt32LittleEndian(suffix.AsSpan(18), key.AuthorEpoch);
        byte[] storageKey = StorageKey(PartitionRoot(partitionKey), 22, suffix);
        if (!snapshot.TryGet(storageKey, out byte[]? bytes))
        {
            value = default;
            return false;
        }
        if (bytes!.Length != 20 || key.Author.IsZero || key.AuthorEpoch == 0)
            throw new InvalidDataException("Origin high-watermark storage row is malformed.");
        value = new OriginIssuedState(BinaryPrimitives.ReadUInt64LittleEndian(bytes),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8)),
            BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(16)));
        return true;
    }

    private IEnumerable<KeyValuePair<TKey, TValue>> EnumerateKeyspace<TKey, TValue>(
        PartitionKey partitionKey, byte kind, int keyLength, int valueLength,
        Func<byte[], int, TKey> decodeKey, Func<byte[], TValue> decodeValue)
    {
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), kind, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] key = row.Key;
                int offset = prefix.Length;
                if (key.Length - offset != keyLength || row.Value.Length != valueLength)
                    throw new InvalidDataException($"Metadata keyspace kind {kind} contains a malformed row.");
                yield return new KeyValuePair<TKey, TValue>(decodeKey(key, offset), decodeValue(row.Value));
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private bool ContainsStorageKey(byte[] key)
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return snapshot.TryGet(key, out _);
    }

    private IEnumerable<KeyValuePair<Guid, PendingApplyValue>> EnumeratePendingApplies(
        PartitionKey partitionKey)
    {
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), 12, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 32;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                if (row.Key.Length != prefix.Length + 16)
                    throw new InvalidDataException("PendingApply storage key has an invalid length.");
                yield return new KeyValuePair<Guid, PendingApplyValue>(
                    new Guid(row.Key.AsSpan(prefix.Length)),
                    new PendingApplyValue(DecodeIncomingBatch(row.Value)));
            }
            if (page.Count < pageSize)
                yield break;
        }
    }

    private PendingApplyValue? TryReadPendingApply(PartitionKey partitionKey, Guid applyToken)
    {
        byte[] key = StorageKey(PartitionRoot(partitionKey), 12, applyToken.ToByteArray());
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return snapshot.TryGet(key, out byte[]? value)
            ? new PendingApplyValue(DecodeIncomingBatch(value!))
            : null;
    }

    private IEnumerable<KeyValuePair<(int NamespaceId, NodeId Peer), uint>> EnumerateRetiredPeerEpochs(
        PartitionKey partitionKey)
    {
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), 18, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] key = row.Key;
                int offset = prefix.Length;
                if (key.Length - offset != 18 || row.Value.Length != 4)
                    throw new InvalidDataException("Retired-peer epoch storage row is invalid.");
                uint epoch = BinaryPrimitives.ReadUInt32LittleEndian(row.Value);
                if (epoch == 0)
                    throw new InvalidDataException("Stored retired-peer epoch fence is invalid.");
                yield return new KeyValuePair<(int NamespaceId, NodeId Peer), uint>(
                    (BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                     NodeId.From(key.AsSpan(offset + 2, 16))), epoch);
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private uint? TryReadRetiredPeerEpoch(
        PartitionKey partitionKey, (int NamespaceId, NodeId Peer) key)
    {
        byte[] storageKey = EncodeRetiredPeerEpochKey(partitionKey, key.NamespaceId, key.Peer);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        if (!snapshot.TryGet(storageKey, out byte[]? value)) return null;
        if (value!.Length != 4)
            throw new InvalidDataException("Retired-peer epoch storage value has an invalid length.");
        uint epoch = BinaryPrimitives.ReadUInt32LittleEndian(value);
        if (epoch == 0)
            throw new InvalidDataException("Stored retired-peer epoch fence is invalid.");
        return epoch;
    }

    private IEnumerable<PeerStreamKey> EnumerateRequiredPeerStreams(PartitionKey partitionKey)
    {
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), 16, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] key = row.Key;
                int offset = prefix.Length;
                if (key.Length - offset != 38 || row.Value.Length != 0)
                    throw new InvalidDataException("Required peer-stream storage row is invalid.");
                yield return new PeerStreamKey(
                    NodeId.From(key.AsSpan(offset + 18, 16)),
                    BinaryPrimitives.ReadUInt32LittleEndian(key.AsSpan(offset + 34)),
                    BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                    NodeId.From(key.AsSpan(offset + 2, 16)));
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private IEnumerable<InterestedPeerKey> EnumerateInterestedPeers(PartitionKey partitionKey)
    {
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), 17, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] key = row.Key;
                int offset = prefix.Length;
                if (key.Length - offset != 22 || row.Value.Length != 0)
                    throw new InvalidDataException("Interested-peer storage row is invalid.");
                yield return new InterestedPeerKey(
                    BinaryPrimitives.ReadUInt16LittleEndian(key.AsSpan(offset)),
                    NodeId.From(key.AsSpan(offset + 2, 16)),
                    BinaryPrimitives.ReadUInt32LittleEndian(key.AsSpan(offset + 18)));
            }
            if (page.Count < pageSize) yield break;
        }
    }

    private IEnumerable<KeyValuePair<PeerStreamKey, PeerStreamStateValue>> EnumeratePeerStreams(
        PartitionKey partitionKey)
    {
        byte[] prefix = StorageKey(PartitionRoot(partitionKey), 15, ReadOnlySpan<byte>.Empty);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        const int pageSize = 64;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            foreach (StorageEntry row in page)
            {
                byte[] storageKey = row.Key;
                int offset = prefix.Length;
                if (storageKey.Length - offset != 38)
                    throw new InvalidDataException("Peer-stream storage key has an invalid length.");
                var peerKey = new PeerStreamKey(
                    NodeId.From(storageKey.AsSpan(offset, 16)),
                    BinaryPrimitives.ReadUInt32LittleEndian(storageKey.AsSpan(offset + 16)),
                    BinaryPrimitives.ReadUInt16LittleEndian(storageKey.AsSpan(offset + 20)),
                    NodeId.From(storageKey.AsSpan(offset + 22, 16)));
                yield return new KeyValuePair<PeerStreamKey, PeerStreamStateValue>(
                    peerKey, DecodePeerStreamState(row.Value));
            }
            if (page.Count < pageSize)
                yield break;
        }
    }

    private PeerStreamStateValue? TryReadPeerStream(PartitionKey partitionKey, PeerStreamKey peerKey)
    {
        byte[] key = EncodePeerStreamKey(partitionKey, peerKey);
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        return snapshot.TryGet(key, out byte[]? value)
            ? DecodePeerStreamState(value!)
            : null;
    }

    private sealed class DiskMap<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
        where TKey : notnull
    {
        private readonly Func<IEnumerable<KeyValuePair<TKey, TValue>>> _enumerate;
        private readonly Func<TKey, (bool Found, TValue Value)> _read;

        public static DiskMap<TKey, TValue> Empty { get; } = new(
            () => Array.Empty<KeyValuePair<TKey, TValue>>(), _ => (false, default!));

        public DiskMap(
            Func<IEnumerable<KeyValuePair<TKey, TValue>>> enumerate,
            Func<TKey, (bool Found, TValue Value)> read)
        {
            _enumerate = enumerate ?? throw new ArgumentNullException(nameof(enumerate));
            _read = read ?? throw new ArgumentNullException(nameof(read));
        }

        public IEnumerable<TKey> Keys
        {
            get
            {
                foreach (var pair in _enumerate())
                    yield return pair.Key;
            }
        }

        public bool ContainsKey(TKey key) => _read(key).Found;

        public bool TryGetValue(TKey key, out TValue value)
        {
            var found = _read(key);
            value = found.Value;
            return found.Found;
        }

        // Mutations are published only after the corresponding Storage batch commits.
        // The durable B+ tree is authoritative; this facade never shadows unbounded keys.
        public void Add(TKey key, TValue value) { }
        public bool Remove(TKey key) => false;
        public void Clear() { }
        public TValue this[TKey key]
        {
            get => TryGetValue(key, out TValue value) ? value : throw new KeyNotFoundException();
            set { }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => _enumerate().GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class PendingApplyMap : IEnumerable<KeyValuePair<Guid, PendingApplyValue>>
    {
        private readonly Func<IEnumerable<KeyValuePair<Guid, PendingApplyValue>>> _enumerate;
        private readonly Func<Guid, PendingApplyValue?> _read;

        public static PendingApplyMap Empty { get; } = new(
            () => Array.Empty<KeyValuePair<Guid, PendingApplyValue>>(),
            _ => null);

        public PendingApplyMap(
            Func<IEnumerable<KeyValuePair<Guid, PendingApplyValue>>> enumerate,
            Func<Guid, PendingApplyValue?> read)
        {
            _enumerate = enumerate ?? throw new ArgumentNullException(nameof(enumerate));
            _read = read ?? throw new ArgumentNullException(nameof(read));
        }

        public IEnumerable<PendingApplyValue> Values
        {
            get
            {
                foreach (KeyValuePair<Guid, PendingApplyValue> pair in _enumerate())
                    yield return pair.Value;
            }
        }

        public bool TryGetValue(Guid key, out PendingApplyValue value)
        {
            PendingApplyValue? found = _read(key);
            if (found is null)
            {
                value = null!;
                return false;
            }
            value = found;
            return true;
        }

        public IEnumerator<KeyValuePair<Guid, PendingApplyValue>> GetEnumerator()
            => _enumerate().GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            => GetEnumerator();
    }

    private sealed class RetiredPeerEpochMap :
        IEnumerable<KeyValuePair<(int NamespaceId, NodeId Peer), uint>>
    {
        private readonly Func<IEnumerable<KeyValuePair<(int NamespaceId, NodeId Peer), uint>>> _enumerate;
        private readonly Func<(int NamespaceId, NodeId Peer), uint?> _read;

        public static RetiredPeerEpochMap Empty { get; } = new(
            () => Array.Empty<KeyValuePair<(int NamespaceId, NodeId Peer), uint>>(), _ => null);

        public RetiredPeerEpochMap(
            Func<IEnumerable<KeyValuePair<(int NamespaceId, NodeId Peer), uint>>> enumerate,
            Func<(int NamespaceId, NodeId Peer), uint?> read)
        {
            _enumerate = enumerate ?? throw new ArgumentNullException(nameof(enumerate));
            _read = read ?? throw new ArgumentNullException(nameof(read));
        }

        public bool TryGetValue((int NamespaceId, NodeId Peer) key, out uint value)
        {
            uint? found = _read(key);
            value = found.GetValueOrDefault();
            return found.HasValue;
        }

        public uint this[(int NamespaceId, NodeId Peer) key]
        {
            get => TryGetValue(key, out uint value) ? value : throw new KeyNotFoundException();
            set { }
        }

        public void Add((int NamespaceId, NodeId Peer) key, uint value) { }

        public IEnumerator<KeyValuePair<(int NamespaceId, NodeId Peer), uint>> GetEnumerator()
            => _enumerate().GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class DiskKeySet<T> : IEnumerable<T>
    {
        private readonly Func<IEnumerable<T>> _enumerate;
        private readonly Func<T, bool> _contains;

        public static DiskKeySet<T> Empty { get; } = new(
            () => Array.Empty<T>(), _ => false);

        public DiskKeySet(Func<IEnumerable<T>> enumerate, Func<T, bool> contains)
        {
            _enumerate = enumerate ?? throw new ArgumentNullException(nameof(enumerate));
            _contains = contains ?? throw new ArgumentNullException(nameof(contains));
        }

        public bool Contains(T key) => _contains(key);

        // Call sites publish these only after the matching Storage batch commits.
        // Storage remains authoritative; this facade deliberately keeps no shadow set.
        public bool Add(T key) => false;
        public bool Remove(T key) => false;

        public IEnumerator<T> GetEnumerator() => _enumerate().GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class PeerStreamMap : IEnumerable<KeyValuePair<PeerStreamKey, PeerStreamStateValue>>
    {
        private readonly Func<IEnumerable<KeyValuePair<PeerStreamKey, PeerStreamStateValue>>> _enumerate;
        private readonly Func<PeerStreamKey, PeerStreamStateValue?> _read;

        public static PeerStreamMap Empty { get; } = new(
            () => Array.Empty<KeyValuePair<PeerStreamKey, PeerStreamStateValue>>(),
            _ => null);

        public PeerStreamMap(
            Func<IEnumerable<KeyValuePair<PeerStreamKey, PeerStreamStateValue>>> enumerate,
            Func<PeerStreamKey, PeerStreamStateValue?> read)
        {
            _enumerate = enumerate ?? throw new ArgumentNullException(nameof(enumerate));
            _read = read ?? throw new ArgumentNullException(nameof(read));
        }

        public IEnumerable<PeerStreamKey> Keys
        {
            get
            {
                foreach (var pair in _enumerate())
                    yield return pair.Key;
            }
        }

        public bool ContainsKey(PeerStreamKey key) => _read(key) is not null;

        public bool TryGetValue(PeerStreamKey key, out PeerStreamStateValue value)
        {
            PeerStreamStateValue? found = _read(key);
            if (found is null)
            {
                value = null!;
                return false;
            }
            value = found;
            return true;
        }

        // The corresponding storage batch is committed before these legacy cache
        // publication calls. Disk is authoritative, so no mutable shadow index exists.
        public void Add(PeerStreamKey key, PeerStreamStateValue value) { }
        public bool Remove(PeerStreamKey key) => false;
        public PeerStreamStateValue this[PeerStreamKey key]
        {
            get => TryGetValue(key, out var value)
                ? value
                : throw new KeyNotFoundException();
            set { }
        }

        public IEnumerator<KeyValuePair<PeerStreamKey, PeerStreamStateValue>> GetEnumerator()
            => _enumerate().GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            => GetEnumerator();
    }

    private sealed class PartitionState
    {
        public DiskMap<ReceiptKey, ReceiptValue> Receipts { get; }
        public DiskMap<ReceiveKey, ReceiveState> Receives { get; }
        public DiskMap<ReceiveChainKey, ReceiveChainState> ReceiveChains { get; }
        public DiskMap<LegacyReceiveChainKey, ReceiveChainState> LegacyReceiveChains { get; }
        public DiskMap<LedgerKey, ChangeLedgerValue> ChangeLedger { get; }
        // Rebuildable in-memory index only; ChangeId→sequence is never duplicated on disk.
        public DiskMap<RelayKey, ulong> RelayAck { get; }
        public DiskMap<RelayKey, ulong> RelayHighWatermarks { get; }
        public DiskMap<OutboundKey, ulong> OutboundHighWatermarks { get; }
        public DiskMap<OriginStreamKey, OriginIssuedState> OriginHighWatermarks { get; }
        public DiskMap<OutboundKey, ulong> OutboundAck { get; }
        public DiskMap<OutboundKey, LeaseState> OutboundLeases { get; }
        public PendingApplyMap PendingApplies { get; }

        public PartitionState(PendingApplyMap pendingApplies, PeerStreamMap peerStreams,
            DiskKeySet<PeerStreamKey> requiredPeerStreams, DiskKeySet<InterestedPeerKey> interestedPeers,
            RetiredPeerEpochMap retiredPeerEpochs,
            DiskMap<ReceiveKey, ReceiveState> receives,
            DiskMap<ReceiveChainKey, ReceiveChainState> receiveChains,
            DiskMap<LegacyReceiveChainKey, ReceiveChainState> legacyReceiveChains,
            DiskMap<ReceiptKey, ReceiptValue> receipts,
            DiskMap<LedgerKey, ChangeLedgerValue> changeLedger,
            DiskMap<Guid, CompletedPendingApplyValue> completedPendingApplies,
            DiskMap<RelayKey, ulong> relayAck,
            DiskMap<RelayKey, ulong> relayHighWatermarks,
            DiskMap<OutboundKey, ulong> outboundHighWatermarks,
            DiskMap<OriginStreamKey, OriginIssuedState> originHighWatermarks,
            DiskMap<OutboundKey, ulong> outboundAck,
            DiskMap<OutboundKey, LeaseState> outboundLeases,
            DiskKeySet<InterestedPeerKey> completedPeerBaselines)
        {
            PendingApplies = pendingApplies ?? throw new ArgumentNullException(nameof(pendingApplies));
            PeerStreams = peerStreams ?? throw new ArgumentNullException(nameof(peerStreams));
            RequiredPeerStreams = requiredPeerStreams ?? throw new ArgumentNullException(nameof(requiredPeerStreams));
            InterestedPeers = interestedPeers ?? throw new ArgumentNullException(nameof(interestedPeers));
            RetiredPeerEpochs = retiredPeerEpochs ?? throw new ArgumentNullException(nameof(retiredPeerEpochs));
            Receives = receives ?? throw new ArgumentNullException(nameof(receives));
            ReceiveChains = receiveChains ?? throw new ArgumentNullException(nameof(receiveChains));
            LegacyReceiveChains = legacyReceiveChains ?? throw new ArgumentNullException(nameof(legacyReceiveChains));
            Receipts = receipts ?? throw new ArgumentNullException(nameof(receipts));
            ChangeLedger = changeLedger ?? throw new ArgumentNullException(nameof(changeLedger));
            CompletedPendingApplies = completedPendingApplies ?? throw new ArgumentNullException(nameof(completedPendingApplies));
            RelayAck = relayAck ?? throw new ArgumentNullException(nameof(relayAck));
            RelayHighWatermarks = relayHighWatermarks ?? throw new ArgumentNullException(nameof(relayHighWatermarks));
            OutboundHighWatermarks = outboundHighWatermarks ?? throw new ArgumentNullException(nameof(outboundHighWatermarks));
            OriginHighWatermarks = originHighWatermarks ?? throw new ArgumentNullException(nameof(originHighWatermarks));
            OutboundAck = outboundAck ?? throw new ArgumentNullException(nameof(outboundAck));
            OutboundLeases = outboundLeases ?? throw new ArgumentNullException(nameof(outboundLeases));
            CompletedPeerBaselines = completedPeerBaselines ?? throw new ArgumentNullException(nameof(completedPeerBaselines));
        }
        public DiskMap<Guid, CompletedPendingApplyValue> CompletedPendingApplies { get; }
        public PeerStreamMap PeerStreams { get; }
        public DiskKeySet<PeerStreamKey> RequiredPeerStreams { get; }
        public DiskKeySet<InterestedPeerKey> InterestedPeers { get; }
        public DiskKeySet<InterestedPeerKey> CompletedPeerBaselines { get; }
        public RetiredPeerEpochMap RetiredPeerEpochs { get; }

        public ulong StorageCommitSeq { get; set; }
        public ulong MetadataRevision { get; set; }
        public bool IsPersisted { get; set; }
        public bool HasDiskPrimaryFormatMarker { get; set; }

        public PartitionState CloneForSnapshot()
        {
            // VolatileSnapshot reads bounded control state from its pinned Storage snapshot.
            // Do not copy disk-primary receipts, receive cursors/digest chains, ChangeLedger,
            // PendingApplies, peer indexes or rebuildable caches.
            var clone = new PartitionState(PendingApplyMap.Empty, PeerStreamMap.Empty,
                DiskKeySet<PeerStreamKey>.Empty, DiskKeySet<InterestedPeerKey>.Empty, RetiredPeerEpochMap.Empty,
                DiskMap<ReceiveKey, ReceiveState>.Empty, DiskMap<ReceiveChainKey, ReceiveChainState>.Empty,
                DiskMap<LegacyReceiveChainKey, ReceiveChainState>.Empty,
                DiskMap<ReceiptKey, ReceiptValue>.Empty, DiskMap<LedgerKey, ChangeLedgerValue>.Empty,
                DiskMap<Guid, CompletedPendingApplyValue>.Empty,
                DiskMap<RelayKey, ulong>.Empty, DiskMap<RelayKey, ulong>.Empty,
                DiskMap<OutboundKey, ulong>.Empty, DiskMap<OriginStreamKey, OriginIssuedState>.Empty,
                DiskMap<OutboundKey, ulong>.Empty, DiskMap<OutboundKey, LeaseState>.Empty,
                DiskKeySet<InterestedPeerKey>.Empty);
            return clone;
        }
    }
}