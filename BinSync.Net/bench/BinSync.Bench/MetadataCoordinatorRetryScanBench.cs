using System.Buffers.Binary;
using BinSync;
using BinSync.Storage;
using BenchmarkDotNet.Attributes;
using Fld1.Storage.Engine;

namespace BinSync.Bench;

/// <summary>
/// Measures the real coordinator retry after Metadata Store freeze succeeded but
/// provider-feed acknowledgement failed. Setup and the first publication attempt
/// are excluded; the measured call scans already-frozen provider feed rows.
/// </summary>
[MemoryDiagnoser]
[InvocationCount(1)]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public sealed class MetadataCoordinatorRetryScanBench
{
    private const string Scope = "metadata-coordinator-retry-bench";
    private const int NamespaceId = 1;
    private static readonly NodeId Local = NodeId.From(IdBytes(1));
    private string _directory = null!;
    private StorageDatabase _database = null!;
    private StorageDataProvider _innerProvider = null!;
    private FailOnceAckProvider _provider = null!;
    private StorageMetadataStore _metadata = null!;
    private Node _node = null!;
    private StorageSyncCoordinator _coordinator = null!;

    [Params(256, 1_024, 4_096)]
    public int PendingFeedRows { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _directory = Path.Combine(Path.GetTempPath(),
            "binsync-coordinator-retry-bench-" + Guid.NewGuid().ToString("N"));
        _database = StorageDatabase.Open(_directory, 703);
        _innerProvider = new StorageDataProvider(_database);
        _provider = new FailOnceAckProvider(_innerProvider);
        _metadata = new StorageMetadataStore(_database);
        _node = new NodeBuilder()
            .WithNodeId(Local)
            .WithClock(static () => 10_000_000)
            .Build();
        _coordinator = new StorageSyncCoordinator(_node, _provider, NamespaceId, _metadata, Scope);
        _coordinator.EnableMetadataStoreOwnedPublication(static _ => { });

        for (int i = 0; i < PendingFeedRows; i++)
        {
            _provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(IdBytes(i + 100)),
                FieldId = 0x0101,
                AuthorNodeId = Local,
                HlcWall = (ulong)(1_000_000 + i),
                HlcLogical = 0,
                Value = BitConverter.GetBytes(i)
            });
        }

        try
        {
            _coordinator.PublishLocalChanges(static _ => { }, limit: PendingFeedRows + 1);
            throw new InvalidOperationException("The injected first provider ACK failure did not occur.");
        }
        catch (IOException ex) when (ex.Message == FailOnceAckProvider.InjectedFailureMessage)
        {
            // The immutable Metadata Store freeze is committed; the provider feed
            // intentionally remains pending for the measured retry.
        }

        if (_metadata.GetOutboundHighWatermark(Scope, Local, NamespaceId, Local) !=
            (ulong)PendingFeedRows)
            throw new InvalidOperationException("Benchmark setup did not freeze the complete provider feed.");
        if (_provider.ReadLocalChanges(0, 1).Changes.Count != 1)
            throw new InvalidOperationException("Benchmark setup unexpectedly acknowledged the provider feed.");
    }

    [IterationCleanup]
    public void Cleanup()
    {
        _node.Dispose();
        _provider.Dispose();
        _metadata.Dispose();
        _database.Dispose();
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }

    [Benchmark(Description = "Coordinator retry after durable freeze / pending feed row scale")]
    public int MetadataCoordinatorRetryFrozenPublication()
    {
        int published = _coordinator.PublishLocalChanges(static _ => { }, limit: PendingFeedRows + 1);
        if (published != PendingFeedRows)
            throw new InvalidOperationException("Coordinator retry did not scan the complete frozen feed.");
        if (_provider.ReadLocalChanges(0, 1).Changes.Count != 0)
            throw new InvalidOperationException("Coordinator retry did not acknowledge the frozen feed.");
        return published;
    }

    private static byte[] IdBytes(long value)
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(8), value);
        return bytes;
    }

    private sealed class FailOnceAckProvider : IDataProvider, IProviderPublicationMode
    {
        public const string InjectedFailureMessage = "Injected provider ACK interruption.";
        private readonly StorageDataProvider _inner;
        private int _failNextAck = 1;

        public FailOnceAckProvider(StorageDataProvider inner) => _inner = inner;

        public RecordId ResolveRecordId(ReadOnlyMemory<byte> sourceItemKey) =>
            _inner.ResolveRecordId(sourceItemKey);
        public ushort ResolveFieldId(ReadOnlyMemory<byte> sourceFieldKey) =>
            _inner.ResolveFieldId(sourceFieldKey);
        public void AppendLocalChange(ProviderMutation mutation) => _inner.AppendLocalChange(mutation);
        public ProviderChangePage ReadLocalChanges(ulong afterSequence, int limit) =>
            _inner.ReadLocalChanges(afterSequence, limit);
        public void AcknowledgeLocalChanges(ulong safeThroughSequence)
        {
            if (Interlocked.Exchange(ref _failNextAck, 0) == 1)
                throw new IOException(InjectedFailureMessage);
            _inner.AcknowledgeLocalChanges(safeThroughSequence);
        }
        public ulong LocalChangeHighWatermark => _inner.LocalChangeHighWatermark;
        public ProviderSnapshotHandle OpenSnapshot() => _inner.OpenSnapshot();
        public ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit) =>
            _inner.ReadSnapshot(snapshotToken, afterIndex, limit);
        public void ReleaseSnapshot(Guid snapshotToken) => _inner.ReleaseSnapshot(snapshotToken);
        public ProviderApplyReceipt ApplyRemoteBatch(Guid applyToken, IReadOnlyList<ProviderMutation> mutations) =>
            _inner.ApplyRemoteBatch(applyToken, mutations);
        public bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation) =>
            _inner.TryReadCurrent(recordId, fieldId, out mutation);
        public bool TryGetMetadataStoreOwnedPublicationCutover(out ulong nodeOutboxHighWatermark) =>
            _inner.TryGetMetadataStoreOwnedPublicationCutover(out nodeOutboxHighWatermark);
        public void EnableMetadataStoreOwnedPublication(ulong nodeOutboxHighWatermark) =>
            _inner.EnableMetadataStoreOwnedPublication(nodeOutboxHighWatermark);
        public void Dispose() => _inner.Dispose();
    }
}
