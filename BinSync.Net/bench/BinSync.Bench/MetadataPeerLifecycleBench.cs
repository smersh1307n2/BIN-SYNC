using System.Buffers.Binary;
using BinSync;
using BinSync.Storage;
using BenchmarkDotNet.Attributes;

namespace BinSync.Bench;

/// <summary>
/// Measures peer lifecycle metadata mutations with a growing unrelated outbound
/// payload set. Iteration setup is excluded from measurements; each operation
/// starts from the same durable-store shape.
/// </summary>
public abstract class MetadataPeerLifecycleBenchBase
{
    protected const string Scope = "metadata-peer-lifecycle-bench";
    protected const int NamespaceId = 1;
    protected const int OperationsPerInvocation = 256;
    protected static readonly NodeId Local = NodeId.From(IdBytes(1));
    protected static readonly NodeId Author = NodeId.From(IdBytes(2));
    protected StorageMetadataStore Store = null!;

    [Params(100, 1_000, 10_000)]
    public int UnrelatedOutboundItems { get; set; }

    protected void CreateStore()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-metadata-peer-bench-" + Guid.NewGuid().ToString("N"));
        Store = new StorageMetadataStore(directory, 701, deleteOnDispose: true);
        var operations = new MetadataFrozenOperation[UnrelatedOutboundItems];
        for (int i = 0; i < operations.Length; i++)
        {
            byte[] identity = IdBytes(i + 100);
            operations[i] = new MetadataFrozenOperation
            {
                ChangeId = BatchId.From(identity),
                Fingerprint = identity.ToArray(),
                Payload = BitConverter.GetBytes(i)
            };
        }
        Store.FreezeOutbound(Scope, Local, NamespaceId, Author, operations);
    }

    protected static NodeId Peer(long id) => NodeId.From(IdBytes(id));

    protected static byte[] IdBytes(long value)
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(8), value);
        return bytes;
    }

    protected void DisposeStore() => Store.Dispose();
}

[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class MetadataPeerRegistrationBench : MetadataPeerLifecycleBenchBase
{
    [IterationSetup]
    public void Setup() => CreateStore();

    [IterationCleanup]
    public void Cleanup() => DisposeStore();

    [Benchmark(OperationsPerInvoke = OperationsPerInvocation, Description = "Register peer / unrelated outbound payload scale")]
    public int RegisterPeer()
    {
        for (int i = 0; i < OperationsPerInvocation; i++)
            Store.RegisterInterestedPeer(Scope, Local, Peer(1_000 + i), 1, NamespaceId);
        return OperationsPerInvocation;
    }
}

[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class MetadataPeerBaselineBench : MetadataPeerLifecycleBenchBase
{
    [IterationSetup]
    public void Setup()
    {
        CreateStore();
        for (int i = 0; i < OperationsPerInvocation; i++)
            Store.RegisterInterestedPeer(Scope, Local, Peer(1_000 + i), 1, NamespaceId);
    }

    [IterationCleanup]
    public void Cleanup() => DisposeStore();

    [Benchmark(OperationsPerInvoke = OperationsPerInvocation, Description = "Complete peer baseline / unrelated outbound payload scale")]
    public int CompleteBaseline()
    {
        for (int i = 0; i < OperationsPerInvocation; i++)
            Store.CompletePeerInitialSync(Scope, Local, Peer(1_000 + i), 1, NamespaceId,
                new[] { new MetadataPeerStreamBaseline(Author, (ulong)UnrelatedOutboundItems, 1) });
        return OperationsPerInvocation;
    }
}

[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class MetadataPeerRetirementBench : MetadataPeerLifecycleBenchBase
{
    [IterationSetup]
    public void Setup()
    {
        CreateStore();
        for (int i = 0; i < OperationsPerInvocation; i++)
        {
            NodeId peer = Peer(1_000 + i);
            Store.RegisterInterestedPeer(Scope, Local, peer, 1, NamespaceId);
            Store.CompletePeerInitialSync(Scope, Local, peer, 1, NamespaceId,
                new[] { new MetadataPeerStreamBaseline(Author, (ulong)UnrelatedOutboundItems, 1) });
            Store.RegisterInterestedPeer(Scope, Local, peer, 2, NamespaceId);
            Store.CompletePeerInitialSync(Scope, Local, peer, 2, NamespaceId,
                new[] { new MetadataPeerStreamBaseline(Author, (ulong)UnrelatedOutboundItems, 1) });
        }
    }

    [IterationCleanup]
    public void Cleanup() => DisposeStore();

    [Benchmark(OperationsPerInvoke = OperationsPerInvocation, Description = "Retire old peer epoch / unrelated outbound payload scale")]
    public int RetireOldEpoch()
    {
        for (int i = 0; i < OperationsPerInvocation; i++)
            Store.RetireInterestedPeer(Scope, Local, Peer(1_000 + i), 1, 2, NamespaceId);
        return OperationsPerInvocation;
    }
}


[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class MetadataPeerChangeIdentityLookupBench
{
    private const string Scope = "metadata-change-identity-bench";
    private const int NamespaceId = 1;
    private const int LookupOperationsPerInvocation = 2_097_152;
    private static readonly NodeId Local = NodeId.From(IdBytes(1));
    private static readonly NodeId Author = NodeId.From(IdBytes(2));
    private StorageMetadataStore _store = null!;
    private IMetadataOutboundChangeIdentityStore _identityStore = null!;
    private BatchId[] _changeIds = Array.Empty<BatchId>();

    [Params(256, 1_024, 4_096)]
    public int PendingFeedRows { get; set; }

    [IterationSetup]
    public void Setup()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-change-identity-bench-" + Guid.NewGuid().ToString("N"));
        _store = new StorageMetadataStore(directory, 702, deleteOnDispose: true);
        _identityStore = _store;
        var operations = new MetadataFrozenOperation[PendingFeedRows];
        _changeIds = new BatchId[PendingFeedRows];
        for (int i = 0; i < PendingFeedRows; i++)
        {
            byte[] identity = IdBytes(i + 100);
            BatchId changeId = BatchId.From(identity);
            _changeIds[i] = changeId;
            operations[i] = new MetadataFrozenOperation
            {
                ChangeId = changeId,
                Fingerprint = identity,
                Payload = BitConverter.GetBytes(i)
            };
        }
        _store.FreezeOutbound(Scope, Local, NamespaceId, Author, operations);
    }

    [IterationCleanup]
    public void Cleanup() => _store.Dispose();

    [Benchmark(OperationsPerInvoke = LookupOperationsPerInvocation,
        Description = "Frozen ChangeId fingerprint lookup / pending feed row scale")]
    public int LookupFrozenChangeIdFingerprints()
    {
        for (int i = 0; i < LookupOperationsPerInvocation; i++)
        {
            BatchId changeId = _changeIds[i % _changeIds.Length];
            if (!_identityStore.TryGetOutboundChangeFingerprint(
                    Scope, Local, NamespaceId, changeId, out ReadOnlyMemory<byte> fingerprint) ||
                fingerprint.Length != 16)
                throw new InvalidOperationException("Frozen ChangeId fingerprint lookup failed.");
        }
        return LookupOperationsPerInvocation;
    }

    private static byte[] IdBytes(long value)
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(8), value);
        return bytes;
    }
}


/// <summary>
/// Measures one current-format local outbound freeze against a large unrelated
/// ChangeId/outbound ledger. Seeding is outside the measured operation.
/// </summary>
[MemoryDiagnoser]
[InvocationCount(1)]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class MetadataStoreCardinalityBench
{
    private const string Scope = "metadata-store-cardinality-bench";
    private const int NamespaceId = 1;
    private static readonly NodeId Local = NodeId.From(IdBytes(1));
    private static readonly NodeId Author = NodeId.From(IdBytes(2));
    private StorageMetadataStore _store = null!;

    [Params(10_000, 100_000)]
    public int ExistingLedgerRows { get; set; }

    [IterationSetup]
    public void Setup()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-metadata-cardinality-bench-" + Guid.NewGuid().ToString("N"));
        _store = new StorageMetadataStore(directory, 704, deleteOnDispose: true);

        const int seedBatchSize = 2_048;
        for (int start = 0; start < ExistingLedgerRows; start += seedBatchSize)
        {
            int count = Math.Min(seedBatchSize, ExistingLedgerRows - start);
            var operations = new MetadataFrozenOperation[count];
            for (int i = 0; i < count; i++)
            {
                byte[] identity = IdBytes(start + i + 100);
                operations[i] = new MetadataFrozenOperation
                {
                    ChangeId = BatchId.From(identity),
                    Fingerprint = identity,
                    Payload = BitConverter.GetBytes(start + i)
                };
            }
            _store.FreezeOutbound(Scope, Local, NamespaceId, Author, operations);
        }

        if (_store.GetOutboundHighWatermark(Scope, Local, NamespaceId, Author) !=
            (ulong)ExistingLedgerRows)
            throw new InvalidOperationException("Cardinality benchmark setup did not seed the requested ledger size.");
    }

    [IterationCleanup]
    public void Cleanup() => _store.Dispose();

    [Benchmark(Description = "Freeze one local operation / existing ChangeId ledger cardinality")]
    public int FreezeOneOperationAtCardinality()
    {
        byte[] identity = IdBytes(2_000_000 + ExistingLedgerRows);
        var operation = new MetadataFrozenOperation
        {
            ChangeId = BatchId.From(identity),
            Fingerprint = identity,
            Payload = new byte[] { 0xA5, 0x5A }
        };
        IReadOnlyList<MetadataOutboundItem> frozen = _store.FreezeOutbound(
            Scope, Local, NamespaceId, Author, new[] { operation });
        if (frozen.Count != 1 || frozen[0].Sequence != (ulong)ExistingLedgerRows + 1)
            throw new InvalidOperationException("Cardinality benchmark freeze did not allocate exactly one next sequence.");
        return frozen.Count;
    }

    private static byte[] IdBytes(long value)
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(8), value);
        return bytes;
    }
}


/// <summary>
/// One-operation freeze measurement after a one-million-row unrelated ledger is
/// seeded once per process. ShortRunJob keeps the setup cost bounded and outside
/// the measured method.
/// </summary>
[MemoryDiagnoser]
[InvocationCount(1)]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class MetadataStoreMillionRowBench
{
    private const string Scope = "metadata-store-million-row-bench";
    private const int NamespaceId = 1;
    private static readonly NodeId Local = NodeId.From(IdBytes(1));
    private static readonly NodeId Author = NodeId.From(IdBytes(2));
    private StorageMetadataStore _store = null!;
    private long _nextId = 3_000_000;

    [GlobalSetup]
    public void Setup()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-metadata-million-row-bench-" + Guid.NewGuid().ToString("N"));
        _store = new StorageMetadataStore(directory, 705, deleteOnDispose: true);

        const int rowCount = 1_000_000;
        const int seedBatchSize = 2_048;
        for (int start = 0; start < rowCount; start += seedBatchSize)
        {
            int count = Math.Min(seedBatchSize, rowCount - start);
            var operations = new MetadataFrozenOperation[count];
            for (int i = 0; i < count; i++)
            {
                byte[] identity = IdBytes(start + i + 100);
                operations[i] = new MetadataFrozenOperation
                {
                    ChangeId = BatchId.From(identity),
                    Fingerprint = identity,
                    Payload = BitConverter.GetBytes(start + i)
                };
            }
            _store.FreezeOutbound(Scope, Local, NamespaceId, Author, operations);
        }

        if (_store.GetOutboundHighWatermark(Scope, Local, NamespaceId, Author) != (ulong)rowCount)
            throw new InvalidOperationException("Million-row benchmark setup did not seed one million ledger rows.");
    }

    [GlobalCleanup]
    public void Cleanup() => _store.Dispose();

    [Benchmark(Description = "Freeze one local operation / 1,000,000 existing ledger rows")]
    public int FreezeOneOperationAtMillionRows()
    {
        byte[] identity = IdBytes(Interlocked.Increment(ref _nextId));
        var operation = new MetadataFrozenOperation
        {
            ChangeId = BatchId.From(identity),
            Fingerprint = identity,
            Payload = new byte[] { 0xA5, 0x5A }
        };
        IReadOnlyList<MetadataOutboundItem> frozen = _store.FreezeOutbound(
            Scope, Local, NamespaceId, Author, new[] { operation });
        if (frozen.Count != 1 || frozen[0].Sequence != 1_000_000UL + (ulong)(_nextId - 3_000_000))
            throw new InvalidOperationException("Million-row benchmark did not allocate the expected next sequence.");
        return frozen.Count;
    }

    private static byte[] IdBytes(long value)
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(8), value);
        return bytes;
    }
}
