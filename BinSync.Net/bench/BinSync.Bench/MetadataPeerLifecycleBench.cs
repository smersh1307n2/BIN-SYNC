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

    private static byte[] IdBytes(long value)
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

    [Benchmark(Description = "Register peer / unrelated outbound payload scale")]
    public int RegisterPeer()
    {
        Store.RegisterInterestedPeer(Scope, Local, Peer(3), 1, NamespaceId);
        return 1;
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
        Store.RegisterInterestedPeer(Scope, Local, Peer(3), 1, NamespaceId);
    }

    [IterationCleanup]
    public void Cleanup() => DisposeStore();

    [Benchmark(Description = "Complete peer baseline / unrelated outbound payload scale")]
    public int CompleteBaseline()
    {
        Store.CompletePeerInitialSync(Scope, Local, Peer(3), 1, NamespaceId,
            new[] { new MetadataPeerStreamBaseline(Author, (ulong)UnrelatedOutboundItems, 1) });
        return 1;
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
        Store.RegisterInterestedPeer(Scope, Local, Peer(3), 1, NamespaceId);
        Store.CompletePeerInitialSync(Scope, Local, Peer(3), 1, NamespaceId,
            new[] { new MetadataPeerStreamBaseline(Author, (ulong)UnrelatedOutboundItems, 1) });
        Store.RegisterInterestedPeer(Scope, Local, Peer(4), 2, NamespaceId);
        Store.CompletePeerInitialSync(Scope, Local, Peer(4), 2, NamespaceId,
            new[] { new MetadataPeerStreamBaseline(Author, (ulong)UnrelatedOutboundItems, 1) });
    }

    [IterationCleanup]
    public void Cleanup() => DisposeStore();

    [Benchmark(Description = "Retire old peer epoch / unrelated outbound payload scale")]
    public int RetireOldEpoch()
    {
        Store.RetireInterestedPeer(Scope, Local, Peer(3), 1, 2, NamespaceId);
        return 1;
    }
}
