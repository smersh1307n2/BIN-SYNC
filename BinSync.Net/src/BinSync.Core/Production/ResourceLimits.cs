// Soft resource bounds for production hosts (outbox / provenance growth).
namespace BinSync;

/// <summary>
/// Optional caps a host can enforce before accepting local writes or inbound relay.
/// The core engine remains unbounded for lab scenarios; production hosts check these
/// before calling <see cref="Node.WriteField"/> / applying untrusted batches.
/// </summary>
public sealed class ResourceLimits
{
    public static ResourceLimits Default { get; } = new();

    /// <summary>Max outbox entries per namespace (0 = unlimited).</summary>
    public int MaxOutboxEntriesPerNamespace { get; init; } = 100_000;

    /// <summary>Max SeenChanges provenance entries (0 = unlimited).</summary>
    public int MaxSeenChanges { get; init; } = 1_000_000;

    /// <summary>Max records held in storage across all namespaces (0 = unlimited).</summary>
    public int MaxRecords { get; init; } = 0;

    public void EnsureCanEnqueueOutbox(Node node, int namespaceId)
    {
        if (MaxOutboxEntriesPerNamespace <= 0) return;
        int n = node.OutboxSize(namespaceId);
        if (n >= MaxOutboxEntriesPerNamespace)
            throw new SyncException("INTERNAL");
    }

    public void EnsureSeenChangesCapacity(Node node)
    {
        if (MaxSeenChanges <= 0) return;
        if (node.SeenChanges.Count >= MaxSeenChanges)
            throw new SyncException("INTERNAL");
    }

    public void EnsureStorageCapacity(Node node)
    {
        if (MaxRecords <= 0) return;
        int total = 0;
        foreach (var ns in node.Storage.Values) total += ns.Count;
        if (total >= MaxRecords)
            throw new SyncException("INTERNAL");
    }
}

/// <summary>
/// Convenience writes that honor <see cref="ResourceLimits"/> before mutating the node.
/// </summary>
public static class BoundedNode
{
    public static bool WriteField(Node node, ResourceLimits limits,
        int namespaceId, byte[] recordId, int fieldId, byte[] value, Hlc? hlc = null)
    {
        limits.EnsureStorageCapacity(node);
        limits.EnsureCanEnqueueOutbox(node, namespaceId);
        return node.WriteField(namespaceId, recordId, fieldId, value, hlc);
    }
}
