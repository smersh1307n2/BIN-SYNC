// Soft resource bounds for production hosts (outbox / provenance growth).
namespace BinSync;

/// <summary>
/// Optional caps a host can enforce before accepting local writes or inbound relay.
/// The core engine remains unbounded for lab scenarios; production hosts check these
/// before calling <see cref="Node.WriteField"/> / applying untrusted batches.
/// </summary>
public sealed class ResourceLimits
{
    private int _maxOutboxEntriesPerNamespace = 100_000;
    private int _maxSeenChanges = 1_000_000;
    private int _maxRecords;

    /// <summary>Default resource-limit configuration.</summary>
    public static ResourceLimits Default { get; } = new();

    /// <summary>Max outbox entries per namespace (0 = unlimited).</summary>
    public int MaxOutboxEntriesPerNamespace
    {
        get
        {
            return _maxOutboxEntriesPerNamespace;
        }
        init
        {
            if (value < 0) throw new ArgumentOutOfRangeException(nameof(MaxOutboxEntriesPerNamespace));
            _maxOutboxEntriesPerNamespace = value;
        }
    }

    /// <summary>Max SeenChanges provenance entries (0 = unlimited).</summary>
    public int MaxSeenChanges
    {
        get
        {
            return _maxSeenChanges;
        }
        init
        {
            if (value < 0) throw new ArgumentOutOfRangeException(nameof(MaxSeenChanges));
            _maxSeenChanges = value;
        }
    }

    /// <summary>Max records held in storage across all namespaces (0 = unlimited).</summary>
    public int MaxRecords
    {
        get
        {
            return _maxRecords;
        }
        init
        {
            if (value < 0) throw new ArgumentOutOfRangeException(nameof(MaxRecords));
            _maxRecords = value;
        }
    }

    /// <summary>Throw if enqueuing another outbox entry would exceed configured resource limits.</summary>
    public void EnsureCanEnqueueOutbox(Node node, int namespaceId)
    {
        if (MaxOutboxEntriesPerNamespace <= 0) return;
        int n = node.OutboxSize(namespaceId);
        if (n >= MaxOutboxEntriesPerNamespace)
            throw new SyncException("INTERNAL");
    }

    /// <summary>Throw if the provenance seen-set would exceed configured capacity.</summary>
    public void EnsureSeenChangesCapacity(Node node)
    {
        if (MaxSeenChanges <= 0) return;
        if (node.SeenChanges.Count >= MaxSeenChanges)
            throw new SyncException("INTERNAL");
    }

    /// <summary>
    /// Preflight a complete inbound batch before ApplyBatch publishes storage, cursors,
    /// provenance or relay entries. Counts are deltas computed from the staged batch.
    /// </summary>
    internal void EnsureCanApplyBatch(Node node, int namespaceId, int newRecords,
        int relayEntries, int newSeenChanges)
    {
        if (MaxOutboxEntriesPerNamespace > 0 &&
            (long)node.OutboxSizeInternal(namespaceId) + relayEntries > MaxOutboxEntriesPerNamespace)
            throw new SyncException("INTERNAL");

        if (MaxSeenChanges > 0 &&
            (long)node.SeenChanges.Count + newSeenChanges > MaxSeenChanges)
            throw new SyncException("INTERNAL");

        if (MaxRecords > 0)
        {
            long total = 0;
            foreach (var ns in node.Storage.Values) total += ns.Count;
            if (total + newRecords > MaxRecords)
                throw new SyncException("INTERNAL");
        }
    }

    /// <summary>Throw if storage would exceed configured record/namespace limits.</summary>
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
    /// <summary>Record a local field write into storage and outbox.</summary>
    public static bool WriteField(Node node, ResourceLimits limits,
        int namespaceId, RecordId recordId, int fieldId, byte[] value, Hlc? hlc = null)
    {
        limits.EnsureStorageCapacity(node);
        limits.EnsureCanEnqueueOutbox(node, namespaceId);
        return node.WriteField(namespaceId, recordId, fieldId, value, hlc);
    }
}
