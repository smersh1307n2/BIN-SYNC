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
    private int _maxGlobalChangeLedgerEntries = 1_000_000;
    private int _maxInboxReceipts = 1_000_000;
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

    /// <summary>Max local WAL replay markers retained in SeenChanges (0 = unlimited).</summary>
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

    /// <summary>Max globally retained ChangeId fingerprints (0 = unlimited).</summary>
    public int MaxGlobalChangeLedgerEntries
    {
        get => _maxGlobalChangeLedgerEntries;
        init
        {
            if (value < 0) throw new ArgumentOutOfRangeException(nameof(MaxGlobalChangeLedgerEntries));
            _maxGlobalChangeLedgerEntries = value;
        }
    }

    /// <summary>Max durable accepted-batch receipts retained in Inbox (0 = unlimited).</summary>
    public int MaxInboxReceipts
    {
        get => _maxInboxReceipts;
        init
        {
            if (value < 0) throw new ArgumentOutOfRangeException(nameof(MaxInboxReceipts));
            _maxInboxReceipts = value;
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
    internal void EnsureCanEnqueueOutbox(Node node, int namespaceId)
    {
        if (MaxOutboxEntriesPerNamespace <= 0) return;
        int n = node.OutboxSize(namespaceId);
        if (n >= MaxOutboxEntriesPerNamespace)
            throw new SyncException("INTERNAL");
    }

    /// <summary>Throw if the local WAL replay-marker set would exceed configured capacity.</summary>
    internal void EnsureSeenChangesCapacity(Node node)
    {
        if (MaxSeenChanges <= 0) return;
        if (node.SeenChanges.Count >= MaxSeenChanges)
            throw new SyncException("INTERNAL");
    }

    /// <summary>Throw if a local write would exceed the global ChangeId ledger cap.</summary>
    internal void EnsureGlobalChangeLedgerCapacity(Node node)
    {
        if (MaxGlobalChangeLedgerEntries > 0 &&
            node.GlobalChangeLedger.Count >= MaxGlobalChangeLedgerEntries)
            throw new SyncException("INTERNAL");
    }

    /// <summary>
    /// Preflight a complete inbound batch before ApplyBatch publishes storage, cursors,
    /// global dedup ledger or relay entries. Counts are deltas computed from the staged batch.
    /// </summary>
    internal void EnsureCanApplyBatch(Node node, int namespaceId, int newRecords,
        int relayEntries, int newGlobalChanges)
    {
        if (MaxOutboxEntriesPerNamespace > 0 &&
            (long)node.OutboxSizeInternal(namespaceId) + relayEntries > MaxOutboxEntriesPerNamespace)
            throw new SyncException("INTERNAL");

        if (MaxGlobalChangeLedgerEntries > 0 &&
            (long)node.GlobalChangeLedger.Count + newGlobalChanges > MaxGlobalChangeLedgerEntries)
            throw new SyncException("INTERNAL");

        // Every newly accepted batch creates one durable idempotency receipt.
        // There is deliberately no TTL-based eviction until the protocol proves
        // a safe duplicate-replay horizon.
        if (MaxInboxReceipts > 0 && node.InboxAuthors.Count >= MaxInboxReceipts)
            throw new SyncException("INTERNAL");

        if (MaxRecords > 0)
        {
            long total = 0;
            foreach (var ns in node.Storage.Values) total += ns.Count;
            if (total + newRecords > MaxRecords)
                throw new SyncException("INTERNAL");
        }
    }

    /// <summary>Preflight a complete snapshot against the configured storage cap.</summary>
    internal void EnsureCanApplySnapshot(Node node, int newRecords)
    {
        if (MaxRecords <= 0 || newRecords == 0) return;
        long total = 0;
        foreach (var ns in node.Storage.Values) total += ns.Count;
        if (total + newRecords > MaxRecords)
            throw new SyncException("INTERNAL");
    }

    /// <summary>Throw if storage would exceed configured record/namespace limits.</summary>
    internal void EnsureStorageCapacity(Node node)
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
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(limits);
        ArgumentNullException.ThrowIfNull(value);
        return node.WriteFieldBounded(limits, namespaceId, recordId, fieldId, value, hlc);
    }
}
