// Compact, durable idempotency receipt for an accepted DELTA_BATCH.
namespace BinSync;

/// <summary>One accepted-batch receipt; legacy receipts may lack cursor and header commitments.</summary>
internal readonly struct InboxReceipt
{
    public NodeId DurableAuthor { get; }
    public long CursorEnd { get; }
    public bool HasCursorEnd { get; }
    public Digest16 Fingerprint { get; }
    public bool HasFingerprint { get; }
    /// <summary>HLC wall time in Unix microseconds when the batch was accepted.</summary>
    public ulong AppliedAt { get; }
    /// <summary>Unix microsecond expiry; zero means no expiry has been scheduled.</summary>
    public ulong ExpiresAt { get; }

    public InboxReceipt(NodeId durableAuthor, long cursorEnd, bool hasCursorEnd,
        Digest16 fingerprint, bool hasFingerprint, ulong appliedAt, ulong expiresAt)
    {
        DurableAuthor = durableAuthor;
        CursorEnd = cursorEnd;
        HasCursorEnd = hasCursorEnd;
        Fingerprint = fingerprint;
        HasFingerprint = hasFingerprint;
        AppliedAt = appliedAt;
        ExpiresAt = expiresAt;
    }
}

/// <summary>
/// Canonical commitment to a DELTA_BATCH header. It deliberately does not inspect
/// entry payloads or causal trailers: a duplicate BatchId is an idempotency hit and
/// must not re-apply or trust replay-supplied ChangeIds/dependencies.
/// </summary>
internal static class InboxReceiptIdentity
{
    private static readonly byte[] Domain =
        System.Text.Encoding.ASCII.GetBytes("BIN-SYNC/INBOX-RECEIPT/v1");

    internal static Digest16 Compute(Batch batch)
    {
        var w = new Writer(128);
        w.Raw(Domain)
            .U16((ushort)batch.NamespaceId)
            .B128(batch.IssuerNodeId)
            .U32((uint)batch.IssuerEpoch)
            .B128(batch.BatchId)
            .U64((ulong)batch.CursorStart)
            .U64((ulong)batch.CursorEnd)
            .B128(batch.PrevBatchDigest)
            .B128(batch.RecordsRoot)
            .B128(batch.BatchDigest)
            .U16((ushort)batch.SchemaId)
            .U16((ushort)batch.SchemaVersion)
            .U8((byte)batch.MergePolicy)
            // The causal-trailer-present transport bit is framing, not batch semantics.
            .U8((byte)(batch.Flags & ~CausalWire.FlagHasCausalTrailer));
        return Digests.Blake3Digest16(w.Value());
    }
}

/// <summary>
/// Adaptive in-memory receipt index. A stream with one receipt stays in a compact
/// singleton map; streams with multiple receipts promote to a BatchId-keyed map so
/// issuer/namespace/epoch are stored once rather than repeated in every key.
/// </summary>
internal sealed class InboxReceiptStore :
    IEnumerable<KeyValuePair<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId), InboxReceipt>>
{
    private readonly record struct StreamKey(NodeId Issuer, int Ns, int Epoch);

    private Dictionary<StreamKey, KeyValuePair<BatchId, InboxReceipt>> _singletons = new();
    private Dictionary<StreamKey, Dictionary<BatchId, InboxReceipt>> _groups = new();
    private int _count;
    private readonly KeyCollection _keys;

    internal InboxReceiptStore() => _keys = new KeyCollection(this);

    internal int Count => _count;
    internal IReadOnlyCollection<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId)> Keys => _keys;

    internal InboxReceipt this[(NodeId Issuer, int Ns, int Epoch, BatchId BatchId) key]
    {
        get => TryGetValue(key, out var receipt)
            ? receipt
            : throw new KeyNotFoundException("Inbox receipt was not found.");
        set
        {
            var stream = new StreamKey(key.Issuer, key.Ns, key.Epoch);
            if (_groups.TryGetValue(stream, out var group) && group.ContainsKey(key.BatchId))
            {
                group[key.BatchId] = value;
                return;
            }
            if (_singletons.TryGetValue(stream, out var singleton) && singleton.Key == key.BatchId)
            {
                _singletons[stream] = new KeyValuePair<BatchId, InboxReceipt>(key.BatchId, value);
                return;
            }
            if (!TryAdd(key, value))
                throw new ArgumentException("Duplicate inbox receipt key.", nameof(key));
        }
    }

    internal bool ContainsKey((NodeId Issuer, int Ns, int Epoch, BatchId BatchId) key) =>
        TryGetValue(key, out _);

    internal bool TryGetValue(
        (NodeId Issuer, int Ns, int Epoch, BatchId BatchId) key,
        out InboxReceipt receipt)
    {
        var stream = new StreamKey(key.Issuer, key.Ns, key.Epoch);
        if (_groups.TryGetValue(stream, out var group))
            return group.TryGetValue(key.BatchId, out receipt);
        if (_singletons.TryGetValue(stream, out var singleton) && singleton.Key == key.BatchId)
        {
            receipt = singleton.Value;
            return true;
        }
        receipt = default;
        return false;
    }

    internal bool TryAdd(
        (NodeId Issuer, int Ns, int Epoch, BatchId BatchId) key,
        InboxReceipt receipt)
    {
        var stream = new StreamKey(key.Issuer, key.Ns, key.Epoch);
        if (_groups.TryGetValue(stream, out var group))
        {
            if (!group.TryAdd(key.BatchId, receipt)) return false;
            _count++;
            return true;
        }

        if (_singletons.TryGetValue(stream, out var singleton))
        {
            if (singleton.Key == key.BatchId) return false;
            var promoted = new Dictionary<BatchId, InboxReceipt>(2)
            {
                [singleton.Key] = singleton.Value,
                [key.BatchId] = receipt,
            };
            _groups.EnsureCapacity(_groups.Count + 1);
            _groups.Add(stream, promoted);
            _singletons.Remove(stream);
            _count++;
            return true;
        }

        _singletons.Add(stream, new KeyValuePair<BatchId, InboxReceipt>(key.BatchId, receipt));
        _count++;
        return true;
    }

    internal void Add(
        (NodeId Issuer, int Ns, int Epoch, BatchId BatchId) key,
        InboxReceipt receipt)
    {
        if (!TryAdd(key, receipt))
            throw new ArgumentException("Duplicate inbox receipt key.", nameof(key));
    }

    internal bool Remove((NodeId Issuer, int Ns, int Epoch, BatchId BatchId) key)
    {
        var stream = new StreamKey(key.Issuer, key.Ns, key.Epoch);
        if (_groups.TryGetValue(stream, out var group))
        {
            if (!group.ContainsKey(key.BatchId)) return false;
            if (group.Count == 1)
            {
                _groups.Remove(stream);
                _count--;
                return true;
            }
            if (group.Count == 2)
            {
                KeyValuePair<BatchId, InboxReceipt> remaining = default;
                foreach (var pair in group)
                    if (pair.Key != key.BatchId) { remaining = pair; break; }
                _singletons.EnsureCapacity(_singletons.Count + 1);
                _singletons.Add(stream, remaining);
                _groups.Remove(stream);
                _count--;
                return true;
            }
            group.Remove(key.BatchId);
            _count--;
            return true;
        }

        if (_singletons.TryGetValue(stream, out var singleton) && singleton.Key == key.BatchId)
        {
            _singletons.Remove(stream);
            _count--;
            return true;
        }
        return false;
    }

    internal void Clear()
    {
        _singletons.Clear();
        _groups.Clear();
        _count = 0;
    }

    /// <summary>Reserve the next add and promote a singleton stream before commit if needed.</summary>
    internal void EnsureCapacityFor(
        (NodeId Issuer, int Ns, int Epoch, BatchId BatchId) key)
    {
        var stream = new StreamKey(key.Issuer, key.Ns, key.Epoch);
        if (_groups.TryGetValue(stream, out var group))
        {
            if (!group.ContainsKey(key.BatchId))
                group.EnsureCapacity(group.Count + 1);
            return;
        }
        if (_singletons.TryGetValue(stream, out var singleton))
        {
            if (singleton.Key == key.BatchId) return;
            var promoted = new Dictionary<BatchId, InboxReceipt>(2)
            {
                [singleton.Key] = singleton.Value,
            };
            _groups.EnsureCapacity(_groups.Count + 1);
            _groups.Add(stream, promoted);
            _singletons.Remove(stream);
            return;
        }
        _singletons.EnsureCapacity(_singletons.Count + 1);
    }

    internal InboxReceiptStore Clone()
    {
        var clone = new InboxReceiptStore
        {
            _singletons = new Dictionary<StreamKey, KeyValuePair<BatchId, InboxReceipt>>(_singletons),
            _groups = new Dictionary<StreamKey, Dictionary<BatchId, InboxReceipt>>(_groups.Count),
            _count = _count,
        };
        foreach (var (stream, receipts) in _groups)
            clone._groups.Add(stream, new Dictionary<BatchId, InboxReceipt>(receipts));
        return clone;
    }

    /// <summary>Swap in a prebuilt clone without allocating during WAL state publication.</summary>
    internal void ReplaceWith(InboxReceiptStore replacement)
    {
        var oldSingletons = _singletons;
        var oldGroups = _groups;
        int oldCount = _count;
        _singletons = replacement._singletons;
        _groups = replacement._groups;
        _count = replacement._count;
        replacement._singletons = oldSingletons;
        replacement._groups = oldGroups;
        replacement._count = oldCount;
    }

    private IEnumerable<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId)> EnumerateKeys()
    {
        foreach (var (stream, singleton) in _singletons)
            yield return (stream.Issuer, stream.Ns, stream.Epoch, singleton.Key);
        foreach (var (stream, group) in _groups)
            foreach (var batchId in group.Keys)
                yield return (stream.Issuer, stream.Ns, stream.Epoch, batchId);
    }

    public IEnumerator<KeyValuePair<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId), InboxReceipt>> GetEnumerator()
    {
        foreach (var (stream, singleton) in _singletons)
            yield return new KeyValuePair<(NodeId, int, int, BatchId), InboxReceipt>(
                (stream.Issuer, stream.Ns, stream.Epoch, singleton.Key), singleton.Value);
        foreach (var (stream, group) in _groups)
            foreach (var (batchId, receipt) in group)
                yield return new KeyValuePair<(NodeId, int, int, BatchId), InboxReceipt>(
                    (stream.Issuer, stream.Ns, stream.Epoch, batchId), receipt);
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

    private sealed class KeyCollection(InboxReceiptStore owner) :
        IReadOnlyCollection<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId)>
    {
        public int Count => owner.Count;
        public IEnumerator<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId)> GetEnumerator() =>
            owner.EnumerateKeys().GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}

