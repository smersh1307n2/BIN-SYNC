using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using BinSync;
using BinSync.DataProvider.Conformance;

namespace BinSync.Tests.DataProvider;

/// <summary>
/// Volatile reference provider. It models the semantic contract but makes no
/// process-restart or durable-storage claim.
/// </summary>
public sealed class VolatileDataProviderAdapter : IDataProviderConformanceAdapter
{
    private readonly Dictionary<string, RecordId> _recordIds = new(StringComparer.Ordinal);
    private readonly Dictionary<RecordId, string> _recordKeys = new();
    private readonly Dictionary<string, ushort> _fieldIds = new(StringComparer.Ordinal);
    private readonly Dictionary<ushort, string> _fieldKeys = new();
    private Dictionary<FieldKey, ProviderMutation> _current = new();
    private readonly List<ProviderChange> _changes = new();
    private readonly Dictionary<Guid, ProviderMutation[]> _snapshots = new();
    private readonly HashSet<ulong> _snapshotWatermarks = new();
    private readonly Dictionary<Guid, ProviderApplyReceipt> _applyReceipts = new();
    private ulong _ackedThrough;
    private ulong _highestContiguousRead;
    private ulong _changeHighWatermark;
    private bool _disposed;

    public bool SupportsRestartDurability => false;
    public ulong LocalChangeHighWatermark { get { ThrowIfDisposed(); return _changeHighWatermark; } }

    public RecordId ResolveRecordId(ReadOnlyMemory<byte> sourceItemKey)
    {
        ThrowIfDisposed();
        if (sourceItemKey.IsEmpty)
            throw new ArgumentException("Source item key is required.", nameof(sourceItemKey));
        string key = Convert.ToHexString(sourceItemKey.Span);
        if (_recordIds.TryGetValue(key, out var existing))
            return existing;

        byte[] hash = SHA256.HashData(sourceItemKey.Span);
        var candidate = RecordId.From(hash.AsSpan(0, 16));
        if (candidate.IsZero)
            candidate = new RecordId(1, 0);
        if (_recordKeys.TryGetValue(candidate, out var prior) && prior != key)
            throw new InvalidDataException("Source item identity mapping collision.");

        _recordIds.Add(key, candidate);
        _recordKeys.Add(candidate, key);
        return candidate;
    }

    public ushort ResolveFieldId(ReadOnlyMemory<byte> sourceFieldKey)
    {
        ThrowIfDisposed();
        if (sourceFieldKey.IsEmpty)
            throw new ArgumentException("Source field key is required.", nameof(sourceFieldKey));
        string key = Convert.ToHexString(sourceFieldKey.Span);
        if (_fieldIds.TryGetValue(key, out var existing))
            return existing;

        byte[] hash = SHA256.HashData(sourceFieldKey.Span);
        ushort candidate = BinaryPrimitives.ReadUInt16LittleEndian(hash);
        if (candidate == 0)
            candidate = 1;
        if (_fieldKeys.TryGetValue(candidate, out var prior) && prior != key)
            throw new InvalidDataException("Source field identity mapping collision.");

        _fieldIds.Add(key, candidate);
        _fieldKeys.Add(candidate, key);
        return candidate;
    }

    public void AppendLocalChange(ProviderMutation mutation)
    {
        ThrowIfDisposed();
        ValidateMutation(mutation);
        ulong sequence = checked(_changeHighWatermark + 1);
        var owned = CloneMutation(mutation);
        _current[new FieldKey(owned.RecordId, owned.FieldId)] = owned;
        _changes.Add(new ProviderChange(sequence, CloneMutation(owned)));
        _changeHighWatermark = sequence;
    }

    public ProviderChangePage ReadLocalChanges(ulong afterSequence, int limit)
    {
        ThrowIfDisposed();
        if (afterSequence > _changeHighWatermark)
            throw new ArgumentOutOfRangeException(nameof(afterSequence));
        if (afterSequence > _ackedThrough && afterSequence > _highestContiguousRead)
        {
            if (!_snapshotWatermarks.Contains(afterSequence))
                throw new InvalidDataException("Provider feed cursor skips an unread source change and is not a captured snapshot watermark.");
            _highestContiguousRead = afterSequence;
        }
        if (limit <= 0)
            throw new ArgumentOutOfRangeException(nameof(limit));

        ulong cursor = Math.Max(afterSequence, _ackedThrough);
        var page = _changes
            .Where(change => change.Sequence > cursor)
            .Take(limit)
            .Select(change => new ProviderChange(change.Sequence, CloneMutation(change.Mutation)))
            .ToArray();
        if (page.Length != 0)
            _highestContiguousRead = Math.Max(_highestContiguousRead, page[^1].Sequence);
        return new ProviderChangePage(page, page.Length == 0 ? cursor : page[^1].Sequence);
    }

    public void AcknowledgeLocalChanges(ulong safeThroughSequence)
    {
        ThrowIfDisposed();
        if (safeThroughSequence < _ackedThrough || safeThroughSequence > _highestContiguousRead)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
        _ackedThrough = safeThroughSequence;
        _changes.RemoveAll(change => change.Sequence <= safeThroughSequence);
        _snapshotWatermarks.RemoveWhere(watermark => watermark <= safeThroughSequence);
    }

    public ProviderSnapshotHandle OpenSnapshot()
    {
        ThrowIfDisposed();
        Guid token = Guid.NewGuid();
        var entries = _current.Values
            .OrderBy(mutation => Convert.ToHexString(mutation.RecordId.ToArray()), StringComparer.Ordinal)
            .ThenBy(mutation => mutation.FieldId)
            .Select(CloneMutation)
            .ToArray();
        _snapshots.Add(token, entries);
        _snapshotWatermarks.Add(_changeHighWatermark);
        return new ProviderSnapshotHandle(token, _changeHighWatermark);
    }

    public ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit)
    {
        ThrowIfDisposed();
        if (!_snapshots.TryGetValue(snapshotToken, out var entries))
            throw new ObjectDisposedException(nameof(snapshotToken));
        if (afterIndex < 0 || afterIndex > entries.Length)
            throw new ArgumentOutOfRangeException(nameof(afterIndex));
        if (limit <= 0)
            throw new ArgumentOutOfRangeException(nameof(limit));

        var page = entries.Skip(afterIndex).Take(limit).Select(CloneMutation).ToArray();
        int next = checked(afterIndex + page.Length);
        return new ProviderSnapshotPage(page, next, next == entries.Length);
    }

    public void ReleaseSnapshot(Guid snapshotToken)
    {
        ThrowIfDisposed();
        _snapshots.Remove(snapshotToken);
    }

    public ProviderApplyReceipt ApplyRemoteBatch(Guid applyToken, IReadOnlyList<ProviderMutation> mutations)
    {
        ThrowIfDisposed();
        if (applyToken == Guid.Empty)
            throw new ArgumentException("Apply token must be non-empty.", nameof(applyToken));
        ArgumentNullException.ThrowIfNull(mutations);
        if (mutations.Count == 0)
            throw new ArgumentException("Remote batch must contain at least one mutation.", nameof(mutations));

        var seen = new HashSet<FieldKey>();
        foreach (var mutation in mutations)
        {
            ValidateMutation(mutation);
            if (!seen.Add(new FieldKey(mutation.RecordId, mutation.FieldId)))
                throw new InvalidDataException("Remote batch contains duplicate canonical field keys.");
        }

        byte[] fingerprint = Fingerprint(mutations);
        if (_applyReceipts.TryGetValue(applyToken, out var priorReceipt))
        {
            if (!priorReceipt.BatchFingerprint.AsSpan().SequenceEqual(fingerprint))
                throw new InvalidDataException("Apply token was reused with different canonical input.");
            return new ProviderApplyReceipt(applyToken, priorReceipt.BatchFingerprint.ToArray(), IsDuplicate: true);
        }

        var next = _current.ToDictionary(pair => pair.Key, pair => CloneMutation(pair.Value));
        foreach (var mutation in mutations)
        {
            var owned = CloneMutation(mutation);
            next[new FieldKey(owned.RecordId, owned.FieldId)] = owned;
        }

        var receipt = new ProviderApplyReceipt(applyToken, fingerprint, IsDuplicate: false);
        _current = next;
        _applyReceipts.Add(applyToken, new ProviderApplyReceipt(applyToken, fingerprint.ToArray(), IsDuplicate: false));
        // Remote effects intentionally do not append to the local source change feed.
        return receipt;
    }

    public bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation)
    {
        ThrowIfDisposed();
        if (_current.TryGetValue(new FieldKey(recordId, fieldId), out var stored))
        {
            mutation = CloneMutation(stored);
            return true;
        }
        mutation = null;
        return false;
    }

    public void Dispose()
    {
        _disposed = true;
        _recordIds.Clear();
        _recordKeys.Clear();
        _fieldIds.Clear();
        _fieldKeys.Clear();
        _current.Clear();
        _changes.Clear();
        _snapshots.Clear();
        _snapshotWatermarks.Clear();
        _applyReceipts.Clear();
    }

    private static void ValidateMutation(ProviderMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        if (mutation.RecordId.IsZero || mutation.AuthorNodeId.IsZero)
            throw new ArgumentException("RecordId and original AuthorNodeId must be non-zero.", nameof(mutation));
        ArgumentNullException.ThrowIfNull(mutation.Value);
        if (mutation.IsTombstone && mutation.Value.Length != 0)
            throw new ArgumentException("Tombstone mutation must not carry a value payload.", nameof(mutation));
    }

    private static ProviderMutation CloneMutation(ProviderMutation mutation)
        => new()
        {
            RecordId = mutation.RecordId,
            FieldId = mutation.FieldId,
            IsTombstone = mutation.IsTombstone,
            HlcWall = mutation.HlcWall,
            HlcLogical = mutation.HlcLogical,
            AuthorNodeId = mutation.AuthorNodeId,
            KeyEpoch = mutation.KeyEpoch,
            Value = mutation.Value.ToArray()
        };

    private static byte[] Fingerprint(IReadOnlyList<ProviderMutation> mutations)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(mutations.Count);
            foreach (var mutation in mutations)
            {
                writer.Write(mutation.RecordId.ToArray());
                writer.Write(mutation.FieldId);
                writer.Write(mutation.IsTombstone);
                writer.Write(mutation.HlcWall);
                writer.Write(mutation.HlcLogical);
                writer.Write(mutation.AuthorNodeId.ToArray());
                writer.Write(mutation.KeyEpoch);
                writer.Write(mutation.Value.Length);
                writer.Write(mutation.Value);
            }
        }
        return SHA256.HashData(stream.ToArray());
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(VolatileDataProviderAdapter));
    }

    private readonly record struct FieldKey(RecordId RecordId, ushort FieldId);
}
