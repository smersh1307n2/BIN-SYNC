// Merge policies (§8): LWW_FIELD with echo suppression [E2], lifecycle delete-wins,
// MAX/MIN, COUNTER_G / COUNTER_PN / SET_OR / MANUAL / SEQ_FRACTIONAL / BLOB_REF.
namespace BinSync;

/// <summary>§8 field merge strategies. Default is <see cref="LwwField"/>.</summary>
public enum MergePolicy : byte
{
    /// <summary>Last-writer-wins by HLC then author (engine default).</summary>
    LwwField = 0,

    // Spec §8.1 wire codes:
    /// <summary>§8.4 grow-only counter — per-author max.</summary>
    CounterG = 0x03,
    /// <summary>§8.5 PN-counter — component-wise max; value = Σpos − Σneg.</summary>
    CounterPn = 0x04,
    /// <summary>§8.6 Observed-Remove set.</summary>
    SetOr = 0x05,
    /// <summary>§8.7 Fractional sequence total order.</summary>
    SeqFractional = 0x06,
    /// <summary>§8.8 Immutable blob reference.</summary>
    BlobRef = 0x08,
    /// <summary>§8.9 Manual — no auto-overwrite; app resolves via FieldResolve 0x00FF.</summary>
    Manual = 0x09,

    // Engine extensions (not in §8.1 table; high range to avoid clashes):
    /// <summary>Lexicographically greater value wins; HLC+author on ties.</summary>
    MaxBytes = 0x81,
    /// <summary>Lexicographically lesser value wins; HLC+author on ties.</summary>
    MinBytes = 0x82,
    /// <summary>Little-endian u64 numeric max (short/empty → 0); HLC+author on ties.</summary>
    MaxU64 = 0x83,
    /// <summary>Little-endian u64 numeric min; HLC+author on ties.</summary>
    MinU64 = 0x84,
}

/// <summary>LWW and CRDT field merge helpers.</summary>
public static class Merge
{
    /// <summary>Merge two field states under default LWW rules; returns winner and whether it changed.</summary>
    public static (FieldState Winner, bool Changed) MergeField(FieldState? local, FieldState remote)
        => MergeFieldWithPolicy(local, remote, MergePolicy.LwwField);

    /// <summary>Merge two field states under an explicit merge policy.</summary>
    public static (FieldState Winner, bool Changed) MergeFieldWithPolicy(
        FieldState? local, FieldState remote, MergePolicy policy)
    {
        if (!Enum.IsDefined(policy))
            throw new ArgumentOutOfRangeException(nameof(policy), policy, "Unknown merge policy.");

        if (local is null)
        {
            ValidateInitialCrdtPayload(policy, remote.Value);
            return (remote, true);
        }

        if (local.Timestamp.Wall == remote.Timestamp.Wall &&
            local.Timestamp.Logical == remote.Timestamp.Logical &&
            local.AuthorNodeId == remote.AuthorNodeId &&
            BytesEq(local.Value, remote.Value))
            return (local, false);

        return policy switch
        {
            MergePolicy.LwwField => PickLww(local, remote),
            MergePolicy.MaxBytes => PickByValue(local, remote, preferGreater: true, numeric: false),
            MergePolicy.MinBytes => PickByValue(local, remote, preferGreater: false, numeric: false),
            MergePolicy.MaxU64 => PickByValue(local, remote, preferGreater: true, numeric: true),
            MergePolicy.MinU64 => PickByValue(local, remote, preferGreater: false, numeric: true),
            MergePolicy.CounterG => MergeCrdtPayload(local, remote, CounterG.Merge),
            MergePolicy.CounterPn => MergeCrdtPayload(local, remote, CounterPn.Merge),
            MergePolicy.SetOr => MergeCrdtPayload(local, remote, SetOr.Merge),
            MergePolicy.SeqFractional => MergeCrdtPayload(local, remote, SeqFractional.Merge),
            MergePolicy.BlobRef => MergeCrdtPayload(local, remote, BlobRef.Merge),
            MergePolicy.Manual => MergeManual(local, remote),
            _ => throw new ArgumentOutOfRangeException(nameof(policy), policy, "Unknown merge policy."),
        };
    }

    // A first value has no local operand to invoke the merge codec. Validate it
    // explicitly so malformed canonical CRDT payloads cannot enter state through
    // the local-null fast path.
    private static void ValidateInitialCrdtPayload(MergePolicy policy, byte[] payload)
    {
        switch (policy)
        {
            case MergePolicy.CounterG:
                _ = CounterG.Decode(payload);
                break;
            case MergePolicy.CounterPn:
                _ = CounterPn.Decode(payload);
                break;
            case MergePolicy.SetOr:
                _ = SetOr.Decode(payload);
                break;
            case MergePolicy.SeqFractional:
                _ = SeqFractional.Decode(payload);
                break;
            case MergePolicy.BlobRef:
                _ = BlobRef.Decode(payload);
                break;
        }
    }

    private static (FieldState Winner, bool Changed) MergeCrdtPayload(
        FieldState local, FieldState remote, Func<byte[]?, byte[], byte[]> mergeFn)
    {
        var mergedVal = mergeFn(local.Value, remote.Value);
        bool valChanged = !BytesEq(mergedVal, local.Value);
        FieldState metaWinner;
        bool metaChanged;
        (metaWinner, metaChanged) = PickLww(local, remote);
        if (!valChanged && !metaChanged)
            return (local, false);
        var winner = new FieldState(
            mergedVal,
            metaWinner.Timestamp,
            metaWinner.AuthorNodeId,
            KeyEpoch: metaWinner.KeyEpoch);
        return (winner, true);
    }

    private static (FieldState Winner, bool Changed) MergeManual(FieldState local, FieldState remote)
    {
        if (BytesEq(local.Value, remote.Value))
            return PickLww(local, remote);
        return (local, false);
    }

    private static (FieldState Winner, bool Changed) PickLww(FieldState local, FieldState remote)
    {
        if (HlcOps.CompareHlc(remote.Timestamp, remote.AuthorNodeId,
                              local.Timestamp, local.AuthorNodeId) > 0)
            return (remote, true);
        return (local, false);
    }

    private static (FieldState Winner, bool Changed) PickByValue(
        FieldState local, FieldState remote, bool preferGreater, bool numeric)
    {
        int cmp = numeric
            ? AsU64(local.Value).CompareTo(AsU64(remote.Value))
            : CompareBytes(local.Value, remote.Value);

        if (cmp == 0)
            return PickLww(local, remote);

        bool remoteWins = preferGreater ? cmp < 0 : cmp > 0;
        return remoteWins ? (remote, true) : (local, false);
    }

    /// <summary>Interpret a little-endian byte value as ulong.</summary>
    public static ulong AsU64(byte[]? v) => AsU64((ReadOnlySpan<byte>)(v ?? Array.Empty<byte>()));
    public static ulong AsU64(ReadOnlyMemory<byte> v) => AsU64(v.Span);
    public static ulong AsU64(ReadOnlySpan<byte> v)
    {
        if (v.Length == 0) return 0;
        ulong r = 0;
        int n = Math.Min(8, v.Length);
        for (int i = 0; i < n; i++)
            r |= (ulong)v[i] << (8 * i);
        return r;
    }

    /// <summary>Lexicographic comparison of two byte arrays.</summary>
    public static int CompareBytes(byte[]? a, byte[]? b) =>
        CompareBytes((ReadOnlySpan<byte>)(a ?? Array.Empty<byte>()), b ?? Array.Empty<byte>());
    public static int CompareBytes(ReadOnlyMemory<byte> a, ReadOnlyMemory<byte> b) =>
        a.Span.SequenceCompareTo(b.Span);
    public static int CompareBytes(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++)
        {
            int d = a[i].CompareTo(b[i]);
            if (d != 0) return d;
        }
        return a.Length.CompareTo(b.Length);
    }

    /// <summary>Merge two record lifecycle states; returns the winning lifecycle and whether it changed.</summary>
    public static (Hlc Hlc, NodeId Author, byte Value, bool Changed) MergeLifecycle(
        Hlc? localHlc, NodeId? localAuthor, byte localValue,
        Hlc remoteHlc, NodeId remoteAuthor, byte remoteValue)
    {
        if (localValue is not Model.Alive and not Model.Dead ||
            remoteValue is not Model.Alive and not Model.Dead)
            throw new ArgumentOutOfRangeException(nameof(remoteValue), "Lifecycle value must be 0x00 (alive) or 0x01 (dead).");
        if (localHlc is null)
            return (remoteHlc, remoteAuthor, remoteValue, true);
        int cmp = HlcOps.CompareHlc(remoteHlc, remoteAuthor, localHlc, localAuthor);
        if (cmp > 0)
            return (remoteHlc, remoteAuthor, remoteValue, true);
        if (cmp == 0 && localValue != remoteValue)
        {
            byte mx = Math.Max(localValue, remoteValue);
            return (localHlc!, localAuthor ?? default, mx, localValue != mx);
        }
        return (localHlc!, localAuthor ?? default, localValue, false);
    }

    /// <summary>Apply a field-write into a record under LWW/merge rules; returns whether state changed.</summary>
    public static bool ApplyFieldWrite(Record record, ushort fieldId, FieldState op,
        MergePolicy policy = MergePolicy.LwwField)
    {
        // A newer tombstone is a version of this field, not a separate side channel:
        // stale writes must not resurrect it. Keep the lazy tomb map cold on the
        // common no-tomb path.
        bool removedTomb = false;
        if (record.HasFieldTombs &&
            record.FieldTombs.TryGetValue(fieldId, out var tomb))
        {
            if (HlcOps.CompareHlc(op.Timestamp, op.AuthorNodeId,
                                  tomb.Timestamp, tomb.AuthorNodeId) <= 0)
                return false;

            removedTomb = record.FieldTombs.Remove(fieldId);
        }

        var local = record.State.TryGetValue(fieldId, out var l) ? l : null;
        var (winner, changed) = MergeFieldWithPolicy(local, op, policy);
        if (changed) record.State[fieldId] = winner;
        return changed || removedTomb;
    }

    /// <summary>Apply a field tombstone into a record; returns whether state changed.</summary>
    public static bool ApplyFieldTomb(Record record, ushort fieldId, FieldState op)
    {
        FieldState? previousTomb = null;
        if (record.HasFieldTombs &&
            record.FieldTombs.TryGetValue(fieldId, out var existingTomb))
            previousTomb = existingTomb;
        bool hadTomb = previousTomb is not null;
        FieldState winner = op;
        bool tombChanged = !hadTomb;

        if (hadTomb)
        {
            var (merged, changed) = MergeField(previousTomb!, op);
            winner = merged;
            tombChanged = changed;
        }

        if (record.State.TryGetValue(fieldId, out var local))
        {
            int order = HlcOps.CompareHlc(winner.Timestamp, winner.AuthorNodeId,
                                          local.Timestamp, local.AuthorNodeId);
            if (order < 0)
            {
                // The live value is newer. An older tombstone is redundant and
                // must not be retained in the canonical field state.
                return hadTomb && record.FieldTombs.Remove(fieldId);
            }

            bool removedValue = record.State.Remove(fieldId);
            if (hadTomb)
            {
                if (tombChanged) record.FieldTombs[fieldId] = winner;
            }
            else
            {
                record.FieldTombs[fieldId] = winner;
            }
            return removedValue || tombChanged;
        }

        if (hadTomb)
        {
            if (tombChanged) record.FieldTombs[fieldId] = winner;
        }
        else
        {
            record.FieldTombs[fieldId] = winner;
        }
        return tombChanged;
    }

    /// <summary>Apply a record lifecycle change; returns whether state changed.</summary>
    public static bool ApplyLifecycle(Record record, Hlc hlc, NodeId author, byte value)
    {
        var (lc, la, lv, changed) = MergeLifecycle(
            record.LifecycleHlc, record.LifecycleAuthor, record.LifecycleValue, hlc, author, value);
        record.LifecycleHlc = lc; record.LifecycleAuthor = la; record.LifecycleValue = lv;
        return changed;
    }

    /// <summary>Equality comparer for byte arrays by content.</summary>
    public static bool BytesEq(byte[] a, byte[] b) => a.AsSpan().SequenceEqual(b);
    public static bool BytesEq(ReadOnlyMemory<byte> a, ReadOnlyMemory<byte> b) => a.Span.SequenceEqual(b.Span);
    public static bool BytesEq(byte[] a, ReadOnlyMemory<byte> b) => a.AsSpan().SequenceEqual(b.Span);
    public static bool BytesEq(ReadOnlyMemory<byte> a, byte[] b) => a.Span.SequenceEqual(b);
}

/// <summary>Registry of per-(namespace, field) merge policies.</summary>
public sealed class MergePolicyRegistry
{
    private readonly Dictionary<(int Ns, ushort FieldId), MergePolicy> _map = new();
    private readonly object _lock = new();

    /// <summary>Configure the merge policy for a specific namespace field.</summary>
    public void Set(int namespaceId, ushort fieldId, MergePolicy policy)
    {
        if (!Enum.IsDefined(policy))
            throw new ArgumentOutOfRangeException(nameof(policy), policy, "Unknown merge policy.");
        lock (_lock) _map[(namespaceId, fieldId)] = policy;
    }

    /// <summary>Resolve the merge policy for a namespace field (falls back to default).</summary>
    public MergePolicy Get(int namespaceId, ushort fieldId)
    {
        lock (_lock)
            return _map.TryGetValue((namespaceId, fieldId), out var p) ? p : MergePolicy.LwwField;
    }

    /// <summary>Fold a record digest out of this accumulator (delete).</summary>
    public bool Remove(int namespaceId, ushort fieldId)
    {
        lock (_lock) return _map.Remove((namespaceId, fieldId));
    }
}
