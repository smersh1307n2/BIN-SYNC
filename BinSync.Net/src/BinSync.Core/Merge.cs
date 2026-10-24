// Merge policies (§8): LWW_FIELD with echo suppression [E2], lifecycle delete-wins,
// MAX/MIN, COUNTER_G / COUNTER_PN / SET_OR / MANUAL.
namespace BinSync;

/// <summary>§8 field merge strategies. Default is <see cref="LwwField"/>.</summary>
public enum MergePolicy : byte
{
    /// <summary>Last-writer-wins by HLC then author (engine default).</summary>
    LwwField = 0,
    /// <summary>Lexicographically greater value wins; HLC+author on ties.</summary>
    MaxBytes = 1,
    /// <summary>Lexicographically lesser value wins; HLC+author on ties.</summary>
    MinBytes = 2,
    /// <summary>Little-endian u64 numeric max (short/empty → 0); HLC+author on ties.</summary>
    MaxU64 = 3,
    /// <summary>Little-endian u64 numeric min; HLC+author on ties.</summary>
    MinU64 = 4,

    // Spec §8.1 wire codes (stable):
    /// <summary>§8.4 grow-only counter — per-author max.</summary>
    CounterG = 0x03,
    /// <summary>§8.5 PN-counter — component-wise max; value = Σpos − Σneg.</summary>
    CounterPn = 0x04,
    /// <summary>§8.6 Observed-Remove set.</summary>
    SetOr = 0x05,
    /// <summary>§8.9 Manual — no auto-overwrite; app resolves via FieldResolve 0x00FF.</summary>
    Manual = 0x09,
}

public static class Merge
{
    /// §8.3 including echo suppression [E2]/[Ш2]: identical (wall, logical, author, value)
    /// → no-op, changed=false. Returns (winner, changed).
    public static (FieldState Winner, bool Changed) MergeField(FieldState? local, FieldState remote)
        => MergeFieldWithPolicy(local, remote, MergePolicy.LwwField);

    /// <summary>Merge with an explicit <see cref="MergePolicy"/>.</summary>
    public static (FieldState Winner, bool Changed) MergeFieldWithPolicy(
        FieldState? local, FieldState remote, MergePolicy policy)
    {
        if (local is null)
            return (remote, true);

        // Echo suppression [E2]: identical metadata + value → no-op
        if (local.Timestamp.Wall == remote.Timestamp.Wall &&
            local.Timestamp.Logical == remote.Timestamp.Logical &&
            BytesEq(local.AuthorNodeId, remote.AuthorNodeId) &&
            BytesEq(local.Value, remote.Value))
            return (local, false);

        return policy switch
        {
            MergePolicy.MaxBytes => PickByValue(local, remote, preferGreater: true, numeric: false),
            MergePolicy.MinBytes => PickByValue(local, remote, preferGreater: false, numeric: false),
            MergePolicy.MaxU64 => PickByValue(local, remote, preferGreater: true, numeric: true),
            MergePolicy.MinU64 => PickByValue(local, remote, preferGreater: false, numeric: true),
            MergePolicy.CounterG => MergeCrdtPayload(local, remote, CounterG.Merge),
            MergePolicy.CounterPn => MergeCrdtPayload(local, remote, CounterPn.Merge),
            MergePolicy.SetOr => MergeCrdtPayload(local, remote, SetOr.Merge),
            MergePolicy.Manual => MergeManual(local, remote),
            _ => PickLww(local, remote),
        };
    }

    /// <summary>
    /// CRDT payload merge: merge value bytes; HLC becomes max(local, remote) for metadata.
    /// </summary>
    private static (FieldState Winner, bool Changed) MergeCrdtPayload(
        FieldState local, FieldState remote, Func<byte[]?, byte[], byte[]> mergeFn)
    {
        var mergedVal = mergeFn(local.Value, remote.Value);
        bool valChanged = !BytesEq(mergedVal, local.Value);
        // Metadata: take the dominating HLC so digests stay monotonic
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

    /// <summary>
    /// §8.9 MANUAL: never auto-overwrite divergent values.
    /// Only echo (already handled) or identical value under newer metadata updates HLC.
    /// Divergent remote is ignored (app must write FieldResolve 0x00FF).
    /// </summary>
    private static (FieldState Winner, bool Changed) MergeManual(FieldState local, FieldState remote)
    {
        if (BytesEq(local.Value, remote.Value))
            return PickLww(local, remote); // same payload — advance metadata if remote newer
        return (local, false);
    }

    private static (FieldState Winner, bool Changed) PickLww(FieldState local, FieldState remote)
    {
        if (HlcOps.CompareHlc(remote.Timestamp, remote.AuthorNodeId,
                              local.Timestamp, local.AuthorNodeId) > 0)
            return (remote, true);
        return (local, false);
    }

    /// <summary>
    /// Value-primary merge: compare values; on equality fall back to LWW (HLC+author).
    /// </summary>
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

    /// <summary>Unsigned LE interpretation; missing/short bytes pad as zero.</summary>
    public static ulong AsU64(byte[] v)
    {
        if (v is null || v.Length == 0) return 0;
        ulong r = 0;
        int n = Math.Min(8, v.Length);
        for (int i = 0; i < n; i++)
            r |= (ulong)v[i] << (8 * i);
        return r;
    }

    public static int CompareBytes(byte[] a, byte[] b)
    {
        a ??= Array.Empty<byte>();
        b ??= Array.Empty<byte>();
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++)
        {
            int d = a[i].CompareTo(b[i]);
            if (d != 0) return d;
        }
        return a.Length.CompareTo(b.Length);
    }

    /// Lifecycle conflict resolution by full order [Ж1–Ж2].
    public static (Hlc Hlc, byte[] Author, byte Value, bool Changed) MergeLifecycle(
        Hlc? localHlc, byte[]? localAuthor, byte localValue,
        Hlc remoteHlc, byte[] remoteAuthor, byte remoteValue)
    {
        if (localHlc is null)
            return (remoteHlc, remoteAuthor, remoteValue, true);
        int cmp = HlcOps.CompareHlc(remoteHlc, remoteAuthor, localHlc, localAuthor);
        if (cmp > 0)
            return (remoteHlc, remoteAuthor, remoteValue, true);
        if (cmp == 0 && localValue != remoteValue)
        {
            byte mx = Math.Max(localValue, remoteValue);
            return (localHlc!, localAuthor!, mx, localValue != mx);
        }
        return (localHlc!, localAuthor!, localValue, false);
    }

    public static bool ApplyFieldWrite(Record record, ushort fieldId, FieldState op,
        MergePolicy policy = MergePolicy.LwwField)
    {
        var local = record.State.TryGetValue(fieldId, out var l) ? l : null;
        var (winner, changed) = MergeFieldWithPolicy(local, op, policy);
        if (changed) record.State[fieldId] = winner;
        return changed;
    }

    public static bool ApplyFieldTomb(Record record, ushort fieldId, FieldState op)
    {
        record.State.TryGetValue(fieldId, out var local);
        record.FieldTombs.TryGetValue(fieldId, out var tombLocal);
        bool changed;
        if (tombLocal is not null)
        {
            var (winner, ch) = MergeField(tombLocal, op);
            if (ch) record.FieldTombs[fieldId] = winner;
            tombLocal = winner;
            changed = ch;
        }
        else
        {
            record.FieldTombs[fieldId] = op;
            tombLocal = op;
            changed = true;
        }
        if (local is not null)
        {
            var (keep, _) = MergeField(local, tombLocal);
            bool keepIsTomb = ReferenceEquals(keep, tombLocal);
            if (keepIsTomb ||
                (HlcOps.CompareHlc(tombLocal.Timestamp, tombLocal.AuthorNodeId,
                                   local.Timestamp, local.AuthorNodeId) >= 0 &&
                 tombLocal.Value.Length == 0))
            {
                record.State.Remove(fieldId);
                changed = true;
            }
        }
        return changed;
    }

    public static bool ApplyLifecycle(Record record, Hlc hlc, byte[] author, byte value)
    {
        var (lc, la, lv, changed) = MergeLifecycle(
            record.LifecycleHlc, record.LifecycleAuthor, record.LifecycleValue, hlc, author, value);
        record.LifecycleHlc = lc; record.LifecycleAuthor = la; record.LifecycleValue = lv;
        return changed;
    }

    public static bool BytesEq(byte[] a, byte[] b) => a.AsSpan().SequenceEqual(b);
}

/// <summary>
/// Optional per-(namespace, fieldId) policy map. Missing keys → <see cref="MergePolicy.LwwField"/>.
/// Thread-safe for concurrent lookups; mutations should be rare (schema time).
/// </summary>
public sealed class MergePolicyRegistry
{
    private readonly Dictionary<(int Ns, ushort FieldId), MergePolicy> _map = new();
    private readonly object _lock = new();

    public void Set(int namespaceId, ushort fieldId, MergePolicy policy)
    {
        lock (_lock) _map[(namespaceId, fieldId)] = policy;
    }

    public MergePolicy Get(int namespaceId, ushort fieldId)
    {
        lock (_lock)
            return _map.TryGetValue((namespaceId, fieldId), out var p) ? p : MergePolicy.LwwField;
    }

    public bool Remove(int namespaceId, ushort fieldId)
    {
        lock (_lock) return _map.Remove((namespaceId, fieldId));
    }
}
