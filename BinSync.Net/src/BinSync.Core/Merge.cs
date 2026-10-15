// Merge policies (§8): LWW_FIELD with echo suppression [E2], lifecycle delete-wins.
namespace BinSync;

public static class Merge
{
    /// §8.3 including echo suppression [E2]/[Ш2]: identical (wall, logical, author, value)
    /// → no-op, changed=false. Returns (winner, changed).
    public static (FieldState Winner, bool Changed) MergeField(FieldState? local, FieldState remote)
    {
        if (local is not null)
        {
            if (local.Timestamp.Wall == remote.Timestamp.Wall &&
                local.Timestamp.Logical == remote.Timestamp.Logical &&
                BytesEq(local.AuthorNodeId, remote.AuthorNodeId) &&
                BytesEq(local.Value, remote.Value))
                return (local, false);
            if (HlcOps.CompareHlc(remote.Timestamp, remote.AuthorNodeId,
                                  local.Timestamp, local.AuthorNodeId) > 0)
                return (remote, true);
            return (local, false);
        }
        return (remote, true);
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
            // same time & author but different value — deterministic: dead wins (§7.4)
            byte mx = Math.Max(localValue, remoteValue);
            return (localHlc!, localAuthor!, mx, localValue != mx);
        }
        return (localHlc!, localAuthor!, localValue, false);
    }

    public static bool ApplyFieldWrite(Record record, ushort fieldId, FieldState op)
    {
        var local = record.State.TryGetValue(fieldId, out var l) ? l : null;
        var (winner, changed) = MergeField(local, op);
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
                record.State.Remove(fieldId);   // tomb wins or ties → hidden, kept as tomb
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
