// GC_NOTICE (§12.6 0x0B): advertise tombstone cutoff + quarantine set.
namespace BinSync;

public static partial class ReconcileFrames
{
    public const byte GcNotice = 0x0B;
}

/// <summary>One quarantined author advertised in GC_NOTICE.</summary>
public sealed class GcQuarantineEntry
{
    public byte[] NodeId = Array.Empty<byte>();
    public int Epoch;
}

/// <summary>Decoded GC_NOTICE payload.</summary>
public sealed class GcNoticeMessage
{
    public int NamespaceId;
    /// <summary>Fields/records with wall strictly below this may be GC'd locally (advisory).</summary>
    public ulong TombstoneCutoffWall;
    public List<GcQuarantineEntry> Quarantined = new();
}

public static class GcNotice
{
    public static GcNoticeMessage Build(Node node, int namespaceId, ulong tombstoneCutoffWall = 0)
    {
        var msg = new GcNoticeMessage
        {
            NamespaceId = namespaceId,
            TombstoneCutoffWall = tombstoneCutoffWall,
        };
        foreach (var nid in node.Quarantined)
        {
            int ep = node.DurableAuthors.TryGetValue(nid, out var e) ? e : 0;
            msg.Quarantined.Add(new GcQuarantineEntry
            {
                NodeId = Codec.CanonicalB128(nid),
                Epoch = ep,
            });
        }
        msg.Quarantined.Sort((a, b) =>
            string.CompareOrdinal(Convert.ToHexString(a.NodeId), Convert.ToHexString(b.NodeId)));
        return msg;
    }

    public static byte[] Encode(GcNoticeMessage m)
    {
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U64(m.TombstoneCutoffWall);
        w.Varint((ulong)m.Quarantined.Count);
        foreach (var e in m.Quarantined)
        {
            w.B128(Codec.CanonicalB128(e.NodeId));
            w.U32(unchecked((uint)e.Epoch));
        }
        return w.Value();
    }

    public static GcNoticeMessage Decode(byte[] payload)
    {
        var r = new Reader(payload);
        var m = new GcNoticeMessage
        {
            NamespaceId = r.U16(),
            TombstoneCutoffWall = r.U64(),
        };
        int n = (int)r.Varint();
        for (int i = 0; i < n; i++)
        {
            m.Quarantined.Add(new GcQuarantineEntry
            {
                NodeId = r.B128(),
                Epoch = unchecked((int)r.U32()),
            });
        }
        return m;
    }

    public static int Apply(Node node, GcNoticeMessage remote)
    {
        int added = 0;
        foreach (var e in remote.Quarantined)
        {
            var id = Codec.CanonicalB128(e.NodeId);
            if (id.AsSpan().SequenceEqual(node.NodeId)) continue;
            if (!node.Quarantined.Contains(id))
            {
                node.Quarantined.Add(id);
                added++;
            }
            if (e.Epoch > 0)
            {
                int prev = node.DurableAuthors.TryGetValue(id, out var cur) ? cur : 0;
                if (e.Epoch > prev)
                {
                    node.DurableAuthors[id] = e.Epoch;
                    node.LearnAuthorEpoch(id, e.Epoch);
                }
            }
        }
        return added;
    }

    public static int PurgeBelowCutoff(Node node, int namespaceId, ulong cutoffWall)
    {
        if (cutoffWall == 0) return 0;
        if (!node.Storage.TryGetValue(namespaceId, out var map)) return 0;
        int removed = 0;
        foreach (var rec in map.Values)
        {
            var dead = new List<ushort>();
            foreach (var (fid, fs) in rec.State)
            {
                if (fs.Timestamp.Wall < cutoffWall)
                    dead.Add(fid);
            }
            foreach (var fid in dead)
            {
                rec.State.Remove(fid);
                removed++;
            }
        }
        return removed;
    }
}
