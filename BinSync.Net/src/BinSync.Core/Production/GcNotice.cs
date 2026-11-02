// GC_NOTICE (§12.6 0x0B): advertise tombstone cutoff + quarantine set.
namespace BinSync;

/// <summary>ReconcileFrames type used by the BIN-SYNC engine.</summary>
public static partial class ReconcileFrames
{
    /// <summary>Constant: gc notice.</summary>
    public const byte GcNotice = 0x0B;
}

/// <summary>One quarantined author advertised in GC_NOTICE.</summary>
public sealed class GcQuarantineEntry
{
    /// <summary>Stable 16-byte node identity; set only in the constructor.</summary>
    public byte[] NodeId { get; set; } = Array.Empty<byte>();
    /// <summary>Session/epoch counter; advanced on load and explicit epoch bumps.</summary>
    public int Epoch { get; set; }
}

/// <summary>Decoded GC_NOTICE payload.</summary>
public sealed class GcNoticeMessage
{
    /// <summary>NamespaceId: namespace id.</summary>
    public int NamespaceId;
    /// <summary>Fields/records with wall strictly below this may be GC'd locally (advisory).</summary>
    public ulong TombstoneCutoffWall;
    /// <summary>Peers currently quarantined from sync.</summary>
    public List<GcQuarantineEntry> Quarantined = new();
}

/// <summary>GcNotice type used by the BIN-SYNC engine.</summary>
public static class GcNotice
{
    /// <summary>Build: build.</summary>
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
                NodeId = Codec.CanonicalB128(nid.ToArray()),
                Epoch = ep,
            });
        }
        msg.Quarantined.Sort((a, b) =>
            string.CompareOrdinal(Convert.ToHexString(a.NodeId), Convert.ToHexString(b.NodeId)));
        return msg;
    }

    /// <summary>Encode: encode.</summary>
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

    /// <summary>Decode: decode.</summary>
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

    /// <summary>Apply: apply.</summary>
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

    /// <summary>PurgeBelowCutoff: purge below cutoff.</summary>
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
