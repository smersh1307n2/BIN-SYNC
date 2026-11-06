// GC_NOTICE (§12.6 0x0B): advertise tombstone cutoff + quarantine set.
namespace BinSync;

/// <summary>Frame type constants for reconcile/GC control messages.</summary>
public static partial class ReconcileFrames
{
    /// <summary>GC notice messages and purge helpers for outdated records.</summary>
    public const byte GcNotice = 0x0C;
}

/// <summary>One quarantined author advertised in GC_NOTICE.</summary>
public sealed class GcQuarantineEntry
{
    /// <summary>Stable 16-byte node identity; set only in the constructor.</summary>
    public NodeId NodeId { get; set; }
    /// <summary>Session/epoch counter; advanced on load and explicit epoch bumps.</summary>
    public int Epoch { get; set; }
}

/// <summary>Decoded GC_NOTICE payload.</summary>
public sealed class GcNoticeMessage
{
    /// <summary>Namespace identifier this message or entry applies to.</summary>
    public int NamespaceId { get; set; }
    /// <summary>Fields/records with wall strictly below this may be GC'd locally (advisory).</summary>
    public ulong TombstoneCutoffWall { get; set; }
    /// <summary>Peers currently quarantined from sync.</summary>
    public List<GcQuarantineEntry> Quarantined { get; set; } = new();
}

/// <summary>GC notice messages and purge helpers for outdated records.</summary>
public static class GcNotice
{
    /// <summary>Build  from current state.</summary>
    public static GcNoticeMessage Build(Node node, int namespaceId, ulong tombstoneCutoffWall = 0)
    {
        ArgumentNullException.ThrowIfNull(node);
        using var gates = node.EnterAllGates();
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
                NodeId = nid,
                Epoch = ep,
            });
        }
        msg.Quarantined.Sort((a, b) =>
            a.NodeId.ToArray().AsSpan().SequenceCompareTo(b.NodeId.ToArray().AsSpan()));
        return msg;
    }

    /// <summary>Encode  into canonical wire bytes.</summary>
    public static byte[] Encode(GcNoticeMessage m)
    {
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U64(m.TombstoneCutoffWall);
        w.Varint((ulong)m.Quarantined.Count);
        foreach (var e in m.Quarantined)
        {
            w.B128(e.NodeId);
            w.U32(unchecked((uint)e.Epoch));
        }
        return w.Value();
    }

    /// <summary>Decode  from canonical wire bytes.</summary>
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
                NodeId = r.ReadNodeId(),
                Epoch = unchecked((int)r.U32()),
            });
        }
        return m;
    }

    /// <summary>Apply  to local state.</summary>
    public static int Apply(Node node, GcNoticeMessage remote)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(remote);
        var entries = remote.Quarantined?.Select(entry => entry is null ? null! : new GcQuarantineEntry
        {
            NodeId = entry.NodeId,
            Epoch = entry.Epoch,
        }).ToArray() ?? throw new FormatException("GC quarantine entries are missing");
        if (entries.Any(entry => entry is null))
            throw new FormatException("GC notice contains a null quarantine entry");

        int added = 0;
        using (node.EnterAllGates())
        {
            foreach (var e in entries)
            {
                var id = e.NodeId;
                if (id == node.NodeId) continue;
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
        }
        return added;
    }

    /// <summary>Purge records whose lifecycle wall is strictly below the cutoff; returns how many were removed.</summary>
    public static int PurgeBelowCutoff(Node node, int namespaceId, ulong cutoffWall)
    {
        ArgumentNullException.ThrowIfNull(node);
        if (cutoffWall == 0) return 0;
        using var gates = node.EnterAllGates();
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
