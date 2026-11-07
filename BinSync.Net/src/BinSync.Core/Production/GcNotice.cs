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
        Node.ValidateNamespaceId(namespaceId);
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
        ArgumentNullException.ThrowIfNull(m);
        Node.ValidateNamespaceId(m.NamespaceId);
        ArgumentNullException.ThrowIfNull(m.Quarantined);
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U64(m.TombstoneCutoffWall);
        w.Varint((ulong)m.Quarantined.Count);
        foreach (var e in m.Quarantined)
        {
            if (e.Epoch < 0 || e.NodeId.IsZero)
                throw new ArgumentException("GC entries require a non-zero node id and non-negative epoch.", nameof(m));
            w.B128(e.NodeId);
            w.U32((uint)e.Epoch);
        }
        return w.Value();
    }

    /// <summary>Decode  from canonical wire bytes.</summary>
    public static GcNoticeMessage Decode(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var r = new Reader(payload);
        var m = new GcNoticeMessage
        {
            NamespaceId = r.U16(),
            TombstoneCutoffWall = r.U64(),
        };
        ulong count = r.Varint();
        if (count > (ulong)(payload.Length / 20))
            throw new InvalidDataException("GC quarantine count exceeds the available payload.");
        for (int i = 0; i < (int)count; i++)
        {
            var nodeId = r.ReadNodeId();
            uint rawEpoch = r.U32();
            if (nodeId.IsZero || rawEpoch > int.MaxValue)
                throw new InvalidDataException("Invalid GC quarantine identity or epoch.");
            m.Quarantined.Add(new GcQuarantineEntry
            {
                NodeId = nodeId,
                Epoch = (int)rawEpoch,
            });
        }
        if (!r.Eof())
            throw new InvalidDataException("GC notice payload contains trailing bytes.");
        return m;
    }

    /// <summary>Apply  to local state.</summary>
    public static int Apply(Node node, GcNoticeMessage remote)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(remote);
        Node.ValidateNamespaceId(remote.NamespaceId);
        var entries = remote.Quarantined?.Select(entry => entry is null ? null! : new GcQuarantineEntry
        {
            NodeId = entry.NodeId,
            Epoch = entry.Epoch,
        }).ToArray() ?? throw new FormatException("GC quarantine entries are missing");
        if (entries.Any(entry => entry is null || entry.NodeId.IsZero || entry.Epoch < 0))
            throw new FormatException("GC notice contains an invalid quarantine entry");

        var ownedNotice = new GcNoticeMessage
        {
            NamespaceId = remote.NamespaceId,
            TombstoneCutoffWall = remote.TombstoneCutoffWall,
            Quarantined = entries.ToList(),
        };
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
        node.RaiseGcNoticeApplied(ownedNotice, added);
        return added;
    }

    /// <summary>
    /// Tombstone reclamation is deliberately disabled until the namespace cutoff is
    /// durably enforced against stale DELTA_BATCH and snapshot peers. The previous
    /// implementation deleted ordinary live fields solely by age, which is unsafe.
    /// </summary>
    public static int PurgeBelowCutoff(Node node, int namespaceId, ulong cutoffWall)
    {
        ArgumentNullException.ThrowIfNull(node);
        Node.ValidateNamespaceId(namespaceId);
        // A time cutoff by itself is not proof that any peer can no longer replay
        // pre-cutoff state. Keep live values and tombstones until the watermark/resync
        // protocol gate and tombstone-specific reclamation rules are implemented.
        return 0;
    }
}
