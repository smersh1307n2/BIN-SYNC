// MEMBERSHIP frame (§12.6 0x05): durable author roster + quarantine hints.
namespace BinSync;

public static class MembershipFrames
{
    public const byte Membership = 0x05;
}

/// <summary>One member row in a MEMBERSHIP payload.</summary>
public sealed class MembershipEntry
{
    public byte[] NodeId = Array.Empty<byte>();
    public int Epoch = 1;
    /// <summary>bit0 = active, bit1 = quarantined.</summary>
    public byte Flags;
    public bool IsActive => (Flags & 0x01) != 0;
    public bool IsQuarantined => (Flags & 0x02) != 0;
}

/// <summary>Decoded MEMBERSHIP message for one namespace view.</summary>
public sealed class MembershipMessage
{
    public int NamespaceId;
    public uint MembershipEpoch;
    public List<MembershipEntry> Members = new();
}

public static class Membership
{
    public const byte FlagActive = 0x01;
    public const byte FlagQuarantined = 0x02;

    /// <summary>
    /// Build MEMBERSHIP from local durable authors + self + optional quarantine set.
    /// </summary>
    public static MembershipMessage Build(Node node, int namespaceId, uint membershipEpoch = 0)
    {
        var msg = new MembershipMessage
        {
            NamespaceId = namespaceId,
            MembershipEpoch = membershipEpoch,
        };
        void Add(byte[] id, int epoch, byte flags)
        {
            msg.Members.Add(new MembershipEntry
            {
                NodeId = Codec.CanonicalB128(id),
                Epoch = epoch,
                Flags = flags,
            });
        }
        Add(node.NodeId, node.Epoch, FlagActive);
        foreach (var (nid, ep) in node.DurableAuthors)
        {
            if (nid.AsSpan().SequenceEqual(node.NodeId)) continue;
            byte flags = FlagActive;
            if (node.Quarantined.Contains(nid)) flags = FlagQuarantined;
            Add(nid, ep, flags);
        }
        msg.Members.Sort((a, b) =>
            string.CompareOrdinal(Convert.ToHexString(a.NodeId), Convert.ToHexString(b.NodeId)));
        return msg;
    }

    public static byte[] Encode(MembershipMessage m)
    {
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U32(m.MembershipEpoch);
        w.Varint((ulong)m.Members.Count);
        foreach (var e in m.Members)
        {
            w.B128(Codec.CanonicalB128(e.NodeId));
            w.U32(unchecked((uint)e.Epoch));
            w.U8(e.Flags);
        }
        return w.Value();
    }

    public static MembershipMessage Decode(byte[] payload)
    {
        var r = new Reader(payload);
        var m = new MembershipMessage
        {
            NamespaceId = r.U16(),
            MembershipEpoch = r.U32(),
        };
        int n = (int)r.Varint();
        for (int i = 0; i < n; i++)
        {
            m.Members.Add(new MembershipEntry
            {
                NodeId = r.B128(),
                Epoch = unchecked((int)r.U32()),
                Flags = r.U8(),
            });
        }
        return m;
    }

    /// <summary>
    /// Merge remote membership into local durable authors / quarantine.
    /// Higher epoch for a nodeId wins. Returns number of authors newly learned.
    /// </summary>
    public static int Apply(Node node, MembershipMessage remote)
    {
        int learned = 0;
        foreach (var e in remote.Members)
        {
            if (e.NodeId.AsSpan().SequenceEqual(node.NodeId)) continue;
            if (e.IsQuarantined)
            {
                node.Quarantined.Add(Codec.CanonicalB128(e.NodeId));
            }
            int prev = node.DurableAuthors.TryGetValue(e.NodeId, out var cur) ? cur : 0;
            if (e.Epoch > prev)
            {
                node.DurableAuthors[Codec.CanonicalB128(e.NodeId)] = e.Epoch;
                node.LearnAuthorEpoch(e.NodeId, e.Epoch);
                if (prev == 0) learned++;
            }
        }
        return learned;
    }
}
