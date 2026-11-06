// MEMBERSHIP frame (§12.6 0x05): durable author roster + quarantine hints.
namespace BinSync;

/// <summary>Frame type constants for membership messages.</summary>
public static class MembershipFrames
{
    /// <summary>Membership list messages and peer flag helpers.</summary>
    public const byte Membership = 0x05;
}

/// <summary>One member row in a MEMBERSHIP payload.</summary>
public sealed class MembershipEntry
{
    /// <summary>Stable 16-byte node identity; set only in the constructor.</summary>
    public NodeId NodeId { get; set; }
    /// <summary>Session/epoch counter; advanced on load and explicit epoch bumps.</summary>
    public int Epoch { get; set; } = 1;
    /// <summary>bit0 = active, bit1 = quarantined.</summary>
    public byte Flags { get; set; }
    /// <summary>True when membership flags include the active bit.</summary>
    public bool IsActive => (Flags & 0x01) != 0;
    /// <summary>True when membership flags include the quarantined bit.</summary>
    public bool IsQuarantined => (Flags & 0x02) != 0;
}

/// <summary>Decoded MEMBERSHIP message for one namespace view.</summary>
public sealed class MembershipMessage
{
    /// <summary>Namespace identifier this message or entry applies to.</summary>
    public int NamespaceId { get; set; }
    /// <summary>Membership list epoch for this roster.</summary>
    public uint MembershipEpoch { get; set; }
    /// <summary>Membership entries describing peers and their flags.</summary>
    public List<MembershipEntry> Members { get; set; } = new();
}

/// <summary>Membership list messages and peer flag helpers.</summary>
public static class Membership
{
    /// <summary>Constant flag active.</summary>
    public const byte FlagActive = 0x01;
    /// <summary>Constant flag quarantined.</summary>
    public const byte FlagQuarantined = 0x02;

    /// <summary>
    /// Build MEMBERSHIP from local durable authors + self + optional quarantine set.
    /// </summary>
    public static MembershipMessage Build(Node node, int namespaceId, uint membershipEpoch = 0)
    {
        ArgumentNullException.ThrowIfNull(node);
        using var gates = node.EnterAllGates();
        var msg = new MembershipMessage
        {
            NamespaceId = namespaceId,
            MembershipEpoch = membershipEpoch,
        };
        void Add(NodeId id, int epoch, byte flags)
        {
            msg.Members.Add(new MembershipEntry
            {
                NodeId = id,
                Epoch = epoch,
                Flags = flags,
            });
        }
        Add(node.NodeId, node.Epoch, FlagActive);
        foreach (var (nid, ep) in node.DurableAuthors)
        {
            if (nid == node.NodeId) continue;
            byte flags = FlagActive;
            if (node.Quarantined.Contains(nid)) flags = FlagQuarantined;
            Add(nid, ep, flags);
        }
        msg.Members.Sort((a, b) =>
        {
            Span<byte> aa = stackalloc byte[16];
            Span<byte> bb = stackalloc byte[16];
            a.NodeId.WriteTo(aa);
            b.NodeId.WriteTo(bb);
            return ((ReadOnlySpan<byte>)aa).SequenceCompareTo(bb);
        });
        return msg;
    }

    /// <summary>Encode  into canonical wire bytes.</summary>
    public static byte[] Encode(MembershipMessage m)
    {
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U32(m.MembershipEpoch);
        w.Varint((ulong)m.Members.Count);
        foreach (var e in m.Members)
        {
            w.B128(e.NodeId);
            w.U32(unchecked((uint)e.Epoch));
            w.U8(e.Flags);
        }
        return w.Value();
    }

    /// <summary>Decode  from canonical wire bytes.</summary>
    public static MembershipMessage Decode(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var r = new Reader(payload);
        var m = new MembershipMessage
        {
            NamespaceId = r.U16(),
            MembershipEpoch = r.U32(),
        };
        ulong count = r.Varint();
        if (count > (ulong)(payload.Length / 6))
            throw new InvalidDataException("Membership entry count exceeds the available payload.");
        for (int i = 0; i < (int)count; i++)
        {
            var nodeId = r.ReadNodeId();
            uint rawEpoch = r.U32();
            if (nodeId.IsZero || rawEpoch > int.MaxValue)
                throw new InvalidDataException("Invalid membership identity or epoch.");
            m.Members.Add(new MembershipEntry
            {
                NodeId = nodeId,
                Epoch = (int)rawEpoch,
                Flags = r.U8(),
            });
        }
        if (!r.Eof())
            throw new InvalidDataException("Membership payload contains trailing bytes.");
        return m;
    }

    /// <summary>
    /// Merge remote membership into local durable authors / quarantine.
    /// Higher epoch for a nodeId wins. Returns number of authors newly learned.
    /// </summary>
    public static int Apply(Node node, MembershipMessage remote)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(remote);
        var entries = remote.Members?.Select(entry => entry is null ? null! : new MembershipEntry
        {
            NodeId = entry.NodeId,
            Epoch = entry.Epoch,
            Flags = entry.Flags,
        }).ToArray() ?? throw new FormatException("Membership entries are missing");
        if (entries.Any(entry => entry is null))
            throw new FormatException("Membership contains a null entry");

        var ownedMessage = new MembershipMessage
        {
            NamespaceId = remote.NamespaceId,
            MembershipEpoch = remote.MembershipEpoch,
            Members = entries.ToList(),
        };
        int learned = 0;
        using (node.EnterAllGates())
        {
            foreach (var e in entries)
            {
                if (e.NodeId == node.NodeId) continue;
                if (e.IsQuarantined)
                    node.Quarantined.Add(e.NodeId);
                int prev = node.DurableAuthors.TryGetValue(e.NodeId, out var cur) ? cur : 0;
                if (e.Epoch > prev)
                {
                    node.DurableAuthors[e.NodeId] = e.Epoch;
                    node.LearnAuthorEpoch(e.NodeId, e.Epoch);
                    if (prev == 0) learned++;
                }
            }
        }
        node.RaiseMembershipApplied(ownedMessage, learned);
        return learned;
    }
}
