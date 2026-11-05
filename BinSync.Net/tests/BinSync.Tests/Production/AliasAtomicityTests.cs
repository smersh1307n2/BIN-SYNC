using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class AliasAtomicityTests
{
    [Fact]
    public void Ensure_alias_rolls_back_local_half_when_peer_reverse_map_rejects_binding()
    {
        using var local = new Node(Fix.Nid(71));
        using var peer = new Node(Fix.Nid(72));
        var author = Fix.Nid(73);

        // Simulate a stale/inconsistent peer reverse map: alias negotiation chooses 1,
        // but the peer already claims that durable identity is registered as alias 99.
        peer.NodeToAlias[local.NodeId] =
            new Dictionary<(NodeId Id, int Epoch), int>
            {
                [(author, 7)] = 99
            };

        Assert.Null(local.EnsureAlias(peer, author, 7));

        Assert.False(local.AliasToNode.ContainsKey(peer.NodeId));
        Assert.False(local.NodeToAlias.ContainsKey(peer.NodeId));
        Assert.Null(local.KnownAuthorEpoch(author));

        // The failed peer-side registration may not leave an empty forward map behind.
        Assert.False(peer.AliasToNode.ContainsKey(local.NodeId));
        Assert.Equal(99, peer.NodeToAlias[local.NodeId][(author, 7)]);
    }
}
