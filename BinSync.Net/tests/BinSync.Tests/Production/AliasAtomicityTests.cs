using System.Threading;
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

    [Fact]
    public void Opposite_direction_alias_negotiation_does_not_deadlock()
    {
        using var a = new Node(Fix.Nid(81));
        using var b = new Node(Fix.Nid(82));
        using var rendezvous = new Barrier(2);

        Exception? errorA = null;
        Exception? errorB = null;

        var threadA = new Thread(() =>
        {
            try
            {
                using var gates = a.EnterAllGates();
                if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(3)))
                    throw new TimeoutException("A rendezvous timed out");
                _ = a.EnsureAlias(b, Fix.Nid(83), 1);
            }
            catch (Exception ex) { errorA = ex; }
        }) { IsBackground = true };

        var threadB = new Thread(() =>
        {
            try
            {
                using var gates = b.EnterAllGates();
                if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(3)))
                    throw new TimeoutException("B rendezvous timed out");
                _ = b.EnsureAlias(a, Fix.Nid(84), 1);
            }
            catch (Exception ex) { errorB = ex; }
        }) { IsBackground = true };

        threadA.Start();
        threadB.Start();

        Assert.True(threadA.Join(TimeSpan.FromSeconds(3)), "A→B alias negotiation deadlocked");
        Assert.True(threadB.Join(TimeSpan.FromSeconds(3)), "B→A alias negotiation deadlocked");
        Assert.Null(errorA);
        Assert.Null(errorB);
    }
}
