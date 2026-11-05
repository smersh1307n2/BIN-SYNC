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
    public void ApplyAliasMap_is_atomic_when_a_late_entry_conflicts()
    {
        using var local = new Node(Fix.Nid(101));
        var peer = Fix.Nid(102);
        var freshAuthor = Fix.Nid(103);
        var conflictingAuthor = Fix.Nid(104);

        local.RegisterAlias(peer, 9, conflictingAuthor, 4);

        var entries = new List<AliasMapEntry>
        {
            new(1, freshAuthor, 7),
            new(9, Fix.Nid(105), 2),
        };

        Assert.Throws<SyncException>(() => SessionWire.ApplyAliasMap(local, peer, entries));

        Assert.False(local.AliasToNode[peer].ContainsKey(1));
        Assert.False(local.NodeToAlias[peer].ContainsKey((freshAuthor, 7)));
        Assert.False(local.DurableAuthors.ContainsKey(freshAuthor));
        Assert.Equal(conflictingAuthor, local.ResolveAlias(peer, 9));
        Assert.Equal(4, local.DurableAuthors[conflictingAuthor]);
    }

    [Fact]
    public void CommitSession_rejects_invalid_map_without_mutating_previous_session()
    {
        using var local = new Node(Fix.Nid(111));
        var peer = Fix.Nid(112);
        var oldAuthor = Fix.Nid(113);
        var newAuthor = Fix.Nid(114);

        local.RegisterAlias(peer, 7, oldAuthor, 3);
        local.RegisterSessionEpoch(peer, 5);

        var invalid = new List<AliasMapEntry>
        {
            new(1, local.NodeId, local.Epoch),
            new(2, peer, 6),
            new(2, newAuthor, 1),
        };

        Assert.Throws<FrameException>(() => local.CommitSession(peer, 6, invalid));

        Assert.Equal(5, local.SessionEpoch(peer, 0));
        Assert.Equal(oldAuthor, local.ResolveAlias(peer, 7));
        Assert.False(local.AliasToNode[peer].ContainsKey(1));
        Assert.False(local.DurableAuthors.ContainsKey(newAuthor));
    }

    [Fact]
    public void CommitSession_replaces_aliases_and_preserves_state_for_same_epoch()
    {
        using var local = new Node(Fix.Nid(121));
        var peer = Fix.Nid(122);
        var oldAuthor = Fix.Nid(123);
        var newAuthor = Fix.Nid(124);

        local.RegisterAlias(peer, 7, oldAuthor, 3);
        local.RegisterSessionEpoch(peer, 5);

        var entries = new List<AliasMapEntry>
        {
            new(1, local.NodeId, local.Epoch),
            new(2, peer, 5),
            new(3, newAuthor, 1),
        };
        local.CommitSession(peer, 5, entries);

        Assert.Throws<SyncException>(() => local.ResolveAlias(peer, 7));
        Assert.Equal(2, local.AliasOf(peer, peer));
        Assert.Equal(3, local.AliasOf(peer, newAuthor));
        Assert.Equal(5, local.SessionEpoch(peer, 0));
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


    [Fact]
    public void Alias_negotiation_retries_after_transient_peer_gate_contention()
    {
        using var local = new Node(Fix.Nid(91));
        using var peer = new Node(Fix.Nid(92));
        using var peerLocked = new ManualResetEventSlim();
        using var releasePeer = new ManualResetEventSlim();

        var peerThread = new Thread(() =>
        {
            using var gates = peer.EnterAllGates();
            peerLocked.Set();
            releasePeer.Wait();
        }) { IsBackground = true };

        peerThread.Start();
        try
        {
            Assert.True(peerLocked.Wait(TimeSpan.FromSeconds(3)));
            using (local.EnterAllGates())
                Assert.Null(local.EnsureAlias(peer, Fix.Nid(93), 1));
        }
        finally
        {
            releasePeer.Set();
        }

        Assert.True(peerThread.Join(TimeSpan.FromSeconds(3)));
        Assert.NotNull(local.EnsureAlias(peer, Fix.Nid(93), 1));
    }
}
