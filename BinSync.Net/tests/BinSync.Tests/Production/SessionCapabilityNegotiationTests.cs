using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class SessionCapabilityNegotiationTests
{
    [Fact]
    public async Task Immutable_origin_provenance_is_negotiated_only_when_both_peers_advertise_it()
    {
        using var a = Fix.MakeNode(701);
        using var b = Fix.MakeNode(702);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var capability = SessionCapabilities.ImmutableOriginProvenance;
        var accept = SyncSession.AcceptAsync(b, bPipe, caps: capability);
        await using var client = await SyncSession.ConnectAsync(a, aPipe, caps: capability);
        await using var server = await accept;

        Assert.True((client.NegotiatedCapabilities & capability) != 0);
        Assert.True((server.NegotiatedCapabilities & capability) != 0);
    }

    [Fact]
    public async Task Immutable_origin_provenance_is_not_negotiated_for_a_legacy_peer()
    {
        using var a = Fix.MakeNode(703);
        using var b = Fix.MakeNode(704);
        var (aPipe, bPipe) = PipeTransport.CreatePair();
        var capability = SessionCapabilities.ImmutableOriginProvenance;
        var accept = SyncSession.AcceptAsync(b, bPipe);
        await using var client = await SyncSession.ConnectAsync(a, aPipe, caps: capability);
        await using var server = await accept;

        Assert.Equal(SessionCapabilities.IncrementalAliasMap, client.NegotiatedCapabilities);
        Assert.Equal(SessionCapabilities.IncrementalAliasMap, server.NegotiatedCapabilities);
    }
}
