// Bilateral IBLT: both peers end with each other's exclusive records.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class BilateralIbltTests
{
    [Fact]
    public async Task Bilateral_Iblt_both_sides_gain_missing_records()
    {
        var a = Fix.MakeNode(110);
        var b = Fix.MakeNode(111);
        var ridA = Fix.Rid(210);
        var ridB = Fix.Rid(211);
        a.WriteField(1, ridA, 0x100, new byte[] { 0xAA });
        b.WriteField(1, ridB, 0x100, new byte[] { 0xBB });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var serverTask = server.BilateralReconcileViaIbltAsync(b, 1, initiate: false, cellCount: 32);
        var (onlyL, onlyR, fieldsA) = await client.BilateralReconcileViaIbltAsync(
            a, 1, initiate: true, cellCount: 32);
        var (onlyL2, onlyR2, fieldsB) = await serverTask;

        Assert.True(onlyR >= 1 || onlyL2 >= 1);
        Assert.True(fieldsA >= 1, "A should apply at least one field from B");
        Assert.True(fieldsB >= 1, "B should apply at least one field from A");

        Assert.True(a.Storage.ContainsKey(1) && a.Storage[1].ContainsKey(ridB));
        Assert.Equal(new byte[] { 0xBB }, a.Storage[1][ridB].State[0x100].Value);

        Assert.True(b.Storage.ContainsKey(1) && b.Storage[1].ContainsKey(ridA));
        Assert.Equal(new byte[] { 0xAA }, b.Storage[1][ridA].State[0x100].Value);
    }

    [Fact]
    public async Task Bilateral_Iblt_noop_when_identical()
    {
        var a = Fix.MakeNode(112);
        var b = Fix.MakeNode(113);
        var rid = Fix.Rid(212);
        // Same record id + value path via independent writes — digests may still differ by HLC/author;
        // empty exclusive sets after peel is the success criterion when both empty.
        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var serverTask = server.BilateralReconcileViaIbltAsync(b, 1, initiate: false, cellCount: 16);
        var (onlyL, onlyR, fieldsA) = await client.BilateralReconcileViaIbltAsync(
            a, 1, initiate: true, cellCount: 16);
        await serverTask;

        Assert.Equal(0, onlyL);
        Assert.Equal(0, onlyR);
        Assert.Equal(0, fieldsA);
    }
}
