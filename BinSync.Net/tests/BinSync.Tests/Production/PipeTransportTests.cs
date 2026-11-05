using BinSync;
using Xunit;

namespace BinSync.Tests;

public class PipeTransportTests
{
    [Fact]
    public async Task ReceiveAsync_drains_all_queued_packets_before_reporting_peer_close()
    {
        var (sender, receiver) = PipeTransport.CreatePair();
        await sender.SendAsync(new byte[] { 1 });
        await sender.SendAsync(new byte[] { 2 });
        await sender.DisposeAsync();

        Assert.Equal(new byte[] { 1 },
            await receiver.ReceiveAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1)));
        Assert.Equal(new byte[] { 2 },
            await receiver.ReceiveAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1)));
        Assert.Null(await receiver.ReceiveAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1)));

        await receiver.DisposeAsync();
    }
}
