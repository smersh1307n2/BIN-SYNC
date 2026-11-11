using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using BinSync;
using BinSync.FldHost;
using Fld1.Storage.Ipc;
using Xunit;

namespace BinSync.FldHost.Tests;

/// <summary>
/// Exercises two independent FLD Database Host processes across a simultaneous
/// restart. Each side retries the exact immutable remote APPLY after reconnect.
/// </summary>
public sealed class FldTwoHostRestartTests
{
    [Fact]
    public async Task Two_database_hosts_restart_and_replay_the_same_remote_apply_receipts()
    {
        string rootA = Path.Combine(Path.GetTempPath(), "fld-two-host-restart-a-" + Guid.NewGuid().ToString("N"));
        string rootB = Path.Combine(Path.GetTempPath(), "fld-two-host-restart-b-" + Guid.NewGuid().ToString("N"));
        const ushort namespaceId = 0x538;
        byte[] authorA = Id(0x81);
        byte[] authorB = Id(0x82);
        byte[] recordAtA = Id(0x83);
        byte[] recordAtB = Id(0x84);
        byte[] valueAtA = "committed-on-host-a"u8.ToArray();
        byte[] valueAtB = "committed-on-host-b"u8.ToArray();
        Process? hostA = null;
        Process? hostB = null;
        FldDatabaseIpcAsyncDataProvider? readerA = null;
        FldDatabaseIpcAsyncDataProvider? readerB = null;
        FldDatabaseIpcAsyncRemoteBatchApplyStore? applyA = null;
        FldDatabaseIpcAsyncRemoteBatchApplyStore? applyB = null;

        var batchAtA = CreateBatch(
            namespaceId, Id(0x85), Id(0x86), Id(0x87), recordAtA, Id(0x88), valueAtA, 11);
        var batchAtB = CreateBatch(
            namespaceId, Id(0x89), Id(0x8A), Id(0x8B), recordAtB, Id(0x8C), valueAtB, 12);

        try
        {
            hostA = StartHost(rootA, namespaceId, authorA);
            hostB = StartHost(rootB, namespaceId, authorB);
            Guid ownerA = (await WaitForEndpoint(rootA, hostA, TimeSpan.FromSeconds(45))).OwnerId;
            Guid ownerB = (await WaitForEndpoint(rootB, hostB, TimeSpan.FromSeconds(45))).OwnerId;

            readerA = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                rootA, Guid.NewGuid(), TimeSpan.FromSeconds(15), NodeId.From(authorA), () => 1);
            readerB = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                rootB, Guid.NewGuid(), TimeSpan.FromSeconds(15), NodeId.From(authorB), () => 1);
            applyA = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                rootA, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            applyB = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                rootB, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));

            ProviderApplyReceipt firstA = await applyA.ApplyRemoteBatchAsync(batchAtA);
            ProviderApplyReceipt firstB = await applyB.ApplyRemoteBatchAsync(batchAtB);
            Assert.False(firstA.IsDuplicate);
            Assert.False(firstB.IsDuplicate);

            await applyA.DisposeAsync(); applyA = null;
            await applyB.DisposeAsync(); applyB = null;
            await readerA.DisposeAsync(); readerA = null;
            await readerB.DisposeAsync(); readerB = null;
            await StopHost(hostA); hostA = null;
            await StopHost(hostB); hostB = null;
            await WaitForWriterRelease(rootA, namespaceId, authorA, TimeSpan.FromSeconds(15));
            await WaitForWriterRelease(rootB, namespaceId, authorB, TimeSpan.FromSeconds(15));

            hostA = StartHost(rootA, namespaceId, authorA);
            hostB = StartHost(rootB, namespaceId, authorB);
            Assert.NotEqual(ownerA, (await WaitForEndpoint(rootA, hostA, TimeSpan.FromSeconds(45), ownerA)).OwnerId);
            Assert.NotEqual(ownerB, (await WaitForEndpoint(rootB, hostB, TimeSpan.FromSeconds(45), ownerB)).OwnerId);

            readerA = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                rootA, Guid.NewGuid(), TimeSpan.FromSeconds(15), NodeId.From(authorA), () => 1);
            readerB = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                rootB, Guid.NewGuid(), TimeSpan.FromSeconds(15), NodeId.From(authorB), () => 1);
            applyA = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                rootA, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));
            applyB = await FldDatabaseIpcAsyncRemoteBatchApplyStore.ConnectAsync(
                rootB, Guid.NewGuid(), namespaceId, TimeSpan.FromSeconds(15));

            ProviderApplyReceipt replayA = await applyA.ApplyRemoteBatchAsync(batchAtA);
            ProviderApplyReceipt replayB = await applyB.ApplyRemoteBatchAsync(batchAtB);
            Assert.True(replayA.IsDuplicate);
            Assert.True(replayB.IsDuplicate);
            Assert.Equal(firstA.ApplyToken, replayA.ApplyToken);
            Assert.Equal(firstB.ApplyToken, replayB.ApplyToken);
            Assert.Equal(firstA.BatchFingerprint, replayA.BatchFingerprint);
            Assert.Equal(firstB.BatchFingerprint, replayB.BatchFingerprint);

            ProviderMutation? currentA = await readerA.ReadCurrentAsync(RecordId.From(recordAtA), 0x0851);
            ProviderMutation? currentB = await readerB.ReadCurrentAsync(RecordId.From(recordAtB), 0x0852);
            Assert.NotNull(currentA);
            Assert.NotNull(currentB);
            Assert.Equal(valueAtA, currentA!.Value);
            Assert.Equal(valueAtB, currentB!.Value);
            Assert.Equal(Id(0x88), currentA.ChangeId.ToArray());
            Assert.Equal(Id(0x8C), currentB.ChangeId.ToArray());
            Assert.Equal(11U, currentA.OriginAuthorEpoch);
            Assert.Equal(12U, currentB.OriginAuthorEpoch);
            Assert.Equal(1UL, currentA.OriginSequence);
            Assert.Equal(1UL, currentB.OriginSequence);
            Assert.Equal(0UL, await readerA.GetLocalChangeHighWatermarkAsync());
            Assert.Equal(0UL, await readerB.GetLocalChangeHighWatermarkAsync());
        }
        finally
        {
            if (applyA is not null) await applyA.DisposeAsync();
            if (applyB is not null) await applyB.DisposeAsync();
            if (readerA is not null) await readerA.DisposeAsync();
            if (readerB is not null) await readerB.DisposeAsync();
            if (hostA is not null) await StopHost(hostA);
            if (hostB is not null) await StopHost(hostB);
            try { Directory.Delete(rootA, recursive: true); } catch { }
            try { Directory.Delete(rootB, recursive: true); } catch { }
        }
    }

    private static ProviderRemoteBatchCommit CreateBatch(
        ushort namespaceId, byte[] sender, byte[] issuer, byte[] batchId,
        byte[] record, byte[] changeId, byte[] value, uint epoch)
    {
        var mutation = new ProviderMutation
        {
            RecordId = RecordId.From(record),
            FieldId = epoch == 11 ? (ushort)0x0851 : (ushort)0x0852,
            AuthorNodeId = NodeId.From(issuer),
            KeyEpoch = 0,
            Value = value,
            ChangeId = BatchId.From(changeId),
            OriginAuthorEpoch = epoch,
            OriginSequence = 1,
            HlcWall = 88_000 + epoch,
            HlcLogical = 1
        };
        return new ProviderRemoteBatchCommit(
            Guid.NewGuid(), NodeId.From(sender), senderTransportEpoch: 1,
            namespaceId: namespaceId, NodeId.From(issuer), issuerEpoch: epoch,
            BatchId.From(batchId), cursorStart: 1, cursorEnd: 1,
            new byte[16], Id((byte)(0x90 + epoch - 11)), Id((byte)(0xA0 + epoch - 11)),
            protocolStateDelta: Array.Empty<byte>(), mutations: [mutation]);
    }

    private static Process StartHost(string root, ushort namespaceId, byte[] author)
    {
        AssemblyMetadataAttribute metadata = typeof(FldTwoHostRestartTests).Assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>()
            .Single(x => x.Key == "Fld1RepositoryRoot");
        string repositoryRoot = metadata.Value
            ?? throw new InvalidOperationException("FLD repository root metadata is empty.");
        string project = Path.Combine(repositoryRoot, "src", "Fld1.Database.Host", "Fld1.Database.Host.csproj");
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true
        };
        foreach (string arg in new[]
        {
            "run", "--project", project, "--configuration",
#if DEBUG
            "Debug",
#else
            "Release",
#endif
            "--no-build", "--no-launch-profile", "--", root,
            namespaceId.ToString(CultureInfo.InvariantCulture), Convert.ToHexString(author)
        }) start.ArgumentList.Add(arg);
        return Process.Start(start) ?? throw new InvalidOperationException("Could not start FLD Database Host.");
    }

    private static async Task<StorageIpcEndpointInfo> WaitForEndpoint(
        string root, Process host, TimeSpan timeout, Guid? previousOwner = null)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (host.HasExited)
                throw new InvalidOperationException($"FLD Database Host exited with {host.ExitCode}: " +
                    await host.StandardError.ReadToEndAsync());
            if (File.Exists(StorageIpcEndpoint.GetEndpointFilePath(root, 0)))
            {
                try
                {
                    StorageIpcEndpointInfo endpoint = StorageIpcEndpoint.Read(root, 0);
                    if (previousOwner is null || endpoint.OwnerId != previousOwner.Value) return endpoint;
                }
                catch (IOException) { }
                catch (InvalidDataException) { }
            }
            await Task.Delay(100);
        }
        throw new TimeoutException("FLD Database Host did not publish a fresh endpoint.");
    }

    private static async Task WaitForWriterRelease(
        string root, uint namespaceId, byte[] author, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try { using var probe = Fld1.Engine.Db.FldDatabase.Open(root, namespaceId, author); return; }
            catch (IOException) { await Task.Delay(50); }
        }
        throw new TimeoutException("FLD Database Host writer fence was not released.");
    }

    private static async Task StopHost(Process host)
    {
        try
        {
            if (!host.HasExited) host.Kill(entireProcessTree: true);
            await host.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
        }
        catch (InvalidOperationException) { }
        catch (System.ComponentModel.Win32Exception) { }
        finally { host.Dispose(); }
    }

    private static byte[] Id(byte value) => Enumerable.Repeat(value, 16).ToArray();
}
