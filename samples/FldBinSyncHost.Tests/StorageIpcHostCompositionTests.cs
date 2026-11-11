using System.Buffers.Binary;
using Fld1.Engine.Db;
using Fld1.Storage.Ipc;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class StorageIpcHostCompositionTests
{
    [Fact]
    public async Task Local_ipc_request_commits_through_host_handler_into_real_fld_database()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-fld-ipc-" + Guid.NewGuid().ToString("N"));
        string pipeName = "fld1-ipc-" + Guid.NewGuid().ToString("N");
        const uint namespaceId = 31;
        byte[] localAuthor = Id(0x11);
        byte[] recordId = Id(0x22);
        const ushort fieldId = 0x0317;
        const ushort keyEpoch = 4;
        byte[] value = "ipc-to-fld-durable-write"u8.ToArray();

        try
        {
            using var database = FldDatabase.Open(directory, namespaceId, localAuthor);
            using var serverCancellation = new CancellationTokenSource();
            var server = new StorageIpcServer(
                pipeName,
                (context, request, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (context.ClientId == Guid.Empty || context.SessionGeneration == 0)
                        throw new InvalidDataException("IPC request context is not authenticated.");

                    if (request.Length < 20)
                        throw new InvalidDataException("FLD write request is truncated.");

                    byte[] requestedRecord = request.Slice(0, 16).ToArray();
                    ushort requestedField = BinaryPrimitives.ReadUInt16LittleEndian(request.Span[16..]);
                    ushort requestedKeyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(request.Span[18..]);
                    byte[] requestedValue = request[20..].ToArray();

                    database.Put(requestedRecord, requestedField, requestedValue, requestedKeyEpoch);
                    _ = database.Commit();
                    return ValueTask.FromResult<ReadOnlyMemory<byte>>(requestedRecord);
                },
                maxConcurrentClients: 1);
            Task serverTask = server.RunAsync(serverCancellation.Token);

            try
            {
                await using var client = await StorageIpcClientConnection.ConnectAsync(
                    pipeName, Guid.NewGuid(), TimeSpan.FromSeconds(10));
                Exception overload = await Assert.ThrowsAnyAsync<Exception>(async () =>
                    await StorageIpcClientConnection.ConnectAsync(
                        pipeName, Guid.NewGuid(), TimeSpan.FromSeconds(10)));
                Assert.True(overload is IOException or InvalidDataException,
                    $"Expected a transport-level overload rejection, received {overload.GetType().Name}.");

                byte[] request = new byte[20 + value.Length];
                recordId.CopyTo(request, 0);
                BinaryPrimitives.WriteUInt16LittleEndian(request.AsSpan(16), fieldId);
                BinaryPrimitives.WriteUInt16LittleEndian(request.AsSpan(18), keyEpoch);
                value.CopyTo(request, 20);

                ReadOnlyMemory<byte> response = await client.RequestAsync(request);
                Assert.Equal(recordId, response.ToArray());

                byte[] secondValue = "same-session-after-overload"u8.ToArray();
                byte[] secondRequest = new byte[20 + secondValue.Length];
                recordId.CopyTo(secondRequest, 0);
                BinaryPrimitives.WriteUInt16LittleEndian(secondRequest.AsSpan(16), checked((ushort)(fieldId + 1)));
                BinaryPrimitives.WriteUInt16LittleEndian(secondRequest.AsSpan(18), keyEpoch);
                secondValue.CopyTo(secondRequest, 20);

                ReadOnlyMemory<byte> secondResponse = await client.RequestAsync(secondRequest);
                Assert.Equal(recordId, secondResponse.ToArray());
            }
            finally
            {
                serverCancellation.Cancel();
                await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
            }

            Assert.True(database.TryGetBytes(recordId, fieldId, out byte[]? stored));
            Assert.Equal(value, stored);
            Assert.True(database.TryGetBytes(recordId, checked((ushort)(fieldId + 1)), out byte[]? storedAfterOverload));
            Assert.Equal("same-session-after-overload"u8.ToArray(), storedAfterOverload);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }
}
