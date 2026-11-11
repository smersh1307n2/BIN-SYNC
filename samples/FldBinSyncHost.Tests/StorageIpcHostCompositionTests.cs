using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.Pipes;
using Fld1.Engine.Db;
using Fld1.Storage.Engine;
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
        var requestIds = new ConcurrentQueue<ulong>();
        var operationIds = new ConcurrentQueue<Guid>();

        try
        {
            using var database = FldDatabase.Open(directory, namespaceId, localAuthor);
            using var serverCancellation = new CancellationTokenSource();
            var server = new StorageIpcServer(
                pipeName,
                (context, request, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (context.ClientId == Guid.Empty || context.SessionGeneration == 0 ||
                        context.RequestId == 0)
                        throw new InvalidDataException("IPC request context is not authenticated.");
                    requestIds.Enqueue(context.RequestId);
                    StorageIpcOperationEnvelope operation = StorageIpcOperationProtocol.Decode(request);
                    if (operation.OperationCode != 1)
                        throw new InvalidDataException("Unknown FLD host operation code.");
                    operationIds.Enqueue(operation.OperationId);
                    ReadOnlyMemory<byte> operationPayload = operation.Payload;

                    if (operationPayload.Length < 20)
                        throw new InvalidDataException("FLD write request is truncated.");

                    byte[] requestedRecord = operationPayload.Slice(0, 16).ToArray();
                    ushort requestedField = BinaryPrimitives.ReadUInt16LittleEndian(operationPayload.Span[16..]);
                    ushort requestedKeyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(operationPayload.Span[18..]);
                    byte[] requestedValue = operationPayload[20..].ToArray();

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

                byte[] firstOperation = StorageIpcOperationProtocol.Encode(
                    new StorageIpcOperationEnvelope(
                        Guid.Parse("31000000-0000-0000-0000-000000000001"), 1, request));
                ReadOnlyMemory<byte> response = await client.RequestAsync(firstOperation);
                Assert.Equal(recordId, response.ToArray());

                byte[] secondValue = "same-session-after-overload"u8.ToArray();
                byte[] secondRequest = new byte[20 + secondValue.Length];
                recordId.CopyTo(secondRequest, 0);
                BinaryPrimitives.WriteUInt16LittleEndian(secondRequest.AsSpan(16), checked((ushort)(fieldId + 1)));
                BinaryPrimitives.WriteUInt16LittleEndian(secondRequest.AsSpan(18), keyEpoch);
                secondValue.CopyTo(secondRequest, 20);
                byte[] secondOperation = StorageIpcOperationProtocol.Encode(
                    new StorageIpcOperationEnvelope(
                        Guid.Parse("31000000-0000-0000-0000-000000000002"), 1, secondRequest));

                ReadOnlyMemory<byte> secondResponse = await client.RequestAsync(secondOperation);
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
            Assert.Equal(new ulong[] { 1, 2 }, requestIds.ToArray());
            Assert.Equal(new[]
            {
                Guid.Parse("31000000-0000-0000-0000-000000000001"),
                Guid.Parse("31000000-0000-0000-0000-000000000002")
            }, operationIds.ToArray());
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Previous_ipc_connection_generation_is_rejected_before_any_fld_write()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-fld-ipc-fence-" + Guid.NewGuid().ToString("N"));
        string pipeName = "fld1-ipc-" + Guid.NewGuid().ToString("N");
        const uint namespaceId = 32;
        byte[] localAuthor = Id(0x31);
        byte[] recordId = Id(0x32);
        const ushort fieldId = 0x0321;
        Guid stableClientId = Guid.NewGuid();

        try
        {
            using var database = FldDatabase.Open(directory, namespaceId, localAuthor);
            using var serverCancellation = new CancellationTokenSource();
            int handlerCalls = 0;
            var server = new StorageIpcServer(
                pipeName,
                (context, request, cancellationToken) =>
                {
                    Interlocked.Increment(ref handlerCalls);
                    database.Put(recordId, fieldId, request.ToArray(), 1);
                    _ = database.Commit();
                    return ValueTask.FromResult<ReadOnlyMemory<byte>>(recordId);
                });
            Task serverTask = server.RunAsync(serverCancellation.Token);
            ulong previousGeneration;

            try
            {
                await using (var previousPipe = new NamedPipeClientStream(
                    ".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous))
                {
                    await previousPipe.ConnectAsync(10_000);
                    await StorageIpcProtocol.WriteAsync(previousPipe,
                        new StorageIpcFrame(
                            StorageIpcFrameType.Hello, 0, 0, stableClientId.ToByteArray()));
                    StorageIpcFrame? previousAck = await StorageIpcProtocol.ReadAsync(previousPipe);
                    Assert.NotNull(previousAck);
                    Assert.Equal(StorageIpcFrameType.HelloAck, previousAck.Value.Type);
                    previousGeneration = previousAck.Value.SessionGeneration;
                }

                await using var currentPipe = new NamedPipeClientStream(
                    ".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
                await currentPipe.ConnectAsync(10_000);
                await StorageIpcProtocol.WriteAsync(currentPipe,
                    new StorageIpcFrame(
                        StorageIpcFrameType.Hello, 0, 0, stableClientId.ToByteArray()));
                StorageIpcFrame? currentAck = await StorageIpcProtocol.ReadAsync(currentPipe);
                Assert.NotNull(currentAck);
                Assert.Equal(StorageIpcFrameType.HelloAck, currentAck.Value.Type);
                Assert.NotEqual(previousGeneration, currentAck.Value.SessionGeneration);

                await StorageIpcProtocol.WriteAsync(currentPipe,
                    new StorageIpcFrame(
                        StorageIpcFrameType.Request, 1, previousGeneration, "must-not-apply"u8.ToArray()));

                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                StorageIpcFrame? response = null;
                Exception? transportFailure = null;
                try
                {
                    response = await StorageIpcProtocol.ReadAsync(currentPipe, timeout.Token);
                }
                catch (Exception exception) when (exception is IOException or ObjectDisposedException)
                {
                    transportFailure = exception;
                }

                Assert.True(response is null || transportFailure is not null,
                    "A previous connection generation must be rejected after reconnect.");
                Assert.Equal(0, Volatile.Read(ref handlerCalls));
                Assert.False(database.TryGetBytes(recordId, fieldId, out _));
            }
            finally
            {
                serverCancellation.Cancel();
                await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Storage_ipc_operation_ledger_replays_durable_result_and_rejects_conflicting_identity()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-fld-ipc-ledger-" + Guid.NewGuid().ToString("N"));
        const ulong partition = 0x3103;
        Guid clientId = Guid.Parse("31030000-0000-0000-0000-000000000001");
        Guid operationId = Guid.Parse("31030000-0000-0000-0000-000000000002");
        byte[] dataKey = "fld-host-storage-key"u8.ToArray();
        byte[] dataValue = "durable-storage-value"u8.ToArray();
        byte[] requestPayload = "put-storage-value"u8.ToArray();
        byte[] fingerprint = StorageIpcOperationLedger.ComputeFingerprint(3, requestPayload);
        byte[] result = "stored"u8.ToArray();

        try
        {
            using (var storage = StorageDatabase.Open(directory, partition))
            {
                var ledger = new StorageIpcOperationLedger(storage);
                using var batch = storage.BeginWriteBatch();
                batch.Put(dataKey, dataValue);
                Assert.True(ledger.TryStageResult(
                    batch, clientId, operationId, fingerprint, result, out byte[]? prior));
                Assert.Null(prior);
                batch.Commit();
            }

            using (var reopened = StorageDatabase.Open(directory, partition))
            {
                var ledger = new StorageIpcOperationLedger(reopened);
                Assert.True(reopened.TryGet(dataKey, out byte[]? stored));
                Assert.Equal(dataValue, stored);
                Assert.True(ledger.TryGetResult(clientId, operationId, fingerprint, out byte[]? replayed));
                Assert.Equal(result, replayed);

                using var duplicate = reopened.BeginWriteBatch();
                Assert.False(ledger.TryStageResult(
                    duplicate, clientId, operationId, fingerprint,
                    "different-local-result"u8, out byte[]? prior));
                Assert.Equal(result, prior);

                byte[] conflictingFingerprint =
                    StorageIpcOperationLedger.ComputeFingerprint(3, "different-request"u8);
                Assert.Throws<InvalidOperationException>(() =>
                    ledger.TryGetResult(clientId, operationId, conflictingFingerprint, out _));
            }
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
