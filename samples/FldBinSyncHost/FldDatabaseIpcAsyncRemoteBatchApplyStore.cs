using BinSync;
using Fld1.Engine.Db;
using Fld1.Engine.Sync;

namespace BinSync.FldHost;

/// <summary>
/// Async IPC implementation of the contextual remote-APPLY capability.
/// FLD remains the durable field-state/WAL receipt authority; BIN-SYNC
/// Metadata Store remains the only relay and peer-delivery journal.
/// </summary>
public sealed class FldDatabaseIpcAsyncRemoteBatchApplyStore : IAsyncProviderRemoteBatchCommitStore, IAsyncDisposable
{
    private readonly FldDatabaseIpcClient _client;
    private readonly ushort _namespaceId;
    private int _disposed;

    private FldDatabaseIpcAsyncRemoteBatchApplyStore(FldDatabaseIpcClient client, ushort namespaceId)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _namespaceId = namespaceId;
    }

    public static async Task<FldDatabaseIpcAsyncRemoteBatchApplyStore> ConnectAsync(
        string rootDirectory,
        Guid clientId,
        ushort namespaceId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        if (clientId == Guid.Empty)
            throw new ArgumentException("A stable non-empty IPC ClientId is required.", nameof(clientId));
        FldDatabaseIpcClient client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
            rootDirectory, clientId, timeout, cancellationToken).ConfigureAwait(false);
        return new FldDatabaseIpcAsyncRemoteBatchApplyStore(client, namespaceId);
    }

    public async ValueTask<ProviderApplyReceipt> ApplyRemoteBatchAsync(
        ProviderRemoteBatchCommit batch,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (batch.NamespaceId != _namespaceId)
            throw new ArgumentException("Remote batch namespace does not match this FLD IPC store.", nameof(batch));

        var writes = new RemoteFieldWrite[batch.Mutations.Count];
        for (int i = 0; i < writes.Length; i++)
            writes[i] = ToRemoteFieldWrite(batch.Mutations[i]);

        var commit = new RemoteBatchCommit(
            batch.SenderPeer.ToArray(),
            batch.SenderTransportEpoch,
            _namespaceId,
            batch.IssuerNodeId.ToArray(),
            batch.IssuerEpoch,
            batch.BatchId.ToArray(),
            batch.CursorStart,
            batch.CursorEnd,
            batch.PreviousBatchDigest,
            batch.BatchDigest,
            batch.RecordsRoot,
            writes,
            Array.Empty<RemoteFieldWrite>(),
            batch.ProtocolStateDelta);

        // ApplyToken is stable across receive recovery. The FLD ApplyJournal also
        // deduplicates by immutable issuer/batch identity, so reconnecting with a
        // fresh IPC ClientId cannot duplicate the committed field-state mutation.
        RemoteBatchReceipt receipt = await _client.ApplyRemoteBatchAsync(
            batch.ApplyToken, commit, cancellationToken).ConfigureAwait(false);
        if (receipt.CommitSeq == 0 ||
            receipt.CursorStart != batch.CursorStart ||
            receipt.CursorEnd != batch.CursorEnd ||
            !receipt.BatchId.AsSpan().SequenceEqual(batch.BatchId.ToArray()) ||
            !receipt.ProtocolStateDelta.AsSpan().SequenceEqual(batch.ProtocolStateDelta) ||
            receipt.AppliedCount < 0 || receipt.AppliedCount > writes.Length)
            throw new InvalidDataException("FLD IPC remote-APPLY receipt does not match the immutable BIN-SYNC batch.");

        return new ProviderApplyReceipt(
            batch.ApplyToken, batch.BatchDigest.ToArray(), receipt.IsDuplicate);
    }

    private static RemoteFieldWrite ToRemoteFieldWrite(ProviderMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        ProviderMutationCodec.ValidatePublished(mutation);
        if (mutation.IsRecordTombstone)
        {
            return new RemoteFieldWrite(
                mutation.RecordId.ToArray(),
                FldLifecycle.FieldId,
                FldLifecycle.DeadValue.ToArray(),
                mutation.HlcWall,
                mutation.HlcLogical,
                mutation.AuthorNodeId.ToArray(),
                mutation.ChangeId.ToArray(),
                keyEpoch: 0,
                isTombstone: false,
                originAuthorEpoch: mutation.OriginAuthorEpoch,
                originSequence: mutation.OriginSequence);
        }

        return new RemoteFieldWrite(
            mutation.RecordId.ToArray(),
            mutation.FieldId,
            mutation.IsTombstone ? Array.Empty<byte>() : mutation.Value.ToArray(),
            mutation.HlcWall,
            mutation.HlcLogical,
            mutation.AuthorNodeId.ToArray(),
            mutation.ChangeId.ToArray(),
            mutation.KeyEpoch,
            mutation.IsTombstone,
            mutation.OriginAuthorEpoch,
            mutation.OriginSequence);
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            return _client.DisposeAsync();
        return ValueTask.CompletedTask;
    }
}
