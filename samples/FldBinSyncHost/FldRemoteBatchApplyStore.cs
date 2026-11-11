using BinSync;
using Fld1.Engine.Db;
using Fld1.Engine.Sync;

namespace BinSync.FldHost;

/// <summary>
/// Maps BIN-SYNC's complete, immutable provider-apply context into FLD's
/// transport-neutral RemoteBatchCommit. This class is a capability adapter only:
/// it is not IDataProvider and does not create a second relay journal.
/// BIN-SYNC Metadata Store remains the sole owner of receipts and relay payloads;
/// FLD persists field state, its WAL recovery record and an idempotent apply receipt.
/// </summary>
public sealed class FldRemoteBatchApplyStore : IProviderRemoteBatchCommitStore
{
    private readonly FldDatabase _database;

    public FldRemoteBatchApplyStore(FldDatabase database)
        => _database = database ?? throw new ArgumentNullException(nameof(database));

    /// <summary>True only for the exact FLD database instance captured by this adapter.</summary>
    public bool IsBoundTo(FldDatabase database)
        => ReferenceEquals(_database, database);

    public ProviderApplyReceipt ApplyRemoteBatch(ProviderRemoteBatchCommit batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.NamespaceId != checked((int)_database.NamespaceId))
            throw new ArgumentException(
                "Provider remote-batch namespace does not match the FLD database.",
                nameof(batch));

        var writes = new RemoteFieldWrite[batch.Mutations.Count];
        for (int i = 0; i < writes.Length; i++)
            writes[i] = ToRemoteFieldWrite(batch.Mutations[i]);

        // v3.0/v3.1 publication ownership places relay payload and downstream
        // delivery progress in BIN-SYNC Metadata Store. FLD receives no duplicate
        // relay payload copy; its ApplyJournal remains a WAL-recovery/idempotency
        // receipt for the field-state transaction only.
        var commit = new RemoteBatchCommit(
            batch.SenderPeer.ToArray(),
            batch.SenderTransportEpoch,
            checked((ushort)batch.NamespaceId),
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

        RemoteBatchReceipt receipt = _database.ApplyRemoteBatch(commit);
        return new ProviderApplyReceipt(
            batch.ApplyToken,
            batch.BatchDigest.ToArray(),
            receipt.IsDuplicate);
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
}
