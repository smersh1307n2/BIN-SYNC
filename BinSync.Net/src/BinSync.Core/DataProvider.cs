namespace BinSync;

/// <summary>
/// Production source-data boundary. A provider owns application records and
/// exposes durable local changes plus idempotent remote application.
/// </summary>
public interface IDataProvider : IDisposable
{
    RecordId ResolveRecordId(ReadOnlyMemory<byte> sourceItemKey);
    ushort ResolveFieldId(ReadOnlyMemory<byte> sourceFieldKey);

    void AppendLocalChange(ProviderMutation mutation);
    ProviderChangePage ReadLocalChanges(ulong afterSequence, int limit);
    void AcknowledgeLocalChanges(ulong safeThroughSequence);
    ulong LocalChangeHighWatermark { get; }

    ProviderSnapshotHandle OpenSnapshot();
    ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit);
    void ReleaseSnapshot(Guid snapshotToken);

    /// <summary>
    /// Applies an immutable remote batch idempotently to the provider's current
    /// application state. This operation MUST NOT append to, rewrite, or acknowledge
    /// the provider's local unpublished-change feed. A remote LWW winner may replace
    /// the visible current value while an older local intent remains pending; that
    /// intent must stay independently readable until the publication coordinator
    /// freezes it and explicitly acknowledges its source-feed sequence.
    /// </summary>
    ProviderApplyReceipt ApplyRemoteBatch(Guid applyToken, IReadOnlyList<ProviderMutation> mutations);
    bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation);
}

/// <summary>
/// Optional durable capability for a provider-authoritative outbound cutover.
/// The stored watermark fences later direct Node writes after Metadata Store
/// becomes the sole allocator for new local outbound sequences.
/// </summary>
public interface IProviderPublicationMode
{
    bool TryGetMetadataStoreOwnedPublicationCutover(out ulong nodeOutboxHighWatermark);
    void EnableMetadataStoreOwnedPublication(ulong nodeOutboxHighWatermark);
}

/// <summary>
/// Optional provider capability for an authenticated authoritative FULL_REPLACE.
/// The caller validates the roster certificate and snapshot generation before
/// invoking this API. Staging is invisible until CommitFullReplace atomically
/// switches the provider's active generation.
/// </summary>
public interface IProviderFullReplace
{
    void BeginFullReplace(Guid generation, ulong sourceChangeFeedWatermark,
        int expectedEntryCount, ReadOnlyMemory<byte> expectedDigest);
    void StageFullReplacePage(Guid generation, int startIndex,
        IReadOnlyList<ProviderMutation> entries);
    void CommitFullReplace(Guid generation);
    bool AbortFullReplace(Guid generation, int limit);
}

public sealed class ProviderMutation
{
    public required RecordId RecordId { get; init; }
    public required ushort FieldId { get; init; }
    public bool IsTombstone { get; init; }
    /// <summary>True for a record lifecycle tombstone; FieldId must be the reserved lifecycle id.</summary>
    public bool IsRecordTombstone { get; init; }
    public ulong HlcWall { get; init; }
    public uint HlcLogical { get; init; }
    public required NodeId AuthorNodeId { get; init; }
    public ushort KeyEpoch { get; init; }
    public required byte[] Value { get; init; }
}

public sealed record ProviderChange(ulong Sequence, ProviderMutation Mutation)
{
    /// <summary>
    /// Stable 128-bit identity of this durable local-feed entry. It must remain
    /// unchanged when the same sequence is read again after interruption/reopen.
    /// Zero denotes a legacy provider feed that has not yet persisted source identities.
    /// </summary>
    public BatchId ChangeId { get; init; }
}
public sealed record ProviderChangePage(IReadOnlyList<ProviderChange> Changes, ulong ScannedThroughSequence);
public sealed record ProviderSnapshotHandle(Guid Token, ulong ChangeFeedWatermark);
public sealed record ProviderSnapshotPage(IReadOnlyList<ProviderMutation> Entries, int NextIndex, bool IsComplete);
public sealed record ProviderApplyReceipt(Guid ApplyToken, byte[] BatchFingerprint, bool IsDuplicate);
