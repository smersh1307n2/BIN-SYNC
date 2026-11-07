using BinSync;

namespace BinSync.DataProvider.Conformance;

/// <summary>
/// Test-only semantic adapter. This does not freeze the production Data Provider API.
/// </summary>
public interface IDataProviderConformanceAdapter : IDisposable
{
    bool SupportsRestartDurability { get; }

    RecordId ResolveRecordId(ReadOnlyMemory<byte> sourceItemKey);
    ushort ResolveFieldId(ReadOnlyMemory<byte> sourceFieldKey);

    void AppendLocalChange(ProviderMutation mutation);
    ProviderChangePage ReadLocalChanges(ulong afterSequence, int limit);
    void AcknowledgeLocalChanges(ulong safeThroughSequence);
    ulong LocalChangeHighWatermark { get; }

    ProviderSnapshotHandle OpenSnapshot();
    ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit);
    void ReleaseSnapshot(Guid snapshotToken);

    ProviderApplyReceipt ApplyRemoteBatch(Guid applyToken, IReadOnlyList<ProviderMutation> mutations);
    bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation);
}

public sealed class ProviderMutation
{
    public required RecordId RecordId { get; init; }
    public required ushort FieldId { get; init; }
    public bool IsTombstone { get; init; }
    public ulong HlcWall { get; init; }
    public uint HlcLogical { get; init; }
    public required NodeId AuthorNodeId { get; init; }
    public ushort KeyEpoch { get; init; }
    public required byte[] Value { get; init; }
}

public sealed record ProviderChange(ulong Sequence, ProviderMutation Mutation);
public sealed record ProviderChangePage(IReadOnlyList<ProviderChange> Changes, ulong ScannedThroughSequence);
public sealed record ProviderSnapshotHandle(Guid Token, ulong ChangeFeedWatermark);
public sealed record ProviderSnapshotPage(IReadOnlyList<ProviderMutation> Entries, int NextIndex, bool IsComplete);
public sealed record ProviderApplyReceipt(Guid ApplyToken, byte[] BatchFingerprint, bool IsDuplicate);
