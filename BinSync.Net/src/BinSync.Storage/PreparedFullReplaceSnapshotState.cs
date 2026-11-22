namespace BinSync.Storage;

/// <summary>Durable source snapshot token and signed manifest needed to resume a v3.1 sender after restart.</summary>
public sealed record PreparedFullReplaceSnapshotState(
    Guid Generation,
    Guid SnapshotToken,
    ulong SourceFeedWatermark,
    ulong HlcWallFloor,
    uint HlcLogicalFloor,
    byte[] ManifestBytes);
