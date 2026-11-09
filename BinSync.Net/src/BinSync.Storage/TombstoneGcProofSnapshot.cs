namespace BinSync.Storage;

internal readonly record struct TombstoneGcOriginProof(
    ulong FinalIssuedSequence,
    ulong MaxIssuedHlcWall,
    uint MaxIssuedHlcLogical,
    ulong SafeAppliedSequence,
    bool AllActiveMembersApplied);

/// <summary>
/// Prevalidated signed causal-horizon proof for one FULL_REPLACE generation.
/// Built once per compaction pass so candidate checks are O(1) average time.
/// </summary>
internal sealed class TombstoneGcProofSnapshot
{
    private readonly ulong _fullReplaceHlcWallFloor;
    private readonly uint _fullReplaceHlcLogicalFloor;
    private readonly Dictionary<(NodeId Author, uint Epoch), TombstoneGcOriginProof> _origins;

    public TombstoneGcProofSnapshot(
        ulong fullReplaceHlcWallFloor,
        uint fullReplaceHlcLogicalFloor,
        Dictionary<(NodeId Author, uint Epoch), TombstoneGcOriginProof> origins)
    {
        _fullReplaceHlcWallFloor = fullReplaceHlcWallFloor;
        _fullReplaceHlcLogicalFloor = fullReplaceHlcLogicalFloor;
        _origins = origins;
    }

    public bool HasProof(
        NodeId originAuthor, uint originEpoch, ulong originSequence,
        ulong tombstoneHlcWall, uint tombstoneHlcLogical)
    {
        if (originAuthor.IsZero || originEpoch == 0 || originSequence == 0 ||
            !_origins.TryGetValue((originAuthor, originEpoch), out TombstoneGcOriginProof proof))
            return false;

        return proof.AllActiveMembersApplied &&
            originSequence <= proof.FinalIssuedSequence &&
            proof.SafeAppliedSequence >= proof.FinalIssuedSequence &&
            tombstoneHlcWall <= proof.MaxIssuedHlcWall &&
            (tombstoneHlcWall != proof.MaxIssuedHlcWall ||
             tombstoneHlcLogical <= proof.MaxIssuedHlcLogical) &&
            proof.MaxIssuedHlcWall <= _fullReplaceHlcWallFloor &&
            (proof.MaxIssuedHlcWall != _fullReplaceHlcWallFloor ||
             proof.MaxIssuedHlcLogical <= _fullReplaceHlcLogicalFloor) &&
            tombstoneHlcWall <= _fullReplaceHlcWallFloor &&
            (tombstoneHlcWall != _fullReplaceHlcWallFloor ||
             tombstoneHlcLogical <= _fullReplaceHlcLogicalFloor);
    }
}
