// §17 scenario 13: a peer whose watermark is behind the local safe cutoff
// must not apply incremental batches — full resync (OUTDATED_WATERMARK 0x000B).
namespace BinSync;

public static class WatermarkGate
{
    /// <summary>
    /// Throws <see cref="SyncException"/> <c>OUTDATED_WATERMARK</c> when
    /// <paramref name="peerWatermarkWall"/> is strictly below
    /// <paramref name="localSafeWatermark"/>. A zero local watermark disables the check.
    /// </summary>
    public static void RequireCurrent(ulong peerWatermarkWall, ulong localSafeWatermark, byte[]? batchId = null)
    {
        if (localSafeWatermark != 0 && peerWatermarkWall < localSafeWatermark)
            throw new SyncException("OUTDATED_WATERMARK", batchId);
    }

    public static bool IsStale(ulong peerWatermarkWall, ulong localSafeWatermark) =>
        localSafeWatermark != 0 && peerWatermarkWall < localSafeWatermark;
}
