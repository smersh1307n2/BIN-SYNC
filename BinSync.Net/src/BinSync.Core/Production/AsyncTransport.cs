// Production hardening (Шаг 4): async transport wrappers over the synchronous engine.
//
// Deadlock prevention (§12.18): NEVER hold gates of two nodes at once. Every hop
// copies data out of the source under its gate, releases, then takes the destination
// gate. See Node.EnterAllGates / domain locks.

using System.Threading.Tasks;

namespace BinSync;

public static class AsyncTransport
{
    /// <summary>Push one batch from <paramref name="src"/> to <paramref name="dst"/> under ordered gates.</summary>
    public static Task PushBatchAsync(Node src, Node dst, Batch batch)
    {
        // Snapshot under src gates, then apply under dst — never nested across nodes.
        byte[] payload;
        using (src.EnterAllGates())
            payload = src.EncodeBatchPayloadInternal(batch);

        using (dst.EnterAllGates())
            dst.ApplyBatchInternal(payload, fromPeer: src.NodeId);

        return Task.CompletedTask;
    }

    /// <summary>Drain pending outbox sequences for a peer under domain gates.</summary>
    public static Task FlushPendingAsync(Node node, NodeId peer)
    {
        using (node.EnterAllGates())
        {
            // PendingSeq access requires all gates for consistency with alias/meta.
            var pending = node.PendingSeqInternal(peer);
            // actual drain is transport-specific; this path only serializes state.
            _ = pending;
        }
        return Task.CompletedTask;
    }
}
