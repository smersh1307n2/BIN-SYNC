using System.Buffers;
// Transport-agnostic session helpers (§3, §10.4): pairwise sync over in-memory pipes,
// plus sneakernet file export/import (§3.5).
// Port of binsync/transport.py — all SPEC HARDENING / errata notes preserved.
namespace BinSync;

/// <summary>Session setup, batch delivery, sync rounds, and sneakernet import/export helpers.</summary>
public static partial class Transport
{
    /// HELLO + ALIAS_MAP exchange for a session between two nodes (§4.3).
    /// Convention: within session (a,b), `a` gets alias aliases[0], `b` gets aliases[1].
    /// Each node registers BOTH directions of the shared session map; a real ALIAS_MAP
    /// message (§4.3) transmits the SENDER's full table — including third parties it
    /// durably knows — so both peers hold an identical map for the session (the actual
    /// §4.3 determinism requirement; per-session allocation per errata E-7, full-table
    /// harvest from the DURABLE author registry per errata E-8/E-9 — see Python port).
    /// <summary>Establish bidirectional session state between two nodes.</summary>
    public static void MakeSession(Node a, Node b, (int, int)? aliases = null)
    {
        var (aa, ab) = aliases ?? (1, 2);
        // SPEC CORRECTION (errata E-2/E-3): HELLO (§4.4) carries each participant's
        // incarnation epoch; the receiver stores it as the per-session regression watermark.
        a.RegisterSessionEpoch(b.NodeId, b.Epoch);
        b.RegisterSessionEpoch(a.NodeId, a.Epoch);
        // Errata E-11 (scenario 23): keep a nodeId -> Node registry so CreateDeltaBatch's
        // lazy ALIAS_MAP negotiation can reach the peer's copy of the session table.
        // In a real stack this is the live connection handle.
        a.PeersByNid ??= new Dictionary<NodeId, Node>();
        b.PeersByNid ??= new Dictionary<NodeId, Node>();
        a.PeersByNid[b.NodeId] = b;
        b.PeersByNid[a.NodeId] = a;
        // BUGFIX (implementation finding): §4.3 ties ALIAS_MAP to a SESSION — reconnecting
        // with a NEW session must REPLACE the old per-peer map, not merge into it
        // (scenario 7: same nodeId, rotated epoch).
        // BUGFIX (persistence resume): when one endpoint is a RESUMED node (loaded from
        // NodePersistence — its PeersByNid values are null placeholders because there is no
        // live handle yet), the two sides still hold their OWN durable copies of the shared
        // session ALIAS_MAP plus the stream-chain checkpoints (PeerStates, LastBatchDigest,
        // LastStreamDigest, LastStreamAuthor — §12.17.2 [Т3], §10.2.2 lost-ACK replay [П7]).
        // A full ResetSession wipe here would (a) destroy wire-stable aliases baked into
        // already-signed outbox batches → UNKNOWN_ALIAS on apply, and (b) erase persisted
        // cursors so the peer REPLAYS batches the resumed node already holds → SEQ_GAP /
        // BATCH_CHAIN_BROKEN instead of an idempotent no-op + ACK. Re-attaching a resumed
        // peer therefore PRESERVES all session state and merely tops up the maps below;
        // a genuine live-session re-handshake keeps the full ResetSession wipe (§4.3,
        // scenario 7: same nodeId, rotated epoch ⇒ REPLACE, never merge).
        bool resumedPeer = (b.PeersByNid is not null && b.PeersByNid.TryGetValue(a.NodeId, out var bh) && bh is null)
                        || (a.PeersByNid is not null && a.PeersByNid.TryGetValue(b.NodeId, out var ah) && ah is null);
        if (!resumedPeer)
        {
            a.ResetSession(b.NodeId);
            b.ResetSession(a.NodeId);
        }
        // Full-table exchange: every author already known durably to either side joins the
        // session map on BOTH ends with the SAME alias numbers (deterministic ordering:
        // ascending nodeId; the two endpoints always land on aliases aa/ab).
        // BUGFIX (errata E-6): keyed by DURABLE NODE ID, never by alias number.
        var authors = new Dictionary<NodeId, (int? Alias, int Epoch)>()
        {
            [a.NodeId] = ((int?)aa, a.Epoch),
            [b.NodeId] = ((int?)ab, b.Epoch),
        };
        // Errata E-8/E-9: harvest source is each endpoint's DURABLE author registry —
        // §4.3's ALIAS_MAP transmits the sender's COMPLETE durable table, including third
        // parties learned through OTHER sessions, and that knowledge must survive
        // re-handshakes (ResetSession wipes the per-peer maps). Over-complete maps are
        // inert under [А1]; resolution still fails closed [А3].
        foreach (var node in new[] { a, b })
            foreach (var (nid2, ep) in node.DurableAuthors)
            {
                if (authors.ContainsKey(nid2)) continue;
                if (nid2 == (NodeId)a.NodeId) authors[nid2] = ((int?)aa, ep);
                else if (nid2 == (NodeId)b.NodeId) authors[nid2] = ((int?)ab, ep);
                else authors[nid2] = ((int?)null, ep);
            }
        var ordered = authors.OrderBy(kv => kv.Key.ToString()).ToList();
        var used = new HashSet<int> { aa, ab };
        int nextFree = 1;
        var assigned = new Dictionary<NodeId, int> { [(NodeId)a.NodeId] = aa, [(NodeId)b.NodeId] = ab };
        foreach (var kv in ordered)
        {
            if (assigned.ContainsKey(kv.Key)) continue;
            while (used.Contains(nextFree)) nextFree++;
            assigned[kv.Key] = nextFree;
            used.Add(nextFree);
        }
        foreach (var kv in ordered)
        {
            int alias = assigned[kv.Key];
            int ep = kv.Value.Epoch;
            a.RegisterAlias(b.NodeId, alias, kv.Key.ToArray(), ep);
            b.RegisterAlias(a.NodeId, alias, kv.Key.ToArray(), ep);
        }
    }

    /// <summary>Encode an ACK payload (batch id, cursor, optional author stream).</summary>
    public static byte[] EncodeAck(BatchId batchId, long ackedSeq, NodeId authorStream = default)
    {
        // Fixed upper bound: 16 + 8 + 16 = 40
        int est = authorStream.IsZero ? 24 : 40;
        var rented = ArrayPool<byte>.Shared.Rent(est);
        try
        {
            var w = new Writer(rented);
            w.B128(batchId).U64((ulong)ackedSeq);
            if (!authorStream.IsZero)
                w.B128(authorStream);
            int n = w.Length;
            var result = new byte[n];
            w.WrittenSpan.CopyTo(result);
            return result;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Decode an ACK payload; the optional author stream is typed and malformed trailers are rejected.</summary>
    public static (BatchId BatchId, long Seq, NodeId? Author) DecodeAck(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        var r = new Reader(data);
        var batchId = BatchId.From(r.B128());
        ulong rawSeq = r.U64();
        if (rawSeq > long.MaxValue)
            throw new InvalidDataException("ACK cursor exceeds Int64.");
        NodeId? author = null;
        if (r.Remaining == 16)
            author = NodeId.From(r.B128());
        else if (r.Remaining != 0)
            throw new InvalidDataException("Invalid ACK author trailer length.");
        return (batchId, (long)rawSeq, author);
    }

    /// Create DELTA_BATCH → wrap into frame → parse → apply on receiver → ACK back.
    /// Returns BatchResult or null if nothing pending. Throws SyncException on rejection.
    /// `dropAck=true` simulates a lost ACK (§10.2.2 / scenario 18): the ISSUER does NOT
    /// learn about the delivery, so a later CreateDeltaBatch replays the same batchId.
    /// <summary>Create, encode, apply one batch and ACK (optional dropAck).</summary>
    public static BatchResult? DeliverBatch(Node sender, Node receiver, int namespaceId,
        NodeId? filterAuthor = null, int schemaVersion = 1, bool dropAck = false)
    {
        var batch = sender.CreateDeltaBatch(receiver.NodeId, namespaceId,
                                           filterAuthorNodeId: filterAuthor,
                                           schemaVersion: schemaVersion);
        if (batch is null) return null;
        sender.SignBatchFor(batch, receiver.NodeId);   // idempotent re-bind of the SAME cached (batchId, prev)
        // In-process path: skip BuildFrame/ParseFrame (CRC is for the wire).
        var payload = sender.EncodeBatchPayload(batch);
        var result = receiver.ApplyBatchPayload(payload, sender.NodeId);
        sender.AdvanceStream(receiver.NodeId, namespaceId, batch);
        // ACK payload only (no frame) — same semantics as wire ACK without CRC tax.
        var ackPayload = EncodeAck(result.BatchId, result.CursorEnd, result.DurableAuthor);
        var (bid, ackSeq, ackAuthor) = DecodeAck(ackPayload);
        if (result.BatchId != bid)
            throw new InvalidOperationException("ACK batch id mismatch");
        if (!dropAck)
            sender.ProcessAck(receiver.NodeId, namespaceId, ackSeq, authorStream: ackAuthor);
        return result;
    }

    /// Full pairwise delta exchange A→B then B→A. Returns count of delivered batches.
    /// <summary>Exchange pending batches A→B then B→A until drained.</summary>
    public static int SyncRound(Node a, Node b, int namespaceId, int schemaVersion = 1)
    {
        int n = 0;
        foreach (var (src, dst) in new[] { (a, b), (b, a) })
            while (true)
            {
                var r = DeliverBatch(src, dst, namespaceId, schemaVersion: schemaVersion);
                if (r is null) break;
                n++;
            }
        return n;
    }

    // ------------------------------------------------------------------ sneakernet (§3.5)
    /// <summary>Write pending DELTA_BATCH frames for a namespace to a binary file (sneakernet).</summary>
    public static int ExportDelta(Node node, int namespaceId, NodeId targetHint, string path)
    {
        int count = 0;
        using (var f = File.Create(path))
        {
            while (true)
            {
                var batch = node.CreateDeltaBatch(targetHint, namespaceId);
                if (batch is null) break;
                var payload = node.EncodeBatchPayload(batch);
                f.Write(FrameCodec.BuildFrame(0x09, 0x0001_0000 | (uint)namespaceId, (ulong)(count + 1), payload));
                count++;
                node.AdvanceStream(targetHint, namespaceId, batch);
                node.ProcessAck(targetHint, namespaceId, batch.CursorEnd, batch.DurableAuthor);
            }
        }
        return count;
    }

    /// <summary>Read frames from a file and apply them as network delivery would.</summary>
    public static int ImportDelta(Node node, string path, NodeId issuerHint)
    {
        var data = File.ReadAllBytes(path);
        int pos = 0, applied = 0;
        while (pos < data.Length)
        {
            var (frame, consumed) = FrameCodec.ParseFrame(data[pos..]);
            pos += consumed;
            node.ApplyBatchPayload(frame.Payload, issuerHint);
            applied++;
        }
        return applied;
    }
}
