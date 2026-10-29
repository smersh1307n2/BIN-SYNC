// Transport-agnostic session helpers (§3, §10.4): pairwise sync over in-memory pipes,
// plus sneakernet file export/import (§3.5).
// Port of binsync/transport.py — all SPEC HARDENING / errata notes preserved.
namespace BinSync;

public static class Transport
{
    /// HELLO + ALIAS_MAP exchange for a session between two nodes (§4.3).
    /// Convention: within session (a,b), `a` gets alias aliases[0], `b` gets aliases[1].
    /// Each node registers BOTH directions of the shared session map; a real ALIAS_MAP
    /// message (§4.3) transmits the SENDER's full table — including third parties it
    /// durably knows — so both peers hold an identical map for the session (the actual
    /// §4.3 determinism requirement; per-session allocation per errata E-7, full-table
    /// harvest from the DURABLE author registry per errata E-8/E-9 — see Python port).
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

    public static byte[] EncodeAck(byte[] batchId, long ackedSeq, byte[]? authorStream = null)
    {
        var w = new Writer();
        w.B128(batchId).U64((ulong)ackedSeq);
        if (authorStream is not null)   // Errata E-4: ACK names the acknowledged stream
            w.B128(authorStream);
        return w.Value();
    }

    public static (byte[] BatchId, long Seq, byte[]? Author) DecodeAck(byte[] data)
    {
        var r = new Reader(data);
        byte[] batchId = r.B128();
        long seq = (long)r.U64();
        byte[]? author = null;
        try { author = r.B128(); }   // optional trailer (E-4); absent ⇒ legacy ACK
        catch { }
        return (batchId, seq, author);
    }

    /// Create DELTA_BATCH → wrap into frame → parse → apply on receiver → ACK back.
    /// Returns BatchResult or null if nothing pending. Throws SyncException on rejection.
    /// `dropAck=true` simulates a lost ACK (§10.2.2 / scenario 18): the ISSUER does NOT
    /// learn about the delivery, so a later CreateDeltaBatch replays the same batchId.
    public static BatchResult? DeliverBatch(Node sender, Node receiver, int namespaceId,
        byte[]? filterAuthor = null, int schemaVersion = 1, bool dropAck = false)
    {
        var batch = sender.CreateDeltaBatch(receiver.NodeId, namespaceId,
                                           filterAuthorNodeId: filterAuthor,
                                           schemaVersion: schemaVersion);
        if (batch is null) return null;
        sender.SignBatchFor(batch, receiver.NodeId);   // idempotent re-bind of the SAME cached (batchId, prev)
        var payload = sender.EncodeBatchPayload(batch);
        uint streamId = 0x0001_0000 | (uint)namespaceId;   // §3.7 data streams
        var raw = FrameCodec.BuildFrame(0x09, streamId, 1, payload);
        var (frame, _) = FrameCodec.ParseFrame(raw);
        var decoded = receiver.DecodeBatchPayload(frame.Payload);
        var result = receiver.ApplyBatch(decoded, sender.NodeId);
        // Receiver's own stream-chain checkpoint for (issuer→me) advances on ACCEPT only;
        // the generic LastBatchDigest map is keyed by (issuer, ns, epoch).
        sender.AdvanceStream(receiver.NodeId, namespaceId, batch);
        // ACK frame flows back to the ISSUER: advances the issuer's lastAckedSeq [СМ6].
        // Errata E-4: the ACK names the acknowledged AUTHOR STREAM.
        var ackRaw = FrameCodec.BuildFrame(0x0B, 0x0002_0000 | (uint)namespaceId, 2,
            EncodeAck(decoded.BatchId, decoded.CursorEnd, decoded.DurableAuthor));
        var (ackFrame, _) = FrameCodec.ParseFrame(ackRaw);
        var (bid, ackSeq, ackAuthor) = DecodeAck(ackFrame.Payload);
        if (!bid.AsSpan().SequenceEqual(decoded.BatchId))
            throw new InvalidOperationException("ACK batch id mismatch");
        if (!dropAck)
            // BUGFIX (implementation finding): the ACK must advance the ISSUER's push
            // cursor for the RECEIVER as peer (self-keyed no-op broke every sync_round).
            sender.ProcessAck(receiver.NodeId, namespaceId, ackSeq, authorStream: ackAuthor);
        return result;
    }

    /// Full pairwise delta exchange A→B then B→A. Returns count of delivered batches.
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
    /// Dump DELTA_BATCH frames to a binary file (writer side of exportDelta).
    public static int ExportDelta(Node node, int namespaceId, byte[] targetHint, string path)
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
                node.ProcessAck(targetHint, namespaceId, batch.CursorEnd);
            }
        }
        return count;
    }

    /// Read frames from a file and apply them exactly like network delivery (§3.5):
    /// same ApplyBatch path — identity and idempotency preserved.
    public static int ImportDelta(Node node, string path, byte[] issuerHint)
    {
        var data = File.ReadAllBytes(path);
        int pos = 0, applied = 0;
        while (pos < data.Length)
        {
            var (frame, consumed) = FrameCodec.ParseFrame(data[pos..]);
            pos += consumed;
            var batch = node.DecodeBatchPayload(frame.Payload);
            node.ApplyBatch(batch, issuerHint);
            applied++;
        }
        return applied;
    }
}
