namespace BinSync;

/// <summary>
/// Durable outbound transport path. The exact encoded payload is reserved before send
/// and remains replayable until the matching ACK is committed to Metadata Store.
/// </summary>
public partial class SyncSession
{
    /// <summary>
    /// Push one batch using durable per-peer Metadata Store progress. The legacy overload
    /// remains available for in-memory sessions and compatibility.
    /// </summary>
    public async Task<bool> PushBatchAsync(Node sender, int namespaceId,
        IMetadataPeerStateStore peerStateStore, string scopeId,
        int schemaVersion = 1, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(sender);
        ArgumentNullException.ThrowIfNull(peerStateStore);
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (sender.NodeId.IsZero) throw new ArgumentException("Sender identity must be non-zero.", nameof(sender));
        if (namespaceId < 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));

        await _pushGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            uint peerEpoch = checked((uint)PeerEpoch);
            var priorPeerEpochs = peerStateStore.ReadInterestedPeers(scopeId, sender.NodeId, namespaceId)
                .Where(peer => peer.Peer == PeerNodeId && peer.PeerEpoch < peerEpoch)
                .Select(peer => peer.PeerEpoch).Distinct().ToArray();
            bool hasPriorIncarnation = priorPeerEpochs.Length != 0 ||
                peerStateStore.GetRetiredPeerEpoch(scopeId, sender.NodeId, PeerNodeId, namespaceId) is not null;
            peerStateStore.RegisterInterestedPeer(scopeId, sender.NodeId, PeerNodeId, peerEpoch, namespaceId);
            foreach (uint oldEpoch in priorPeerEpochs)
                peerStateStore.RetireInterestedPeer(scopeId, sender.NodeId, PeerNodeId,
                    oldEpoch, peerEpoch, namespaceId);

            var streams = peerStateStore.ReadPeerStreamStates(
                scopeId, sender.NodeId, PeerNodeId, peerEpoch, namespaceId);

            MetadataOutboundBatchReservation reservation;
            NodeId authorStream;
            var pending = streams.FirstOrDefault(stream => stream.State.PendingBatch is not null);
            if (pending is not null)
            {
                authorStream = pending.AuthorStream;
                reservation = pending.State.PendingBatch!;
            }
            else
            {
                var authors = peerStateStore.ReadOutboundAuthors(scopeId, sender.NodeId, namespaceId)
                    .Concat(peerStateStore.ReadRelayAuthors(scopeId, sender.NodeId, namespaceId))
                    .Distinct()
                    .OrderBy(author => Convert.ToHexString(author.ToArray()), StringComparer.Ordinal)
                    .ToArray();
                NodeId? selectedAuthor = null;
                MetadataPeerStreamState selectedState = default!;
                ulong selectedSequence = ulong.MaxValue;
                bool selectedIsContiguous = false;

                foreach (NodeId candidateAuthor in authors)
                {
                    if (candidateAuthor == PeerNodeId) continue;
                    var saved = peerStateStore.GetPeerStreamState(
                        scopeId, sender.NodeId, PeerNodeId, peerEpoch, namespaceId, candidateAuthor);
                    bool pristine = saved.AckedCursor == 0 && saved.PushedCursor == 0 &&
                        saved.StreamOffset == 0 && saved.IssuerEpoch == 0 &&
                        saved.PendingBatch is null && saved.LastBatchDigest.All(value => value == 0);
                    if (pristine && !hasPriorIncarnation)
                    {
                        var legacy = sender.ReadPeerStreamProgress(PeerNodeId, namespaceId, candidateAuthor);
                        if (legacy.PushedCursor > legacy.AckedCursor)
                            throw new InvalidDataException("Legacy Node peer state has an unacknowledged batch without a durable replay reservation; finish that retry before enabling Metadata Store transport.");
                        peerStateStore.RegisterInterestedPeerStream(scopeId, sender.NodeId,
                            PeerNodeId, peerEpoch, namespaceId, candidateAuthor,
                            legacy.AckedCursor, legacy.StreamOffset, legacy.IssuerEpoch, legacy.LastBatchDigest);
                        saved = peerStateStore.GetPeerStreamState(
                            scopeId, sender.NodeId, PeerNodeId, peerEpoch, namespaceId, candidateAuthor);
                    }
                    if (saved.PendingBatch is null && saved.PushedCursor > saved.AckedCursor)
                        throw new InvalidDataException("Durable peer pushed cursor has no immutable replay reservation.");

                    ulong firstSequence;
                    if (candidateAuthor == sender.NodeId)
                    {
                        var first = peerStateStore.ReadOutboundForPeer(scopeId, sender.NodeId,
                            PeerNodeId, peerEpoch, namespaceId, candidateAuthor, 1);
                        if (first.Items.Count == 0) continue;
                        firstSequence = first.Items[0].Sequence;
                    }
                    else
                    {
                        var first = peerStateStore.ReadRelayForPeer(scopeId, sender.NodeId,
                            PeerNodeId, peerEpoch, namespaceId, candidateAuthor, 1);
                        if (first.Items.Count == 0) continue;
                        firstSequence = first.Items[0].Sequence;
                    }

                    ulong expectedSequence = checked(saved.AckedCursor + saved.StreamOffset + 1);
                    bool contiguous = firstSequence == expectedSequence;
                    if (contiguous)
                    {
                        if (!selectedIsContiguous || firstSequence < selectedSequence)
                        {
                            selectedAuthor = candidateAuthor;
                            selectedState = saved;
                            selectedSequence = firstSequence;
                            selectedIsContiguous = true;
                        }
                    }
                    else if (!selectedIsContiguous && firstSequence < selectedSequence)
                    {
                        // A gap is a fallback only. It must not block another author
                        // stream whose next record is contiguous for this peer.
                        selectedAuthor = candidateAuthor;
                        selectedState = saved;
                        selectedSequence = firstSequence;
                    }
                }

                if (selectedAuthor is null) return false;
                authorStream = selectedAuthor.Value;
                IReadOnlyList<(ulong Sequence, BatchId ChangeId, byte[] Payload)> pageItems;
                if (authorStream == sender.NodeId)
                {
                    var page = peerStateStore.ReadOutboundForPeer(scopeId, sender.NodeId,
                        PeerNodeId, peerEpoch, namespaceId, authorStream, 256);
                    pageItems = page.Items.Select(item => (item.Sequence, item.ChangeId, item.Payload)).ToArray();
                }
                else
                {
                    var page = peerStateStore.ReadRelayForPeer(scopeId, sender.NodeId,
                        PeerNodeId, peerEpoch, namespaceId, authorStream, 256);
                    pageItems = page.Items.Select(item => (item.Sequence, item.ChangeId, item.Payload)).ToArray();
                }

                if (pageItems.Count == 0) return false;
                var created = sender.CreateDeltaBatchPayloadFromMetadata(
                    PeerNodeId, namespaceId, authorStream, pageItems, selectedState,
                    schemaVersion: schemaVersion);
                if (created is null) return false;
                var createdBatch = created.Value;
                var candidate = new MetadataOutboundBatchReservation
                {
                    BatchId = createdBatch.BatchId,
                    IssuerEpoch = checked((uint)createdBatch.IssuerEpoch),
                    StreamOffset = createdBatch.StreamOffset,
                    CursorStart = checked((ulong)createdBatch.CursorStart),
                    CursorEnd = checked((ulong)createdBatch.CursorEnd),
                    PreviousBatchDigest = createdBatch.PreviousBatchDigest.ToArray(),
                    BatchDigest = createdBatch.BatchDigest.ToArray(),
                    Payload = createdBatch.Payload
                };
                reservation = peerStateStore.ReserveOutboundBatch(
                    scopeId, sender.NodeId, PeerNodeId, peerEpoch, namespaceId, authorStream, candidate);
            }

            await SendFrameAsync(0x09, 0x0001_0000u | (uint)namespaceId, 1,
                reservation.Payload, ct).ConfigureAwait(false);
            peerStateStore.MarkOutboundBatchPushed(
                scopeId, sender.NodeId, PeerNodeId, peerEpoch, namespaceId, authorStream, reservation.BatchId);

            var raw = await ReceivePacketAsync(0x0B, ct).ConfigureAwait(false);
            var frame = ParseExpectedSessionFrame(raw, 0x0B);
            ValidateNamespaceStream(frame, namespaceId);
            var (id, seq, author) = Transport.DecodeAck(frame.Payload);
            if (!reservation.BatchId.EqualsBytes(id))
                throw new InvalidOperationException("ACK batch id mismatch");
            if (seq != checked((long)reservation.CursorEnd) || author is null ||
                NodeId.From(author) != authorStream)
                throw new SyncException("ACK_OUT_OF_RANGE", reservation.BatchId);

            // Commit the durable cursor/digest first. If the process stops before the
            // Node mirror is updated, the next call restores it from this committed row.
            peerStateStore.AcknowledgeOutboundBatch(scopeId, sender.NodeId,
                PeerNodeId, peerEpoch, namespaceId, authorStream,
                reservation.BatchId, reservation.CursorEnd, reservation.BatchDigest);
            // Metadata Store is authoritative in this overload. Do not mirror the committed
            // ACK/digest back into Node: the next call restores the exact progress from
            // durable peer state before building another batch.
            return true;
        }
        finally
        {
            _pushGate.Release();
        }
    }

}
