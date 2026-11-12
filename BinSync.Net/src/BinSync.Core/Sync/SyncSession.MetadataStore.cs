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
    public Task<bool> PushBatchAsync(Node sender, int namespaceId,
        IMetadataPeerStateStore peerStateStore, string scopeId,
        int schemaVersion = 1, CancellationToken ct = default)
        => PushBatchCoreAsync(sender, namespaceId, peerStateStore, scopeId,
            publicationCeilings: null, schemaVersion, ct);

    /// <summary>
    /// Push using an explicit per-author durable publication ceiling. Authors absent
    /// from the map and authors with a zero ceiling are ineligible; a pending immutable
    /// reservation above its author's ceiling fails closed instead of being sent.
    /// </summary>
    public Task<bool> PushBatchAsync(Node sender, int namespaceId,
        IMetadataPeerStateStore peerStateStore, string scopeId,
        IReadOnlyDictionary<NodeId, ulong> publicationCeilings,
        int schemaVersion = 1, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publicationCeilings);
        return PushBatchCoreAsync(sender, namespaceId, peerStateStore, scopeId,
            publicationCeilings, schemaVersion, ct);
    }

    private async Task<bool> PushBatchCoreAsync(Node sender, int namespaceId,
        IMetadataPeerStateStore peerStateStore, string scopeId,
        IReadOnlyDictionary<NodeId, ulong>? publicationCeilings,
        int schemaVersion, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(sender);
        ValidateLocalNode(sender);
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
                ValidatePublicationCeiling(authorStream, reservation, publicationCeilings);
            }
            else
            {
                var outboundAuthors = peerStateStore.ReadOutboundAuthors(
                    scopeId, sender.NodeId, namespaceId).ToHashSet();
                var relayAuthors = peerStateStore.ReadRelayAuthors(
                    scopeId, sender.NodeId, namespaceId).ToHashSet();
                if (outboundAuthors.Overlaps(relayAuthors))
                    throw new InvalidDataException(
                        "A peer author stream is present in both outbound and relay payload stores.");
                var authors = outboundAuthors.Concat(relayAuthors)
                    .Where(author => publicationCeilings is null ||
                        (publicationCeilings.TryGetValue(author, out ulong ceiling) && ceiling != 0))
                    .OrderBy(author => Convert.ToHexString(author.ToArray()), StringComparer.Ordinal)
                    .ToArray();
                NodeId? selectedAuthor = null;
                MetadataPeerStreamState selectedState = default!;
                ulong selectedSequence = ulong.MaxValue;
                bool selectedIsContiguous = false;
                ulong selectedPublicationCeiling = ulong.MaxValue;

                foreach (NodeId candidateAuthor in authors)
                {
                    if (candidateAuthor == PeerNodeId) continue;
                    ulong candidateCeiling = ulong.MaxValue;
                    if (publicationCeilings is not null &&
                        (!publicationCeilings.TryGetValue(candidateAuthor, out candidateCeiling) ||
                         candidateCeiling == 0))
                        continue;
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
                    if (outboundAuthors.Contains(candidateAuthor))
                    {
                        var first = peerStateStore.ReadOutboundForPeer(scopeId, sender.NodeId,
                            PeerNodeId, peerEpoch, namespaceId, candidateAuthor, 1);
                        if (first.Items.Count == 0 ||
                            first.Items[0].Sequence > candidateCeiling) continue;
                        firstSequence = first.Items[0].Sequence;
                    }
                    else
                    {
                        var first = peerStateStore.ReadRelayForPeer(scopeId, sender.NodeId,
                            PeerNodeId, peerEpoch, namespaceId, candidateAuthor, 1);
                        if (first.Items.Count == 0 ||
                            first.Items[0].Sequence > candidateCeiling) continue;
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
                            selectedPublicationCeiling = candidateCeiling;
                        }
                    }
                    else if (!selectedIsContiguous && firstSequence < selectedSequence)
                    {
                        // A gap is a fallback only. It must not block another author
                        // stream whose next record is contiguous for this peer.
                        selectedAuthor = candidateAuthor;
                        selectedState = saved;
                        selectedSequence = firstSequence;
                        selectedPublicationCeiling = candidateCeiling;
                    }
                }

                if (selectedAuthor is null) return false;
                authorStream = selectedAuthor.Value;
                IReadOnlyList<(ulong Sequence, BatchId ChangeId, uint OriginAuthorEpoch, ulong OriginSequence, byte[] Payload)> pageItems;
                if (outboundAuthors.Contains(authorStream))
                {
                    var page = peerStateStore.ReadOutboundForPeer(scopeId, sender.NodeId,
                        PeerNodeId, peerEpoch, namespaceId, authorStream, 256);
                    pageItems = publicationCeilings is null
                        ? page.Items.Select(item => (item.Sequence, item.ChangeId,
                            item.OriginAuthorEpoch, item.OriginSequence, item.Payload)).ToArray()
                        : ProjectWithinPublicationCeiling(page, selectedPublicationCeiling);
                }
                else
                {
                    var page = peerStateStore.ReadRelayForPeer(scopeId, sender.NodeId,
                        PeerNodeId, peerEpoch, namespaceId, authorStream, 256);
                    pageItems = publicationCeilings is null
                        ? page.Items.Select(item => (item.Sequence, item.ChangeId,
                            item.OriginAuthorEpoch, item.OriginSequence, item.Payload)).ToArray()
                        : ProjectWithinPublicationCeiling(page, selectedPublicationCeiling);
                }

                if (pageItems.Count == 0) return false;
                await EnsureRemoteAliasAsync(authorStream,
                    checked((int)selectedState.IssuerEpoch), ct).ConfigureAwait(false);
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
                ValidatePublicationCeiling(authorStream, reservation, publicationCeilings);
            }

            if (reservation.IssuerEpoch != 0)
                await EnsureRemoteAliasAsync(authorStream, checked((int)reservation.IssuerEpoch), ct).ConfigureAwait(false);
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


    private static void ValidatePublicationCeiling(
        NodeId author,
        MetadataOutboundBatchReservation reservation,
        IReadOnlyDictionary<NodeId, ulong>? publicationCeilings)
    {
        if (publicationCeilings is null)
            return;
        if (!publicationCeilings.TryGetValue(author, out ulong ceiling) ||
            checked(reservation.CursorEnd + reservation.StreamOffset) > ceiling)
            throw new InvalidDataException(
                "An immutable outbound reservation exceeds the captured per-author publication ceiling.");
    }

    private static IReadOnlyList<(ulong Sequence, BatchId ChangeId,
        uint OriginAuthorEpoch, ulong OriginSequence, byte[] Payload)> ProjectWithinPublicationCeiling(
        MetadataRelayPage page, ulong ceiling)
    {
        int count = 0;
        ulong previous = 0;
        bool ceilingReached = false;
        foreach (MetadataRelayItem item in page.Items)
        {
            if (item.Sequence == 0 || (previous != 0 && item.Sequence <= previous))
                throw new InvalidDataException("Metadata relay page is not strictly sequence ordered.");
            previous = item.Sequence;
            if (item.Sequence > ceiling)
                ceilingReached = true;
            else if (ceilingReached)
                throw new InvalidDataException("Metadata relay page is not ordered at the publication ceiling.");
            else
                count++;
        }

        if (count == 0)
            return Array.Empty<(ulong, BatchId, uint, ulong, byte[])>();
        var result = new (ulong Sequence, BatchId ChangeId,
            uint OriginAuthorEpoch, ulong OriginSequence, byte[] Payload)[count];
        for (int i = 0; i < count; i++)
        {
            MetadataRelayItem item = page.Items[i];
            result[i] = (item.Sequence, item.ChangeId,
                item.OriginAuthorEpoch, item.OriginSequence, item.Payload);
        }
        return result;
    }

    private static IReadOnlyList<(ulong Sequence, BatchId ChangeId,
        uint OriginAuthorEpoch, ulong OriginSequence, byte[] Payload)> ProjectWithinPublicationCeiling(
        MetadataOutboundPage page, ulong ceiling)
    {
        int count = 0;
        ulong previous = 0;
        bool ceilingReached = false;
        foreach (MetadataOutboundItem item in page.Items)
        {
            if (item.Sequence == 0 || (previous != 0 && item.Sequence <= previous))
                throw new InvalidDataException("Metadata outbound page is not strictly sequence ordered.");
            previous = item.Sequence;
            if (item.Sequence > ceiling)
                ceilingReached = true;
            else if (ceilingReached)
                throw new InvalidDataException("Metadata outbound page is not ordered at the publication ceiling.");
            else
                count++;
        }

        if (count == 0)
            return Array.Empty<(ulong, BatchId, uint, ulong, byte[])>();
        var result = new (ulong Sequence, BatchId ChangeId,
            uint OriginAuthorEpoch, ulong OriginSequence, byte[] Payload)[count];
        for (int i = 0; i < count; i++)
        {
            MetadataOutboundItem item = page.Items[i];
            result[i] = (item.Sequence, item.ChangeId,
                item.OriginAuthorEpoch, item.OriginSequence, item.Payload);
        }
        return result;
    }

}
