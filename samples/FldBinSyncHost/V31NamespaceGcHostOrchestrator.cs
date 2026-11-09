using BinSync;
using BinSync.Storage;

namespace BinSync.FldHost;

/// <summary>
/// Application-owned, one-shot v3.1 namespace-GC round orchestration.
/// The caller owns sessions, durable frame-sequence allocation, cadence and retries;
/// this helper only enforces the required ordering across the coordinator APIs.
/// Use one instance per namespace/coordinator. The round gate enforces one local
/// control-plane round at a time. No timer, worker or background receive loop is created.
/// </summary>
public sealed class V31NamespaceGcHostOrchestrator
{
    private readonly StorageSyncCoordinator _coordinator;
    private readonly IApplicationGcReferenceScanner _externalState;
    private readonly Func<IDisposable?> _acquireHostCommitLease;
    private readonly Func<SyncSession, ulong> _nextFrameSequence;
    private readonly SemaphoreSlim _roundGate = new(1, 1);

    public V31NamespaceGcHostOrchestrator(
        StorageSyncCoordinator coordinator,
        IApplicationGcReferenceScanner externalState,
        Func<IDisposable?> acquireHostCommitLease,
        Func<SyncSession, ulong> nextFrameSequence)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _externalState = externalState ?? throw new ArgumentNullException(nameof(externalState));
        _acquireHostCommitLease = acquireHostCommitLease
            ?? throw new ArgumentNullException(nameof(acquireHostCommitLease));
        _nextFrameSequence = nextFrameSequence
            ?? throw new ArgumentNullException(nameof(nextFrameSequence));
    }

    /// <summary>
    /// Publish this local authority/member's OFR2 report without sending it.
    /// Call before RunAuthorityRoundAsync when the namespace authority is also an
    /// Active roster member; otherwise its missing report correctly contributes zero.
    /// </summary>
    public AppliedOriginFrontierReport PublishAuthorityMemberFrontier(
        byte[] authorityMemberSeed,
        int pageSize = 1024)
    {
        if (!_roundGate.Wait(0))
            throw new InvalidOperationException(
                "A v3.1 namespace control-plane round is already active.");
        try
        {
            ArgumentNullException.ThrowIfNull(authorityMemberSeed);
            if (authorityMemberSeed.Length != 32)
                throw new ArgumentException(
                    "Ed25519 authority-member seed must be 32 bytes.",
                    nameof(authorityMemberSeed));
            if (pageSize <= 0 || pageSize > 4096)
                throw new ArgumentOutOfRangeException(nameof(pageSize));

            return _coordinator.PublishAppliedOriginFrontier(
                authorityMemberSeed,
                _externalState,
                _acquireHostCommitLease,
                pageSize);
        }
        finally
        {
            _roundGate.Release();
        }
    }

    /// <summary>
    /// Run one serialized authority round: send the accepted roster to each supplied
    /// member, receive/accept one OFR2 from each session, persist exactly one RCP2,
    /// then broadcast that same checkpoint. A missing Active roster member that has
    /// no supplied session/report remains at frontier zero in the checkpoint.
    /// </summary>
    public async Task<NamespaceRetentionCheckpoint> RunAuthorityRoundAsync(
        IReadOnlyList<SyncSession> activeMemberSessions,
        byte[] authoritySeed,
        Guid? fullReplaceGeneration = null,
        byte[]? fullReplaceDigest = null,
        byte[]? authorityMemberSeed = null,
        int pageSize = 1024,
        CancellationToken cancellationToken = default)
    {
        await _roundGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
        ArgumentNullException.ThrowIfNull(authoritySeed);
        if (authoritySeed.Length != 32)
            throw new ArgumentException("Ed25519 authority seed must be 32 bytes.", nameof(authoritySeed));
        if (authorityMemberSeed is not null && authorityMemberSeed.Length != 32)
            throw new ArgumentException("Ed25519 authority-member seed must be 32 bytes.", nameof(authorityMemberSeed));
        if (pageSize <= 0 || pageSize > 4096)
            throw new ArgumentOutOfRangeException(nameof(pageSize));
        if (fullReplaceGeneration.HasValue)
        {
            if (fullReplaceGeneration.Value == Guid.Empty ||
                fullReplaceDigest is null ||
                fullReplaceDigest.Length != NamespaceRosterProtocol.DigestSize)
                throw new ArgumentException(
                    "FULL_REPLACE binding requires a non-empty generation and 32-byte digest.",
                    nameof(fullReplaceDigest));
        }
        else if (fullReplaceDigest is { Length: > 0 })
        {
            throw new ArgumentException(
                "FULL_REPLACE digest cannot be supplied without a generation.",
                nameof(fullReplaceDigest));
        }

        SyncSession[] sessions = ValidateSessions(activeMemberSessions);
        foreach (SyncSession session in sessions)
        {
            await _coordinator.SendAcceptedNamespaceRosterAsync(
                session,
                authoritySeed,
                NextSequence(session),
                cancellationToken).ConfigureAwait(false);
        }

        if (authorityMemberSeed is not null)
        {
            _ = _coordinator.PublishAppliedOriginFrontier(
                authorityMemberSeed,
                _externalState,
                _acquireHostCommitLease,
                pageSize);
        }

        foreach (SyncSession session in sessions)
        {
            AppliedOriginFrontierReport report =
                await _coordinator.ReceiveAndAcceptOriginFrontierAsync(
                    session, cancellationToken).ConfigureAwait(false);
            if (report.MemberNodeId != session.PeerNodeId ||
                report.MemberTransportEpoch != checked((uint)session.PeerEpoch))
                throw new InvalidDataException(
                    "The received OFR2 member identity/epoch does not match the authenticated session peer.");
            if (report.NamespaceId != _coordinator.NamespaceId)
                throw new InvalidDataException("The received OFR2 report belongs to another namespace.");
        }

        // Publish once only after all supplied sessions have supplied their report.
        // If a broadcast fails, retry BroadcastAcceptedCheckpointAsync with this same
        // object instead of creating a new checkpoint sequence.
        NamespaceRetentionCheckpoint checkpoint = _coordinator.PublishRetentionCheckpoint(
            authoritySeed, fullReplaceGeneration, fullReplaceDigest);
        await BroadcastAcceptedCheckpointCoreAsync(
            sessions, checkpoint, authoritySeed, cancellationToken).ConfigureAwait(false);
        return checkpoint;
        }
        finally
        {
            _roundGate.Release();
        }
    }

    /// <summary>
    /// Broadcast an already accepted RCP2 object. Safe to retry after a partial send:
    /// the coordinator verifies the object is still the current durable checkpoint
    /// and does not mint another CheckpointSequence.
    /// </summary>
    public async Task BroadcastAcceptedCheckpointAsync(
        IReadOnlyList<SyncSession> memberSessions,
        NamespaceRetentionCheckpoint checkpoint,
        byte[] authoritySeed,
        CancellationToken cancellationToken = default)
    {
        await _roundGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            SyncSession[] sessions = ValidateSessions(memberSessions);
            await BroadcastAcceptedCheckpointCoreAsync(
                sessions, checkpoint, authoritySeed, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _roundGate.Release();
        }
    }

    private async Task BroadcastAcceptedCheckpointCoreAsync(
        IReadOnlyList<SyncSession> sessions,
        NamespaceRetentionCheckpoint checkpoint,
        byte[] authoritySeed,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        ArgumentNullException.ThrowIfNull(authoritySeed);
        if (authoritySeed.Length != 32)
            throw new ArgumentException("Ed25519 authority seed must be 32 bytes.", nameof(authoritySeed));

        foreach (SyncSession session in sessions)
        {
            await _coordinator.SendAcceptedRetentionCheckpointAsync(
                session,
                checkpoint,
                authoritySeed,
                NextSequence(session),
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Authority-side FULL_REPLACE transfer for one accepted non-retired member.
    /// The durable accepted roster is sent first; the sender verifies its signed
    /// manifest and persists the member's signed FRA2 in the same namespace-GC store.
    /// Keep the authority provider unchanged for all members that must install the
    /// same generation/digest before publishing the bound RCP2.
    /// </summary>
    public async Task<FullReplaceInstalledAck> TransferFullReplaceToMemberAsync(
        IDataProvider authorityProvider,
        SyncSession memberSession,
        IMetadataNamespaceGcStore gcStore,
        TrustStore trustStore,
        byte[] authoritySeed,
        Guid generation,
        CancellationToken cancellationToken = default)
    {
        await _roundGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ArgumentNullException.ThrowIfNull(authorityProvider);
            ArgumentNullException.ThrowIfNull(memberSession);
            ArgumentNullException.ThrowIfNull(gcStore);
            ArgumentNullException.ThrowIfNull(trustStore);
            ArgumentNullException.ThrowIfNull(authoritySeed);
            if (authoritySeed.Length != 32)
                throw new ArgumentException("Ed25519 authority seed must be 32 bytes.", nameof(authoritySeed));
            if (generation == Guid.Empty)
                throw new ArgumentException("FULL_REPLACE generation must be non-empty.", nameof(generation));
            EnsureNamespaceGcBinding(gcStore, trustStore);

            if (!gcStore.TryReadNamespaceRoster(
                    _coordinator.NamespaceId, trustStore, out NamespaceRosterCertificate? roster) ||
                roster is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");
            NamespaceRosterProtocol.VerifyOrThrow(roster, trustStore);
            if (roster.AuthorityNodeId != _coordinator.LocalReplicaId)
                throw new InvalidOperationException("Only the local pinned namespace authority may initiate FULL_REPLACE.");
            NamespaceRosterMember authority = FindRosterMember(roster, roster.AuthorityNodeId);
            if (authority.Status != NamespaceMemberStatus.Active)
                throw new InvalidOperationException("A Joining or Retired member cannot originate FULL_REPLACE.");

            NamespaceRosterMember target = FindRosterMember(roster, memberSession.PeerNodeId);
            if (target.Status == NamespaceMemberStatus.Retired ||
                target.TransportPeerEpoch != checked((uint)memberSession.PeerEpoch))
                throw new InvalidDataException(
                    "FULL_REPLACE target is retired or does not match the accepted roster transport epoch.");

            await _coordinator.SendAcceptedNamespaceRosterAsync(
                memberSession, authoritySeed, NextSequence(memberSession), cancellationToken)
                .ConfigureAwait(false);

            FullReplaceInstalledAck acknowledgement = await StorageFullReplaceSender.SendAsync(
                authorityProvider,
                memberSession,
                gcStore,
                trustStore,
                roster,
                _coordinator.LocalReplicaId,
                generation,
                authoritySeed,
                cancellationToken).ConfigureAwait(false);

            if (acknowledgement.MemberNodeId != memberSession.PeerNodeId ||
                acknowledgement.MemberTransportEpoch != target.TransportPeerEpoch ||
                acknowledgement.RosterGeneration != roster.RosterGeneration ||
                acknowledgement.Generation != generation ||
                !acknowledgement.Installed)
                throw new InvalidDataException("FULL_REPLACE installed ACK does not match the accepted target roster.");
            return acknowledgement;
        }
        finally
        {
            _roundGate.Release();
        }
    }

    /// <summary>
    /// Member-side Joining receive. It accepts the authority's signed roster before
    /// reading FULL_REPLACE frames. StorageFullReplaceReceiver performs durable staging,
    /// cross-store guarded commit, HLC/Node refresh and FRA2 persistence before sending
    /// the installed ACK. An ABORT returns null and never promotes the member.
    /// </summary>
    public async Task<FullReplaceInstalledAck?> ReceiveFullReplaceAsync(
        SyncSession authoritySession,
        StorageFullReplaceReceiver receiver,
        CancellationToken cancellationToken = default)
    {
        await _roundGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ArgumentNullException.ThrowIfNull(authoritySession);
            ArgumentNullException.ThrowIfNull(receiver);
            NamespaceRosterCertificate roster =
                await _coordinator.ReceiveAndAcceptNamespaceRosterAsync(
                    authoritySession, cancellationToken).ConfigureAwait(false);
            if (roster.NamespaceId != _coordinator.NamespaceId ||
                roster.AuthorityNodeId != authoritySession.PeerNodeId)
                throw new InvalidDataException("FULL_REPLACE roster does not match the authenticated authority session.");
            NamespaceRosterMember authority = FindRosterMember(roster, authoritySession.PeerNodeId);
            if (authority.Status != NamespaceMemberStatus.Active ||
                authority.TransportPeerEpoch != checked((uint)authoritySession.PeerEpoch))
                throw new InvalidDataException("FULL_REPLACE authority is not Active at this transport epoch.");
            NamespaceRosterMember local = FindRosterMember(roster, _coordinator.LocalReplicaId);
            if (local.Status != NamespaceMemberStatus.Joining)
                throw new InvalidOperationException("This receive entry point is only for a Joining member.");

            FullReplaceInstalledAck? acknowledgement =
                await receiver.ReceiveTransferAsync(authoritySession, cancellationToken).ConfigureAwait(false);
            if (acknowledgement is not null &&
                (acknowledgement.MemberNodeId != _coordinator.LocalReplicaId ||
                 acknowledgement.MemberTransportEpoch != local.TransportPeerEpoch ||
                 acknowledgement.RosterGeneration != roster.RosterGeneration ||
                 !acknowledgement.Installed))
                throw new InvalidDataException("FULL_REPLACE receiver returned an ACK outside the accepted Joining roster.");
            return acknowledgement;
        }
        finally
        {
            _roundGate.Release();
        }
    }

    /// <summary>
    /// Promote one Joining member only after the current durable RCP2 is bound to the
    /// same FULL_REPLACE generation/digest and the member's FRA2 is durably accepted
    /// under the prior roster. The updated NRS2 is persisted before it is broadcast.
    /// </summary>
    public async Task<NamespaceRosterCertificate> PromoteInstalledJoiningMemberAsync(
        NodeId memberNodeId,
        Guid generation,
        byte[] fullReplaceDigest,
        byte[] authoritySeed,
        IReadOnlyList<SyncSession> allMemberSessions,
        IMetadataNamespaceGcStore gcStore,
        TrustStore trustStore,
        CancellationToken cancellationToken = default)
    {
        await _roundGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ArgumentNullException.ThrowIfNull(fullReplaceDigest);
            ArgumentNullException.ThrowIfNull(authoritySeed);
            ArgumentNullException.ThrowIfNull(gcStore);
            ArgumentNullException.ThrowIfNull(trustStore);
            if (memberNodeId.IsZero || generation == Guid.Empty)
                throw new ArgumentException("Joining member identity and FULL_REPLACE generation are required.");
            if (fullReplaceDigest.Length != NamespaceRosterProtocol.DigestSize)
                throw new ArgumentException("FULL_REPLACE digest must be 32 bytes.", nameof(fullReplaceDigest));
            if (authoritySeed.Length != 32)
                throw new ArgumentException("Ed25519 authority seed must be 32 bytes.", nameof(authoritySeed));
            EnsureNamespaceGcBinding(gcStore, trustStore);

            if (!gcStore.TryReadNamespaceRoster(
                    _coordinator.NamespaceId, trustStore, out NamespaceRosterCertificate? prior) ||
                prior is null)
                throw new InvalidOperationException("No accepted namespace roster is available.");
            NamespaceRosterProtocol.VerifyOrThrow(prior, trustStore);
            if (prior.AuthorityNodeId != _coordinator.LocalReplicaId)
                throw new InvalidOperationException("Only the local pinned namespace authority may promote a member.");
            NamespaceRosterMember joining = FindRosterMember(prior, memberNodeId);
            if (joining.Status != NamespaceMemberStatus.Joining)
                throw new InvalidOperationException("Only a Joining member can be promoted by this operation.");

            if (!gcStore.TryReadRetentionCheckpoint(prior, trustStore, out NamespaceRetentionCheckpoint? checkpoint) ||
                checkpoint is null ||
                checkpoint.FullReplaceGeneration != generation ||
                !checkpoint.FullReplaceDigest.AsSpan().SequenceEqual(fullReplaceDigest) ||
                !gcStore.AreAllActiveMembersInstalled(prior, generation, fullReplaceDigest))
                throw new InvalidOperationException(
                    "The prior roster must have a durable FULL_REPLACE-bound RCP2 and installed ACKs from every current Active member.");

            FullReplaceInstalledAck? joiningAck = gcStore.ReadFullReplaceInstalledAcks(prior, generation)
                .FirstOrDefault(ack => ack.MemberNodeId == memberNodeId);
            if (joiningAck is null ||
                joiningAck.MemberTransportEpoch != joining.TransportPeerEpoch ||
                joiningAck.RosterGeneration != prior.RosterGeneration ||
                !joiningAck.Installed ||
                !joiningAck.Digest.AsSpan().SequenceEqual(fullReplaceDigest))
                throw new InvalidOperationException(
                    "The Joining member has not durably installed the exact FULL_REPLACE generation under the prior roster.");

            SyncSession[] sessions = ValidateSessions(allMemberSessions);
            ValidateCompleteRosterSessionSet(prior, sessions);

            NamespaceRosterMember[] promotedMembers = prior.Members
                .Select(member => member.NodeId == memberNodeId
                    ? new NamespaceRosterMember
                    {
                        NodeId = member.NodeId,
                        PublicKey = member.PublicKey.ToArray(),
                        TransportPeerEpoch = member.TransportPeerEpoch,
                        Status = NamespaceMemberStatus.Active,
                        RetiredThroughEpoch = member.RetiredThroughEpoch
                    }
                    : member)
                .ToArray();
            var unsigned = new NamespaceRosterCertificate
            {
                NamespaceId = prior.NamespaceId,
                RosterGeneration = checked(prior.RosterGeneration + 1),
                PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(prior),
                AuthorityNodeId = prior.AuthorityNodeId,
                Members = promotedMembers,
                RetiredOriginEpochs = prior.RetiredOriginEpochs,
                Signature = Array.Empty<byte>()
            };
            byte[] signed = NamespaceRosterProtocol.Sign(unsigned, authoritySeed);
            NamespaceRosterCertificate accepted = gcStore.AcceptNamespaceRoster(signed, trustStore);
            await BroadcastAcceptedRosterCoreAsync(sessions, cancellationToken).ConfigureAwait(false);
            return accepted;
        }
        finally
        {
            _roundGate.Release();
        }
    }

    /// <summary>
    /// Accept a later Active roster on the rejoined member and only then activate its
    /// fresh local author epoch. This is deliberately separate from FULL_REPLACE commit:
    /// installed data does not grant permission to originate new local operations.
    /// </summary>
    public async Task<NamespaceRosterCertificate> AcceptActiveRosterAndActivateLocalAuthorEpochAsync(
        SyncSession authoritySession,
        uint newAuthorEpoch,
        CancellationToken cancellationToken = default)
    {
        await _roundGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ArgumentNullException.ThrowIfNull(authoritySession);
            if (newAuthorEpoch == 0)
                throw new ArgumentOutOfRangeException(nameof(newAuthorEpoch));
            NamespaceRosterCertificate roster =
                await _coordinator.ReceiveAndAcceptNamespaceRosterAsync(
                    authoritySession, cancellationToken).ConfigureAwait(false);
            if (roster.NamespaceId != _coordinator.NamespaceId ||
                roster.AuthorityNodeId != authoritySession.PeerNodeId)
                throw new InvalidDataException("Activation roster does not match the authenticated authority session.");
            NamespaceRosterMember authority = FindRosterMember(roster, authoritySession.PeerNodeId);
            if (authority.Status != NamespaceMemberStatus.Active ||
                authority.TransportPeerEpoch != checked((uint)authoritySession.PeerEpoch))
                throw new InvalidDataException("Activation roster authority is not Active at this transport epoch.");
            NamespaceRosterMember local = FindRosterMember(roster, _coordinator.LocalReplicaId);
            if (local.Status != NamespaceMemberStatus.Active)
                throw new InvalidOperationException("A local author epoch can be activated only after the accepted roster marks this member Active.");

            _coordinator.ActivateLocalAuthorEpoch(newAuthorEpoch);
            return roster;
        }
        finally
        {
            _roundGate.Release();
        }
    }

    /// <summary>Retry broadcasting the currently accepted signed roster without changing its generation.</summary>
    public async Task BroadcastAcceptedRosterAsync(
        IReadOnlyList<SyncSession> memberSessions,
        CancellationToken cancellationToken = default)
    {
        await _roundGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            SyncSession[] sessions = ValidateSessions(memberSessions);
            if (_coordinator.NamespaceId <= 0)
                throw new InvalidOperationException("The coordinator namespace is invalid.");
            await BroadcastAcceptedRosterCoreAsync(sessions, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _roundGate.Release();
        }
    }

    private async Task BroadcastAcceptedRosterCoreAsync(
        IReadOnlyList<SyncSession> sessions,
        CancellationToken cancellationToken)
    {
        foreach (SyncSession session in sessions)
        {
            await _coordinator.SendAcceptedNamespaceRosterAsync(
                session, Array.Empty<byte>(), NextSequence(session), cancellationToken).ConfigureAwait(false);
        }
    }

    private void EnsureNamespaceGcBinding(
        IMetadataNamespaceGcStore gcStore, TrustStore trustStore)
    {
        if (!_coordinator.UsesNamespaceGcStore(gcStore) ||
            !_coordinator.UsesNamespaceGcTrustStore(trustStore))
            throw new InvalidOperationException(
                "The v3.1 host helper and StorageSyncCoordinator must share the exact namespace-GC store and TrustStore instances.");
    }

    private static NamespaceRosterMember FindRosterMember(
        NamespaceRosterCertificate roster, NodeId nodeId)
        => roster.Members.FirstOrDefault(member => member.NodeId == nodeId)
            ?? throw new InvalidDataException("Namespace roster does not contain the required member.");

    private void ValidateCompleteRosterSessionSet(
        NamespaceRosterCertificate roster, IReadOnlyList<SyncSession> sessions)
    {
        NamespaceRosterMember[] expected = roster.Members
            .Where(member => member.Status != NamespaceMemberStatus.Retired &&
                member.NodeId != _coordinator.LocalReplicaId)
            .ToArray();
        if (sessions.Count != expected.Length)
            throw new ArgumentException(
                "Roster promotion must be broadcast to every non-retired remote member.", nameof(sessions));
        foreach (SyncSession session in sessions)
        {
            NamespaceRosterMember member = FindRosterMember(roster, session.PeerNodeId);
            if (member.Status == NamespaceMemberStatus.Retired ||
                member.TransportPeerEpoch != checked((uint)session.PeerEpoch))
                throw new ArgumentException(
                    "Roster promotion session set does not match the accepted member epochs.", nameof(sessions));
        }
        if (expected.Any(member => !sessions.Any(session => session.PeerNodeId == member.NodeId)))
            throw new ArgumentException("Roster promotion omitted a non-retired member session.", nameof(sessions));
    }

    /// <summary>
    /// Run one Active-member round: accept the authority roster, publish/send this
    /// member's OFR2 under the composed application commit lease, then receive and
    /// durably accept the authority RCP2. Joining members must complete FULL_REPLACE
    /// and receive an Active roster before using this method.
    /// </summary>
    public async Task<NamespaceRetentionCheckpoint> RunActiveMemberRoundAsync(
        SyncSession authoritySession,
        byte[] memberSeed,
        int pageSize = 1024,
        CancellationToken cancellationToken = default)
    {
        await _roundGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
        ArgumentNullException.ThrowIfNull(authoritySession);
        ArgumentNullException.ThrowIfNull(memberSeed);
        if (memberSeed.Length != 32)
            throw new ArgumentException("Ed25519 member seed must be 32 bytes.", nameof(memberSeed));

        NamespaceRosterCertificate roster =
            await _coordinator.ReceiveAndAcceptNamespaceRosterAsync(
                authoritySession, cancellationToken).ConfigureAwait(false);
        if (roster.NamespaceId != _coordinator.NamespaceId ||
            roster.AuthorityNodeId != authoritySession.PeerNodeId)
            throw new InvalidDataException(
                "The accepted namespace roster does not belong to this authority session.");

        NamespaceRosterMember? localMember = roster.Members
            .FirstOrDefault(member => member.NodeId == _coordinator.LocalReplicaId);
        if (localMember is null || localMember.Status != NamespaceMemberStatus.Active)
            throw new InvalidOperationException(
                "Only an Active roster member may publish an applied-origin frontier; Joining members must install FULL_REPLACE first.");

        AppliedOriginFrontierReport report =
            await _coordinator.PublishAndSendAppliedOriginFrontierAsync(
                authoritySession,
                memberSeed,
                _externalState,
                _acquireHostCommitLease,
                pageSize,
                NextSequence(authoritySession),
                ct: cancellationToken).ConfigureAwait(false);
        if (report.MemberNodeId != _coordinator.LocalReplicaId ||
            report.MemberTransportEpoch != localMember.TransportPeerEpoch ||
            report.RosterGeneration != roster.RosterGeneration)
            throw new InvalidDataException("The published OFR2 report does not match the accepted local member epoch/roster.");

        NamespaceRetentionCheckpoint checkpoint =
            await _coordinator.ReceiveAndAcceptRetentionCheckpointAsync(
                authoritySession, cancellationToken).ConfigureAwait(false);
        if (checkpoint.RosterGeneration != roster.RosterGeneration)
            throw new InvalidDataException("The accepted RCP2 checkpoint does not match the accepted roster generation.");
        return checkpoint;
        }
        finally
        {
            _roundGate.Release();
        }
    }

    /// <summary>Resend the latest durable local OFR2 without allocating a new report sequence.</summary>
    public async Task<AppliedOriginFrontierReport> ResendAcceptedFrontierAsync(
        SyncSession authoritySession,
        byte[] memberSeed,
        CancellationToken cancellationToken = default)
    {
        await _roundGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ArgumentNullException.ThrowIfNull(authoritySession);
            ArgumentNullException.ThrowIfNull(memberSeed);
            return await _coordinator.SendAcceptedAppliedOriginFrontierAsync(
                authoritySession,
                memberSeed,
                NextSequence(authoritySession),
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _roundGate.Release();
        }
    }

    private SyncSession[] ValidateSessions(IReadOnlyList<SyncSession> sessions)
    {
        ArgumentNullException.ThrowIfNull(sessions);
        var seen = new HashSet<NodeId>();
        var result = new SyncSession[sessions.Count];
        for (int i = 0; i < sessions.Count; i++)
        {
            SyncSession session = sessions[i]
                ?? throw new ArgumentException("Member session collection contains null.", nameof(sessions));
            if (session.PeerNodeId.IsZero ||
                session.PeerNodeId == _coordinator.LocalReplicaId ||
                !seen.Add(session.PeerNodeId))
                throw new ArgumentException(
                    "Member sessions must contain unique, non-zero remote peers and must not include the local replica.",
                    nameof(sessions));
            result[i] = session;
        }
        return result;
    }

    private ulong NextSequence(SyncSession session)
    {
        ulong sequence = _nextFrameSequence(session);
        if (sequence == 0)
            throw new InvalidOperationException("The host frame-sequence allocator returned zero.");
        return sequence;
    }
}
