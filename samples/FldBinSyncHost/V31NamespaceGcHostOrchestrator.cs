using BinSync;
using BinSync.Storage;

namespace BinSync.FldHost;

/// <summary>
/// Application-owned, one-shot v3.1 namespace-GC round orchestration.
/// The caller owns sessions, durable frame-sequence allocation, cadence and retries;
/// this helper only enforces the required ordering across the coordinator APIs.
/// It deliberately creates no timer, worker or background receive loop.
/// </summary>
public sealed class V31NamespaceGcHostOrchestrator
{
    private readonly StorageSyncCoordinator _coordinator;
    private readonly IApplicationGcReferenceScanner _externalState;
    private readonly Func<IDisposable?> _acquireHostCommitLease;
    private readonly Func<SyncSession, ulong> _nextFrameSequence;

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
        => _coordinator.PublishAppliedOriginFrontier(
            authorityMemberSeed,
            _externalState,
            _acquireHostCommitLease,
            pageSize);

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
        ArgumentNullException.ThrowIfNull(authoritySeed);
        if (authoritySeed.Length != 32)
            throw new ArgumentException("Ed25519 authority seed must be 32 bytes.", nameof(authoritySeed));
        if (pageSize <= 0 || pageSize > 4096)
            throw new ArgumentOutOfRangeException(nameof(pageSize));

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
            if (authorityMemberSeed.Length != 32)
                throw new ArgumentException("Ed25519 authority-member seed must be 32 bytes.", nameof(authorityMemberSeed));
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
            if (report.MemberNodeId != session.PeerNodeId)
                throw new InvalidDataException(
                    "The received OFR2 member identity does not match the authenticated session peer.");
            if (report.NamespaceId != _coordinator.NamespaceId)
                throw new InvalidDataException("The received OFR2 report belongs to another namespace.");
        }

        // Publish once only after all supplied sessions have supplied their report.
        // If a broadcast fails, retry BroadcastAcceptedCheckpointAsync with this same
        // object instead of creating a new checkpoint sequence.
        NamespaceRetentionCheckpoint checkpoint = _coordinator.PublishRetentionCheckpoint(
            authoritySeed, fullReplaceGeneration, fullReplaceDigest);
        await BroadcastAcceptedCheckpointAsync(
            sessions, checkpoint, authoritySeed, cancellationToken).ConfigureAwait(false);
        return checkpoint;
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
        ArgumentNullException.ThrowIfNull(checkpoint);
        ArgumentNullException.ThrowIfNull(authoritySeed);
        if (authoritySeed.Length != 32)
            throw new ArgumentException("Ed25519 authority seed must be 32 bytes.", nameof(authoritySeed));

        SyncSession[] sessions = ValidateSessions(memberSessions);
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
                cancellationToken: cancellationToken).ConfigureAwait(false);
        if (report.MemberNodeId != _coordinator.LocalReplicaId ||
            report.RosterGeneration != roster.RosterGeneration)
            throw new InvalidDataException("The published OFR2 report does not match the accepted local member/roster.");

        NamespaceRetentionCheckpoint checkpoint =
            await _coordinator.ReceiveAndAcceptRetentionCheckpointAsync(
                authoritySession, cancellationToken).ConfigureAwait(false);
        if (checkpoint.RosterGeneration != roster.RosterGeneration)
            throw new InvalidDataException("The accepted RCP2 checkpoint does not match the accepted roster generation.");
        return checkpoint;
    }

    /// <summary>Resend the latest durable local OFR2 without allocating a new report sequence.</summary>
    public Task<AppliedOriginFrontierReport> ResendAcceptedFrontierAsync(
        SyncSession authoritySession,
        byte[] memberSeed,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(authoritySession);
        ArgumentNullException.ThrowIfNull(memberSeed);
        return _coordinator.SendAcceptedAppliedOriginFrontierAsync(
            authoritySession,
            memberSeed,
            NextSequence(authoritySession),
            cancellationToken);
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
