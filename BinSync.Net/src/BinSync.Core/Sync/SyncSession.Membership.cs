// MEMBERSHIP exchange + bilateral AutoReconcile on SyncSession.
namespace BinSync;

/// <summary>Established TCP (optionally TLS) sync session with a peer.</summary>
public partial class SyncSession
{
    /// <summary>Send local MEMBERSHIP roster for a namespace.</summary>
    public async Task SendMembershipAsync(
        Node local, int namespaceId, uint membershipEpoch = 0, CancellationToken ct = default)
    {
        uint streamId = 0u;
        var msg = Membership.Build(local, namespaceId, membershipEpoch);
        await SendFrameAsync(MembershipFrames.Membership, streamId, 14, Membership.Encode(msg), ct).ConfigureAwait(false);
    }

    /// <summary>Receive MEMBERSHIP and merge into local durable authors.</summary>
    public async Task<int> ReceiveMembershipAsync(Node local, CancellationToken ct = default)
    {
        var raw = await ReceiveExpectedExclusiveAsync(MembershipFrames.Membership, ct).ConfigureAwait(false)
           ;
        var frame = ParseExpectedSessionFrame(raw, MembershipFrames.Membership);
        ValidateControlStream(frame);
        var msg = Membership.Decode(frame.Payload);
        return Membership.Apply(local, msg);
    }

    /// <summary>
    /// Pairwise MEMBERSHIP exchange: initiate sends first, then receives; acceptor reverse.
    /// </summary>
    public async Task<(int LearnedLocal, int LearnedRemote)> ExchangeMembershipAsync(
        Node local, int namespaceId, bool initiate, uint membershipEpoch = 0, CancellationToken ct = default)
    {
        if (initiate)
        {
            await SendMembershipAsync(local, namespaceId, membershipEpoch, ct).ConfigureAwait(false);
            int learned = await ReceiveMembershipAsync(local, ct).ConfigureAwait(false);
            return (0, learned);
        }
        else
        {
            int learned = await ReceiveMembershipAsync(local, ct).ConfigureAwait(false);
            await SendMembershipAsync(local, namespaceId, membershipEpoch, ct).ConfigureAwait(false);
            return (learned, 0);
        }
    }

    /// <summary>
    /// Two-phase AutoReconcile: first pull from peer, then serve peer's pull.
    /// Covers the case where each side holds unique offline records.
    /// </summary>
    public async Task<(int PrefixesPhase1, int FieldsPhase1, int PrefixesPhase2, int FieldsPhase2)>
        BilateralAutoReconcileAsync(
            Node local, int namespaceId, bool initiate, int maxPrefixes = 16, CancellationToken ct = default)
    {
        var (p1, f1) = await AutoReconcileAsync(local, namespaceId, initiate, maxPrefixes, ct)
            .ConfigureAwait(false);
        var (p2, f2) = await AutoReconcileAsync(local, namespaceId, !initiate, maxPrefixes, ct)
            .ConfigureAwait(false);
        return (p1, f1, p2, f2);
    }
}
