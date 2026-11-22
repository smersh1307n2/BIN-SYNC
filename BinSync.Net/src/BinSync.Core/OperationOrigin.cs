namespace BinSync;

/// <summary>
/// Immutable provenance assigned once when an operation is first published.
/// It is independent of a relay's local Outbox sequence and transport issuer cursor.
/// </summary>
public readonly record struct OperationOrigin
{
    public NodeId AuthorNodeId { get; }
    public uint AuthorEpoch { get; }
    public ulong Sequence { get; }

    public bool IsValid => !AuthorNodeId.IsZero && AuthorEpoch != 0 && Sequence != 0;

    public OperationOrigin(NodeId authorNodeId, uint authorEpoch, ulong sequence)
    {
        if (authorNodeId.IsZero)
            throw new ArgumentException("Origin author NodeId must be non-zero.", nameof(authorNodeId));
        if (authorEpoch == 0)
            throw new ArgumentOutOfRangeException(nameof(authorEpoch), "Origin author epoch must be non-zero.");
        if (sequence == 0)
            throw new ArgumentOutOfRangeException(nameof(sequence), "Origin sequence must be non-zero.");

        AuthorNodeId = authorNodeId;
        AuthorEpoch = authorEpoch;
        Sequence = sequence;
    }

    /// <summary>Validate a value that may have been obtained from default(T) or untrusted persisted state.</summary>
    public OperationOrigin EnsureValid()
    {
        if (!IsValid)
            throw new InvalidDataException("Immutable Origin must contain a non-zero author, epoch and sequence.");
        return this;
    }
}
