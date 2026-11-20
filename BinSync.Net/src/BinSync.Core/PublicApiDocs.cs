// Central XML-doc anchors for the public engine surface (StyleCop / IntelliSense).
// Detailed behavior remains in method-level comments and docs/PUBLIC-API.md.

namespace BinSync;

/// <summary>BIN-SYNC CORE reference engine: offline-first binary sync (LWW, HLC, delta batches).</summary>
public static partial class Codec
{
}

/// <summary>Wire and engine DTOs used by <see cref="Node"/> (batch, outbox entry, peer state).</summary>
public sealed partial class Batch
{
}

/// <summary>
/// Sync node: storage, outbox, alias maps, apply/create delta batch.
/// Identity (<see cref="NodeId"/>) is fixed at construction; clock state is observed through <see cref="ClockState"/>.
/// </summary>
public sealed partial class Node
{
}

/// <summary>Transport helpers: session setup, deliver batch, sync round, sneakernet import/export.</summary>
public static partial class Transport
{
}

/// <summary>Operation encode/decode (FieldWrite / FieldTomb / RecordTombstone) and origin rewrite.</summary>
public static partial class Ops
{
}

/// <summary>128-bit node identity with value equality.</summary>
public readonly partial struct NodeId
{
}

/// <summary>128-bit record identity with value equality.</summary>
public readonly partial struct RecordId
{
}

/// <summary>128-bit batch identity with value equality.</summary>
public readonly partial struct BatchId
{
}
