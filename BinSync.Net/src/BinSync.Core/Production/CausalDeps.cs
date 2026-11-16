// Causal dependencies §16.19 (experimental): KЗ1–KЗ5.
// Optional header causalDeps: array<b128> of changeId parents.
// Not auto-wired into Node.WriteField / ApplyBatch — embedders opt in.
namespace BinSync;

/// <summary>Why a causal-gated op was rejected or is still waiting.</summary>
public enum CausalOutcome
{
    /// <summary>All deps present (or none declared); op may be applied now.</summary>
    Ready = 0,
    /// <summary>Missing parent(s); held in quarantine (does not advance cursor — KЗ4).</summary>
    Pending = 1,
    /// <summary>Wait exceeded <see cref="CausalTracker.DepTimeout"/> — drop (KЗ1).</summary>
    TimedOut = 2,
    /// <summary>Dep graph contains a cycle involving this changeId (KЗ2).</summary>
    Cycle = 3,
    /// <summary>Explicitly marked lost / unrecoverable parent (KЗ3).</summary>
    LostDep = 4,
}

/// <summary>One op waiting on causal parents.</summary>
public sealed class PendingCausalOp
{
    /// <summary>Idempotency key for this change.</summary>
    public required BatchId ChangeId { get; init; }
    /// <summary>Causal dependency change ids required before apply.</summary>
    public required BatchId[] Deps { get; init; }
    /// <summary>UTC time in milliseconds when the item was enqueued.</summary>
    public required long EnqueuedUtcMs { get; init; }
    /// <summary>Optional held batch / payload for later retry.</summary>
    public object? Payload { get; init; }
}

/// <summary>
/// Tracks known changeIds, quarantines ops with missing parents, detects cycles,
/// and expires timed-out waits. Thread-safe. Keys are BatchId (no hex-string allocations).
/// </summary>
public sealed class CausalTracker
{
    private readonly object _gate = new();
    internal object SyncRoot => _gate;
    private readonly HashSet<BatchId> _known = new();
    private readonly Dictionary<BatchId, PendingCausalOp> _pending = new();
    private readonly Dictionary<BatchId, HashSet<BatchId>> _edges = new(); // child → parents
    private readonly Dictionary<BatchId, HashSet<BatchId>> _dependents = new(); // parent → pending children
    private readonly HashSet<BatchId> _lost = new(); // permanently unavailable change IDs

    private TimeSpan _depTimeout = TimeSpan.FromSeconds(30);
    private Func<long> _utcNowMs = () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>How long a missing dep may stay pending before KЗ1 timeout.</summary>
    public TimeSpan DepTimeout
    {
        get
        {
            lock (_gate) return _depTimeout;
        }
        set
        {
            if (value < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), "Dependency timeout cannot be negative.");
            lock (_gate) _depTimeout = value;
        }
    }

    /// <summary>Clock for tests; defaults to UTC now. The delegate cannot be null.</summary>
    public Func<long> UtcNowMs
    {
        get
        {
            lock (_gate) return _utcNowMs;
        }
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            lock (_gate) _utcNowMs = value;
        }
    }

    /// <summary>Number of change ids marked known/applied in the tracker.</summary>
    public int KnownCount { get { lock (_gate) return _known.Count; } }
    /// <summary>Number of ops still waiting on causal dependencies.</summary>
    public int PendingCount { get { lock (_gate) return _pending.Count; } }

    /// <summary>Hex key for diagnostics (allocates).</summary>
    public static string Key(BatchId id)
    {
        Span<byte> tmp = stackalloc byte[16];
        id.WriteTo(tmp);
        return Convert.ToHexString(tmp);
    }

    /// <summary>Record that a typed change id is durably known.</summary>
    public void NoteApplied(BatchId changeId)
    {
        if (changeId.IsZero)
            throw new ArgumentException("ChangeId cannot be zero.", nameof(changeId));
        lock (_gate)
        {
            if (_lost.Contains(changeId))
                throw new InvalidOperationException("A permanently lost change cannot be marked as applied.");
            _known.Add(changeId);
            RemovePendingLocked(changeId);
        }
    }

    /// <summary>True when this change id is already marked applied in the tracker.</summary>
    public bool IsKnown(BatchId changeId)
    {
        lock (_gate) return _known.Contains(changeId);
    }

    /// <summary>Evaluate with typed change ids.</summary>
    private CausalOutcome EvaluateLocked(BatchId changeId, IReadOnlyList<BatchId> deps, out string? detail)
    {
        detail = null;
        if (_lost.Contains(changeId))
        {
            detail = Key(changeId);
            return CausalOutcome.LostDep;
        }
        if (deps.Count == 0 || _known.Contains(changeId))
            return CausalOutcome.Ready;

        foreach (var dep in deps)
        {
            if (dep == changeId)
            {
                detail = "self-dep";
                return CausalOutcome.Cycle;
            }
            if (_lost.Contains(dep))
            {
                detail = Key(dep);
                return CausalOutcome.LostDep;
            }
        }

        if (deps is not HashSet<BatchId> parents)
            parents = new HashSet<BatchId>(deps);
        if (WouldCycle(changeId, parents))
        {
            detail = "cycle";
            return CausalOutcome.Cycle;
        }

        foreach (var dep in deps)
        {
            if (!_known.Contains(dep))
            {
                detail = Key(dep);
                return CausalOutcome.Pending;
            }
        }
        return CausalOutcome.Ready;
    }

    public CausalOutcome Evaluate(BatchId changeId, IReadOnlyList<BatchId>? deps, out string? detail)
    {
        if (changeId.IsZero)
            throw new ArgumentException("ChangeId cannot be zero.", nameof(changeId));
        deps ??= Array.Empty<BatchId>();
        for (int i = 0; i < deps.Count; i++)
            if (deps[i].IsZero)
                throw new ArgumentException("Causal dependency ID cannot be zero.", nameof(deps));
        lock (_gate)
            return EvaluateLocked(changeId, deps, out detail);
    }

    /// <summary>Hold an op until parents arrive (or timeout).</summary>
    public void EnqueuePending(BatchId changeId, IReadOnlyList<BatchId> deps, object? payload = null)
    {
        ArgumentNullException.ThrowIfNull(deps);
        if (changeId.IsZero)
            throw new ArgumentException("ChangeId cannot be zero.", nameof(changeId));
        lock (_gate)
        {
            // Duplicate pending IDs are common on independent mesh paths; keep
            // that fast path allocation-free. Snapshot and validate only new entries.
            if (_known.Contains(changeId) || _lost.Contains(changeId) || _pending.ContainsKey(changeId))
                return;
            var depArr = deps.ToArray();
            for (int i = 0; i < depArr.Length; i++)
                if (depArr[i].IsZero)
                    throw new ArgumentException("Causal dependency ID cannot be zero.", nameof(deps));
            if (depArr.Any(_lost.Contains))
            {
                _ = PropagateLostLocked(changeId);
                return;
            }
            _pending[changeId] = new PendingCausalOp
            {
                ChangeId = changeId,
                Deps = depArr,
                EnqueuedUtcMs = _utcNowMs(),
                Payload = payload,
            };
            AddPendingEdgesLocked(changeId, depArr);
        }
    }

    /// <summary>
    /// Release pending ops whose dependencies are ready, timed out, or cyclic.
    /// Ready ops are not marked known until the caller confirms successful application.
    /// </summary>
    public List<(PendingCausalOp Op, CausalOutcome Outcome)> Drain()
    {
        var released = new List<(PendingCausalOp, CausalOutcome)>();
        lock (_gate)
        {
            long now = _utcNowMs();
            // Snapshot keys because dropping one lost/cyclic/timed-out parent can
            // transitively remove other pending entries during this pass.
            var candidates = _pending.Keys.ToArray();
            foreach (var cid in candidates)
            {
                if (!_pending.TryGetValue(cid, out var op))
                    continue;

                CausalOutcome outcome;
                bool lostDependency = false;
                for (int i = 0; i < op.Deps.Length; i++)
                {
                    if (_lost.Contains(op.Deps[i]))
                    {
                        lostDependency = true;
                        break;
                    }
                }

                if (lostDependency)
                    outcome = CausalOutcome.LostDep;
                else
                {
                    var parents = _edges.GetValueOrDefault(cid);
                    if (parents is not null && WouldCycle(cid, parents))
                        outcome = CausalOutcome.Cycle;
                    else
                    {
                        bool allKnown = true;
                        for (int i = 0; i < op.Deps.Length; i++)
                            if (!_known.Contains(op.Deps[i])) { allKnown = false; break; }

                        // Timeout applies only while causal parents are still missing.
                        // A released batch may be requeued after a normal stream gate
                        // failure; once its parents are known it must not later become
                        // LostDep merely because that unrelated gate stayed blocked.
                        outcome = allKnown
                            ? CausalOutcome.Ready
                            : now >= op.EnqueuedUtcMs &&
                              (double)now - op.EnqueuedUtcMs > _depTimeout.TotalMilliseconds
                                ? CausalOutcome.TimedOut
                                : CausalOutcome.Pending;
                    }
                }

                if (outcome == CausalOutcome.Pending)
                    continue;

                RemovePendingLocked(cid);
                released.Add((op, outcome));
                if (outcome != CausalOutcome.Ready)
                {
                    foreach (var dependent in PropagateLostLocked(cid))
                        released.Add((dependent, CausalOutcome.LostDep));
                }
            }
        }
        return released;
    }

    /// <summary>Restore a released item when its held batch could not be applied.</summary>
    internal void RequeuePending(PendingCausalOp op)
    {
        ArgumentNullException.ThrowIfNull(op);
        lock (_gate)
        {
            if (_known.Contains(op.ChangeId) || _lost.Contains(op.ChangeId)) return;
            if (_pending.ContainsKey(op.ChangeId))
                RemovePendingLocked(op.ChangeId);
            if (op.Deps.Any(_lost.Contains))
            {
                _ = PropagateLostLocked(op.ChangeId);
                return;
            }
            _pending[op.ChangeId] = op;
            AddPendingEdgesLocked(op.ChangeId, op.Deps);
        }
    }

    /// <summary>Mark specific parent changeIds as permanently lost (KЗ3) and drop dependents.</summary>
    public List<PendingCausalOp> MarkLost(BatchId lostParent)
    {
        if (lostParent.IsZero)
            throw new ArgumentException("ChangeId cannot be zero.", nameof(lostParent));
        lock (_gate)
        {
            if (_known.Contains(lostParent))
                return new List<PendingCausalOp>();

            var dropped = new List<PendingCausalOp>();
            if (_pending.TryGetValue(lostParent, out var root))
            {
                RemovePendingLocked(lostParent);
                dropped.Add(root);
            }
            dropped.AddRange(PropagateLostLocked(lostParent));
            return dropped;
        }
    }

    /// <summary>Batch gate with typed change ids and atomic intra-batch dependencies.</summary>
    public CausalOutcome EvaluateBatch(
        IReadOnlyList<(BatchId ChangeId, IReadOnlyList<BatchId>? Deps)> members,
        out string? detail)
    {
        ArgumentNullException.ThrowIfNull(members);
        detail = null;
        CausalOutcome worst = CausalOutcome.Ready;
        for (int i = 0; i < members.Count; i++)
        {
            var (changeId, deps) = members[i];
            if (changeId.IsZero)
                throw new InvalidDataException("Causal batch ChangeId cannot be zero.");
            if (deps is not null)
                for (int j = 0; j < deps.Count; j++)
                    if (deps[j].IsZero)
                        throw new InvalidDataException("Causal dependency ID cannot be zero.");
        }
        lock (_gate)
        {
            // Earlier members satisfy their intra-batch dependents at the same
            // atomic commit. Forward references cannot be satisfied in stream order.
            var positions = new Dictionary<BatchId, int>(members.Count);
            for (int i = 0; i < members.Count; i++)
            {
                if (!positions.TryAdd(members[i].ChangeId, i))
                    throw new InvalidDataException("Duplicate ChangeId in causal batch.");
            }

            for (int i = 0; i < members.Count; i++)
            {
                var (cid, rawDeps) = members[i];
                var deps = rawDeps ?? Array.Empty<BatchId>();
                List<BatchId>? externalMissing = null;
                CausalOutcome memberOutcome = CausalOutcome.Ready;
                string? memberDetail = null;

                if (_lost.Contains(cid))
                {
                    memberOutcome = CausalOutcome.LostDep;
                    memberDetail = Key(cid);
                }
                else if (!_known.Contains(cid))
                {
                    for (int j = 0; j < deps.Count; j++)
                    {
                        var dep = deps[j];
                        if (dep == cid)
                        {
                            memberOutcome = CausalOutcome.Cycle;
                            memberDetail = Key(dep);
                            break;
                        }
                        if (_lost.Contains(dep))
                        {
                            memberOutcome = CausalOutcome.LostDep;
                            memberDetail = Key(dep);
                            break;
                        }
                        if (_known.Contains(dep))
                            continue;
                        if (positions.TryGetValue(dep, out int parentIndex))
                        {
                            if (parentIndex >= i)
                            {
                                memberOutcome = CausalOutcome.Cycle;
                                memberDetail = Key(dep);
                                break;
                            }
                            continue;
                        }
                        (externalMissing ??= new List<BatchId>()).Add(dep);
                    }
                }

                if (memberOutcome == CausalOutcome.Ready && externalMissing is not null)
                    memberOutcome = EvaluateLocked(cid, externalMissing, out memberDetail);

                if (memberOutcome > worst)
                {
                    worst = memberOutcome;
                    detail = memberDetail;
                }
            }
        }
        return worst;
    }

    private void AddPendingEdgesLocked(BatchId child, IReadOnlyList<BatchId> dependencies)
    {
        var parents = new HashSet<BatchId>(dependencies);
        _edges[child] = parents;
        foreach (var parent in parents)
        {
            if (!_dependents.TryGetValue(parent, out var children))
                _dependents[parent] = children = new HashSet<BatchId>();
            children.Add(child);
        }
    }

    private void RemovePendingLocked(BatchId child)
    {
        _pending.Remove(child);
        if (!_edges.Remove(child, out var parents))
            return;
        foreach (var parent in parents)
        {
            if (!_dependents.TryGetValue(parent, out var children))
                continue;
            children.Remove(child);
            if (children.Count == 0)
                _dependents.Remove(parent);
        }
    }

    // Mark root permanently unavailable and recursively discard every pending
    // operation that depends on it. The reverse index keeps this O(V + E).
    private List<PendingCausalOp> PropagateLostLocked(BatchId root)
    {
        var dropped = new List<PendingCausalOp>();
        var queue = new Queue<BatchId>();
        var visited = new HashSet<BatchId>();
        queue.Enqueue(root);
        while (queue.Count > 0)
        {
            var lost = queue.Dequeue();
            if (!visited.Add(lost))
                continue;
            _lost.Add(lost);

            if (!_dependents.TryGetValue(lost, out var children) || children.Count == 0)
                continue;
            foreach (var child in children.ToArray())
            {
                if (!_pending.TryGetValue(child, out var op))
                    continue;
                RemovePendingLocked(child);
                dropped.Add(op);
                queue.Enqueue(child);
            }
        }
        return dropped;
    }

    private bool WouldCycle(BatchId child, HashSet<BatchId> parents)
    {
        var stack = new Stack<BatchId>(parents);
        var seen = new HashSet<BatchId>();
        while (stack.Count > 0)
        {
            var n = stack.Pop();
            if (n == child) return true;
            if (!seen.Add(n)) continue;
            if (_edges.TryGetValue(n, out var next))
            {
                foreach (var p in next)
                    stack.Push(p);
            }
        }
        return false;
    }
}

/// <summary>Protocol error code for causal dep failures (§12 error table).</summary>
public static class CausalError
{
    /// <summary>Constant causal dep missing.</summary>
    public const ushort CausalDepMissing = 0x0012; // CAUSAL_DEP_MISSING
}
