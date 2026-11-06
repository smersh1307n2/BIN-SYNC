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
    private readonly HashSet<BatchId> _known = new();
    private readonly Dictionary<BatchId, PendingCausalOp> _pending = new();
    private readonly Dictionary<BatchId, HashSet<BatchId>> _edges = new(); // child → parents

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
        lock (_gate)
        {
            _known.Add(changeId);
            _edges.Remove(changeId);
        }
    }

    /// <summary>True when this change id is already marked applied in the tracker.</summary>
    public bool IsKnown(BatchId changeId)
    {
        lock (_gate) return _known.Contains(changeId);
    }

    /// <summary>Evaluate with typed change ids.</summary>
    public CausalOutcome Evaluate(BatchId changeId, IReadOnlyList<BatchId>? deps, out string? detail)
    {
        detail = null;
        deps ??= Array.Empty<BatchId>();
        lock (_gate)
        {
            if (_known.Contains(changeId) && deps.Count == 0)
                return CausalOutcome.Ready;

            foreach (var d in deps)
            {
                if (d == changeId)
                {
                    detail = "self-dep";
                    return CausalOutcome.Cycle;
                }
            }

            var parentSet = deps is HashSet<BatchId> hs ? hs : new HashSet<BatchId>(deps);
            if (WouldCycle(changeId, parentSet))
            {
                detail = "cycle";
                return CausalOutcome.Cycle;
            }

            foreach (var d in deps)
            {
                if (!_known.Contains(d))
                {
                    detail = Key(d); // missing parent hex for diagnostics / tests
                    return CausalOutcome.Pending;
                }
            }
            return CausalOutcome.Ready;
        }
    }

    /// <summary>Hold an op until parents arrive (or timeout).</summary>
    public void EnqueuePending(BatchId changeId, IReadOnlyList<BatchId> deps, object? payload = null)
    {
        lock (_gate)
        {
            var depArr = deps.ToArray();
            _pending[changeId] = new PendingCausalOp
            {
                ChangeId = changeId,
                Deps = depArr,
                EnqueuedUtcMs = _utcNowMs(),
                Payload = payload,
            };
            _edges[changeId] = new HashSet<BatchId>(depArr);
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
            var done = new List<BatchId>();
            foreach (var (cid, op) in _pending)
            {
                if (now - op.EnqueuedUtcMs > _depTimeout.TotalMilliseconds)
                {
                    released.Add((op, CausalOutcome.TimedOut));
                    done.Add(cid);
                    continue;
                }

                var parents = _edges.GetValueOrDefault(cid) ?? new HashSet<BatchId>();
                if (WouldCycle(cid, parents))
                {
                    released.Add((op, CausalOutcome.Cycle));
                    done.Add(cid);
                    continue;
                }

                bool allKnown = true;
                for (int i = 0; i < op.Deps.Length; i++)
                    if (!_known.Contains(op.Deps[i])) { allKnown = false; break; }
                if (allKnown)
                {
                    released.Add((op, CausalOutcome.Ready));
                    done.Add(cid);
                }
            }
            foreach (var cid in done)
            {
                _pending.Remove(cid);
                _edges.Remove(cid);
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
            if (_known.Contains(op.ChangeId)) return;
            _pending[op.ChangeId] = op;
            _edges[op.ChangeId] = new HashSet<BatchId>(op.Deps);
        }
    }

    /// <summary>Mark specific parent changeIds as permanently lost (KЗ3) and drop dependents.</summary>
    public List<PendingCausalOp> MarkLost(BatchId lostParent)
    {
        var dropped = new List<PendingCausalOp>();
        lock (_gate)
        {
            var toRemove = new List<BatchId>();
            foreach (var (cid, op) in _pending)
            {
                bool hit = false;
                for (int i = 0; i < op.Deps.Length; i++)
                    if (op.Deps[i] == lostParent) { hit = true; break; }
                if (hit)
                {
                    dropped.Add(op);
                    toRemove.Add(cid);
                }
            }
            foreach (var cid in toRemove)
            {
                _pending.Remove(cid);
                _edges.Remove(cid);
            }
        }
        return dropped;
    }

    /// <summary>Batch gate with typed change ids.</summary>
    public CausalOutcome EvaluateBatch(
        IReadOnlyList<(BatchId ChangeId, IReadOnlyList<BatchId>? Deps)> members,
        out string? detail)
    {
        detail = null;
        CausalOutcome worst = CausalOutcome.Ready;
        foreach (var (cid, deps) in members)
        {
            var o = Evaluate(cid, deps, out var d);
            if (o != CausalOutcome.Ready)
            {
                detail = d;
                if (o > worst) worst = o;
            }
        }
        return worst;
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
