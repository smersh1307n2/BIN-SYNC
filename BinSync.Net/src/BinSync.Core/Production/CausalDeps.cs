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
    public required byte[] ChangeId { get; init; }
    public required byte[][] Deps { get; init; }
    public required long EnqueuedUtcMs { get; init; }
    public object? Payload { get; init; }
}

/// <summary>
/// Tracks known changeIds, quarantines ops with missing parents, detects cycles,
/// and expires timed-out waits. Thread-safe.
/// </summary>
public sealed class CausalTracker
{
    private readonly object _gate = new();
    private readonly HashSet<string> _known = new(StringComparer.Ordinal);
    private readonly Dictionary<string, PendingCausalOp> _pending = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<string>> _edges = new(StringComparer.Ordinal); // child → parents

    /// <summary>How long a missing dep may stay pending before KЗ1 timeout.</summary>
    public TimeSpan DepTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Clock for tests; defaults to UTC now.</summary>
    public Func<long> UtcNowMs { get; set; } = () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public int KnownCount { get { lock (_gate) return _known.Count; } }
    public int PendingCount { get { lock (_gate) return _pending.Count; } }

    public static string Key(byte[] id) => Convert.ToHexString(id);

    /// <summary>Record that <paramref name="changeId"/> is durably known (applied or authored locally).</summary>
    public void NoteApplied(byte[] changeId)
    {
        lock (_gate)
        {
            _known.Add(Key(changeId));
            // Clear edge from pending graph if any
            _edges.Remove(Key(changeId));
        }
    }

    public bool IsKnown(byte[] changeId)
    {
        lock (_gate) return _known.Contains(Key(changeId));
    }

    /// <summary>
    /// Evaluate an incoming op. Returns Ready when all deps are known and no cycle;
    /// otherwise Pending / Cycle / TimedOut / LostDep. Never mutates storage itself.
    /// </summary>
    public CausalOutcome Evaluate(byte[] changeId, IReadOnlyList<byte[]>? deps, out string? detail)
    {
        detail = null;
        deps ??= Array.Empty<byte[]>();
        lock (_gate)
        {
            var cid = Key(changeId);
            if (_known.Contains(cid) && deps.Count == 0)
                return CausalOutcome.Ready; // idempotent re-delivery of applied op

            // Self-dependency is a trivial cycle
            foreach (var d in deps)
            {
                if (Key(d) == cid)
                {
                    detail = "self-dep";
                    return CausalOutcome.Cycle;
                }
            }

            // Build tentative edges for cycle check
            var parentKeys = new HashSet<string>(StringComparer.Ordinal);
            foreach (var d in deps)
                parentKeys.Add(Key(d));

            if (WouldCycle(cid, parentKeys))
            {
                detail = "cycle";
                return CausalOutcome.Cycle;
            }

            var missing = new List<string>();
            foreach (var d in deps)
            {
                var k = Key(d);
                if (!_known.Contains(k))
                    missing.Add(k);
            }

            if (missing.Count == 0)
                return CausalOutcome.Ready;

            detail = string.Join(",", missing);
            return CausalOutcome.Pending;
        }
    }

    /// <summary>Quarantine an op that is Pending. KЗ4: caller must not advance cursors.</summary>
    public void EnqueuePending(byte[] changeId, IReadOnlyList<byte[]> deps, object? payload = null)
    {
        lock (_gate)
        {
            var cid = Key(changeId);
            var depArr = deps.Select(d => (byte[])d.Clone()).ToArray();
            _pending[cid] = new PendingCausalOp
            {
                ChangeId = (byte[])changeId.Clone(),
                Deps = depArr,
                EnqueuedUtcMs = UtcNowMs(),
                Payload = payload,
            };
            _edges[cid] = new HashSet<string>(depArr.Select(Key), StringComparer.Ordinal);
        }
    }

    /// <summary>
    /// After a parent becomes known, return pending ops that are now Ready
    /// (and mark them known). Timed-out entries are returned with TimedOut outcome.
    /// </summary>
    public List<(PendingCausalOp Op, CausalOutcome Outcome)> Drain()
    {
        var released = new List<(PendingCausalOp, CausalOutcome)>();
        lock (_gate)
        {
            long now = UtcNowMs();
            var done = new List<string>();

            foreach (var (cid, op) in _pending)
            {
                // Timeout first (KЗ1)
                if (now - op.EnqueuedUtcMs > DepTimeout.TotalMilliseconds)
                {
                    released.Add((op, CausalOutcome.TimedOut));
                    done.Add(cid);
                    continue;
                }

                if (WouldCycle(cid, _edges.GetValueOrDefault(cid) ?? new HashSet<string>()))
                {
                    released.Add((op, CausalOutcome.Cycle));
                    done.Add(cid);
                    continue;
                }

                bool allKnown = op.Deps.All(d => _known.Contains(Key(d)));
                if (allKnown)
                {
                    _known.Add(cid);
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

    /// <summary>Mark specific parent changeIds as permanently lost (KЗ3) and drop dependents.</summary>
    public List<PendingCausalOp> MarkLost(byte[] lostParent)
    {
        var dropped = new List<PendingCausalOp>();
        var lost = Key(lostParent);
        lock (_gate)
        {
            var toRemove = new List<string>();
            foreach (var (cid, op) in _pending)
            {
                if (op.Deps.Any(d => Key(d) == lost))
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

    /// <summary>
    /// Batch gate (KЗ5): if any op in the set is not Ready, the whole batch must not apply.
    /// Returns Ready only when every member is Ready; otherwise the worst outcome among members.
    /// </summary>
    public CausalOutcome EvaluateBatch(
        IReadOnlyList<(byte[] ChangeId, IReadOnlyList<byte[]>? Deps)> members,
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

    private bool WouldCycle(string child, HashSet<string> parents)
    {
        // DFS: if following parents (and existing edges) we can reach child again → cycle
        var stack = new Stack<string>(parents);
        var seen = new HashSet<string>(StringComparer.Ordinal);
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
    public const ushort CausalDepMissing = 0x0012; // CAUSAL_DEP_MISSING
}
