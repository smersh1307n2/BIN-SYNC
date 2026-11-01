// Scenario harness — exercises realistic multi-step flows with logging.
// Exit 0 only if all scenarios pass.
//
//   bash scripts/vendor-ed25519-from-ztsharp.sh
//   dotnet run --project samples/ScenarioHarness -c Release
using System.Text;
using BinSync;

const int Ns = 1;
var log = new Log();
int failed = 0;

failed += Run("S1_TwoNode_Bidirectional", S1_TwoNode_Bidirectional);
failed += Run("S2_ThreeNode_Relay", S2_ThreeNode_Relay);
failed += Run("S3_Lww_Conflict", S3_Lww_Conflict);
failed += Run("S4_Tomb_Propagates", S4_Tomb_Propagates);
failed += Run("S5_Persist_Resume", S5_Persist_Resume);
failed += Run("S6_Sneakernet_ExportImport", S6_Sneakernet_ExportImport);
failed += Run("S7_Idempotent_Replay", S7_Idempotent_Replay);
failed += Run("S8_Concurrent_Writers", S8_Concurrent_Writers);

log.Info($"DONE failed={failed}");
return failed == 0 ? 0 : 1;

int Run(string name, Func<Log, bool> scenario)
{
    log.Info($"--- {name} ---");
    try
    {
        bool ok = scenario(log);
        log.Info(ok ? $"{name}: PASS" : $"{name}: FAIL");
        return ok ? 0 : 1;
    }
    catch (Exception ex)
    {
        log.Error($"{name}: EXCEPTION {ex.GetType().Name}: {ex.Message}");
        log.Error(ex.StackTrace ?? "");
        return 1;
    }
}

// --- scenarios ---

static bool S1_TwoNode_Bidirectional(Log log)
{
    var a = N(1); var b = N(2);
    Transport.MakeSession(a, b);
    a.WriteField(Ns, Rid(100), 0x101, Val("a→b"));
    b.WriteField(Ns, Rid(200), 0x101, Val("b→a"));
    int n = Transport.SyncRound(a, b, Ns);
    log.Info($"SyncRound batches={n}");
    return Has(b, Rid(100), "a→b") && Has(a, Rid(200), "b→a");
}

static bool S2_ThreeNode_Relay(Log log)
{
    // A writes; sync A↔B then B↔C — C should see A's write via B (relay path).
    var a = N(1); var b = N(2); var c = N(3);
    Transport.MakeSession(a, b);
    Transport.MakeSession(b, c);
    a.WriteField(Ns, Rid(300), 0x101, Val("from-a"));
    int ab = Transport.SyncRound(a, b, Ns);
    int bc = Transport.SyncRound(b, c, Ns);
    log.Info($"A↔B={ab} B↔C={bc}");
    bool ok = Has(b, Rid(300), "from-a") && Has(c, Rid(300), "from-a");
    if (!ok)
        log.Error($"relay missing: b={Has(b, Rid(300), "from-a")} c={Has(c, Rid(300), "from-a")}");
    return ok;
}

static bool S3_Lww_Conflict(Log log)
{
    var a = N(1); var b = N(2);
    Transport.MakeSession(a, b);
    // Same record/field; B has later HLC → B should win after sync.
    a.WriteField(Ns, Rid(400), 0x101, Val("older"), new Hlc(1_000_000, 0));
    b.WriteField(Ns, Rid(400), 0x101, Val("newer"), new Hlc(2_000_000, 0));
    Transport.SyncRound(a, b, Ns);
    string? va = Get(a, Rid(400));
    string? vb = Get(b, Rid(400));
    log.Info($"after LWW a='{va}' b='{vb}'");
    return va == "newer" && vb == "newer";
}

static bool S4_Tomb_Propagates(Log log)
{
    var a = N(1); var b = N(2);
    Transport.MakeSession(a, b);
    a.WriteField(Ns, Rid(500), 0x101, Val("live"));
    Transport.SyncRound(a, b, Ns);
    if (!Has(b, Rid(500), "live")) return false;
    a.TombField(Ns, Rid(500), 0x101);
    Transport.SyncRound(a, b, Ns);
    bool aGone = !Has(a, Rid(500), "live");
    bool bGone = !Has(b, Rid(500), "live");
    log.Info($"tomb aGone={aGone} bGone={bGone}");
    return aGone && bGone;
}

static bool S5_Persist_Resume(Log log)
{
    var dir = Path.Combine(Path.GetTempPath(), "binsync-harness-" + Guid.NewGuid().ToString("N"));
    Directory.CreateDirectory(dir);
    try
    {
        var a = N(1); var b = N(2);
        Transport.MakeSession(a, b);
        a.WriteField(Ns, Rid(600), 0x101, Val("before-save"));
        Transport.SyncRound(a, b, Ns);

        var path = Path.Combine(dir, "a.state");
        NodePersistence.Save(a, path);
        var a2 = NodePersistence.Load(path);
        log.Info($"loaded a2 NodeId={a2.NodeId}");

        // Resume session with live B (maps from snapshot + re-handshake).
        Transport.MakeSession(a2, b);
        b.WriteField(Ns, Rid(601), 0x101, Val("after-load"));
        int n = Transport.SyncRound(b, a2, Ns);
        log.Info($"resume SyncRound={n}");
        return Has(a2, Rid(600), "before-save") && Has(a2, Rid(601), "after-load");
    }
    finally
    {
        try { Directory.Delete(dir, true); } catch { /* best effort */ }
    }
}

static bool S6_Sneakernet_ExportImport(Log log)
{
    var dir = Path.Combine(Path.GetTempPath(), "binsync-sneak-" + Guid.NewGuid().ToString("N"));
    Directory.CreateDirectory(dir);
    try
    {
        var a = N(1); var b = N(2);
        Transport.MakeSession(a, b);
        a.WriteField(Ns, Rid(700), 0x101, Val("sneaker"));
        var path = Path.Combine(dir, "delta.bin");
        int exported = Transport.ExportDelta(a, Ns, b.NodeId.ToArray(), path);
        log.Info($"exported frames={exported} bytes={new FileInfo(path).Length}");
        int imported = Transport.ImportDelta(b, path, a.NodeId.ToArray());
        log.Info($"imported={imported}");
        return exported > 0 && imported > 0 && Has(b, Rid(700), "sneaker");
    }
    finally
    {
        try { Directory.Delete(dir, true); } catch { }
    }
}

static bool S7_Idempotent_Replay(Log log)
{
    var a = N(1); var b = N(2);
    Transport.MakeSession(a, b);
    a.WriteField(Ns, Rid(800), 0x101, Val("once"));
    var batch = a.CreateDeltaBatch(b.NodeId, Ns)
        ?? throw new InvalidOperationException("null batch");
    var r1 = b.ApplyBatch(batch, a.NodeId);
    var r2 = b.ApplyBatch(batch, a.NodeId);
    log.Info($"first Applied={r1.Applied} Duplicate={r1.Duplicate}; second Applied={r2.Applied} Duplicate={r2.Duplicate}");
    return r1.Applied > 0 && (r2.Duplicate || r2.Applied == 0) && Has(b, Rid(800), "once");
}

static bool S8_Concurrent_Writers(Log log)
{
    var a = N(1); var b = N(2);
    Transport.MakeSession(a, b);
    const int n = 40;
    Exception? err = null;
    var t1 = Task.Run(() =>
    {
        try
        {
            for (int i = 0; i < n; i++)
                a.WriteField(Ns, Rid(900 + i), 0x101, Val($"a{i}"));
        }
        catch (Exception e) { Interlocked.CompareExchange(ref err, e, null); }
    });
    var t2 = Task.Run(() =>
    {
        try
        {
            for (int i = 0; i < n; i++)
                b.WriteField(Ns, Rid(1000 + i), 0x101, Val($"b{i}"));
        }
        catch (Exception e) { Interlocked.CompareExchange(ref err, e, null); }
    });
    Task.WaitAll(t1, t2);
    if (err is not null)
    {
        log.Error($"concurrent write: {err.Message}");
        return false;
    }
    int rounds = Transport.SyncRound(a, b, Ns);
    log.Info($"concurrent SyncRound={rounds}");
    int missing = 0;
    for (int i = 0; i < n; i++)
    {
        if (!Has(b, Rid(900 + i), $"a{i}")) missing++;
        if (!Has(a, Rid(1000 + i), $"b{i}")) missing++;
    }
    log.Info($"missing={missing}");
    return missing == 0;
}

// --- helpers ---

static Node N(int seed) => new(nodeId: Rid(seed), epoch: 1);

static byte[] Rid(int n)
{
    var b = new byte[16];
    BitConverter.GetBytes(n).CopyTo(b, 0);
    return b;
}

static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);

static bool Has(Node node, byte[] rid, string expected)
{
    var g = Get(node, rid);
    return g == expected;
}

static string? Get(Node node, byte[] rid)
{
    if (!node.Storage.TryGetValue(Ns, out var store)) return null;
    if (!store.TryGetValue(rid, out var rec)) return null;
    if (!rec.State.TryGetValue(0x101, out var fs)) return null;
    return Encoding.UTF8.GetString(fs.Value);
}

sealed class Log
{
    public void Info(string m) => Console.WriteLine($"[INFO] {m}");
    public void Error(string m) => Console.Error.WriteLine($"[ERR ] {m}");
}
