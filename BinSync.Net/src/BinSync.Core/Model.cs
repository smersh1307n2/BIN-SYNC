// Data model (§7): Record, FieldState, lifecycle (delete-wins), ghosts.
namespace BinSync;

public static class Model
{
    public const ushort LifecycleFieldId = 0x0000;   // §7.2 reserved
    public const ushort FieldResolveId = 0x00FF;     // §8.9 MANUAL resolve field
    public const ushort UserFieldMin = 0x0100;
    public const byte Alive = 0x00;
    public const byte Dead = 0x01;
}

public sealed record class FieldState(
    byte[] Value, Hlc Timestamp, byte[] AuthorNodeId, ushort KeyEpoch = 0);

public class Record
{
    public byte[] RecordId;
    public ushort NamespaceId;
    public Dictionary<ushort, FieldState> State = new();
    public Hlc? LifecycleHlc;
    public byte[]? LifecycleAuthor;
    public byte LifecycleValue = Model.Alive;
    public Dictionary<ushort, FieldState> FieldTombs = new();

    public Record(byte[] recordId, ushort namespaceId)
    {
        RecordId = recordId; NamespaceId = namespaceId;
    }

    public bool IsDead => LifecycleValue == Model.Dead;

    /// Ghost semantics (§7.4): dead records hide fields ordered <= lifecycle.
    public Dictionary<ushort, FieldState> VisibleFields()
    {
        var outp = new Dictionary<ushort, FieldState>();
        foreach (var (fid, fs) in State)
        {
            if (IsDead && !IsGhost(fs, this)) outp[fid] = fs;
            else if (!IsDead) outp[fid] = fs;
        }
        return outp;
    }

    /// [D7]/[У7] with SPEC HARDENING ghost rule (see binsync/model.py docstring):
    /// ghost status only for DEAD records; time dominates, durable author is the
    /// tie-break at exactly equal times.
    public static bool IsGhost(FieldState fs, Record rec)
    {
        if (rec.LifecycleHlc is null || rec.LifecycleAuthor is null) return false;
        if (!rec.IsDead) return false;
        if (fs.Timestamp.Key() != rec.LifecycleHlc.Key())
            return CompareKeys(fs.Timestamp, rec.LifecycleHlc) < 0;
        return HlcOps.CompareHlc(fs.Timestamp, fs.AuthorNodeId,
                                 rec.LifecycleHlc, rec.LifecycleAuthor) <= 0;
    }

    private static int CompareKeys(Hlc a, Hlc b)
    {
        if (a.Wall != b.Wall) return a.Wall < b.Wall ? -1 : 1;
        if (a.Logical != b.Logical) return a.Logical < b.Logical ? -1 : 1;
        return 0;
    }
}
