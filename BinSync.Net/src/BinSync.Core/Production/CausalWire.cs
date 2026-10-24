// Optional on-wire causal deps trailer (§16.19) appended after the standard batch payload.
// Does not alter EncodeRecordsArray / recordsRoot — trailer is outside the digest domain.
namespace BinSync;

/// <summary>
/// Wire format for optional causal metadata carried with a DELTA_BATCH.
/// Layout after the standard <see cref="Node.EncodeBatchPayload"/> bytes:
///   magic u32 LE = 0x31444343 ("CCD1") | entryCount varint |
///   per entry: changeId b128 | depCount varint | dep[i] b128…
/// Present only when at least one entry declares CausalDeps or a non-empty ChangeId
/// that the sender wants the receiver to gate on.
/// </summary>
public static class CausalWire
{
    public const uint Magic = 0x31444343; // "CCD1" LE
    /// <summary>Batch.Flags bit: payload is followed by a causal trailer.</summary>
    public const int FlagHasCausalTrailer = 0x20;

    public static bool HasDeps(IReadOnlyList<OutboxEntry> entries) =>
        entries.Any(e => e.CausalDeps.Count > 0);

    public static byte[] EncodeTrailer(IReadOnlyList<OutboxEntry> entries)
    {
        var w = new Writer();
        w.U32(Magic);
        w.Varint((ulong)entries.Count);
        foreach (var e in entries)
        {
            w.B128(e.ChangeId.Length == 16 ? e.ChangeId : Codec.CanonicalB128(e.ChangeId));
            w.Varint((ulong)e.CausalDeps.Count);
            foreach (var d in e.CausalDeps)
                w.B128(d.Length == 16 ? d : Codec.CanonicalB128(d));
        }
        return w.Value();
    }

    /// <summary>
    /// If <paramref name="payload"/> ends with a valid trailer, populate ChangeId/CausalDeps
    /// on <paramref name="entries"/> (must already be decoded from the body) and return the
    /// body length. Otherwise return payload.Length (no trailer).
    /// </summary>
    public static int TryApplyTrailer(byte[] payload, List<OutboxEntry> entries)
    {
        if (payload.Length < 4) return payload.Length;
        // Scan from the end is hard; instead: if Flags said so, body is prefix.
        // For auto-detect: try parse from offset = payload.Length - estimate, or linear search magic.
        for (int start = 0; start + 4 <= payload.Length; start++)
        {
            uint m = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(start));
            if (m != Magic) continue;
            try
            {
                var r = new Reader(payload, start + 4);
                int n = (int)r.Varint();
                if (n != entries.Count) continue;
                for (int i = 0; i < n; i++)
                {
                    var cid = r.B128();
                    int dc = (int)r.Varint();
                    var deps = new List<byte[]>(dc);
                    for (int j = 0; j < dc; j++)
                        deps.Add(r.B128());
                    entries[i].ChangeId = cid;
                    entries[i].CausalDeps = deps;
                }
                if (r.Pos != payload.Length) continue; // must consume to EOF
                return start; // body length
            }
            catch
            {
                // not a valid trailer at this offset
            }
        }
        return payload.Length;
    }

    /// <summary>Standard batch payload + optional causal trailer.</summary>
    public static byte[] EncodeWithTrailer(byte[] standardPayload, IReadOnlyList<OutboxEntry> entries)
    {
        if (!HasDeps(entries)) return standardPayload;
        var trailer = EncodeTrailer(entries);
        var combined = new byte[standardPayload.Length + trailer.Length];
        standardPayload.CopyTo(combined, 0);
        trailer.CopyTo(combined, standardPayload.Length);
        return combined;
    }
}
