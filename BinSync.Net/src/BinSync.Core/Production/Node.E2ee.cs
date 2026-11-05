// Opt-in E2EE WriteField / ReadField (§14.5).
// Plain WriteField is unchanged — ciphertext only when E2eeKeys is set and *Sealed APIs used.
namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    /// <summary>
    /// When set, sealed field APIs use the ring to seal and open values.
    /// Default WriteField never consults this.
    /// </summary>
    public E2eeKeyRing? E2eeKeys { get; set; }

    /// <summary>
    /// Seal <paramref name="plaintext"/> under the namespace key and write ciphertext
    /// (ChaCha20-Poly1305). Uses current key epoch from <see cref="E2eeKeys"/>.
    /// </summary>
    public bool WriteFieldSealed(
        int namespaceId, RecordId recordId, int fieldId, byte[] plaintext, Hlc? hlc = null)
    {
        ArgumentNullException.ThrowIfNull(plaintext);
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            var ring = E2eeKeys
                ?? throw new InvalidOperationException("E2eeKeys is not configured");
            if (!ring.TryGet(namespaceId, out var key, out var epoch))
                throw new InvalidOperationException($"no E2EE key for namespace {namespaceId}");

            try
            {
                var store = NsInternal(namespaceId);
                bool exists = store.TryGetValue(recordId, out var current);
                var rec = exists ? current! : new Record(recordId, (ushort)namespaceId);

                var h = hlc ?? Clock.SendTick();
                Span<byte> ridBuf = stackalloc byte[16];
                recordId.WriteTo(ridBuf);
                Span<byte> authorBuf = stackalloc byte[16];
                NodeId.WriteTo(authorBuf);
                var sealedValue = E2ee.SealField(
                    key, ridBuf, (ushort)fieldId, h.Wall, h.Logical, authorBuf, epoch, plaintext);

                var fs = new FieldState(sealedValue, h, NodeId, KeyEpoch: epoch);
                var (winner, changed) = Merge.MergeFieldWithPolicy(
                    rec.State.TryGetValue((ushort)fieldId, out var old) ? old : null,
                    fs,
                    MergePolicy.LwwField);
                if (!changed)
                    return false;

                EnsureLocalWriteCapacity(namespaceId, isNewRecord: !exists);
                var op = new OpFieldWrite
                {
                    FieldId = (ushort)fieldId,
                    KeyEpoch = epoch,
                    Wall = h.Wall,
                    Logical = h.Logical,
                    Origin = 1,
                    AuthorNodeId = NodeId,
                    Value = sealedValue,
                };

                if (!exists)
                    store.Add(recordId, rec);
                rec.State[(ushort)fieldId] = winner;
                PushOutboxInternal(namespaceId, recordId, op, NodeId.ToArray());
                return true;
            }
            finally
            {
                System.Security.Cryptography.CryptographicOperations.ZeroMemory(key);
            }
        }
    }

    /// <summary>Sealed write with legacy byte[] record id.</summary>
    public bool WriteFieldSealed(
        int namespaceId, byte[] recordId, int fieldId, byte[] plaintext, Hlc? hlc = null) =>
        WriteFieldSealed(namespaceId, RecordId.From(recordId), fieldId, plaintext, hlc);

    /// <summary>
    /// Read a field and open ciphertext with the namespace key.
    /// Returns null when the field is absent.
    /// </summary>
    public byte[]? ReadFieldOpen(int namespaceId, RecordId recordId, int fieldId)
    {
        lock (_lockStorage) lock (_lockMeta)
        {
            var ring = E2eeKeys
                ?? throw new InvalidOperationException("E2eeKeys is not configured");
            if (!ring.TryGet(namespaceId, out var key, out _))
                throw new InvalidOperationException($"no E2EE key for namespace {namespaceId}");

            try
            {
                if (!Storage.TryGetValue(namespaceId, out var map))
                    return null;
                if (!map.TryGetValue(recordId, out var rec))
                    return null;
                if (!rec.State.TryGetValue((ushort)fieldId, out var fs))
                    return null;

                Span<byte> ridBuf = stackalloc byte[16];
                recordId.WriteTo(ridBuf);
                Span<byte> authorBuf = stackalloc byte[16];
                fs.AuthorNodeId.WriteTo(authorBuf);
                return E2ee.OpenField(
                    key, ridBuf, (ushort)fieldId,
                    fs.Timestamp.Wall, fs.Timestamp.Logical,
                    authorBuf, fs.KeyEpoch, fs.Value.AsSpan());
            }
            finally
            {
                System.Security.Cryptography.CryptographicOperations.ZeroMemory(key);
            }
        }
    }

    /// <summary>Open field with legacy byte[] record id.</summary>
    public byte[]? ReadFieldOpen(int namespaceId, byte[] recordId, int fieldId) =>
        ReadFieldOpen(namespaceId, RecordId.From(recordId), fieldId);

    /// <summary>
    /// True when the stored field opens successfully under the configured key.
    /// </summary>
    public bool TryOpenField(
        int namespaceId, RecordId recordId, int fieldId, out byte[]? plaintext)
    {
        plaintext = null;
        try
        {
            plaintext = ReadFieldOpen(namespaceId, recordId, fieldId);
            return plaintext is not null;
        }
        catch (System.Security.Cryptography.CryptographicException)
        {
            return false;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>Try-open with legacy byte[] record id.</summary>
    public bool TryOpenField(int namespaceId, byte[] recordId, int fieldId, out byte[]? plaintext) =>
        TryOpenField(namespaceId, RecordId.From(recordId), fieldId, out plaintext);
}
