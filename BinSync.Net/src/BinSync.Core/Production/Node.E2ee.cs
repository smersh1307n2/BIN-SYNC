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
    private E2eeKeyRing? _e2eeKeys;

    /// <summary>Optional E2EE key ring. Access is serialized with sealed field operations.</summary>
    public E2eeKeyRing? E2eeKeys
    {
        get { lock (_lockMeta) { ThrowIfDisposed(); return _e2eeKeys; } }
        set { lock (_lockMeta) _e2eeKeys = value; }
    }

    /// <summary>
    /// Seal <paramref name="plaintext"/> under the namespace key and write ciphertext
    /// (ChaCha20-Poly1305). Uses current key epoch from <see cref="E2eeKeys"/>.
    /// </summary>
    public bool WriteFieldSealed(
        int namespaceId, RecordId recordId, int fieldId, byte[] plaintext, Hlc? hlc = null)
    {
        ValidateNamespaceId(namespaceId);
        ValidateFieldId(fieldId);
        ArgumentNullException.ThrowIfNull(plaintext);
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            ThrowIfDisposed();
            var ring = E2eeKeys
                ?? throw new InvalidOperationException("E2eeKeys is not configured");
            if (!ring.TryGet(namespaceId, out var key, out var epoch))
                throw new InvalidOperationException($"no E2EE key for namespace {namespaceId}");

            try
            {
                Storage.TryGetValue(namespaceId, out var store);
                Record? current = null;
                bool exists = store is not null && store.TryGetValue(recordId, out current);
                var rec = exists ? Clone(current!) : new Record(recordId, (ushort)namespaceId);

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

                rec.State[(ushort)fieldId] = winner;
                var entry = PrepareOutboxEntryInternal(
                    namespaceId, recordId, op, NodeId.ToArray(), suppressRestoredEcho: false)
                    ?? throw new InvalidOperationException("A changed sealed write must produce an Outbox entry.");
                // Use the same stage → durable WAL append → publish sequence as ordinary writes.
                CommitLocalMutationInternal(entry, rec, applyToStorage: true, preserveRecordIdentity: true);
            }
            finally
            {
                System.Security.Cryptography.CryptographicOperations.ZeroMemory(key);
            }
        }

        RaiseFieldWritten(namespaceId, recordId, fieldId);
        return true;
    }

    /// <summary>
    /// Read a field and open ciphertext with the namespace key.
    /// Returns null when the field is absent.
    /// </summary>
    public byte[]? ReadFieldOpen(int namespaceId, RecordId recordId, int fieldId)
    {
        ValidateNamespaceId(namespaceId);
        ValidateFieldId(fieldId);
        lock (_lockStorage) lock (_lockMeta)
        {
            ThrowIfDisposed();
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

}
