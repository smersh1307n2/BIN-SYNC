using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>
/// Durable source JournalSeq ↔ BIN-SYNC OutboxSeq bridge for synchronized FLD databases.
/// ChangeId identity remains in BIN-SYNC's global ChangeId ledger; this stores only the
/// cross-coordinate mapping and scan/ACK cursors, never ChangeId or operation payloads.
/// </summary>
public sealed class StorageRelayProvenanceMap
{
    private const byte RootKind = 0x6D;
    private const byte CursorKind = 0x01;
    private const byte MappingKind = 0x02;
    private const int RootLength = 49;
    private const int CursorValueLength = 60;

    private readonly StorageDatabase _storage;
    private readonly object _gate;
    private readonly byte[] _root;
    private static readonly ConditionalWeakTable<StorageDatabase, Dictionary<StreamKey, StreamState>> SharedStates = new();
    private readonly Dictionary<StreamKey, StreamState> _states;

    private readonly record struct StreamKey(string RootHex, int NamespaceId, string AuthorHex);

    private sealed class StreamState
    {
        public ulong ScannedThroughJournalSeq;
        public ulong AckedThroughOutboxSeq;
        public ulong AckedThroughJournalSeq;
        public byte[] MappingSetDigest = new byte[32];
        public readonly List<Mapping> Mappings = new();
    }

    private readonly record struct Mapping(ulong JournalSeq, ulong OutboxSeq);

    internal bool IsBoundTo(string scopeId, NodeId localReplica) =>
        StringComparer.Ordinal.Equals(_scopeId, scopeId) && _localReplica == localReplica;

    internal bool IsBoundTo(StorageMetadataStore metadataStore) =>
        metadataStore.IsBoundTo(_storage);

    public StorageRelayProvenanceMap(StorageDatabase storage, string scopeId, NodeId localReplica)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero)
            throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));

        _gate = StorageDatabaseTransactionGate.For(storage);
        _states = SharedStates.GetValue(storage, static _ => new Dictionary<StreamKey, StreamState>());
        byte[] scopeHash = SHA256.HashData(Encoding.UTF8.GetBytes(scopeId));
        _root = new byte[RootLength];
        _root[0] = RootKind;
        scopeHash.CopyTo(_root, 1);
        localReplica.WriteTo(_root.AsSpan(33));
    }

    /// <summary>Returns the durable FLD scan cursor and last durably translated relay ACK.</summary>
    public StorageRelayProvenanceCursor ReadCursor(int namespaceId, NodeId author)
    {
        ValidateStream(namespaceId, author);
        lock (_gate)
        {
            StreamState state = GetState(namespaceId, author);
            return new StorageRelayProvenanceCursor(
                state.ScannedThroughJournalSeq,
                state.AckedThroughOutboxSeq,
                state.AckedThroughJournalSeq);
        }
    }

    /// <summary>
    /// Atomically persists one complete FLD relay page's sequence mapping and its
    /// ScannedThroughJournalSeq. The caller must first resolve every source ChangeId through
    /// the durable BIN-SYNC ChangeId ledger; ChangeId bytes are deliberately not duplicated
    /// here. Pages must end on whole JournalSeq boundaries; a source journal containing
    /// multiple operations cannot be split across pages. The caller may publish/send the mapped Outbox entries
    /// only after this method returns. JournalSeq and OutboxSeq remain separate domains.
    /// </summary>
    public void AppendPage(
        int namespaceId,
        NodeId author,
        IReadOnlyList<StorageRelayProvenanceEntry> entries,
        ulong scannedThroughJournalSeq)
    {
        ValidateStream(namespaceId, author);
        ArgumentNullException.ThrowIfNull(entries);
        lock (_gate)
        {
            StreamState state = GetState(namespaceId, author);
            if (scannedThroughJournalSeq < state.ScannedThroughJournalSeq)
                throw new InvalidDataException("FLD relay scan cursor cannot regress.");

            ulong priorJournal = state.ScannedThroughJournalSeq;
            ulong priorOutbox = state.Mappings.Count == 0
                ? state.AckedThroughOutboxSeq
                : state.Mappings[^1].OutboxSeq;
            bool firstEntry = true;
            var writes = new List<(byte[] Key, byte[] Value)>(entries.Count + 1);
            byte[] nextDigest = state.MappingSetDigest.ToArray();
            var appended = new List<Mapping>(entries.Count);

            foreach (StorageRelayProvenanceEntry entry in entries)
            {
                if ((firstEntry && entry.JournalSeq <= priorJournal) ||
                    (!firstEntry && entry.JournalSeq < priorJournal) ||
                    entry.JournalSeq > scannedThroughJournalSeq)
                    throw new InvalidDataException("Every new relay mapping must be after the prior scan cursor and at/before the new scan cursor.");
                if (entry.OutboxSeq == 0 || entry.OutboxSeq <= priorOutbox)
                    throw new InvalidDataException("Mapped OutboxSeq values must be non-zero and strictly increasing per author.");

                byte[] key = MappingKey(namespaceId, author, entry.OutboxSeq);
                byte[] value = new byte[8];
                BinaryPrimitives.WriteUInt64LittleEndian(value, entry.JournalSeq);
                XorRowDigest(nextDigest, key, value);
                writes.Add((key, value));
                appended.Add(new Mapping(entry.JournalSeq, entry.OutboxSeq));
                priorJournal = entry.JournalSeq;
                priorOutbox = entry.OutboxSeq;
                firstEntry = false;
            }

            if (entries.Count != 0 && entries[^1].JournalSeq > scannedThroughJournalSeq)
                throw new InvalidDataException("Relay page extends beyond its scan cursor.");

            if (scannedThroughJournalSeq == state.ScannedThroughJournalSeq)
            {
                if (entries.Count == 0) return;
                throw new InvalidDataException("A committed scan cursor cannot be reused for new relay mappings.");
            }

            byte[] cursorKey = CursorKey(namespaceId, author);
            byte[] cursorValue = EncodeCursor(
                scannedThroughJournalSeq,
                state.AckedThroughOutboxSeq,
                state.AckedThroughJournalSeq,
                nextDigest);
            using var batch = _storage.BeginWriteBatch();
            foreach (var write in writes) batch.Put(write.Key, write.Value);
            batch.Put(cursorKey, cursorValue);
            batch.Commit();

            state.Mappings.AddRange(appended);
            state.ScannedThroughJournalSeq = scannedThroughJournalSeq;
            state.MappingSetDigest = nextDigest;
        }
    }

    /// <summary>
    /// Translates BIN-SYNC's per-author safe OutboxSeq into FLD's safe-through
    /// JournalSeq. The first source journal containing an unacknowledged OutboxSeq
    /// is retained in full; all earlier source journals are safe. An empty scanned tail
    /// may advance after an already-positive durable Outbox ACK; zero never authorizes
    /// a positive FLD ACK.
    /// </summary>
    public ulong TranslateSafeOutboxSequence(int namespaceId, NodeId author, ulong safeOutboxSeq)
    {
        ValidateStream(namespaceId, author);
        lock (_gate)
        {
            StreamState state = GetState(namespaceId, author);
            if (safeOutboxSeq < state.AckedThroughOutboxSeq)
                throw new InvalidDataException("Safe Outbox ACK cannot regress below the already committed FLD relay ACK.");
            if (safeOutboxSeq == state.AckedThroughOutboxSeq)
            {
                // Once a positive Outbox watermark is already durable and no mappings
                // remain, newly scanned source journals contain no unacknowledged
                // operations for this author and can be ACKed through the scan cursor.
                // A zero Outbox watermark never authorizes a positive source ACK.
                return safeOutboxSeq != 0 && state.Mappings.Count == 0
                    ? state.ScannedThroughJournalSeq
                    : state.AckedThroughJournalSeq;
            }

            if (state.Mappings.Count == 0 || safeOutboxSeq > state.Mappings[^1].OutboxSeq)
                throw new InvalidDataException("Safe Outbox ACK exceeds the last mapped sequence; scan/resolution must catch up before translating it.");

            int index = FindFirstOutboxSequenceGreaterThan(state.Mappings, safeOutboxSeq);
            if (index < state.Mappings.Count)
                return Math.Max(state.AckedThroughJournalSeq, state.Mappings[index].JournalSeq - 1);
            return state.ScannedThroughJournalSeq;
        }
    }

    /// <summary>
    /// Call only after FLD has durably accepted AcknowledgeRelayBatches(author,
    /// safeThroughJournalSeq). Persists the translated ACK and prunes mapping rows
    /// atomically. If the process crashes before this call, replaying the FLD ACK is
    /// safe and the mappings remain available for retry.
    /// </summary>
    public void CommitAfterFldRelayAck(
        int namespaceId,
        NodeId author,
        ulong safeOutboxSeq,
        ulong safeThroughJournalSeq)
    {
        ValidateStream(namespaceId, author);
        lock (_gate)
        {
            StreamState state = GetState(namespaceId, author);
            ulong expected = TranslateSafeOutboxSequence(namespaceId, author, safeOutboxSeq);
            if (safeThroughJournalSeq != expected)
                throw new InvalidDataException("FLD relay ACK does not match the durable JournalSeq mapping.");
            if (safeOutboxSeq < state.AckedThroughOutboxSeq ||
                safeThroughJournalSeq < state.AckedThroughJournalSeq)
                throw new InvalidDataException("Committed relay ACK watermarks cannot regress.");
            if (safeOutboxSeq == state.AckedThroughOutboxSeq &&
                safeThroughJournalSeq == state.AckedThroughJournalSeq)
                return;

            var deletes = new List<byte[]>();
            byte[] nextDigest = state.MappingSetDigest.ToArray();
            foreach (Mapping mapping in state.Mappings)
            {
                if (mapping.JournalSeq > safeThroughJournalSeq) break;
                byte[] key = MappingKey(namespaceId, author, mapping.OutboxSeq);
                byte[] value = EncodeMappingValue(mapping);
                XorRowDigest(nextDigest, key, value);
                deletes.Add(key);
            }
            byte[] cursorKey = CursorKey(namespaceId, author);
            byte[] cursorValue = EncodeCursor(
                state.ScannedThroughJournalSeq, safeOutboxSeq, safeThroughJournalSeq, nextDigest);

            using var batch = _storage.BeginWriteBatch();
            foreach (byte[] key in deletes) batch.Delete(key);
            batch.Put(cursorKey, cursorValue);
            batch.Commit();

            state.Mappings.RemoveAll(mapping => mapping.JournalSeq <= safeThroughJournalSeq);
            state.AckedThroughOutboxSeq = safeOutboxSeq;
            state.AckedThroughJournalSeq = safeThroughJournalSeq;
            state.MappingSetDigest = nextDigest;
        }
    }

    private StreamState GetState(int namespaceId, NodeId author)
    {
        byte[] authorBytes = new byte[16];
        author.WriteTo(authorBytes);
        string authorHex = Convert.ToHexString(authorBytes);
        string rootHex = Convert.ToHexString(_root);
        var key = new StreamKey(rootHex, namespaceId, authorHex);
        if (_states.TryGetValue(key, out StreamState? cached))
            return cached;

        var state = new StreamState();
        using var snapshot = _storage.OpenSnapshot();
        IReadOnlyList<StorageEntry> cursors = snapshot.ScanPrefix(_root, CursorSuffix(namespaceId, author), 2);
        if (cursors.Count > 1)
            throw new InvalidDataException("Duplicate relay provenance cursor rows.");
        if (cursors.Count == 1)
            DecodeCursor(cursors[0].Value, state);

        byte[] computedDigest = new byte[32];
        using (StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(Join(_root, MappingPrefix(namespaceId, author))))
        {
            const int pageSize = 1024;
            while (true)
            {
                IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
                foreach (StorageEntry row in page)
                {
                    if (row.Key.Length != RootLength + 1 + 2 + 16 + 8 ||
                        row.Value.Length != 8)
                        throw new InvalidDataException("Malformed relay provenance mapping row.");
                    ulong outboxSeq = BinaryPrimitives.ReadUInt64BigEndian(row.Key.AsSpan(RootLength + 1 + 2 + 16, 8));
                    ulong journalSeq = BinaryPrimitives.ReadUInt64LittleEndian(row.Value);
                    if (journalSeq <= state.AckedThroughJournalSeq ||
                        journalSeq > state.ScannedThroughJournalSeq ||
                        outboxSeq <= state.AckedThroughOutboxSeq ||
                        (state.Mappings.Count != 0 &&
                            (outboxSeq <= state.Mappings[^1].OutboxSeq ||
                             journalSeq < state.Mappings[^1].JournalSeq)))
                        throw new InvalidDataException("Relay provenance mapping is outside its durable cursor or not monotonic.");
                    state.Mappings.Add(new Mapping(journalSeq, outboxSeq));
                    XorRowDigest(computedDigest, row.Key, row.Value);
                }
                if (page.Count < pageSize) break;
            }
        }
        if (!CryptographicOperations.FixedTimeEquals(computedDigest, state.MappingSetDigest))
            throw new InvalidDataException("Relay provenance mapping-set checksum does not match its durable cursor.");
        _states[key] = state;
        return state;
    }

    private static byte[] EncodeMappingValue(Mapping mapping)
    {
        byte[] value = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(value, mapping.JournalSeq);
        return value;
    }

    private static void XorRowDigest(Span<byte> aggregate, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (aggregate.Length != 32)
            throw new ArgumentException("Mapping-set digest must be 32 bytes.", nameof(aggregate));
        Span<byte> row = stackalloc byte[key.Length + value.Length];
        key.CopyTo(row);
        value.CopyTo(row[key.Length..]);
        Span<byte> hash = stackalloc byte[32];
        _ = SHA256.HashData(row, hash);
        for (int i = 0; i < aggregate.Length; i++)
            aggregate[i] ^= hash[i];
    }

    private static int FindFirstOutboxSequenceGreaterThan(List<Mapping> mappings, ulong sequence)
    {
        int low = 0;
        int high = mappings.Count;
        while (low < high)
        {
            int middle = low + ((high - low) >> 1);
            if (mappings[middle].OutboxSeq <= sequence)
                low = middle + 1;
            else
                high = middle;
        }
        return low;
    }

    private byte[] CursorKey(int namespaceId, NodeId author) => Join(_root, CursorSuffix(namespaceId, author));

    private static byte[] CursorSuffix(int namespaceId, NodeId author)
    {
        byte[] suffix = new byte[1 + 2 + 16];
        suffix[0] = CursorKind;
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(1), checked((ushort)namespaceId));
        author.WriteTo(suffix.AsSpan(3));
        return suffix;
    }

    private static byte[] MappingPrefix(int namespaceId, NodeId author)
    {
        byte[] suffix = new byte[1 + 2 + 16];
        suffix[0] = MappingKind;
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(1), checked((ushort)namespaceId));
        author.WriteTo(suffix.AsSpan(3));
        return suffix;
    }

    private byte[] MappingKey(int namespaceId, NodeId author, ulong outboxSeq)
    {
        byte[] suffix = new byte[1 + 2 + 16 + 8];
        suffix[0] = MappingKind;
        BinaryPrimitives.WriteUInt16LittleEndian(suffix.AsSpan(1), checked((ushort)namespaceId));
        author.WriteTo(suffix.AsSpan(3));
        BinaryPrimitives.WriteUInt64BigEndian(suffix.AsSpan(19), outboxSeq);
        return Join(_root, suffix);
    }

    private static byte[] EncodeCursor(
        ulong scanned, ulong ackedOutbox, ulong ackedJournal, ReadOnlySpan<byte> mappingSetDigest)
    {
        if (mappingSetDigest.Length != 32)
            throw new ArgumentException("Mapping-set digest must be 32 bytes.", nameof(mappingSetDigest));
        byte[] value = new byte[CursorValueLength];
        "RPM3"u8.CopyTo(value);
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(4), scanned);
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(12), ackedOutbox);
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(20), ackedJournal);
        mappingSetDigest.CopyTo(value.AsSpan(28));
        return value;
    }

    private static void DecodeCursor(byte[] value, StreamState state)
    {
        if (value.Length != CursorValueLength || !value.AsSpan(0, 4).SequenceEqual("RPM3"u8))
            throw new InvalidDataException("Relay provenance cursor marker or length is invalid.");
        state.ScannedThroughJournalSeq = BinaryPrimitives.ReadUInt64LittleEndian(value.AsSpan(4));
        state.AckedThroughOutboxSeq = BinaryPrimitives.ReadUInt64LittleEndian(value.AsSpan(12));
        state.AckedThroughJournalSeq = BinaryPrimitives.ReadUInt64LittleEndian(value.AsSpan(20));
        value.AsSpan(28, 32).CopyTo(state.MappingSetDigest);
        if (state.AckedThroughJournalSeq > state.ScannedThroughJournalSeq)
            throw new InvalidDataException("Relay provenance ACK cursor exceeds the scanned-through cursor.");
        if (state.AckedThroughOutboxSeq == 0 && state.AckedThroughJournalSeq != 0)
            throw new InvalidDataException("A positive FLD relay ACK cannot exist without a positive BIN-SYNC Outbox watermark.");
    }

    private static byte[] Join(byte[] left, byte[] right)
    {
        byte[] result = new byte[left.Length + right.Length];
        left.CopyTo(result, 0);
        right.CopyTo(result, left.Length);
        return result;
    }

    private static void ValidateStream(int namespaceId, NodeId author)
    {
        if (namespaceId < 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (author.IsZero)
            throw new ArgumentException("Original author identity must be non-zero.", nameof(author));
    }
}

/// <summary>A relay operation's immutable source and destination sequence coordinates.</summary>
public readonly record struct StorageRelayProvenanceEntry(ulong JournalSeq, ulong OutboxSeq);

/// <summary>Transient source identity supplied by an FLD relay page before global ChangeId resolution.</summary>
public readonly record struct StorageRelaySourceChange(ulong JournalSeq, BatchId ChangeId);

/// <summary>Durable scan and post-FLD-ACK watermarks for one original-author stream.</summary>
public readonly record struct StorageRelayProvenanceCursor(
    ulong ScannedThroughJournalSeq,
    ulong AckedThroughOutboxSeq,
    ulong AckedThroughJournalSeq);
