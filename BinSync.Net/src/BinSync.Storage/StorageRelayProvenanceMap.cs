using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>
/// Durable source JournalSeq → ChangeId → BIN-SYNC OutboxSeq bridge for synchronized
/// FLD databases. This stores only the cross-coordinate mapping and scan/ACK cursors;
/// it never stores operation payloads or assigns protocol OriginSequence.
/// </summary>
public sealed class StorageRelayProvenanceMap
{
    private const byte RootKind = 0x6D;
    private const byte CursorKind = 0x01;
    private const byte MappingKind = 0x02;
    private const int RootLength = 49;
    private const int ChangeIdLength = 16;
    private const int CursorValueLength = 28;

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
        public readonly List<Mapping> Mappings = new();
        public readonly HashSet<string> ChangeIds = new(StringComparer.Ordinal);
    }

    private readonly record struct Mapping(ulong JournalSeq, byte[] ChangeId, ulong OutboxSeq);

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
    /// Atomically persists one complete FLD relay page's identity mapping and its
    /// ScannedThroughJournalSeq. The caller may publish/send the mapped Outbox entries
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
            var pageChangeIds = new HashSet<string>(StringComparer.Ordinal);
            bool firstEntry = true;
            var writes = new List<(byte[] Key, byte[] Value)>(entries.Count + 1);
            var appended = new List<Mapping>(entries.Count);

            foreach (StorageRelayProvenanceEntry entry in entries)
            {
                if ((firstEntry && entry.JournalSeq <= priorJournal) ||
                    (!firstEntry && entry.JournalSeq < priorJournal) ||
                    entry.JournalSeq > scannedThroughJournalSeq)
                    throw new InvalidDataException("Every new relay mapping must be after the prior scan cursor and at/before the new scan cursor.");
                if (entry.ChangeId is null || entry.ChangeId.Length != ChangeIdLength ||
                    IsZero(entry.ChangeId))
                    throw new InvalidDataException("Relay ChangeId must be a non-zero 16-byte identity.");
                byte[] changeId = entry.ChangeId.ToArray();
                if (entry.OutboxSeq == 0 || entry.OutboxSeq <= priorOutbox)
                    throw new InvalidDataException("Mapped OutboxSeq values must be non-zero and strictly increasing per author.");
                string identity = Convert.ToHexString(changeId);
                if (state.ChangeIds.Contains(identity) || !pageChangeIds.Add(identity))
                    throw new InvalidDataException("A ChangeId is already mapped in this author stream or occurs more than once in the page.");

                byte[] key = MappingKey(namespaceId, author, entry.OutboxSeq);
                byte[] value = new byte[8 + ChangeIdLength];
                BinaryPrimitives.WriteUInt64LittleEndian(value, entry.JournalSeq);
                changeId.CopyTo(value, 8);
                writes.Add((key, value));
                appended.Add(new Mapping(entry.JournalSeq, changeId, entry.OutboxSeq));
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
                state.AckedThroughJournalSeq);
            using var batch = _storage.BeginWriteBatch();
            foreach (var write in writes) batch.Put(write.Key, write.Value);
            batch.Put(cursorKey, cursorValue);
            batch.Commit();

            state.Mappings.AddRange(appended);
            foreach (Mapping mapping in appended)
                state.ChangeIds.Add(Convert.ToHexString(mapping.ChangeId));
            state.ScannedThroughJournalSeq = scannedThroughJournalSeq;
        }
    }

    /// <summary>
    /// Translates BIN-SYNC's per-author safe OutboxSeq into FLD's safe-through
    /// JournalSeq. The first source journal containing an unacknowledged OutboxSeq
    /// is retained in full; all earlier source journals are safe.
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
                return state.AckedThroughJournalSeq;

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

            var deletes = state.Mappings
                .Where(mapping => mapping.JournalSeq <= safeThroughJournalSeq)
                .Select(mapping => MappingKey(namespaceId, author, mapping.OutboxSeq))
                .ToArray();
            byte[] cursorKey = CursorKey(namespaceId, author);
            byte[] cursorValue = EncodeCursor(
                state.ScannedThroughJournalSeq, safeOutboxSeq, safeThroughJournalSeq);

            using var batch = _storage.BeginWriteBatch();
            foreach (byte[] key in deletes) batch.Delete(key);
            batch.Put(cursorKey, cursorValue);
            batch.Commit();

            foreach (Mapping mapping in state.Mappings)
            {
                if (mapping.JournalSeq > safeThroughJournalSeq) break;
                state.ChangeIds.Remove(Convert.ToHexString(mapping.ChangeId));
            }
            state.Mappings.RemoveAll(mapping => mapping.JournalSeq <= safeThroughJournalSeq);
            state.AckedThroughOutboxSeq = safeOutboxSeq;
            state.AckedThroughJournalSeq = safeThroughJournalSeq;
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

        var loadedChangeIds = new HashSet<string>(StringComparer.Ordinal);
        using (StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(Join(_root, MappingPrefix(namespaceId, author))))
        {
            const int pageSize = 1024;
            while (true)
            {
                IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
                foreach (StorageEntry row in page)
                {
                    if (row.Key.Length != RootLength + 1 + 2 + 16 + 8 ||
                        row.Value.Length != 8 + ChangeIdLength)
                        throw new InvalidDataException("Malformed relay provenance mapping row.");
                    ulong outboxSeq = BinaryPrimitives.ReadUInt64BigEndian(row.Key.AsSpan(RootLength + 1 + 2 + 16, 8));
                    ulong journalSeq = BinaryPrimitives.ReadUInt64LittleEndian(row.Value);
                    byte[] changeId = row.Value.AsSpan(8, ChangeIdLength).ToArray();
                    string changeIdHex = Convert.ToHexString(changeId);
                    if (!loadedChangeIds.Add(changeIdHex))
                        throw new InvalidDataException("A ChangeId is mapped more than once in one author stream.");
                    if (journalSeq <= state.AckedThroughJournalSeq ||
                        journalSeq > state.ScannedThroughJournalSeq ||
                        outboxSeq <= state.AckedThroughOutboxSeq ||
                        (state.Mappings.Count != 0 &&
                            (outboxSeq <= state.Mappings[^1].OutboxSeq ||
                             journalSeq < state.Mappings[^1].JournalSeq)))
                        throw new InvalidDataException("Relay provenance mapping is outside its durable cursor or not monotonic.");
                    state.Mappings.Add(new Mapping(journalSeq, changeId, outboxSeq));
                    state.ChangeIds.Add(changeIdHex);
                }
                if (page.Count < pageSize) break;
            }
        }
        _states[key] = state;
        return state;
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

    private static byte[] EncodeCursor(ulong scanned, ulong ackedOutbox, ulong ackedJournal)
    {
        byte[] value = new byte[CursorValueLength];
        "RPM1"u8.CopyTo(value);
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(4), scanned);
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(12), ackedOutbox);
        BinaryPrimitives.WriteUInt64LittleEndian(value.AsSpan(20), ackedJournal);
        return value;
    }

    private static void DecodeCursor(byte[] value, StreamState state)
    {
        if (value.Length != CursorValueLength || !value.AsSpan(0, 4).SequenceEqual("RPM1"u8))
            throw new InvalidDataException("Relay provenance cursor marker or length is invalid.");
        state.ScannedThroughJournalSeq = BinaryPrimitives.ReadUInt64LittleEndian(value.AsSpan(4));
        state.AckedThroughOutboxSeq = BinaryPrimitives.ReadUInt64LittleEndian(value.AsSpan(12));
        state.AckedThroughJournalSeq = BinaryPrimitives.ReadUInt64LittleEndian(value.AsSpan(20));
        if (state.AckedThroughJournalSeq > state.ScannedThroughJournalSeq)
            throw new InvalidDataException("Relay provenance ACK cursor exceeds the scanned-through cursor.");
    }

    private static byte[] Join(byte[] left, byte[] right)
    {
        byte[] result = new byte[left.Length + right.Length];
        left.CopyTo(result, 0);
        right.CopyTo(result, left.Length);
        return result;
    }

    private static bool IsZero(byte[] bytes)
    {
        int value = 0;
        foreach (byte b in bytes) value |= b;
        return value == 0;
    }

    private static void ValidateStream(int namespaceId, NodeId author)
    {
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (author.IsZero)
            throw new ArgumentException("Original author identity must be non-zero.", nameof(author));
    }
}

/// <summary>A relay operation's immutable source and destination sequence coordinates.</summary>
public sealed record StorageRelayProvenanceEntry(ulong JournalSeq, byte[] ChangeId, ulong OutboxSeq);

/// <summary>Durable scan and post-FLD-ACK watermarks for one original-author stream.</summary>
public readonly record struct StorageRelayProvenanceCursor(
    ulong ScannedThroughJournalSeq,
    ulong AckedThroughOutboxSeq,
    ulong AckedThroughJournalSeq);
