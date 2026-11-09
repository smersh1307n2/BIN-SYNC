using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;

string root = Path.Combine(Path.GetTempPath(), "binsync-relay-provenance-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);
byte[] authorBytes = new byte[16];
authorBytes[0] = 0xA1;
NodeId author = NodeId.From(authorBytes);
const int namespaceId = 7;
string databasePath = Path.Combine(root, "metadata");

try
{
    using (var storage = StorageDatabase.Open(databasePath, 213))
    {
        var map = new StorageRelayProvenanceMap(storage, "relay-provenance-sample", author);
        map.AppendPage(namespaceId, author,
        [
            new StorageRelayProvenanceEntry(1, 1),
            new StorageRelayProvenanceEntry(1, 2),
            new StorageRelayProvenanceEntry(2, 3)
        ], scannedThroughJournalSeq: 3);
        VerifyGoldenRows(storage, author, namespaceId);

        Require(map.TranslateSafeOutboxSequence(namespaceId, author, 1) == 0,
            "A partial ACK must not split one FLD JournalSeq.");
        map.CommitAfterFldRelayAck(namespaceId, author, safeOutboxSeq: 1, safeThroughJournalSeq: 0);
    }

    using (var storage = StorageDatabase.Open(databasePath, 213))
    {
        var map = new StorageRelayProvenanceMap(storage, "relay-provenance-sample", author);
        StorageRelayProvenanceCursor cursor = map.ReadCursor(namespaceId, author);
        Require(cursor.ScannedThroughJournalSeq == 3 && cursor.AckedThroughOutboxSeq == 1 &&
            cursor.AckedThroughJournalSeq == 0, "Durable cursor did not survive reopen.");

        ulong safeJournal = map.TranslateSafeOutboxSequence(namespaceId, author, 2);
        Require(safeJournal == 1, "ACK translation did not retain the next unacknowledged source journal.");
        map.CommitAfterFldRelayAck(namespaceId, author, safeOutboxSeq: 2, safeThroughJournalSeq: safeJournal);

        safeJournal = map.TranslateSafeOutboxSequence(namespaceId, author, 3);
        Require(safeJournal == 3, "Fully acknowledged mapping did not advance through the scanned source cursor.");
        map.CommitAfterFldRelayAck(namespaceId, author, safeOutboxSeq: 3, safeThroughJournalSeq: safeJournal);

    }

    using (var storage = StorageDatabase.Open(databasePath, 213))
    {
        var map = new StorageRelayProvenanceMap(storage, "relay-provenance-sample", author);
        StorageRelayProvenanceCursor cursor = map.ReadCursor(namespaceId, author);
        Require(cursor.ScannedThroughJournalSeq == 3 && cursor.AckedThroughOutboxSeq == 3 &&
            cursor.AckedThroughJournalSeq == 3, "Fully pruned cursor did not survive restart.");

        map.AppendPage(namespaceId, author, Array.Empty<StorageRelayProvenanceEntry>(),
            scannedThroughJournalSeq: 4);
        ulong safeJournal = map.TranslateSafeOutboxSequence(namespaceId, author, 3);
        Require(safeJournal == 4,
            "An empty author page may advance only after a positive Outbox watermark is already durable.");
        map.CommitAfterFldRelayAck(namespaceId, author, safeOutboxSeq: 3, safeThroughJournalSeq: 4);

        byte[] secondAuthorBytes = new byte[16];
        secondAuthorBytes[0] = 0xA2;
        NodeId secondAuthor = NodeId.From(secondAuthorBytes);
        map.AppendPage(namespaceId, secondAuthor, Array.Empty<StorageRelayProvenanceEntry>(),
            scannedThroughJournalSeq: 5);
        Require(map.TranslateSafeOutboxSequence(namespaceId, secondAuthor, 0) == 0,
            "A zero Outbox watermark must never authorize a positive FLD ACK.");

        bool rejected = false;
        try
        {
            _ = map.TranslateSafeOutboxSequence(namespaceId, author, 4);
        }
        catch (InvalidDataException)
        {
            rejected = true;
        }
        Require(rejected, "An unmapped destination watermark must fail closed.");
    }

    Console.WriteLine("Relay provenance mapping / reopen / ACK boundaries: PASS");
    return 0;
}
catch (Exception exception)
{
    Console.Error.WriteLine(exception);
    return 1;
}
finally
{
    try { Directory.Delete(root, recursive: true); }
    catch { /* Preserve the original sample failure. */ }
}

static void Require(bool condition, string message)
{
    if (!condition) throw new InvalidOperationException(message);
}

static void VerifyGoldenRows(StorageDatabase storage, NodeId author, int namespaceId)
{
    byte[] scopeHash = SHA256.HashData(Encoding.UTF8.GetBytes("relay-provenance-sample"));
    byte[] root = new byte[49];
    root[0] = 0x6D;
    scopeHash.CopyTo(root, 1);
    author.WriteTo(root.AsSpan(33));

    byte[] cursorPrefix = new byte[19];
    cursorPrefix[0] = 0x01;
    BinaryPrimitives.WriteUInt16LittleEndian(cursorPrefix.AsSpan(1), checked((ushort)namespaceId));
    author.WriteTo(cursorPrefix.AsSpan(3));

    byte[] mappingPrefix = new byte[19];
    mappingPrefix[0] = 0x02;
    BinaryPrimitives.WriteUInt16LittleEndian(mappingPrefix.AsSpan(1), checked((ushort)namespaceId));
    author.WriteTo(mappingPrefix.AsSpan(3));

    using StorageSnapshot snapshot = storage.OpenSnapshot();
    IReadOnlyList<StorageEntry> cursorRows = snapshot.ScanPrefix(root, cursorPrefix, 2);
    Require(cursorRows.Count == 1, "Expected exactly one durable provenance cursor.");
    byte[] cursor = cursorRows[0].Value;
    Require(cursor.Length == 60 && cursor.AsSpan(0, 4).SequenceEqual("RPM3"u8),
        "Persisted provenance cursor does not match RPM3.");
    Require(BinaryPrimitives.ReadUInt64LittleEndian(cursor.AsSpan(4)) == 3 &&
        BinaryPrimitives.ReadUInt64LittleEndian(cursor.AsSpan(12)) == 0 &&
        BinaryPrimitives.ReadUInt64LittleEndian(cursor.AsSpan(20)) == 0,
        "Persisted provenance cursor watermarks differ from the golden fixture.");

    IReadOnlyList<StorageEntry> rows = snapshot.ScanPrefix(root, mappingPrefix, 4);
    Require(rows.Count == 3, "Expected three durable mapping rows.");
    ulong[] journalSequences = [1, 1, 2];
    byte[] aggregate = new byte[32];
    for (int i = 0; i < rows.Count; i++)
    {
        StorageEntry row = rows[i];
        Require(row.Key.Length == 76 && row.Value.Length == 8,
            "Mapping key/value length differs from the golden fixture.");
        Require(BinaryPrimitives.ReadUInt64BigEndian(row.Key.AsSpan(68, 8)) == (ulong)(i + 1),
            "Mapping key is not ordered by big-endian OutboxSeq.");
        Require(BinaryPrimitives.ReadUInt64LittleEndian(row.Value) == journalSequences[i],
            "Mapping value differs from the JournalSeq-only golden fixture.");

        byte[] rowBytes = new byte[row.Key.Length + row.Value.Length];
        row.Key.CopyTo(rowBytes, 0);
        row.Value.CopyTo(rowBytes, row.Key.Length);
        byte[] hash = SHA256.HashData(rowBytes);
        for (int j = 0; j < aggregate.Length; j++)
            aggregate[j] ^= hash[j];
    }

    Require(CryptographicOperations.FixedTimeEquals(aggregate, cursor.AsSpan(28, 32)),
        "Persisted mapping-set checksum differs from the recomputed golden digest.");
}
