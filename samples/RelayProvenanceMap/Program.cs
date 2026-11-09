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
            new StorageRelayProvenanceEntry(1, Guid.Parse("10000000-0000-0000-0000-000000000001").ToByteArray(), 1),
            new StorageRelayProvenanceEntry(1, Guid.Parse("10000000-0000-0000-0000-000000000002").ToByteArray(), 2),
            new StorageRelayProvenanceEntry(2, Guid.Parse("20000000-0000-0000-0000-000000000001").ToByteArray(), 3)
        ], scannedThroughJournalSeq: 3);

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
