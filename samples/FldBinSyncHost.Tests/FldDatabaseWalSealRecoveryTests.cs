using Fld1.Engine.Db;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldDatabaseWalSealRecoveryTests
{
    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }

    [Fact]
    public void Wal_seal_before_tombstone_delete_survives_restart_and_releases_receipt_barrier()
    {
        string root = Path.Combine(
            Path.GetTempPath(), "binsync-fld-gc-wal-seal-" + Guid.NewGuid().ToString("N"));
        const ushort namespaceId = 0x76A;
        byte[] localAuthor = Id(0x41);
        byte[] recordId = Id(0x42);
        try
        {
            // Commit a value and seal its initial WAL prefix.
            using (var database = FldDatabase.Open(root, namespaceId, localAuthor))
            {
                database.Put(recordId, 7, "before-delete"u8);
                Assert.NotEqual(0UL, database.Commit());
                Assert.NotEqual(0UL, database.Checkpoint());

                // Commit the tombstone and acknowledge its local Outbox entry. The
                // committed WAL prefix is still unsealed at this exact crash boundary.
                database.Delete(recordId, 7);
                Assert.NotEqual(0UL, database.Commit());
                ulong highWatermark = database.GetOutboxHighWatermark();
                if (highWatermark != 0)
                {
                    var lease = database.AcquireOutboxLease(Guid.NewGuid());
                    database.AcknowledgeOutbox(highWatermark, lease);
                }

                using (FldReferenceSnapshot beforeSeal = database.OpenReferenceSnapshot())
                {
                    Assert.True(beforeSeal.UnpublishedLocalDraftHorizon.Count > 0);
                    Assert.True(beforeSeal.HasReceiptReference(
                        Id(0x43), 17, 18, namespaceId, Id(0x44)));
                }

                // This is the durable WAL-seal phase performed by the Host before
                // physical tombstone deletion. Simulate Host death immediately after
                // the seal and before CompactTombstones is invoked.
                Assert.NotEqual(0UL, database.Checkpoint());
                Assert.True(database.TryResolve(recordId, 7, out var retained));
                Assert.True(retained.IsTombstone);

                using FldReferenceSnapshot afterSeal = database.OpenReferenceSnapshot();
                Assert.Equal(0UL, afterSeal.UnpublishedLocalDraftHorizon.Count);
                Assert.False(afterSeal.HasReceiptReference(
                    Id(0x43), 17, 18, namespaceId, Id(0x44)));
            }

            // Reopen exactly as a new Host process would. The tombstone must still
            // exist (the seal is not a deletion), while the uncertainty barrier stays
            // cleared and a later bounded GC page can safely reclaim it.
            using (var reopened = FldDatabase.Open(root, namespaceId, localAuthor))
            {
                Assert.True(reopened.TryResolve(recordId, 7, out var retained));
                Assert.True(retained.IsTombstone);
                using FldReferenceSnapshot snapshot = reopened.OpenReferenceSnapshot();
                Assert.Equal(0UL, snapshot.UnpublishedLocalDraftHorizon.Count);
                Assert.False(snapshot.HasReceiptReference(
                    Id(0x43), 17, 18, namespaceId, Id(0x44)));

                Assert.Equal(1, reopened.CompactTombstones(
                    _ => true, limit: 1));
                Assert.False(reopened.TryResolve(recordId, 7, out _));
            }

            using var final = FldDatabase.Open(root, namespaceId, localAuthor);
            Assert.False(final.TryResolve(recordId, 7, out _));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }
}
