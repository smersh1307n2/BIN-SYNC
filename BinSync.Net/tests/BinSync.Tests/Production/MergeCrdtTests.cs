// §8.4–8.9 COUNTER_G / COUNTER_PN / SET_OR / MANUAL.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class MergeCrdtTests
{
    static byte[] Author(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    static FieldState Fs(byte[] value, ulong wall, byte authorTail) =>
        new(value, new Hlc(wall, 0), Author(authorTail));

    [Fact]
    public void CounterG_per_author_max()
    {
        var a = Author(1);
        var b = Author(2);
        var left = CounterG.Encode(new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 3, [b] = 1 });
        var right = CounterG.Encode(new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 2, [b] = 5 });
        var merged = CounterG.Merge(left, right);
        var map = CounterG.Decode(merged);
        Assert.Equal(3UL, map[a]);
        Assert.Equal(5UL, map[b]);
        Assert.Equal(8UL, CounterG.Total(merged));
    }

    [Fact]
    public void First_crdt_value_is_validated_when_local_state_is_missing()
    {
        var actor = Author(3);
        var validPayloads = new (MergePolicy Policy, byte[] Payload)[]
        {
            (MergePolicy.CounterG, CounterG.Encode(new Dictionary<byte[], ulong>(BytesEq.Instance) { [actor] = 1 })),
            (MergePolicy.CounterPn, CounterPn.Encode(
                new Dictionary<byte[], ulong>(BytesEq.Instance) { [actor] = 1 },
                new Dictionary<byte[], ulong>(BytesEq.Instance))),
            (MergePolicy.SetOr, SetOr.Encode(Array.Empty<SetOr.Add>(), Array.Empty<SetOr.Dot>())),
            (MergePolicy.SeqFractional, SeqFractional.Encode(new SeqFractional.Item { ParentId = Author(0), AuthorNodeId = actor, Seq = 1 })),
            (MergePolicy.BlobRef, BlobRef.Encode(new BlobRef.Descriptor { BlobId = actor, Hash = new byte[32] })),
        };

        foreach (var (policy, payload) in validPayloads)
            Assert.Throws<InvalidDataException>(() =>
                Merge.MergeFieldWithPolicy(null, Fs(payload.Concat(new byte[] { 0 }).ToArray(), 1, 3), policy));
    }

    [Fact]
    public void CounterG_field_merge_policy()
    {
        var a = Author(1);
        var local = Fs(CounterG.Encode(new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 1 }), wall: 1, authorTail: 1);
        var remote = Fs(CounterG.Encode(new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 4 }), wall: 2, authorTail: 2);
        var (w, changed) = Merge.MergeFieldWithPolicy(local, remote, MergePolicy.CounterG);
        Assert.True(changed);
        Assert.Equal(4UL, CounterG.Total(w.Value));
    }

    [Fact]
    public void CounterPn_value_is_pos_minus_neg()
    {
        var a = Author(1);
        var b = Author(2);
        var payload = CounterPn.Encode(
            new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 10, [b] = 3 },
            new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 2 });
        Assert.Equal(11L, CounterPn.Value(payload)); // 10+3-2
    }

    [Fact]
    public void CounterPn_merge_componentwise()
    {
        var a = Author(1);
        var left = CounterPn.Encode(
            new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 5 },
            new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 1 });
        var right = CounterPn.Encode(
            new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 3 },
            new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 4 });
        var merged = CounterPn.Merge(left, right);
        // pos max=5, neg max=4 → value 1
        Assert.Equal(1L, CounterPn.Value(merged));
    }

    [Fact]
    public void SetOr_visible_excludes_removed_dots()
    {
        var actor = Author(1);
        var adds = new List<SetOr.Add>
        {
            new() { Element = new byte[] { 0xAA }, Actor = actor, Seq = 1 },
            new() { Element = new byte[] { 0xBB }, Actor = actor, Seq = 2 },
        };
        var rems = new List<SetOr.Dot> { new() { Actor = actor, Seq = 1 } };
        var payload = SetOr.Encode(adds, rems);
        var visible = SetOr.Visible(payload);
        Assert.Single(visible);
        Assert.Equal(new byte[] { 0xBB }, visible[0]);
    }

    [Fact]
    public void SetOr_merge_rejects_same_add_dot_for_different_elements_across_replicas()
    {
        var actor = Author(7);
        var local = SetOr.Encode(
            new[] { new SetOr.Add { Element = new byte[] { 0xA1 }, Actor = actor, Seq = 42 } },
            Array.Empty<SetOr.Dot>());
        var remote = SetOr.Encode(
            new[] { new SetOr.Add { Element = new byte[] { 0xB2 }, Actor = actor, Seq = 42 } },
            Array.Empty<SetOr.Dot>());

        Assert.Throws<InvalidDataException>(() => SetOr.Merge(local, remote));
        Assert.Throws<InvalidDataException>(() => SetOr.Merge(remote, local));
    }

    [Fact]
    public void SetOr_merge_unions_adds_and_removes()
    {
        var a1 = Author(1);
        var a2 = Author(2);
        var left = SetOr.Encode(
            new[] { new SetOr.Add { Element = new byte[] { 1 }, Actor = a1, Seq = 1 } },
            Array.Empty<SetOr.Dot>());
        var right = SetOr.Encode(
            new[] { new SetOr.Add { Element = new byte[] { 2 }, Actor = a2, Seq = 1 } },
            new[] { new SetOr.Dot { Actor = a1, Seq = 1 } });
        var merged = SetOr.Merge(left, right);
        var visible = SetOr.Visible(merged);
        Assert.Single(visible);
        Assert.Equal(new byte[] { 2 }, visible[0]);
    }

    [Fact]
    public void Manual_does_not_overwrite_divergent_value()
    {
        var local = Fs(new byte[] { 0x01 }, wall: 1, authorTail: 1);
        var remote = Fs(new byte[] { 0x02 }, wall: 99, authorTail: 2);
        var (w, changed) = Merge.MergeFieldWithPolicy(local, remote, MergePolicy.Manual);
        Assert.False(changed);
        Assert.Equal(new byte[] { 0x01 }, w.Value);
    }

    [Fact]
    public void Manual_advances_metadata_when_value_identical()
    {
        var local = Fs(new byte[] { 0xAA }, wall: 1, authorTail: 1);
        var remote = Fs(new byte[] { 0xAA }, wall: 5, authorTail: 2);
        var (w, changed) = Merge.MergeFieldWithPolicy(local, remote, MergePolicy.Manual);
        Assert.True(changed);
        Assert.Equal(5UL, w.Timestamp.Wall);
        Assert.Equal(new byte[] { 0xAA }, w.Value);
    }

    [Fact]
    public void Counter_decoders_reject_duplicate_unsorted_and_trailing_entries()
    {
        var a = Author(1);
        var b = Author(2);
        var duplicate = new Writer().Varint(2).B128(a).U64(1).B128(a).U64(2).Value();
        Assert.Throws<InvalidDataException>(() => CounterG.Decode(duplicate));

        var unsorted = new Writer().Varint(2).B128(b).U64(1).B128(a).U64(2).Value();
        Assert.Throws<InvalidDataException>(() => CounterG.Decode(unsorted));

        var pnDuplicate = new Writer().Varint(2)
            .B128(a).U64(1).B128(a).U64(2)
            .Varint(0).Value();
        Assert.Throws<InvalidDataException>(() => CounterPn.Decode(pnDuplicate));

        var trailing = CounterG.Encode(new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = 1 })
            .Concat(new byte[] { 0 }).ToArray();
        Assert.Throws<InvalidDataException>(() => CounterG.Decode(trailing));
    }

    [Fact]
    public void Counter_decoders_reject_impossible_counts_before_allocation()
    {
        var huge = new Writer().Varint(ulong.MaxValue).Value();
        Assert.Throws<InvalidDataException>(() => CounterG.Decode(huge));
        Assert.Throws<InvalidDataException>(() => CounterPn.Decode(huge));
        Assert.Throws<InvalidDataException>(() => SetOr.Decode(huge));
    }

    [Fact]
    public void Counter_totals_reject_signed_and_unsigned_overflow()
    {
        var a = Author(1);
        var b = Author(2);
        var growOnly = CounterG.Encode(new Dictionary<byte[], ulong>(BytesEq.Instance)
        {
            [a] = ulong.MaxValue,
            [b] = 1,
        });
        Assert.Throws<OverflowException>(() => CounterG.Total(growOnly));

        var pn = CounterPn.Encode(
            new Dictionary<byte[], ulong>(BytesEq.Instance) { [a] = (ulong)long.MaxValue + 1 },
            new Dictionary<byte[], ulong>(BytesEq.Instance));
        Assert.Throws<OverflowException>(() => CounterPn.Value(pn));
    }

    [Fact]
    public void CounterPn_value_allows_large_positive_and_negative_totals_to_cancel()
    {
        var a = Author(1);
        var b = Author(2);
        var payload = CounterPn.Encode(
            new Dictionary<byte[], ulong>(BytesEq.Instance)
            {
                [a] = ulong.MaxValue,
                [b] = 17,
            },
            new Dictionary<byte[], ulong>(BytesEq.Instance)
            {
                [a] = ulong.MaxValue,
                [b] = 12,
            });

        Assert.Equal(5L, CounterPn.Value(payload));
    }

    [Fact]
    public void SetOr_decoder_rejects_duplicate_dots_and_trailing_bytes()
    {
        var actor = Author(1);
        var duplicateAddDot = new Writer().Varint(2)
            .Bytes(new byte[] { 1 }).B128(actor).U64(7)
            .Bytes(new byte[] { 2 }).B128(actor).U64(7)
            .Varint(0).Value();
        Assert.Throws<InvalidDataException>(() => SetOr.Decode(duplicateAddDot));

        var duplicateRemove = new Writer().Varint(0).Varint(2)
            .B128(actor).U64(7).B128(actor).U64(7).Value();
        Assert.Throws<InvalidDataException>(() => SetOr.Decode(duplicateRemove));

        var valid = SetOr.Encode(Array.Empty<SetOr.Add>(), Array.Empty<SetOr.Dot>());
        Assert.Throws<InvalidDataException>(() => SetOr.Decode(valid.Concat(new byte[] { 0 }).ToArray()));
    }

    [Fact]
    public void Unknown_merge_policy_is_rejected_by_helpers_and_registry()
    {
        var local = Fs(new byte[] { 1 }, wall: 1, authorTail: 1);
        var remote = Fs(new byte[] { 2 }, wall: 2, authorTail: 2);
        var unknown = (MergePolicy)0xFF;

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Merge.MergeFieldWithPolicy(null, remote, unknown));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Merge.MergeFieldWithPolicy(local, remote, unknown));

        var registry = new MergePolicyRegistry();
        Assert.Throws<ArgumentOutOfRangeException>(() => registry.Set(1, 0x100, unknown));
        Assert.Equal(MergePolicy.LwwField, registry.Get(1, 0x100));
    }

    [Fact]
    public void Spec_wire_codes_match()
    {
        Assert.Equal(0x03, (byte)MergePolicy.CounterG);
        Assert.Equal(0x04, (byte)MergePolicy.CounterPn);
        Assert.Equal(0x05, (byte)MergePolicy.SetOr);
        Assert.Equal(0x09, (byte)MergePolicy.Manual);
        Assert.Equal(Model.FieldResolveId, 0x00FF);
    }
}
