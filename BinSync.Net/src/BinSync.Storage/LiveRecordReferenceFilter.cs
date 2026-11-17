using BinSync;

namespace BinSync.Storage;

/// <summary>
/// Bounded prefilter for retained Metadata RecordId references.
/// False positives are permitted; false negatives are not.
/// </summary>
internal sealed class LiveRecordReferenceFilter
{
    private const int BitsPerExpectedItem = 32;
    private const int HashCount = 7;
    private const int MaximumBytes = 16 * 1024 * 1024;
    private const int MaximumBits = MaximumBytes * 8;

    private readonly ulong[] _words;
    private readonly ulong _mask;
    private readonly bool _empty;

    public LiveRecordReferenceFilter(long expectedItems)
    {
        if (expectedItems < 0) throw new ArgumentOutOfRangeException(nameof(expectedItems));
        _empty = expectedItems == 0;

        long desiredBits = Math.Max(64L, Math.Min(MaximumBits, expectedItems * BitsPerExpectedItem));
        int bitCount = 64;
        while (bitCount < desiredBits && bitCount < MaximumBits)
            bitCount <<= 1;

        _words = new ulong[bitCount / 64];
        _mask = (ulong)bitCount - 1;
    }

    public void Add(RecordId recordId)
    {
        if (recordId.IsZero)
            throw new ArgumentException("A live RecordId reference cannot be zero.", nameof(recordId));

        GetHashes(recordId, out ulong first, out ulong step);
        for (ulong i = 0; i < HashCount; i++)
        {
            ulong bit = (first + i * step) & _mask;
            _words[(int)(bit >> 6)] |= 1UL << (int)(bit & 63);
        }
    }

    public bool MightContain(RecordId recordId)
    {
        if (_empty || recordId.IsZero)
            return false;

        GetHashes(recordId, out ulong first, out ulong step);
        for (ulong i = 0; i < HashCount; i++)
        {
            ulong bit = (first + i * step) & _mask;
            if ((_words[(int)(bit >> 6)] & (1UL << (int)(bit & 63))) == 0)
                return false;
        }
        return true;
    }

    private static void GetHashes(RecordId recordId, out ulong first, out ulong step)
    {
        first = Mix(recordId.Lo ^ RotateLeft(recordId.Hi, 17));
        step = Mix(recordId.Hi ^ RotateLeft(recordId.Lo, 31)) | 1UL;
    }

    private static ulong RotateLeft(ulong value, int offset)
        => (value << offset) | (value >> (64 - offset));

    private static ulong Mix(ulong value)
    {
        unchecked
        {
            value += 0x9E3779B97F4A7C15UL;
            value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9UL;
            value = (value ^ (value >> 27)) * 0x94D049BB133111EBUL;
            return value ^ (value >> 31);
        }
    }
}
