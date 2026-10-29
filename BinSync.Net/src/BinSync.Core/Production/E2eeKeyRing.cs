// Per-namespace E2EE keys + current key epoch (§14.5 / §14.9).
using System.Security.Cryptography;

namespace BinSync;

/// <summary>
/// Holds namespace encryption keys and the active key epoch used when sealing fields.
/// Epoch advances after a successful KEY_ROTATION; callers may also seed via <see cref="Set"/>.
/// </summary>
public sealed class E2eeKeyRing
{
    private readonly object _gate = new();
    private readonly Dictionary<int, byte[]> _keys = new();
    private readonly Dictionary<int, ushort> _epochs = new();

    /// <summary>Install or replace the 32-byte namespace key and optional starting epoch.</summary>
    public void Set(int namespaceId, ReadOnlySpan<byte> key, ushort keyEpoch = 0)
    {
        if (key.Length != E2ee.KeySize)
            throw new ArgumentException($"E2EE key must be {E2ee.KeySize} bytes", nameof(key));
        lock (_gate)
        {
            if (_keys.TryGetValue(namespaceId, out var old))
                CryptographicOperations.ZeroMemory(old);
            _keys[namespaceId] = key.ToArray();
            _epochs[namespaceId] = keyEpoch;
        }
    }

    /// <summary>Remove key material for a namespace (ZeroMemory before drop).</summary>
    public bool Remove(int namespaceId)
    {
        lock (_gate)
        {
            if (!_keys.TryGetValue(namespaceId, out var old)) return false;
            CryptographicOperations.ZeroMemory(old);
            _keys.Remove(namespaceId);
            _epochs.Remove(namespaceId);
            return true;
        }
    }

    public bool TryGet(int namespaceId, out byte[] key, out ushort keyEpoch)
    {
        lock (_gate)
        {
            if (!_keys.TryGetValue(namespaceId, out key!))
            {
                keyEpoch = 0;
                return false;
            }
            keyEpoch = _epochs.TryGetValue(namespaceId, out var e) ? e : (ushort)0;
            return true;
        }
    }

    /// <summary>Advance the active key epoch (after KEY_ROTATION). Key material unchanged.</summary>
    public void AdvanceEpoch(int namespaceId, ushort newEpoch)
    {
        lock (_gate)
        {
            if (!_keys.ContainsKey(namespaceId))
                throw new InvalidOperationException($"no E2EE key for namespace {namespaceId}");
            ushort cur = _epochs.TryGetValue(namespaceId, out var e) ? e : (ushort)0;
            if (newEpoch <= cur)
                throw new ArgumentException("newEpoch must be greater than current");
            _epochs[namespaceId] = newEpoch;
        }
    }

    public ushort GetEpoch(int namespaceId)
    {
        lock (_gate)
            return _epochs.TryGetValue(namespaceId, out var e) ? e : (ushort)0;
    }

    public bool HasKey(int namespaceId)
    {
        lock (_gate) return _keys.ContainsKey(namespaceId);
    }
}
