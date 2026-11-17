using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;

namespace BinSync.FldHost;

/// <summary>
/// Durable per-namespace GC operator policy and audit adapter over the same
/// BIN-SYNC StorageDatabase as StorageMetadataStore. Policy + audit are one
/// StorageWriteBatch; maintenance leases are OS file-share locks, released by
/// the OS if the owning process exits. No second journal is introduced.
/// </summary>
public sealed class StorageFldBinSyncGcOperatorPolicyStore : IFldBinSyncGcOperatorPolicyStore
{
    private const int BindingLength = 36;
    private const int MaximumTextBytes = 4096;
    private static readonly byte[] KeyMagic = "BSGC"u8.ToArray();
    private static readonly byte[] PolicyMagic = "GCP1"u8.ToArray();
    private static readonly byte[] AuditMagic = "GCA1"u8.ToArray();

    private readonly StorageDatabase _storage;
    private readonly StorageMetadataStore _metadata;
    private readonly string _leaseDirectory;

    public StorageFldBinSyncGcOperatorPolicyStore(
        StorageDatabase storage,
        StorageMetadataStore metadata)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        if (!_metadata.IsBoundTo(_storage))
            throw new ArgumentException(
                "The policy store and Metadata adapter must share the exact StorageDatabase.",
                nameof(metadata));
        _leaseDirectory = Path.Combine(_storage.DirectoryPath, "binsync-gc-operator-leases");
        Directory.CreateDirectory(_leaseDirectory);
    }

    public ValueTask<FldBinSyncGcOperatorPolicy?> ReadAsync(
        FldBinSyncGcOperatorPolicyBinding binding,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ValidateBinding(binding);
        return ValueTask.FromResult(
            _storage.TryGet(PolicyKey(binding), out byte[]? value)
                ? DecodePolicy(value!, binding)
                : null);
    }

    public async ValueTask<bool> TryCommitAsync(
        ulong expectedGeneration,
        FldBinSyncGcOperatorPolicy policy,
        FldBinSyncGcOperatorAuditEntry auditEntry,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(auditEntry);
        ValidateBinding(policy.Binding);
        ValidateBinding(auditEntry.Binding);
        if (policy.Binding != auditEntry.Binding ||
            policy.Generation != checked(expectedGeneration + 1) ||
            policy.AuditSequence == 0 ||
            auditEntry.Sequence != policy.AuditSequence ||
            auditEntry.PolicyGeneration != policy.Generation)
            throw new ArgumentException("Policy and audit identity/generation do not form one commit.");

        ValidateText(policy.UpdatedBy, nameof(policy.UpdatedBy));
        ValidateText(policy.Reason, nameof(policy.Reason));
        ValidateText(auditEntry.ActorId, nameof(auditEntry.ActorId));
        ValidateText(auditEntry.Action, nameof(auditEntry.Action));
        ValidateText(auditEntry.Reason, nameof(auditEntry.Reason));

        // Updates acquire every domain fence in stable order. This is deliberately
        // conservative: a policy generation is one atomic namespace-wide decision.
        await using var receipt = await AcquireFileLeaseAsync(
            LeasePath(policy.Binding, FldBinSyncGcDomain.Receipt), cancellationToken).ConfigureAwait(false);
        await using var changeId = await AcquireFileLeaseAsync(
            LeasePath(policy.Binding, FldBinSyncGcDomain.ChangeId), cancellationToken).ConfigureAwait(false);
        await using var tombstone = await AcquireFileLeaseAsync(
            LeasePath(policy.Binding, FldBinSyncGcDomain.Tombstone), cancellationToken).ConfigureAwait(false);

        using IDisposable writerAdmission =
            await _metadata.AcquireWriterAdmissionAsync(cancellationToken).ConfigureAwait(false);

        using StorageWriteBatch batch = _storage.BeginWriteBatch();
        byte[] policyKey = PolicyKey(policy.Binding);
        ulong currentGeneration = 0;
        ulong currentAuditSequence = 0;
        if (batch.TryGet(policyKey, out byte[]? currentBytes))
        {
            FldBinSyncGcOperatorPolicy current = DecodePolicy(currentBytes!, policy.Binding);
            currentGeneration = current.Generation;
            currentAuditSequence = current.AuditSequence;
        }

        if (currentGeneration != expectedGeneration)
            return false;
        if (policy.AuditSequence != checked(currentAuditSequence + 1))
            throw new InvalidDataException("GC operator audit sequence is not the next durable sequence.");

        byte[] auditKey = AuditKey(policy.Binding, auditEntry.Sequence);
        if (batch.TryGet(auditKey, out _))
            throw new InvalidDataException("GC operator audit sequence already exists.");

        batch.Put(policyKey, EncodePolicy(policy));
        batch.Put(auditKey, EncodeAudit(auditEntry));
        try
        {
            batch.Commit();
            return true;
        }
        catch (StorageConflictException)
        {
            // Another writer changed the shared StorageDatabase after BeginWriteBatch.
            // The caller must reload the policy and retry with its observed generation.
            return false;
        }
    }

    public async ValueTask<IAsyncDisposable?> TryAcquireMaintenanceLeaseAsync(
        FldBinSyncGcOperatorPolicyBinding binding,
        ulong expectedGeneration,
        FldBinSyncGcDomain domain,
        CancellationToken cancellationToken = default)
    {
        ValidateBinding(binding);
        string path = LeasePath(binding, domain);
        FileLease lease = await AcquireFileLeaseAsync(path, cancellationToken).ConfigureAwait(false);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_storage.TryGet(PolicyKey(binding), out byte[]? value))
                return await ReleaseAndReturnNullAsync(lease).ConfigureAwait(false);

            FldBinSyncGcOperatorPolicy policy = DecodePolicy(value!, binding);
            if (policy.Generation != expectedGeneration || !IsDomainEnabled(policy, domain))
                return await ReleaseAndReturnNullAsync(lease).ConfigureAwait(false);

            return lease;
        }
        catch
        {
            await lease.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Reads a bounded, sequence-ordered audit page for operator diagnostics.</summary>
    public IReadOnlyList<FldBinSyncGcOperatorAuditEntry> ReadAudit(
        FldBinSyncGcOperatorPolicyBinding binding,
        ulong afterSequence = 0,
        int limit = 256)
    {
        ValidateBinding(binding);
        if (limit is < 1 or > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        byte[] prefix = AuditPrefix(binding);
        byte[] afterKey = afterSequence == 0 ? Array.Empty<byte>() : AuditKey(binding, afterSequence);
        IReadOnlyList<StorageEntry> entries = snapshot.ScanPrefix(prefix, afterKey, limit);
        var result = new FldBinSyncGcOperatorAuditEntry[entries.Count];
        for (int i = 0; i < entries.Count; i++)
            result[i] = DecodeAudit(entries[i].Key, entries[i].Value, binding);
        return result;
    }

    private static bool IsDomainEnabled(FldBinSyncGcOperatorPolicy policy, FldBinSyncGcDomain domain)
        => policy.MaintenanceStarted && domain switch
        {
            FldBinSyncGcDomain.Receipt => policy.ReceiptEnabled,
            FldBinSyncGcDomain.ChangeId => policy.ChangeIdEnabled,
            FldBinSyncGcDomain.Tombstone => policy.TombstoneEnabled,
            _ => throw new ArgumentOutOfRangeException(nameof(domain))
        };

    private async ValueTask<FileLease> AcquireFileLeaseAsync(
        string path,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                // FileShare.None is an OS-enforced process lock on supported .NET
                // platforms. The empty file is only a stable lock inode, not lease state.
                return new FileLease(new FileStream(
                    path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None));
            }
            catch (IOException)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(20), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private string LeasePath(FldBinSyncGcOperatorPolicyBinding binding, FldBinSyncGcDomain domain)
    {
        byte[] identity = BindingBytes(binding);
        byte[] domainBytes = new byte[identity.Length + 1];
        identity.CopyTo(domainBytes, 0);
        domainBytes[^1] = checked((byte)domain);
        string name = Convert.ToHexString(SHA256.HashData(domainBytes));
        return Path.Combine(_leaseDirectory, name + ".lock");
    }

    private static byte[] PolicyKey(FldBinSyncGcOperatorPolicyBinding binding)
        => BuildKey(binding, kind: 1, suffix: ReadOnlySpan<byte>.Empty);

    private static byte[] AuditPrefix(FldBinSyncGcOperatorPolicyBinding binding)
        => BuildKey(binding, kind: 2, suffix: ReadOnlySpan<byte>.Empty);

    private static byte[] AuditKey(FldBinSyncGcOperatorPolicyBinding binding, ulong sequence)
    {
        Span<byte> suffix = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(suffix, sequence);
        return BuildKey(binding, kind: 2, suffix);
    }

    private static byte[] BuildKey(
        FldBinSyncGcOperatorPolicyBinding binding,
        byte kind,
        ReadOnlySpan<byte> suffix)
    {
        byte[] identity = BindingBytes(binding);
        byte[] key = new byte[KeyMagic.Length + 1 + identity.Length + 1 + suffix.Length];
        KeyMagic.CopyTo(key, 0);
        key[4] = 1;
        identity.CopyTo(key, 5);
        key[41] = kind;
        suffix.CopyTo(key.AsSpan(42));
        return key;
    }

    private static byte[] BindingBytes(FldBinSyncGcOperatorPolicyBinding binding)
    {
        byte[] bytes = new byte[BindingLength];
        if (!binding.MetadataScopeId.TryWriteBytes(bytes))
            throw new InvalidOperationException("Could not encode Metadata scope identity.");
        binding.LocalReplica.WriteTo(bytes.AsSpan(16, 16));
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(32, 4), binding.NamespaceId);
        return bytes;
    }

    private static byte[] EncodePolicy(FldBinSyncGcOperatorPolicy policy)
        => EncodeRecord(PolicyMagic, writer =>
        {
            WriteBinding(writer, policy.Binding);
            writer.Write(policy.Generation);
            writer.Write(policy.AuditSequence);
            WriteText(writer, policy.UpdatedBy);
            WriteText(writer, policy.Reason);
            WriteFlags(writer, policy.MaintenanceStarted, policy.ReceiptEnabled,
                policy.ChangeIdEnabled, policy.TombstoneEnabled);
        });

    private static FldBinSyncGcOperatorPolicy DecodePolicy(
        byte[] bytes,
        FldBinSyncGcOperatorPolicyBinding expectedBinding)
        => DecodeRecord(bytes, PolicyMagic, reader =>
        {
            FldBinSyncGcOperatorPolicyBinding binding = ReadBinding(reader);
            if (binding != expectedBinding)
                throw new InvalidDataException("Persisted GC operator policy binding does not match its key.");
            ulong generation = reader.ReadUInt64();
            ulong auditSequence = reader.ReadUInt64();
            string updatedBy = ReadText(reader);
            string reason = ReadText(reader);
            (bool started, bool receipt, bool changeId, bool tombstone) = ReadFlags(reader);
            if (generation == 0 || auditSequence == 0)
                throw new InvalidDataException("Persisted GC operator policy generation/sequence is zero.");
            return new FldBinSyncGcOperatorPolicy(binding, generation, auditSequence,
                updatedBy, reason, started, receipt, changeId, tombstone);
        });

    private static byte[] EncodeAudit(FldBinSyncGcOperatorAuditEntry audit)
        => EncodeRecord(AuditMagic, writer =>
        {
            WriteBinding(writer, audit.Binding);
            writer.Write(audit.Sequence);
            writer.Write(audit.PolicyGeneration);
            WriteText(writer, audit.ActorId);
            WriteText(writer, audit.Action);
            WriteText(writer, audit.Reason);
            WriteFlags(writer, audit.MaintenanceStarted, audit.ReceiptEnabled,
                audit.ChangeIdEnabled, audit.TombstoneEnabled);
            writer.Write(audit.RecordedAtUtc.UtcDateTime.Ticks);
        });

    private static FldBinSyncGcOperatorAuditEntry DecodeAudit(
        byte[] key,
        byte[] bytes,
        FldBinSyncGcOperatorPolicyBinding expectedBinding)
    {
        if (key.Length != 50 || !key.AsSpan(0, 42).SequenceEqual(AuditPrefix(expectedBinding)) ||
            BinaryPrimitives.ReadUInt64BigEndian(key.AsSpan(42)) == 0)
            throw new InvalidDataException("GC operator audit key is malformed.");
        FldBinSyncGcOperatorAuditEntry entry = DecodeRecord(bytes, AuditMagic, reader =>
        {
            FldBinSyncGcOperatorPolicyBinding binding = ReadBinding(reader);
            if (binding != expectedBinding)
                throw new InvalidDataException("Persisted GC operator audit binding does not match its key.");
            ulong sequence = reader.ReadUInt64();
            ulong generation = reader.ReadUInt64();
            string actor = ReadText(reader);
            string action = ReadText(reader);
            string reason = ReadText(reader);
            (bool started, bool receipt, bool changeId, bool tombstone) = ReadFlags(reader);
            long ticks = reader.ReadInt64();
            DateTimeOffset timestamp;
            try { timestamp = new DateTimeOffset(ticks, TimeSpan.Zero); }
            catch (ArgumentOutOfRangeException exception)
            {
                throw new InvalidDataException("GC operator audit timestamp is invalid.", exception);
            }
            return new FldBinSyncGcOperatorAuditEntry(binding, sequence, generation,
                actor, action, reason, started, receipt, changeId, tombstone, timestamp);
        });
        ulong keySequence = BinaryPrimitives.ReadUInt64BigEndian(key.AsSpan(42));
        if (entry.Sequence != keySequence)
            throw new InvalidDataException("GC operator audit key/value sequence mismatch.");
        return entry;
    }

    private static byte[] EncodeRecord(byte[] magic, Action<BinaryWriter> write)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(magic);
            writer.Write((byte)1);
            write(writer);
        }
        return stream.ToArray();
    }

    private static T DecodeRecord<T>(byte[] bytes, byte[] magic, Func<BinaryReader, T> read)
    {
        try
        {
            using var stream = new MemoryStream(bytes, writable: false);
            using var reader = new BinaryReader(stream, new UTF8Encoding(false, true), leaveOpen: true);
            if (!reader.ReadBytes(magic.Length).AsSpan().SequenceEqual(magic) || reader.ReadByte() != 1)
                throw new InvalidDataException("GC operator record magic/version is invalid.");
            T result = read(reader);
            if (stream.Position != stream.Length)
                throw new InvalidDataException("GC operator record has trailing bytes.");
            return result;
        }
        catch (EndOfStreamException exception)
        {
            throw new InvalidDataException("GC operator record is truncated.", exception);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException("GC operator record contains invalid UTF-8.", exception);
        }
    }

    private static void WriteBinding(BinaryWriter writer, FldBinSyncGcOperatorPolicyBinding binding)
    {
        writer.Write(binding.MetadataScopeId.ToByteArray());
        byte[] replica = new byte[16];
        binding.LocalReplica.WriteTo(replica);
        writer.Write(replica);
        writer.Write(binding.NamespaceId);
    }

    private static FldBinSyncGcOperatorPolicyBinding ReadBinding(BinaryReader reader)
    {
        Guid scope = new(reader.ReadBytes(16));
        byte[] replica = reader.ReadBytes(16);
        if (replica.Length != 16) throw new EndOfStreamException();
        return new FldBinSyncGcOperatorPolicyBinding(
            scope, NodeId.From(replica), reader.ReadInt32());
    }

    private static void WriteText(BinaryWriter writer, string value)
    {
        byte[] bytes = new UTF8Encoding(false, true).GetBytes(value);
        if (bytes.Length > MaximumTextBytes)
            throw new ArgumentException("GC operator text exceeds the durable record limit.");
        writer.Write(bytes.Length);
        writer.Write(bytes);
    }

    private static string ReadText(BinaryReader reader)
    {
        int length = reader.ReadInt32();
        if (length is < 0 or > MaximumTextBytes)
            throw new InvalidDataException("GC operator text length is invalid.");
        byte[] bytes = reader.ReadBytes(length);
        if (bytes.Length != length) throw new EndOfStreamException();
        return new UTF8Encoding(false, true).GetString(bytes);
    }

    private static void WriteFlags(
        BinaryWriter writer, bool started, bool receipt, bool changeId, bool tombstone)
    {
        writer.Write(started);
        writer.Write(receipt);
        writer.Write(changeId);
        writer.Write(tombstone);
    }

    private static (bool Started, bool Receipt, bool ChangeId, bool Tombstone) ReadFlags(BinaryReader reader)
        => (ReadStrictBool(reader), ReadStrictBool(reader), ReadStrictBool(reader), ReadStrictBool(reader));

    private static bool ReadStrictBool(BinaryReader reader)
    {
        byte value = reader.ReadByte();
        return value switch
        {
            0 => false,
            1 => true,
            _ => throw new InvalidDataException("GC operator boolean flag is invalid.")
        };
    }

    private static void ValidateBinding(FldBinSyncGcOperatorPolicyBinding binding)
    {
        ArgumentNullException.ThrowIfNull(binding);
        if (binding.MetadataScopeId == Guid.Empty || binding.NamespaceId < 0)
            throw new ArgumentException("GC operator policy binding is invalid.", nameof(binding));
    }

    private static void ValidateText(string value, string parameter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameter);
        if (Encoding.UTF8.GetByteCount(value) > MaximumTextBytes)
            throw new ArgumentException("GC operator text exceeds the durable record limit.", parameter);
    }

    private static async ValueTask<IAsyncDisposable?> ReleaseAndReturnNullAsync(FileLease lease)
    {
        await lease.DisposeAsync().ConfigureAwait(false);
        return null;
    }

    private sealed class FileLease(FileStream stream) : IAsyncDisposable
    {
        private FileStream? _stream = stream;
        public ValueTask DisposeAsync()
        {
            Interlocked.Exchange(ref _stream, null)?.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
