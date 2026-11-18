using BinSync;
using BinSync.FldHost;
using BinSync.Storage;
using Fld1.Storage.Engine;

if (args.Length == 2 && args[0] == "hold-lock")
{
    await using var lease = new FileStream(
        args[1], FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
    Console.WriteLine("READY");
    Console.Out.Flush();
    await Console.In.ReadLineAsync();
    return 0;
}

if (args.Length == 2 && args[0] == "commit-policy")
{
    const ulong partitionId = 979;
    var binding = new FldBinSyncGcOperatorPolicyBinding(
        Guid.Parse("c19f9d30-0f9a-4a5c-91d8-6f24e6c3d81b"), default, 17);
    using var storage = StorageDatabase.Open(args[1], partitionId);
    using var metadata = new StorageMetadataStore(storage);
    var store = new StorageFldBinSyncGcOperatorPolicyStore(storage, metadata);
    const string reason = "process death after atomic policy/audit commit";
    var policy = new FldBinSyncGcOperatorPolicy(
        binding, 1, 1, "operator-crash-test", reason,
        true, true, false, false);
    var audit = new FldBinSyncGcOperatorAuditEntry(
        binding, 1, 1, "operator-crash-test", "Initialize", reason,
        true, true, false, false, DateTimeOffset.UtcNow);
    if (!await store.TryCommitAsync(0, policy, audit))
        return 3;

    Console.WriteLine("COMMITTED");
    Console.Out.Flush();
    await Console.In.ReadLineAsync();
    return 0;
}

return 2;
