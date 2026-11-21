// BenchmarkDotNet entry — baseline for perf/hotpath-baseline.
//
// Run (Release):
//   cd BinSync.Net
//   bash ../../scripts/vendor-ed25519-from-ztsharp.sh   # once, if Crypto not vendored
//   dotnet run -c Release --project bench/BinSync.Bench -- --filter '*'
//
// Filter examples:
//   --filter *Codec*
//   --filter *Merge*
//   --filter *Frame*
//   --filter *Batch*
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

var config = DefaultConfig.Instance
    .AddDiagnoser(MemoryDiagnoser.Default)
    .AddJob(Job.Default
        // Use the normal out-of-process toolchain: long stateful lifecycle fixtures
        // can exceed the in-process runner timeout during iteration setup/measurement.
        .WithWarmupCount(3)
        .WithIterationCount(10));

if (args.Contains("--coordinator-retry-only", StringComparer.Ordinal))
{
    // The 4,096-row fixture performs durable setup in IterationSetup. Keep the
    // full measurement count, but use the out-of-process toolchain: the in-process
    // runner aborts this long setup before all configured iterations can finish.
    var coordinatorConfig = DefaultConfig.Instance
        .AddDiagnoser(MemoryDiagnoser.Default)
        .AddJob(Job.Default
            .WithWarmupCount(3)
            .WithIterationCount(10));
    BenchmarkRunner.Run<BinSync.Bench.MetadataCoordinatorRetryScanBench>(coordinatorConfig);
}
else if (args.Contains("--metadata-cardinality-only", StringComparer.Ordinal))
    BenchmarkRunner.Run<BinSync.Bench.MetadataStoreCardinalityBench>(config);
else if (args.Contains("--metadata-gc-only", StringComparer.Ordinal))
{
    // IterationSetup seeds the large namespace. Keep MemoryDiagnoser disabled here;
    // it includes fixture setup allocations in this stateful one-invocation benchmark.
    var gcConfig = DefaultConfig.Instance
        .AddJob(Job.Default
            .WithToolchain(InProcessEmitToolchain.Instance)
            .WithWarmupCount(3)
            .WithIterationCount(10));
    BenchmarkRunner.Run<BinSync.Bench.MetadataChangeIdGcBench>(gcConfig);
}
else if (args.Contains("--metadata-million-only", StringComparer.Ordinal))
{
    var millionConfig = DefaultConfig.Instance
        .AddDiagnoser(MemoryDiagnoser.Default)
        .AddJob(Job.Default
            .WithWarmupCount(1)
            .WithIterationCount(3)
            .WithId("MillionRowShort"));
    BenchmarkRunner.Run<BinSync.Bench.MetadataStoreMillionRowBench>(millionConfig);
}
else
    BenchmarkSwitcher.FromAssembly(typeof(BinSync.Bench.MetadataCoordinatorRetryScanBench).Assembly).Run(args, config);

public partial class Program { }
