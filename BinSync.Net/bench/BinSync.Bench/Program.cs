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
        .WithToolchain(InProcessEmitToolchain.Instance) // no secondary process; simpler CI
        .WithWarmupCount(3)
        .WithIterationCount(10));

if (args.Contains("--coordinator-retry-only", StringComparer.Ordinal))
    BenchmarkRunner.Run<BinSync.Bench.MetadataCoordinatorRetryScanBench>(config);
else if (args.Contains("--metadata-cardinality-only", StringComparer.Ordinal))
    BenchmarkRunner.Run<BinSync.Bench.MetadataStoreCardinalityBench>(config);
else if (args.Contains("--metadata-million-only", StringComparer.Ordinal))
{
    var millionConfig = DefaultConfig.Instance
        .AddDiagnoser(MemoryDiagnoser.Default)
        .AddJob(Job.Default
            .WithToolchain(InProcessEmitToolchain.Instance)
            .WithWarmupCount(1)
            .WithIterationCount(3)
            .WithId("MillionRowShort"));
    BenchmarkRunner.Run<BinSync.Bench.MetadataStoreMillionRowBench>(millionConfig);
}
else
    BenchmarkSwitcher.FromAssembly(typeof(BinSync.Bench.MetadataCoordinatorRetryScanBench).Assembly).Run(args, config);

public partial class Program { }
