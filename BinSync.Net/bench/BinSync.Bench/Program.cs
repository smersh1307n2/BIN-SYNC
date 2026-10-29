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

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);

public partial class Program { }
