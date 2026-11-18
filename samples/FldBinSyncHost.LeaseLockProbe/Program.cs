if (args.Length != 1)
    return 2;

await using var lease = new FileStream(
    args[0], FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
Console.WriteLine("READY");
Console.Out.Flush();
await Console.In.ReadLineAsync();
return 0;
