namespace Fld1.Storage.Wal.IO;

/// <summary>
/// Holds a process- and OS-managed exclusive lease for one WAL partition.
/// The lock file is permanent; exclusivity comes from the open handle's
/// FileShare.None lock, which the OS releases when the process exits.
/// A process-local set rejects duplicate opens before opening a second handle
/// to the same file (important for Unix file-lock semantics).
/// </summary>
internal sealed class WalWriterFence : IDisposable
{
    private static readonly object Gate = new();
    private static readonly HashSet<string> ActiveNames = new(StringComparer.Ordinal);
    private readonly string _identity;
    private readonly FileStream _lockStream;
    private int _disposed;

    private WalWriterFence(string identity, FileStream lockStream)
    {
        _identity = identity;
        _lockStream = lockStream;
    }

    public static WalWriterFence Acquire(string walDirectory, ulong partitionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(walDirectory);
        string fullPath = ResolvePhysicalDirectory(walDirectory);
        string identity = fullPath + "|" + partitionId.ToString(System.Globalization.CultureInfo.InvariantCulture);

        lock (Gate)
        {
            if (!ActiveNames.Add(identity))
                throw new IOException("WAL partition is already open for writing in this process.");

            FileStream? lockStream = null;
            try
            {
                Directory.CreateDirectory(fullPath);
                string lockPath = Path.Combine(fullPath, $"p_{partitionId}.writer.lock");
                lockStream = new FileStream(
                    lockPath,
                    FileMode.OpenOrCreate,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    bufferSize: 1,
                    FileOptions.None);
                return new WalWriterFence(identity, lockStream);
            }
            catch (Exception ex)
            {
                lockStream?.Dispose();
                ActiveNames.Remove(identity);
                if (ex is IOException)
                    throw new IOException("WAL partition is already open for writing.", ex);
                throw new IOException("Failed to acquire the WAL partition writer fence.", ex);
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        lock (Gate)
        {
            try
            {
                _lockStream.Dispose();
            }
            finally
            {
                ActiveNames.Remove(_identity);
            }
        }
    }

    private static string ResolvePhysicalDirectory(string path)
    {
        string fullPath = Path.GetFullPath(path);
        for (int pass = 0; pass < 40; pass++)
        {
            string root = Path.GetPathRoot(fullPath)
                ?? throw new IOException("WAL directory path has no filesystem root.");
            if (OperatingSystem.IsWindows())
                root = root.ToUpperInvariant();

            string current = root;
            string[] segments = fullPath[root.Length..].Split(
                new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar },
                StringSplitOptions.RemoveEmptyEntries);
            bool restart = false;

            for (int i = 0; i < segments.Length; i++)
            {
                string segment = segments[i];
                string actualSegment = segment;
                if (Directory.Exists(current))
                {
                    string?[] matches = Directory.EnumerateFileSystemEntries(current)
                        .Select(entry => Path.GetFileName(entry))
                        .Where(name => name is not null &&
                            string.Equals(name, segment, StringComparison.OrdinalIgnoreCase))
                        .Take(2)
                        .ToArray();
                    if (matches.Length == 1)
                        actualSegment = matches[0]!;
                    else if (matches.Length > 1 &&
                             matches.Any(name => string.Equals(name, segment, StringComparison.Ordinal)))
                        actualSegment = segment;
                }

                current = Path.Combine(current, actualSegment);
                FileSystemInfo? target = new DirectoryInfo(current)
                    .ResolveLinkTarget(returnFinalTarget: true);
                if (target is null)
                    continue;

                string resolvedTarget = Path.GetFullPath(target.FullName);
                string remaining = i + 1 < segments.Length
                    ? Path.Combine(segments[(i + 1)..])
                    : string.Empty;
                fullPath = remaining.Length == 0
                    ? resolvedTarget
                    : Path.Combine(resolvedTarget, remaining);
                restart = true;
                break;
            }

            if (!restart)
                return Path.TrimEndingDirectorySeparator(current);
        }

        throw new IOException("WAL directory path contains too many symbolic-link indirections.");
    }
}
