using BinSync;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>
/// Stores complete, integrity-protected BIN-SYNC Node checkpoints inside a caller-owned
/// StorageDatabase. The database can be shared with StorageDataProvider and
/// StorageMetadataStore; this adapter never owns or disposes it.
/// </summary>
public static class StorageNodePersistence
{
    private const byte NodeCheckpointKeyPrefix = 0xF0;

    /// <summary>Atomically persist the complete Node image in one Storage write batch.</summary>
    public static void Save(StorageDatabase storage, Node node)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(node);

        byte[] image = NodePersistence.SerializeToBytes(node);
        using var batch = storage.BeginWriteBatch();
        batch.Put(Key(node.NodeId), image);
        batch.Commit();
    }

    /// <summary>Load and integrity-check a Node checkpoint by its stable NodeId.</summary>
    public static Node Load(StorageDatabase storage, NodeId nodeId)
    {
        ArgumentNullException.ThrowIfNull(storage);
        if (nodeId.IsZero)
            throw new ArgumentException("Node identity must be non-zero.", nameof(nodeId));
        if (!storage.TryGet(Key(nodeId), out var image))
            throw new FileNotFoundException("Node checkpoint was not found in Storage.", nodeId.ToString());

        Node node = NodePersistence.LoadFromBytes(image!);
        if (node.NodeId != nodeId)
            throw new InvalidDataException("Stored Node checkpoint identity does not match its Storage key.");
        return node;
    }

    /// <summary>Try to load a Node checkpoint without treating absence as corruption.</summary>
    public static bool TryLoad(StorageDatabase storage, NodeId nodeId, out Node? node)
    {
        ArgumentNullException.ThrowIfNull(storage);
        if (nodeId.IsZero)
            throw new ArgumentException("Node identity must be non-zero.", nameof(nodeId));
        if (!storage.TryGet(Key(nodeId), out var image))
        {
            node = null;
            return false;
        }

        Node loaded = NodePersistence.LoadFromBytes(image!);
        if (loaded.NodeId != nodeId)
            throw new InvalidDataException("Stored Node checkpoint identity does not match its Storage key.");
        node = loaded;
        return true;
    }

    private static byte[] Key(NodeId nodeId)
    {
        byte[] key = new byte[17];
        key[0] = NodeCheckpointKeyPrefix;
        nodeId.WriteTo(key.AsSpan(1));
        return key;
    }
}
