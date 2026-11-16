using BinSync.DataProvider.Conformance;
using BinSync.MetadataStore.Conformance;
using BinSync.Storage;

namespace BinSync.Storage.Integration;

public sealed class StorageDataProviderConformanceAdapter : StorageDataProvider, IDataProviderConformanceAdapter
{
    public StorageDataProviderConformanceAdapter(string directory, ulong partitionId = 1, bool deleteOnDispose = true)
        : base(directory, partitionId, deleteOnDispose) { }
}

public sealed class StorageMetadataStoreConformanceAdapter : StorageMetadataStore, IMetadataStoreConformanceAdapter
{
    public StorageMetadataStoreConformanceAdapter(string directory, ulong partitionId = 301, bool deleteOnDispose = true)
        : base(directory, partitionId, deleteOnDispose) { }
}
