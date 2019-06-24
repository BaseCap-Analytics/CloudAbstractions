using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for interfacing with a Storage Account containing Blobs, Queues, and Tables
    /// </summary>
    public interface IStorageAccount
    {
        /// <summary>
        /// Retrieves a contract to Table storage
        /// </summary>
        ITableStorage GetTableStorage();

        /// <summary>
        /// Retrieves a specific Blob Container
        /// </summary>
        Task<IBlobStorage> GetBlobStorageAsync(string containerName);
    }
}
