using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for interfacing with a Storage Account containing Blobs, Queues, and Tables
    /// </summary>
    public interface IStorageAccount
    {
        /// <summary>
        /// Retrieves a specific Blob Container
        /// </summary>
        Task<IBlobStorage> GetBlobStorageAsync(string containerName);

        /// <summary>
        /// List the available blob storage containers
        /// </summary>
        /// <returns>Returns an enumeration of the available blob containers</returns>
        Task<IEnumerable<string>> ListBlobContainersAsync();

        /// <summary>
        /// Retrieves the Azure CloudTable reference
        /// </summary>
        /// <param name="tableName">The name of the table to get a reference to</param>
        /// <returns>Returns a CloudTable reference</returns>
        Task<Microsoft.Azure.Cosmos.Table.CloudTable> GetAzureTableStorageAsync(string tableName);
    }
}
