using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// A contract for communicating with a typed document db
    /// </summary>
    public interface IDocumentDb<T>
    {
        /// <summary>
        /// Creates a new collection with the specified indexes
        /// </summary>
        /// <param name="name">The name of the collection</param>
        /// <param name="ascendingIndexes">Any indexes for sorting ascending</param>
        /// <param name="descendingIndexes">Any indexes for sorting descending</param>
        /// <param name="expireyIndex">The index to use for document expiration</param>
        /// <param name="ttl">The time before documents expire</param>
        /// <returns>Returns an awaitable Task</returns>
        Task CreateCollectionAsync(
            string name,
            IEnumerable<string>? ascendingIndexes,
            IEnumerable<string>? descendingIndexes,
            string expireyIndex,
            TimeSpan ttl);

        /// <summary>
        /// Initializes the connection for use with an existing collection
        /// </summary>
        /// <param name="name">The name of the collection</param>
        void UseExistingCollection(string name);

        /// <summary>
        /// Finds a group of entities with the specified key/value search criteria. For example: PropertyA = "Foo"
        /// </summary>
        /// <param name="searchCriteria">The key/value search criteria to filter documents by</param>
        /// <param name="maxCount">An optional limit to the number of results returned</param>
        /// <param name="token">The Cancellation Token for cancellation</param>
        /// <returns>Returns a List of the result objects</returns>
        Task<List<T>> FindEntityAsync(Dictionary<string, string> searchCriteria, int? maxCount, CancellationToken token);

        /// <summary>
        /// Finds a single entity with the specified key/value search criteria. For example: PropertyA = "Foo"
        /// </summary>
        /// <param name="searchCriteria">The key/value search criteria to filter documents by</param>
        /// <param name="token">The Cancellation Token for cancellation</param>
        /// <returns>Returns the result object, if it exists; otherwise, returns null</returns>
        Task<T> FindEntityAsync(Dictionary<string, string> searchCriteria, CancellationToken token);

        /// <summary>
        /// Inserts entities into the collection
        /// </summary>
        /// <param name="entities">The entities to insert</param>
        /// <param name="token">The Cancellation Token for cancellation</param>
        /// <returns>Returns an awaitable Task</returns>
        Task InsertEntitiesAsync(IEnumerable<T> entities, CancellationToken token);
    }
}
