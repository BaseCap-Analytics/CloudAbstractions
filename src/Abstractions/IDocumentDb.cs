using System;
using System.Collections.Generic;
using System.Linq.Expressions;
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
            IEnumerable<Expression<Func<T, object>>>? ascendingIndexes,
            IEnumerable<Expression<Func<T, object>>>? descendingIndexes,
            string expireyIndex,
            TimeSpan ttl);

        /// <summary>
        /// Initializes the connection for use with an existing collection
        /// </summary>
        /// <param name="name">The name of the collection</param>
        void UseExistingCollection(string name);

        /// <summary>
        /// Finds a group of entities with the specified where clause expression
        /// </summary>
        /// <param name="whereClause">The WHERE clause in a filter expression</param>
        /// <param name="token">The Cancellation Token for cancellation</param>
        /// <returns>Returns a cursor to the result objects</returns>
        Task<IDocumentDbCursor<T>> FindEntitiesAsync(Expression<Func<T, bool>> whereClause, CancellationToken token);

        /// <summary>
        /// Finds a group of entities, sorted ascending, with the specified where clause expression
        /// </summary>
        /// <param name="whereClause">The WHERE clause in a filter expression</param>
        /// <param name="orderByClause">The ORDER BY clause to sort the results</param>
        /// <param name="maxCount">The maximum number of entries to return in the result set</param>
        /// <param name="token">The Cancellation Token for cancellation</param>
        /// <returns>Returns a cursor to the result objects</returns>
        Task<IDocumentDbCursor<T>> FindEntitiesAscendingAsync(
            Expression<Func<T, bool>> whereClause,
            Expression<Func<T, object>> orderByClause,
            int maxCount,
            CancellationToken token);

        /// <summary>
        /// Finds a group of entities, sorted descending, with the specified where clause expression
        /// </summary>
        /// <param name="whereClause">The WHERE clause in a filter expression</param>
        /// <param name="orderByClause">The ORDER BY clause to sort the results</param>
        /// <param name="maxCount">The maximum number of entries to return in the result set</param>
        /// <param name="token">The Cancellation Token for cancellation</param>
        /// <returns>Returns a cursor to the result objects</returns>
        Task<IDocumentDbCursor<T>> FindEntitiesDescendingAsync(
            Expression<Func<T, bool>> whereClause,
            Expression<Func<T, object>> orderByClause,
            int maxCount,
            CancellationToken token);

        /// <summary>
        /// Finds a single entity with the specified where clause expression
        /// </summary>
        /// <param name="whereClause">The WHERE clause in a filter expression</param>
        /// <param name="token">The Cancellation Token for cancellation</param>
        /// <returns>Returns the result object, if it exists; otherwise, returns null</returns>
        Task<T> FindEntityAsync(Expression<Func<T, bool>> whereClause, CancellationToken token);

        /// <summary>
        /// Inserts entities into the collection
        /// </summary>
        /// <param name="entities">The entities to insert</param>
        /// <param name="token">The Cancellation Token for cancellation</param>
        /// <returns>Returns an awaitable Task</returns>
        Task InsertEntitiesAsync(IEnumerable<T> entities, CancellationToken token);

        /// <summary>
        /// Retrieves the Count of documents that fit the given search criteria
        /// </summary>
        /// <param name="whereClause">The WHERE clause in a filter expression</param>
        /// <param name="token">The Cancellation Token for cancellation</param>
        /// <returns>Returns the number of documents that fit the search criteria</returns>
        Task<long> EntityCountAsync(Expression<Func<T, bool>> whereClause, CancellationToken token);
    }
}
