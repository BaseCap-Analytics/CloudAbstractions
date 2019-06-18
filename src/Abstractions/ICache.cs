using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// A connection to a distributed cache
    /// </summary>
    public interface ICache
    {
        /// <summary>
        /// Initializes the connection
        /// </summary>
        Task SetupAsync();

        /// <summary>
        /// Adds a value to a list
        /// </summary>
        Task AddToListAsync(string key, string value);

        /// <summary>
        /// Retrieves all values in a List
        /// </summary>
        Task<IEnumerable<string>> GetListAsync(string key);

        /// <summary>
        /// Retrieves the number of entries in a list
        /// </summary>
        /// <returns>Returns the number of entries in the list</returns>
        Task<long> GetListCountAsync(string key);

        /// <summary>
        /// Sets a cache value
        /// </summary>
        /// <param name="key">The key to store the value as</param>
        /// <param name="obj">The object to store in the cache</param>
        Task SetCacheObjectAsync<T>(string key, T obj) where T : class;

        /// <summary>
        /// Sets a cache value with an expiration
        /// </summary>
        /// <param name="key">The key to store the value as</param>
        /// <param name="obj">The object to store in the cache</param>
        /// <param name="expiry">The time it takes for this cache entry to be evicted</param>
        Task SetCacheObjectAsync<T>(string key, T obj, TimeSpan expiry) where T : class;

        /// <summary>
        /// Retrieves a previously set value
        /// </summary>
        /// <param name="key">The cache key</param>
        /// <returns>Returns the cached value; returns null if the value does not exist</returns>
        Task<T> GetCacheObjectAsync<T>(string key) where T : class;

        /// <summary>
        /// Deletes a value from the cache
        /// </summary>
        /// <param name="key">The key of the value to delete</param>
        Task DeleteCacheObjectAsync(string key);
    }
}
