using System;
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
