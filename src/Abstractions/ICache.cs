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
        /// Creates a HyperLogLog from this cache
        /// </summary>
        /// <param name="logName">The name of the HyperLogLog to create</param>
        /// <returns>Returns an IHyperLogLog to use</returns>
        IHyperLogLog CreateHyperLogLog(string logName);

        /// <summary>
        /// Adds a value to a list
        /// </summary>
        /// <param name="key">The key to store the value as</param>
        /// <param name="value">The value to add to the list in the cache</param>
        /// <returns>Returns the length of the list after the push operation</returns>
        Task<long> AddToListAsync(string key, string value);

        /// <summary>
        /// Retrieves all values in a List
        /// </summary>
        /// <param name="key">The key to store the value as</param>
        /// <returns>Returns the values in the list</returns>
        Task<IEnumerable<string>> GetListAsync(string key);

        /// <summary>
        /// Retrieves the number of entries in a list
        /// </summary>
        /// <param name="key">The key to store the value as</param>
        /// <returns>Returns the number of entries in the list</returns>
        Task<long> GetListCountAsync(string key);

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
        /// <returns>Returns true if the entry was deleted; otherwise, returns false</returns>
        Task<bool> DeleteCacheObjectAsync(string key);

        /// <summary>
        /// Increments the hashset field by 1
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fieldKey">The field name in the hashset</param>
        /// <returns>Returns the value in the field after the operation completes</returns>
        Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey);

        /// <summary>
        /// Increments the hashset field by a specified amount
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fieldKey">The field name in the hashset</param>
        /// <param name="increment">The amount to increment the field by</param>
        /// <returns>Returns the value in the field after the operation completes</returns>
        Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey, int increment);

        /// <summary>
        /// Retrieves specific field values from the hashset
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fields">The fields to retrieve the values of</param>
        /// <returns>Returns the value for each field; if the field does not exist, null is returned for it's value</returns>
        Task<IEnumerable<long?>> GetHashKeyFieldValuesAsync(string hashKey, params string[] fields);
    }
}
