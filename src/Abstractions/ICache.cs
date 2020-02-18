using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// A connection to a distributed cache
    /// </summary>
    public interface ICache : IDisposable
    {
        /// <summary>
        /// Initializes the connection
        /// </summary>
        Task SetupAsync();

        /// <summary>
        /// Sets the expiry on a cache key
        /// </summary>
        /// <param name="key">The key to expire</param>
        /// <param name="expire">When the key should expire</param>
        /// <returns>Returns an awaitable Task</returns>
        Task SetKeyExpiryAsync(string key, DateTimeOffset expire);

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
        /// <param name="waitForResponse">Flag indicating if we care about redis' response</param>
        /// <returns>Returns the length of the list after the push operation</returns>
        Task<long> AddToListAsync(string key, string value, bool waitForResponse = false);

        /// <summary>
        /// Retrieves all values in a List
        /// </summary>
        /// <param name="key">The key of the List</param>
        /// <returns>Returns the values in the list</returns>
        Task<IEnumerable<string>> GetListAsync(string key);

        /// <summary>
        /// Retrieves an element from a List at an Index
        /// </summary>
        /// <param name="key">The key of the List</param>
        /// <returns>Returns the element at the given index</returns>
        Task<string> GetListElementAtIndexAsync(string key, long index);

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
        Task<T?> GetCacheObjectAsync<T>(string key) where T : class;

        /// <summary>
        /// Deletes a value from the cache
        /// </summary>
        /// <param name="key">The key of the value to delete</param>
        /// <param name="waitForResponse">Flag indicating if we care about redis' response</param>
        /// <returns>Returns true if the entry was deleted; otherwise, returns false</returns>
        Task<bool> DeleteCacheObjectAsync(string key, bool waitForResponse = false);

        /// <summary>
        /// Increments the hashset field by 1
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fieldKey">The field name in the hashset</param>
        /// <param name="waitForResponse">Flag indicating if we care about redis' response</param>
        /// <returns>Returns the value in the field after the operation completes</returns>
        Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey, bool waitForResponse = false);

        /// <summary>
        /// Increments the hashset field by a specified amount
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fieldKey">The field name in the hashset</param>
        /// <param name="increment">The amount to increment the field by</param>
        /// <param name="waitForResponse">Flag indicating if we care about redis' response</param>
        /// <returns>Returns the value in the field after the operation completes</returns>
        Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey, int increment, bool waitForResponse = false);

        /// <summary>
        /// Sets a flag in a hash set to 1
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fieldKey">The field name in the hashset</param>
        /// <returns>Returns True if the flag was set; otherwise returns false</returns>
        Task<bool> SetHashFieldFlagAsync(string hashKey, string fieldKey);

        /// <summary>
        /// Checks if the given field exists in the specified hash set
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fieldKey">The field name in the hashset</param>
        /// <returns>Returns true if the field exists; otherwise, returns false</returns>
        Task<bool> DoesHashFieldExistAsync(string hashKey, string fieldKey);

        /// <summary>
        /// Sets the specified field of a hashset
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fieldKey">The field name in the hashset</param>
        /// <param name="value">The value to put into the field</param>
        /// <param name="waitForResponse">Flag indicating if we care about redis' response</param>
        /// <returns>Returns true if the value was set; otherwise, returns false</returns>
        Task<bool> SetHashFieldAsync(string hashKey, string fieldKey, string value, bool waitForResponse = false);

        /// <summary>
        /// Appends a value to the end of a Hash Field
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fieldKey">The field name in the hashset</param>
        /// <param name="value">The value to put into the field</param>
        /// <param name="cancellation">A Cancellation Token to cancel the request</param>
        /// <returns>Returns true if the value was set; otherwise, returns false</returns>
        Task<bool> AppendHashFieldAsync(string hashKey, string fieldKey, string value, CancellationToken cancellation);

        /// <summary>
        /// Retrieves the specified field value of a hashset
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fieldKey">The field name in the hashset</param>
        /// <returns>Returns the value in the field, if it exists; otherwise, returns null</returns>
        Task<object?> GetHashFieldAsync(string hashKey, string fieldKey);

        /// <summary>
        /// Removes the specified field from a hashset
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fieldKey">The field name in the hashset</param>
        /// <returns>Returns an awaitable task</returns>
        Task RemoveHashFieldAsync(string hashKey, string fieldKey);

        /// <summary>
        /// Retrieves all Keys and Values in a HashField
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <returns>Returns the Dictionary of keys-to-values, if the hash key exists; otherwise, returns null</returns>
        Task<Dictionary<string, string?>?> GetAllHashFieldsAsync(string hashKey);

        /// <summary>
        /// Retrieves specific field values from the hashset
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <param name="fields">The fields to retrieve the values of</param>
        /// <returns>Returns the value for each field; if the field does not exist, null is returned for it's value</returns>
        Task<IEnumerable<long?>> GetHashKeyFieldValuesAsync(string hashKey, params string[] fields);

        /// <summary>
        /// Retrieves an enumerable over all entries in the hash set
        /// </summary>
        /// <param name="hashKey">The key to the hashset</param>
        /// <returns>Returns an enumerable to the hash entries</returns>
        IAsyncEnumerable<HashEntry> GetHashEntriesEnumerable(string hashKey);

        /// <summary>
        /// Adds a value to a set
        /// </summary>
        /// <param name="setName">The set name</param>
        /// <param name="member">The name of the member in the set</param>
        /// <param name="waitForResponse">Flag indicating if we care about redis' response</param>
        /// <returns>Returns an awaitable Task</returns>
        Task<bool> AddToSetAsync(string setName, string member, bool waitForResponse = false);

        /// <summary>
        /// Removes a value from a set
        /// </summary>
        /// <param name="setName">The set name</param>
        /// <param name="member">The name of the member in the set</param>
        /// <param name="waitForResponse">Flag indicating if we care about redis' response</param>
        /// <returns>Returns an awaitable Task</returns>
        Task RemoveFromSetAsync(string setName, string member, bool waitForResponse = false);

        /// <summary>
        /// Retrieves all values in a set
        /// </summary>
        /// <param name="setName">The set name</param>
        /// <returns>Returns an enumeration of the members</param>
        Task<IEnumerable<string>> GetSetMembersAsync(string setName);

        /// <summary>
        /// Iterate over every member of a Set
        /// </summary>
        /// <param name="setName">The set name</param>
        /// <returns>Returns an async enumerable of the members</returns>
        IAsyncEnumerable<RedisValue> GetSetMembersEnumerable(string setName);

        /// <summary>
        /// Increments the score of a sorted set member by the increment value
        /// </summary>
        /// <param name="setName">The sorted set name</param>
        /// <param name="member">The name of the member in the sorted set</param>
        /// <param name="increment">The amount to increment the score by</param>
        /// <param name="waitForResponse">Flag indicating if we care about redis' response</param>
        /// <returns>Returns the new score of the member</returns>
        Task<double> SortedSetIncrementAsync(string setName, string member, double increment = 1, bool waitForResponse = false);

        /// <summary>
        /// Removes an entry from a sorted set
        /// </summary>
        /// <param name="setName">The sorted set name</param>
        /// <param name="member">The name of the member in the sorted set</param>
        /// <param name="waitForResponse">Flag indicating if we care about redis' response</param>
        /// <returns>Returns an awaitable task</returns>
        Task SortedSetMemberRemoveAsync(string setName, string member, bool waitForResponse = false);

        /// <summary>
        /// Retrieves a specified number of top members from a sorted set
        /// </summary>
        /// <param name="setName">The sorted set name</param>
        /// <param name="count">The maximum number of entries to retrieve</param>
        /// <param name="sortDesc">Flag indicating if the retrieval should be for the highest score (true) or lowest score (false)</param>
        /// <returns>Returns a sorted enumeration of the members with their scores</param>
        Task<IEnumerable<KeyValuePair<string, double>>> GetSortedSetMembersAsync(string setName, int count, bool sortDesc);

        /// <summary>
        /// Retrieves an enumerator over the unordered members from a sorted set
        /// </summary>
        /// <param name="setName">The sorted set name</param>
        /// <returns>Returns an enumerable to the unsorted members</returns>
        IAsyncEnumerable<SortedSetEntry> GetSortedSetMembersEnumerable(string setName);
    }
}
