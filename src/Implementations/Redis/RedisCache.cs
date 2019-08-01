using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// A connection to a redis cache cluster
    /// </summary>
    public class RedisCache : RedisBase, ICache
    {
        /// <summary>
        /// Creates a new RedisCache
        /// </summary>
        public RedisCache(IEnumerable<string> endpoints, string password, bool useSsl, ILogger logger)
            : base(endpoints, password, useSsl, "Cache", "[default]", logger)
        {
        }

        Task ICache.SetupAsync()
        {
            return base.SetupAsync();
        }

        /// <inheritdoc />
        public IHyperLogLog CreateHyperLogLog(string logName)
        {
            return (IHyperLogLog)new RedisHyperLogLog(logName, _options, _logger);
        }

        protected virtual Task<string> SerializeObject(object o)
        {
            return Task.FromResult(JsonConvert.SerializeObject(o));
        }

        protected virtual Task<T> DeserializeObject<T>(string value)
        {
            return Task.FromResult(JsonConvert.DeserializeObject<T>(value));
        }

        /// <inheritdoc />
        public async Task<T> GetCacheObjectAsync<T>(string key) where T : class
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            RedisValue value = await _database.StringGetAsync(key);
            if (value.IsNullOrEmpty)
            {
                return null;
            }
            else
            {
                return await DeserializeObject<T>(value);
            }
        }

        /// <inheritdoc />
        public Task SetCacheObjectAsync<T>(string key, T obj, TimeSpan expiry) where T : class
        {
            return SetCacheObjectInternalAsync(key, obj, expiry);
        }

        private async Task SetCacheObjectInternalAsync<T>(string key, T obj, TimeSpan? expiry) where T : class
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            string str = await SerializeObject(obj);
            if (await _database.StringSetAsync(key, RedisValue.Unbox(str), expiry) == false)
            {
                throw new InvalidOperationException($"Failed to set cache entry for '{key}'");
            }
        }

        /// <inheritdoc />
        public Task<bool> DeleteCacheObjectAsync(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _database.KeyDeleteAsync(key);
        }

        /// <inheritdoc />
        public Task<long> AddToListAsync(string key, string value)
        {
            return _database.ListRightPushAsync(key, value);
        }

        /// <inheritdoc />
        public async Task<IEnumerable<string>> GetListAsync(string key)
        {
            RedisValue[] values = await _database.ListRangeAsync(key);
            if (values == null)
            {
                return Array.Empty<string>();
            }
            else
            {
                return values.Select(v => (string)v);
            }
        }

        /// <inheritdoc />
        public Task<long> GetListCountAsync(string key)
        {
            return _database.ListLengthAsync(key);
        }

        /// <inheritdoc />
        public Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey)
        {
            return _database.HashIncrementAsync(hashKey, fieldKey);
        }

        /// <inheritdoc />
        public Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey, int increment)
        {
            return _database.HashIncrementAsync(hashKey, fieldKey, increment);
        }

        /// <inheritdoc />
        public Task<bool> DoesHashFieldExistAsync(string hashKey, string fieldKey)
        {
            return _database.HashExistsAsync(hashKey, fieldKey);
        }

        /// <inheritdoc />
        public async Task<IEnumerable<long?>> GetHashKeyFieldValuesAsync(string hashKey, params string[] fields)
        {
            RedisValue[] values = new RedisValue[fields.Length];
            for (int i = 0; i < fields.Length; i++)
            {
                values[i] = fields[i];
            }

            RedisValue[] fieldValues = await _database.HashGetAsync(hashKey, values).ConfigureAwait(false);
            long?[] returnValues = new long?[fieldValues.Length];
            for (int i = 0; i < fieldValues.Length; i++)
            {
                if (fieldValues[i].IsInteger)
                {
                    returnValues[i] = (long)fieldValues[i];
                }
                else
                {
                    returnValues[i] = null;
                }
            }

            return returnValues;
        }
    }
}
