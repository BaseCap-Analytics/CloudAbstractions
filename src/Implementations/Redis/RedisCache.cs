using BaseCap.CloudAbstractions.Abstractions;
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

        public Task SetupAsync()
        {
            return base.InitializeAsync();
        }

        /// <inheritdoc />
        public IHyperLogLog CreateHyperLogLog(string logName)
        {
            return (IHyperLogLog)new RedisHyperLogLog(logName, _options, _logger);
        }

        /// <inheritdoc />
        public async Task<T?> GetCacheObjectAsync<T>(string key) where T : class
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(GetCacheObjectAsync)}");
            }
            else if (string.IsNullOrWhiteSpace(key))
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
                return DeserializeObject<T>(value);
            }
        }

        /// <inheritdoc />
        public Task SetCacheObjectAsync<T>(string key, T obj, TimeSpan expiry) where T : class
        {
            return SetCacheObjectInternalAsync(key, obj, expiry);
        }

        private async Task SetCacheObjectInternalAsync<T>(string key, T obj, TimeSpan? expiry) where T : class
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(SetCacheObjectAsync)}");
            }
            else if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }
            else if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            string str = SerializeObject(obj);
            if (await _database.StringSetAsync(key, RedisValue.Unbox(str), expiry) == false)
            {
                throw new InvalidOperationException($"Failed to set cache entry for '{key}'");
            }
        }

        /// <inheritdoc />
        public Task<bool> DeleteCacheObjectAsync(string key)
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(DeleteCacheObjectAsync)}");
            }
            else if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _database.KeyDeleteAsync(key);
        }

        /// <inheritdoc />
        public Task<long> AddToListAsync(string key, string value)
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(AddToListAsync)}");
            }

            return _database.ListRightPushAsync(key, value);
        }

        /// <inheritdoc />
        public async Task<IEnumerable<string>> GetListAsync(string key)
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(GetListAsync)}");
            }

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
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(GetListCountAsync)}");
            }

            return _database.ListLengthAsync(key);
        }

        /// <inheritdoc />
        public Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey)
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(IncrementHashKeyAsync)}");
            }

            return _database.HashIncrementAsync(hashKey, fieldKey);
        }

        /// <inheritdoc />
        public Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey, int increment)
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(IncrementHashKeyAsync)}");
            }

            return _database.HashIncrementAsync(hashKey, fieldKey, increment);
        }

        /// <inheritdoc />
        public Task<bool> DoesHashFieldExistAsync(string hashKey, string fieldKey)
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(DoesHashFieldExistAsync)}");
            }

            return _database.HashExistsAsync(hashKey, fieldKey);
        }

        /// <inheritdoc />
        public Task<bool> SetHashFieldNxAsync(string hashKey, string fieldKey, string value)
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(SetHashFieldNxAsync)}");
            }

            return _database.HashSetAsync(hashKey, fieldKey, value, When.NotExists);
        }

        /// <inheritdoc />
        public async Task<object?> GetHashFieldAsync(string hashKey, string fieldKey)
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(GetHashFieldAsync)}");
            }

            RedisValue? value = await _database.HashGetAsync(hashKey, fieldKey).ConfigureAwait(false);
            if ((value == null) || value.Value.IsNullOrEmpty || string.IsNullOrWhiteSpace(value.Value.ToString()))
            {
                return null;
            }
            else
            {
                if (value.Value.TryParse(out long lngval))
                {
                    return lngval;
                }
                else if (value.Value.TryParse(out double dblval))
                {
                    return dblval;
                }
                else
                {
                    return value.Value.ToString();
                }
            }
        }

        /// <inheritdoc />
        public async Task<IEnumerable<long?>> GetHashKeyFieldValuesAsync(string hashKey, params string[] fields)
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(GetHashKeyFieldValuesAsync)}");
            }

            RedisValue[] values = new RedisValue[fields.Length];
            for (int i = 0; i < fields.Length; i++)
            {
                values[i] = fields[i];
            }

            RedisValue[] fieldValues = await _database.HashGetAsync(hashKey, values).ConfigureAwait(false);
            long?[] returnValues = new long?[fieldValues.Length];
            for (int i = 0; i < fieldValues.Length; i++)
            {
                if (fieldValues[i].HasValue)
                {
                    try
                    {
                        returnValues[i] = (long)fieldValues[i];
                    }
                    catch
                    {
                        returnValues[i] = null;
                    }
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
