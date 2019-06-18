using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// A connection to a redis cache cluster
    /// </summary>
    public class AzureRedisCache : ICache, IDisposable
    {
        private readonly string _connectionString;
        private ConnectionMultiplexer _cacheConnection;
        private IDatabaseAsync _database;

        /// <summary>
        /// Creates a new AzureRedisCache
        /// </summary>
        public AzureRedisCache(string connectionString)
        {
            _connectionString = connectionString;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing && (_cacheConnection != null))
            {
                _cacheConnection.Close();
                _cacheConnection.Dispose();
                _cacheConnection = null;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc />
        public async Task SetupAsync()
        {
            _cacheConnection = await ConnectionMultiplexer.ConnectAsync(_connectionString);
            _database = _cacheConnection.GetDatabase();
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
        public Task SetCacheObjectAsync<T>(string key, T obj) where T : class
        {
            return SetCacheObjectInternalAsync(key, obj, null);
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
            await _database.StringSetAsync(key, RedisValue.Unbox(str), expiry);
        }

        /// <inheritdoc />
        public Task DeleteCacheObjectAsync(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _database.KeyDeleteAsync(key);
        }

        /// <inheritdoc />
        public Task AddToListAsync(string key, string value)
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
                return values.Select(v => v.Box() as string);
            }
        }

        /// <inheritdoc />
        public Task<long> GetListCountAsync(string key)
        {
            return _database.ListLengthAsync(key);
        }
    }
}
