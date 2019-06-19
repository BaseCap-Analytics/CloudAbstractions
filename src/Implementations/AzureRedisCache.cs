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
        private const int DATABASE_ID = 12;
        private readonly ConfigurationOptions _options;
        private ConnectionMultiplexer _cacheConnection;
        private IDatabaseAsync _database;

        /// <summary>
        /// Creates a new AzureRedisCache
        /// </summary>
        public AzureRedisCache(string endpoint, string password)
        {
            _options = new ConfigurationOptions()
            {
                AbortOnConnectFail = false,
                ConnectRetry = 3,
                Password = password,
                Ssl = true,
            };
            _options.EndPoints.Add(endpoint);
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
            _cacheConnection = await ConnectionMultiplexer.ConnectAsync(_options);
            _database = _cacheConnection.GetDatabase(DATABASE_ID);
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
