using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
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
                return JsonConvert.DeserializeObject<T>(value);
            }
        }

        /// <inheritdoc />
        public Task SetCacheObjectAsync<T>(string key, T obj) where T : class
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            string str = JsonConvert.SerializeObject(obj);
            return _database.StringSetAsync(key, RedisValue.Unbox(str));
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
    }
}
