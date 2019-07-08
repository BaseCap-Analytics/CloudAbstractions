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
        private readonly ConfigurationOptions _options;
        private readonly ILogger _logger;
        private ConnectionMultiplexer _cacheConnection;
        private IDatabaseAsync _database;

        /// <summary>
        /// Creates a new AzureRedisCache
        /// </summary>
        public AzureRedisCache(string endpoint, string password, bool useSsl, ILogger logger)
        {
            _options = new ConfigurationOptions()
            {
                AbortOnConnectFail = false,
                ConnectRetry = 3,
                ConnectTimeout = TimeSpan.FromSeconds(30).Milliseconds,
                Password = password,
                Ssl = useSsl,
            };
            _options.EndPoints.Add(endpoint);
            _logger = logger;
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
        public Task SetupAsync()
        {
            CreateConnection();
            return Task.CompletedTask;
        }

        private void ResetConnection()
        {
            try
            {
                _cacheConnection.Close();
                _cacheConnection.Dispose();
            }
            finally
            {
                CreateConnection();
            }
        }

        private void CreateConnection()
        {
            _cacheConnection = ConnectionMultiplexer.Connect(_options);
            _cacheConnection.ConnectionFailed += OnConnectionFailure;
            _cacheConnection.ConnectionRestored += OnConnectionRestored;
            _cacheConnection.ErrorMessage += OnError;
            _cacheConnection.InternalError += OnRedisInternalError;
            _cacheConnection.IncludeDetailInExceptions = true;
            _cacheConnection.IncludePerformanceCountersInExceptions = true;
            _database = _cacheConnection.GetDatabase();
        }

        private void OnConnectionFailure(object sender, ConnectionFailedEventArgs e)
        {
            _logger.LogException(
                e.Exception,
                new Dictionary<string, string>()
                {
                    ["ConnectionType"] = e.ConnectionType.ToString(),
                    ["Endpoint"] = e.EndPoint.ToString(),
                    ["FailureType"] = e.FailureType.ToString(),
                });
            _logger.LogEvent(
                "CacheConnectionFailure",
                new Dictionary<string, string>()
                {
                });

            ResetConnection();
        }

        private void OnConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            _logger.LogEvent(
                "CacheConnectionRestored",
                new Dictionary<string, string>()
                {
                });
        }

        private void OnError(object sender, RedisErrorEventArgs e)
        {
            _logger.LogException(
                new Exception(e.Message),
                new Dictionary<string, string>()
                {
                    ["Endpoint"] = e.EndPoint.ToString(),
                });

            ResetConnection();
        }

        private void OnRedisInternalError(object sender, InternalErrorEventArgs e)
        {
            _logger.LogException(
                e.Exception,
                new Dictionary<string, string>()
                {
                    ["ConnectionType"] = e.ConnectionType.ToString(),
                    ["Endpoint"] = e.EndPoint.ToString(),
                    ["Origin"] = e.Origin,
                });

            ResetConnection();
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
    }
}
