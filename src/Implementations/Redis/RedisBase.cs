using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// Base class for communicating with Redis
    /// </summary>
    public abstract class RedisBase : IDisposable
    {
        protected ConnectionMultiplexer _cacheConnection;
        protected IDatabaseAsync _database;
        protected ISubscriber _subscription;
        protected readonly ILogger _logger;
        private readonly ConfigurationOptions _options;

        internal RedisBase(string endpoint, string password, bool useSsl, ILogger logger)
        {
            _options = new ConfigurationOptions()
            {
                AbortOnConnectFail = false,
                ConnectRetry = 3,
                ConnectTimeout = Convert.ToInt32(TimeSpan.FromSeconds(30).TotalMilliseconds),
                Password = password,
                Ssl = useSsl,
                SyncTimeout = Convert.ToInt32(TimeSpan.FromSeconds(30).TotalMilliseconds),
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
                _database = null;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Subscribe(string channel, Action<RedisChannel, RedisValue> handler)
        {
            _subscription.Subscribe(channel, handler);
        }

        protected virtual void ResetConnection()
        {
            try
            {
                _subscription.UnsubscribeAll();
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
            _subscription = _cacheConnection.GetSubscriber();
        }

        protected virtual void OnConnectionFailure(object sender, ConnectionFailedEventArgs e)
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
                "RedisConnectionFailure",
                new Dictionary<string, string>()
                {
                });

            ResetConnection();
        }

        protected virtual void OnConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            _logger.LogEvent(
                "RedisConnectionRestored",
                new Dictionary<string, string>()
                {
                });
        }

        protected virtual void OnError(object sender, RedisErrorEventArgs e)
        {
            _logger.LogException(
                new Exception(e.Message),
                new Dictionary<string, string>()
                {
                    ["Endpoint"] = e.EndPoint.ToString(),
                });

            ResetConnection();
        }

        protected virtual void OnRedisInternalError(object sender, InternalErrorEventArgs e)
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

        protected Task SetupAsync()
        {
            CreateConnection();
            return Task.CompletedTask;
        }

        protected async Task ShutdownAsync()
        {
            _database = null;
            await _subscription.UnsubscribeAllAsync().ConfigureAwait(false);
            await _cacheConnection.CloseAsync().ConfigureAwait(false);
            _cacheConnection.Dispose();
            _cacheConnection = null;
        }
    }
}
