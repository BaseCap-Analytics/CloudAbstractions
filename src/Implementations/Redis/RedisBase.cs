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
    /// Base class for communicating with Redis
    /// </summary>
    public abstract class RedisBase : IDisposable
    {
        private const int MAX_STREAM_LENGTH = 100000; // 100k records in stream before removing old ones
        protected readonly ConfigurationOptions _options;
        private readonly JsonSerializerSettings _jsonOptions;
        protected ConnectionMultiplexer _cacheConnection;
        protected IDatabaseAsync _database;
        protected ISubscriber _subscription;
        protected readonly string _errorContextName;
        protected readonly string _errorContextValue;
        protected readonly ILogger _logger;

        internal RedisBase(IEnumerable<string> endpoints, string password, bool useSsl, string errorContextName, string errorContextValue, ILogger logger)
        {
            if (endpoints?.Any() == false)
            {
                throw new ArgumentNullException(nameof(endpoints));
            }

            _options = new ConfigurationOptions()
            {
                AbortOnConnectFail = false,
                ConnectRetry = 3,
                ConnectTimeout = Convert.ToInt32(TimeSpan.FromSeconds(30).TotalMilliseconds),
                Password = password,
                Ssl = useSsl,
                SyncTimeout = Convert.ToInt32(TimeSpan.FromSeconds(30).TotalMilliseconds),
            };
            foreach (string endpoint in endpoints)
            {
                _options.EndPoints.Add(endpoint);
            }
            _jsonOptions = new JsonSerializerSettings()
            {
                Formatting = Formatting.None,
                MaxDepth = 15, // Arbitrary value,
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                PreserveReferencesHandling = PreserveReferencesHandling.Objects,
            };
            _errorContextName = errorContextName;
            _errorContextValue = errorContextValue;
            _logger = logger;
        }

        internal RedisBase(ConfigurationOptions options, string errorContextName, string errorContextValue, ILogger logger)
        {
            _options = options;
            _errorContextName = errorContextName;
            _errorContextValue = errorContextValue;
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

        protected async Task CreateStreamIfNecessaryAsync(string streamName)
        {
            // If the stream doesn't exist, create it the only way possible; adding a value.
            // To ensure we don't mess up readers, we then delete the value, leaving an empty stream.
            if (await _database.KeyExistsAsync(streamName) == false)
            {
                RedisValue msgId = await _database.StreamAddAsync(
                    streamName,
                    "create",
                    "create",
                    maxLength: MAX_STREAM_LENGTH,
                    useApproximateMaxLength: true)
                    .ConfigureAwait(false);
                await _database.StreamDeleteAsync(streamName, new[] { msgId }).ConfigureAwait(false);
            }
        }

        protected async Task CreateStreamConsumerGroupIfNecessaryAsync(string streamName, string consumerGroup)
        {
            // Check if the consumer group exists; if it doesn't create it
            StreamGroupInfo[] groups = await _database.StreamGroupInfoAsync(streamName).ConfigureAwait(false);
            if (groups.Any(g => string.Equals(g.Name, consumerGroup, StringComparison.OrdinalIgnoreCase)) == false)
            {
                await _database.StreamCreateConsumerGroupAsync(streamName, consumerGroup).ConfigureAwait(false);
            }
        }

        private void OnConnectionFailure(object sender, ConnectionFailedEventArgs e)
        {
            _logger.LogException(
                e.Exception,
                new Dictionary<string, string>()
                {
                    [_errorContextName] = _errorContextValue,
                    ["ConnectionType"] = e.ConnectionType.ToString(),
                    ["Endpoint"] = e.EndPoint.ToString(),
                    ["FailureType"] = e.FailureType.ToString(),
                });
            _logger.LogEvent(
                "RedisConnectionFailure",
                new Dictionary<string, string>()
                {
                    [_errorContextName] = _errorContextValue,
                });

            ResetConnection();
        }

        private void OnConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            _logger.LogEvent(
                "RedisConnectionRestored",
                new Dictionary<string, string>()
                {
                    [_errorContextName] = _errorContextValue,
                });
        }

        private void OnError(object sender, RedisErrorEventArgs e)
        {
            _logger.LogException(
                new Exception(e.Message),
                new Dictionary<string, string>()
                {
                    [_errorContextName] = _errorContextValue,
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
                    [_errorContextName] = _errorContextValue,
                    ["ConnectionType"] = e.ConnectionType.ToString(),
                    ["Endpoint"] = e.EndPoint.ToString(),
                    ["Origin"] = e.Origin,
                });

            ResetConnection();
        }

        protected Task InitializeAsync()
        {
            CreateConnection();
            return Task.CompletedTask;
        }

        protected async Task CleanupAsync()
        {
            _database = null;
            await _subscription.UnsubscribeAllAsync().ConfigureAwait(false);
            await _cacheConnection.CloseAsync().ConfigureAwait(false);
            _cacheConnection.Dispose();
            _cacheConnection = null;
        }

        protected virtual string SerializeObject(object o) => JsonConvert.SerializeObject(o, _jsonOptions);
        protected virtual T DeserializeObject<T>(string str) => JsonConvert.DeserializeObject<T>(str, _jsonOptions);
    }
}
