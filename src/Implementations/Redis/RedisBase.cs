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
        private readonly ConfigurationOptions _options;
        private readonly JsonSerializerSettings _jsonOptions;
        private readonly object _synclock;
        protected readonly string _errorContextName;
        protected readonly string _errorContextValue;
        protected readonly ILogger _logger;
        private ConnectionMultiplexer? _cacheConnection;

        internal RedisBase(IEnumerable<string> endpoints, string password, bool useSsl, string errorContextName, string errorContextValue, ILogger logger)
        {
            if ((endpoints == null) || (endpoints.Any() == false))
            {
                throw new ArgumentNullException(nameof(endpoints));
            }

            _synclock = new object();
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
            _synclock = new object();
            _jsonOptions = new JsonSerializerSettings()
            {
                Formatting = Formatting.None,
                MaxDepth = 15, // Arbitrary value,
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                PreserveReferencesHandling = PreserveReferencesHandling.Objects,
            };
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
        public IHyperLogLog CreateHyperLogLog(string logName)
        {
            return (IHyperLogLog)new RedisHyperLogLog(logName, _options, _logger);
        }

        protected void Subscribe(string channel, Action<RedisChannel, RedisValue> handler)
        {
            ISubscriber sub = GetSubscriber();
            sub.Subscribe(channel, handler);
        }

        protected IDatabase GetRedisDatabase()
        {
            IDatabase database;
            lock (_synclock)
            {
                if (_cacheConnection == null)
                {
                    throw new InvalidOperationException("Cannot get Database from a null connection");
                }
                else
                {
                    database = _cacheConnection.GetDatabase();
                }

            }

            return database;
        }

        protected ISubscriber GetSubscriber()
        {
            ISubscriber sub;
            lock (_synclock)
            {
                if (_cacheConnection == null)
                {
                    throw new InvalidOperationException("Cannot get Subscriber from a null connection");
                }
                else
                {
                    sub = _cacheConnection.GetSubscriber();
                }
            }

            return sub;
        }

        protected async Task CreateStreamIfNecessaryAsync(string streamName)
        {
            // If the stream doesn't exist, create it the only way possible; adding a value.
            // To ensure we don't mess up readers, we then delete the value, leaving an empty stream.
            IDatabase db = GetRedisDatabase();
            if (await db.KeyExistsAsync(streamName) == false)
            {
                RedisValue msgId = await db.StreamAddAsync(
                    streamName,
                    "create",
                    "create",
                    maxLength: MAX_STREAM_LENGTH,
                    useApproximateMaxLength: true)
                    .ConfigureAwait(false);
                await db.StreamDeleteAsync(streamName, new[] { msgId }).ConfigureAwait(false);
            }
        }

        protected Task TrimStreamAsync(string streamName)
        {
            IDatabase db = GetRedisDatabase();
            return db.StreamTrimAsync(streamName, MAX_STREAM_LENGTH, true);
        }

        protected async Task CreateStreamConsumerGroupIfNecessaryAsync(string streamName, string consumerGroup)
        {
            // Check if the consumer group exists; if it doesn't create it
            IDatabase db = GetRedisDatabase();
            StreamGroupInfo[] groups = await db.StreamGroupInfoAsync(streamName).ConfigureAwait(false);
            if (groups.Any(g => string.Equals(g.Name, consumerGroup, StringComparison.OrdinalIgnoreCase)) == false)
            {
                await db.StreamCreateConsumerGroupAsync(streamName, consumerGroup).ConfigureAwait(false);
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
        }

        protected async Task InitializeAsync()
        {
            _cacheConnection = await ConnectionMultiplexer.ConnectAsync(_options).ConfigureAwait(false);
            _cacheConnection.ConnectionFailed += OnConnectionFailure;
            _cacheConnection.ConnectionRestored += OnConnectionRestored;
            _cacheConnection.InternalError += OnRedisInternalError;
            _cacheConnection.IncludeDetailInExceptions = true;
            _cacheConnection.IncludePerformanceCountersInExceptions = true;
        }

        protected Task CleanupAsync()
        {
            if (_cacheConnection == null)
            {
                return Task.CompletedTask;
            }

            ISubscriber sub = GetSubscriber();
            sub.UnsubscribeAll();

            lock (_synclock)
            {
                _cacheConnection.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                _cacheConnection.Dispose();
                _cacheConnection = null;
            }

            return Task.CompletedTask;
        }

        protected virtual string SerializeObject(object o) => JsonConvert.SerializeObject(o, _jsonOptions);
        protected virtual T DeserializeObject<T>(string str) => JsonConvert.DeserializeObject<T>(str, _jsonOptions);
    }
}
