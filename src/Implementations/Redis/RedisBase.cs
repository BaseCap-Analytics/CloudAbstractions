using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
using Prometheus;
using Serilog;
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
        private const int MAX_RETRIES = 5; // 5 attempts at a command
        protected static readonly Counter DecryptFailures = Metrics.CreateCounter("bca_redis_decrypt_failures", "Counts the number of times we failed to decrypt a value in Redis");
        private static readonly Gauge ConnectionsBroken = Metrics.CreateGauge("bca_redis_broken_connections", "Number of currently broken Redis Connections");
        private static readonly Counter InternalErrors = Metrics.CreateCounter("bca_redis_internal_errors", "Number of Redis internal errors hit");
        private readonly ConfigurationOptions _options;
        private readonly JsonSerializerSettings _jsonOptions;
        private readonly object _synclock;
        protected readonly string _errorContextName;
        protected readonly string _errorContextValue;
        private ConnectionMultiplexer? _cacheConnection;

        internal RedisBase(string connectionString, string errorContextName, string errorContextValue)
            : this(ConfigurationOptions.Parse(connectionString), errorContextName, errorContextValue)
        {
        }

        internal RedisBase(ConfigurationOptions options, string errorContextName, string errorContextValue)
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
            _options.ConnectRetry = 3;
            _options.AsyncTimeout = Convert.ToInt32(TimeSpan.FromSeconds(30).TotalMilliseconds);
            _options.ConnectTimeout = Convert.ToInt32(TimeSpan.FromSeconds(30).TotalMilliseconds);
            _options.SyncTimeout = Convert.ToInt32(TimeSpan.FromSeconds(30).TotalMilliseconds);
            _errorContextName = errorContextName;
            _errorContextValue = errorContextValue;
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
            return (IHyperLogLog)new RedisHyperLogLog(logName, _options);
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

        private Task ResetConnectionIfNoConnectionBugAsync(Exception ex)
        {
            // Due to bug https://github.com/StackExchange/StackExchange.Redis/issues/1120, if there's an
            // error with `No connection is available` then we need to rebuild the connection
            if (ex.Message.Contains("No connection is available", StringComparison.OrdinalIgnoreCase))
            {
                if (_cacheConnection != null)
                {
                    _cacheConnection.Close();
                    _cacheConnection.Dispose();
                }

                return InitializeAsync();
            }

            return Task.CompletedTask;
        }

        protected async Task<T> ExecuteRedisCommandAsync<T>(Func<Task<T>> command)
        {
            int attempts = 0;
            do
            {
                try
                {
                    T result = await command().ConfigureAwait(false);
                    return result;
                }
                catch (Exception ex)
                {
                    await ResetConnectionIfNoConnectionBugAsync(ex).ConfigureAwait(false);

                    if (attempts < MAX_RETRIES)
                    {
                        Log.Logger.Warning("Retrying failed Redis Command with error {Msg}", ex.Message);
                        await Task.Delay(TimeSpan.FromSeconds(3)).ConfigureAwait(false);
                        attempts++;
                    }
                    else
                    {
                        Log.Logger.Error(ex, "Max retries hit on Redis Command; Error: {Msg}", ex.Message);
                        throw;
                    }
                }
            }
            while (attempts <= MAX_RETRIES);

#pragma warning disable CS8603
            return default(T);
#pragma warning restore CS8603
        }

        protected IAsyncEnumerable<T>? ExecuteRedisCommandAsync<T>(Func<IAsyncEnumerable<T>> command)
        {
            int attempts = 0;
            do
            {
                try
                {
                    IAsyncEnumerable<T> result = command();
                    return result;
                }
                catch (Exception ex)
                {
                    ResetConnectionIfNoConnectionBugAsync(ex).GetAwaiter().GetResult();

                    // Retry if we aren't past our max retry count
                    if (attempts < MAX_RETRIES)
                    {
                        Log.Logger.Warning("Retrying failed Redis Command with error {Msg}", ex.Message);
                        Task.Delay(TimeSpan.FromSeconds(3)).ConfigureAwait(false).GetAwaiter().GetResult();
                        attempts++;
                    }
                    else
                    {
                        Log.Logger.Error(ex, "Max retries hit on Redis Command; Error: {Msg}", ex.Message);
                        throw;
                    }
                }
            }
            while (attempts <= MAX_RETRIES);

            return null;
        }
        protected async Task ExecuteRedisCommandAsync(Func<Task> command)
        {
            int attempts = 0;
            do
            {
                try
                {
                    await command().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    await ResetConnectionIfNoConnectionBugAsync(ex).ConfigureAwait(false);

                    if (attempts < MAX_RETRIES)
                    {
                        Log.Logger.Warning("Retrying failed Redis Command with error {Msg}", ex.Message);
                        await Task.Delay(TimeSpan.FromSeconds(3)).ConfigureAwait(false);
                        attempts++;
                    }
                    else
                    {
                        Log.Logger.Error(ex, "Max retries hit on Redis Command; Error: {Msg}", ex.Message);
                        throw;
                    }
                }
            }
            while (attempts <= MAX_RETRIES);
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
            Log.Logger.Error(e.Exception, "Redis Connection Failure {Type} on {Name} {Context} at Endpoint {Endpoint}", e.FailureType, _errorContextName, _errorContextValue, e.EndPoint);
            Log.Logger.Warning("Redis Connection Failure at {Name} {Value}", _errorContextName, _errorContextValue);
            ConnectionsBroken.Inc();
        }

        private void OnConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            Log.Logger.Warning("Redis Connection Restored at {Name} {Value}", _errorContextName, _errorContextValue);
            ConnectionsBroken.Dec();
        }

        private void OnRedisInternalError(object sender, InternalErrorEventArgs e)
        {
            Log.Logger.Error(e.Exception, "Redis Internal Failure: {Type} on {Name} {Context} at {Endpoint}", e.Origin, _errorContextName, _errorContextValue, e.EndPoint);
            Log.Logger.Warning("Redis Internal Failure at {Name} {Value}", _errorContextName, _errorContextValue);
            InternalErrors.Inc();
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
