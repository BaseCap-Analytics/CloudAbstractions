using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
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
        private readonly ConfigurationOptions _options;
        private readonly JsonSerializerSettings _jsonOptions;
        private readonly object _synclock;
        private readonly bool _useSsl;
        protected readonly string _errorContextName;
        protected readonly string _errorContextValue;
        private ConnectionMultiplexer? _cacheConnection;

        internal RedisBase(List<string> endpoints, string password, string errorContextName, string errorContextValue, bool useSsl)
            : this(new ConfigurationOptions(), errorContextName, errorContextValue, useSsl)
        {
            if (endpoints.Any() == false)
            {
                throw new ArgumentNullException(nameof(endpoints));
            }

            foreach (string e in endpoints)
            {
                _options.EndPoints.Add(e);
            }

            _options.Password = password;
        }

        internal RedisBase(ConfigurationOptions options, string errorContextName, string errorContextValue, bool useSsl)
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
            _options.Ssl = useSsl;
            _options.AbortOnConnectFail = false;
            _errorContextName = errorContextName;
            _errorContextValue = errorContextValue;
            _useSsl = useSsl;
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
            return (IHyperLogLog)new RedisHyperLogLog(logName, _options, _useSsl);
        }

        protected void Subscribe(string channel, Action<RedisChannel, RedisValue> handler)
        {
            ISubscriber sub = GetSubscriber();
            sub.Subscribe(channel, handler);
        }

        protected ChannelMessageQueue Subscribe(string channel)
        {
            ISubscriber sub = GetSubscriber();
            return sub.Subscribe(channel);
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

        private async Task ResetConnectionIfNoConnectionBugAsync(Exception ex)
        {
            // Due to bug https://github.com/StackExchange/StackExchange.Redis/issues/1120, if there's an
            // error with `No connection is available` then the whole redis instance is crapped out.
            if (ex.Message.Contains("No connection is available", StringComparison.OrdinalIgnoreCase))
            {
                await CleanupAsync().ConfigureAwait(false);
                await InitializeAsync().ConfigureAwait(false);
            }

            return;
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

#pragma warning disable CS8603, CS8653
            return default(T);
#pragma warning restore CS8603, CS8653
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
        }

        private void OnConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            Log.Logger.Warning("Redis Connection Restored at {Name} {Value}", _errorContextName, _errorContextValue);
        }

        private void OnRedisInternalError(object sender, InternalErrorEventArgs e)
        {
            Log.Logger.Error(e.Exception, "Redis Internal Failure: {Type} on {Name} {Context} at {Endpoint}", e.Origin, _errorContextName, _errorContextValue, e.EndPoint);
            Log.Logger.Warning("Redis Internal Failure at {Name} {Value}", _errorContextName, _errorContextValue);
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