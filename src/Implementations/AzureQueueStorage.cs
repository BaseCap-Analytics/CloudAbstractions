using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Provides a connection for data be passed to and from Azure Blob Queue Storage
    /// </summary>
    public class AzureQueueStorage : IQueue, IDisposable
    {
        private const string DEADLETTER_QUEUE = "DEADLETTER";
        private readonly TimeSpan PROCESSING_CHECK_TIMEOUT = TimeSpan.FromMinutes(1);
        private readonly TimeSpan READ_DELAY = TimeSpan.FromSeconds(3);
        protected readonly string _queueName;
        protected readonly ILogger _logger;
        protected readonly ConfigurationOptions _options;
        private readonly string _processingQueueName;
        private ConnectionMultiplexer _cacheConnection;
        protected IDatabase _database;
        private Task _runner;
        private bool _keepReading;
        protected Func<QueueMessage, Task<bool>> _onMessageReceived;

        /// <summary>
        /// Creates a new connection to an Azure Queue Storage
        /// </summary>
        public AzureQueueStorage(string endpoint, string password, string queueName, bool useSsl, ILogger logger)
        {
            _queueName = queueName;
            _processingQueueName = $"{queueName}_processing";
            _logger = logger;
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
            CreateConnection();
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

        private void ResetConnection()
        {
            try
            {
                _cacheConnection.Close(false);
                _cacheConnection.Dispose();
            }
            finally
            {
                CreateConnection();
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing && (_cacheConnection != null))
            {
                _cacheConnection.Close(false);
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
        public Task SetupAsync(Func<QueueMessage, Task<bool>> onMessageReceived)
        {
            _onMessageReceived = onMessageReceived;
            _keepReading = true;
            _runner = Task.Run(ReadFromQueueAsync);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            _keepReading = false;
            await Task.WhenAny(new [] { _runner, Task.Delay(TimeSpan.FromSeconds(5)) });
        }

        private async Task ReadFromQueueAsync()
        {
            DateTimeOffset lastProcessingRead = DateTimeOffset.MinValue;
            while (_keepReading)
            {
                RedisValue value = await _database.ListRightPopLeftPushAsync(_queueName, _processingQueueName).ConfigureAwait(false);
                await ProcessMessageAsync(value, _queueName);

                if ((DateTimeOffset.Now - lastProcessingRead) > PROCESSING_CHECK_TIMEOUT)
                {
                    value = await _database.ListRightPopAsync(_queueName).ConfigureAwait(false);
                    await ProcessMessageAsync(value, _queueName);
                    lastProcessingRead = DateTimeOffset.Now;
                }

                await Task.Delay(READ_DELAY);
            }
        }

        private Task ProcessMessageAsync(RedisValue message, string queueName)
        {
            if (message.IsNullOrEmpty == false)
            {
                return OnMessageReceivedAsync(queueName, message);
            }

            return Task.CompletedTask;
        }

        protected virtual async Task OnMessageReceivedAsync(RedisChannel queueName, RedisValue message)
        {
            try
            {
                // Dequeue our message and push to the processing list
                string processingMsg;
                QueueMessage msg = JsonConvert.DeserializeObject<QueueMessage>(message);
                msg.DequeueCount++;
                processingMsg = JsonConvert.SerializeObject(msg);

                // If we need to deadletter this message, do it and don't send to a listener
                if (msg.DequeueCount > 5)
                {
                    await _database.ListLeftPushAsync(DEADLETTER_QUEUE, processingMsg).ConfigureAwait(false);
                    _logger.LogEvent(
                        "QueueDeadletter",
                        new Dictionary<string, string>()
                        {
                            ["Message"] = processingMsg,
                            ["Queue"] = _queueName,
                        });
                    return;
                }

                // Send to the received and see if they are successful
                bool result = await _onMessageReceived(msg).ConfigureAwait(false);
                if (result == false)
                {
                    // They weren't successful so re-queue the new message
                    await _database.ListLeftPushAsync(_queueName, processingMsg).ConfigureAwait(false);
                }

                // We got a response so delete the message from the processing list
                await _database.ListRemoveAsync(_processingQueueName, processingMsg).ConfigureAwait(false);
            }
            catch
            {
                _logger.LogEvent(
                    "UnknownQueueMessage",
                    new Dictionary<string, string>()
                    {
                        ["QueueName"] = _queueName,
                        ["Message"] = message,
                    });
            }
        }

        private void OnConnectionFailure(object sender, ConnectionFailedEventArgs e)
        {
            _logger.LogException(
                e.Exception,
                new Dictionary<string, string>()
                {
                    ["QueueName"] = _queueName,
                    ["ConnectionType"] = e.ConnectionType.ToString(),
                    ["Endpoint"] = e.EndPoint.ToString(),
                    ["FailureType"] = e.FailureType.ToString(),
                });
            _logger.LogEvent(
                "QueueConnectionFailure",
                new Dictionary<string, string>()
                {
                    ["QueueName"] = _queueName,
                });

            ResetConnection();
        }

        private void OnConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            _logger.LogEvent(
                "QueueConnectionRestored",
                new Dictionary<string, string>()
                {
                    ["QueueName"] = _queueName,
                });
        }

        private void OnError(object sender, RedisErrorEventArgs e)
        {
            _logger.LogException(
                new Exception(e.Message),
                new Dictionary<string, string>()
                {
                    ["QueueName"] = _queueName,
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
                    ["QueueName"] = _queueName,
                    ["ConnectionType"] = e.ConnectionType.ToString(),
                    ["Endpoint"] = e.EndPoint.ToString(),
                    ["Origin"] = e.Origin,
                });

            ResetConnection();
        }

        /// <inheritdoc />
        public async Task PushObjectAsMessageAsync(object data)
        {
            string serialized = JsonConvert.SerializeObject(data);
            QueueMessage msg = await CreateQueueMessageAsync(serialized).ConfigureAwait(false);
            string msgString = JsonConvert.SerializeObject(msg);
            await _database.ListLeftPushAsync(_queueName, msgString).ConfigureAwait(false);
        }

        protected virtual Task<QueueMessage> CreateQueueMessageAsync(string content)
        {
            return Task.FromResult(new QueueMessage(content));
        }
    }
}
