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
        protected readonly string _queueName;
        protected readonly ILogger _logger;
        protected readonly ConfigurationOptions _options;
        private readonly string _processingQueueName;
        private ConnectionMultiplexer _cacheConnection;
        protected ISubscriber _queue;
        private IDatabase _database;
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
                ConnectTimeout = TimeSpan.FromSeconds(30).Milliseconds,
                Password = password,
                Ssl = useSsl,
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
            _queue = _cacheConnection.GetSubscriber();
            _database = _cacheConnection.GetDatabase();
        }

        private void ResetConnection()
        {
            try
            {
                _queue.UnsubscribeAll();
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
            return _queue.SubscribeAsync(_queueName, OnMessageReceived);
        }

        /// <inheritdoc />
        public Task StopAsync()
        {
            return _queue.UnsubscribeAllAsync();
        }

        protected virtual void OnMessageReceived(RedisChannel queueName, RedisValue message)
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
                    _database.ListLeftPush(DEADLETTER_QUEUE, processingMsg);
                    _logger.LogEvent(
                        "QueueDeadletter",
                        new Dictionary<string, string>()
                        {
                            ["Message"] = processingMsg,
                            ["Queue"] = _queueName,
                        });
                    return;
                }
                else
                {
                    _database.ListLeftPush(_processingQueueName, processingMsg);
                }

                // Send to the received and see if they are successful
                bool result = _onMessageReceived(msg).ConfigureAwait(false).GetAwaiter().GetResult();
                if (result == false)
                {
                    // They weren't successful so re-queue the new message
                    _queue.Publish(_queueName, processingMsg);
                }

                // We got a response so delete the message from the processing list
                _database.ListRemove(_processingQueueName, processingMsg);
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
        public Task PushObjectAsMessageAsync(object data)
        {
            return InternalPushObjectAsMessageAsync(data);
        }

        protected virtual Task InternalPushObjectAsMessageAsync(object data)
        {
            string serialized = JsonConvert.SerializeObject(data);
            QueueMessage msg = new QueueMessage(serialized);
            string msgString = JsonConvert.SerializeObject(msg);
            return _queue.PublishAsync(_queueName, msgString);
        }
    }
}
