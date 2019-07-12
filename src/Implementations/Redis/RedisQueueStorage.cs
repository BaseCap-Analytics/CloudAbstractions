using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// Provides a connection for data be passed to and from Redis Queues
    /// </summary>
    public class RedisQueueStorage : RedisBase, IQueue
    {
        private const string DEADLETTER_QUEUE = "DEADLETTER";
        private readonly TimeSpan PROCESSING_CHECK_TIMEOUT = TimeSpan.FromMinutes(1);
        private readonly TimeSpan READ_DELAY = TimeSpan.FromSeconds(3);
        protected readonly string _queueName;
        private readonly string _processingQueueName;
        private Task _runner;
        private bool _keepReading;
        protected Func<QueueMessage, Task<bool>> _onMessageReceived;

        /// <summary>
        /// Creates a new connection to an Azure Queue Storage
        /// </summary>
        public RedisQueueStorage(string endpoint, string password, string queueName, bool useSsl, ILogger logger)
            : base(endpoint, password, useSsl, logger)
        {
            _queueName = queueName;
            _processingQueueName = $"{queueName}_processing";
        }

        /// <inheritdoc />
        public async Task SetupAsync(Func<QueueMessage, Task<bool>> onMessageReceived)
        {
            await base.SetupAsync();
            _onMessageReceived = onMessageReceived;
            _keepReading = true;
            _runner = Task.Run(ReadFromQueueAsync);
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            _keepReading = false;
            await Task.WhenAny(new [] { _runner, Task.Delay(TimeSpan.FromSeconds(5)) });
            await base.ShutdownAsync().ConfigureAwait(false);
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

        protected override void OnConnectionFailure(object sender, ConnectionFailedEventArgs e)
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

        protected override void OnConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            _logger.LogEvent(
                "QueueConnectionRestored",
                new Dictionary<string, string>()
                {
                    ["QueueName"] = _queueName,
                });
        }

        protected override void OnError(object sender, RedisErrorEventArgs e)
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

        protected override void OnRedisInternalError(object sender, InternalErrorEventArgs e)
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
