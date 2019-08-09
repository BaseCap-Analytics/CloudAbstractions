using BaseCap.CloudAbstractions.Abstractions;
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
        private const string DEADLETTER_CHANNEL = DEADLETTER_QUEUE + CHANNEL_SUFFIX;
        private const string CHANNEL_SUFFIX = "_notifications";
        private readonly TimeSpan POLLING_FALLBACK_DELAY = TimeSpan.FromMinutes(5);
        protected readonly string _queueName;
        protected readonly string _channelName;
        protected Func<QueueMessage, Task<bool>> _onMessageReceived;
        private Task _pollingFallback;
        private bool _keepPolling;

        /// <summary>
        /// Creates a new connection to an Azure Queue Storage
        /// </summary>
        public RedisQueueStorage(IEnumerable<string> endpoints, string password, string queueName, bool useSsl, ILogger logger)
            : base(endpoints, password, useSsl, "QueueName", queueName, logger)
        {
            _queueName = queueName;
            _channelName = $"{_queueName}{CHANNEL_SUFFIX}";
        }

        /// <inheritdoc />
        public async Task SetupAsync(Func<QueueMessage, Task<bool>> onMessageReceived)
        {
            _onMessageReceived = onMessageReceived;
            await base.InitializeAsync().ConfigureAwait(false);
            base.Subscribe(_channelName, InternalOnMessageReceived);
            _keepPolling = true;
            _pollingFallback = Task.Run(PollingFallbackAsync);
        }

        /// <inheritdoc />
        public Task SetupAsync()
        {
            return base.InitializeAsync();
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            _keepPolling = false;
            await Task.WhenAny(new [] { _pollingFallback, Task.Delay(TimeSpan.FromSeconds(3)) }).ConfigureAwait(false);
            await base.CleanupAsync().ConfigureAwait(false);
        }

        protected override void ResetConnection()
        {
            base.ResetConnection();
            base.Subscribe(_channelName, InternalOnMessageReceived);
        }

        private async Task PollingFallbackAsync()
        {
            // Have a polling fallback just in case the pub/sub fails
            while (_keepPolling)
            {
                try
                {
                    HandleMessage();
                    await Task.Delay(POLLING_FALLBACK_DELAY).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogException(ex);
                }
            }
        }

        private void HandleMessage()
        {
            string work = _database.ListRightPopAsync(_queueName).ConfigureAwait(false).GetAwaiter().GetResult();
            while (string.IsNullOrWhiteSpace(work) == false)
            {
                // The Pub/Sub is used as a shoulder tap to tell us that there is work to do.
                // We use this instead of Blocking Queues because StackExchange.Redis does not
                // support blocking operations on Redis.
                OnMessageReceivedAsync(work).ConfigureAwait(false).GetAwaiter().GetResult();
                work = _database.ListRightPopAsync(_queueName).ConfigureAwait(false).GetAwaiter().GetResult();
            }
        }

        private void InternalOnMessageReceived(RedisChannel channel, RedisValue message)
        {
            // This is a notification that there is work to do, check for the actual work and see if we got it
            HandleMessage();
        }

        protected virtual async Task OnMessageReceivedAsync(RedisValue message)
        {
            try
            {
                // Dequeue our message and push to the processing list
                string processingMsg;
                QueueMessage msg = DeserializeObject<QueueMessage>(message);
                msg.DequeueCount++;
                processingMsg = SerializeObject(msg);

                // If we need to deadletter this message, do it and don't send to a listener
                if (msg.DequeueCount > 25)
                {
                    await _database.ListLeftPushAsync(DEADLETTER_QUEUE, processingMsg).ConfigureAwait(false);
                    await _subscription.PublishAsync(DEADLETTER_CHANNEL, "").ConfigureAwait(false);
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
                    // They weren't successful so re-queue the new message. We call the CreateMessage function
                    // to make sure we re-encrypt the content if necessary.
                    QueueMessage newMessage = await CreateQueueMessageAsync(msg.Content).ConfigureAwait(false);
                    newMessage.DequeueCount = msg.DequeueCount;
                    await PushObjectToQueueAsync(newMessage);
                }
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

        /// <inheritdoc />
        public async Task PushObjectAsMessageAsync(object data)
        {
            string serialized = SerializeObject(data);
            QueueMessage msg = await CreateQueueMessageAsync(serialized).ConfigureAwait(false);
            await PushObjectToQueueAsync(msg);
        }

        private async Task PushObjectToQueueAsync(QueueMessage msg)
        {
            string msgString = SerializeObject(msg);
            await _database.ListLeftPushAsync(_queueName, msgString, flags: CommandFlags.FireAndForget).ConfigureAwait(false);
            await _subscription.PublishAsync(_channelName, "").ConfigureAwait(false);
        }

        protected virtual Task<QueueMessage> CreateQueueMessageAsync(string content)
        {
            return Task.FromResult(new QueueMessage(content));
        }
    }
}
