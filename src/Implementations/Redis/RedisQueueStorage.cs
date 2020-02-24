using BaseCap.CloudAbstractions.Abstractions;
using Prometheus;
using Serilog;
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
        private static readonly Counter DeadletterCounter = Metrics.CreateCounter("bca_redis_deadletter", "Counts the number of deadletter messages hit");
        private static readonly Counter UnhandledMessageCount = Metrics.CreateCounter("bca_redis_unhandled_queue_message_error", "Counts the number of messages not properly received");
        private const string DEADLETTER_QUEUE = "DEADLETTER";
        private const string DEADLETTER_CHANNEL = DEADLETTER_QUEUE + CHANNEL_SUFFIX;
        private const string CHANNEL_SUFFIX = "_notifications";
        private readonly TimeSpan POLLING_FALLBACK_DELAY = TimeSpan.FromMinutes(5);
        protected readonly string _queueName;
        protected readonly string _channelName;
        protected Func<QueueMessage, Task<bool>>? _onMessageReceived;
        private Task? _pollingFallback;
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
                    _logger.Error(ex, "Redis Queue Polling Error");
                }
            }
        }

        private void HandleMessage()
        {
            IDatabase db = GetRedisDatabase();
            string work = db.ListRightPopAsync(_queueName).ConfigureAwait(false).GetAwaiter().GetResult();
            while (string.IsNullOrWhiteSpace(work) == false)
            {
                // The Pub/Sub is used as a shoulder tap to tell us that there is work to do.
                // We use this instead of Blocking Queues because StackExchange.Redis does not
                // support blocking operations on Redis.
                OnMessageReceivedAsync(work).ConfigureAwait(false).GetAwaiter().GetResult();
                work = db.ListRightPopAsync(_queueName).ConfigureAwait(false).GetAwaiter().GetResult();
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
                msg.ReDelivery = msg.DequeueCount > 1;
                processingMsg = SerializeObject(msg);

                // If we need to deadletter this message, do it and don't send to a listener
                if (msg.DequeueCount > 25)
                {
                    IDatabase db = GetRedisDatabase();
                    ISubscriber sub = GetSubscriber();
                    await db.ListLeftPushAsync(DEADLETTER_QUEUE, processingMsg).ConfigureAwait(false);
                    await sub.PublishAsync(DEADLETTER_CHANNEL, "").ConfigureAwait(false);
                    _logger.Warning("Redis Queue {QueueName} Deadletter: {Message}", _queueName, processingMsg);
                    DeadletterCounter.Inc();
                    return;
                }

                // Send to the received and see if they are successful
                bool result = await _onMessageReceived!(msg).ConfigureAwait(false);
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
                _logger.Warning("Redis Queue {QueueName} Unhandled Message: {Message}", _queueName, message);
                UnhandledMessageCount.Inc();
            }
        }

        /// <inheritdoc />
        public async Task PushObjectAsMessageAsync(object data)
        {
            string serialized = SerializeObject(data);
            if (string.IsNullOrWhiteSpace(serialized))
            {
                throw new InvalidOperationException("Cannot send empty message");
            }

            QueueMessage msg = await CreateQueueMessageAsync(serialized).ConfigureAwait(false);
            await PushObjectToQueueAsync(msg);
        }

        private async Task PushObjectToQueueAsync(QueueMessage msg)
        {
            string serialized = SerializeObject(msg);
            if (string.IsNullOrWhiteSpace(serialized))
            {
                throw new InvalidOperationException("Cannot send empty message");
            }

            IDatabase db = GetRedisDatabase();
            ISubscriber sub = GetSubscriber();
            await db.ListLeftPushAsync(_queueName, serialized, flags: CommandFlags.FireAndForget).ConfigureAwait(false);
            await sub.PublishAsync(_channelName, "").ConfigureAwait(false);
        }

        protected virtual Task<QueueMessage> CreateQueueMessageAsync(string content)
        {
            return Task.FromResult(new QueueMessage(content));
        }
    }
}
