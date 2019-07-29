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
        private const string DEADLETTER_CHANNEL = DEADLETTER_QUEUE + CHANNEL_SUFFIX;
        private const string CHANNEL_SUFFIX = "_notifications";
        protected readonly string _queueName;
        protected readonly string _channelName;
        protected Func<QueueMessage, Task<bool>> _onMessageReceived;

        /// <summary>
        /// Creates a new connection to an Azure Queue Storage
        /// </summary>
        public RedisQueueStorage(string endpoint, string password, string queueName, bool useSsl, ILogger logger)
            : base(endpoint, password, useSsl, "QueueName", queueName, logger)
        {
            _queueName = queueName;
            _channelName = $"{_queueName}{CHANNEL_SUFFIX}";
        }

        /// <inheritdoc />
        public async Task SetupAsync(Func<QueueMessage, Task<bool>> onMessageReceived)
        {
            _onMessageReceived = onMessageReceived;
            await base.SetupAsync();
            base.Subscribe(_channelName, InternalOnMessageReceived);
        }

        /// <inheritdoc />
        public Task StopAsync()
        {
            return base.ShutdownAsync();
        }

        protected override void ResetConnection()
        {
            base.ResetConnection();
            base.Subscribe(_channelName, InternalOnMessageReceived);
        }

        private void InternalOnMessageReceived(RedisChannel channel, RedisValue message)
        {
            // This is a notification that there is work to do, check for the actual work and see if we got it
            string work = _database.ListRightPopAsync(_queueName).ConfigureAwait(false).GetAwaiter().GetResult();
            if (string.IsNullOrWhiteSpace(work))
            {
                return;
            }
            else
            {
                // The Pub/Sub is used as a shoulder tap to tell us that there is work to do.
                // We use this instead of Blocking Queues because StackExchange.Redis does not
                // support blocking operations on Redis.
                OnMessageReceivedAsync(work).ConfigureAwait(false).GetAwaiter().GetResult();
            }
        }

        protected virtual async Task OnMessageReceivedAsync(RedisValue message)
        {
            try
            {
                // Dequeue our message and push to the processing list
                string processingMsg;
                QueueMessage msg = JsonConvert.DeserializeObject<QueueMessage>(message);
                msg.DequeueCount++;
                processingMsg = JsonConvert.SerializeObject(msg);

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
            string serialized = JsonConvert.SerializeObject(data);
            QueueMessage msg = await CreateQueueMessageAsync(serialized).ConfigureAwait(false);
            await PushObjectToQueueAsync(msg);
        }

        private async Task PushObjectToQueueAsync(QueueMessage msg)
        {
            string msgString = JsonConvert.SerializeObject(msg);
            await _database.ListLeftPushAsync(_queueName, msgString, flags: CommandFlags.FireAndForget).ConfigureAwait(false);
            await _subscription.PublishAsync(_channelName, "").ConfigureAwait(false);
        }

        protected virtual Task<QueueMessage> CreateQueueMessageAsync(string content)
        {
            return Task.FromResult(new QueueMessage(content));
        }
    }
}
