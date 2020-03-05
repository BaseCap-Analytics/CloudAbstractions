using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// Redis Pub/Sub implementation for a Notification Sender
    /// </summary>
    public class RedisPubSubSender : RedisBase, INotificationSender
    {
        private readonly string _channel;

        public RedisPubSubSender(string connectionString, string channel)
            : base(connectionString, "Channel", channel)
        {
            _channel = channel;
        }

        /// <inheritdoc />
        public Task SetupAsync()
        {
            return base.InitializeAsync();
        }

        /// <inheritdoc />
        public Task SendNotificationAsync(object notification)
        {
            string serialized = GetNotificationValue(notification);
            if (string.IsNullOrWhiteSpace(serialized))
            {
                throw new InvalidOperationException("Cannot send empty message");
            }

            ISubscriber  sub = GetSubscriber();
            return sub.PublishAsync(_channel, serialized);
        }

        internal virtual string GetNotificationValue(object notification)
        {
            return SerializeObject(notification);
        }
    }
}
