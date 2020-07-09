using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// Redis Pub/Sub implementation for a Notification Sender
    /// </summary>
    public class RedisPubSubSender : RedisBase, INotificationSender
    {
        private readonly string _channel;

        public RedisPubSubSender(List<string> endpoints, string password, string channel, bool useSsl)
            : base(endpoints, password, "Channel", channel, useSsl)
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

            return ExecuteRedisCommandAsync(() =>
            {
                ISubscriber sub = GetSubscriber();
                return sub.PublishAsync(_channel, serialized);
            });
        }

        internal virtual string GetNotificationValue(object notification)
        {
            return SerializeObject(notification);
        }
    }
}
