using BaseCap.CloudAbstractions.Abstractions;
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

        public RedisPubSubSender(IEnumerable<string> endpoints, string password, string channel, bool useSsl, ILogger logger)
            : base(endpoints, password, useSsl, "Channel", channel, logger)
        {
            _channel = channel;
        }

        /// <inheritdoc />
        public Task SetupAsync()
        {
            return base.InitializeAsync();
        }

        /// <inheritdoc />
        public async Task SendNotificationAsync(object notification)
        {
            if (_subscription == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(SendNotificationAsync)}");
            }

            string serialized = GetNotificationValue(notification);
            if (string.IsNullOrWhiteSpace(serialized))
            {
                throw new InvalidOperationException("Cannot send empty message");
            }

            await _subscription.PublishAsync(_channel, serialized).ConfigureAwait(false);
        }

        internal virtual string GetNotificationValue(object notification)
        {
            return SerializeObject(notification);
        }
    }
}
