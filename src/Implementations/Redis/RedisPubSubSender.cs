using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
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
        Task INotificationSender.SetupAsync()
        {
            return base.SetupAsync();
        }

        /// <inheritdoc />
        public Task SendNotificationAsync(object notification)
        {
            string value = JsonConvert.SerializeObject(notification);
            return _subscription.PublishAsync(_channel, value);
        }
    }
}
