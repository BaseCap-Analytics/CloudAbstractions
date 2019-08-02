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
        public async Task SendNotificationAsync(object notification)
        {
            string value = await GetNotificationValueAsync(notification).ConfigureAwait(false);
            await _subscription.PublishAsync(_channel, value).ConfigureAwait(false);
        }

        internal virtual Task<string> GetNotificationValueAsync(object notification)
        {
            return Task.FromResult(JsonConvert.SerializeObject(notification));
        }
    }
}
