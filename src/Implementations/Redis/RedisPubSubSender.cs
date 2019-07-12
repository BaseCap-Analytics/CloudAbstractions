using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// Redis Pub/Sub implementation for a Notification Sender
    /// </summary>
    public class RedisPubSubSender : RedisBase, INotificationSender
    {
        public RedisPubSubSender(string endpoint, string password, bool useSsl, ILogger logger)
            : base(endpoint, password, useSsl, logger)
        {
        }

        /// <inheritdoc />
        Task INotificationSender.SetupAsync()
        {
            return base.SetupAsync();
        }

        /// <inheritdoc />
        public Task SendNotificationAsync(object notification, string channel)
        {
            string value = JsonConvert.SerializeObject(notification);
            return _subscription.PublishAsync(channel, value);
        }
    }
}
