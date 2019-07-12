using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// Redis Pub/Sub implementation for a Notification Receiver
    /// </summary>
    public class RedisPubSubReceiver : RedisBase, INotificationReceiver
    {
        private string _channel;
        private Action<string> _handler;

        public RedisPubSubReceiver(string endpoint, string password, bool useSsl, ILogger logger)
            : base(endpoint, password, useSsl, logger)
        {
        }

        /// <inheritdoc />
        public async Task SetupAsync(string channel, Action<string> handler)
        {
            _channel = channel;
            _handler = handler;
            await base.SetupAsync().ConfigureAwait(false);
            base.Subscribe(channel, ReceiveHandler);
        }

        /// <inheritdoc />
        Task INotificationReceiver.ShutdownAsync()
        {
            return base.ShutdownAsync();
        }

        protected override void ResetConnection()
        {
            base.ResetConnection();
            base.Subscribe(_channel, ReceiveHandler);
        }

        private void ReceiveHandler(RedisChannel channel, RedisValue value)
        {
            _handler(value);
        }
    }
}
