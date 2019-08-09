using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// Redis Pub/Sub implementation for a Notification Receiver
    /// </summary>
    public class RedisPubSubReceiver : RedisBase, INotificationReceiver
    {
        protected readonly string _channel;
        private Func<string, Task> _handler;

        public RedisPubSubReceiver(IEnumerable<string> endpoints, string password, string channel, bool useSsl, ILogger logger)
            : base(endpoints, password, useSsl, "Channel", channel, logger)
        {
            _channel = channel;
        }

        /// <inheritdoc />
        public async Task SetupAsync(Func<string, Task> handler)
        {
            _handler = handler;
            await base.InitializeAsync().ConfigureAwait(false);
            base.Subscribe(_channel, ReceiveHandler);
        }

        /// <inheritdoc />
        public Task ShutdownAsync()
        {
            return base.CleanupAsync();
        }

        protected override void ResetConnection()
        {
            base.ResetConnection();
            base.Subscribe(_channel, ReceiveHandler);
        }

        internal virtual void ReceiveHandler(RedisChannel channel, RedisValue value)
        {
            _handler(value).GetAwaiter().GetResult();
        }
    }
}
