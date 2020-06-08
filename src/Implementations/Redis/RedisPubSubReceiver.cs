using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// Redis Pub/Sub implementation for a Notification Receiver
    /// </summary>
    public class RedisPubSubReceiver : RedisBase, INotificationReceiver
    {
        protected readonly string _channel;
        private Func<string, Task>? _handler;
        private ChannelMessageQueue? _blockingHandler;

        public RedisPubSubReceiver(List<string> endpoints, string password, string channel)
            : base(endpoints, password, "Channel", channel)
        {
            _channel = channel;
        }

        /// <inheritdoc />
        public async Task SetupAsync(Func<string, Task> handler)
        {
            if ((_handler != null) || (_blockingHandler != null))
            {
                throw new InvalidOperationException("Cannot setup when already running");
            }

            _handler = handler;
            await base.InitializeAsync().ConfigureAwait(false);
            base.Subscribe(_channel, ReceiveHandler);
        }

        public async ValueTask SetupBlockingAsync()
        {
            if ((_handler != null) || (_blockingHandler != null))
            {
                throw new InvalidOperationException("Cannot setup when already running");
            }

            await base.InitializeAsync().ConfigureAwait(false);
            _blockingHandler = base.Subscribe(_channel);
        }

        /// <inheritdoc />
        public bool IsSetupForBlocking() => _blockingHandler != null;

        /// <inheritdoc />
        public async ValueTask<string> BlockingReadAsync(CancellationToken token)
        {
            if (_blockingHandler == null)
            {
                throw new InvalidOperationException("Must setup for blocking reads");
            }

            ChannelMessage msg = await _blockingHandler.ReadAsync(token);
            return TransformResult(msg.Message);
        }

        /// <inheritdoc />
        public Task ShutdownAsync()
        {
            return base.CleanupAsync();
        }

        internal virtual void ReceiveHandler(RedisChannel channel, RedisValue value)
        {
            if (_handler == null)
            {
                throw new InvalidOperationException("Handler is null when it shouldn't be");
            }

            _handler(value).GetAwaiter().GetResult();
        }

        internal virtual string TransformResult(RedisValue value) => (string)value;
    }
}
