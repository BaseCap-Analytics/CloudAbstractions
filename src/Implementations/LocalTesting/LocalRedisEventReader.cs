using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.Azure.EventHubs;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.LocalTesting
{
    /// <summary>
    /// Provides a connection for data coming out of an Event Hub partition
    /// </summary>
    public class LocalRedisEventReader
    {
        private ConnectionMultiplexer _cacheConnection;
        private ISubscriber _queue;
        private string _partitionName;
        protected readonly Func<IEnumerable<EventMessage>, string, Task> _onMessagesReceived;

        /// <summary>
        /// Creates a new connection to a Redis pub/sub
        /// </summary>
        internal LocalRedisEventReader(
            string endpoint,
            string password,
            string partitionName,
            bool useSsl,
            Func<IEnumerable<EventMessage>, string, Task> onMessagesReceived)
        {
            ConfigurationOptions options = new ConfigurationOptions()
            {
                AbortOnConnectFail = false,
                ConnectRetry = 3,
                Password = password,
                Ssl = useSsl,
            };
            options.EndPoints.Add(endpoint);
            _cacheConnection = ConnectionMultiplexer.ConnectAsync(options).GetAwaiter().GetResult();
            _queue = _cacheConnection.GetSubscriber();
            _partitionName = partitionName;
            _onMessagesReceived = onMessagesReceived;
        }

        /// <summary>
        /// Closes the connection to the event stream
        /// </summary>
        internal async Task CloseAsync()
        {
            await _queue.UnsubscribeAllAsync();
            await _cacheConnection.CloseAsync();
            _cacheConnection.Dispose();
        }

        internal void Open(CancellationToken token)
        {
            _queue.Subscribe(_partitionName, OnMessageReceivedAsync);
        }

        protected virtual void OnMessageReceivedAsync(RedisChannel channel, RedisValue value)
        {
            using (EventData d = new EventData(System.Text.Encoding.UTF8.GetBytes(value)))
            {
                d.SystemProperties = new EventData.SystemPropertiesCollection(5, DateTime.Now, Guid.NewGuid().ToString(), _partitionName);
                EventMessage msg = new EventMessage(d);
                _onMessagesReceived(new [] { msg }, _partitionName).GetAwaiter().GetResult();
            }
        }
    }
}
