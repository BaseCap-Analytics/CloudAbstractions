using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.LocalTesting
{
    /// <summary>
    /// Provides a connection for data going into an Event Hub
    /// </summary>
    public class LocalRedisEventWriter : IEventStreamWriter
    {
        private ConnectionMultiplexer _cacheConnection;
        private ISubscriber _queue;

        /// <summary>
        /// Creates a connection to an Azure Event Hub
        /// </summary>
        public LocalRedisEventWriter(
            string endpoint,
            string password,
            bool useSsl)
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
        }

        /// <summary>
        /// Initializes the connection to Azure
        /// </summary>
        public Task SetupAsync()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Closes the connection to the stream
        /// </summary>
        public async Task CloseAsync()
        {
            await _cacheConnection.CloseAsync();
            _cacheConnection.Dispose();
        }

        /// <summary>
        /// Sends the event into the specified partition
        /// </summary>
        public virtual Task SendEventDataAsync(EventMessage msg, string partition)
        {
            string input = JsonConvert.SerializeObject(msg);
            return _queue.PublishAsync(partition, input);
        }

        /// <summary>
        /// Sends the specified object as an event into the specified partition
        /// </summary>
        public virtual Task SendEventDataAsync(object obj, string partition)
        {
            return SendEventDataAsync(new EventMessage(obj), partition);
        }

        /// <summary>
        /// Sends a batch of events into the specified partition
        /// </summary>
        public virtual async Task SendEventDataAsync(IEnumerable<EventMessage> msgs, string partition)
        {
            foreach (EventMessage m in msgs)
            {
                await SendEventDataAsync(m, partition);
            }
        }

        /// <summary>
        /// Sends the batch of objects as separate events into the specified partition
        /// </summary>
        public virtual Task SendEventDataAsync(IEnumerable<object> msgs, string partition)
        {
            return SendEventDataAsync(msgs.Select(o => new EventMessage(o)), partition);
        }
    }
}
