using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.LocalTesting
{
    /// <summary>
    /// A connection to a redis queue
    /// </summary>
    public class LocalRedisQueue : IQueue
    {
        private ConnectionMultiplexer _cacheConnection;
        private ISubscriber _queue;
        private string _queueName;
        private Func<QueueMessage, Task> _onMessageReceived;

        /// <summary>
        /// Creates a new LocalRedisQueue
        /// </summary>
        public LocalRedisQueue(string endpoint, string password, string queueName, bool useSsl)
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
            _queueName = queueName;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing && (_cacheConnection != null))
            {
                _cacheConnection.Close();
                _cacheConnection.Dispose();
                _cacheConnection = null;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public Task SetupAsync(Func<QueueMessage, Task> onMessageReceived, int numberOfReaders)
        {
            _onMessageReceived = onMessageReceived;
            _queue.Subscribe(_queueName, OnMessageReceivedAsync);
            return Task.CompletedTask;
        }

        public Task StopAsync()
        {
            return _queue.UnsubscribeAllAsync();
        }

        protected virtual void OnMessageReceivedAsync(RedisChannel channel, RedisValue value)
        {
            QueueMessage msg = new QueueMessage()
            {
                Content = System.Text.Encoding.UTF8.GetBytes(value),
                DequeueCount = 0,
                ExpirationTime = DateTimeOffset.Now,
                Id = string.Empty,
                InsertionTime = DateTimeOffset.Now.AddSeconds(-1),
                LockToken = string.Empty,
                TimeToLive = TimeSpan.FromSeconds(1),
            };
            _onMessageReceived(msg).GetAwaiter().GetResult();
        }

        public Task PushObjectAsMessageAsync(object data)
        {
            string value = JsonConvert.SerializeObject(data);
            return _queue.PublishAsync(_queueName, value);
        }

        public Task PushObjectAsMessageAsync(object data, TimeSpan initialDelay)
        {
            string value = JsonConvert.SerializeObject(data);
            return _queue.PublishAsync(_queueName, value);
        }

        public Task DeleteMessageAsync(QueueMessage msg)
        {
            return Task.CompletedTask; // No-op
        }
    }
}
