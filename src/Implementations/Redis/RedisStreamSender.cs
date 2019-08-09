using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// Provides write access to a Redis stream
    /// </summary>
    public class RedisStreamSender : RedisBase, IEventStreamWriter
    {
        private const string DATA_FIELD = "data";
        private readonly string _streamName;

        public RedisStreamSender(
            IEnumerable<string> endpoints,
            string password,
            bool useSsl,
            string streamName,
            ILogger logger)
            : base(endpoints, password, useSsl, "EventStreamWriter", $"{streamName}", logger)
        {
            _streamName = streamName;
        }

        /// <inheritdoc />
        public async Task SetupAsync()
        {
            await base.InitializeAsync().ConfigureAwait(false);
            await base.CreateStreamIfNecessaryAsync(_streamName).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public Task CloseAsync()
        {
            return base.CleanupAsync();
        }

        /// <inheritdoc />
        public Task SendEventDataAsync(object obj)
        {
            return SendEventDataAsync(obj, string.Empty);
        }

        /// <inheritdoc />
        public async Task SendEventDataAsync(object obj, string partition)
        {
            string data = SerializeObject(obj);
            await _database.StreamAddAsync(_streamName, DATA_FIELD, data).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public Task SendEventDataAsync(IEnumerable<object> msgs)
        {
            return SendEventDataAsync(msgs, string.Empty);
        }

        /// <inheritdoc />
        public Task SendEventDataAsync(IEnumerable<object> msgs, string partition)
        {
            NameValueEntry[] entries = msgs.Select(m => new NameValueEntry(DATA_FIELD, SerializeObject(m)))
                                            .ToArray();
            return _database.StreamAddAsync(_streamName, entries);
        }
    }
}
