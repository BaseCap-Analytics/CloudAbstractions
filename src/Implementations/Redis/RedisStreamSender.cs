using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System;
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
        public Task SendEventDataAsync(IList<object> msgs)
        {
            return SendEventDataAsync(msgs, string.Empty);
        }

        /// <inheritdoc />
        public Task SendEventDataAsync(IList<object> msgs, string partition)
        {
            if (msgs.Any() == false)
            {
                throw new InvalidOperationException("Cannot send empty message");
            }

            NameValueEntry[] entries = new NameValueEntry[msgs.Count()];
            for (int i = 0; i < msgs.Count(); i++)
            {
                entries[i] = new NameValueEntry($"{DATA_FIELD}_{i}", SerializeObject(msgs[i]));
            }

            IDatabase db = GetRedisDatabase();
            return db.StreamAddAsync(_streamName, entries);
        }
    }
}
