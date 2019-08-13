using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// Provides basic functionality for reading messages in a Redis Stream
    /// </summary>
    public class RedisStreamReader : RedisBase, IEventStreamReader
    {
        private const string CONSUMER_GROUP_LATEST_UNREAD_MESSAGES = ">";
        private readonly TimeSpan POLL_TIMEOUT = TimeSpan.FromSeconds(3);
        private const int MAX_MESSAGES_PER_BATCH = 50;
        protected readonly string _streamName;
        protected readonly string _consumerGroup;
        protected readonly string _consumerName;

        public RedisStreamReader(
            IEnumerable<string> endpoints,
            string password,
            bool useSsl,
            string streamName,
            string consumerGroup,
            string consumerName,
            ILogger logger)
            : base(endpoints, password, useSsl, "EventStreamReader", $"{streamName}.{consumerGroup}:{consumerName}", logger)
        {
            _streamName = streamName;
            _consumerGroup = consumerGroup;
            _consumerName = consumerName;
        }

        public async Task SetupAsync()
        {
            await base.InitializeAsync().ConfigureAwait(false);
            await base.CreateStreamIfNecessaryAsync(_streamName).ConfigureAwait(false);
            await base.CreateStreamConsumerGroupIfNecessaryAsync(_streamName, _consumerGroup).ConfigureAwait(false);
        }

        public async Task ReadAsync(
            Func<IEnumerable<EventMessage>, string, Task> onMessageReceived,
            int? maxMessagesToRead,
            CancellationToken token)
        {
            try
            {
                bool gotMessages = false;
                int maxMessages = maxMessagesToRead ?? MAX_MESSAGES_PER_BATCH;
                while (token.IsCancellationRequested == false)
                {
                    StreamEntry[] messages = await _database.StreamReadGroupAsync(
                        _streamName,
                        _consumerGroup,
                        _consumerName,
                        CONSUMER_GROUP_LATEST_UNREAD_MESSAGES,
                        maxMessages).ConfigureAwait(false);
                    if (messages.Any())
                    {
                        // Process and acknowledge that we received these messages
                        await ProcessMessagesAsync(messages, onMessageReceived).ConfigureAwait(false);
                        await _database.StreamAcknowledgeAsync(_streamName, _consumerGroup, messages.Select(m => m.Id).ToArray()).ConfigureAwait(false);
                        gotMessages = true;
                    }
                    else
                    {
                        gotMessages = false;
                    }

                    // Until the StackExchange library supports blocking operations, we need to poll
                    if (gotMessages == false)
                    {
                        await Task.Delay(POLL_TIMEOUT, token).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogException(
                    ex,
                    new Dictionary<string, string>()
                    {
                        ["StreamName"] = _streamName,
                        ["ConsumerGroup"] = _consumerGroup,
                        ["ConsumerName"] = _consumerName,
                    });
                _logger.LogEvent(
                    "EventStreamReaderFatalError",
                    new Dictionary<string, string>()
                    {
                        ["StreamName"] = _streamName,
                        ["ConsumerGroup"] = _consumerGroup,
                        ["ConsumerName"] = _consumerName,
                    });

                throw;
            }
        }

        internal virtual Task<List<EventMessage>> ProcessMessagesAsync(StreamEntry[] entries)
        {
            List<EventMessage> messages = new List<EventMessage>();

            foreach (StreamEntry e in entries)
            {
                foreach (NameValueEntry nv in e.Values)
                {
                    messages.Add(new EventMessage(e.Id, nv));
                }
            }

            return Task.FromResult(messages);
        }

        private async Task ProcessMessagesAsync(StreamEntry[] entries, Func<IEnumerable<EventMessage>, string, Task> onMessagesReceived)
        {
            try
            {
                List<EventMessage> messages = await ProcessMessagesAsync(entries).ConfigureAwait(false);
                await onMessagesReceived(messages, _streamName).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogException(
                    ex,
                    new Dictionary<string, string>()
                    {
                        ["StreamName"] = _streamName,
                        ["ConsumerGroup"] = _consumerGroup,
                        ["ConsumerName"] = _consumerName,
                        ["EntryCount"] = entries.Length.ToString(),
                    });
            }
        }
    }
}
