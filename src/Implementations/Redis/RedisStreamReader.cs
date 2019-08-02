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
        private readonly string _streamName;
        private readonly string _consumerGroup;
        private readonly string _consumerName;

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

        Task IEventStreamReader.SetupAsync()
        {
            return base.SetupAsync();
        }

        public async Task ReadAsync(Func<EventMessage, string, Task> onMessageReceived, CancellationToken token)
        {
            try
            {
                while (token.IsCancellationRequested == false)
                {
                    StreamEntry[] messages = await _database.StreamReadGroupAsync(
                        _streamName,
                        _consumerGroup,
                        _consumerName,
                        CONSUMER_GROUP_LATEST_UNREAD_MESSAGES,
                        MAX_MESSAGES_PER_BATCH).ConfigureAwait(false);
                    if (messages.Any())
                    {
                        foreach (StreamEntry entry in messages)
                        {
                            await ProcessMessageAsync(entry.Id, entry.Values.First(), onMessageReceived).ConfigureAwait(false);
                        }

                        // Acknowledge that we received these messages
                        await _database.StreamAcknowledgeAsync(_streamName, _consumerGroup, messages.Select(m => m.Id).ToArray()).ConfigureAwait(false);
                    }

                    // Until the StackExchange library supports blocking operations, we need to poll
                    await Task.Delay(POLL_TIMEOUT, token).ConfigureAwait(false);
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

        internal virtual async Task ProcessMessageAsync(string entryId, NameValueEntry value, Func<EventMessage, string, Task> onMessageReceived)
        {
            try
            {
                EventMessage msg = new EventMessage(entryId, value);
                await onMessageReceived(msg, _streamName).ConfigureAwait(false);
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
                        ["MessageId"] = entryId,
                    });
            }
        }
    }
}
