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
            if ((endpoints == null) || (endpoints.Any() == false) || (endpoints.Any(e => string.IsNullOrWhiteSpace(e))))
            {
                throw new ArgumentNullException(nameof(endpoints));
            }
            if (string.IsNullOrWhiteSpace(streamName))
            {
                throw new ArgumentNullException(nameof(streamName));
            }
            if (string.IsNullOrWhiteSpace(consumerGroup))
            {
                throw new ArgumentNullException(nameof(consumerGroup));
            }
            if (string.IsNullOrWhiteSpace(consumerName))
            {
                throw new ArgumentNullException(nameof(consumerName));
            }
            if (logger == null)
            {
                throw new ArgumentNullException(nameof(logger));
            }
            _streamName = streamName;
            _consumerGroup = consumerGroup;
            _consumerName = consumerName;
        }

        public RedisStreamReader(
            IEnumerable<string> endpoints,
            string password,
            bool useSsl,
            string streamName,
            string consumerName,
            ILogger logger)
            : base(endpoints, password, useSsl, "EventStreamReader-NonGroup", $"{streamName}:{consumerName}", logger)
        {
            if ((endpoints == null) || (endpoints.Any() == false) || (endpoints.Any(e => string.IsNullOrWhiteSpace(e))))
            {
                throw new ArgumentNullException(nameof(endpoints));
            }
            if (string.IsNullOrWhiteSpace(streamName))
            {
                throw new ArgumentNullException(nameof(streamName));
            }
            if (string.IsNullOrWhiteSpace(consumerName))
            {
                throw new ArgumentNullException(nameof(consumerName));
            }
            if (logger == null)
            {
                throw new ArgumentNullException(nameof(logger));
            }
            _streamName = streamName;
            _consumerGroup = string.Empty;
            _consumerName = consumerName;
        }

        public async Task SetupAsync()
        {
            await base.InitializeAsync().ConfigureAwait(false);
            await base.CreateStreamIfNecessaryAsync(_streamName).ConfigureAwait(false);
            await base.CreateStreamConsumerGroupIfNecessaryAsync(_streamName, _consumerGroup).ConfigureAwait(false);
            await base.TrimStreamAsync(_streamName).ConfigureAwait(false);
        }

        public async Task ReadAsync(
            Func<IEnumerable<EventMessage>, string, Task> onMessageReceived,
            int? maxMessagesToRead,
            CancellationToken token = default(CancellationToken))
        {
            if (_database == null)
            {
                throw new InvalidOperationException($"Must call {nameof(SetupAsync)} before calling {nameof(ReadAsync)}");
            }
            else if (onMessageReceived == null)
            {
                throw new ArgumentNullException(nameof(onMessageReceived));
            }

            Func<int, RedisValue, Task<StreamEntry[]>> readFunction = string.IsNullOrWhiteSpace(_consumerGroup) ?
                                                        (Func<int, RedisValue, Task<StreamEntry[]>>)ReadWithoutConsumerGroupAsync :
                                                        (Func<int, RedisValue, Task<StreamEntry[]>>)ReadUsingConsumerGroupAsync;
            Func<StreamEntry[], Task<RedisValue>> acknowledgeFunction = string.IsNullOrWhiteSpace(_consumerGroup) ?
                                                            (Func<StreamEntry[], Task<RedisValue>>)AcknowledgeReadAsync :
                                                            (Func<StreamEntry[], Task<RedisValue>>)AcknowledgeConsumerGroupReadAsync;
            RedisValue streamPosition;
            if (string.IsNullOrWhiteSpace(_consumerGroup))
            {
                StreamInfo info = await _database.StreamInfoAsync(_streamName).ConfigureAwait(false);
                streamPosition = info.LastGeneratedId;
            }
            else
            {
                streamPosition = StreamPosition.NewMessages;
            }

            try
            {
                bool gotMessages = false;
                int maxMessages = maxMessagesToRead ?? MAX_MESSAGES_PER_BATCH;
                while (token.IsCancellationRequested == false)
                {
                    if (string.IsNullOrWhiteSpace(streamPosition))
                    {
                        throw new InvalidOperationException($"Failed to get StreamPosition for {_streamName} with group {_consumerGroup} on {_consumerName}");
                    }

                    StreamEntry[] messages = await readFunction(maxMessages, streamPosition).ConfigureAwait(false);
                    if ((messages != null) && (messages.Any()))
                    {
                        // Process and acknowledge that we received these messages
                        await ProcessMessagesAsync(messages, onMessageReceived).ConfigureAwait(false);
                        streamPosition = await acknowledgeFunction(messages).ConfigureAwait(false);
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

        private Task<StreamEntry[]> ReadWithoutConsumerGroupAsync(int maxMessages, RedisValue streamPosition)
        {
            return _database!.StreamReadAsync(_streamName, streamPosition, maxMessages);
        }

        private Task<StreamEntry[]> ReadUsingConsumerGroupAsync(int maxMessages, RedisValue streamPosition)
        {
            return _database!.StreamReadGroupAsync(
                        _streamName,
                        _consumerGroup,
                        _consumerName,
                        streamPosition,
                        maxMessages);
        }

        private Task<RedisValue> AcknowledgeReadAsync(StreamEntry[] messages)
        {
            return Task.FromResult(messages.Last().Id);
        }

        private async Task<RedisValue> AcknowledgeConsumerGroupReadAsync(StreamEntry[] messages)
        {
            await _database!.StreamAcknowledgeAsync(_streamName, _consumerGroup, messages.Select(m => m.Id).ToArray());
            return StreamPosition.NewMessages;
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
