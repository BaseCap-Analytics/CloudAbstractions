using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Azure
{
    /// <summary>
    /// Provides a connection for data coming out of an Event Hub partition
    /// </summary>
    internal class AzureEventHubReader
    {
        public string PartitionId => _reader.PartitionId;
        private const int MAX_RETRIES = 3;
        private const int MAX_MESSAGES = 100;
        protected PartitionReceiver _reader;
        protected Task _readerTask;
        protected readonly Func<IEnumerable<EventMessage>, string, Task> _onMessagesReceived;
        protected readonly Func<PartitionReceiver, Task<PartitionReceiver>> _receiverRefreshAsync;
        protected readonly ILogger _logger;
        protected readonly MemoryCache _eventIdCache;

        /// <summary>
        /// Creates a new connection to a plaintext Event Hub partition
        /// </summary>
        internal AzureEventHubReader(
            PartitionReceiver reader,
            Func<PartitionReceiver, Task<PartitionReceiver>> receiverRefresh,
            Func<IEnumerable<EventMessage>, string, Task> onMessagesReceived,
            ILogger logger)
        {
            _reader = reader;
            _receiverRefreshAsync = receiverRefresh;
            _onMessagesReceived = onMessagesReceived;
            _logger = logger;
            _eventIdCache = new MemoryCache(new MemoryCacheOptions());
        }

        /// <summary>
        /// Closes the connection to the event stream
        /// </summary>
        internal Task CloseAsync()
        {
            return _reader.CloseAsync();
        }

        internal void Open(CancellationToken token)
        {
            _readerTask = Task.Run(async () => await ReadEventsAsync(token));
        }

        /// <summary>
        /// Reads up to a specified number of events from the stream
        /// </summary>
        internal async Task ReadEventsAsync(CancellationToken token)
        {
            while (token.IsCancellationRequested == false)
            {
                try
                {
                    IEnumerable<EventData> events = await _reader.ReceiveAsync(MAX_MESSAGES);
                    if ((events != null) && (events.Any()))
                    {
                        List<EventMessage> messages = new List<EventMessage>();

                        foreach (EventData ed in events)
                        {
                            string messageId = ed.SystemProperties.Offset;

                            // Check if we have received this message in the last 60 seconds...if so, ignore
                            // it since it has already been processed
                            if (_eventIdCache.TryGetValue(messageId, out _) == false)
                            {
                                _eventIdCache.Set(messageId, messageId, TimeSpan.FromMinutes(1));
                                messages.Add(await GetEventMessageAsync(ed).ConfigureAwait(false));
                            }
                        }

                        await _onMessagesReceived(messages, _reader.PartitionId);
                    }
                }
                catch (StorageException sx)
                {
                    // This usually means a problem with the reader; rebuild it and try again
                    _reader = await _receiverRefreshAsync(_reader);
                    _logger.LogException(
                        sx,
                        new Dictionary<string, string>()
                        {
                            ["Partition"] = _reader.PartitionId,
                            ["ConsumerGroup"] = _reader.ConsumerGroupName,
                        });
                }
            }
        }

        internal virtual Task<EventMessage> GetEventMessageAsync(EventData eventData)
        {
            return Task.FromResult(new EventMessage(eventData));
        }
    }
}
