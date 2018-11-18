using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Provides a connection for data coming out of an Event Hub partition
    /// </summary>
    public class AzureEventHubReader : IEventStreamReader
    {
        private const int MAX_RETRIES = 3;
        protected string _connectionString;
        protected string _partitionId;
        protected string _consumerGroup;
        protected PartitionReceiver _reader;
        protected EventHubRuntimeInformation _eventHubInfo;
        protected ICheckpointer _checkpointer;
        protected TelemetryClient _logger;
        protected MemoryCache _eventIdCache;

        public string PartitionId => _partitionId;

        public int PartitionCount => _eventHubInfo.PartitionCount;

        /// <summary>
        /// Creates a new connection to a plaintext Event Hub partition
        /// </summary>
        public AzureEventHubReader(
            string eventHubConnectionString,
            string eventHubEntity,
            string partitionId,
            string consumerGroup,
            ICheckpointer checkpointer)
        {
            EventHubsConnectionStringBuilder builder = new EventHubsConnectionStringBuilder(eventHubConnectionString) { EntityPath = eventHubEntity };
            _connectionString = builder.ToString();
            _partitionId = partitionId;
            _consumerGroup = consumerGroup;
            _checkpointer = checkpointer;
            _logger = new TelemetryClient(TelemetryConfiguration.Active);
            _eventIdCache = new MemoryCache(new MemoryCacheOptions());
        }

        /// <summary>
        /// Initializes the connection with Azure from the last read location in the event stream
        /// </summary>
        public async Task SetupAsync()
        {
            string offset = await _checkpointer.GetCheckpointAsync(_partitionId);
            await this.SetupAsync(offset);
        }

        /// <summary>
        /// Initializes the connection with Azure from the specified offset in the event stream
        /// </summary>
        public async Task SetupAsync(string offset)
        {
            EventHubClient client = EventHubClient.CreateFromConnectionString(_connectionString);
            _eventHubInfo = await client.GetRuntimeInformationAsync();

            EventPosition position;
            if (string.IsNullOrEmpty(offset))
            {
                position = EventPosition.FromStart();
            }
            else
            {
                position = EventPosition.FromOffset(offset, false);
            }
            _reader = client.CreateReceiver(_consumerGroup, _partitionId, position);
        }

        /// <summary>
        /// Closes the connection to the event stream
        /// </summary>
        public Task CloseAsync()
        {
            return _reader.CloseAsync();
        }

        /// <summary>
        /// Reads up to a specified number of events from the stream
        /// </summary>
        public virtual async Task<IEnumerable<EventMessage>> ReadEventsAsync(int count)
        {
            for (int i = 0; i < MAX_RETRIES; i++)
            {
                try
                {
                    IEnumerable<EventData> events = await _reader.ReceiveAsync(count);
                    if (events == null)
                    {
                        return Array.Empty<EventMessage>();
                    }
                    else
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
                                messages.Add(new EventMessage(ed));
                            }
                        }

                        return messages;
                    }
                }
                catch (StorageException sx)
                {
                    // This usually means a problem with the reader; rebuild it and try again
                    await SetupAsync();
                    _logger.TrackException(sx, new Dictionary<string, string>()
                    {
                        ["Partition"] = _partitionId,
                        ["ConsumerGroup"] = _consumerGroup,
                        ["RetryCount"] = i.ToString(),
                    });
                }
            }

            _logger.TrackEvent($"{nameof(ReadEventsAsync)} hit max retry count", new Dictionary<string, string>()
            {
                ["Partition"] = _partitionId,
                ["ConsumerGroup"] = _consumerGroup,
            });
            return Array.Empty<EventMessage>();
        }
    }
}
