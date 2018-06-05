using BaseCap.CloudAbstractions.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.EventHubs;
using Microsoft.WindowsAzure.Storage;

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
        protected string _offset;
        protected TimeSpan _timeout;
        protected PartitionReceiver _reader;
        protected EventHubRuntimeInformation _eventHubInfo;
        protected ICheckpointer _checkpointer;
        protected TelemetryClient _logger;

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
            TimeSpan maxWaitTime,
            ICheckpointer checkpointer)
        {
            EventHubsConnectionStringBuilder builder = new EventHubsConnectionStringBuilder(eventHubConnectionString) { EntityPath = eventHubEntity };
            _connectionString = builder.ToString();
            _partitionId = partitionId;
            _consumerGroup = consumerGroup;
            _timeout = maxWaitTime;
            _checkpointer = checkpointer;
            _logger = new TelemetryClient(TelemetryConfiguration.Active);
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
        /// Reads up to a specified number of events from the stream, or waits a specified amount of time
        /// for the events before returning
        /// </summary>
        public virtual async Task<IEnumerable<EventMessage>> ReadEventsAsync(int count, TimeSpan timeout)
        {
            for (int i = 0; i < MAX_RETRIES; i++)
            {
                try
                {
                    IEnumerable<EventData> events = await _reader.ReceiveAsync(count, timeout);
                    if (events == null)
                        return Array.Empty<EventMessage>();
                    else
                        return events.Select(s => new EventMessage(s));
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
