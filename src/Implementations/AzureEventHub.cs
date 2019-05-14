using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Class to communicate with an Azure Event Hub
    /// </summary>
    public class AzureEventHub : IEventStreamHub
    {
        private readonly List<AzureEventHubReader> _readers;
        private Func<IEnumerable<EventMessage>, string, Task> _onMessagesReceived;
        private readonly EventHubClient _client;
        private readonly ICheckpointer _checkpointer;
        private readonly ILogger _logger;

        /// <summary>
        /// Creates a new connection with an Azure Event Hub
        /// </summary>
        public AzureEventHub(
            string eventHubConnectionString,
            string eventHubEntity,
            ICheckpointer checkpointer,
            ILogger logger)
        {
            _readers = new List<AzureEventHubReader>();
            _client = EventHubClient.CreateFromConnectionString(
                new EventHubsConnectionStringBuilder(eventHubConnectionString) { EntityPath = eventHubEntity }.ToString());
            _checkpointer = checkpointer;
            _logger = logger;
        }

        /// <summary>
        /// Starts receiving messages from Event Hub
        /// </summary>
        public async Task StartAsync(Func<IEnumerable<EventMessage>, string, Task> onMessagesReceived, string consumerGroup, CancellationToken token)
        {
            _onMessagesReceived = onMessagesReceived;

            Func<string, string, Task<PartitionReceiver>> setupTask = _checkpointer == null ?
                                            (Func<string, string, Task<PartitionReceiver>>)SetupFromEndOfStreamAsync :
                                            (Func<string, string, Task<PartitionReceiver>>)SetupWithCheckpointAsync;
            EventHubRuntimeInformation info = await _client.GetRuntimeInformationAsync();
            foreach (string partition in info.PartitionIds)
            {
                PartitionReceiver receiver = await setupTask(consumerGroup, partition);
                AzureEventHubReader reader = new AzureEventHubReader(receiver, RefreshPartitionReaderAsync, _onMessagesReceived, _logger);
                _readers.Add(reader);
                reader.Open(token);
            }
        }

        /// <summary>
        /// Stops receiving messages from Event Hub
        /// </summary>
        public async Task StopAsync()
        {
            foreach (AzureEventHubReader reader in _readers)
            {
                await reader.CloseAsync();
            }

            _readers.Clear();
        }

        private async Task<PartitionReceiver> SetupWithCheckpointAsync(string consumerGroup, string partitionId)
        {
            string offset = await _checkpointer.GetCheckpointAsync(partitionId);
            EventPosition position;
            if (string.IsNullOrEmpty(offset))
            {
                position = EventPosition.FromStart();
            }
            else
            {
                position = EventPosition.FromOffset(offset, false);
            }

            return _client.CreateReceiver(consumerGroup, partitionId, position);
        }

        private Task<PartitionReceiver> SetupFromEndOfStreamAsync(string consumerGroup, string partitionId)
        {
            return Task.FromResult(_client.CreateReceiver(consumerGroup, partitionId, EventPosition.FromEnd()));
        }

        private Task<PartitionReceiver> RefreshPartitionReaderAsync(PartitionReceiver reader)
        {
            if (_checkpointer == null)
            {
                return SetupWithCheckpointAsync(reader.ConsumerGroupName, reader.PartitionId);
            }
            else
            {
                return SetupFromEndOfStreamAsync(reader.ConsumerGroupName, reader.PartitionId);
            }
        }
    }
}
