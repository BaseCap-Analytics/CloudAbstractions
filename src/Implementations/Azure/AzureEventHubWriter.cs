using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.Azure.EventHubs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Azure
{
    /// <summary>
    /// Provides a connection for data going into an Event Hub
    /// </summary>
    public class AzureEventHubWriter : IEventStreamWriter
    {
        protected const int MAX_TIMEOUT_IN_SECONDS = 60;
        protected const int MIN_TIMEOUT_IN_SECONDS = 3;
        protected const int MAX_RETRIES = 5;
        protected const int MAX_BATCH_SIZE = 150;

        protected string _connectionString;
        protected EventHubClient _client;

        /// <summary>
        /// Creates a connection to an Azure Event Hub
        /// </summary>
        public AzureEventHubWriter(
            string eventHubConnectionString,
            string eventHubEntity)
        {
            EventHubsConnectionStringBuilder builder = new EventHubsConnectionStringBuilder(eventHubConnectionString) { EntityPath = eventHubEntity };
            _connectionString = builder.ToString();
        }

        /// <summary>
        /// Initializes the connection to Azure
        /// </summary>
        public Task SetupAsync()
        {
            _client = EventHubClient.CreateFromConnectionString(_connectionString);
            _client.RetryPolicy = new RetryExponential(
                TimeSpan.FromSeconds(MIN_TIMEOUT_IN_SECONDS),
                TimeSpan.FromSeconds(MAX_TIMEOUT_IN_SECONDS),
                MAX_RETRIES);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Closes the connection to the stream
        /// </summary>
        public Task CloseAsync()
        {
            return _client.CloseAsync();
        }

        /// <summary>
        /// Sends the event into the specified partition
        /// </summary>
        public virtual async Task SendEventDataAsync(EventMessage msg, string partition)
        {
            using (EventData data = msg.ToEventData())
            {
                await _client.SendAsync(data, partition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Sends the specified object as an event into the specified partition
        /// </summary>
        public virtual Task SendEventDataAsync(object obj, string partition)
        {
            return SendEventDataAsync(new EventMessage(obj), partition);
        }

        /// <summary>
        /// Sends a batch of events into the specified partition
        /// </summary>
        public virtual async Task SendEventDataAsync(IEnumerable<EventMessage> msgs, string partition)
        {
            Queue<EventData> data = new Queue<EventData>(msgs.Select(m => m.ToEventData()));

            do
            {
                List<EventData> messages = new List<EventData>();
                while ((data.Count > 0) && (messages.Count < MAX_BATCH_SIZE))
                    messages.Add(data.Dequeue());

                await _client.SendAsync(messages, partition);
            }
            while (data.Count > 0);
        }

        /// <summary>
        /// Sends the batch of objects as separate events into the specified partition
        /// </summary>
        public virtual Task SendEventDataAsync(IEnumerable<object> msgs, string partition)
        {
            return SendEventDataAsync(msgs.Select(o => new EventMessage(o)), partition);
        }
    }
}
