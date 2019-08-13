using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

        internal virtual Task<EventData> GetEventDataAsync(object message)
        {
            string serialized = JsonConvert.SerializeObject(message);
            byte[] data = Encoding.UTF8.GetBytes(serialized);
            return Task.FromResult(new EventData(data));
        }

        /// <summary>
        /// Sends the batch of objects as separate events into the specified partition
        /// </summary>
        public async Task SendEventDataAsync(IList<object> msgs, string partition)
        {
            Queue<EventData> data = new Queue<EventData>(msgs.Select(m => GetEventDataAsync(m).ConfigureAwait(false).GetAwaiter().GetResult()));

            do
            {
                List<EventData> messages = new List<EventData>();
                while ((data.Count > 0) && (messages.Count < MAX_BATCH_SIZE))
                    messages.Add(data.Dequeue());

                await _client.SendAsync(messages, partition);
            }
            while (data.Count > 0);
        }

        public async Task SendEventDataAsync(IList<object> msgs)
        {
            Queue<EventData> data = new Queue<EventData>(msgs.Select(m => GetEventDataAsync(m).ConfigureAwait(false).GetAwaiter().GetResult()));

            do
            {
                List<EventData> messages = new List<EventData>();
                while ((data.Count > 0) && (messages.Count < MAX_BATCH_SIZE))
                    messages.Add(data.Dequeue());

                await _client.SendAsync(messages);
            }
            while (data.Count > 0);
        }
    }
}
