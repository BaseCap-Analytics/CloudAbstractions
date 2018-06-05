using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using Microsoft.Azure.EventHubs;

namespace BaseCap.CloudAbstractions.Implementations.Secure
{
    /// <summary>
    /// Provides seamless encryption for data going into an Event Hub
    /// </summary>
    public class AzureEncryptedEventHubWriter : AzureEventHubWriter
    {
        private byte[] _encryptionKey;

        /// <summary>
        /// Creates a connection to an Azure Event Hub
        /// </summary>
        public AzureEncryptedEventHubWriter(
            string eventHubConnectionString,
            string eventHubEntity,
            byte[] encryptionKey) : base(eventHubConnectionString, eventHubEntity)
        {
            _encryptionKey = encryptionKey;
        }

        /// <summary>
        /// Sends a single event to a specific partition
        /// </summary>
        public override async Task SendEventDataAsync(EventMessage msg, string partition)
        {
            msg.Content = await EncryptionHelpers.EncryptDataAsync(msg.Content, _encryptionKey);
            await base.SendEventDataAsync(msg, partition);
        }

        /// <summary>
        /// Wraps an object into a single event and sends it to a specific partition
        /// </summary>
        public override Task SendEventDataAsync(object obj, string partition)
        {
            return this.SendEventDataAsync(new EventMessage(obj), partition);
        }

        /// <summary>
        /// Wraps objects into a batch of events and sends them to a specific partition
        /// </summary>
        public override Task SendEventDataAsync(IEnumerable<object> msgs, string partition)
        {
            return this.SendEventDataAsync(msgs.Select(o => new EventMessage(o)), partition);
        }

        /// <summary>
        /// Sends a collection of events to a specific partition
        /// </summary>
        public override async Task SendEventDataAsync(IEnumerable<EventMessage> msgs, string partition)
        {
            Queue<EventData> data = new Queue<EventData>(await EncryptMessages(msgs));

            do
            {
                List<EventData> messages = new List<EventData>();
                while ((data.Count > 0) && (messages.Count < MAX_BATCH_SIZE))
                    messages.Add(data.Dequeue());

                await _client.SendAsync(messages, partition);
            }
            while (data.Count > 0);
        }

        private async Task<IEnumerable<EventData>> EncryptMessages(IEnumerable<EventMessage> msgs)
        {
            List<EventData> data = new List<EventData>();
            foreach (EventMessage m in msgs)
            {
                m.Content = await EncryptionHelpers.EncryptDataAsync(m.Content, _encryptionKey);
                data.Add(m.ToEventData());
            }
            return data;
        }
    }
}
