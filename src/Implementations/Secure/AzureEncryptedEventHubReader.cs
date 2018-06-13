using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;

namespace BaseCap.CloudAbstractions.Implementations.Secure
{
    /// <summary>
    /// Provides seamless decryption for data coming out of an Event Hub partition
    /// </summary>
    public class AzureEncryptedEventHubReader : AzureEventHubReader
    {
        private byte[] _encryptionKey;

        /// <summary>
        /// Creates a new connection to an encrypted Event Hub partition
        /// </summary>
        public AzureEncryptedEventHubReader(
            string eventHubConnectionString,
            string eventHubEntity,
            string partitionId,
            string consumerGroup,
            ICheckpointer checkpointer,
            byte[] encryptionKey)
             : base(eventHubConnectionString, eventHubEntity, partitionId, consumerGroup, checkpointer)
        {
            _encryptionKey = encryptionKey;
        }

        /// <summary>
        /// Read up to the number of events, or timeout after a time; whichever happens first
        /// </summary>
        /// <param name="count">The maximum number of events to read</param>
        public override async Task<IEnumerable<EventMessage>> ReadEventsAsync(int count)
        {
            IEnumerable<EventMessage> msgs = await base.ReadEventsAsync(count);
            List<EventMessage> output = new List<EventMessage>(msgs.Count());
            foreach (EventMessage m in msgs)
            {
                m.Content = await EncryptionHelpers.DecryptDataAsync(m.Content, _encryptionKey);
                output.Add(m);
            }

            return output;
        }
    }
}
