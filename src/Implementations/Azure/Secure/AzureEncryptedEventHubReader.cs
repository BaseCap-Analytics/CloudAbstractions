using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using Microsoft.Azure.EventHubs;
using Serilog;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Azure.Secure
{
    /// <summary>
    /// Reads encrypted data on an Azure Event Hub
    /// </summary>
    internal class AzureEncryptedEventHubReader : AzureEventHubReader
    {
        private readonly byte[] _encryptionKey;

        internal AzureEncryptedEventHubReader(
            PartitionReceiver reader,
            Func<PartitionReceiver, Task<PartitionReceiver>> receiverRefresh,
            Func<IEnumerable<EventMessage>, string, Task> onMessagesReceived,
            byte[] encryptionKey,
            ILogger logger)
            : base(reader, receiverRefresh, onMessagesReceived, logger)
        {
            _encryptionKey = encryptionKey;
        }

        internal override async Task<EventMessage> GetEventMessageAsync(EventData eventData)
        {
            EventMessage decrypted;
            byte[] decryptedData = await EncryptionHelpers.DecryptDataAsync(eventData.Body.Array, _encryptionKey).ConfigureAwait(false);
            using (EventData data = new EventData(decryptedData))
            {
                data.SystemProperties = eventData.SystemProperties;
                foreach (KeyValuePair<string, object> kv in eventData.Properties)
                {
                    data.Properties.Add(kv);
                }
                decrypted = new EventMessage(data);
            }
            return decrypted;
        }
    }
}
