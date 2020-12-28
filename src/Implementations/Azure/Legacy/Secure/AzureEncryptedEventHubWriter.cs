using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System.Text;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Azure.Secure
{
    /// <summary>
    /// Provides seamless encryption for data going into an Event Hub
    /// </summary>
    public class AzureEncryptedEventHubWriter : AzureEventHubWriter
    {
        private readonly byte[] _encryptionKey;

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

        internal override async Task<EventData> GetEventDataAsync(object message)
        {
            string serialized = JsonConvert.SerializeObject(message);
            byte[] data = Encoding.UTF8.GetBytes(serialized);
            byte[] encrypted = await EncryptionHelpers.EncryptDataAsync(data, _encryptionKey).ConfigureAwait(false);
            return new EventData(encrypted);
        }
    }
}