using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;

namespace BaseCap.CloudAbstractions.Implementations.Secure
{
    /// <summary>
    /// Provides seamless encryption and decryption for data be passed to and from Azure Blob Queue Storage
    /// </summary>
    public class AzureEncryptedQueueStorage : AzureQueueStorage
    {
        private byte[] _encryptionKey;

        /// <summary>
        /// Creates a new connection to a Azure Queue Storage container with a given encryption key
        /// </summary>
        public AzureEncryptedQueueStorage(string storageConnectionString, string queueName, byte[] encryptionKey) : base(storageConnectionString, queueName)
        {
            _encryptionKey = encryptionKey;
        }

        /// <summary>
        /// Retrieves the next message from the queue
        /// </summary>
        public override async Task<QueueMessage> GetMessageAsync(TimeSpan visibility, CancellationToken token)
        {
            CloudQueueMessage msg = await _queue.GetMessageAsync(visibility, _options, null, token);
            if (msg == null)
                return null;
            else
            {
                byte[] decrypted = await EncryptionHelpers.DecryptDataAsync(msg.AsBytes, _encryptionKey);
                msg.SetMessageContent(decrypted);
                return new QueueMessage(msg);
            }
        }

        /// <summary>
        /// Wraps an object into a queue message and pushes it onto the queue
        /// </summary>
        public override async Task PushObjectAsMessageAsync(object data)
        {
            string serialized = JsonConvert.SerializeObject(data);
            byte[] raw = Encoding.UTF8.GetBytes(serialized);
            byte[] encrypted = await EncryptionHelpers.EncryptDataAsync(raw, _encryptionKey);
            await _queue.AddMessageAsync(CloudQueueMessage.CreateCloudQueueMessageFromByteArray(encrypted), null, null, _options, null);
        }
    }
}
