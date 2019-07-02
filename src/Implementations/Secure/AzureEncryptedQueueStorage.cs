using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
        public AzureEncryptedQueueStorage(string serviceBusConnectionString, string queueName, byte[] encryptionKey, ILogger logger)
            : base(serviceBusConnectionString, queueName, logger)
        {
            _encryptionKey = encryptionKey;
        }

        /// <inheritdoc />
        protected override async Task InternalPushObjectAsMessageAsync(object data)
        {
            string serialized = JsonConvert.SerializeObject(data);
            byte[] raw = Encoding.UTF8.GetBytes(serialized);
            byte[] encrypted = await EncryptionHelpers.EncryptDataAsync(raw, _encryptionKey);
            Message m = new Message(encrypted);
            await _queue.SendAsync(m);
        }

        /// <inheritdoc />
        protected override async Task InternalPushObjectAsMessageAsync(object data, TimeSpan initialDelay)
        {
            string serialized = JsonConvert.SerializeObject(data);
            byte[] raw = Encoding.UTF8.GetBytes(serialized);
            byte[] encrypted = await EncryptionHelpers.EncryptDataAsync(raw, _encryptionKey);
            Message m = new Message(encrypted);
            await _queue.ScheduleMessageAsync(m, DateTimeOffset.UtcNow + initialDelay);
        }

        protected override async Task OnMessageReceivedAsync(Message m, CancellationToken token)
        {
            if (m != null)
            {
                byte[] decrypted = await EncryptionHelpers.DecryptDataAsync(m.Body, _encryptionKey);
                m.Body = decrypted;
            }

            await base.OnMessageReceivedAsync(m, token);
        }
    }
}
