using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Azure.Secure
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
        public AzureEncryptedQueueStorage(string endpoint, string password, string queueName, bool useSsl, byte[] encryptionKey, ILogger logger)
            : base(endpoint, password, queueName, useSsl, logger)
        {
            _encryptionKey = encryptionKey;
        }

        protected override async Task<QueueMessage> CreateQueueMessageAsync(string content)
        {
            byte[] raw = Encoding.UTF8.GetBytes(content);
            byte[] rawEncrypted = await EncryptionHelpers.EncryptDataAsync(raw, _encryptionKey).ConfigureAwait(false);
            string encrypted = Convert.ToBase64String(rawEncrypted);
            return new QueueMessage(encrypted);
        }

        protected override async Task OnMessageReceivedAsync(RedisChannel queueName, RedisValue message)
        {
            try
            {
                // Decrypt the message then push to the base function for processing
                string decryptedMsg;
                QueueMessage msg = JsonConvert.DeserializeObject<QueueMessage>(message);
                byte[] encryptedBytes = Convert.FromBase64String(msg.Content);
                byte[] decryptedBytes = await EncryptionHelpers.DecryptDataAsync(encryptedBytes, _encryptionKey).ConfigureAwait(false);
                string decrypted = Encoding.UTF8.GetString(decryptedBytes);
                msg.Content = decrypted;
                decryptedMsg = JsonConvert.SerializeObject(msg);
                await base.OnMessageReceivedAsync(queueName, decryptedMsg);
            }
            catch
            {
                _logger.LogEvent(
                    "UnknownEncryptedQueueMessage",
                    new Dictionary<string, string>()
                    {
                        ["QueueName"] = _queueName,
                        ["Message"] = message,
                    });
            }
        }
    }
}
