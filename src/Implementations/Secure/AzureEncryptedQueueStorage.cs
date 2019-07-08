using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
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
        public AzureEncryptedQueueStorage(string endpoint, string password, string queueName, bool useSsl, byte[] encryptionKey, ILogger logger)
            : base(endpoint, password, queueName, useSsl, logger)
        {
            _encryptionKey = encryptionKey;
        }

        /// <inheritdoc />
        protected override async Task InternalPushObjectAsMessageAsync(object data)
        {
            string serialized = JsonConvert.SerializeObject(data);
            byte[] raw = Encoding.UTF8.GetBytes(serialized);
            byte[] rawEncrypted = await EncryptionHelpers.EncryptDataAsync(raw, _encryptionKey);
            string encrypted = Convert.ToBase64String(rawEncrypted);
            QueueMessage msg = new QueueMessage(encrypted);
            string msgString = JsonConvert.SerializeObject(msg);
            await _queue.PublishAsync(_queueName, msgString);
        }

        protected override void OnMessageReceived(RedisChannel queueName, RedisValue message)
        {
            try
            {
                // Decrypt the message then push to the base function for processing
                string decryptedMsg;
                QueueMessage msg = JsonConvert.DeserializeObject<QueueMessage>(message);
                byte[] encryptedBytes = Convert.FromBase64String(msg.Content);
                byte[] decryptedBytes = EncryptionHelpers.DecryptDataAsync(encryptedBytes, _encryptionKey).GetAwaiter().GetResult();
                string decrypted = Encoding.UTF8.GetString(decryptedBytes);
                msg.Content = decrypted;
                decryptedMsg = JsonConvert.SerializeObject(msg);
                base.OnMessageReceived(queueName, decryptedMsg);
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
