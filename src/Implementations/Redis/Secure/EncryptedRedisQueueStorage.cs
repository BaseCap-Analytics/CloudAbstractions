using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using Serilog;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis.Secure
{
    /// <summary>
    /// Provides seamless encryption and decryption for data be passed to and from Azure Blob Queue Storage
    /// </summary>
    public class EncryptedRedisQueueStorage : RedisQueueStorage
    {
        private byte[] _encryptionKey;

        /// <summary>
        /// Creates a new connection to a Azure Queue Storage container with a given encryption key
        /// </summary>
        public EncryptedRedisQueueStorage(List<string> endpoints, string password, string queueName, byte[] encryptionKey)
            : base(endpoints, password, queueName)
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

        protected override async Task OnMessageReceivedAsync(RedisValue message)
        {
            try
            {
                // Decrypt the message then push to the base function for processing
                string decryptedMsg;
                QueueMessage msg = base.DeserializeObject<QueueMessage>(message);
                byte[] encryptedBytes = Convert.FromBase64String(msg.Content);
                byte[] decryptedBytes = await EncryptionHelpers.DecryptDataAsync(encryptedBytes, _encryptionKey).ConfigureAwait(false);
                string decrypted = Encoding.UTF8.GetString(decryptedBytes);
                msg.Content = decrypted;
                decryptedMsg = base.SerializeObject(msg);
                await base.OnMessageReceivedAsync(decryptedMsg);
            }
            catch (Exception ex)
            {
                Log.Logger.Error(ex, "Failed decrypting on Queue {Name}: {Value}", _queueName, message);
                DecryptFailures.Inc();
            }
        }
    }
}
