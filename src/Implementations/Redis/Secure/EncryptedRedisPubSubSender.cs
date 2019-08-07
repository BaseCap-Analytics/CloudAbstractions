using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis.Secure
{
    /// <summary>
    /// Provides seamless encryption writing data to a Redis Pub/Sub channel
    /// </summary>
    public class EncryptedRedisPubSubSender : RedisPubSubSender
    {
        private readonly byte[] _encryptionKey;

        public EncryptedRedisPubSubSender(
            byte[] encryptionKey,
            IEnumerable<string> endpoints,
            string password,
            string channel,
            bool useSsl,
            ILogger logger)
            : base(endpoints, password, channel, useSsl, logger)
        {
            _encryptionKey = encryptionKey;
        }

        internal override async Task<string> GetNotificationValueAsync(object notification)
        {
            string serialized = JsonConvert.SerializeObject(notification, Formatting.None, _settings);
            byte[] plaintext = Encoding.UTF8.GetBytes(serialized);
            byte[] encrypted = await EncryptionHelpers.EncryptDataAsync(plaintext, _encryptionKey).ConfigureAwait(false);
            return Convert.ToBase64String(encrypted);
        }
    }
}
