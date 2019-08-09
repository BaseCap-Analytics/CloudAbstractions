using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace BaseCap.CloudAbstractions.Implementations.Redis.Secure
{
    /// <summary>
    /// Provides seamless decryption reading data from a Redis Pub/Sub channel
    /// </summary>
    public class EncryptedRedisPubSubReceiver : RedisPubSubReceiver
    {
        private readonly byte[] _encryptionKey;

        public EncryptedRedisPubSubReceiver(
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

        internal override void ReceiveHandler(RedisChannel channel, RedisValue value)
        {
            try
            {
                string encoded = (string)value;
                byte[] decoded = Convert.FromBase64String(encoded);
                byte[] decrypted = EncryptionHelpers.DecryptDataAsync(decoded, _encryptionKey).ConfigureAwait(false).GetAwaiter().GetResult();
                string data = Encoding.UTF8.GetString(decrypted);
                base.ReceiveHandler(channel, data);
            }
            catch
            {
                _logger.LogEvent(
                    "UnknownPubSubMessage",
                    new Dictionary<string, string>()
                    {
                        ["Channel"] = _channel,
                        ["Message"] = value,
                    });
            }
        }
    }
}
