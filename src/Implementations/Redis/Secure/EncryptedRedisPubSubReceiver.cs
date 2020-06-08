using BaseCap.Security;
using Serilog;
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
            List<string> endpoints,
            string password,
            byte[] encryptionKey,
            string channel)
            : base(endpoints, password, channel)
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
            catch (Exception ex)
            {
                Log.Logger.Error(ex, "Failed decrypting on Channel {Channel}: {Value}", _channel, value);
                DecryptFailures.Inc();
            }
        }

        internal override string TransformResult(RedisValue value)
        {
            string encoded = (string)value;
            byte[] decoded = Convert.FromBase64String(encoded);
            byte[] decrypted = EncryptionHelpers.DecryptDataAsync(decoded, _encryptionKey).ConfigureAwait(false).GetAwaiter().GetResult();
            return Encoding.UTF8.GetString(decrypted);
        }
    }
}
