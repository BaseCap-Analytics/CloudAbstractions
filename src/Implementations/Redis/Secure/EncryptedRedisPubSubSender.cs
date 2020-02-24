using BaseCap.Security;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text;

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

        internal override string GetNotificationValue(object notification)
        {
            try
            {
                string serialized = base.SerializeObject(notification);
                byte[] plaintext = Encoding.UTF8.GetBytes(serialized);
                byte[] encrypted = EncryptionHelpers.EncryptDataAsync(plaintext, _encryptionKey).ConfigureAwait(false).GetAwaiter().GetResult();
                return Convert.ToBase64String(encrypted);
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Failed encrypting {@Value}", notification);
                DecryptFailures.Inc();
                throw;
            }
        }
    }
}
