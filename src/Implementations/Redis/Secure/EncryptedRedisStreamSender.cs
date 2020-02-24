using BaseCap.Security;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text;

namespace BaseCap.CloudAbstractions.Implementations.Redis.Secure
{
    /// <summary>
    /// Provides seamless encryption writing to a Redis Stream
    /// </summary>
    public class EncryptedRedisStreamSender : RedisStreamSender
    {
        private readonly byte[] _encryptionKey;

        public EncryptedRedisStreamSender(
            byte[] encryptionKey,
            IEnumerable<string> endpoints,
            string password,
            bool useSsl,
            string streamName)
            : base(endpoints, password, useSsl, streamName)
        {
            _encryptionKey = encryptionKey;
        }

        protected override string SerializeObject(object obj)
        {
            try
            {
                string serialized = base.SerializeObject(obj);
                byte[] plaintextBytes = Encoding.UTF8.GetBytes(serialized);
                byte[] encryptedBytes = EncryptionHelpers.EncryptDataAsync(plaintextBytes, _encryptionKey).ConfigureAwait(false).GetAwaiter().GetResult();
                return Convert.ToBase64String(encryptedBytes);
            }
            catch (Exception ex)
            {
                Log.Logger.Error(ex, "Failed encrypting {@Value}", obj);
                DecryptFailures.Inc();
                throw;
            }
        }
    }
}
