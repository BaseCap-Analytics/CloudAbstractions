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
            string streamName,
            ILogger logger)
            : base(endpoints, password, useSsl, streamName, logger)
        {
            _encryptionKey = encryptionKey;
        }

        internal override async Task<string> SerializeDataAsync(object obj)
        {
            string serialized = JsonConvert.SerializeObject(obj);
            byte[] plaintextBytes = Encoding.UTF8.GetBytes(serialized);
            byte[] encryptedBytes = await EncryptionHelpers.EncryptDataAsync(plaintextBytes, _encryptionKey);
            return Convert.ToBase64String(encryptedBytes);
        }
    }
}
