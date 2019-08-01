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
    /// An encrypted connection to a redis cache cluster
    /// </summary>
    public class EncryptedRedisCache : RedisCache
    {
        private byte[] _encryptionKey;

        public EncryptedRedisCache(IEnumerable<string> endpoints, string password, bool useSsl, byte[] encryptionKey, ILogger logger)
            : base(endpoints, password, useSsl, logger)
        {
            _encryptionKey = encryptionKey;
        }

        protected override async Task<string> SerializeObject(object o)
        {
            string strdata = JsonConvert.SerializeObject(o);
            byte[] data = Encoding.UTF8.GetBytes(strdata);
            byte[] encrypted = await EncryptionHelpers.EncryptDataAsync(data, _encryptionKey);
            return Convert.ToBase64String(encrypted);
        }

        protected override async Task<T> DeserializeObject<T>(string value)
        {
            byte[] encrypted = Convert.FromBase64String(value);
            byte[] decrypted = await EncryptionHelpers.DecryptDataAsync(encrypted, _encryptionKey);
            string strdata = Encoding.UTF8.GetString(decrypted);
            return JsonConvert.DeserializeObject<T>(strdata);
        }
    }
}
