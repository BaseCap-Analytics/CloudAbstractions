using BaseCap.Security;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text;

namespace BaseCap.CloudAbstractions.Implementations.Redis.Legacy.Secure
{
    /// <summary>
    /// An encrypted connection to a redis cache cluster
    /// </summary>
    public class EncryptedRedisCache : RedisCache
    {
        private byte[] _encryptionKey;

        public EncryptedRedisCache(List<string> endpoints, string password, byte[] encryptionKey, bool useSsl)
            : base(endpoints, password, useSsl)
        {
            _encryptionKey = encryptionKey;
        }

        protected override string SerializeObject(object o)
        {
            string strdata = base.SerializeObject(o);
            byte[] data = Encoding.UTF8.GetBytes(strdata);
            byte[] encrypted = EncryptionHelpers.EncryptDataAsync(data, _encryptionKey).ConfigureAwait(false).GetAwaiter().GetResult();
            return Convert.ToBase64String(encrypted);
        }

        protected override T DeserializeObject<T>(string value)
        {
            try
            {
                byte[] encrypted = Convert.FromBase64String(value);
                byte[] decrypted = EncryptionHelpers.DecryptDataAsync(encrypted, _encryptionKey).ConfigureAwait(false).GetAwaiter().GetResult();
                string strdata = Encoding.UTF8.GetString(decrypted);
                return base.DeserializeObject<T>(strdata);
            }
            catch (Exception ex)
            {
                Log.Logger.Error(ex, "Failed decrypting {Value}", value);

#nullable disable // Nullable doesn't work with generics and default
                return default(T);
#nullable enable
            }
        }
    }
}