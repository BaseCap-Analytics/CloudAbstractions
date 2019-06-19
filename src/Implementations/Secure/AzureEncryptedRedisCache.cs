using BaseCap.Security;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Secure
{
    /// <summary>
    /// An encrypted connection to a redis cache cluster
    /// </summary>
    public class AzureEncryptedRedisCache : AzureRedisCache
    {
        private byte[] _encryptionKey;

        public AzureEncryptedRedisCache(string endpoint, string password, byte[] encryptionKey) : base(endpoint, password)
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
