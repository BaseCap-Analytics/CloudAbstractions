using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis.Secure
{
    /// <summary>
    /// Provides seamless decryption reading from a Redis stream
    /// </summary>
    public class EncryptedRedisStreamReader : RedisStreamReader
    {
        private readonly byte[] _encryptionKey;

        /// <inheritdoc />
        public EncryptedRedisStreamReader(
            byte[] encryptionKey,
            IEnumerable<string> endpoints,
            string password,
            bool useSsl,
            string streamName,
            string consumerGroup,
            string consumerName,
            ILogger logger)
            : base(endpoints, password, useSsl, streamName, consumerGroup, consumerName, logger)
        {
            _encryptionKey = encryptionKey;
        }

        /// <inheritdoc />
        internal override async Task ProcessMessagesAsync(
            Dictionary<string, NameValueEntry> entries,
            Func<IEnumerable<EventMessage>, string, Task> onMessagesReceived)
        {
            Dictionary<string, NameValueEntry> decryptedEntries = new Dictionary<string, NameValueEntry>();
            foreach (string id in entries.Keys)
            {
                byte[] encrypted = Convert.FromBase64String(entries[id].Value);
                byte[] plaintextBytes = await EncryptionHelpers.DecryptDataAsync(encrypted, _encryptionKey).ConfigureAwait(false);
                string plaintext = Encoding.UTF8.GetString(plaintextBytes);
                decryptedEntries.Add(id, new NameValueEntry(entries[id].Name, plaintext));
            }

            await base.ProcessMessagesAsync(decryptedEntries, onMessagesReceived).ConfigureAwait(false);
        }
    }
}
