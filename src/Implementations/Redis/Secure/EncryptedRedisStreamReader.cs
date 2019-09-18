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
        public EncryptedRedisStreamReader(
            byte[] encryptionKey,
            IEnumerable<string> endpoints,
            string password,
            bool useSsl,
            string streamName,
            string consumerName,
            ILogger logger)
            : base(endpoints, password, useSsl, streamName, consumerName, logger)
        {
            _encryptionKey = encryptionKey;
        }

        /// <inheritdoc />
        internal override async Task<List<EventMessage>> ProcessMessagesAsync(StreamEntry[] entries)
        {
            List<EventMessage> messages = await base.ProcessMessagesAsync(entries);
            foreach (EventMessage msg in messages)
            {
                try
                {
                    string? content = msg.Content as string;
                    byte[] encrypted = Convert.FromBase64String(content);
                    byte[] plaintextBytes = await EncryptionHelpers.DecryptDataAsync(encrypted, _encryptionKey).ConfigureAwait(false);
                    string plaintext = Encoding.UTF8.GetString(plaintextBytes);
                    msg.Content = plaintext;
                }
                catch
                {
                    _logger.LogEvent(
                        "InvalidMessageInEncryptedStream",
                        new Dictionary<string, string>()
                        {
                            ["StreamName"] = _streamName,
                            ["ConsumerGroup"] = _consumerGroup,
                            ["ConsumerName"] = _consumerName,
                            ["MessageOffset"] = msg.Offset,
                            ["MessageSequence"] = msg.SequenceNumber.ToString(),
                            ["MessageValue"] = msg.Content?.ToString() ?? string.Empty,
                        });
                }
            }

            return messages;
        }
    }
}
