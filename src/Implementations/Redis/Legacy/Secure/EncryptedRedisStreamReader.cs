using BaseCap.CloudAbstractions.Abstractions.Legacy;
using BaseCap.Security;
using Serilog;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis.Legacy.Secure
{
    /// <summary>
    /// Provides seamless decryption reading from a Redis stream
    /// </summary>
    public class EncryptedRedisStreamReader : RedisStreamReader
    {
        private readonly byte[] _encryptionKey;

        /// <inheritdoc />
        public EncryptedRedisStreamReader(
            List<string> endpoints,
            string password,
            byte[] encryptionKey,
            string streamName,
            string consumerGroup,
            string consumerName,
            bool useSsl)
            : base(endpoints, password, streamName, consumerGroup, consumerName, useSsl)
        {
            _encryptionKey = encryptionKey;
        }

        /// <inheritdoc />
        public EncryptedRedisStreamReader(
            List<string> endpoints,
            string password,
            byte[] encryptionKey,
            string streamName,
            string consumerName,
            bool useSsl)
            : base(endpoints, password, streamName, consumerName, useSsl)
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
                catch (Exception ex)
                {
                    Log.Logger.Error(
                        ex,
                        "Failed decrypting on Stream {Name} Consumer Group {Group} Consumer {Consumer} at {Offset}-{Sequence}: {Value}",
                        _streamName,
                        _consumerGroup,
                        _consumerName,
                        msg.Offset,
                        msg.SequenceNumber,
                        msg.Content?.ToString() ?? string.Empty);
                }
            }

            return messages;
        }
    }
}