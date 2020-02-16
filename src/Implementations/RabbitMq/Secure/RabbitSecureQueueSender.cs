using BaseCap.Security;
using RabbitMQ.Client;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.RabbitMq
{
    /// <summary>
    /// Class used to send auto-encrypted messages to a rabbitmq queue
    /// </summary>
    internal class RabbitSecureQueueSender : RabbitQueueSender
    {
        private readonly byte[] _encryptionKey;

        /// <param name="connection">The connection to the Rabbit MQ server</param>
        /// <param name="model">The Rabbit MQ queue to send to</param>
        /// <param name="confirmSend">Flag indicating if we should wait for a confirmation that the message has sent</param>
        /// <param name="exchange">The routing exchange to send to</param>
        /// <param name="queue">The queue to send the message to</param>
        /// <param name="encryptionKey">The key used to encrypt the message</param>
        internal RabbitSecureQueueSender(IConnection connection, IModel model, bool confirmSend, string exchange, string queue, byte[] encryptionKey)
            : base(connection, model, confirmSend, exchange, queue)
        {
            _encryptionKey = encryptionKey;
        }

        /// <inheritdoc />
        internal override async Task<byte[]> GetMessageContentsAsync<T>(T data) where T : class
        {
            byte[] plaintext = await base.GetMessageContentsAsync(data).ConfigureAwait(false);
            byte[] encrypted = await EncryptionHelpers.EncryptDataAsync(plaintext, _encryptionKey).ConfigureAwait(false);
            return encrypted;
        }
    }
}
