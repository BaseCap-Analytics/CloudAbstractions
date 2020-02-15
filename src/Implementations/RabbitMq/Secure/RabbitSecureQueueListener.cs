using BaseCap.CloudAbstractions.Abstractions;
using BaseCap.Security;
using RabbitMQ.Client;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.RabbitMq
{
    /// <summary>
    /// A listener to a Rabbit MQ queue that will decrypt secured messages on-the-fly
    /// </summary>
    internal sealed class RabbitSecureQueueListener : RabbitQueueListener
    {
        private readonly byte[] _encryptionKey;

        /// <summary>
        /// Creates a new RabbitSecureQueueListener
        /// </summary>
        /// <param name="connection">The connection to the Rabbit MQ server</param>
        /// <param name="model">The Rabbit MQ queue to send to</param>
        /// <param name="queue">The queue to listen on</param>
        /// <param name="encryptionKey">The encryption key used to encrypt messages</param>
        internal RabbitSecureQueueListener(IConnection connection, IModel model, string queue, byte[] encryptionKey)
            : base(connection, model, queue)
        {
            _encryptionKey = encryptionKey;
        }

        /// <inheritdoc />
        protected override async Task<QueueMessage> GetQueueMessageAsync(byte[] body, IBasicProperties properties, bool redelivered)
        {
            byte[] decrypted = await EncryptionHelpers.DecryptDataAsync(body, _encryptionKey);
            return new QueueMessage(properties, decrypted, redelivered);
        }
    }
}
