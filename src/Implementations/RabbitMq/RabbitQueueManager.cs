using BaseCap.CloudAbstractions.Abstractions;
using RabbitMQ.Client;
using System;

namespace BaseCap.CloudAbstractions.Implementations.RabbitMq
{
    public static class RabbitQueueManager
    {
        private static void CreateRabbitConnection(
            string username,
            string password,
            string virtualHost,
            string hostname,
            out IConnection connection,
            out IModel model)
        {
            if (string.IsNullOrWhiteSpace(username))
            {
                throw new ArgumentNullException(nameof(username));
            }
            else if (string.IsNullOrWhiteSpace(password))
            {
                throw new ArgumentNullException(nameof(password));
            }
            else if (string.IsNullOrWhiteSpace(virtualHost))
            {
                throw new ArgumentNullException(nameof(virtualHost));
            }
            else if (string.IsNullOrWhiteSpace(hostname))
            {
                throw new ArgumentNullException(nameof(hostname));
            }

            ConnectionFactory factory = new ConnectionFactory()
            {
                UserName = username,
                Password = password,
                VirtualHost = virtualHost,
                HostName = hostname,
                AutomaticRecoveryEnabled = true,
                ContinuationTimeout = TimeSpan.FromSeconds(30),
                DispatchConsumersAsync = true,
            };
            connection = factory.CreateConnection();
            model = connection.CreateModel();
        }

        private static void EnsureQueueExists(IConnection connection, IModel model, string queue)
        {
            if (connection == null)
            {
                throw new ArgumentNullException(nameof(connection));
            }
            else if (model == null)
            {
                throw new ArgumentNullException(nameof(model));
            }
            else if (string.IsNullOrWhiteSpace(queue))
            {
                throw new ArgumentNullException(nameof(queue));
            }

            model.QueueDeclare(queue, true, false, false); // Make sure our queue exists
            model.BasicQos(0, 1, false); // Make sure the exchange waits for a message reader to ack that it completed processing before giving it more messages
        }

        /// <summary>
        /// Creates a new Queue Listener to listen for plaintext messages
        /// </summary>
        /// <param name="queue">The queue to send the message to</param>
        /// <param name="username">The username to connect to the queue with</param>
        /// <param name="password">The password to authenticate to the queue with</param>
        /// <param name="host">The RabbitMQ server</param>
        /// <param name="virtualHost">The path to route messages in multi-use server</param>
        /// <returns>Returns an IQueueListener capable of receiving plaintext messages</returns>
        public static IQueueListener CreateListener(
            string queue,
            string username,
            string password,
            string host,
            string virtualHost)
        {
            CreateRabbitConnection(username, password, virtualHost, host, out IConnection connection, out IModel model);
            EnsureQueueExists(connection, model, queue);
            return new RabbitQueueListener(connection, model, queue);
        }

        /// <summary>
        /// Creates a new Queue Listener to listen for encrypted messages
        /// </summary>
        /// <param name="queue">The queue to send the message to</param>
        /// <param name="username">The username to connect to the queue with</param>
        /// <param name="password">The password to authenticate to the queue with</param>
        /// <param name="host">The RabbitMQ server</param>
        /// <param name="virtualHost">The path to route messages in multi-use server</param>
        /// <param name="encryptionKey">The encryption key used to encrypt messages</param>
        /// <returns>Returns an IQueueListener capable of receiving encrypted messages</returns>
        public static IQueueListener CreateSecureListener(
            string queue,
            string username,
            string password,
            string host,
            string virtualHost,
            byte[] encryptionKey)
        {
            CreateRabbitConnection(username, password, virtualHost, host, out IConnection connection, out IModel model);
            EnsureQueueExists(connection, model, queue);
            return new RabbitSecureQueueListener(connection, model, queue, encryptionKey);
        }

        /// <summary>
        /// Creates a new Queue Sender to send plaintext messages
        /// </summary>
        /// <param name="queue">The queue to send the message to</param>
        /// <param name="username">The username to connect to the queue with</param>
        /// <param name="password">The password to authenticate to the queue with</param>
        /// <param name="host">The RabbitMQ server</param>
        /// <param name="virtualHost">The path to route messages in multi-use server</param>
        /// <param name="confirmMessageSent">Flag indicating if we should wait for a confirmation that the message has sent</param>
        /// <param name="encryptionKey">The key used to encrypt the message</param>
        /// <returns>Returns an IQueueSender capable of sending plaintext messages</returns>
        public static IQueueSender CreateSender(
            string queue,
            string username,
            string password,
            string host,
            string virtualHost,
            bool confirmMessageSent)
        {
            CreateRabbitConnection(username, password, virtualHost, host, out IConnection connection, out IModel model);
            EnsureQueueExists(connection, model, queue);
            return new RabbitQueueSender(connection, model, confirmMessageSent, string.Empty, queue);
        }

        /// <summary>
        /// Creates a new Queue Sender to send encrypted messages
        /// </summary>
        /// <param name="queue">The queue to send the message to</param>
        /// <param name="username">The username to connect to the queue with</param>
        /// <param name="password">The password to authenticate to the queue with</param>
        /// <param name="host">The RabbitMQ server</param>
        /// <param name="virtualHost">The path to route messages in multi-use server</param>
        /// <param name="confirmMessageSent">Flag indicating if we should wait for a confirmation that the message has sent</param>
        /// <param name="encryptionKey">The key used to encrypt the message</param>
        /// <returns>Returns an IQueueSender capable of sending encrypted messages</returns>
        public static IQueueSender CreateSecureSender(
            string queue,
            string username,
            string password,
            string host,
            string virtualHost,
            bool confirmMessageSent,
            byte[] encryptionKey)
        {
            CreateRabbitConnection(username, password, virtualHost, host, out IConnection connection, out IModel model);
            EnsureQueueExists(connection, model, queue);
            return new RabbitSecureQueueSender(connection, model, confirmMessageSent, string.Empty, queue, encryptionKey);
        }
    }
}
