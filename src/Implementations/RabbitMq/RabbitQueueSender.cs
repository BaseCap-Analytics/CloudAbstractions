using BaseCap.CloudAbstractions.Abstractions;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.RabbitMq
{
    /// <summary>
    /// Class used to send messages to a rabbitmq queue
    /// </summary>
    internal class RabbitQueueSender : IQueueSender, IDisposable
    {
        private const int MAX_RETRIES = 10;
        private readonly bool _confirmSend;
        private readonly string _exchange;
        private readonly string _queue;
        private IConnection _connection;
        private IModel _model;

        /// <summary>
        /// Creates a new RabbitQueueSender
        /// </summary>
        /// <param name="connection">The connection to the Rabbit MQ server</param>
        /// <param name="model">The Rabbit MQ queue to send to</param>
        /// <param name="confirmSend">Flag indicating if we should wait for a confirmation that the message has sent</param>
        /// <param name="exchange">The routing exchange to send to</param>
        /// <param name="queue">The queue to send the message to</param>
        internal RabbitQueueSender(IConnection connection, IModel model, bool confirmSend, string exchange, string queue)
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

            _connection = connection;
            _model = model;
            _confirmSend = confirmSend;
            _exchange = exchange;
            _queue = queue;
        }

        protected virtual void Dispose(bool disposing)
        {
            if ((_connection != null) && disposing)
            {
#nullable disable
                _model.Close();
                _model.Dispose();
                _model = null;
                _connection.Close();
                _connection.Dispose();
                _connection = null;
#nullable enable
            }
        }

        /// <summary>This code added to correctly implement the disposable pattern.</summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Convert an object to a queue message to send
        /// </summary>
        /// <param name="data">The object to send as a message</param>
        /// <returns>Returns the object in a byte-serialized format</returns>
        internal virtual Task<byte[]> GetMessageContentsAsync<T>(T data) where T : class
        {
            string serialized = JsonConvert.SerializeObject(data);
            byte[] body = Encoding.UTF8.GetBytes(serialized);
            return Task.FromResult(body);
        }

        /// <inheritdoc />
        public async Task PublishMessageAsync<T>(T data) where T : class
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            byte[] body = await GetMessageContentsAsync(data).ConfigureAwait(false);
            InternalPublishMessage(body);
        }

        /// <inheritdoc />
        public async Task PublishMessageAsync<T>(IEnumerable<T> data) where T : class
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }
            else if (data.Any() == false)
            {
                throw new ArgumentNullException(nameof(data));
            }

            foreach (T item in data)
            {
                byte[] body = await GetMessageContentsAsync(item).ConfigureAwait(false);
                InternalPublishMessage(body);
            }
        }

        private void InternalPublishMessage(byte[] body)
        {
            int attempts = 0;
            bool publishSucceeded = false;
            IBasicProperties props = _model.CreateBasicProperties();
            props.Persistent = true;

            do
            {
                _model.BasicPublish(_exchange, _queue, props, body);
                attempts++;

                if (_confirmSend)
                {
                    publishSucceeded = _model.WaitForConfirms(TimeSpan.FromSeconds(30));
                }
                else
                {
                    publishSucceeded = true;
                }
            }
            while ((publishSucceeded == false) && (attempts < MAX_RETRIES));
        }
    }
}
