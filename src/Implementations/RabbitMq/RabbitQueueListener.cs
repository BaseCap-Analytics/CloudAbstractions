using BaseCap.CloudAbstractions.Abstractions;
using RabbitMQ.Client;
using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.RabbitMq
{
    /// <summary>
    /// A listener to a Rabbit MQ queue that will process queued messages
    /// </summary>
    internal class RabbitQueueListener : AsyncDefaultBasicConsumer, IQueueListener, IDisposable
    {
        private readonly string _queue;
        private IConnection _connection;
        private IModel _model;
        private IQueueListenerTarget? _target;

        /// <summary>
        /// Creates a new RabbitQueueListener
        /// </summary>
        /// <param name="connection">The connection to the Rabbit MQ server</param>
        /// <param name="model">The Rabbit MQ queue to send to</param>
        /// <param name="queue">The queue to listen on</param>

        public RabbitQueueListener(IConnection connection, IModel model, string queue)
            : base(model)
        {
            _queue = queue;
            _connection = connection;
            _model = model;
            _target = null;
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

        /// <inheritdoc />
        public Task StartListeningAsync(IQueueListenerTarget target)
        {
            if (target == null)
            {
                throw new ArgumentNullException(nameof(target));
            }

            _target = target;
            _model.BasicConsume(_queue, false, this);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task StopListeningAsync()
        {
            _model.Close();
            return Task.CompletedTask;
        }

        /// <summary>
        /// Creates a Queue Message from the delivered information
        /// </summary>
        protected virtual Task<QueueMessage> GetQueueMessageAsync(byte[] body, IBasicProperties properties, bool redelivered) =>
            Task.FromResult(new QueueMessage(properties, body, redelivered));

        /// <inheritdoc />
        public override async Task HandleBasicDeliver(
            string consumerTag,
            ulong deliveryTag,
            bool redelivered,
            string exchange,
            string routingKey,
            IBasicProperties properties,
            byte[] body)
        {
            try
            {
                QueueMessage msg = await GetQueueMessageAsync(body, properties, redelivered).ConfigureAwait(false);
                await _target!.OnMessageReceived(msg).ConfigureAwait(false);
            }
            finally
            {
                this.Model.BasicAck(deliveryTag, false);
            }
        }
    }
}
