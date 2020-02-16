using BaseCap.CloudAbstractions.Abstractions;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace BaseCap.CloudAbstractions.Implementations.RabbitMq
{
    /// <summary>
    /// A listener to a Rabbit MQ queue that will process queued messages
    /// </summary>
    internal class RabbitQueueListener : AsyncDefaultBasicConsumer, IQueueListener, IDisposable
    {
        private const int MAX_BATCH_SIZE = 100; // arbitrary
        private readonly string _queue;
        private readonly List<QueueMessage> _messages;
        private IConnection _connection;
        private IModel _model;
        private IQueueListenerTarget? _singleTarget;
        private IQueueBatchListenerTarget? _batchTarget;
        private System.Timers.Timer? _fallback;
        private AutoResetEvent? _lock;
        private Func<QueueMessage, Task>? _handler;

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
            _singleTarget = null;
            _batchTarget = null;
            _messages = new List<QueueMessage>(MAX_BATCH_SIZE);
            _fallback = null;
            _lock = null;
            _handler = null;
        }

        protected virtual void Dispose(bool disposing)
        {
            if ((_connection != null) && disposing)
            {
#nullable disable
                if (_fallback != null)
                {
                    _fallback.Close();
                    _fallback.Dispose();
                    _fallback = null;
                }
                _model.Close();
                _model.Dispose();
                _model = null;
                _connection.Close();
                _connection.Dispose();
                _connection = null;

                if (_lock != null)
                {
                    _lock.Close();
                    _lock.Dispose();
                    _lock = null;
                }
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

        private void FallbackTimerEvent(object sender, ElapsedEventArgs e)
        {
            _lock!.WaitOne(TimeSpan.FromSeconds(60));

            try
            {
                if (_messages.Count > 0)
                {
                    FireMessageBatchReceivedHandlerAsync(_messages.Last().MessageId).ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }
            finally
            {
                _lock.Set();
                _fallback!.Start();
            }
        }

        /// <inheritdoc />
        public Task StartListeningAsync(IQueueBatchListenerTarget target)
        {
            _handler = HandleBatchDeliveryAsync;
            _lock = new AutoResetEvent(true);
            _batchTarget = target;
            _model.BasicQos(0, MAX_BATCH_SIZE, false); // We can batch up messages
            _model.BasicConsume(_queue, false, this);
            _fallback = new System.Timers.Timer(TimeSpan.FromSeconds(10).TotalMilliseconds);
            _fallback.AutoReset = false;
            _fallback.Elapsed += FallbackTimerEvent;
            _fallback.Start();
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task StartListeningAsync(IQueueListenerTarget target)
        {
            if (target == null)
            {
                throw new ArgumentNullException(nameof(target));
            }

            _handler = HandleSingleDeliveryAsync;
            _singleTarget = target;
            _model.BasicQos(0, 1, false); // Make sure the exchange waits for a message reader to ack that it completed processing before giving it more messages
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
        protected virtual Task<QueueMessage> GetQueueMessageAsync(byte[] body, IBasicProperties properties, bool redelivered, ulong messageId) =>
            Task.FromResult(new QueueMessage(properties, body, redelivered, messageId));

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
            QueueMessage msg = await GetQueueMessageAsync(body, properties, redelivered, deliveryTag).ConfigureAwait(false);
            await _handler!.Invoke(msg).ConfigureAwait(false);
        }

        private async Task HandleBatchDeliveryAsync(QueueMessage message)
        {
            _lock!.WaitOne(TimeSpan.FromSeconds(60));

            try
            {
                _messages.Add(message);
                if (_messages.Count >= MAX_BATCH_SIZE)
                {
                    await FireMessageBatchReceivedHandlerAsync(message.MessageId).ConfigureAwait(false);
                }
            }
            finally
            {
                _lock.Set();
            }
        }

        private async Task HandleSingleDeliveryAsync(QueueMessage message)
        {
            try
            {
                await _singleTarget!.OnMessageReceived(message).ConfigureAwait(false);
            }
            finally
            {
                this.Model.BasicAck(message.MessageId, false);
            }
        }

        // Run this on a threadpool thread so we don't mess with RabbitMQ threads
        private async Task FireMessageBatchReceivedHandlerAsync(ulong lastReceivedMessageId)
        {
            try
            {
                await Task.Run(async () => await _batchTarget!.OnMessagesReceivedAsync(_messages).ConfigureAwait(false)).ConfigureAwait(false);
                _messages.Clear();
            }
            finally
            {
                this.Model.BasicAck(lastReceivedMessageId, true);
            }
        }
    }
}
