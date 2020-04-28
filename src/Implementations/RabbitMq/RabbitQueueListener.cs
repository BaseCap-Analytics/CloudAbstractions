using BaseCap.CloudAbstractions.Abstractions;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
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
        private readonly SortedDictionary<QueueMessage, bool> _messageResults;
        private IConnection _connection;
        private IModel _model;
        private IQueueListenerTarget? _singleTarget;
        private IQueueBatchListenerTarget? _batchTarget;
        private System.Timers.Timer? _timer;
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
            _messageResults = new SortedDictionary<QueueMessage, bool>(Comparer<QueueMessage>.Create((x, y) => x.MessageId.CompareTo(y.MessageId)));
            _timer = null;
            _handler = null;
        }

        protected virtual void Dispose(bool disposing)
        {
            if ((_connection != null) && disposing)
            {
#nullable disable
                if (_timer != null)
                {
                    _timer.Close();
                    _timer.Dispose();
                    _timer = null;
                }
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
        public Task StartListeningAsync(IQueueBatchListenerTarget target)
        {
            _handler = HandleBatchDeliveryAsync;
            _batchTarget = target;
            _model.BasicQos(0, MAX_BATCH_SIZE, false); // We can batch up messages
            _model.BasicConsume(_queue, false, this);
            _timer = new System.Timers.Timer(TimeSpan.FromSeconds(10).TotalMilliseconds);
            _timer.AutoReset = false;
            _timer.Elapsed += TimerEvent;
            _timer.Start();
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
        protected virtual Task<QueueMessage> GetQueueMessageAsync(ReadOnlyMemory<byte> body, IBasicProperties properties, bool redelivered, ulong messageId) =>
            Task.FromResult(new QueueMessage(properties, body, redelivered, messageId));

        /// <inheritdoc />
        public override async Task HandleBasicDeliver(
            string consumerTag,
            ulong deliveryTag,
            bool redelivered,
            string exchange,
            string routingKey,
            IBasicProperties properties,
            ReadOnlyMemory<byte> body)
        {
            QueueMessage msg = await GetQueueMessageAsync(body, properties, redelivered, deliveryTag).ConfigureAwait(false);
            await _handler!.Invoke(msg).ConfigureAwait(false);
        }

        private async Task HandleBatchDeliveryAsync(QueueMessage message)
        {
            lock (_messages)
            {
                _messages.Add(message);
            }

            if (_messages.Count >= MAX_BATCH_SIZE)
            {
                _timer!.Stop();
                await FireMessageBatchReceivedHandlerAsync().ConfigureAwait(false);
                _timer.Start();
            }
        }

        private void TimerEvent(object sender, ElapsedEventArgs e)
        {
            _timer!.Stop();
            FireMessageBatchReceivedHandlerAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            _timer.Start();
        }

        private Task HandleSingleDeliveryAsync(QueueMessage message)
        {
            return _singleTarget!.OnMessageReceived(message);
        }

        // Run this on a threadpool thread so we don't mess with RabbitMQ threads
        private async Task FireMessageBatchReceivedHandlerAsync()
        {
            // Don't do anything if we don't have messages
            if (_messages.Count < 1)
            {
                return;
            }

            // Create an array of the elements to send so we don't block
            QueueMessage[] toSend;
            lock (_messages)
            {
                toSend = new QueueMessage[_messages.Count];
                _messages.CopyTo(toSend, 0);
                _messages.Clear();
            }

            if (toSend.Length > 0)
            {
                await Task.Run(async () => await _batchTarget!.OnMessagesReceivedAsync(toSend).ConfigureAwait(false)).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public void SetBatchCommitResult(bool succeeded)
        {
            // If the batch commit failed then fail all messages
            // since the processing completed but the client failed
            // to commit the result
            if (succeeded == false)
            {
                FireMessageResult(_messageResults.Last().Key, false);
            }
            else
            {
                // Try to short-circuit if all messages passed
                if (_messageResults.Values.All(v => v))
                {
                    FireMessageResult(_messageResults.Last().Key, true);
                }
                else
                {
                    // Not all messages passed to iterate over them all
                    foreach (KeyValuePair<QueueMessage, bool> result in _messageResults)
                    {
                        FireMessageResult(result.Key, result.Value);
                    }
                }
            }
        }

        private void FireMessageResult(QueueMessage message, bool result)
        {
            if (result)
            {
                Model.BasicAck(message.MessageId, true);
            }
            else
            {
                Model.BasicNack(message.MessageId, true, true);
            }
        }

        /// <inheritdoc />
        public void SetMessageResult(QueueMessage message, bool succeeded)
        {
            _messageResults.Add(message, succeeded);
        }
    }
}
