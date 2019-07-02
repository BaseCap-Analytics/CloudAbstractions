using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Provides a connection for data be passed to and from Azure Blob Queue Storage
    /// </summary>
    public class AzureQueueStorage : IQueue, IDisposable
    {
        private string _connectionString;
        private string _queueName;
        protected IQueueClient _queue;
        protected Func<QueueMessage, Task> _onMessageReceived;
        protected int _numberOfReaders;
        protected ILogger _logger;

        /// <summary>
        /// Creates a new connection to an Azure Queue Storage
        /// </summary>
        public AzureQueueStorage(string serviceBusConnectionString, string queueName, ILogger logger)
        {
            _queue = new QueueClient(serviceBusConnectionString, queueName, ReceiveMode.PeekLock, new RetryExponential(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(60), 5));
            _logger = logger;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing && _queue != null)
            {
                _queue.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                _queue = null;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Initializes the connection into Azure
        /// </summary>
        public Task SetupAsync(Func<QueueMessage, Task> onMessageReceived, int numberOfReaders)
        {
            MessageHandlerOptions options = new MessageHandlerOptions(OnExceptionAsync)
            {
                AutoComplete = false,
                MaxAutoRenewDuration = TimeSpan.FromMinutes(5),
                MaxConcurrentCalls = numberOfReaders,
            };
            _queue.RegisterMessageHandler(OnMessageReceivedAsync, options);
            _onMessageReceived = onMessageReceived;
            _numberOfReaders = numberOfReaders;
            return Task.CompletedTask;
        }

        protected virtual Task OnMessageReceivedAsync(Message m, CancellationToken token)
        {
            if (m != null)
            {
                return _onMessageReceived(new QueueMessage(m));
            }

            return Task.CompletedTask;
        }

        protected virtual Task OnExceptionAsync(ExceptionReceivedEventArgs e)
        {
            _logger.LogException(
                e.Exception,
                new Dictionary<string, string>()
                {
                    ["Action"] = e.ExceptionReceivedContext.Action,
                    ["ClientId"] = e.ExceptionReceivedContext.ClientId,
                    ["Endpoint"] = e.ExceptionReceivedContext.Endpoint,
                    ["EntityPath"] = e.ExceptionReceivedContext.EntityPath,
                });
            return Task.CompletedTask;
        }

        /// <summary>
        /// Deletes the specified message from the queue
        /// </summary>
        public virtual Task DeleteMessageAsync(QueueMessage msg)
        {
            return _queue.CompleteAsync(msg.LockToken);
        }

        /// <inheritdoc />
        public async Task PushObjectAsMessageAsync(object data)
        {
            try
            {
                await InternalPushObjectAsMessageAsync(data).ConfigureAwait(false);
            }
            catch (System.Net.Sockets.SocketException soc)
            {
                await ResetQueueAsync().ConfigureAwait(false);
                _logger.LogEvent(
                    "QueueReset",
                    new Dictionary<string, string>()
                    {
                        ["ExceptionMessage"] = soc.Message,
                        ["Stack"] = soc.StackTrace,
                    });
            }
            catch (System.InvalidOperationException ioex)
            {
                await ResetQueueAsync().ConfigureAwait(false);
                _logger.LogEvent(
                    "QueueReset",
                    new Dictionary<string, string>()
                    {
                        ["ExceptionMessage"] = ioex.Message,
                        ["Stack"] = ioex.StackTrace,
                    });
            }
        }

        /// <inheritdoc />
        public async Task PushObjectAsMessageAsync(object data, TimeSpan initialDelay)
        {
            try
            {
                await InternalPushObjectAsMessageAsync(data, initialDelay).ConfigureAwait(false);
            }
            catch (System.Net.Sockets.SocketException soc)
            {
                await ResetQueueAsync().ConfigureAwait(false);
                _logger.LogEvent(
                    "QueueReset",
                    new Dictionary<string, string>()
                    {
                        ["ExceptionMessage"] = soc.Message,
                        ["Stack"] = soc.StackTrace,
                    });
            }
            catch (System.InvalidOperationException ioex)
            {
                await ResetQueueAsync().ConfigureAwait(false);
                _logger.LogEvent(
                    "QueueReset",
                    new Dictionary<string, string>()
                    {
                        ["ExceptionMessage"] = ioex.Message,
                        ["Stack"] = ioex.StackTrace,
                    });
            }
        }

        protected virtual Task InternalPushObjectAsMessageAsync(object data)
        {
            string serialized = JsonConvert.SerializeObject(data);
            byte[] raw = Encoding.UTF8.GetBytes(serialized);
            Message m = new Message(raw);
            return _queue.SendAsync(m);
        }

        protected virtual Task InternalPushObjectAsMessageAsync(object data, TimeSpan initialDelay)
        {
            string serialized = JsonConvert.SerializeObject(data);
            byte[] raw = Encoding.UTF8.GetBytes(serialized);
            Message m = new Message(raw);
            return _queue.ScheduleMessageAsync(m, DateTimeOffset.UtcNow + initialDelay);
        }

        private async Task ResetQueueAsync()
        {
            try
            {
                await _queue.CloseAsync().ConfigureAwait(false);
            }
            catch
            {
                // No-op
            }

            _queue = new QueueClient(_connectionString, _queueName, ReceiveMode.PeekLock, new RetryExponential(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(60), 5));
            await SetupAsync(_onMessageReceived, _numberOfReaders);
        }
    }
}
