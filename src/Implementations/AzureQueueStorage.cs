using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Provides a connection for data be passed to and from Azure Blob Queue Storage
    /// </summary>
    public class AzureQueueStorage : IQueue
    {
        private static readonly TimeSpan TIMEOUT = TimeSpan.FromSeconds(20);
        private static readonly IRetryPolicy RETRY_POLICY = new ExponentialRetry();
        protected CloudQueue _queue;
        protected QueueRequestOptions _options;

        /// <summary>
        /// Creates a new connection to an Azure Queue Storage container
        /// </summary>
        public AzureQueueStorage(string storageConnectionString, string queueName)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(storageConnectionString);
            _queue = account.CreateCloudQueueClient().GetQueueReference(queueName);
            _options = new QueueRequestOptions()
            {
                RetryPolicy = RETRY_POLICY,
                ServerTimeout = TIMEOUT,
            };
        }

        /// <summary>
        /// Creates a new connection to an Azure Queue Storage container
        /// </summary>
        internal AzureQueueStorage(CloudStorageAccount account, string queueName)
        {
            _queue = account.CreateCloudQueueClient().GetQueueReference(queueName);
            _options = new QueueRequestOptions()
            {
                RetryPolicy = RETRY_POLICY,
                ServerTimeout = TIMEOUT,
            };
        }

        /// <summary>
        /// Initializes the connection into Azure
        /// </summary>
        public Task SetupAsync()
        {
            return _queue.CreateIfNotExistsAsync(_options, null);
        }

        /// <summary>
        /// Deletes the specified message from the queue
        /// </summary>
        public virtual async Task DeleteMessageAsync(QueueMessage msg)
        {
            await _queue.DeleteMessageAsync(msg.Id, msg.PopReceipt);
        }

        /// <summary>
        /// Retrieves the next message from the queue
        /// </summary>
        public virtual async Task<QueueMessage> GetMessageAsync(TimeSpan visibility, CancellationToken token)
        {
            CloudQueueMessage msg = await _queue.GetMessageAsync(visibility, _options, null, token);
            if (msg == null)
                return null;
            else
                return new QueueMessage(msg);
        }

        /// <summary>
        /// Adds a new object onto the queue
        /// </summary>
        public virtual async Task PushObjectAsMessageAsync(object data)
        {
            string serialized = JsonConvert.SerializeObject(data);
            byte[] raw = Encoding.UTF8.GetBytes(serialized);
            await _queue.AddMessageAsync(CloudQueueMessage.CreateCloudQueueMessageFromByteArray(raw), null, null, _options, null);
        }

        /// <inheritdoc />
        public virtual async Task PushObjectAsMessageAsync(object data, TimeSpan initialDelay)
        {
            string serialized = JsonConvert.SerializeObject(data);
            byte[] raw = Encoding.UTF8.GetBytes(serialized);
            await _queue.AddMessageAsync(CloudQueueMessage.CreateCloudQueueMessageFromByteArray(raw), null, initialDelay, _options, null);
        }
    }
}
