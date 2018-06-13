using BaseCap.CloudAbstractions.Abstractions;
using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System.Threading;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Provides a connection for data be passed to and from Azure Blob Queue Storage
    /// </summary>
    public class AzureQueueStorage : IQueue
    {
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
                RetryPolicy = new Microsoft.WindowsAzure.Storage.RetryPolicies.ExponentialRetry(),
                ServerTimeout = TimeSpan.FromSeconds(20),
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
    }
}
