using System;
using Microsoft.WindowsAzure.Storage.Queue;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Abstraction around message data going into and out of a queue
    /// </summary>
    public class QueueMessage
    {
        /// <summary>
        /// The unique ID of this message
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// The data of the queued message
        /// </summary>
        public byte[] Content { get; set; }

        /// <summary>
        /// A timestamp of when this message was put into the queue
        /// </summary>
        public DateTimeOffset? InsertionTime { get; set; }

        /// <summary>
        /// A timestamp of when this message must be processed by before it will be re-delivered
        /// </summary>
        public DateTimeOffset? ExpirationTime { get; set; }

        /// <summary>
        /// A timestamp of when this message will be visible again; correlated to <see cref="QueueMessage.ExpirationTime" />
        /// </summary>
        public DateTimeOffset? NextVisibleTime { get; set; }

        /// <summary>
        /// The number of times this message has been delivered
        /// </summary>
        public int DequeueCount { get; set; }

        /// <summary>
        /// State identifier used to delete a message
        /// </summary>
        internal string PopReceipt { get; set; }

        /// <summary>
        /// Converts an Azure message into our abstraction
        /// </summary>
        internal QueueMessage(CloudQueueMessage message)
        {
            Id = message.Id;
            Content = message.AsBytes;
            InsertionTime = message.InsertionTime;
            ExpirationTime = message.ExpirationTime;
            NextVisibleTime = message.NextVisibleTime;
            DequeueCount = message.DequeueCount;
            PopReceipt = message.PopReceipt;
        }

        /// <summary>
        /// Creates a blank QueueMessage
        /// </summary>
        public QueueMessage()
        {
        }
    }
}
