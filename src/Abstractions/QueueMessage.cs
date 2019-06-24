using Microsoft.Azure.ServiceBus;
using System;

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
        /// A TimeSpan of how long this message has until it is visible again
        /// </summary>
        public TimeSpan TimeToLive { get; set; }

        /// <summary>
        /// The number of times this message has been delivered
        /// </summary>
        public int DequeueCount { get; set; }

        /// <summary>
        /// State identifier used to delete a message
        /// </summary>
        internal string LockToken { get; set; }

        /// <summary>
        /// Converts an Azure message into our abstraction
        /// </summary>
        internal QueueMessage(Message message)
        {
            Id = message.MessageId;
            Content = message.Body;
            InsertionTime = message.SystemProperties.EnqueuedTimeUtc;
            ExpirationTime = message.ExpiresAtUtc;
            TimeToLive = message.TimeToLive;
            DequeueCount = message.SystemProperties.DeliveryCount;
            LockToken = message.SystemProperties.LockToken;
        }

        /// <summary>
        /// Creates a blank QueueMessage
        /// </summary>
        public QueueMessage()
        {
        }
    }
}
