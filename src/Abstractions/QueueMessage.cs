using System;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Abstraction around message data going into and out of a queue
    /// </summary>
    public class QueueMessage
    {
        /// <summary>
        /// The data of the queued message
        /// </summary>
        public string Content { get; set; }

        /// <summary>
        /// A timestamp of when this message was put into the queue
        /// </summary>
        public DateTimeOffset InsertionTime { get; set; }

        /// <summary>
        /// The number of times this message has been delivered
        /// </summary>
        public int DequeueCount { get; set; }

        /// <summary>
        /// Converts an Azure message into our abstraction
        /// </summary>
        internal QueueMessage(string content)
        {
            Content = content;
            InsertionTime = DateTimeOffset.UtcNow;
            DequeueCount = 0;
        }

        /// <summary>
        /// Creates a blank QueueMessage
        /// </summary>
        public QueueMessage()
        {
        }
    }
}
