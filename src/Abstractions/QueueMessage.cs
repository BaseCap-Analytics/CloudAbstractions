using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Text;

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
        /// Flag indicating if there has been an attempt to deliver this message before
        /// </summary>
        public bool ReDelivery { get; set; }

        [JsonProperty]
        internal int DequeueCount { get; set; }

        [JsonProperty]
        internal ulong MessageId { get; set; }

        /// <summary>
        /// Converts a RabbitMQ message into our abstraction
        /// </summary>
        internal QueueMessage(IBasicProperties properties, ReadOnlyMemory<byte> body, bool isReDelivery, ulong messageId)
        {
            Content = Encoding.UTF8.GetString(body.Span);
            InsertionTime = DateTimeOffset.FromUnixTimeMilliseconds(properties.Timestamp.UnixTime);
            ReDelivery = isReDelivery;
            DequeueCount = 0;
            MessageId = messageId;
        }

        /// <summary>
        /// Converts a Redis message into our abstraction
        /// </summary>
        internal QueueMessage(string data)
        {
            Content = data;
            InsertionTime = DateTimeOffset.UtcNow;
            ReDelivery = false;
            DequeueCount = 0;
        }

        /// <summary>
        /// Creates a blank QueueMessage
        /// </summary>
        public QueueMessage()
        {
            Content = string.Empty;
            InsertionTime = DateTimeOffset.UtcNow;
            ReDelivery = false;
            DequeueCount = 0;
        }
    }
}
