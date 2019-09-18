using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Abstraction around event data going into and out of an event hub
    /// </summary>
    public class EventMessage
    {
        /// <summary>
        /// The data in this Event
        /// </summary>
        public object Content { get; internal set; }

        /// <summary>
        /// When this message was enqueued
        /// </summary>
        public DateTimeOffset EnqueuedTimeUtc { get; private set; }

        /// <summary>
        /// The location of this message in the stream
        /// </summary>
        public string Offset { get; private set; }

        /// <summary>
        /// Which partition, if applicable, this message is on
        /// </summary>
        public string PartitionKey { get; private set; }

        /// <summary>
        /// A sequence number, combined with the Offset, give a unique ID of the message
        /// </summary>
        public long SequenceNumber { get; private set; }

        /// <summary>
        /// Any user properties of this message
        /// </summary>
        public IDictionary<string, object> Properties { get; private set; }

        /// <summary>
        /// Creates a new EventMessage with the specified data
        /// </summary>
        /// <param name="data">The data to put into the Event</param>
        public EventMessage(byte[] data)
        {
            string encoded = Encoding.UTF8.GetString(data);
            Content = encoded;
            Offset = string.Empty;
            PartitionKey = string.Empty;
            Properties = new Dictionary<string, object>();
        }

        /// <summary>
        /// Creates a new EventMessage with the specified object
        /// </summary>
        /// <param name="obj">The object to serialize and put into the event</param>
        public EventMessage(object obj)
        {
            string serialized = JsonConvert.SerializeObject(obj);
            Content = serialized;
            Offset = string.Empty;
            PartitionKey = string.Empty;
            Properties = new Dictionary<string, object>();
        }

        /// <summary>
        /// Converts an Azure EventData event into our abstraction
        /// </summary>
        /// <param name="e">The Azure EventHub data</param>
        internal EventMessage(EventData e)
        {
            string decoded = Encoding.UTF8.GetString(e.Body.Array);
            IEnumerable<KeyValuePair<string, string>> value = JsonConvert.DeserializeObject<IEnumerable<KeyValuePair<string, string>>>(decoded);
            Content = value.FirstOrDefault().Value;
            EnqueuedTimeUtc = e.SystemProperties.EnqueuedTimeUtc;
            Offset = e.SystemProperties.Offset;
            PartitionKey = e.SystemProperties.PartitionKey;
            SequenceNumber = e.SystemProperties.SequenceNumber;
            Properties = e.Properties;
        }

        /// <summary>
        /// Converts a Redis Event Stream Entry event into our abstraction
        /// </summary>
        internal EventMessage(string id, NameValueEntry value)
        {
            string[] idParts = ((string)id).Split('-');
            long enqueuedUnixMs = long.Parse(idParts[0]);
            long sequenceNumber = long.Parse(idParts[1]);
            DateTimeOffset enqueuedTimeUtc = DateTimeOffset.FromUnixTimeMilliseconds(enqueuedUnixMs);
            Content = value.Value.ToString();
            EnqueuedTimeUtc = enqueuedTimeUtc;
            Offset = id;
            PartitionKey = id;
            SequenceNumber = sequenceNumber;
            Properties = new Dictionary<string, object>()
            {
                ["Name"] = value.Name,
            };
        }
    }
}
