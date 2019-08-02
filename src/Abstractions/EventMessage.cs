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
        /// Any user properties of this message
        /// </summary>
        public IDictionary<string, object> Properties { get; private set; }

        /// <summary>
        /// System set properties of this message
        /// </summary>
        public Dictionary<string, object> SystemProperties { get; private set; }

        /// <summary>
        /// Creates a new EventMessage with the specified data
        /// </summary>
        /// <param name="data">The data to put into the Event</param>
        public EventMessage(byte[] data)
        {
            string encoded = Encoding.UTF8.GetString(data);
            Content = encoded;
        }

        /// <summary>
        /// Creates a new EventMessage with the specified object
        /// </summary>
        /// <param name="obj">The object to serialize and put into the event</param>
        public EventMessage(object obj)
        {
            string serialized = JsonConvert.SerializeObject(obj);
            Content = serialized;
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
            Properties = e.Properties;
            SystemProperties = new Dictionary<string, object>()
            {
                [nameof(e.SystemProperties.EnqueuedTimeUtc)] = e.SystemProperties.EnqueuedTimeUtc,
                [nameof(e.SystemProperties.Offset)] = e.SystemProperties.Offset,
                [nameof(e.SystemProperties.PartitionKey)] = e.SystemProperties.PartitionKey,
                [nameof(e.SystemProperties.SequenceNumber)] = e.SystemProperties.SequenceNumber,
            };
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

            // Use the same name as EventHub, to keep consistency
            SystemProperties = new Dictionary<string, object>()
            {
                [nameof(EventData.SystemProperties.EnqueuedTimeUtc)] = enqueuedTimeUtc,
                [nameof(EventData.SystemProperties.Offset)] = id,
                [nameof(EventData.SystemProperties.PartitionKey)] = id,
                [nameof(EventData.SystemProperties.SequenceNumber)] = sequenceNumber,
            };
        }
    }
}
