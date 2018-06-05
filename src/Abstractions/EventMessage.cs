using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Abstraction around event data going into and out of an event hub
    /// </summary>
    public class EventMessage
    {
        /// <summary>
        /// Creates a new EventMessage with the specified data
        /// </summary>
        /// <param name="data">The data to put into the Event</param>
        public EventMessage(byte[] data)
        {
            Content = data;
        }

        /// <summary>
        /// Creates a new EventMessage with the specified object
        /// </summary>
        /// <param name="obj">The object to serialize and put into the event</param>
        public EventMessage(object obj)
        {
            string data = JsonConvert.SerializeObject(obj);
            Content = Encoding.UTF8.GetBytes(data);
        }

        /// <summary>
        /// Converts an Azure EventData event into our abstraction
        /// </summary>
        /// <param name="e">The Azure EventHub data</param>
        internal EventMessage(EventData e)
        {
            Content = new byte[e.Body.Count];
            Array.Copy(e.Body.Array, e.Body.Offset, Content, 0, Content.Length);
            Properties = e.Properties;
            SystemProperties = new Dictionary<string, object>()
            {
                [nameof(e.SystemProperties.EnqueuedTimeUtc)] = e.SystemProperties.EnqueuedTimeUtc,
                [nameof(e.SystemProperties.Offset)] = e.SystemProperties.Offset,
                [nameof(e.SystemProperties.PartitionKey)] = e.SystemProperties.PartitionKey,
                [nameof(e.SystemProperties.SequenceNumber)] = e.SystemProperties.SequenceNumber,
            };
        }

        internal EventData ToEventData()
        {
            return new EventData(Content);
        }

        /// <summary>
        /// The data in this Event
        /// </summary>
        public byte[] Content { get; internal set; }

        /// <summary>
        /// Any user properties of this message
        /// </summary>
        public IDictionary<string, object> Properties { get; private set; }

        /// <summary>
        /// System set properties of this message
        /// </summary>
        public Dictionary<string, object> SystemProperties { get; private set; }
    }
}
