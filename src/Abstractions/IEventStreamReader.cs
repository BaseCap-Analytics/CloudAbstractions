using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for reading from an eventing system
    /// </summary>
    public interface IEventStreamReader
    {
        /// <summary>
        /// Gets the Partition ID for this event stream
        /// </summary>
        string PartitionId { get; }

        /// <summary>
        /// Gets the total number of partitions in this event stream
        /// </summary>
        int PartitionCount { get; }

        /// <summary>
        /// Constructs the underlying stream connection
        /// </summary>
        Task SetupAsync();

        /// <summary>
        /// Constructs the underlying stream connection at the given offset
        /// </summary>
        /// <param name="offset">The offset into the stream to begin reading from</param>
        Task SetupAsync(string offset);

        /// <summary>
        /// Reads up to the number of specified events from the event system
        /// </summary>
        /// <param name="count">The maximum number of events to retrieve</param>
        /// <returns>Returns an enumeration of events, up to the number specified, on success</returns>
        Task<IEnumerable<EventMessage>> ReadEventsAsync(int count);
    }
}
