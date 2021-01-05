using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Contract for reading from an Event Stream
    /// </summary>
    public interface IEventStreamReader : IDisposable
    {
        /// <summary>
        /// Initializes the Stream Reader
        /// </summary>
        Task SetupAsync();

        /// <summary>
        /// Begins reading from Event Streams
        /// </summary>
        /// <param name="onMessagesReceived">Callback to fire when a message is received</param>
        /// <param name="maxMessagesToRead">The maximum number of messages to read</param>
        /// <param name="token">The CancellationToken for graceful shutdown</param>
        Task ReadAsync(
            Func<IEnumerable<EventMessage>, string, Task> onMessagesReceived,
            int? maxMessagesToRead,
            CancellationToken token);
    }
}