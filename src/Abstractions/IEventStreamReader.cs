using System;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Contract for reading from an Event Stream
    /// </summary>
    public interface IEventStreamReader
    {
        /// <summary>
        /// Initializes the Stream Reader
        /// </summary>
        Task SetupAsync();

        /// <summary>
        /// Begins reading from Event Streams
        /// </summary>
        /// <param name="onMessagesReceived">Callback to fire when a message is received</param>
        /// <param name="token">The CancellationToken for graceful shutdown</param>
        Task ReadAsync(Func<EventMessage, string, Task> onMessageReceived, CancellationToken token);
    }
}
