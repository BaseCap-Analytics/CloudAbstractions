using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Contract for communicating with a collection of Event Streams
    /// </summary>
    public interface IEventStreamHub
    {
        /// <summary>
        /// Begins reading from Event Streams
        /// </summary>
        /// <param name="onMessagesReceived">Callback to fire when messages are received</param>
        /// <param name="consumerGroup">The consumer group to use</param>
        /// <param name="token">The CancellationToken for graceful shutdown</param>
        Task StartAsync(Func<IEnumerable<EventMessage>, string, Task> onMessagesReceived, string consumerGroup, CancellationToken token);

        /// <summary>
        /// Stops reading from Event Streams
        /// </summary>
        Task StopAsync();
    }
}