using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for interacting with distributed, cloud-based queues
    /// </summary>
    public interface IQueue : IDisposable
    {
        /// <summary>
        /// Constructs the underlying stream connection for write connections
        /// </summary>
        Task SetupAsync();

        /// <summary>
        /// Constructs the underlying stream connection for read connections
        /// </summary>
        Task SetupAsync(Func<QueueMessage, Task<bool>> onMessageReceived);

        /// <summary>
        /// Stops reading from the queue
        /// </summary>
        Task StopAsync();

        /// <summary>
        /// Pushes a new object, as a message, into the Queue
        /// </summary>
        /// <param name="data">The object data to serialize and push into the queue</param>
        Task PushObjectAsMessageAsync(object data);
    }
}
