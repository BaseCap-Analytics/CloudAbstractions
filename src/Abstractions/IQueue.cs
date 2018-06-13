using System;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for interacting with distributed, cloud-based queues
    /// </summary>
    public interface IQueue
    {
        /// <summary>
        /// Constructs the underlying stream connection
        /// </summary>
        Task SetupAsync();

        /// <summary>
        /// Retrieves the next message from the queue and retrieves it
        /// </summary>
        /// <param name="visibility">The time this message should be invisible to other readers</param>
        /// <param name="token">A cancellation token to escape waiting</param>
        /// <returns>Returns a <see cref="BaseCap.Azure.Abstractions.QueueMessage" />
        Task<QueueMessage> GetMessageAsync(TimeSpan visibility, CancellationToken token);

        /// <summary>
        /// Pushes a new object, as a message, into the Queue
        /// </summary>
        /// <param name="data">The object data to serialize and push into the queue</param>
        Task PushObjectAsMessageAsync(object data);

        /// <summary>
        /// Removes the specified message from the queue permanently
        /// </summary>
        /// <param name="msg">The message to remove from the queue</param>
        Task DeleteMessageAsync(QueueMessage msg);
    }
}
