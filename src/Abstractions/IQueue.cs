using System;
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
        Task SetupAsync(Func<QueueMessage, Task> onMessageReceived, int numberOfReaders);

        /// <summary>
        /// Stops reading from the queue
        /// </summary>
        Task StopAsync();

        /// <summary>
        /// Pushes a new object, as a message, into the Queue
        /// </summary>
        /// <param name="data">The object data to serialize and push into the queue</param>
        Task PushObjectAsMessageAsync(object data);

        /// <summary>
        /// Pushes a new object, as a message, into the Queue with an initial delay before the message can be retrieved
        /// </summary>
        /// <param name="data">The object data to serialize and push into the queue</param>
        /// <param name="initialDelay">The amount of time to delay before the message can be delivered</param>
        Task PushObjectAsMessageAsync(object data, TimeSpan initialDelay);

        /// <summary>
        /// Removes the specified message from the queue permanently
        /// </summary>
        /// <param name="msg">The message to remove from the queue</param>
        Task DeleteMessageAsync(QueueMessage msg);
    }
}
