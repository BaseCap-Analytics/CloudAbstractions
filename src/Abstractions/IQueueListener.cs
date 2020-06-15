using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Contract for working with Queue Listeners
    /// </summary>
    public interface IQueueListener : IDisposable
    {
        /// <summary>
        /// Begins listening for messages on the queue
        /// </summary>
        /// <param name="target">The application listener for queue messages</param>
        /// <returns>Returns an awaitable task</returns>
        Task StartListeningAsync(IQueueListenerTarget target);

        /// <summary>
        /// Begins listening for batches of messages on the queue
        /// </summary>
        /// <param name="target">The application listener for queue messages</param>
        /// <param name="maxBatchSize">The maximum number of entries that can be returned in one batch</param>
        /// <returns>Returns an awaitable task</returns>
        Task StartListeningAsync(IQueueBatchListenerTarget target, ushort maxBatchSize);

        /// <summary>
        /// Stops listening for messages on the queue
        /// </summary>
        /// <returns>Returns an awaitable task</returns>
        Task StopListeningAsync();

        /// <summary>
        /// Sets the processing result of a single message
        /// </summary>
        /// <param name="message">The message to set the result of</param>
        /// <param name="succeeded">True to signal the message was successfully processed; otherwise, false</param>
        void SetMessageResult(QueueMessage message, bool succeeded);

        /// <summary>
        /// Sets the processing result of the act of committing the batch of results
        /// </summary>
        /// <param name="succeeded">True to signal the batch was successfully processed; otherwise, false</param>
        void SetBatchCommitResult(bool succeeded);
    }
}
