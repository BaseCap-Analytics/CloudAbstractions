using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Contract for working with Queue Listeners
    /// </summary>
    public interface IQueueListener
    {
        /// <summary>
        /// Begins listening for messages on the queue
        /// </summary>
        /// <param name="target">The application listener for queue messages</param>
        /// <returns>Returns an awaitable task</returns>
        Task StartListeningAsync(IQueueListenerTarget target);

        /// <summary>
        /// Stops listening for messages on the queue
        /// </summary>
        /// <returns>Returns an awaitable task</returns>
        Task StopListeningAsync();
    }
}
