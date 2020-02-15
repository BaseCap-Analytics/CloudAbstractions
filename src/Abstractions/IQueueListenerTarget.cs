using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Contract for receiving messages from an IQueueListener
    /// </summary>
    public interface IQueueListenerTarget
    {
        /// <summary>
        /// Callback for when a Queue Message has been received
        /// </summary>
        /// <returns>Returns an awaitable Task</returns>
        Task OnMessageReceived(QueueMessage message);
    }
}
