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
        /// <returns>Returns true when the messages are correctly processed; otherwise, returns false</returns>
        Task<bool> OnMessageReceived(QueueMessage message);
    }
}
