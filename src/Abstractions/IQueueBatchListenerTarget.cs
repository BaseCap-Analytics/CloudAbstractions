using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Contract for receiving batches of messages from an IQueueBatchListener
    /// </summary>
    public interface IQueueBatchListenerTarget
    {
        /// <summary>
        /// Callback for when Queue Messages have been received
        /// </summary>
        /// <returns>Returns an awaitable Task</returns>
        Task OnMessagesReceivedAsync(IEnumerable<QueueMessage> messages);
    }
}
