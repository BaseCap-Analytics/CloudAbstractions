using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Contract for sending messages to a queue service
    /// </summary>
    public interface IQueueSender
    {
        /// <summary>
        /// Publish an object to a queue
        /// </summary>
        /// <returns>Returns an awaitable task</returns>
        Task PublishMessageAsync(object data);

        /// <summary>
        /// Publish a raw string to a queue
        /// </summary>
        /// <returns>Returns an awaitable task</returns>
        Task PublishMessageAsync(string data);
    }
}
