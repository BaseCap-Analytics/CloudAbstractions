using System.Collections.Generic;
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
        Task PublishMessageAsync<T>(T data) where T : class;

        /// <summary>
        /// Publish multiple objects to a queue
        /// </summary>
        /// <returns>Returns an awaitable task</returns>
        Task PublishMessageAsync<T>(IEnumerable<T> data) where T : class;
    }
}
