using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for writing to an eventing system
    /// </summary>
    public interface IEventStreamWriter
    {
        /// <summary>
        /// Constructs the underlying stream connection
        /// </summary>
        Task SetupAsync();

        /// <summary>
        /// Closes the connection to the stream
        /// </summary>
        Task CloseAsync();

        /// <summary>
        /// Sends a single event to the eventing system on the specified partition
        /// </summary>
        /// <param name="obj">The object to send to the system</param>
        /// <param name="partition">The event partition to send on</param>
        Task SendEventDataAsync(object obj, string partition);

        /// <summary>
        /// Sends a single event to the eventing system
        /// </summary>
        /// <param name="obj">The object to send to the system</param>
        Task SendEventDataAsync(object obj);

        /// <summary>
        /// Sends a batch of objects to the eventing system on the specified partition
        /// </summary>
        /// <param name="objs">The object batch to send to the system</param>
        /// <param name="partition">The event partition to send on</param>
        Task SendEventDataAsync(IList<object> msgs, string partition);

        /// <summary>
        /// Sends a batch of objects to the eventing system
        /// </summary>
        /// <param name="objs">The object batch to send to the system</param>
        Task SendEventDataAsync(IList<object> msgs);
    }
}
