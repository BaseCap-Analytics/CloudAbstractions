using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for receiving notifications
    /// </summary>
    public interface INotificationReceiver
    {
        /// <summary>
        /// Starts receiving messages on the given channel
        /// </summary>
        Task SetupAsync(string channel, Func<string, Task> handler);

        /// <summary>
        /// Stops receiving messages and cleans up
        /// </summary>
        Task ShutdownAsync();
    }
}
