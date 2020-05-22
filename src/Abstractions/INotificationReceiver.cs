using System;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for receiving notifications
    /// </summary>
    public interface INotificationReceiver : IDisposable
    {
        /// <summary>
        /// Starts receiving messages on the given channel
        /// </summary>
        Task SetupAsync(Func<string, Task> handler);

        /// <summary>
        /// Sets the receiver up for blocking reads
        /// </summary>
        ValueTask SetupBlockingAsync();

        /// <summary>
        /// Starts a blocking read on the notification channel
        /// </summary>
        /// <param name="token">Cancellation token for exiting without a message</param>
        /// <returns>Returns the message received</returns>
        ValueTask<string> BlockingReadAsync(CancellationToken token);

        /// <summary>
        /// Stops receiving messages and cleans up
        /// </summary>
        Task ShutdownAsync();
    }
}
