using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for sending a notification to receivers
    /// </summary>
    public interface INotificationSender : IDisposable
    {
        /// <summary>
        /// Initializes the notification system
        /// </summary>
        Task SetupAsync();

        /// <summary>
        /// Sends the notification to any receivers on the given channel
        /// </summary>
        Task SendNotificationAsync(object notification);
    }
}
