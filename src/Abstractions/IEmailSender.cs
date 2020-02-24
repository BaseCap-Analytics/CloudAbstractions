using Serilog;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Constract for interacting with an email provider to send templated, automated emails
    /// </summary>
    public interface IEmailSender
    {
        /// <summary>
        /// Sends a pre-defined template email to a set of users
        /// </summary>
        /// <param name="toAddresses">The name/address combination of recipients</param>
        /// <param name="templateId">The ID of the template to send</param>
        /// <param name="templateData">Any object data to be JSON serialized as variables to the template</param>
        /// <param name="logger">The logger to use for error reporting</param>
        /// <param name="token">A cancellation token to cancel the request</param>
        /// <returns>Returns true if the request was successful; otherwise, returns false</returns>
        Task<bool> SendTemplateEmailAsync<T>(
            IEnumerable<(string email, string name)> toAddresses,
            string templateId,
            T? templateData,
            ILogger logger,
            CancellationToken token = default(CancellationToken)) where T: class;
    }
}
