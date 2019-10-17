using BaseCap.CloudAbstractions.Abstractions;
using SendGrid;
using SendGrid.Helpers.Mail;
using SendGrid.Helpers.Reliability;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.SendGrid
{
    /// <summary>
    /// A connection to SendGrid to send automated emails
    /// </summary>
    public class SendGridEmailSender : IEmailSender
    {
        private readonly SendGridClient _client;
        private readonly EmailAddress _fromAddress;

        /// <summary>
        /// Creates a new SendGridEmailSender
        /// </summary>
        /// <param name="apiKey">The SendGrid API key</param>
        /// <param name="fromAddress">The email address that the emails should come from</param>
        /// <param name="fromName">The display name of the from address</param>
        public SendGridEmailSender(string apiKey, string fromAddress, string fromName)
        {
            _client = new SendGridClient(new SendGridClientOptions()
            {
                ApiKey = apiKey,
                ReliabilitySettings = new ReliabilitySettings(
                    3,
                    TimeSpan.FromSeconds(3),
                    TimeSpan.FromSeconds(10),
                    TimeSpan.FromSeconds(1)),
            });
            _fromAddress = new EmailAddress(fromAddress, fromName);
        }

        /// <inheritdoc />
        public async Task<bool> SendTemplateEmailAsync<T>(
            IEnumerable<(string email, string name)> toAddresses,
            string templateId,
            T? templateData,
            ILogger logger,
            CancellationToken token = default(CancellationToken)) where T : class
        {
            SendGridMessage msg = new SendGridMessage();
            msg.SetFrom(_fromAddress);
            msg.AddTos(toAddresses.Select(t => new EmailAddress(t.email, t.name)).ToList());
            msg.SetTemplateId(templateId);

            if (templateData != null)
            {
                msg.SetTemplateData(templateData);
            }

            Response resp = await _client.SendEmailAsync(msg, token).ConfigureAwait(false);
            if ((resp.StatusCode == System.Net.HttpStatusCode.Accepted) ||
                (resp.StatusCode == System.Net.HttpStatusCode.OK))
            {
                return true;
            }
            else
            {
                logger.LogEvent(
                    "SendEmailFailed",
                    new Dictionary<string, string>()
                    {
                        ["TemplateId"] = templateId,
                        ["RecipientCount"] = toAddresses.Count().ToString(),
                        ["ErrorCode"] = resp.StatusCode.ToString(),
                    });
                return false;
            }
        }
    }
}
