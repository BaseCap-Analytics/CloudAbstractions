using BaseCap.CloudAbstractions.Abstractions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Adds logging functionality targeting stdout
    /// </summary>
    public sealed class ConsoleLogger : ILogger
    {
        /// <inheritdoc />
        public Task LogEventAsync(string eventName, IDictionary<string, string> additionalData, IDictionary<string, double> metrics)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Event Name: {eventName}");
            sb.AppendLine("\tAdditional Data");
            foreach (string key in additionalData.Keys)
            {
                sb.AppendLine($"\t\t'{key}': '{additionalData[key]}'");
            }

            sb.AppendLine("\tMetrics");

            foreach (string key in metrics.Keys)
            {
                sb.AppendLine($"\t\t'{key}': '{metrics[key]}'");
            }

            Console.WriteLine(sb.ToString());
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogEventAsync(string eventName, IDictionary<string, double> metrics)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Event Name: {eventName}");
            sb.AppendLine("\tMetrics");

            foreach (string key in metrics.Keys)
            {
                sb.AppendLine($"\t\t'{key}': '{metrics[key]}'");
            }

            Console.WriteLine(sb.ToString());
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogEventAsync(string eventName, IDictionary<string, string> additionalData)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Event Name: {eventName}");
            sb.AppendLine("\tAdditional Data");
            foreach (string key in additionalData.Keys)
            {
                sb.AppendLine($"\t\t'{key}': '{additionalData[key]}'");
            }

            Console.WriteLine(sb.ToString());
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogExceptionAsync(Exception ex, IDictionary<string, string> additionalData)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Exception Message: {ex.Message}");
            sb.AppendLine($"Exception Stack: {ex.StackTrace}");
            sb.AppendLine("\tAdditional Data");
            foreach (string key in additionalData.Keys)
            {
                sb.AppendLine($"\t\t'{key}': '{additionalData[key]}'");
            }

            Console.WriteLine(sb.ToString());
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogExceptionAsync(Exception ex)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Exception Message: {ex.Message}");
            sb.AppendLine($"Exception Stack: {ex.StackTrace}");
            Console.WriteLine(sb.ToString());
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogLineAsync(string message)
        {
            Console.WriteLine($"Message: '{message}'");
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogLineAsync(string message, IDictionary<string, string> additionalData)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Message: '{message}'");
            sb.AppendLine("\tAdditional Data");
            foreach (string key in additionalData.Keys)
            {
                sb.AppendLine($"\t\t'{key}': '{additionalData[key]}'");
            }

            Console.WriteLine(sb.ToString());
            return Task.CompletedTask;
        }
    }
}
