using BaseCap.CloudAbstractions.Abstractions;
using System;
using System.Collections.Generic;
using System.Text;

namespace BaseCap.CloudAbstractions.Implementations.Generic
{
    /// <summary>
    /// Adds logging functionality targeting stdout
    /// </summary>
    public sealed class ConsoleLogger : ILogger
    {
        /// <inheritdoc />
        public void LogEvent(string eventName, IDictionary<string, string> additionalData, IDictionary<string, double> metrics)
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
        }

        /// <inheritdoc />
        public void LogEvent(string eventName, IDictionary<string, double> metrics)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Event Name: {eventName}");
            sb.AppendLine("\tMetrics");

            foreach (string key in metrics.Keys)
            {
                sb.AppendLine($"\t\t'{key}': '{metrics[key]}'");
            }

            Console.WriteLine(sb.ToString());
        }

        /// <inheritdoc />
        public void LogEvent(string eventName, IDictionary<string, string> additionalData)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Event Name: {eventName}");
            sb.AppendLine("\tAdditional Data");
            foreach (string key in additionalData.Keys)
            {
                sb.AppendLine($"\t\t'{key}': '{additionalData[key]}'");
            }

            Console.WriteLine(sb.ToString());
        }

        /// <inheritdoc />
        public void LogException(Exception ex, IDictionary<string, string> additionalData)
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
        }

        /// <inheritdoc />
        public void LogException(Exception ex)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Exception Message: {ex.Message}");
            sb.AppendLine($"Exception Stack: {ex.StackTrace}");
            Console.WriteLine(sb.ToString());
        }

        /// <inheritdoc />
        public void LogLine(string message)
        {
            Console.WriteLine($"Message: '{message}'");
        }

        /// <inheritdoc />
        public void LogLine(string message, IDictionary<string, string> additionalData)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Message: '{message}'");
            sb.AppendLine("\tAdditional Data");
            foreach (string key in additionalData.Keys)
            {
                sb.AppendLine($"\t\t'{key}': '{additionalData[key]}'");
            }

            Console.WriteLine(sb.ToString());
        }
    }
}
