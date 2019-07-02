using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Contract to interact with a platform-wide logger
    /// </summary>
    public interface ILogger
    {
        /// <summary>
        /// Log an exception record with additional data
        /// </summary>
        /// <param name="ex">The exception record to log</param>
        /// <param name="additionalData">Additional contextual data to add to the log</param>
        void LogException(Exception ex, IDictionary<string, string> additionalData);

        /// <summary>
        /// Log an exception record
        /// </summary>
        /// <param name="ex">The exception record to log</param>
        void LogException(Exception ex);

        /// <summary>
        /// Log a debugging statement
        /// </summary>
        /// <param name="message">The message to log</param>
        void LogLine(string message);

        /// <summary>
        /// Log a debugging statement
        /// </summary>
        /// <param name="message">The message to log</param>
        /// <param name="additionalData">Additional contextual data to add to the log</param>
        void LogLine(string message, IDictionary<string, string> additionalData);

        /// <summary>
        /// Log an Event with additional data and metrics
        /// </summary>
        /// <param name="eventName">The event name to log</param>
        /// <param name="additionalData">Additional contextual data to add to the log</param>
        /// <param name="metrics">The metrics to add to the log entry</param>
        void LogEvent(
            string eventName,
            IDictionary<string, string> additionalData,
            IDictionary<string, double> metrics);

        /// <summary>
        /// Log an Event with only metrics
        /// </summary>
        /// <param name="eventName">The event name to log</param>
        /// <param name="metrics">The metrics to add to the log entry</param>
        void LogEvent(
            string eventName,
            IDictionary<string, double> metrics);

        /// <summary>
        /// Log an event with only additional data
        /// </summary>
        /// <param name="eventName">The event name to log</param>
        /// <param name="additionalData">Additional contextual data to add to the log</param>
        void LogEvent(
            string eventName,
            IDictionary<string, string> additionalData);
    }
}
