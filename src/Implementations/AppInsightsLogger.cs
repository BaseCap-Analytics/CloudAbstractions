using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using System;
using System.Collections.Generic;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Adds logging functionality targeting Azure App Insights
    /// </summary>
    public sealed class AppInsightsLogger : ILogger
    {
        private readonly TelemetryClient _logger;

        /// <inheritdoc />
        public AppInsightsLogger()
        {
            _logger = new TelemetryClient(TelemetryConfiguration.Active);
        }

        /// <inheritdoc />
        public void LogEvent(string eventName, IDictionary<string, string> additionalData, IDictionary<string, double> metrics)
        {
            _logger.TrackEvent(eventName, additionalData, metrics);
        }

        /// <inheritdoc />
        public void LogEvent(string eventName, IDictionary<string, double> metrics)
        {
            _logger.TrackEvent(eventName, null, metrics);
        }

        /// <inheritdoc />
        public void LogEvent(string eventName, IDictionary<string, string> additionalData)
        {
            _logger.TrackEvent(eventName, additionalData);
        }

        /// <inheritdoc />
        public void LogException(Exception ex, IDictionary<string, string> additionalData)
        {
            _logger.TrackException(ex, additionalData);
        }

        /// <inheritdoc />
        public void LogException(Exception ex)
        {
            _logger.TrackException(ex);
        }

        /// <inheritdoc />
        public void LogLine(string message)
        {
            _logger.TrackTrace(message);
        }

        /// <inheritdoc />
        public void LogLine(string message, IDictionary<string, string> additionalData)
        {
            _logger.TrackTrace(message, additionalData);
        }
    }
}
