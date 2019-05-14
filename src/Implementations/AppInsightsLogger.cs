using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ApplicationInsights.WindowsServer.TelemetryChannel;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Adds logging functionality targeting Azure App Insights
    /// </summary>
    public sealed class AppInsightsLogger : ILogger
    {
        private const string _tempDirectory = "telemetry_temp";
        private TelemetryClient _logger;

        /// <inheritdoc />
        public AppInsightsLogger()
        {
            // When running on non-Windows machine, we need to specify a location for
            // telemetry data to be saved locally when AppInsights uses ServerTelemetryChannels.
            foreach (TelemetrySink sink in TelemetryConfiguration.Active.TelemetrySinks)
            {
                ITelemetryChannel channel = sink.TelemetryChannel;
                if (channel is ServerTelemetryChannel)
                {
                    if (System.IO.Directory.Exists(_tempDirectory) == false)
                    {
                        System.IO.Directory.CreateDirectory(_tempDirectory);
                    }
                    ((ServerTelemetryChannel)channel).StorageFolder = System.IO.Path.GetFullPath(_tempDirectory);
                }
            }

            _logger = new TelemetryClient(TelemetryConfiguration.Active);
        }

        /// <inheritdoc />
        public Task LogEventAsync(string eventName, IDictionary<string, string> additionalData, IDictionary<string, double> metrics)
        {
            _logger.TrackEvent(eventName, additionalData, metrics);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogEventAsync(string eventName, IDictionary<string, double> metrics)
        {
            _logger.TrackEvent(eventName, null, metrics);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogEventAsync(string eventName, IDictionary<string, string> additionalData)
        {
            _logger.TrackEvent(eventName, additionalData);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogExceptionAsync(Exception ex, IDictionary<string, string> additionalData)
        {
            _logger.TrackException(ex, additionalData);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogExceptionAsync(Exception ex)
        {
            _logger.TrackException(ex);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogLineAsync(string message)
        {
            _logger.TrackTrace(message);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task LogLineAsync(string message, IDictionary<string, string> additionalData)
        {
            _logger.TrackTrace(message, additionalData);
            return Task.CompletedTask;
        }
    }
}
