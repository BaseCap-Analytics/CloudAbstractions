using System;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Information about a Scheduled Job run in the cloud
    /// </summary>
    public class CloudScheduledJob
    {
        /// <summary>
        /// The ID of the Job
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// The display name of the Job
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// How often the job should run
        /// </summary>
        public TimeSpan Recurrence { get; set; }
    }
}
