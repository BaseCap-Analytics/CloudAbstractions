namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Information about a Job run in the cloud
    /// </summary>
    public class CloudJob
    {
        /// <summary>
        /// The ID of the Job
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// The display name of the Job
        /// </summary>
        public string Name { get; set; }
    }
}
