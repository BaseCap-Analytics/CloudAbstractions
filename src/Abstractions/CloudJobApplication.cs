namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Information about a Job Application
    /// </summary>
    public class CloudJobApplication
    {
        /// <summary>
        /// The ID of the Application
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// The version number of the application
        /// </summary>
        public string Version { get; set; }

        /// <summary>
        /// Relative path to the source blob
        /// </summary>
        public string SourceZipFile { get; set; }
    }
}
