using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// Metadata about a Blob
    /// </summary>
    public class BlobItem
    {
        /// <summary>
        /// The Name of the Blob
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The path to the Blob, relative to the Storage root. This
        /// includes the Blob name.
        /// </summary>
        public string RelativePath { get; set; }

        /// <summary>
        /// The fully qualified path to this blob
        /// </summary>
        public Uri FullPath { get; set; }

        /// <summary>
        /// The name of the Container that this Blob is contained in
        /// </summary>
        public string ContainerName { get; set; }

        /// <summary>
        /// When this Blob was last created
        /// </summary>
        public DateTimeOffset Created { get; set; }

        /// <summary>
        /// When this Blob was last modified
        /// </summary>
        public DateTimeOffset LastModified { get; set; }

        internal BlobItem(CloudBlockBlob blobItem)
        {
            Name = Path.GetFileName(blobItem.Name);
            RelativePath = blobItem.Name;
            FullPath = blobItem.Uri;
            ContainerName = blobItem.Container.Name;
            Created = blobItem.Properties.Created.HasValue ?
                            blobItem.Properties.Created.Value :
                            DateTimeOffset.MinValue;
            LastModified = blobItem.Properties.LastModified.HasValue ?
                            blobItem.Properties.LastModified.Value :
                            DateTimeOffset.MinValue;
        }

        internal BlobItem(CloudBlobDirectory directory)
        {
            Name = Path.GetFileName(Path.GetDirectoryName(directory.Prefix));
            RelativePath = directory.Prefix;
            FullPath = directory.Uri;
            ContainerName = directory.Container.Name;
            Created = DateTimeOffset.MinValue;
            LastModified = DateTimeOffset.MinValue;
        }
    }
}
