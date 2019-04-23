using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    /// <summary>
    /// Provides a connection to and from Azure Blob Storage
    /// </summary>
    public class AzureBlobStorage : IBlobStorage
    {
        private CloudBlobContainer _blobStorage;

        /// <summary>
        /// Creates a connection to Azure Blob Storage
        /// </summary>
        public AzureBlobStorage(string storageConnectionString, string container)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(storageConnectionString);
            _blobStorage = account.CreateCloudBlobClient().GetContainerReference(container);
        }

        /// <summary>
        /// Creates a connection to Azure Blob Storage from a SaS token URL
        /// </summary>
        public AzureBlobStorage(string containerSasUrl)
        {
            _blobStorage = new CloudBlobContainer(new Uri(containerSasUrl));
        }

        /// <summary>
        /// Creates a connection to Azure Blob Storage
        /// </summary>
        internal AzureBlobStorage(CloudStorageAccount account, string container)
        {
            _blobStorage = account.CreateCloudBlobClient().GetContainerReference(container);
        }

        /// <summary>
        /// Initializes the connection with Azure
        /// </summary>
        public virtual async Task SetupAsync()
        {
            await _blobStorage.CreateIfNotExistsAsync(new BlobRequestOptions()
            {
                AbsorbConditionalErrorsOnRetry = true,
                RetryPolicy = new ExponentialRetry(),
            }, null);
        }

        /// <summary>
        /// Checks if the given blob exists or not
        /// </summary>
        public async virtual Task<bool> DoesBlobExistAsync(string path)
        {
            Stream s = await GetBlobReadStreamAsync(path);
            if (s == null)
            {
                return false;
            }
            else
            {
                s.Dispose();
                return true;
            }
        }

        /// <summary>
        /// Opens a read-only stream to the specified blob
        /// </summary>
        public async virtual Task<Stream> GetBlobReadStreamAsync(string path)
        {
            CloudBlockBlob blob = _blobStorage.GetBlockBlobReference(path);
            if (await blob.ExistsAsync())
            {
                return await blob.OpenReadAsync();
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Opens a write-only stream to the specified blob
        /// </summary>
        public async virtual Task<Stream> GetBlobWriteStreamAsync(string path, bool createNewBlob = false)
        {
            CloudBlockBlob blob = _blobStorage.GetBlockBlobReference(path);
            if (createNewBlob && await blob.ExistsAsync())
                return null;
            else
                return await blob.OpenWriteAsync();
        }

        /// <summary>
        /// Retrieves a Sas token for public sharing the specified blob
        /// </summary>
        public async virtual Task<string> GetUrlWithSasToken(string path, DateTimeOffset start, DateTimeOffset expire, bool readWrite)
        {
            CloudBlockBlob blob = _blobStorage.GetBlockBlobReference(path);
            if (await blob.ExistsAsync() == false)
                throw new FileNotFoundException($"Blob '{path}' does not exist");
            else
            {
                SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy()
                {
                    SharedAccessStartTime = start,
                    SharedAccessExpiryTime = expire,
                    Permissions = readWrite ? SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.Read : SharedAccessBlobPermissions.Read,
                };
                string sas = blob.GetSharedAccessSignature(policy);
                UriBuilder builder = new UriBuilder(blob.Uri);
                builder.Query = sas;
                return builder.ToString();
            }
        }

        /// <summary>
        /// Deletes the specified blob
        /// </summary>
        public virtual Task DeleteBlobAsync(string path)
        {
            CloudBlockBlob blob = _blobStorage.GetBlockBlobReference(path);
            return blob.DeleteIfExistsAsync();
        }

        /// <summary>
        /// Retrieves metadata about every blob in the storage medium under the given path
        /// </summary>
        public async Task<IEnumerable<BlobItem>> GetAllBlobMetadatasAsync(string path)
        {
            List<BlobItem> blobs = new List<BlobItem>();
            BlobResultSegment segment = await _blobStorage.ListBlobsSegmentedAsync(
                path,
                false,
                BlobListingDetails.Metadata,
                null,
                null,
                null,
                null);

            do
            {
                foreach (IListBlobItem blob in segment.Results)
                {
                    if (blob is CloudBlockBlob)
                    {
                        CloudBlockBlob cbb = (CloudBlockBlob)blob;

                        // Don't add deleted blobs since they're there as artifacts
                        if (cbb.IsDeleted == false)
                        {
                            blobs.Add(new BlobItem(cbb));
                        }
                    }
                }

                segment = await _blobStorage.ListBlobsSegmentedAsync(segment.ContinuationToken);
            }
            while ((segment.ContinuationToken != null) && (segment.Results.Any()));

            return blobs;
        }

        /// <summary>
        /// Retrieves metadata about every directory in the storage medium under the given path
        /// </summary>
        public async Task<IEnumerable<BlobItem>> GetAllDirectoriesInPathAsync(string path)
        {
            List<BlobItem> blobs = new List<BlobItem>();
            BlobResultSegment segment = await _blobStorage.ListBlobsSegmentedAsync(
                path,
                false,
                BlobListingDetails.Metadata,
                null,
                null,
                null,
                null);

            do
            {
                foreach (IListBlobItem blob in segment.Results)
                {
                    if (blob is CloudBlobDirectory)
                    {
                        blobs.Add(new BlobItem((CloudBlobDirectory)blob));
                        continue;
                    }
                }

                segment = await _blobStorage.ListBlobsSegmentedAsync(segment.ContinuationToken);
            }
            while ((segment.ContinuationToken != null) && (segment.Results.Any()));

            return blobs;
        }

        /// <inheritdoc />
        public async Task DeleteBlobsAync(DateTimeOffset cutOffDate, CancellationToken cancellationToken = default(CancellationToken))
        {
            BlobContinuationToken continuationToken = null;
            do
            {
                BlobResultSegment response = await _blobStorage.ListBlobsSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;
                IEnumerable<CloudBlockBlob> blobs = response.Results.OfType<CloudBlockBlob>()
                    .Where(x => x.Properties.LastModified < cutOffDate);
                foreach (CloudBlockBlob blob in blobs)
                {
                    await blob.DeleteIfExistsAsync();
                }
            }
            while (continuationToken != null && (cancellationToken == null || cancellationToken.IsCancellationRequested == false));
        }
    }
}
