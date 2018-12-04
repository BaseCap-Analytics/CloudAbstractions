using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            string containerName = new Uri(containerSasUrl).Segments[1];
            CloudStorageAccount account = new CloudStorageAccount(new StorageCredentials(containerSasUrl), true);
            _blobStorage = account.CreateCloudBlobClient().GetContainerReference(containerName);
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
        /// Retrieves metadata about every blob in the storage medium
        /// </summary>
        public async Task<IEnumerable<BlobItem>> GetAllBlobMetadatasAsync()
        {
            List<BlobItem> blobs = new List<BlobItem>();
            BlobResultSegment segment = await _blobStorage.ListBlobsSegmentedAsync(
                null,
                true,
                BlobListingDetails.All,
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
                        Console.WriteLine($"{((CloudBlobDirectory)blob).Uri}");
                        continue;
                    }

                    CloudBlockBlob cbb = (CloudBlockBlob)blob;

                    // Don't add deleted blobs since they're there as artifacts
                    if (cbb.IsDeleted == false)
                    {
                        blobs.Add(new BlobItem(cbb));
                    }
                }

                segment = await _blobStorage.ListBlobsSegmentedAsync(segment.ContinuationToken);
            }
            while ((segment.ContinuationToken != null) && (segment.Results.Any()));

            return blobs;
        }
    }
}
