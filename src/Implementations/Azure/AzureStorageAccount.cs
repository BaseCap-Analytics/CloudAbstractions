using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Azure
{
    /// <summary>
    /// Class to communicate with Azure Storage Accounts
    /// </summary>
    public class AzureStorageAccount : IStorageAccount
    {
        protected readonly CloudStorageAccount _account;
        protected readonly Dictionary<string, AzureBlobStorage> _storageContainers;
        private ITableStorage? _tableStorage;

        /// <summary>
        /// Creates a connection to Azure Storage from a connection string
        /// </summary>
        public AzureStorageAccount(string connectionString)
        {
            _account = CloudStorageAccount.Parse(connectionString);
            _storageContainers = new Dictionary<string, AzureBlobStorage>();
            _tableStorage = null;
        }

        /// <summary>
        /// Creates a connection to Azure Storage from an account name, key, and endpoints
        /// </summary>
        public AzureStorageAccount(
            string accountName,
            string accountKey,
            Uri? blobStorageEndpoint,
            Uri? queueStorageEndpoint,
            Uri? tableStorageEndpoint,
            Uri? fileStorageEndpoint)
        {
            if ((blobStorageEndpoint == null) &&
                (queueStorageEndpoint == null) &&
                (tableStorageEndpoint == null) &&
                (fileStorageEndpoint == null))
            {
                throw new ArgumentException($"{nameof(AzureStorageAccount)} must have at least one remote URI to connect to");
            }

            StorageCredentials credentials = new StorageCredentials(accountName, accountKey);
            _account = new CloudStorageAccount(credentials, blobStorageEndpoint, queueStorageEndpoint, tableStorageEndpoint, fileStorageEndpoint);
            _storageContainers = new Dictionary<string, AzureBlobStorage>();
            _tableStorage = null;
        }

        /// <inheritdoc />
        public virtual async Task<IEnumerable<string>> ListBlobContainersAsync()
        {
            List<string> containers = new List<string>();
            BlobContinuationToken token = new BlobContinuationToken();
            CloudBlobClient client = _account.CreateCloudBlobClient();
            ContainerResultSegment result;
            do
            {
                result = await client.ListContainersSegmentedAsync(token).ConfigureAwait(false);
                if (result.Results?.Any() == false)
                {
                    break;
                }

                containers.AddRange(result.Results.Select(c => c.Name));
                token = result.ContinuationToken;
            }
            while (token != null);

            return containers;
        }

        /// <inheritdoc />
        public virtual async Task<IBlobStorage> GetBlobStorageAsync(string containerName)
        {
            if (_storageContainers.ContainsKey(containerName) == false)
            {
                _storageContainers[containerName] = new AzureBlobStorage(_account, containerName);
                await _storageContainers[containerName].SetupAsync();
            }

            return _storageContainers[containerName];
        }

        /// <inheritdoc />
        public ITableStorage GetTableStorage()
        {
            if (_tableStorage == null)
            {
                _tableStorage = new AzureTableStorage(_account);
            }

            return _tableStorage;
        }
    }
}
