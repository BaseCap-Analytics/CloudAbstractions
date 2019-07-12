using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Azure
{
    /// <summary>
    /// Class to communicate with Azure Storage Accounts
    /// </summary>
    public class AzureStorageAccount : IStorageAccount
    {
        protected CloudStorageAccount _account;
        protected Dictionary<string, AzureBlobStorage> _storageContainers;
        private ITableStorage _tableStorage;

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
        public AzureStorageAccount(string accountName, string accountKey, Uri blobStorageEndpoint, Uri queueStorageEndpoint, Uri tableStorageEndpoint, Uri fileStorageEndpoint)
        {
            StorageCredentials credentials = new StorageCredentials(accountName, accountKey);
            _account = new CloudStorageAccount(credentials, blobStorageEndpoint, queueStorageEndpoint, tableStorageEndpoint, fileStorageEndpoint);
            _storageContainers = new Dictionary<string, AzureBlobStorage>();
            _tableStorage = null;
        }

        /// <summary>
        /// Retrieves a connection to a Blob Storage container
        /// </summary>
        public virtual async Task<IBlobStorage> GetBlobStorageAsync(string containerName)
        {
            if (_storageContainers.ContainsKey(containerName) == false)
            {
                _storageContainers[containerName] = new AzureBlobStorage(_account, containerName);
                await _storageContainers[containerName].SetupAsync();
            }

            return _storageContainers[containerName];
        }

        /// <summary>
        /// Retrieves a connection to Table Storage
        /// </summary>
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
