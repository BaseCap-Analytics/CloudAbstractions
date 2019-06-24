using BaseCap.CloudAbstractions.Abstractions;
using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Secure
{
    /// <summary>
    /// Class to communicate with Azure Storage Accounts with seamless encryption
    /// </summary>
    public class AzureEncryptedStorageAccount : AzureStorageAccount
    {
        private byte[] _encryptionKey;

        /// <summary>
        /// Creates a connection to Azure Storage from a connection string and encrypts the data at-rest
        /// </summary>
        public AzureEncryptedStorageAccount(string connectionString, byte[] encryptionKey) : base(connectionString)
        {
            _encryptionKey = encryptionKey;
        }

        /// <summary>
        /// Creates a connection to Azure Storage from an account name, key, and endpoints while encrypting the data at-rest
        /// </summary>
        public AzureEncryptedStorageAccount(
            byte[] encryptionKey,
            string accountName,
            string accountKey,
            Uri blobStorageEndpoint,
            Uri queueStorageEndpoint,
            Uri tableStorageEndpoint,
            Uri fileStorageEndpoint) : base(accountName, accountKey, blobStorageEndpoint, queueStorageEndpoint, tableStorageEndpoint, fileStorageEndpoint)
        {
            _encryptionKey = encryptionKey;
        }

        /// <summary>
        /// Retrieves a connection to a Blob Storage container which encrypts the data at-rest
        /// </summary>
        public override async Task<IBlobStorage> GetBlobStorageAsync(string containerName)
        {
            if (_storageContainers.ContainsKey(containerName) == false)
            {
                _storageContainers[containerName] = new AzureEncryptedBlobStorage(_account, containerName, _encryptionKey);
                await _storageContainers[containerName].SetupAsync();
            }

            return _storageContainers[containerName];
        }
    }
}
