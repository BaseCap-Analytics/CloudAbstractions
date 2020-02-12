using BaseCap.Security;
using Microsoft.Azure.Storage;
using System.IO;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Azure.Secure
{
    /// <summary>
    /// Provides seamless encryption and decryption for data be passed to and from Azure Blob Storage
    /// </summary>
    public sealed class AzureEncryptedBlobStorage : AzureBlobStorage
    {
        private const int IV_SIZE_IN_BYTES = 16;
        private byte[] _encryptionKey;

        /// <summary>
        /// Creates a new connection to a Azure Blob Storage container with a given encryption key
        /// </summary>
        public AzureEncryptedBlobStorage(string storageConnectionString, string container, byte[] encryptionKey) : base(storageConnectionString, container)
        {
            _encryptionKey = encryptionKey;
        }

        /// <summary>
        /// Creates a new connection to a Azure Blob Storage container with a given encryption key
        /// </summary>
        internal AzureEncryptedBlobStorage(CloudStorageAccount account, string container, byte[] encryptionKey) : base(account, container)
        {
            _encryptionKey = encryptionKey;
        }


        /// <summary>
        /// Opens a read-only stream to the given blob path.
        /// </summary>
        public override async Task<Stream?> GetBlobReadStreamAsync(string path)
        {
            Stream? underlying = await base.GetBlobReadStreamAsync(path);
            if (underlying == null)
                return null;
            else
            {
                Stream seekable = new MemoryStream();
                using (CryptographicStream s = await EncryptionHelpers.GetDecryptionStreamAsync(_encryptionKey, underlying))
                {
                    await s.CopyToAsync(seekable);
                    seekable.Seek(0, SeekOrigin.Begin);
                }
                return seekable;
            }
        }

        /// <summary>
        /// Opens a write-only stream to the given blob path.
        /// </summary>
        public override async Task<Stream?> GetBlobWriteStreamAsync(string path, bool createNewBlob = false)
        {
            Stream? underlying = await base.GetBlobWriteStreamAsync(path, createNewBlob);
            if (underlying == null)
                return null;
            else
                return await EncryptionHelpers.GetEncryptionStreamAsync(_encryptionKey, underlying);
        }
    }
}
