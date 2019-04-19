using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for interacting with blobs of data on a cloud storage medium
    /// </summary>
    public interface IBlobStorage
    {
        /// <summary>
        /// Constructs the underlying stream connection
        /// </summary>
        Task SetupAsync();

        /// <summary>
        /// Checks if the specified blob exists in storage.
        /// </summary>
        /// <param name="path">The path to the blob to check</param>
        /// <returns>Returns true if the blob exists; otherwise, returns false<returns>
        Task<bool> DoesBlobExistAsync(string path);

        /// <summary>
        /// Retrieves a read-only stream to the blob specified
        /// </summary>
        /// <param name="path">The path of the blob on the storage medium</param>
        /// <returns>Returns a <see cref="System.IO.Stream"/> to the blob on success; otherwise, returns null</returns>
        Task<Stream> GetBlobReadStreamAsync(string path);

        /// <summary>
        /// Retrieves a write-only stream to the blob specified
        /// </summary>
        /// <param name="path">The path of the blob on the storage medium</param>
        /// <param name="createNewBlob">Flag designating if the blob should be new and not exist or not</param>
        /// <returns>Returns a <see cref="System.IO.StreamWriter"/> to the blob</returns>
        Task<Stream> GetBlobWriteStreamAsync(string path, bool createNewBlob = false);

        /// <summary>
        /// Retrieves a URL to the specified Blob for external access within the given time range and with the given permissions
        /// </summary>
        /// <param name="path">The blob to give access to</param>
        /// <param name="start">The timestamp to start giving access</param>
        /// <param name="expire">The timestamp to stop allowing access</param>
        /// <param name="readWrite">Whether the token has Read or ReadWrite privileges</param>
        /// <returns>Returns a full qualified URL to the blob including access token</returns>
        Task<string> GetUrlWithSasToken(string path, DateTimeOffset start, DateTimeOffset expire, bool readWrite);

        /// <summary>
        /// Deletes the specified blob from the storage medium
        /// </summary>
        /// <param name="path">The blob to delete</param>
        Task DeleteBlobAsync(string path);

        /// <summary>
        /// Retrieves metadata about every blob in the storage medium
        /// </summary>
        Task<IEnumerable<BlobItem>> GetAllBlobMetadatasAsync(string path);

        /// <summary>
        /// Retrieves metadata about every directory in the storage medium under the given path
        /// </summary>
        Task<IEnumerable<BlobItem>> GetAllDirectoriesInPathAsync(string path);

        /// <summary>
        /// Deletes blobs older than the specified cutOffDate
        /// </summary>
        /// <param name="cutOffDate">The DateTimeOffSet to delete blobs older than</param>
        /// <param name="cancellationToken">CancellationToken used to cancel the async execution</param>
        /// <returns></returns>
        Task DeleteBlobsAync(DateTimeOffset cutOffDate, CancellationToken cancellationToken = default(CancellationToken));
    }
}
