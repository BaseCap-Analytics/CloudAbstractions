using BaseCap.CloudAbstractions.Abstractions;
using Serilog;
using System.IO;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Generic
{
    /// <summary>
    /// Provides a system to save and load the last read location from an event stream
    /// </summary>
    public class EventCheckpointer : ICheckpointer
    {
        protected readonly IBlobStorage _storage;
        protected readonly string _applicationName;

        /// <summary>
        /// Creates a connection to an application's checkpoint list
        /// </summary>
        public EventCheckpointer(IBlobStorage storage, string applicationName)
        {
            _storage = storage;
            _applicationName = applicationName;
        }

        private string GetBlobName(string id)
        {
            return $"{_applicationName}\\{id}.checkpoint";
        }

        /// <summary>
        /// Retrieves the last read location in an event stream for the given partition ID
        /// </summary>
        /// <param name="id">The ID of the partition to retrieve the checkpoint for</param>
        public async Task<string?> GetCheckpointAsync(string id)
        {
            string? value;
            string path = GetBlobName(id);

            try
            {
                using (Stream? blobStream = await _storage.GetBlobReadStreamAsync(path))
                using (StreamReader sr = new StreamReader(blobStream))
                {
                    value = sr.ReadToEnd()?.Trim();
                    Log.Logger.Warning("Not Checkpoint Found on Partition {Partition}", id);
                }
            }
            catch
            {
                value = null;
            }

            return value;
        }

        /// <summary>
        /// Sets the last read location in an event stream for a given partition ID
        /// </summary>
        /// <param name="id">The partition ID</param>
        /// <param name="value">The last read location</param>
        public async Task SetCheckpointAsync(string id, string value)
        {
            string path = GetBlobName(id);

            using (Stream? blobStream = await _storage.GetBlobWriteStreamAsync(path))
            using (StreamWriter sw = new StreamWriter(blobStream))
            {
                await sw.WriteLineAsync(value);
            }
        }
    }
}
