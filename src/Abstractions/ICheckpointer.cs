using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for interacting with a checkpointing system to keep track of progress
    /// </summary>
    public interface ICheckpointer
    {
        /// <summary>
        /// Retrieves the stored checkpoint data for the given ID
        /// </summary>
        /// <param name="id">The ID to retrieve the checkpoint for</param>
        /// <returns>Returns the value stored in the checkpoint, if exists; returns null if the checkpoint does not exist</returns>
        Task<string> GetCheckpointAsync(string id);

        /// <summary>
        /// Sets the checkpoint data for a given ID
        /// </summary>
        /// <param name="id">The ID to store the value for</param>
        /// <param name="value">The data to store in the checkpoint</param>
        Task SetCheckpointAsync(string id, string value);
    }
}
