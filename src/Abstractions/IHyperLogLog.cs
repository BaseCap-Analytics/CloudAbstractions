using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for utilizing a HyperLogLog
    /// </summary>
    public interface IHyperLogLog
    {
        /// <summary>
        /// Checks if the provided key is unique in the set
        /// </summary>
        /// <param name="key">The entry to check for uniqueness</param>
        /// <returns>Returns true if the key is unique; otherwise, returns false</returns>
        Task<bool> CheckIfUniqueAsync(string key);

        /// <summary>
        /// Retrieves the number of unique entries in the set
        /// </summary>
        /// <returns>Returns the number of unique entries in the set</returns>
        Task<long> GetUniqueCountAsync();

        /// <summary>
        /// Deletes the set
        /// </summary>
        Task DeleteLog();
    }
}
