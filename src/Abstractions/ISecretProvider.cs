using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    /// <summary>
    /// The contract for interacting with an application secret storage system
    /// </summary>
    public interface ISecretProvider
    {
        /// <summary>
        /// Retrieves a secret specified by a given name
        /// </summary>
        /// <param name="name">The name of the secret to retrieve</param>
        /// <returns>Returns the secret value on success; otherwise, returns null</returns>
        string GetSecret(string name);

        /// <summary>
        /// Retrieves a specific version of a given secret
        /// </summary>
        /// <param name="name">The name of the secret to retrieve</param>
        /// <param name="version">The version of the secret value to retrieve</param>
        string GetSecret(string name, string version);

        /// <summary>
        /// Retrieves a secret specified by a given name
        /// </summary>
        /// <param name="name">The name of the secret to retrieve</param>
        /// <returns>Returns the secret value on success; otherwise, returns null</returns>
        Task<string> GetSecretAsync(string name);

        /// <summary>
        /// Retrieves a specific version of a given secret
        /// </summary>
        /// <param name="name">The name of the secret to retrieve</param>
        /// <param name="version">The version of the secret value to retrieve</param>
        Task<string> GetSecretAsync(string name, string version);

        /// <summary>
        /// Writes a secret value to the storage medium
        /// </summary>
        /// <param name="name">The name of the secret</param>
        /// <param name="value">The secret value to store</param>
        Task SetSecretAsync(string name, string value);

        /// <summary>
        /// Writes a secret value to the storage medium
        /// </summary>
        /// <param name="name">The name of the secret</param>
        /// <param name="value">The secret value to store</param>
        void SetSecret(string name, string value);
    }
}
