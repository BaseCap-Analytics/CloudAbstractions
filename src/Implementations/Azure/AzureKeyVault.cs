using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Azure
{
    /// <summary>
    /// Provides access to Azure Key Vault secret store
    /// </summary>
    public class AzureKeyVault : ISecretProvider
    {
        private string _appId;
        private string _appSecret;
        private string _vaultUrl;
        private KeyVaultClient _vaultClient;
        private TokenCache _cache;

        /// <summary>
        /// Creates the connection into the secret store
        /// </summary>
        public AzureKeyVault(string appId, string appSecret, string vaultUrl)
        {
            _appId = appId;
            _appSecret = appSecret;
            _vaultUrl = vaultUrl;
            _cache = new TokenCache();
            _vaultClient = new KeyVaultClient(Authenticate);
        }

        private async Task<string> Authenticate(string authority, string resource, string scope)
        {
            ClientCredential creds = new ClientCredential(_appId, _appSecret);
            AuthenticationContext context = new AuthenticationContext(authority, _cache);
            AuthenticationResult result = await context.AcquireTokenAsync(resource, creds);

            if (result == null)
            {
                throw new InvalidOperationException("Failed to authenticate with KeyVault; please check your credentials");
            }
            else
            {
                return result.AccessToken;
            }
        }

        /// <summary>
        /// Retrieves the most recent version value for the specified secret
        /// </summary>
        public string GetSecret(string name)
        {
            return GetSecretAsync(name).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Retrieves the specified version of a secret's value
        /// </summary>
        public string GetSecret(string name, string version)
        {
            return GetSecretAsync(name, version).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Retrieves the most recent version value for the specified secret
        /// </summary>
        public async Task<string> GetSecretAsync(string name)
        {
            SecretBundle s = await _vaultClient.GetSecretAsync(_vaultUrl, name);
            return s.Value;
        }

        /// <summary>
        /// Retrieves the specified version of a secret's value
        /// </summary>
        public async Task<string> GetSecretAsync(string name, string version)
        {
            SecretBundle s = await _vaultClient.GetSecretAsync(_vaultUrl, name, version);
            return s.Value;
        }

        /// <summary>
        /// Writes a secret value to the storage medium
        /// </summary>
        public Task SetSecretAsync(string name, string value)
        {
            return _vaultClient.SetSecretAsync(_vaultUrl, name, value);
        }

        /// <summary>
        /// Writes a secret value to the storage medium
        /// </summary>
        public void SetSecret(string name, string value)
        {
            SetSecretAsync(name, value).ConfigureAwait(false).GetAwaiter().GetResult();
        }
    }
}
