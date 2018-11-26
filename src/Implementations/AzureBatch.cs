using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    public class AzureBatch : IJobScheduler
    {
        private const string BatchResourceUri = "https://batch.core.windows.net/";
        private string _accountUrl;
        private string _appId;
        private string _appSecret;
        private string _authorityUrl;
        private string _poolId;
        private string _vaultUrl;
        private BatchClient _batch;

        public AzureBatch(string accountUrl, string appId, string appSecret, string tenantId, string poolId, string keyVaultUrl)
        {
            _accountUrl = accountUrl;
            _appId = appId;
            _appSecret = appSecret;
            _authorityUrl = $"https://login.microsoftonline.com/{tenantId}";
            _poolId = poolId;
            _vaultUrl = keyVaultUrl;
            _batch = null;
        }

        protected virtual void Dispose(bool disposing)
        {
            if ((_batch != null) && (disposing))
            {
                _batch.Dispose();
                _batch = null;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private async Task<string> GetAuthenticationTokenAsync()
        {
            AuthenticationContext authContext = new AuthenticationContext(_authorityUrl);
            AuthenticationResult authResult = await authContext.AcquireTokenAsync(BatchResourceUri, new ClientCredential(_appId, _appSecret));

            return authResult.AccessToken;
        }

        public Task SetupAsync()
        {
            _batch = BatchClient.Open(new BatchTokenCredentials(_accountUrl, () => GetAuthenticationTokenAsync()));
            return Task.CompletedTask;
        }

        public async Task CreateJobAsync(Abstractions.CloudJob jobInput, CloudJobTask taskInput)
        {
            Microsoft.Azure.Batch.CloudJob job = _batch.JobOperations.CreateJob(jobInput.Id, new PoolInformation() { PoolId = _poolId });
            job.DisplayName = jobInput.Name;
            job.OnTaskFailure = OnTaskFailure.PerformExitOptionsJobAction;
            job.CommonEnvironmentSettings = new List<EnvironmentSetting>()
            {
                new EnvironmentSetting("AppId", _appId),
                new EnvironmentSetting("AppSecret", _appSecret),
                new EnvironmentSetting("VaultUrl", _vaultUrl),
            };
            await job.CommitAsync();

            CloudTask cloudTask = new CloudTask(taskInput.Id, taskInput.CommandLine)
            {
                DisplayName = taskInput.DisplayName,
                ExitConditions = new ExitConditions()
                {
                    ExitCodes = new List<ExitCodeMapping>()
                    {
                        new ExitCodeMapping(0, new ExitOptions() { JobAction = JobAction.Terminate }),
                    }
                }
            };
            await _batch.JobOperations.AddTaskAsync(job.Id, cloudTask);

            job = await _batch.JobOperations.GetJobAsync(job.Id);
            job.OnAllTasksComplete = OnAllTasksComplete.TerminateJob;
            await job.CommitChangesAsync();
        }
    }
}
