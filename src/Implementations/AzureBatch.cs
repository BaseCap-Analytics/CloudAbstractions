using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    public class AzureBatch : IJobScheduler
    {
        private string _appId;
        private string _appSecret;
        private string _batchAccountUrl;
        private string _batchAccountName;
        private string _batchAccountKey;
        private string _vaultUrl;
        private BatchClient _batch;

        public AzureBatch(
            string accountUrl,
            string accountName,
            string accountKey,
            string appId,
            string appSecret,
            string keyVaultUrl)
        {
            _batchAccountUrl = accountUrl;
            _batchAccountName = accountName;
            _batchAccountKey = accountKey;
            _appId = appId;
            _appSecret = appSecret;
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

        public Task SetupAsync()
        {
            _batch = BatchClient.Open(new BatchSharedKeyCredentials(_batchAccountUrl, _batchAccountName, _batchAccountKey));
            return Task.CompletedTask;
        }

        public async Task CreateJobAsync(Abstractions.CloudJob jobInput, CloudJobTask taskInput, string poolId)
        {
            Microsoft.Azure.Batch.CloudJob job = _batch.JobOperations.CreateJob(jobInput.Id, new PoolInformation() { PoolId = poolId });
            job.DisplayName = jobInput.Name;
            job.OnTaskFailure = OnTaskFailure.PerformExitOptionsJobAction;
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
                },
                EnvironmentSettings = new List<EnvironmentSetting>()
                {
                    new EnvironmentSetting("AppId", _appId),
                    new EnvironmentSetting("AppSecret", _appSecret),
                    new EnvironmentSetting("VaultUrl", _vaultUrl),
                },
            };
            await _batch.JobOperations.AddTaskAsync(job.Id, cloudTask);

            job = await _batch.JobOperations.GetJobAsync(job.Id);
            job.OnAllTasksComplete = OnAllTasksComplete.TerminateJob;
            await job.CommitChangesAsync();
        }
    }
}
