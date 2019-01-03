using BaseCap.CloudAbstractions.Abstractions;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations
{
    public class AzureBatch : IJobScheduler
    {
        private readonly string _authority;
        private string _appId;
        private string _appSecret;
        private string _batchAccountUrl;
        private string _batchAccountName;
        private string _batchAccountKey;
        private string _vaultUrl;
        private string _subscriptionId;
        private string _resourceGroup;
        private BatchClient _batch;
        private BatchManagementClient _management;

        public AzureBatch(
            string accountUrl,
            string accountName,
            string accountKey,
            string appId,
            string appSecret,
            string keyVaultUrl,
            string tenantId,
            string subscriptionId,
            string resourceGroup)
        {
            _batchAccountUrl = accountUrl;
            _batchAccountName = accountName;
            _batchAccountKey = accountKey;
            _appId = appId;
            _appSecret = appSecret;
            _vaultUrl = keyVaultUrl;
            _subscriptionId = subscriptionId;
            _resourceGroup = resourceGroup;
            _authority = $"https://login.microsoftonline.com/{tenantId}";
            _batch = null;
            _management = null;
        }

        protected virtual void Dispose(bool disposing)
        {
            if ((_batch != null) && (disposing))
            {
                _batch.Dispose();
                _batch = null;
                _management.Dispose();
                _management = null;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public async Task SetupAsync()
        {
            string token = await GetActiveDirectoryTokenAsync();
            _batch = BatchClient.Open(new BatchSharedKeyCredentials(_batchAccountUrl, _batchAccountName, _batchAccountKey));
            _management = new BatchManagementClient(new TokenCredentials(token, "Bearer")) { SubscriptionId = _subscriptionId };
        }

        public async Task SetPoolStartTaskAndRebootAsync(string poolId, string commandLine)
        {
            Pool currentPool = await _management.Pool.GetAsync(_resourceGroup, _batchAccountName, poolId);
            Pool poolUpdate = new Pool(id: currentPool.Id)
            {
                StartTask = new Microsoft.Azure.Management.Batch.Models.StartTask()
                {
                    CommandLine = commandLine,
                    MaxTaskRetryCount = 0,
                    WaitForSuccess = true,
                    UserIdentity = new Microsoft.Azure.Management.Batch.Models.UserIdentity()
                    {
                        AutoUser = new Microsoft.Azure.Management.Batch.Models.AutoUserSpecification()
                        {
                            ElevationLevel = Microsoft.Azure.Management.Batch.Models.ElevationLevel.Admin,
                            Scope = Microsoft.Azure.Management.Batch.Models.AutoUserScope.Task,
                        }
                    }
                }
            };
            await _management.Pool.UpdateAsync(_resourceGroup, _batchAccountName, poolId, poolUpdate);
        }

        private async Task<string> GetActiveDirectoryTokenAsync()
        {
            AuthenticationResult result = null;
            int retryCount = 0;

            do
            {
                try
                {
                    ClientCredential creds = new ClientCredential(_appId, _appSecret);
                    AuthenticationContext context = new AuthenticationContext(_authority, false);
                    result = await context.AcquireTokenAsync("https://management.azure.com/", creds);
                    break;
                }
                catch (AdalException aex)
                {
                    if (aex.ErrorCode == "temporarily_unavailable")
                    {
                        await Task.Delay(3000);
                        retryCount++;
                    }

                    if (retryCount >= 3)
                    {
                        throw;
                    }
                }
            }
            while (retryCount < 3);

            return result.AccessToken;
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
                EnvironmentSettings = new List<Microsoft.Azure.Batch.EnvironmentSetting>()
                {
                    new Microsoft.Azure.Batch.EnvironmentSetting("AppId", _appId),
                    new Microsoft.Azure.Batch.EnvironmentSetting("AppSecret", _appSecret),
                    new Microsoft.Azure.Batch.EnvironmentSetting("VaultUrl", _vaultUrl),
                },
            };
            await _batch.JobOperations.AddTaskAsync(job.Id, cloudTask);

            job = await _batch.JobOperations.GetJobAsync(job.Id);
            job.OnAllTasksComplete = OnAllTasksComplete.TerminateJob;
            await job.CommitChangesAsync();
        }

        public async Task CreateOrUpdateCloudJobApplicationAsync(IBlobStorage sourceBlobStorage, CloudJobApplication application, IEnumerable<string> poolNames)
        {
            // Create the Application Package
            ApplicationPackage pack = await _management.ApplicationPackage.CreateAsync(
                _resourceGroup,
                _batchAccountName,
                application.Id,
                application.Version);
            CloudBlockBlob blob = new CloudBlockBlob(new Uri(pack.StorageUrl));
            blob.Properties.ContentType = "application/x-zip-compressed";
            await blob.UploadFromStreamAsync(await sourceBlobStorage.GetBlobReadStreamAsync(application.SourceZipFile));
            await _management.ApplicationPackage.ActivateAsync(_resourceGroup, _batchAccountName, application.Id, application.Version, "zip");

            // Apply the application to the pools
            foreach (string poolName in poolNames)
            {
                Pool currentPool = await _management.Pool.GetAsync(_resourceGroup, _batchAccountName, poolName);
                Pool poolToChange = new Pool(id: currentPool.Id, applicationPackages: currentPool.ApplicationPackages);
                string addAppId = $"/subscriptions/{_subscriptionId}/resourceGroups/{_resourceGroup}/providers/Microsoft.Batch/batchAccounts/{_batchAccountName}/applications/{application.Id}";
                currentPool.ApplicationPackages.Add(new Microsoft.Azure.Management.Batch.Models.ApplicationPackageReference(application.Id, application.Version));
                await _management.Pool.UpdateAsync(_resourceGroup, _batchAccountName, poolName, currentPool);
            }
        }

        public async Task<bool> DoesApplicationPackageExistAsync(string appId, string version)
        {
            try
            {
                ApplicationPackage pack = await _management.ApplicationPackage.GetAsync(_resourceGroup, _batchAccountName, appId, version);
                return pack?.Id == appId;
            }
            catch
            {
                return false;
            }
        }

        public async Task ChangePoolSizeAsync(string poolName, int newSize)
        {
            Pool currentPool = await _management.Pool.GetAsync(_resourceGroup, _batchAccountName, poolName);
            Pool poolUpdate = new Pool(id: currentPool.Id)
            {
                ScaleSettings = new ScaleSettings()
                {
                    FixedScale = new FixedScaleSettings()
                    {
                        NodeDeallocationOption = Microsoft.Azure.Management.Batch.Models.ComputeNodeDeallocationOption.TaskCompletion,
                        TargetDedicatedNodes = newSize,
                    }
                }
            };
            await _management.Pool.UpdateAsync(_resourceGroup, _batchAccountName, poolName, poolUpdate);
        }

        public async Task CreatedScheduledJobAsync(CloudScheduledJob scheduledJob, Abstractions.CloudJob jobToRun, CloudJobTask taskToRun, string poolId)
        {
            CloudJobSchedule schedule = _batch.JobScheduleOperations.CreateJobSchedule(
                scheduledJob.Id,
                new Schedule()
                {
                    RecurrenceInterval = scheduledJob.Recurrence,
                },
                new JobSpecification()
                {
                    DisplayName = scheduledJob.Name,
                    OnAllTasksComplete = OnAllTasksComplete.TerminateJob,
                    OnTaskFailure = OnTaskFailure.PerformExitOptionsJobAction,
                    PoolInformation = new PoolInformation()
                    {
                        PoolId = poolId,
                    },
                    JobManagerTask = new JobManagerTask(taskToRun.Id, taskToRun.CommandLine)
                    {
                        KillJobOnCompletion = true,
                        EnvironmentSettings = new List<Microsoft.Azure.Batch.EnvironmentSetting>()
                        {
                            new Microsoft.Azure.Batch.EnvironmentSetting("AppId", _appId),
                            new Microsoft.Azure.Batch.EnvironmentSetting("AppSecret", _appSecret),
                            new Microsoft.Azure.Batch.EnvironmentSetting("VaultUrl", _vaultUrl),
                        },
                    }
                });
            await schedule.CommitAsync();
            await _batch.JobScheduleOperations.EnableJobScheduleAsync(schedule.Id);
        }

        public async Task<bool> DoesScheduledJobExistAsync(string scheduleId)
        {
            try
            {
                CloudJobSchedule schedule = await _batch.JobScheduleOperations.GetJobScheduleAsync(scheduleId);
                return schedule?.Id == scheduleId;
            }
            catch
            {
                return false;
            }
        }
    }
}
