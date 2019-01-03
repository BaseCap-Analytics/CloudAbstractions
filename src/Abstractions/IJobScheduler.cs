using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    public interface IJobScheduler : IDisposable
    {
        Task SetupAsync();
        Task SetPoolStartTaskAsync(string poolId, string commandLine);
        Task CreateJobAsync(Abstractions.CloudJob jobInput, CloudJobTask taskInput, string poolId);
        Task CreateOrUpdateCloudJobApplicationAsync(IBlobStorage sourceBlobStorage, CloudJobApplication application, IEnumerable<string> poolNames);
        Task<bool> DoesApplicationPackageExistAsync(string appId, string version);
        Task ChangePoolSizeAsync(string poolName, int newSize);
        Task CreatedScheduledJobAsync(CloudScheduledJob scheduledJob, Abstractions.CloudJob jobToRun, CloudJobTask taskToRun, string poolId);
        Task<bool> DoesScheduledJobExistAsync(string scheduleId);
    }
}
