using System;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Abstractions
{
    public interface IJobScheduler : IDisposable
    {
        Task SetupAsync();
        Task CreateJobAsync(Abstractions.CloudJob jobInput, CloudJobTask taskInput, string poolId);
    }
}
