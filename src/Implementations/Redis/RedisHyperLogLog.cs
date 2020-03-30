using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// A connection to a redis hyperloglog
    /// </summary>
    public class RedisHyperLogLog : RedisBase, IHyperLogLog
    {
        private readonly string _logName;

        /// <summary>
        /// Creates a new RedisHyperLogLog
        /// </summary>
        public RedisHyperLogLog(string connectionString, string logName)
            : base(connectionString, "HyperLogLog", "[default]")
        {
            _logName = logName;
        }

        internal RedisHyperLogLog(string logName, ConfigurationOptions options)
            : base(options, "HyperLogLog", "[default]")
        {
            _logName = logName;
        }

        /// <inheritdoc />
        public Task SetupAsync()
        {
            return base.InitializeAsync();
        }

        /// <inheritdoc />
        public Task<bool> CheckIfUniqueAsync(string key) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            return db.HyperLogLogAddAsync(_logName, key);
        });

        /// <inheritdoc />
        public Task<long> GetUniqueCountAsync() => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            return db.HyperLogLogLengthAsync(_logName);
        });

        /// <inheritdoc />
        public Task DeleteLogAsync() => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            return db.KeyDeleteAsync(_logName);
        });
    }
}
