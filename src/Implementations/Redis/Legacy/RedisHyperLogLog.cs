using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis.Legacy
{
    /// <summary>
    /// A connection to a redis hyperloglog
    /// </summary>
    public class RedisHyperLogLog : RedisBase, IHyperLogLog
    {
        private readonly string _logName;
        private readonly bool _useSsl;

        /// <summary>
        /// Creates a new RedisHyperLogLog
        /// </summary>
        public RedisHyperLogLog(List<string> endpoints, string password, string logName, bool useSsl)
            : base(endpoints, password, "HyperLogLog", "[default]", useSsl)
        {
            _logName = logName;
            _useSsl = useSsl;
        }

        internal RedisHyperLogLog(string logName, ConfigurationOptions options, bool useSsl)
            : base(options, "HyperLogLog", "[default]", useSsl)
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