using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System.Collections.Generic;
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
        public RedisHyperLogLog(IEnumerable<string> endpoints, string password, string logName, bool useSsl, ILogger logger)
            : base(endpoints, password, useSsl, "HyperLogLog", "[default]", logger)
        {
            _logName = logName;
        }

        internal RedisHyperLogLog(string logName, ConfigurationOptions options, ILogger logger)
            : base(options, "HyperLogLog", "[default]", logger)
        {
            _logName = logName;
        }

        /// <inheritdoc />
        public Task SetupAsync()
        {
            return base.InitializeAsync();
        }

        /// <inheritdoc />
        public Task<bool> CheckIfUniqueAsync(string key)
        {
            return _database.HyperLogLogAddAsync(_logName, key);
        }

        /// <inheritdoc />
        public Task<long> GetUniqueCountAsync()
        {
            return _database.HyperLogLogLengthAsync(_logName);
        }

        /// <inheritdoc />
        public Task DeleteLogAsync()
        {
            return _database.KeyDeleteAsync(_logName);
        }
    }
}
