using BaseCap.CloudAbstractions.Abstractions;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Implementations.Redis
{
    /// <summary>
    /// A connection to a redis cache cluster
    /// </summary>
    public class RedisCache : RedisBase, ICache
    {
        /// <summary>
        /// Creates a new RedisCache
        /// </summary>
        public RedisCache(string connectionString)
            : base(connectionString, "Cache", "[default]")
        {
        }

        public Task SetupAsync()
        {
            return base.InitializeAsync();
        }

        /// <inheritdoc />
        public Task SetKeyExpiryAsync(string key, DateTimeOffset expire)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            IDatabase db = GetRedisDatabase();
            return ExecuteRedisCommandAsync(() => db.KeyExpireAsync(key, expire.ToUniversalTime().UtcDateTime, CommandFlags.FireAndForget));
        }

        /// <inheritdoc />
        public async Task<T?> GetCacheObjectAsync<T>(string key) where T : class
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            IDatabase db = GetRedisDatabase();
            RedisValue value = await ExecuteRedisCommandAsync(() => db.StringGetAsync(key));
            if (value.IsNullOrEmpty)
            {
                return null;
            }
            else
            {
                return DeserializeObject<T>(value);
            }
        }

        /// <inheritdoc />
        public Task SetCacheObjectAsync<T>(string key, T obj, TimeSpan expiry) where T : class
        {
            return SetCacheObjectInternalAsync(key, obj, expiry);
        }

        private async Task SetCacheObjectInternalAsync<T>(string key, T obj, TimeSpan? expiry) where T : class
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }
            else if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            IDatabase db = GetRedisDatabase();
            string str = SerializeObject(obj);
            bool success = await ExecuteRedisCommandAsync(() => db.StringSetAsync(key, RedisValue.Unbox(str), expiry));
            if (success == false)
            {
                throw new InvalidOperationException($"Failed to set cache entry for '{key}'");
            }
        }

        /// <inheritdoc />
        public Task<bool> DeleteCacheObjectAsync(string key, bool waitForResponse = false)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() => db.KeyDeleteAsync(key, flags));
        }

        /// <inheritdoc />
        public Task<long> AddToListAsync(string key, string value, bool waitForResponse = false)
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() => db.ListRightPushAsync(key, value, flags: flags));
        }

        /// <inheritdoc />
        public async Task<IEnumerable<string>> GetListAsync(string key)
        {
            IDatabase db = GetRedisDatabase();
            RedisValue[] values = await ExecuteRedisCommandAsync(() => db.ListRangeAsync(key));
            if (values == null)
            {
                return Array.Empty<string>();
            }
            else
            {
                return values.Select(v => (string)v);
            }
        }

        /// <inheritdoc />
        public async Task<string> GetListElementAtIndexAsync(string key, long index)
        {
            IDatabase db = GetRedisDatabase();
            RedisValue? val = await ExecuteRedisCommandAsync(() => db.ListGetByIndexAsync(key, index));
            if ((val == null) || val.Value.IsNullOrEmpty || string.IsNullOrWhiteSpace(val.Value.ToString()))
            {
                throw new ArgumentException($"No element found at index {index}");
            }
            else
            {
                return val.Value.ToString();
            }
        }

        /// <inheritdoc />
        public Task<long> GetListCountAsync(string key)
        {
            IDatabase db = GetRedisDatabase();
            return ExecuteRedisCommandAsync(() => db.ListLengthAsync(key));
        }

        /// <inheritdoc />
        public Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey, bool waitForResponse = false)
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() => db.HashIncrementAsync(hashKey, fieldKey, flags: flags));
        }

        /// <inheritdoc />
        public Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey, int increment, bool waitForResponse = false)
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() => db.HashIncrementAsync(hashKey, fieldKey, increment, flags: flags));
        }

        /// <inheritdoc />
        public Task<bool> SetHashFieldFlagAsync(string hashKey, string fieldKey)
        {
            IDatabase db = GetRedisDatabase();
            ITransaction txn = db.CreateTransaction();
            txn.AddCondition(Condition.HashEqual(hashKey, fieldKey, 0));
            txn.HashIncrementAsync(hashKey, fieldKey);
            return ExecuteRedisCommandAsync(() => txn.ExecuteAsync());
        }

        /// <inheritdoc />
        public Task<bool> DoesHashFieldExistAsync(string hashKey, string fieldKey)
        {
            IDatabase db = GetRedisDatabase();
            return ExecuteRedisCommandAsync(() => db.HashExistsAsync(hashKey, fieldKey));
        }

        /// <inheritdoc />
        public Task<bool> SetHashFieldAsync(string hashKey, string fieldKey, string value, bool waitForResponse = false)
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() => db.HashSetAsync(hashKey, fieldKey, value, When.Always, flags));
        }

        /// <inheritdoc />
        public async Task<object?> GetHashFieldAsync(string hashKey, string fieldKey)
        {
            IDatabase db = GetRedisDatabase();
            RedisValue? value = await ExecuteRedisCommandAsync(() => db.HashGetAsync(hashKey, fieldKey));
            if ((value == null) || value.Value.IsNullOrEmpty || string.IsNullOrWhiteSpace(value.Value.ToString()))
            {
                return null;
            }
            else
            {
                if (value.Value.TryParse(out long lngval))
                {
                    return lngval;
                }
                else if (value.Value.TryParse(out double dblval))
                {
                    return dblval;
                }
                else
                {
                    return value.Value.ToString();
                }
            }
        }

        /// <inheritdoc />
        public Task RemoveHashFieldAsync(string hashKey, string fieldKey)
        {
            IDatabase db = GetRedisDatabase();
            return ExecuteRedisCommandAsync(() => db.HashDeleteAsync(hashKey, fieldKey, CommandFlags.FireAndForget));
        }

        /// <inheritdoc />
        public async Task<Dictionary<string, string?>?> GetAllHashFieldsAsync(string hashKey)
        {
            IDatabase db = GetRedisDatabase();
            HashEntry[] entries = await ExecuteRedisCommandAsync(() => db.HashGetAllAsync(hashKey));
            Dictionary<string, string?>? lookup = new Dictionary<string, string?>();
            if (entries.Any())
            {
                foreach (HashEntry e in entries)
                {
                    string? val = e.Value.ToString();
                    if (string.IsNullOrWhiteSpace(val))
                    {
                        val = null;
                    }

                    lookup.Add(e.Name, val);
                }
            }
           else
           {
               lookup = null;
           }

            return lookup;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<long?>> GetHashKeyFieldValuesAsync(string hashKey, params string[] fields)
        {
            RedisValue[] values = new RedisValue[fields.Length];
            for (int i = 0; i < fields.Length; i++)
            {
                values[i] = fields[i];
            }

            IDatabase db = GetRedisDatabase();
            RedisValue[] fieldValues = await ExecuteRedisCommandAsync(() => db.HashGetAsync(hashKey, values));
            long?[] returnValues = new long?[fieldValues.Length];
            for (int i = 0; i < fieldValues.Length; i++)
            {
                if (fieldValues[i].HasValue)
                {
                    try
                    {
                        returnValues[i] = (long)fieldValues[i];
                    }
                    catch
                    {
                        returnValues[i] = null;
                    }
                }
                else
                {
                    returnValues[i] = null;
                }
            }

            return returnValues;
        }

        /// <inheritdoc />
        public IAsyncEnumerable<HashEntry>? GetHashEntriesEnumerable(string hashKey)
        {
            IDatabase db = GetRedisDatabase();
            return ExecuteRedisCommandAsync(() => db.HashScanAsync(hashKey));
        }

        /// <inheritdoc />
        public Task<bool> AddToSetAsync(string setName, string member, bool waitForResponse = false)
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() => db.SetAddAsync(setName, member, flags));
        }

        /// <inheritdoc />
        public Task RemoveFromSetAsync(string setName, string member, bool waitForResponse = false)
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() => db.SetRemoveAsync(setName, member, flags));
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetSetMembersAsync(string setName)
        {
            IDatabase db = GetRedisDatabase();
            List<string> output = new List<string>();
            RedisValue[] values = await ExecuteRedisCommandAsync(() => db.SetMembersAsync(setName));
            if (values.Any())
            {
                foreach (RedisValue v in values)
                {
                    output.Add(v.ToString());
                }
            }

            return output;
        }

        /// <inheritdoc />
        public IAsyncEnumerable<RedisValue>? GetSetMembersEnumerable(string setName)
        {
            IDatabase db = GetRedisDatabase();
            return ExecuteRedisCommandAsync(() => db.SetScanAsync(setName));
        }

        /// <inheritdoc />
        public Task<double> SortedSetIncrementAsync(string setName, string member, double increment = 1, bool waitForResponse = false)
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() => db.SortedSetIncrementAsync(setName, member, increment));
        }

        /// <inheritdoc />
        public Task SortedSetMemberRemoveAsync(string setName, string member, bool waitForResponse = false)
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() => db.SortedSetRemoveAsync(setName, member, flags));
        }

        /// <inheritdoc />
        public async Task<IEnumerable<KeyValuePair<string, double>>>  GetSortedSetMembersAsync(string setName, int count, bool sortDesc)
        {
            List<KeyValuePair<string, double>> output = new List<KeyValuePair<string, double>>();
            Order sortOrder = sortDesc ? Order.Descending : Order.Ascending;
            IDatabase db = GetRedisDatabase();
            SortedSetEntry[] values = await ExecuteRedisCommandAsync(() => db.SortedSetRangeByRankWithScoresAsync(setName, 0, count, sortOrder));
            if (values.Any())
            {
                foreach (SortedSetEntry v in values)
                {
                    output.Add(new KeyValuePair<string, double>(v.Element.ToString(), v.Score));
                }
            }

            return output;
        }

        /// <inheritdoc />
        public IAsyncEnumerable<SortedSetEntry>? GetSortedSetMembersEnumerable(string setName)
        {
            IDatabase db = GetRedisDatabase();
            return ExecuteRedisCommandAsync(() => db.SortedSetScanAsync(setName));
        }

        /// <inheritdoc />
        public Task SortedSetAddItemByScoreAsync(string setName, string memberToAdd, double score, bool waitForResponse = false)
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() => db.SortedSetAddAsync(setName, memberToAdd, score, flags));
        }

        /// <inheritdoc />
        public async Task<IEnumerable<string>> GetSortedSetItemsByScoreAsync(string setName, double score)
        {
            IDatabase db = GetRedisDatabase();
            RedisValue[] values = await ExecuteRedisCommandAsync(() => db.SortedSetRangeByScoreAsync(setName, score, score));
            return values.Select(v => v.ToString());
        }
    }
}
