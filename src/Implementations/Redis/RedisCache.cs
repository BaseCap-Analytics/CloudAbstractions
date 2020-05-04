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
        public RedisCache(List<string> endpoints, string password)
            : base(endpoints, password, "Cache", "[default]")
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

            return ExecuteRedisCommandAsync(() =>
            {
                IDatabase db = GetRedisDatabase();
                return db.KeyExpireAsync(key, expire.ToUniversalTime().UtcDateTime, CommandFlags.FireAndForget);
            });
        }

        /// <inheritdoc />
        public async Task<T?> GetCacheObjectAsync<T>(string key) where T : class
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            RedisValue value = await ExecuteRedisCommandAsync(() =>
            {
                IDatabase db = GetRedisDatabase();
                return db.StringGetAsync(key);
            });
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

            string str = SerializeObject(obj);
            bool success = await ExecuteRedisCommandAsync(() =>
            {
                IDatabase db = GetRedisDatabase();
                return db.StringSetAsync(key, RedisValue.Unbox(str), expiry);
            });
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

            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() =>
            {
                IDatabase db = GetRedisDatabase();
                return db.KeyDeleteAsync(key, flags);
            });
        }

        /// <inheritdoc />
        public Task<long> AddToListAsync(string key, string value, bool waitForResponse = false)
        {
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return ExecuteRedisCommandAsync(() =>
            {
                IDatabase db = GetRedisDatabase();
                return db.ListRightPushAsync(key, value, flags: flags);
            });
        }

        /// <inheritdoc />
        public async Task<IEnumerable<string>> GetListAsync(string key)
        {
            RedisValue[] values = await ExecuteRedisCommandAsync(() =>
            {
                IDatabase db = GetRedisDatabase();
                return db.ListRangeAsync(key);
            });
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
            RedisValue? val = await ExecuteRedisCommandAsync(() =>
            {
                IDatabase db = GetRedisDatabase();
                return db.ListGetByIndexAsync(key, index);
            });
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
        public Task<long> GetListCountAsync(string key) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            return db.ListLengthAsync(key);
        });

        /// <inheritdoc />
        public Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey, bool waitForResponse = false) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return db.HashIncrementAsync(hashKey, fieldKey, flags: flags);
        });

        /// <inheritdoc />
        public Task<long> IncrementHashKeyAsync(string hashKey, string fieldKey, int increment, bool waitForResponse = false) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return db.HashIncrementAsync(hashKey, fieldKey, increment, flags: flags);
        });

        /// <inheritdoc />
        public Task<bool> SetHashFieldFlagAsync(string hashKey, string fieldKey) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            ITransaction txn = db.CreateTransaction();
            txn.AddCondition(Condition.HashEqual(hashKey, fieldKey, 0));
            txn.HashIncrementAsync(hashKey, fieldKey);
            return txn.ExecuteAsync();
        });

        /// <inheritdoc />
        public Task<bool> DoesHashFieldExistAsync(string hashKey, string fieldKey) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            return db.HashExistsAsync(hashKey, fieldKey);
        });

        /// <inheritdoc />
        public Task<bool> SetHashFieldAsync(string hashKey, string fieldKey, string value, bool waitForResponse = false) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return db.HashSetAsync(hashKey, fieldKey, value, When.Always, flags);
        });

        /// <inheritdoc />
        public async Task<object?> GetHashFieldAsync(string hashKey, string fieldKey)
        {
            RedisValue? value = await ExecuteRedisCommandAsync(() =>
            {
                IDatabase db = GetRedisDatabase();
                return db.HashGetAsync(hashKey, fieldKey);
            });
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
        public Task RemoveHashFieldAsync(string hashKey, string fieldKey) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            return db.HashDeleteAsync(hashKey, fieldKey, CommandFlags.FireAndForget);
        });

        /// <inheritdoc />
        public async Task<Dictionary<string, string?>?> GetAllHashFieldsAsync(string hashKey)
        {
            HashEntry[] entries = await ExecuteRedisCommandAsync(() =>
            {
                IDatabase db = GetRedisDatabase();
                return db.HashGetAllAsync(hashKey);
            });
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

            RedisValue[] fieldValues = await ExecuteRedisCommandAsync(() =>
            {
                IDatabase db = GetRedisDatabase();
                return db.HashGetAsync(hashKey, values);
            });
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
        public IAsyncEnumerable<HashEntry>? GetHashEntriesEnumerable(string hashKey) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            return db.HashScanAsync(hashKey);
        });

        /// <inheritdoc />
        public Task<bool> AddToSetAsync(string setName, string member, bool waitForResponse = false) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return db.SetAddAsync(setName, member, flags);
        });

        /// <inheritdoc />
        public Task RemoveFromSetAsync(string setName, string member, bool waitForResponse = false) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return db.SetRemoveAsync(setName, member, flags);
        });

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetSetMembersAsync(string setName)
        {
            List<string> output = new List<string>();
            RedisValue[] values = await ExecuteRedisCommandAsync(() =>
            {
                output.Clear();

                IDatabase db = GetRedisDatabase();
                return db.SetMembersAsync(setName);
            });
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
        public IAsyncEnumerable<RedisValue>? GetSetMembersEnumerable(string setName) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            return db.SetScanAsync(setName);
        });

        /// <inheritdoc />
        public Task<double> SortedSetIncrementAsync(string setName, string member, double increment = 1, bool waitForResponse = false) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return db.SortedSetIncrementAsync(setName, member, increment);
        });

        /// <inheritdoc />
        public Task SortedSetMemberRemoveAsync(string setName, string member, bool waitForResponse = false) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return db.SortedSetRemoveAsync(setName, member, flags);
        });

        /// <inheritdoc />
        public async Task<IEnumerable<KeyValuePair<string, double>>>  GetSortedSetMembersAsync(string setName, int count, bool sortDesc)
        {
            List<KeyValuePair<string, double>> output = new List<KeyValuePair<string, double>>();
            Order sortOrder = sortDesc ? Order.Descending : Order.Ascending;
            SortedSetEntry[] values = await ExecuteRedisCommandAsync(() =>
            {
                output.Clear();

                IDatabase db = GetRedisDatabase();
                return db.SortedSetRangeByRankWithScoresAsync(setName, 0, count, sortOrder);
            });
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
        public IAsyncEnumerable<SortedSetEntry>? GetSortedSetMembersEnumerable(string setName) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            return db.SortedSetScanAsync(setName);
        });

        /// <inheritdoc />
        public Task SortedSetAddItemByScoreAsync(string setName, string memberToAdd, double score, bool waitForResponse = false) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return db.SortedSetAddAsync(setName, memberToAdd, score, flags);
        });

        /// <inheritdoc />
        public Task<IEnumerable<string>> GetSortedSetItemsByScoreAsync(string setName, double score) => ExecuteRedisCommandAsync(async () =>
        {
            IDatabase db = GetRedisDatabase();
            RedisValue[] values = await db.SortedSetRangeByScoreAsync(setName, score, score);
            return values.Select(v => v.ToString());
        });

        /// <inheritdoc />
        public Task DeleteSortedSetItemByScoreAsync(string setName, double score, bool waitForResponse = false) => ExecuteRedisCommandAsync(() =>
        {
            IDatabase db = GetRedisDatabase();
            CommandFlags flags = waitForResponse ? CommandFlags.None : CommandFlags.FireAndForget;
            return db.SortedSetRemoveRangeByScoreAsync(setName, score, score, flags: flags);
        });
    }
}
