using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Database
{
    public interface IRedisDatabase
    {
        ValueTask<Dictionary<string, string>> HelloAsync(CancellationToken token);

        ValueTask<long> HSetAsync(string keyName, Dictionary<string, string> hashValues, CancellationToken token);

        ValueTask<string?> HGetAsync(string keyName, string fieldName, CancellationToken token);

        ValueTask<T?> HGetAsync<T>(string keyName, string fieldName, CancellationToken token) where T : class;

        ValueTask<long> HIncrementByAsync(string keyName, string fieldName, CancellationToken token, long increment = 1);

        ValueTask<long> HDelAsync(string keyName, CancellationToken token, params string[] fields);

        ValueTask<bool> ExistsAsync(CancellationToken token, params string[] keys);

        ValueTask<long> ExpireAtAsync(string key, DateTimeOffset when, CancellationToken token);

        ValueTask<long> DelAsync(CancellationToken token, params string[] keyNames);

        ValueTask SubscribeAsync(string channel, ChannelWriter<object?> handler, CancellationToken token);

        ValueTask UnsubscribeAsync(string channel, CancellationToken token);

        ValueTask<long> PublishAsync(string channel, string message, CancellationToken token);
    }
}
