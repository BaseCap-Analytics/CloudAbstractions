using BaseCap.CloudAbstractions.Redis.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Database
{
    public abstract partial class RedisDatabase : IDisposable
    {
        public async ValueTask<bool> ExistsAsync(CancellationToken token, params string[] keys)
        {
            if (keys.Length < 1)
            {
                throw new ArgumentNullException(nameof(keys));
            }
            else if (keys.Any(k => string.IsNullOrWhiteSpace(k)))
            {
                throw new ArgumentNullException(nameof(keys), "Empty key is not allowed");
            }
            else if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            string cmd = PackageCommand("EXISTS", keys);
            int bytesReceived = await SendCommandAsync(cmd, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            return ParseIntegerResponse(result) == keys.Length;
        }

        public async ValueTask<bool> ExpireAtAsync(string key, DateTimeOffset when, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }
            else if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            string cmd = PackageCommand("EXPIREAT", key, when.ToUnixTimeSeconds().ToString());
            int bytesReceived = await SendCommandAsync(cmd, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            return ParseIntegerResponse(result) == 1;
        }

        public async ValueTask<long> DelAsync(CancellationToken token, params string[] keyNames)
        {
            if (keyNames.Any() == false)
            {
                throw new ArgumentNullException(nameof(keyNames));
            }
            else if (keyNames.Any(k => string.IsNullOrWhiteSpace(k)))
            {
                throw new ArgumentNullException(nameof(keyNames), "Empty key name not allowed");
            }
            else if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            string cmd = PackageCommand("DEL", keyNames);
            int bytesReceived = await SendCommandAsync(cmd, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            return ParseIntegerResponse(result);
        }
    }
}
