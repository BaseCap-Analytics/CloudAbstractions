using BaseCap.CloudAbstractions.Redis.Protocol;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Database
{
    public abstract partial class RedisDatabase : IDisposable
    {
        public async ValueTask<long> HSetAsync(string keyName, Dictionary<string, string> hashValues, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(keyName))
            {
                throw new ArgumentNullException(nameof(keyName));
            }
            else if (hashValues.Any() == false)
            {
                throw new ArgumentNullException(nameof(hashValues));
            }
            else if (hashValues.Keys.Any(k => string.IsNullOrWhiteSpace(k)))
            {
                throw new ArgumentNullException(nameof(hashValues));
            }
            else if (hashValues.Values.Any(v => string.IsNullOrWhiteSpace(v)))
            {
                throw new ArgumentNullException(nameof(hashValues));
            }
            else if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            string[] toPackage = new string[hashValues.Count * 2];
            int index = 0;
            foreach (KeyValuePair<string, string> kv in hashValues)
            {
                toPackage[index++] = kv.Key;
                toPackage[index++] = kv.Value;
            }

            string cmd = PackageCommand("HSET", keyName, toPackage);
            int bytesReceived = await SendCommandAsync(cmd, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            return ParseIntegerResponse(result);
        }

        public async ValueTask<T> HGetAsync<T>(string keyName, string fieldName, CancellationToken token)
        {
            object? result = await InternalHGetAsync(keyName, fieldName, token);
            if (result == null)
            {
                return default!;
            }
            else if (result is T)
            {
                return (T)result;
            }
            else
            {
                throw new RedisException($"Could not parse {result.GetType()} as {typeof(T)}");
            }
        }

        public async ValueTask<T?> HGetSerializedAsync<T>(string keyName, string fieldName, CancellationToken token) where T : class
        {
            if (string.IsNullOrWhiteSpace(keyName))
            {
                throw new ArgumentNullException(nameof(keyName));
            }
            else if (string.IsNullOrWhiteSpace(fieldName))
            {
                throw new ArgumentNullException(nameof(fieldName));
            }
            else if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            object? data = await InternalHGetAsync(keyName, fieldName, token);
            if (data is string)
            {
                string? strData = data as string;
                if (string.IsNullOrWhiteSpace(strData))
                {
                    return null;
                }
                else
                {
                    return JsonConvert.DeserializeObject<T>(strData);
                }
            }
            else
            {
                throw new RedisException("Data in Hash Field is not a string");
            }
        }

        private async ValueTask<object?> InternalHGetAsync(string keyName, string fieldName, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(keyName))
            {
                throw new ArgumentNullException(nameof(keyName));
            }
            else if (string.IsNullOrWhiteSpace(fieldName))
            {
                throw new ArgumentNullException(nameof(fieldName));
            }
            else if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            string cmd = PackageCommand("HGET", keyName, fieldName);
            int bytesReceived = await SendCommandAsync(cmd, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            return ParseScalarResponse(result);
        }

        public async ValueTask<long> HIncrementByAsync(string keyName, string fieldName, CancellationToken token, long increment = 1)
        {
            if (string.IsNullOrWhiteSpace(keyName))
            {
                throw new ArgumentNullException(nameof(keyName));
            }
            else if (string.IsNullOrWhiteSpace(fieldName))
            {
                throw new ArgumentNullException(nameof(fieldName));
            }
            else if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            string cmd = PackageCommand("HINCRBY", keyName, fieldName, increment.ToString());
            int bytesReceived = await SendCommandAsync(cmd, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            return ParseIntegerResponse(result);
        }

        public async ValueTask<long> HDelAsync(string keyName, CancellationToken token, params string[] fields)
        {
            if (string.IsNullOrWhiteSpace(keyName))
            {
                throw new ArgumentNullException(nameof(keyName));
            }
            else if (fields.Any() == false)
            {
                throw new ArgumentNullException(nameof(fields));
            }
            else if (fields.Any(f => string.IsNullOrWhiteSpace(f)))
            {
                throw new ArgumentNullException(nameof(fields), "Empty field is not supported");
            }
            else if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            string cmd = PackageCommand("HDEL", keyName, fields);
            int bytesReceived = await SendCommandAsync(cmd, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            return ParseIntegerResponse(result);
        }
    }
}
