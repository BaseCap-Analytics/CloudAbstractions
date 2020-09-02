using BaseCap.CloudAbstractions.Redis.Cluster;
using BaseCap.CloudAbstractions.Redis.Protocol;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Database
{
    public abstract partial class RedisDatabase : IDisposable, IRedisDatabase
    {
        protected abstract int ProtocolVersion { get; }
        protected private readonly ParserBase _parser;
        protected private ConnectionBase _connection;
        protected ChannelWriter<object?>? _pubsubSender;
        private bool _isPubSubMode;
        private Task? _pubsubReader;
        private CancellationToken? _pubsubToken;

        internal RedisDatabase(ConnectionBase connection, ParserBase parser)
        {
            _connection = connection;
            _parser = parser;
            _isPubSubMode = false;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing && (_connection != null))
            {
#nullable disable
                _isPubSubMode = false;
                _pubsubSender?.TryComplete();
                _pubsubSender = null;
                _connection.Dispose();
                _connection = null;
#nullable enable
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        protected abstract string PackageCommand(string commandName, string keyName, params string[] args);

        protected abstract string PackageCommand(string commandName, params string[] args);

        protected ValueTask<int> SendCommandAsync(string command, CancellationToken token) => _connection.SendCommandAsync(command, token);

        protected ValueTask SendCommandWithoutResponseAsync(string command, CancellationToken token) => _connection.SendCommandWithoutResponseAsync(command, token);

        protected abstract bool ParseOkResult(List<DataType> received);

        protected abstract long ParseIntegerResponse(List<DataType> received);

        protected abstract string? ParseStringResponse(List<DataType> received);

        protected abstract object? ParseScalarResponse(List<DataType> received);

        protected abstract Dictionary<string, string> ParseDictionaryResponse(List<DataType> received);
    }
}
