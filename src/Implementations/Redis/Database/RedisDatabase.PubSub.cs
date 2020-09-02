using BaseCap.CloudAbstractions.Redis.Protocol;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Database
{
    public abstract partial class RedisDatabase : IDisposable
    {
        protected const string PUBSUB_RESPONSE_KIND = "message";

        public async ValueTask SubscribeAsync(string channel, ChannelWriter<object?> handler, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(channel))
            {
                throw new ArgumentNullException(nameof(channel));
            }

            string cmd = PackageCommand("SUBSCRIBE", channel);
            int bytesReceived = await SendCommandAsync(cmd, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            _isPubSubMode = true;
            _pubsubToken = token;
            _pubsubReader = ReadPubSubAsync();
            _pubsubSender = handler;
        }

        public async ValueTask UnsubscribeAsync(string channel, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(channel))
            {
                throw new ArgumentNullException(nameof(channel));
            }

            string cmd = PackageCommand("UNSUBSCRIBE", channel);
            await SendCommandAsync(cmd, token);
            _isPubSubMode = false;
        }

        public async ValueTask<long> PublishAsync(string channel, string message, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(channel))
            {
                throw new ArgumentNullException(nameof(channel));
            }
            else if (string.IsNullOrWhiteSpace(message))
            {
                throw new ArgumentNullException(nameof(message));
            }
            else if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            string cmd = PackageCommand("PUBLISH", channel, message);
            int bytesReceived = await SendCommandAsync(cmd, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            return ParseIntegerResponse(result);
        }

        private async Task ReadPubSubAsync()
        {
            while ((_pubsubToken?.IsCancellationRequested == false) && _isPubSubMode)
            {
                int bytesRead = await _connection.ReadAdditionalResponseDataFromBufferAsync(_pubsubToken.Value);
                if (bytesRead > 0)
                {
                    List<DataType> parsed = await _parser.ParseAsync(bytesRead, _pubsubToken.Value);
                    object? result = GetPubSubMessageContents(parsed);
                    if (_pubsubSender != null)
                    {
                        await _pubsubSender.WriteAsync(result, _pubsubToken.Value);
                    }
                }
            }
        }

        protected abstract object? GetPubSubMessageContents(List<DataType> received);
    }
}
