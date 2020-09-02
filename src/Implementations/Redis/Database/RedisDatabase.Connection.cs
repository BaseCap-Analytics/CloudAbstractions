using BaseCap.CloudAbstractions.Redis.Protocol;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Database
{
    public abstract partial class RedisDatabase : IDisposable
    {
        private const string CLIENT_REPLY_ON = "ON";
        private const string CLIENT_REPLY_OFF = "OFF";
        private const string CLIENT_REPLY_SKIP = "SKIP";

        internal async ValueTask<bool> AuthAsync(string password, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(password))
            {
                throw new ArgumentNullException(nameof(password));
            }
            else if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            string authCommand = PackageCommand("AUTH", password);
            int bytesReceived = await SendCommandAsync(authCommand, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            return ParseOkResult(result);
        }

        public async ValueTask<Dictionary<string, string>> HelloAsync(CancellationToken token)
        {
            if (_isPubSubMode)
            {
                throw new InvalidOperationException("Cannot send command in PubSub mode");
            }

            string cmd = PackageCommand("HELLO", ProtocolVersion.ToString());
            int bytesReceived = await SendCommandAsync(cmd, token);
            List<DataType> result = await _parser.ParseAsync(bytesReceived, token);
            return ParseDictionaryResponse(result);
        }

        internal ValueTask SetClientReplyOnAsync(CancellationToken token) => SetClientReplyAsync(CLIENT_REPLY_ON, expectReply: true, token);
        internal ValueTask SetClientReplyOffAsync(CancellationToken token) => SetClientReplyAsync(CLIENT_REPLY_OFF, expectReply: false, token);
        internal ValueTask SetClientReplySkipAsync(CancellationToken token) => SetClientReplyAsync(CLIENT_REPLY_SKIP, expectReply: false, token);
        private async ValueTask SetClientReplyAsync(string replyType, bool expectReply, CancellationToken token)
        {
            string cmd = PackageCommand("CLIENT", "REPLY", replyType);
            if (expectReply)
            {
                int bytesReceived = await SendCommandAsync(cmd, token);
                await _parser.ParseAsync(bytesReceived, token); // If this responds, it'll be with OK
            }
        }
    }
}
