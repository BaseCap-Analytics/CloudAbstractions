using System;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Cluster
{
    internal abstract class ConnectionBase : IDisposable
    {
        protected const int MINIMUM_BUFFER_SIZE_BYTES = 4096;
        protected Stream? _stream;
        protected readonly PipeWriter _dataPipe;
        private const int RETRY_TIMEOUT_IN_SECONDS = 2;
        private const int MAX_RETRIES = 3;
        private static int _reconnectCount = 0;
        private readonly string _hostname;
        private readonly IPAddress _host;
        private readonly ushort _port;
        private readonly bool _useSsl;
        private TcpClient? _client;

        internal ConnectionBase(string hostname, IPAddress host, ushort port, bool useSsl, PipeWriter pipe)
        {
            _hostname = hostname;
            _host = host;
            _port = port;
            _useSsl = useSsl;
            _dataPipe = pipe;
        }

        private void Dispose(bool disposing)
        {
            if (disposing && (_client != null))
            {
                _client.Dispose();
                _client = null;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        protected void CheckForOpenStream()
        {
            if (_stream == null)
            {
                throw new InvalidOperationException("Connection must be opened before use");
            }
        }

        internal ValueTask OpenAsync(CancellationToken token) => ReconnectAsync(token);

        internal async ValueTask ReconnectAsync(CancellationToken token)
        {
            // Make sure we don't get stuck in a reconnect loop
            if (_reconnectCount >= MAX_RETRIES)
            {
                throw new InvalidOperationException("Could not connect to Redis");
            }
            else
            {
                _reconnectCount++;
            }

            _client = new TcpClient();
            _client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.DontLinger, true);
            _client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _client.Client.NoDelay = true;

            // If we're targeting windows, set the SIO_LOOPBACK_FAST_PATH flag for faster access to the loopback address
            if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows))
            {
                const int SIO_LOOPBACK_FAST_PATH = -1744830448;

                // Win8/Server2012+ only
                if ((Environment.OSVersion.Version.Major > 6) || ((Environment.OSVersion.Version.Major == 6) && (Environment.OSVersion.Version.Minor >= 2)))
                {
                    byte[] optionInValue = BitConverter.GetBytes(1);
                    _client.Client.IOControl(SIO_LOOPBACK_FAST_PATH, optionInValue, null);
                }
            }

            Task connect = _client.ConnectAsync(_host, _port);
            Task timeout = Task.Delay(TimeSpan.FromSeconds(60));
            Task cancel = Task.FromCanceled(token);
            Task completed = await Task.WhenAny(connect, timeout, cancel);

            if (completed == connect)
            {
                if (_useSsl)
                {
                    SslStream ssl = new SslStream(_client.GetStream(), true, new RemoteCertificateValidationCallback(ValidateServerCertificate), null);
                    await ssl.AuthenticateAsClientAsync(_hostname);
                    _stream = ssl;
                }
                else
                {
                    _stream = _client.GetStream();
                }

                // We've successfully connected, so reset our counter
                _reconnectCount = 0;
            }
            else if (completed == timeout)
            {
                throw new TaskCanceledException("Timeout connecting to Redis");
            }
        }

        private static bool ValidateServerCertificate(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            return sslPolicyErrors == SslPolicyErrors.None;
        }

        internal async ValueTask<int> SendCommandAsync(string commandPackage, CancellationToken token)
        {
            CheckForOpenStream();

            await InternalSendCommandAsync(commandPackage, token);

            return await ReadAvailableDataAsync(token);
        }

        internal ValueTask SendCommandWithoutResponseAsync(string commandPackage, CancellationToken token)
        {
            CheckForOpenStream();
            return InternalSendCommandAsync(commandPackage, token);
        }

        protected abstract ValueTask InternalSendCommandAsync(string commandPackage, CancellationToken token);

        internal ValueTask<int> ReadAdditionalResponseDataFromBufferAsync(CancellationToken token) => ReadAvailableDataAsync(token);

        internal ValueTask<int> ListenForDataAsync(CancellationToken token) => ReadAvailableDataAsync(token);

        protected abstract ValueTask<int> ReadAvailableDataAsync(CancellationToken token);

        protected async ValueTask ExecuteTaskWithRetryAsync(ValueTask toPerform, CancellationToken token)
        {
            bool successful = false;
            for (int i = 0; (i < MAX_RETRIES) && (successful == false); i++)
            {
                try
                {
                    await toPerform;
                    successful = true;
                }
                catch
                {
                    await Task.Delay(TimeSpan.FromSeconds(RETRY_TIMEOUT_IN_SECONDS));

                    if (_client?.Connected == false)
                    {
                        await ReconnectAsync(token);
                    }
                }
            }
        }

        protected async ValueTask<int> ExecuteTaskWithRetryAndResultAsync(ValueTask<int> toPerform, CancellationToken token)
        {
            int result = 0;
            bool successful = false;
            for (int i = 0; (i < MAX_RETRIES) && (successful == false); i++)
            {
                try
                {
                    result = await toPerform;
                    successful = true;
                }
                catch
                {
                    await Task.Delay(TimeSpan.FromSeconds(RETRY_TIMEOUT_IN_SECONDS));

                    if (_client?.Connected == false)
                    {
                        await ReconnectAsync(token);
                    }
                }
            }

            return result;
        }
    }
}
