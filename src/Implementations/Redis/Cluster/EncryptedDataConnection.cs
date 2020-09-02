using BaseCap.Security;
using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Cluster
{
    internal sealed class EncryptedDataConnection : PlaintextDataConnection
    {
        private readonly byte[] _encryptionKey;

        internal EncryptedDataConnection(string hostname, IPAddress host, ushort port, bool useSsl, PipeWriter pipe, byte[] encryptionKey) :
            base(hostname, host, port, useSsl, pipe)
        {
            _encryptionKey = encryptionKey;
        }

        protected override IMemoryOwner<byte> GetBuffer(int packagebytes)
        {
            // The encrypted data will take up (n + 8) - (n % 8) bytes plus the IV size
            int encryptedBytes = (packagebytes + 8 - (packagebytes % 8)) + 16;
            return MemoryPool<byte>.Shared.Rent(encryptedBytes);
        }

        protected override ValueTask<Memory<byte>> FillSendBufferAsync(string commandPackage, IMemoryOwner<byte> sendBuffer)
        {
            byte[] package = Encoding.UTF8.GetBytes(commandPackage);
            int encryptedCount = EncryptionHelpers.EncryptData(package.AsMemory(), sendBuffer.Memory, _encryptionKey);
            return new ValueTask<Memory<byte>>(sendBuffer.Memory.Slice(0, encryptedCount));
        }

        protected override async ValueTask<int> ReadAvailableDataAsync(CancellationToken token)
        {
            CheckForOpenStream();

            Memory<byte> buffer = _dataPipe.GetMemory(MINIMUM_BUFFER_SIZE_BYTES);
            int totalRead = 0;
            int bytesRead = 0;

            do
            {
                bytesRead = await ExecuteTaskWithRetryAndResultAsync(_stream!.ReadAsync(buffer, token), token);
                totalRead += bytesRead;
                await _dataPipe.WriteAsync(buffer.Slice(0, bytesRead), token);
            }
            while (bytesRead >= buffer.Length);

            _dataPipe.Advance(totalRead);
            return totalRead;
        }
    }
}
