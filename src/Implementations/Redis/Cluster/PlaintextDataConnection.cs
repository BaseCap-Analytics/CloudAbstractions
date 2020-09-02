using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Cluster
{
    internal class PlaintextDataConnection : ConnectionBase
    {
        internal PlaintextDataConnection(string hostname, IPAddress host, ushort port, bool useSsl, PipeWriter pipe) : base(hostname, host, port, useSsl, pipe)
        {
        }

        protected virtual ValueTask<Memory<byte>> FillSendBufferAsync(string commandPackage, IMemoryOwner<byte> bufferOwner)
        {
            int count = Encoding.UTF8.GetBytes(commandPackage, bufferOwner.Memory.Span);
            return new ValueTask<Memory<byte>>(bufferOwner.Memory.Slice(0, count));
        }

        protected virtual IMemoryOwner<byte> GetBuffer(int packagebytes) => MemoryPool<byte>.Shared.Rent(packagebytes);

        protected override async ValueTask InternalSendCommandAsync(string commandPackage, CancellationToken token)
        {
            int byteCount = Encoding.UTF8.GetByteCount(commandPackage);
            using (IMemoryOwner<byte> buffer = GetBuffer(byteCount))
            {
                Memory<byte> sendBuffer = await FillSendBufferAsync(commandPackage, buffer);
                await ExecuteTaskWithRetryAsync(_stream!.WriteAsync(sendBuffer, token), token);
            }
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
