using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BaseCap.CloudAbstractions.Redis.Protocol
{
    internal abstract class ParserBase
    {
        protected static readonly byte[] CRLF = { 13, 10 };
        internal abstract int Version { get; }

        private readonly PipeReader _reader;

        internal ParserBase(PipeReader reader)
        {
            _reader = reader;
        }

        internal async Task<List<DataType>> ParseAsync(int count, CancellationToken token)
        {
            List<DataType> elements;
            ReadResult result = await _reader.ReadAsync(token);
            try
            {
                if (count > result.Buffer.Length)
                {
                    throw new InvalidOperationException("Received more bytes than given");
                }

                StringBuilder converted = GetResponse(result);
                elements = ParseInternal(converted);
            }
            finally
            {
                _reader.AdvanceTo(result.Buffer.End);
            }

            return elements;
        }

        private StringBuilder GetResponse(ReadResult result)
        {
            SequenceReader<byte> reader = new SequenceReader<byte>(result.Buffer);
            reader.AdvancePast(0);

            StringBuilder sb = new StringBuilder();
            while (reader.Remaining > 0)
            {
                ReadOnlySpan<byte> unread = reader.UnreadSpan;
                sb.Append(Encoding.UTF8.GetString(unread).Trim('\0'));
                reader.Advance(unread.Length);
            }

            return sb;
        }

        private List<DataType> ParseInternal(StringBuilder converted)
        {
            List<DataType> elements = new List<DataType>();
            ReadOnlySpan<char> input = converted.ToString().AsSpan();

            int ptr = 0;
            while (ptr < converted.Length)
            {
                elements.Add(ParseType(input, ref ptr));
            }

            return elements;
        }

        protected abstract DataType ParseType(ReadOnlySpan<char> input, ref int ptr);
        protected bool IsCrLf(ReadOnlySpan<char> input, int ptr)
        {
            if ((ptr > input.Length) || (ptr + 1 > input.Length))
            {
                throw new InvalidOperationException("Expected CRLF, instead found EOF");
            }
            else
            {
                return ((input[ptr] == '\r') && (input[ptr + 1] == '\n'));
            }
        }

        protected void SkipCrLf(ref int ptr) => ptr += 2;
    }
}
