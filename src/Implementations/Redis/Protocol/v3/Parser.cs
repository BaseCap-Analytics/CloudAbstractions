using System;
using System.IO.Pipelines;
using System.Numerics;
using System.Text;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    internal sealed class Parser : ParserBase
    {
        internal override int Version => 3;

        public Parser(PipeReader reader)
            : base(reader)
        {
        }

        protected override DataType ParseType(ReadOnlySpan<char> input, ref int ptr)
        {
            char type = Convert.ToChar(input[ptr]);
            return type switch
            {
                BlobString.PREFIX => ParseBlobString(input, ref ptr),
                SimpleString.PREFIX => ParseSimpleString(input, ref ptr),
                SimpleError.PREFIX => ParseSimpleString(input, ref ptr),
                Number.PREFIX => ParseNumber(input, ref ptr),
                Null.PREFIX => ParseNull(input, ref ptr),
                RedisDouble.PREFIX => ParseDouble(input, ref ptr),
                RedisBoolean.PREFIX => ParseBoolean(input, ref ptr),
                BlobError.PREFIX => ParseBlobString(input, ref ptr),
                VerbatimString.PREFIX => ParseBlobString(input, ref ptr),
                BigNumber.PREFIX => ParseBigNumber(input, ref ptr),
                ArrayContainer.PREFIX => ParseContainer(input, ref ptr),
                Set.PREFIX => ParseContainer(input, ref ptr),
                Map.PREFIX => ParseMap(input, ref ptr),
                PushData.PREFIX => ParseContainer(input, ref ptr),
                AttributedDataType.ATTRIBUTE_PREFIX => ParseAttributedValue(input, ref ptr),
                _ => throw new InvalidOperationException("Unknown Redis type prefix"),
            };
        }

        public BlobString ParseBlobString(ReadOnlySpan<char> input, ref int ptr)
        {
            char type = Convert.ToChar(input[ptr]);
            int byteCountStart = ++ptr;
            int? byteCountEnd = null;
            while (ptr < input.Length)
            {
                if (IsCrLf(input, ptr))
                {
                    byteCountEnd = ptr;
                    SkipCrLf(ref ptr);
                    break;
                }
                else
                {
                    ptr++;
                }
            }

            if (byteCountEnd == null)
            {
                throw new InvalidOperationException("No byte count end found for Bulk String");
            }

            ReadOnlySpan<char> byteCountContent = input.Slice(byteCountStart, byteCountEnd.Value - byteCountStart);
            if (int.TryParse(byteCountContent, out int byteCount) == false)
            {
                throw new InvalidOperationException($"Could not convert '{byteCountContent.ToString()}' to a Byte Count");
            }

            if (byteCount == 0)
            {
                if (type == BlobError.PREFIX)
                {
                    throw new InvalidOperationException("Blob Error cannot be empty");
                }
                else if (type == VerbatimString.PREFIX)
                {
                    throw new InvalidOperationException("Verbatim String must have type");
                }
                else
                {
                    return new BlobString(string.Empty);
                }
            }

            int msgStart = ptr;
            int msgEnd = ptr + byteCount;
            if (msgEnd > input.Length)
            {
                throw new InvalidOperationException("No Bulk String End found");
            }
            else
            {
                ptr += byteCount;
                SkipCrLf(ref ptr);
            }

            string str = new string(input.Slice(msgStart, msgEnd - msgStart));
            if (type == BlobError.PREFIX)
            {
                return new BlobError(str);
            }
            else if (type == VerbatimString.PREFIX)
            {
                return new VerbatimString(str);
            }
            else if (type == BlobString.PREFIX)
            {
                return new BlobString(str);
            }
            else
            {
                throw new InvalidOperationException("Unknown Blob prefix");
            }
        }

        public SimpleString ParseSimpleString(ReadOnlySpan<char> input, ref int ptr)
        {
            char type = Convert.ToChar(input[ptr]);
            int start = ++ptr;
            int? end = null;
            while (ptr < input.Length)
            {
                if (IsCrLf(input, ptr))
                {
                    end = ptr;
                    SkipCrLf(ref ptr);
                    break;
                }
                else
                {
                    ptr++;
                }
            }

            if (end == null)
            {
                throw new InvalidOperationException("No end found for Simple String");
            }

            string str = new string(input.Slice(start, end.Value - start));
            if (type == SimpleError.PREFIX)
            {
                return new SimpleError(str);
            }
            else if (type == SimpleString.PREFIX)
            {
                return new SimpleString(str);
            }
            else
            {
                throw new InvalidOperationException("Unknown Simple Prefix");
            }
        }

        private Number ParseNumber(ReadOnlySpan<char> input, ref int ptr)
        {
            int start = ++ptr;
            int? end = null;
            while (ptr < input.Length)
            {
                if (IsCrLf(input, ptr))
                {
                    end = ptr;
                    SkipCrLf(ref ptr);
                    break;
                }
                else
                {
                    ptr++;
                }
            }

            if (end == null)
            {
                throw new InvalidOperationException("No end found for Integer type");
            }

            ReadOnlySpan<char> content = input.Slice(start, end.Value - start);
            if (long.TryParse(content, out long result) == false)
            {
                throw new InvalidOperationException($"Couldn't parse '{content.ToString()}' as Number");
            }

            return new Number(result);
        }

        private Null ParseNull(ReadOnlySpan<char> input, ref int ptr)
        {
            ptr++;
            if (IsCrLf(input, ptr) == false)
            {
                throw new InvalidOperationException("Null must end with CRLF");
            }

            SkipCrLf(ref ptr);
            return new Null();
        }

        private RedisDouble ParseDouble(ReadOnlySpan<char> input, ref int ptr)
        {
            int start = ++ptr;
            int? end = null;
            while (ptr < input.Length)
            {
                if (IsCrLf(input, ptr))
                {
                    end = ptr;
                    SkipCrLf(ref ptr);
                    break;
                }
                else
                {
                    ptr++;
                }
            }

            if (end == null)
            {
                throw new InvalidOperationException("No end found for Double type");
            }

            ReadOnlySpan<char> content = input.Slice(start, end.Value - start);
            if (content.Equals(RedisDouble.INFINITY_STRING, StringComparison.OrdinalIgnoreCase))
            {
                return RedisDouble.INFINITY;
            }
            else if (content.Equals(RedisDouble.NEGATIVE_INFINITY_STRING, StringComparison.OrdinalIgnoreCase))
            {
                return RedisDouble.NEGATIVE_INFINITY;
            }
            else if (decimal.TryParse(content, out decimal result))
            {
                return new RedisDouble(result);
            }
            else
            {
                throw new InvalidOperationException($"Couldn't parse '{content.ToString()}' as a Double");
            }
        }

        private RedisBoolean ParseBoolean(ReadOnlySpan<char> input, ref int ptr)
        {
            ptr++;

            if (ptr > input.Length)
            {
                throw new InvalidOperationException("Boolean value should come after the boolean prefix");
            }

            char flag = Convert.ToChar(input[ptr]);
            ptr++;
            if (IsCrLf(input, ptr) == false)
            {
                throw new InvalidOperationException("Expected CRLF after Boolean");
            }

            SkipCrLf(ref ptr);

            if (flag == RedisBoolean.TRUE)
            {
                return new RedisBoolean(true);
            }
            else if (flag == RedisBoolean.FALSE)
            {
                return new RedisBoolean(false);
            }
            else
            {
                throw new InvalidOperationException($"Valid boolean expressions are '{RedisBoolean.TRUE}' and '{RedisBoolean.FALSE}'");
            }
        }

        private BigNumber ParseBigNumber(ReadOnlySpan<char> input, ref int ptr)
        {
            int start = ++ptr;
            int? end = null;
            while (ptr < input.Length)
            {
                if (IsCrLf(input, ptr))
                {
                    end = ptr;
                    SkipCrLf(ref ptr);
                    break;
                }
                else
                {
                    ptr++;
                }
            }

            if (end == null)
            {
                throw new InvalidOperationException("No end found for Double type");
            }

            ReadOnlySpan<char> number = input.Slice(start, end.Value - start);
            if (BigInteger.TryParse(number, out BigInteger result) == false)
            {
                throw new InvalidOperationException($"Could not convert '{number.ToString()}' to BigInteger");
            }

            return new BigNumber(result);
        }

        public ContainerBase ParseContainer(ReadOnlySpan<char> input, ref int ptr)
        {
            char type = Convert.ToChar(input[ptr]);
            int elementCountStart = ++ptr;
            int? elementCountEnd = null;
            while (ptr < input.Length)
            {
                if (IsCrLf(input, ptr))
                {
                    elementCountEnd = ptr;
                    SkipCrLf(ref ptr);
                    break;
                }
                else
                {
                    ptr++;
                }
            }

            if (elementCountEnd == null)
            {
                throw new InvalidOperationException("No element count end found for Container");
            }

            ReadOnlySpan<char> content = input.Slice(elementCountStart, elementCountEnd.Value - elementCountStart);
            if (int.TryParse(content, out int elementCount) == false)
            {
                throw new InvalidOperationException($"Could not convert '{content.ToString()}' to Element Count");
            }

            if (elementCount < 0)
            {
                throw new InvalidOperationException("Container must have at least 1 element");
            }

            ContainerBase result;
            if (type == ArrayContainer.PREFIX)
            {
                result = new ArrayContainer();
            }
            else if (type == Set.PREFIX)
            {
                result = new Set();
            }
            else if (type == PushData.PREFIX)
            {
                result = new PushData();
            }
            else
            {
                throw new InvalidOperationException("Unknown Container type");
            }

            int count = 0;
            bool firstElement = type == PushData.PREFIX;
            for (int i = 0; (i < elementCount) && (ptr < input.Length); i++)
            {
                DataType dt = ParseType(input, ref ptr);
                if (firstElement && (type == PushData.PREFIX))
                {
                    if (dt.GetType() != typeof(BlobString))
                    {
                        throw new InvalidOperationException("Push Data's first element must be a Blob String");
                    }

                    ((PushData)result).Kind = (BlobString)dt;
                    firstElement = false;
                    count++;
                }
                else
                {
                    result.Add(dt);
                    count++;
                }
            }

            if (count != elementCount)
            {
                throw new InvalidOperationException("Container count mismatch");
            }

            return result;
        }

        public Map ParseMap(ReadOnlySpan<char> input, ref int ptr)
        {
            int elementCountStart = ++ptr;
            int? elementCountEnd = null;
            while (ptr < input.Length)
            {
                if (IsCrLf(input, ptr))
                {
                    elementCountEnd = ptr;
                    SkipCrLf(ref ptr);
                    break;
                }
                else
                {
                    ptr++;
                }
            }

            if (elementCountEnd == null)
            {
                throw new InvalidOperationException("No element count end found for Map");
            }

            ReadOnlySpan<char> content = input.Slice(elementCountStart, elementCountEnd.Value - elementCountStart);
            if (int.TryParse(content, out int elementCount) == false)
            {
                throw new InvalidOperationException($"Could not convert '{content.ToString()}' to Element Count");
            }

            if (elementCount <= 0)
            {
                throw new InvalidOperationException("Array Container must have at least 1 element");
            }

            Map result = new Map();
            for (int i = 0; (i < elementCount) && (ptr < input.Length); i++)
            {
                DataType key = ParseType(input, ref ptr);
                DataType value = ParseType(input, ref ptr);
                result.Elements.Add(key, value);
            }

            if (result.Elements.Count != elementCount)
            {
                throw new InvalidOperationException("Map count mismatch");
            }

            return result;
        }

        public AttributedDataType ParseAttributedValue(ReadOnlySpan<char> input, ref int ptr)
        {
            int elementCountStart = ++ptr;
            int? elementCountEnd = null;
            while (ptr < input.Length)
            {
                if (IsCrLf(input, ptr))
                {
                    elementCountEnd = ptr;
                    SkipCrLf(ref ptr);
                    break;
                }
                else
                {
                    ptr++;
                }
            }

            if (elementCountEnd == null)
            {
                throw new InvalidOperationException("No element count end found for Map");
            }

            ReadOnlySpan<char> content = input.Slice(elementCountStart, elementCountEnd.Value - elementCountStart);
            if (int.TryParse(content, out int elementCount) == false)
            {
                throw new InvalidOperationException($"Could not convert '{content.ToString()}' to Element Count");
            }

            if (elementCount <= 0)
            {
                throw new InvalidOperationException("Array Container must have at least 1 element");
            }

            Map result = new Map();
            for (int i = 0; (i < elementCount) && (ptr < input.Length); i++)
            {
                DataType key = ParseType(input, ref ptr);
                DataType value = ParseType(input, ref ptr);
                result.Elements.Add(key, value);
            }

            if (result.Elements.Count != elementCount)
            {
                throw new InvalidOperationException("Map count mismatch");
            }
            else if (ptr >= input.Length)
            {
                throw new InvalidOperationException("Attribute must preceed another type");
            }

            AttributedDataType dt = (AttributedDataType)ParseType(input, ref ptr);
            dt.Attributes = result;

            return dt;
        }
    }
}
