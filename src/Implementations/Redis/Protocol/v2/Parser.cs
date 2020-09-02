using System;
using System.IO.Pipelines;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v2
{
    internal sealed class Parser : ParserBase
    {
        internal override int Version => 2;

        public Parser(PipeReader reader)
            : base(reader)
        {
        }

        protected override DataType ParseType(ReadOnlySpan<char> input, ref int ptr)
        {
            char type = Convert.ToChar(input[ptr]);
            ptr++;
            return type switch
            {
                RedisArray.PREFIX => ParseArray(input, ref ptr),
                BulkString.PREFIX => ParseBulkString(input, ref ptr),
                RedisError.PREFIX => ParseError(input, ref ptr),
                RedisInteger.PREFIX => ParseInteger(input, ref ptr),
                SimpleString.PREFIX => ParseSimpleString(input, ref ptr),
                _ => throw new InvalidOperationException("Unknown Redis type prefix"),
            };
        }

        private DataType ParseInteger(ReadOnlySpan<char> input, ref int ptr)
        {
            int start = ptr;
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
            if (double.TryParse(content, out double result) == false)
            {
                throw new InvalidOperationException($"Could not convert '{content.ToString()}' to Double");
            }

            return new RedisInteger(result);
        }

        public RedisError ParseError(ReadOnlySpan<char> input, ref int ptr)
        {
            int typeStart = ptr;
            int? typeEnd = null;
            int? msgStart = null;
            int? msgEnd = null;
            bool lookingForType = true;
            while (ptr < input.Length)
            {
                bool bIsCrlf = IsCrLf(input, ptr);
                bool isSpace = input[ptr] == ' ';
                if (lookingForType && (bIsCrlf || isSpace))
                {
                    typeEnd = ptr;
                    lookingForType = false;
                    if (bIsCrlf)
                    {
                        SkipCrLf(ref ptr);
                    }
                    else
                    {
                        ptr++;
                    }
                    msgStart = ptr;
                }
                else if ((lookingForType == false) && bIsCrlf)
                {
                    msgEnd = ptr;
                    SkipCrLf(ref ptr);
                    break;
                }
                else
                {
                    ptr++;
                }
            }

            // Check if we have a msgEnd, which would mean we have every field
            if ((typeEnd == null) || (msgStart == null) || (msgEnd == null))
            {
                throw new InvalidOperationException("Not a valid Error type");
            }

            // Get the message and type
            string type = new string(input.Slice(typeStart, typeEnd.Value - typeStart));
            string message = new string(input.Slice(msgStart.Value, msgEnd.Value - msgStart.Value));
            return new RedisError(message, type);
        }

        public SimpleString ParseSimpleString(ReadOnlySpan<char> input, ref int ptr)
        {
            int start = ptr;
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
            return new SimpleString(str);
        }

        public BulkString ParseBulkString(ReadOnlySpan<char> input, ref int ptr)
        {
            int byteCountStart = ptr;
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

            ReadOnlySpan<char> content = input.Slice(byteCountStart, byteCountEnd.Value - byteCountStart);
            if (int.TryParse(content, out int byteCount) == false)
            {
                throw new InvalidOperationException($"Could not convert '{content.ToString()}' to Byte Count");
            }

            if (byteCount == -1)
            {
                return BulkString.Empty;
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
            return new BulkString(str);
        }

        public RedisArray ParseArray(ReadOnlySpan<char> input, ref int ptr)
        {
            int elementCountStart = ptr;
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
                throw new InvalidOperationException("No element count end found for Array");
            }

            ReadOnlySpan<char> content = input.Slice(elementCountStart, elementCountEnd.Value - elementCountStart);
            if (int.TryParse(content, out int elementCount) == false)
            {
                throw new InvalidOperationException($"Could not convert '{content.ToString()}' to Element Count");
            }

            if (elementCount == 0)
            {
                return RedisArray.Empty;
            }
            else if (elementCount == -1)
            {
                return RedisArray.Null;
            }

            RedisArray result = new RedisArray();
            for (int i = 0; (i < elementCount) && (ptr < input.Length); i++)
            {
                DataType dt = ParseType(input, ref ptr);
                result.Elements.Add(dt);
            }

            if (result.Elements.Count != elementCount)
            {
                throw new InvalidOperationException("Array count mismatch");
            }

            return result;
        }
    }
}
