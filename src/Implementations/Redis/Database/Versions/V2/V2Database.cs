using BaseCap.CloudAbstractions.Redis.Cluster;
using BaseCap.CloudAbstractions.Redis.Protocol;
using BaseCap.CloudAbstractions.Redis.Protocol.v2;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;

namespace BaseCap.CloudAbstractions.Redis.Database.Versions.V2
{
    internal class V2Database : RedisDatabase
    {
        protected override int ProtocolVersion => 2;

        internal V2Database(ConnectionBase connection, PipeReader pipe)
            : base(connection, new Parser(pipe))
        {
        }

        protected override string PackageCommand(string commandName, string keyName, params string[] args)
        {
            BulkString[] cmd = new BulkString[args.Length + 2];
            cmd[0] = new BulkString(commandName);
            cmd[1] = new BulkString(keyName);
            for (int i = 0; i < args.Length; i++)
            {
                cmd[i + 2] = new BulkString(args[i]);
            }

            return RedisArray.Package(cmd);
        }

        protected override string PackageCommand(string commandName, params string[] args)
        {
            BulkString[] cmd = new BulkString[args.Length + 1];
            cmd[0] = new BulkString(commandName);
            for (int i = 0; i < args.Length; i++)
            {
                cmd[i + 1] = new BulkString(args[i]);
            }

            return RedisArray.Package(cmd);
        }

        protected override bool ParseOkResult(List<DataType> received)
        {
            if (received.Count != 1)
            {
                throw new RedisException("Response should be 1 element long");
            }

            DataType e = received[0];
            switch (e)
            {
                case RedisError se: throw new RedisException(se.Value, se.ErrorType);
                case SimpleString ss: return string.Equals(ss.Value, "OK", StringComparison.OrdinalIgnoreCase);
                default: throw new RedisException($"Unexpected Result Type: {e.GetType().Name}");
            }
        }

        protected override long ParseIntegerResponse(List<DataType> received)
        {
            if (received.Count != 1)
            {
                throw new RedisException("Response should be 1 element long");
            }

            DataType e = received[0];
            switch (e)
            {
                case RedisError se: throw new RedisException(se.Value, se.ErrorType);
                case RedisInteger ri: return Convert.ToInt64(ri.Value);
                default: throw new RedisException($"Unexpected Result Type: {e.GetType().Name}");
            }
        }

        protected override string? ParseStringResponse(List<DataType> received)
        {
            if (received.Count != 1)
            {
                throw new RedisException("Response should be 1 element long");
            }

            DataType e = received[0];
            switch (e)
            {
                case RedisError se: throw new RedisException(se.Value, se.ErrorType);
                case SimpleString ss: return ss.Value;
                case BulkString bs: return bs.IsNull ? null : bs.Value;
                default: throw new RedisException($"Unexpected Result Type: {e.GetType().Name}");
            }
        }

        protected override object? ParseScalarResponse(List<DataType> received)
        {
            if (received.Count != 1)
            {
                throw new RedisException("Response should be 1 element long");
            }

            object? result = GetObjectFromDataType(received[0]);
            if (result is List<object?>)
            {
                throw new RedisException("Expected Scalar result; instead got a collection");
            }

            return result;
        }

        protected override Dictionary<string, string> ParseDictionaryResponse(List<DataType> received)
        {
            if (received.Count != 1)
            {
                throw new RedisException("Response should be 1 element long");
            }

            DataType e = received[0];
            switch (e)
            {
                case RedisError se: throw new RedisException(se.Value, se.ErrorType);
                case RedisArray ra:
                    {
                        DataType? key = null;
                        Dictionary<string, string> result = new Dictionary<string, string>();
                        foreach (DataType dt in ra.Elements)
                        {
                            if (key == null)
                            {
                                key = dt;
                            }
                            else
                            {
                                result.Add(key.ToString()!, dt.ToString()!);
                                key = null;
                            }
                        }

                        return result;
                    }
                default: throw new RedisException($"Unexpected Result Type: {e.GetType().Name}");
            }
        }

        protected override object? GetPubSubMessageContents(List<DataType> received)
        {
            foreach (DataType dt in received)
            {
                if ((dt is RedisArray arr) &&
                    (arr.Elements.Count > 0) &&
                    (string.Equals(arr.Elements[0].ToString(), PUBSUB_RESPONSE_KIND, StringComparison.OrdinalIgnoreCase)))
                {
                    DataType channel = arr.Elements[1];
                    DataType content = arr.Elements[2];
                    return GetObjectFromDataType(content);
                }
            }

            throw new RedisException("No valid PubSub response found");
        }

        private object? GetObjectFromDataType(DataType dt)
        {
            switch (dt)
            {
                case BulkString bs: return bs.Value;
                case RedisArray ra:
                    List<object?> objs = new List<object?>();
                    foreach (DataType d in ra.Elements)
                    {
                        objs.Add(GetObjectFromDataType(d));
                    }
                    return objs;
                case RedisError re: return re.Value;
                case RedisInteger ri: return ri.Value;
                case SimpleString ss: return ss.Value;
                default:
                    throw new RedisException($"Unknown Redis V2 type: {dt.GetType()}");
            }
        }
    }
}
