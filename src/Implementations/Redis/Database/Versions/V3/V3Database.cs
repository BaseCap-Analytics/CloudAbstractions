using BaseCap.CloudAbstractions.Redis.Cluster;
using BaseCap.CloudAbstractions.Redis.Protocol;
using BaseCap.CloudAbstractions.Redis.Protocol.v3;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;

namespace BaseCap.CloudAbstractions.Redis.Database.Versions.V3
{
    internal class V3Database : RedisDatabase
    {
        protected override int ProtocolVersion => 3;

        internal V3Database(ConnectionBase connection, PipeReader pipe)
            : base(connection, new Parser(pipe))
        {
        }

        protected override string PackageCommand(string commandName, string keyName, params string[] args)
        {
            BlobString[] cmd = new BlobString[args.Length + 2];
            cmd[0] = new BlobString(commandName);
            cmd[1] = new BlobString(keyName);
            for (int i = 0; i < args.Length; i++)
            {
                cmd[i + 2] = new BlobString(args[i]);
            }

            return ArrayContainer.Package(cmd);
        }

        protected override string PackageCommand(string commandName, params string[] args)
        {
            BlobString[] cmd = new BlobString[args.Length + 1];
            cmd[0] = new BlobString(commandName);
            for (int i = 0; i < args.Length; i++)
            {
                cmd[i + 1] = new BlobString(args[i]);
            }

            return ArrayContainer.Package(cmd);
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
                case SimpleError se: throw new RedisException(se.Value, se.ErrorType);
                case BlobError be: throw new RedisException(be.Value.ToString(), be.ErrorType);
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
                case SimpleError se: throw new RedisException(se.Value, se.ErrorType);
                case BlobError be: throw new RedisException(be.Value.ToString(), be.ErrorType);
                case Number num: return num.Value;
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
                case SimpleError se: throw new RedisException(se.Value, se.ErrorType);
                case BlobError be: throw new RedisException(be.Value.ToString(), be.ErrorType);
                case SimpleString ss: return ss.Value;
                case VerbatimString vs: return vs.ToString();
                case BlobString bs: return bs.ToString();
                default: throw new RedisException($"Unexpected Result Type: {e.GetType().Name}");
            }
        }

        protected override object? ParseScalarResponse(List<DataType> received)
        {
            if (received.Count != 1)
            {
                throw new RedisException("Response should be 1 element long");
            }

            DataType e = received[0];
            object? result = GetObjectFromDataType(e);
            if ((result is List<object?>) || (result is Dictionary<object, object?>))
            {
                throw new RedisException($"Expected Scalar value; instead found {result?.GetType()}");
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
                case SimpleError se: throw new RedisException(se.Value, se.ErrorType);
                case BlobError be: throw new RedisException(be.Value.ToString(), be.ErrorType);
                case Map m:
                    {
                        Dictionary<string, string> result = new Dictionary<string, string>();
                        foreach ((DataType key, DataType val) in m.Elements)
                        {
                            result.Add(key.ToString()!, val.ToString()!);
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
                if ((dt is PushData pd) && (string.Equals(pd.Kind, PUBSUB_RESPONSE_KIND, StringComparison.OrdinalIgnoreCase)))
                {
                    DataType channel = pd.Elements.First();
                    DataType content = pd.Elements.Last();
                    return GetObjectFromDataType(content);
                }
            }

            throw new RedisException("No valid PubSub response found");
        }

        private object? GetObjectFromDataType(DataType dt)
        {
            switch (dt)
            {
                case ArrayContainer ac:
                    {
                        List<object?> objs = new List<object?>();
                        foreach (DataType d in ac.Elements)
                        {
                            objs.Add(GetObjectFromDataType(d));
                        }

                        return objs;
                    }
                case BigNumber bn: return bn.Value;
                case BlobError be: return be.Value.ToString();
                case VerbatimString vs: return vs.Value.ToString();
                case BlobString bs: return bs.Value.ToString();
                case Map mp:
                    {
                        Dictionary<object, object?> objs = new Dictionary<object, object?>();
                        foreach (KeyValuePair<DataType, DataType> kv in mp.Elements)
                        {
                            object? key = GetObjectFromDataType(kv.Key);
                            if (key == null)
                            {
                                throw new RedisException("Found invalid, null key in Map");
                            }

                            object? val = GetObjectFromDataType(kv.Value);
                            objs.Add(key, val);
                        }

                        return objs;
                    }
                case Null nl: return null;
                case Number num: return num.Value;
                case RedisBoolean rb: return rb.Value;
                case RedisDouble db: return db.Value;
                case Set st:
                    {
                        List<object?> objs = new List<object?>();
                        foreach (DataType d in st.Elements)
                        {
                            objs.Add(GetObjectFromDataType(d));
                        }

                        return objs;
                    }
                case SimpleError se: return se.Value;
                case SimpleString ss: return ss.Value;
                default:
                    throw new RedisException($"Unknown Redis V3 type: {dt.GetType()}");
            }
        }
    }
}
