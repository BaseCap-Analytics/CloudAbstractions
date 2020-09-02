using System.Collections.Generic;
using System.Text;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v2
{
    public sealed class RedisArray : DataType
    {
        public const char PREFIX = '*';

        public static readonly RedisArray Empty = new RedisArray();

        public static readonly RedisArray Null = new RedisArray(isNull: true);

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue
        {
            get
            {
                if (IsNull)
                {
                    return "-1";
                }
                else
                {
                    StringBuilder sb = new StringBuilder($"{Elements.Count}\r\n");
                    foreach (DataType e in Elements)
                    {
                        sb.Append(e.Package());
                    }

                    return sb.ToString();
                }
            }
        }

        public List<DataType> Elements { get; private set; }

        private bool IsNull { get; set; }

        public RedisArray()
        {
            Elements = new List<DataType>();
            IsNull = false;
        }

        private RedisArray(bool isNull)
        {
            Elements = new List<DataType>();
            IsNull = isNull;
        }

        internal RedisArray(BulkString[] command)
        {
            Elements = new List<DataType>(command);
            IsNull = false;
        }

        public override string ToString() => $"[{string.Join(',', Elements)}]";

        public static string Package(BulkString[] command)
        {
            StringBuilder sb = new StringBuilder($"{command.Length}\r\n");
            foreach (BulkString e in command)
            {
                sb.Append(e.Package());
            }

            return sb.ToString();
        }
    }
}
