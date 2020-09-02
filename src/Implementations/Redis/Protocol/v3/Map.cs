using System.Collections.Generic;
using System.Text;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class Map : AttributedDataType
    {
        public const char PREFIX = '%';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue
        {
            get
            {
                StringBuilder sb = new StringBuilder($"{Elements.Count}\r\n");
                foreach ((DataType key, DataType value) in Elements)
                {
                    sb.Append(key.Package()).Append(value.Package());
                }

                return sb.ToString();
            }
        }

        public Dictionary<DataType, DataType> Elements { get; private set; }

        public Map()
        {
            Elements = new Dictionary<DataType, DataType>();
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            foreach ((DataType key, DataType val) in Elements)
            {
                sb.Append($"[{key}, {val}]");
            }

            return $"[{sb}]";
        }
    }
}
