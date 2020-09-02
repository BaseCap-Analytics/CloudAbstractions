using System.Collections.Generic;
using System.Text;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class Set : ContainerBase
    {
        public const char PREFIX = '~';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue
        {
            get
            {
                StringBuilder sb = new StringBuilder($"{Elements.Count}\r\n");
                foreach (DataType e in Elements)
                {
                    sb.Append(e.Package());
                }

                return sb.ToString();
            }
        }

        public Set()
        {
            Elements = new HashSet<DataType>();
        }

        public override string ToString() => $"[{string.Join(',', Elements)}]";
    }
}
