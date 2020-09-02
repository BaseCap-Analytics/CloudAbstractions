using System.Collections.Generic;
using System.Text;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class PushData : ContainerBase
    {
        public const char PREFIX = '>';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue
        {
            get
            {
                StringBuilder sb = new StringBuilder($"{Elements.Count}\r\n{Kind.Package()}\r\n");
                foreach (DataType e in Elements)
                {
                    sb.Append(e.Package());
                }

                return sb.ToString();
            }
        }

        public BlobString Kind { get; internal set; }

        public PushData()
        {
            Kind = new BlobString();
            Elements = new List<DataType>();
        }

        public override string ToString() => $"{Kind}\t[{string.Join(',', Elements)}]";
    }
}
