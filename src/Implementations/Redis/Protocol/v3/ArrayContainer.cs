using System.Collections.Generic;
using System.Text;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class ArrayContainer : ContainerBase
    {
        public const char PREFIX = '*';

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

        public ArrayContainer()
        {
            Elements = new List<DataType>();
        }

        internal ArrayContainer(BlobString[] command)
        {
            Elements = new List<DataType>(command);
        }

        public override string ToString() => $"[{string.Join(',', Elements)}]";

        public static string Package(BlobString[] command)
        {
            StringBuilder sb = new StringBuilder($"{command.Length}\r\n");
            foreach (BlobString c in command)
            {
                sb.Append(c.Package());
            }

            return sb.ToString();
        }
    }
}
