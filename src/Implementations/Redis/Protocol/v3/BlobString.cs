using System.Text;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public class BlobString : AttributedDataType
    {
        internal const char PREFIX = '$';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue
        {
            get
            {
                string val = Value.ToString();
                int byteCount = Encoding.UTF8.GetByteCount(val);
                return $"{byteCount}\r\n{val}";
            }
        }

        /// <inheritdoc />
        public StringBuilder Value { get; set; }

        public BlobString()
        {
            Value = new StringBuilder();
        }

        internal BlobString(string value)
        {
            Value = new StringBuilder(value);
        }

        public override string ToString() => Value.ToString();

        public static implicit operator string(BlobString s) => s.Value.ToString();
    }
}
