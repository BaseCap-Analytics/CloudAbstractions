using System;
using System.Text;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class VerbatimString : BlobString
    {
        internal new const char PREFIX = '=';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue
        {
            get
            {
                string val = $"{Type}:{Value.ToString()}";
                int byteCount = Encoding.UTF8.GetByteCount(val);
                return $"{byteCount}\r\n{val}";
            }
        }

        public string Type { get; set; }

        public VerbatimString()
        {
            Type = string.Empty;
        }

        internal VerbatimString(string value)
        {
            int typeStart = value.IndexOf(':');
            if (typeStart < 0)
            {
                throw new InvalidOperationException("Verbatim String needs a string type");
            }
            else if (typeStart + 1 < value.Length)
            {
                Value.Append(value.Substring(typeStart + 1));
            }

            Type = value.Substring(0, typeStart);
        }

        public override string ToString() => $"{Type}\t{Value}";

        public static implicit operator string(VerbatimString s) => s.Value.ToString();
    }
}
