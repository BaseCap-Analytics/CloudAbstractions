using System.Text;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v2
{
    public sealed class BulkString : DataType
    {
        public static readonly BulkString Empty = new BulkString();
        internal const char PREFIX = '$';

        private string _value;

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue
        {
            get
            {
                // In Redis, the -1 byte count means null string
                if (IsNull)
                {
                    return "-1";
                }
                else
                {
                    int byteCount = Encoding.UTF8.GetByteCount(Value);
                    return $"{byteCount}\r\n{Value}";
                }
            }
        }

        /// <inheritdoc />
        public string Value
        {
            get => _value;
            set
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    _value = string.Empty;
                    IsNull = true;
                }
                else
                {
                    _value = value;
                    IsNull = false;
                }
            }
        }

        internal bool IsNull { get; set; }

        public BulkString()
        {
            _value = string.Empty;
            IsNull = true;
        }

        internal BulkString(string value)
        {
            _value = value;
            IsNull = string.IsNullOrEmpty(value);
        }

        public override string ToString() => Value;

        public static implicit operator string(BulkString s) => s.Value;
    }
}
