namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class Number : AttributedDataType
    {
        public const char PREFIX = ':';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue => Value.ToString();

        /// <inheritdoc />
        public long Value { get; set; }

        public Number()
        {
            Value = 0;
        }

        internal Number(long value)
        {
            Value = value;
        }

        public override string ToString() => Value.ToString();

        public static implicit operator long(Number n) => n.Value;
    }
}
