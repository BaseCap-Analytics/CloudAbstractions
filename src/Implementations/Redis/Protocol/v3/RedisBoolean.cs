namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class RedisBoolean : AttributedDataType
    {
        internal const char TRUE = 't';
        internal const char FALSE = 'f';

        public const char PREFIX = '#';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue => Value ? TRUE.ToString() : FALSE.ToString();

        /// <inheritdoc />
        public bool Value { get; set; }

        public RedisBoolean()
        {
            Value = false;
        }

        internal RedisBoolean(bool value)
        {
            Value = value;
        }

        public override string ToString() => Value.ToString();

        public static implicit operator bool(RedisBoolean b) => b.Value;
    }
}
