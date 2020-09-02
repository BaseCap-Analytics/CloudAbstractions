namespace BaseCap.CloudAbstractions.Redis.Protocol.v2
{
    public sealed class RedisInteger : DataType
    {
        public const char PREFIX = ':';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue => Value.ToString();

        /// <inheritdoc />
        public double Value { get; set ; }

        public RedisInteger()
        {
            Value = 0;
        }

        internal RedisInteger(double value)
        {
            Value = value;
        }

        public override string ToString() => Value.ToString();

        public static implicit operator double(RedisInteger s) => s.Value;
    }
}
