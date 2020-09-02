namespace BaseCap.CloudAbstractions.Redis.Protocol.v2
{
    public sealed class RedisError : DataType
    {
        internal const char PREFIX = '-';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue => Value;

        /// <inheritdoc />
        public string Value { get; set; }

        public string ErrorType { get; set; }

        public RedisError()
        {
            Value = string.Empty;
            ErrorType = string.Empty;
        }

        internal RedisError(string value, string type)
        {
            Value = value;
            ErrorType = type;
        }

        public override string ToString() => $"{ErrorType} {Value}";
    }
}
