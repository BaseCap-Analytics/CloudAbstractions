namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class RedisDouble : AttributedDataType
    {
        internal static readonly string INFINITY_STRING = "inf";
        internal static readonly string NEGATIVE_INFINITY_STRING = "-inf";

        public static readonly RedisDouble INFINITY = new RedisDouble(positiveInfinity: true);
        public static readonly RedisDouble NEGATIVE_INFINITY = new RedisDouble(positiveInfinity: false);

        public const char PREFIX = ',';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue
        {
            get
            {
                string ret;
                if (_isInfinity && _isPositiveInfinity)
                {
                    ret = INFINITY_STRING;
                }
                else if (_isInfinity && (_isPositiveInfinity == false))
                {
                    ret = NEGATIVE_INFINITY_STRING;
                }
                else
                {
                    ret = Value.ToString();
                }

                return ret;
            }
        }

        /// <inheritdoc />
        public decimal Value { get; set; }

        private bool _isInfinity = false;
        private bool _isPositiveInfinity = false;

        public RedisDouble()
        {
            Value = 0;
        }

        internal RedisDouble(decimal value)
        {
            Value = value;
        }

        private RedisDouble(bool positiveInfinity)
        {
            _isInfinity = true;
            _isPositiveInfinity = positiveInfinity;
        }

        public override string ToString() => _isInfinity && _isPositiveInfinity ? INFINITY_STRING :
                                                _isInfinity && _isInfinity == false ? NEGATIVE_INFINITY_STRING :
                                                    Value.ToString();

        public static implicit operator decimal(RedisDouble d) => d._isInfinity ?
                                                                d._isPositiveInfinity ? decimal.MaxValue : decimal.MinValue
                                                                : d.Value;
    }
}
