using System.Numerics;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class BigNumber : AttributedDataType
    {
        public const char PREFIX = '(';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue => Value.ToString();

        /// <inheritdoc />
        public BigInteger Value { get; set; }

        public BigNumber()
        {
            Value = 0;
        }

        internal BigNumber(BigInteger value)
        {
            Value = value;
        }

        public override string ToString() => Value.ToString();

        public static implicit operator BigInteger(BigNumber n) => n.Value;
    }
}
