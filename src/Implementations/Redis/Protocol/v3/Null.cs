namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class Null : AttributedDataType
    {
        public const char PREFIX = '_';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue => "_";

        public override string ToString() => "{null}";
    }
}
