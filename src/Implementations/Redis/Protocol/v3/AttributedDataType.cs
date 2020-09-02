namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public abstract class AttributedDataType : DataType
    {
        internal const char ATTRIBUTE_PREFIX = '|';

        public Map? Attributes { get; internal set; }
    }
}
