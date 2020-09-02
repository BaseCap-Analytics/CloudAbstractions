namespace BaseCap.CloudAbstractions.Redis.Protocol
{
    public abstract class DataType
    {
        protected abstract char Prefix { get; }

        protected abstract string PackagedValue { get; }

        public string Package() => $"{Prefix}{PackagedValue}\r\n";
    }
}
