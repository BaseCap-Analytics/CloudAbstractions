using System;
using System.Text;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class BlobError : BlobString
    {
        internal new const char PREFIX = '!';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue
        {
            get
            {
                string val = $"{ErrorType.ToUpperInvariant()}\r\n{Value.ToString()}";
                int byteCount = Encoding.UTF8.GetByteCount(val);
                return $"{byteCount}\r\n{val}";
            }
        }

        public string ErrorType { get; set; }

        public BlobError() : base()
        {
            ErrorType = string.Empty;
        }

        internal BlobError(string value) : base()
        {
            int errorCode = value.IndexOf(' ');
            if (errorCode < 0)
            {
                throw new InvalidOperationException("Blob Error must have error code");
            }
            else if (errorCode + 1 > value.Length)
            {
                throw new InvalidOperationException("Blob Error must have error code and message");
            }

            ErrorType = value.Substring(0, errorCode);
            Value.Append(value.Substring(errorCode + 1));
        }

        public override string ToString() => $"{ErrorType} {Value}";
    }
}
