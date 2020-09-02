using System;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v3
{
    public sealed class SimpleError : SimpleString
    {
        internal new const char PREFIX = '-';

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue => Value;

        public string ErrorType { get; set; }

        public SimpleError()
        {
            ErrorType = string.Empty;
        }

        internal SimpleError(string value)
        {
            int errorCode = value.IndexOf(' ');
            if (errorCode < 0)
            {
                throw new InvalidOperationException("Simple Error must have error code");
            }
            else if (errorCode + 1 > value.Length)
            {
                throw new InvalidOperationException("Simple Error must have error code and message");
            }

            ErrorType = value.Substring(0, errorCode);
            Value = value.Substring(errorCode + 1);
        }

        public override string ToString() => $"{ErrorType} {Value}";
    }
}
