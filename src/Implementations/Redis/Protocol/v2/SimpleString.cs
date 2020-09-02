using System;

namespace BaseCap.CloudAbstractions.Redis.Protocol.v2
{
    public sealed class SimpleString : DataType
    {
        internal const char PREFIX = '+';

        private string _value;

        /// <inheritdoc />
        protected override char Prefix => PREFIX;

        /// <inheritdoc />
        protected override string PackagedValue => Value;

        /// <inheritdoc />
        public string Value
        {
            get => _value;
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    throw new ArgumentNullException(nameof(value));
                }
                else if (value.Contains('\r') || value.Contains('\n'))
                {
                    throw new ArgumentException("Simple Strings may not contain CR or LF");
                }

                _value = value;
            }
        }

        public SimpleString()
        {
            _value = string.Empty;
        }

        internal SimpleString(string value)
        {
            _value = string.Empty;
            Value = value.ToString();
        }

        public override string ToString() => Value;

        public static implicit operator string(SimpleString s) => s.Value;
    }
}
