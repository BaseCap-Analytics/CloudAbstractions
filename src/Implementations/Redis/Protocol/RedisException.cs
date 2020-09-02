using System;

namespace BaseCap.CloudAbstractions.Redis.Protocol
{
    public class RedisException : Exception
    {
        public string ErrorCode { get; private set; }

        public RedisException(string message) : base(message)
        {
            ErrorCode = string.Empty;
        }

        public RedisException(string message, string errorCode) : base(message)
        {
            ErrorCode = errorCode;
        }
    }
}
