using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;

namespace RabbitMQClient
{
    static class WritableBufferExtensions
    {
        public static void WriteShortString(this WritableBuffer buffer, string value)
        {
            var valueBytes = Encoding.UTF8.GetBytes(value);

            if (valueBytes.Length > byte.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "value is too long for a short string");
            }

            buffer.WriteBigEndian((byte)valueBytes.Length);
            buffer.Write(valueBytes);
        }
    }
}
