using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;

namespace RabbitMQClient
{
    static class ReadableBufferExtensions
    {
        //TODO need to actually parse the table, not just return a byte array
        public static (byte[] result, ReadCursor position) ReadTable(this ReadableBuffer buffer)
        {
            var tableLength = buffer.ReadBigEndian<uint>();
            var table = buffer.Slice(sizeof(uint), (int)tableLength);

            return (table.ToArray(), table.End);
        }

        public static (string result, ReadCursor position) ReadLongString(this ReadableBuffer buffer)
        {
            var length = buffer.ReadBigEndian<uint>();
            var bytes = buffer.Slice(sizeof(uint), (int)length);

            return (Encoding.UTF8.GetString(bytes.ToArray()), bytes.End);
        }
    }
}
