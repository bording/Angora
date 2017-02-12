using System;
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

        public static void WriteLongString(this WritableBuffer buffer, string value)
        {
            var valueBytes = Encoding.UTF8.GetBytes(value);

            buffer.WriteBigEndian((uint)valueBytes.Length);
            buffer.Write(valueBytes);
        }

        //TODO stub for now. Needs to accept an actual table and write it out
        public static void WriteTable(this WritableBuffer buffer, byte[] value)
        {
            buffer.WriteBigEndian((uint)value.Length);
            buffer.Write(value);
        }

        public static void WriteBits(this WritableBuffer buffer, bool bit0 = false, bool bit1 = false, bool bit2 = false, bool bit3 = false, bool bit4 = false, bool bit5 = false, bool bit6 = false, bool bit7 = false)
        {
            byte bits = 0;

            bits |= (byte)(Convert.ToInt32(bit0) << 0);
            bits |= (byte)(Convert.ToInt32(bit1) << 1);
            bits |= (byte)(Convert.ToInt32(bit2) << 2);
            bits |= (byte)(Convert.ToInt32(bit3) << 3);
            bits |= (byte)(Convert.ToInt32(bit4) << 4);
            bits |= (byte)(Convert.ToInt32(bit5) << 5);
            bits |= (byte)(Convert.ToInt32(bit6) << 6);
            bits |= (byte)(Convert.ToInt32(bit7) << 7);

            buffer.WriteBigEndian(bits);
        }
    }
}
