using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;

namespace Angora
{
    static class WritableBufferExtensions
    {
        static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static Span<byte> WriteFrameHeader(this WritableBuffer buffer, byte frameType, ushort channel)
        {
            buffer.WriteBigEndian(frameType);
            buffer.WriteBigEndian(channel);

            buffer.Ensure(sizeof(uint));
            var sizeBookmark = buffer.Memory.Span;
            buffer.Advance(sizeof(uint));

            return sizeBookmark;
        }

        public static void WriteShortString(this WritableBuffer buffer, string value)
        {
            if (value == null)
            {
                buffer.WriteBigEndian<byte>(0);
                return;
            }

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
            if (value == null)
            {
                buffer.WriteBigEndian<uint>(0);
                return;
            }

            var valueBytes = Encoding.UTF8.GetBytes(value);

            buffer.WriteBigEndian((uint)valueBytes.Length);
            buffer.Write(valueBytes);
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

        static void WriteArray(this WritableBuffer buffer, List<object> value)
        {
            buffer.Ensure(sizeof(uint));
            var sizeBookmark = buffer.Memory.Span;
            buffer.Advance(sizeof(uint));

            var before = (uint)buffer.BytesWritten;

            if (value != null)
            {
                foreach (var item in value)
                {
                    buffer.WriteFieldValue(item);
                }
            }

            sizeBookmark.WriteBigEndian((uint)buffer.BytesWritten - before);
        }

        public static void WriteTable(this WritableBuffer buffer, Dictionary<string, object> value)
        {
            buffer.Ensure(sizeof(uint));
            var sizeBookmark = buffer.Memory.Span;
            buffer.Advance(sizeof(uint));

            var before = (uint)buffer.BytesWritten;

            if (value != null)
            {
                foreach (var item in value)
                {
                    buffer.WriteShortString(item.Key);
                    buffer.WriteFieldValue(item.Value);
                }
            }

            sizeBookmark.WriteBigEndian((uint)buffer.BytesWritten - before);
        }

        static void WriteFieldValue(this WritableBuffer buffer, object value)
        {
            switch (value)
            {
                case null:
                    buffer.WriteBigEndian((byte)'V');
                    break;
                case bool t:
                    buffer.WriteBigEndian((byte)'t');
                    buffer.WriteBigEndian(t ? (byte)1 : (byte)0);
                    break;
                case sbyte b:
                    buffer.WriteBigEndian((byte)'b');
                    buffer.WriteBigEndian(b);
                    break;
                case byte B:
                    buffer.WriteBigEndian((byte)'B');
                    buffer.WriteBigEndian(B);
                    break;
                case short s:
                    buffer.WriteBigEndian((byte)'s');
                    buffer.WriteBigEndian(s);
                    break;
                case ushort u:
                    buffer.WriteBigEndian((byte)'u');
                    buffer.WriteBigEndian(u);
                    break;
                case int I:
                    buffer.WriteBigEndian((byte)'I');
                    buffer.WriteBigEndian(I);
                    break;
                case uint i:
                    buffer.WriteBigEndian((byte)'i');
                    buffer.WriteBigEndian(i);
                    break;
                case long l:
                    buffer.WriteBigEndian((byte)'l');
                    buffer.WriteBigEndian(l);
                    break;
                case float f:
                    buffer.WriteBigEndian((byte)'f');
                    buffer.WriteBigEndian(f);
                    break;
                case double d:
                    buffer.WriteBigEndian((byte)'f');
                    buffer.WriteBigEndian(d);
                    break;
                case decimal D:
                    buffer.WriteBigEndian((byte)'D');
                    buffer.WriteDecimal(D);
                    break;
                case string S:
                    buffer.WriteBigEndian((byte)'S');
                    buffer.WriteLongString(S);
                    break;
                case List<object> A:
                    buffer.WriteBigEndian((byte)'A');
                    buffer.WriteArray(A);
                    break;
                case DateTime T:
                    buffer.WriteBigEndian((byte)'T');
                    buffer.WriteTimestamp(T);
                    break;
                case Dictionary<string, object> F:
                    buffer.WriteBigEndian((byte)'F');
                    buffer.WriteTable(F);
                    break;
                case byte[] x:
                    buffer.WriteBigEndian((byte)'x');
                    buffer.WriteBytes(x);
                    break;
                default:
                    throw new Exception($"Unknown field value type: '{value.GetType()}'.");
            }
        }

        static void WriteDecimal(this WritableBuffer buffer, decimal value)
        {
            //TODO write real values

            buffer.WriteBigEndian((byte)0); //scale
            buffer.WriteBigEndian((uint)0); //value
        }

        static void WriteTimestamp(this WritableBuffer buffer, DateTime value)
        {
            var timestamp = (ulong)(value.ToUniversalTime() - UnixEpoch).TotalSeconds;
            buffer.WriteBigEndian(timestamp);
        }

        static void WriteBytes(this WritableBuffer buffer, byte[] value)
        {
            if (value == null)
            {
                buffer.WriteBigEndian<byte>(0);
                return;
            }

            buffer.WriteBigEndian((byte)value.Length);

            for (int i = 0; i < value.Length; i++)
            {
                buffer.WriteBigEndian(value[i]);
            }
        }

        public static void WriteBasicProperties(this WritableBuffer buffer, MessageProperties properties)
        {
            buffer.Ensure(sizeof(ushort));
            var flagsBookmark = buffer.Memory.Span;
            buffer.Advance(sizeof(ushort));

            var flags = (ushort)0;

            if (properties.ContentType != null)
            {
                flags |= 1 << 15;
                buffer.WriteShortString(properties.ContentType);
            }

            if (properties.ContentEncoding != null)
            {
                flags |= 1 << 14;
                buffer.WriteShortString(properties.ContentEncoding);
            }

            if (properties.Headers != null)
            {
                flags |= 1 << 13;
                buffer.WriteTable(properties.Headers);
            }

            if (properties.DeliveryMode != 0)
            {
                flags |= 1 << 12;
                buffer.WriteBigEndian(properties.DeliveryMode);
            }

            if (properties.Priority != 0)
            {
                flags |= 1 << 11;
                buffer.WriteBigEndian(properties.Priority);
            }

            if (properties.CorrelationId != null)
            {
                flags |= 1 << 10;
                buffer.WriteShortString(properties.CorrelationId);
            }

            if (properties.ReplyTo != null)
            {
                flags |= 1 << 9;
                buffer.WriteShortString(properties.ReplyTo);
            }

            if (properties.Expiration != null)
            {
                flags |= 1 << 8;
                buffer.WriteShortString(properties.Expiration);
            }

            if (properties.MessageId != null)
            {
                flags |= 1 << 7;
                buffer.WriteShortString(properties.MessageId);
            }

            if (properties.Timestamp != default(DateTime))
            {
                flags |= 1 << 6;
                buffer.WriteTimestamp(properties.Timestamp);
            }

            if (properties.Type != null)
            {
                flags |= 1 << 5;
                buffer.WriteShortString(properties.Type);
            }

            if (properties.UserId != null)
            {
                flags |= 1 << 4;
                buffer.WriteShortString(properties.UserId);
            }

            if (properties.AppId != null)
            {
                flags |= 1 << 3;
                buffer.WriteShortString(properties.AppId);
            }

            flagsBookmark.WriteBigEndian(flags);
        }
    }
}
