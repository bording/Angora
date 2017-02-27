using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;

namespace Angora
{
    static class ReadableBufferExtensions
    {
        static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        static (List<object> value, ReadCursor position) ReadArray(this ReadableBuffer buffer)
        {
            var result = new List<object>();

            var arrayLength = buffer.ReadBigEndian<uint>();
            buffer = buffer.Slice(sizeof(uint), (int)arrayLength);

            while (!buffer.IsEmpty)
            {
                var (fieldValue, cursor) = buffer.ReadFieldValue();
                buffer = buffer.Slice(cursor);

                result.Add(fieldValue);
            }

            return (result, buffer.End);
        }

        public static (Dictionary<string, object> value, ReadCursor position) ReadTable(this ReadableBuffer buffer)
        {
            var result = new Dictionary<string, object>();

            var tableLength = buffer.ReadBigEndian<uint>();
            buffer = buffer.Slice(sizeof(uint), (int)tableLength);

            while (!buffer.IsEmpty)
            {
                var (fieldName, cursor1) = buffer.ReadShortString();
                buffer = buffer.Slice(cursor1);

                var (fieldValue, cursor2) = buffer.ReadFieldValue();
                buffer = buffer.Slice(cursor2);

                result.Add(fieldName, fieldValue);
            }

            return (result, buffer.End);
        }

        static (object value, ReadCursor position) ReadFieldValue(this ReadableBuffer buffer)
        {
            var fieldValueType = buffer.ReadBigEndian<byte>();
            buffer = buffer.Slice(sizeof(byte));

            switch ((char)fieldValueType)
            {
                case 't':
                    return (Convert.ToBoolean(buffer.ReadBigEndian<byte>()), buffer.Move(buffer.Start, sizeof(byte)));
                case 'b':
                    return (buffer.ReadBigEndian<sbyte>(), buffer.Move(buffer.Start, sizeof(sbyte)));
                case 'B':
                    return (buffer.ReadBigEndian<byte>(), buffer.Move(buffer.Start, sizeof(byte)));
                case 's':
                    return (buffer.ReadBigEndian<short>(), buffer.Move(buffer.Start, sizeof(short)));
                case 'u':
                    return (buffer.ReadBigEndian<ushort>(), buffer.Move(buffer.Start, sizeof(ushort)));
                case 'I':
                    return (buffer.ReadBigEndian<int>(), buffer.Move(buffer.Start, sizeof(int)));
                case 'i':
                    return (buffer.ReadBigEndian<uint>(), buffer.Move(buffer.Start, sizeof(uint)));
                case 'l':
                    return (buffer.ReadBigEndian<long>(), buffer.Move(buffer.Start, sizeof(long)));
                case 'f':
                    return (buffer.ReadBigEndian<float>(), buffer.Move(buffer.Start, sizeof(float)));
                case 'd':
                    return (buffer.ReadBigEndian<double>(), buffer.Move(buffer.Start, sizeof(double)));
                case 'D':
                    return buffer.ReadDecimal();
                case 'S':
                    return buffer.ReadLongString();
                case 'A':
                    return buffer.ReadArray();
                case 'T':
                    return buffer.ReadTimestamp();
                case 'F':
                    return buffer.ReadTable();
                case 'V':
                    return (null, buffer.Start);
                case 'x':
                    return buffer.ReadBytes();
                default:
                    throw new Exception($"Unknown field value type: '{fieldValueType}'.");
            }
        }

        public static (string value, ReadCursor position) ReadShortString(this ReadableBuffer buffer)
        {
            var length = buffer.ReadBigEndian<byte>();
            var bytes = buffer.Slice(sizeof(byte), length);

            return (Encoding.UTF8.GetString(bytes.ToArray()), bytes.End);
        }

        public static (string value, ReadCursor position) ReadLongString(this ReadableBuffer buffer)
        {
            var length = buffer.ReadBigEndian<uint>();
            var bytes = buffer.Slice(sizeof(uint), (int)length);

            return (Encoding.UTF8.GetString(bytes.ToArray()), bytes.End);
        }

        static (byte[] value, ReadCursor position) ReadBytes(this ReadableBuffer buffer)
        {
            var length = buffer.ReadBigEndian<uint>();
            var bytes = buffer.Slice(sizeof(uint), (int)length);

            return (bytes.ToArray(), bytes.End);
        }

        static (decimal value, ReadCursor position) ReadDecimal(this ReadableBuffer buffer)
        {
            var scale = buffer.ReadBigEndian<byte>();
            buffer = buffer.Slice(sizeof(byte));

            var value = buffer.ReadBigEndian<uint>();
            buffer = buffer.Slice(sizeof(uint));

            return (default(decimal), buffer.Start); //TODO return real value
        }

        static (DateTime value, ReadCursor position) ReadTimestamp(this ReadableBuffer buffer)
        {
            var time = buffer.ReadBigEndian<ulong>();
            buffer = buffer.Slice(sizeof(ulong));

            return (UnixEpoch.AddSeconds(time), buffer.Start);
        }

        public static MessageProperties ReadBasicProperties(this ReadableBuffer buffer)
        {
            var properties = new MessageProperties();

            var flags = buffer.ReadBigEndian<ushort>();
            buffer = buffer.Slice(sizeof(ushort));

            ReadCursor cursor;

            if ((flags & 1 << 15) != 0)
            {
                (properties.ContentType, cursor) = buffer.ReadShortString();
                buffer = buffer.Slice(cursor);
            }

            if ((flags & 1 << 14) != 0)
            {
                (properties.ContentEncoding, cursor) = buffer.ReadShortString();
                buffer = buffer.Slice(cursor);
            }

            if ((flags & 1 << 13) != 0)
            {
                (properties.Headers, cursor) = buffer.ReadTable();
                buffer = buffer.Slice(cursor);
            }

            if ((flags & 1 << 12) != 0)
            {
                properties.DeliveryMode = buffer.ReadBigEndian<byte>();
                buffer = buffer.Slice(sizeof(byte));
            }

            if ((flags & 1 << 11) != 0)
            {
                properties.Priority = buffer.ReadBigEndian<byte>();
                buffer = buffer.Slice(sizeof(byte));
            }

            if ((flags & 1 << 10) != 0)
            {
                (properties.CorrelationId, cursor) = buffer.ReadShortString();
                buffer = buffer.Slice(cursor);
            }

            if ((flags & 1 << 9) != 0)
            {
                (properties.ReplyTo, cursor) = buffer.ReadShortString();
                buffer = buffer.Slice(cursor);
            }

            if ((flags & 1 << 8) != 0)
            {
                (properties.Expiration, cursor) = buffer.ReadShortString();
                buffer = buffer.Slice(cursor);
            }

            if ((flags & 1 << 7) != 0)
            {
                (properties.MessageId, cursor) = buffer.ReadShortString();
                buffer = buffer.Slice(cursor);
            }

            if ((flags & 1 << 6) != 0)
            {
                (properties.Timestamp, cursor) = buffer.ReadTimestamp();
                buffer = buffer.Slice(cursor);
            }

            if ((flags & 1 << 5) != 0)
            {
                (properties.Type, cursor) = buffer.ReadShortString();
                buffer = buffer.Slice(cursor);
            }

            if ((flags & 1 << 4) != 0)
            {
                (properties.UserId, cursor) = buffer.ReadShortString();
                buffer = buffer.Slice(cursor);
            }

            if ((flags & 1 << 3) != 0)
            {
                (properties.AppId, cursor) = buffer.ReadShortString();
                buffer = buffer.Slice(cursor);
            }

            return properties;
        }
    }
}
