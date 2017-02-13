using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;

namespace RabbitMQClient
{
    static class ReadableBufferExtensions
    {
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
                    break; //TODO add Decimal
                case 'S':
                    return buffer.ReadLongString();
                case 'A':
                    break; //TODO add Array
                case 'T':
                    break; //TODO add Timestamp
                case 'F':
                    return buffer.ReadTable();
                case 'V':
                    break; //TODO handle Void
                case 'x':
                    break; //TODO handle byte array qpid type
            }

            return (null, new ReadCursor());
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
    }
}
