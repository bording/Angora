﻿using System;
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
    }
}