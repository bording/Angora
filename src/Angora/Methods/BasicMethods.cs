using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    class BasicMethods
    {
        readonly Socket socket;
        readonly ushort channelNumber;
        readonly uint maxContentBodySize;

        internal BasicMethods(Socket socket, ushort channelNumber, uint maxContentBodySize)
        {
            this.socket = socket;
            this.channelNumber = channelNumber;
            this.maxContentBodySize = maxContentBodySize;
        }

        public async Task Send_Qos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Qos);
                buffer.WriteBigEndian(prefetchSize);
                buffer.WriteBigEndian(prefetchCount);
                buffer.WriteBits(global);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Send_Consume(string queue, string consumerTag, bool autoAck, bool exclusive, Dictionary<string, object> arguments)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Consume);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteShortString(consumerTag);
                buffer.WriteBits(false, autoAck, exclusive);
                buffer.WriteTable(arguments);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Send_Cancel(string consumerTag)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Cancel);
                buffer.WriteShortString(consumerTag);
                buffer.WriteBits();

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Send_Recover()
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Recover);
                buffer.WriteBits(true);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Send_Ack(ulong deliveryTag, bool multiple)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Ack);
                buffer.WriteBigEndian(deliveryTag);
                buffer.WriteBits(multiple);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }


        public async Task Send_Publish(string exchange, string routingKey, bool mandatory, MessageProperties properties, Span<byte> body)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Publish);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(exchange);
                buffer.WriteShortString(routingKey);
                buffer.WriteBits(mandatory);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);
                buffer.WriteBigEndian(FrameEnd);

                WriteContentHeaderFrame(ref buffer, properties, (ulong)body.Length);

                var framesToWrite = body.Length > 0;

                while (framesToWrite)
                {
                    Span<byte> frame;

                    if (body.Length > maxContentBodySize)
                    {
                        frame = body.Slice(0, (int)maxContentBodySize);
                        body = body.Slice((int)maxContentBodySize);
                    }
                    else
                    {
                        frame = body;
                        framesToWrite = false;
                    }

                    WriteContentBodyFrame(ref buffer, frame);
                }

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        void WriteContentHeaderFrame(ref WritableBuffer buffer, MessageProperties properties, ulong length)
        {
            var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.ContentHeader, channelNumber);

            var bytesWrittenBefore = (uint)buffer.BytesWritten;

            buffer.WriteBigEndian(ClassId.Basic);
            buffer.WriteBigEndian(Reserved);
            buffer.WriteBigEndian(Reserved);
            buffer.WriteBigEndian(length);
            buffer.WriteBasicProperties(properties);

            payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - bytesWrittenBefore);

            buffer.WriteBigEndian(FrameEnd);
        }

        void WriteContentBodyFrame(ref WritableBuffer buffer, Span<byte> body)
        {
            var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.ContentBody, channelNumber);

            var bytesWrittenBefore = (uint)buffer.BytesWritten;

            buffer.Write(body);

            payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - bytesWrittenBefore);

            buffer.WriteBigEndian(FrameEnd);
        }
    }
}
