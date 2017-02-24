using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    class QueueMethods
    {
        readonly Socket socket;
        readonly ushort channelNumber;

        internal QueueMethods(Socket socket, ushort channelNumber)
        {
            this.socket = socket;
            this.channelNumber = channelNumber;
        }

        public async Task Send_Declare(string queueName, bool passive, bool durable, bool exclusive, bool autoDelete, Dictionary<string, object> arguments)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Queue.Declare);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queueName);
                buffer.WriteBits(passive, durable, exclusive, autoDelete);
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

        public async Task Send_Bind(string queue, string exchange, string routingKey, Dictionary<string, object> arguments)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Queue.Bind);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteShortString(exchange);
                buffer.WriteShortString(routingKey);
                buffer.WriteBits();
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

        public async Task Send_Unbind(string queue, string exchange, string routingKey, Dictionary<string, object> arguments)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Queue.Unbind);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteShortString(exchange);
                buffer.WriteShortString(routingKey);
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

        public async Task Send_Purge(string queue)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Queue.Purge);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
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

        public async Task Send_Delete(string queue, bool onlyIfUnused, bool onlyIfEmpty)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Queue.Delete);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteBits(onlyIfEmpty, onlyIfUnused);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }
    }
}
