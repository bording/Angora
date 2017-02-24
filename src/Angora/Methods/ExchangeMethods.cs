using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    class ExchangeMethods
    {
        readonly Socket socket;
        readonly ushort channelNumber;

        internal ExchangeMethods(Socket socket, ushort channelNumber)
        {
            this.socket = socket;
            this.channelNumber = channelNumber;
        }

        public async Task Send_Declare(string exchangeName, string type, bool passive, bool durable, bool autoDelete, bool @internal, Dictionary<string, object> arguments)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Exchange.Declare);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(exchangeName);
                buffer.WriteShortString(type);
                buffer.WriteBits(passive, durable, autoDelete, @internal);
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

        public async Task Send_Delete(string exchange, bool onlyIfUnused)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Exchange.Delete);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(exchange);
                buffer.WriteBits(onlyIfUnused);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Send_Bind(string source, string destination, string routingKey, Dictionary<string, object> arguments)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Exchange.Bind);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(source);
                buffer.WriteShortString(destination);
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

        public async Task Send_Unbind(string source, string destination, string routingKey, Dictionary<string, object> arguments)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Exchange.Unbind);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(source);
                buffer.WriteShortString(destination);
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
    }
}
