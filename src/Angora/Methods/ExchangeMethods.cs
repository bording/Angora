using System.Buffers.Binary;
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
                WritePayload();
                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            void WritePayload()
            {
                var writer = new CustomBufferWriter<PipeWriter>(buffer);

                var payloadSizeHeader = writer.WriteFrameHeader(FrameType.Method, channelNumber);

                writer.Write(Method.Exchange.Declare);
                writer.Write(Reserved);
                writer.Write(Reserved);
                writer.WriteShortString(exchangeName);
                writer.WriteShortString(type);
                writer.WriteBits(passive, durable, autoDelete, @internal);
                writer.WriteTable(arguments);

                writer.Commit();
                BinaryPrimitives.WriteUInt32BigEndian(payloadSizeHeader, ((uint)writer.BytesCommitted - FrameHeaderSize));

                writer.Write(FrameEnd);

                writer.Commit();
            }
        }

        public async Task Send_Delete(string exchange, bool onlyIfUnused)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                WritePayload();
                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            void WritePayload()
            {
                var writer = new CustomBufferWriter<PipeWriter>(buffer);

                var payloadSizeHeader = writer.WriteFrameHeader(FrameType.Method, channelNumber);

                writer.Write(Method.Exchange.Delete);
                writer.Write(Reserved);
                writer.Write(Reserved);
                writer.WriteShortString(exchange);
                writer.WriteBits(onlyIfUnused);

                writer.Commit();
                BinaryPrimitives.WriteUInt32BigEndian(payloadSizeHeader, ((uint)writer.BytesCommitted - FrameHeaderSize));

                writer.Write(FrameEnd);

                writer.Commit();
            }
        }

        public async Task Send_Bind(string source, string destination, string routingKey, Dictionary<string, object> arguments)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                WritePayload();
                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            void WritePayload()
            {
                var writer = new CustomBufferWriter<PipeWriter>(buffer);

                var payloadSizeHeader = writer.WriteFrameHeader(FrameType.Method, channelNumber);

                writer.Write(Method.Exchange.Bind);
                writer.Write(Reserved);
                writer.Write(Reserved);
                writer.WriteShortString(source);
                writer.WriteShortString(destination);
                writer.WriteShortString(routingKey);
                writer.WriteBits();
                writer.WriteTable(arguments);

                writer.Commit();
                BinaryPrimitives.WriteUInt32BigEndian(payloadSizeHeader, ((uint)writer.BytesCommitted - FrameHeaderSize));

                writer.Write(FrameEnd);

                writer.Commit();
            }
        }

        public async Task Send_Unbind(string source, string destination, string routingKey, Dictionary<string, object> arguments)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                WritePayload();
                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            void WritePayload()
            {

                var writer = new CustomBufferWriter<PipeWriter>(buffer);

                var payloadSizeHeader = writer.WriteFrameHeader(FrameType.Method, channelNumber);

                writer.Write(Method.Exchange.Unbind);
                writer.Write(Reserved);
                writer.Write(Reserved);
                writer.WriteShortString(source);
                writer.WriteShortString(destination);
                writer.WriteShortString(routingKey);
                writer.WriteBits();
                writer.WriteTable(arguments);

                writer.Commit();
                BinaryPrimitives.WriteUInt32BigEndian(payloadSizeHeader, ((uint)writer.BytesCommitted - FrameHeaderSize));

                writer.Write(FrameEnd);

                writer.Commit();
            }
        }
    }
}
