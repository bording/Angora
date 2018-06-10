using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    class ChannelMethods
    {
        readonly Socket socket;
        readonly ushort channelNumber;

        internal ChannelMethods(Socket socket, ushort channelNumber)
        {
            this.socket = socket;
            this.channelNumber = channelNumber;
        }

        public async Task Send_Open()
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

                writer.Write(Method.Channel.Open);
                writer.Write(Reserved);

                writer.Commit();
                BinaryPrimitives.WriteUInt32BigEndian(payloadSizeHeader, ((uint)writer.BytesCommitted - FrameHeaderSize));

                writer.Write(FrameEnd);

                writer.Commit();
            }
        }

        public async Task Send_Close(ushort replyCode, string replyText, ushort failingClass, ushort failingMethod)
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

                writer.Write(Method.Channel.Close);
                writer.Write(replyCode);
                writer.WriteShortString(replyText);
                writer.Write(failingClass);
                writer.Write(failingMethod);

                writer.Commit();
                BinaryPrimitives.WriteUInt32BigEndian(payloadSizeHeader, ((uint)writer.BytesCommitted - FrameHeaderSize));

                writer.Write(FrameEnd);

                writer.Commit();
            }
        }

        public async Task Send_CloseOk()
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

                writer.Write(Method.Channel.CloseOk);

                writer.Commit();
                BinaryPrimitives.WriteUInt32BigEndian(payloadSizeHeader, ((uint)writer.BytesCommitted - FrameHeaderSize));

                writer.Write(FrameEnd);

                writer.Commit();
            }
        }
    }
}
