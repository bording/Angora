using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    class ConnectionMethods
    {
        const ushort connectionChannelNumber = 0;

        static readonly byte[] protocolHeader = { 0x41, 0x4d, 0x51, 0x50, 0x00, 0x00, 0x09, 0x01 };

        static readonly Dictionary<string, object> capabilities = new Dictionary<string, object>
        {
            { "exchange_exchange_bindings", true }
        };

        readonly Socket socket;

        internal ConnectionMethods(Socket socket)
        {
            this.socket = socket;
        }

        public async Task Send_Heartbeat()
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                if (socket.HeartbeatNeeded)
                {
                    buffer.WriteBigEndian(FrameType.Heartbeat);
                    buffer.WriteBigEndian(connectionChannelNumber);
                    buffer.WriteBigEndian((uint)0);
                    buffer.WriteBigEndian(FrameEnd);
                }

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer(true);
            }
        }

        public async Task Send_ProtocolHeader()
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                buffer.Write(protocolHeader);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Send_StartOk(string connectionName, string userName, string password, string mechanism = "PLAIN", string locale = "en_US")
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

                buffer.WriteBigEndian(Method.Connection.StartOk);

                var clientProperties = new Dictionary<string, object>
                {
                    { "product", "Angora" },
                    { "capabilities", capabilities },
                    { "connection_name", connectionName }
                };

                buffer.WriteTable(clientProperties);
                buffer.WriteShortString(mechanism);
                buffer.WriteLongString($"\0{userName}\0{password}"); //response
                buffer.WriteShortString(locale);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Send_TuneOk(ushort channelMax, uint frameMax, ushort heartbeat)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

                buffer.WriteBigEndian(Method.Connection.TuneOk);
                buffer.WriteBigEndian(channelMax);
                buffer.WriteBigEndian(frameMax);
                buffer.WriteBigEndian(heartbeat);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Send_Open(string virtualHost)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

                buffer.WriteBigEndian(Method.Connection.Open);
                buffer.WriteShortString(virtualHost);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Send_Close(ushort replyCode = ConnectionReplyCode.Success, string replyText = "Goodbye", ushort failingClass = 0, ushort failingMethod = 0)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

                buffer.WriteBigEndian(Method.Connection.Close);
                buffer.WriteBigEndian(replyCode);
                buffer.WriteShortString(replyText);
                buffer.WriteBigEndian(failingClass);
                buffer.WriteBigEndian(failingMethod);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Send_CloseOk()
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

                buffer.WriteBigEndian(Method.Connection.CloseOk);

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
