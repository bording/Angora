using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.IO.Pipelines.Networking.Sockets;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using static RabbitMQClient.AmqpConstants;

namespace RabbitMQClient
{
    public class Connection
    {
        static readonly byte[] protocolHeader = { 0x41, 0x4d, 0x51, 0x50, 0x00, 0x00, 0x09, 0x01 };

        static readonly Dictionary<string, object> capabilities = new Dictionary<string, object>
        {
            { "exchange_exchange_bindings", true }
        };

        const ushort connectionChannelNumber = 0;

        readonly string hostName;
        readonly string userName;
        readonly string password;
        readonly string virtualHost;

        readonly Socket socket;

        readonly Dictionary<ushort, Channel> channels;

        readonly TaskCompletionSource<StartResult> startSent = new TaskCompletionSource<StartResult>();
        readonly TaskCompletionSource<bool> openOk = new TaskCompletionSource<bool>();
        readonly TaskCompletionSource<bool> closeOk = new TaskCompletionSource<bool>();
        readonly TaskCompletionSource<bool> readyToOpenConnection = new TaskCompletionSource<bool>();

        ushort nextChannelNumber;
        bool isOpen;

        internal Connection(string hostName, string userName, string password, string virtualHost)
        {
            this.hostName = hostName;
            this.userName = userName;
            this.password = password;
            this.virtualHost = virtualHost;

            socket = new Socket();

            channels = new Dictionary<ushort, Channel>();
        }

        internal async Task Connect(string connectionName = null)
        {
            var addresses = await Dns.GetHostAddressesAsync(hostName);
            var address = addresses.First();
            var endpoint = new IPEndPoint(address, 5672);

            await socket.Connect(endpoint);

            Task.Run(() => ReadLoop()).Ignore();

            var startResult = await Send_ProtocolHeader();
            await Send_StartOk(connectionName);

            await readyToOpenConnection.Task;

            isOpen = await Send_Open();
        }

        public async Task<Channel> CreateChannel()
        {
            var channel = new Channel(socket, ++nextChannelNumber);
            channels.Add(channel.ChannelNumber, channel);

            await channel.Open();

            return channel;
        }

        public async Task Close()
        {
            if (isOpen)
            {
                isOpen = false;

                await Send_Close();

                await socket.Close();
            }
            else
            {
                throw new Exception("already closed");
            }
        }

        async Task ReadLoop()
        {
            while (true)
            {
                var readResult = await socket.Input.ReadAsync();
                var buffer = readResult.Buffer;

                if (buffer.IsEmpty && readResult.IsCompleted)
                {
                    break;
                }

                var frameType = buffer.ReadBigEndian<byte>();
                buffer = buffer.Slice(sizeof(byte));

                var channelNumber = buffer.ReadBigEndian<ushort>();
                buffer = buffer.Slice(sizeof(ushort));

                var payloadSize = buffer.ReadBigEndian<uint>();
                buffer = buffer.Slice(sizeof(uint));

                var payload = buffer.Slice(buffer.Start, (int)payloadSize);
                buffer = buffer.Slice((int)payloadSize);

                var frameEnd = buffer.ReadBigEndian<byte>();
                buffer = buffer.Slice(sizeof(byte));

                if (frameEnd != FrameEnd)
                {
                    //TODO other stuff here around what this means
                    throw new Exception();
                }

                switch (frameType)
                {
                    case FrameType.Method:
                        await HandleIncomingMethodFrame(channelNumber, payload);
                        break;
                }

                socket.Input.Advance(buffer.Start, buffer.End);
            }
        }

        async Task SendHeartbeats(ushort interval)
        {
            await Task.Delay(200);

            while (true)
            {
                var buffer = await socket.GetWriteBuffer();

                try
                {
                    uint length = 0;

                    buffer.WriteBigEndian(FrameType.Heartbeat);
                    buffer.WriteBigEndian(connectionChannelNumber);
                    buffer.WriteBigEndian(length);
                    buffer.WriteBigEndian(FrameEnd);

                    await buffer.FlushAsync();
                }
                finally
                {
                    socket.ReleaseWriteBuffer();
                }

                await Task.Delay(TimeSpan.FromSeconds(interval));
            }
        }

        Task HandleIncomingMethodFrame(ushort channelNumber, ReadableBuffer payload)
        {
            var classId = payload.ReadBigEndian<ushort>();
            payload = payload.Slice(sizeof(ushort));

            var methodId = payload.ReadBigEndian<ushort>();
            payload = payload.Slice(sizeof(ushort));

            if (classId == Command.Connection.ClassId) //TODO validate channel 0
            {
                return HandleIncomingMethod(methodId, payload);
            }
            else
            {
                channels[channelNumber].HandleIncomingMethod((classId, methodId), payload);
            }

            return Task.CompletedTask;
        }

        Task HandleIncomingMethod(ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Connection.Start:
                    Handle_Start(arguments);
                    break;

                case Command.Connection.Tune:
                    return Handle_Tune(arguments);

                case Command.Connection.OpenOk:
                    Handle_OpenOk();
                    break;

                case Command.Connection.CloseOk:
                    Handle_CloseOk();
                    break;
            }

            return Task.CompletedTask;
        }

        struct StartResult
        {
            public byte VersionMajor;
            public byte VersionMinor;
            public Dictionary<string, object> ServerProperties;
            public string Mechanisms;
            public string Locales;
        }

        void Handle_Start(ReadableBuffer arguments)
        {
            StartResult result;
            ReadCursor cursor;

            result.VersionMajor = arguments.ReadBigEndian<byte>();
            arguments = arguments.Slice(sizeof(byte));

            result.VersionMinor = arguments.ReadBigEndian<byte>();
            arguments = arguments.Slice(sizeof(byte));

            (result.ServerProperties, cursor) = arguments.ReadTable();
            arguments = arguments.Slice(cursor);

            (result.Mechanisms, cursor) = arguments.ReadLongString();
            arguments = arguments.Slice(cursor);

            (result.Locales, cursor) = arguments.ReadLongString();

            startSent.SetResult(result);
        }

        async Task Handle_Tune(ReadableBuffer arguments)
        {
            var channelMax = arguments.ReadBigEndian<ushort>();
            arguments = arguments.Slice(sizeof(ushort));

            var frameMax = arguments.ReadBigEndian<uint>();
            arguments = arguments.Slice(sizeof(uint));

            var heartbeat = arguments.ReadBigEndian<ushort>();

            Task.Run(() => SendHeartbeats(heartbeat)).Ignore();

            await Send_TuneOk(channelMax, frameMax, heartbeat);
        }

        void Handle_OpenOk()
        {
            openOk.SetResult(true);
        }

        void Handle_CloseOk()
        {
            closeOk.SetResult(true);
        }

        async Task<StartResult> Send_ProtocolHeader()
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                buffer.Write(protocolHeader);

                await buffer.FlushAsync();

                return await startSent.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        async Task Send_StartOk(string connectionName)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

                buffer.WriteBigEndian(Command.Connection.ClassId);
                buffer.WriteBigEndian(Command.Connection.StartOk);

                var clientProperties = new Dictionary<string, object>
                {
                    { "product", "RabbitMQClient" },
                    { "capabilities", capabilities },
                    { "connection_name", connectionName }
                };

                buffer.WriteTable(clientProperties);
                buffer.WriteShortString("PLAIN"); //mechanism
                buffer.WriteLongString($"\0{userName}\0{password}"); //response
                buffer.WriteShortString("en_US"); //locale

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        async Task Send_TuneOk(ushort channelMax, uint frameMax, ushort heartbeat)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

                buffer.WriteBigEndian(Command.Connection.ClassId);
                buffer.WriteBigEndian(Command.Connection.TuneOk);
                buffer.WriteBigEndian(channelMax);
                buffer.WriteBigEndian(frameMax);
                buffer.WriteBigEndian(heartbeat);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                readyToOpenConnection.SetResult(true);
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        async Task<bool> Send_Open()
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

                buffer.WriteBigEndian(Command.Connection.ClassId);
                buffer.WriteBigEndian(Command.Connection.Open);
                buffer.WriteShortString(virtualHost);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                return await openOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        async Task Send_Close(ushort replyCode = ConnectionReplyCode.Success, string replyText = "Goodbye", ushort failingClass = 0, ushort failingMethod = 0)
        {
            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

                buffer.WriteBigEndian(Command.Connection.ClassId);
                buffer.WriteBigEndian(Command.Connection.Close);
                buffer.WriteBigEndian(replyCode);
                buffer.WriteShortString(replyText);
                buffer.WriteBigEndian(failingClass);
                buffer.WriteBigEndian(failingMethod);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                await closeOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }
    }
}

