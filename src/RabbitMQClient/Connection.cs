using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.IO.Pipelines.Networking.Sockets;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using static RabbitMQClient.AmqpConstants;

namespace RabbitMQClient
{
    public class Connection
    {
        static readonly byte[] protocolHeader = { 0x41, 0x4d, 0x51, 0x50, 0x00, 0x00, 0x09, 0x01 };

        const ushort connectionChannelNumber = 0;

        readonly string hostName;
        readonly string userName;
        readonly string password;
        readonly string virtualHost;

        SocketConnection connection;

        ushort nextChannelNumber;

        readonly Dictionary<ushort, Channel> channels;

        internal Connection(string hostName, string userName, string password, string virtualHost)
        {
            this.hostName = hostName;
            this.userName = userName;
            this.password = password;
            this.virtualHost = virtualHost;

            channels = new Dictionary<ushort, Channel>();
        }

        public async Task<Channel> CreateChannel()
        {
            var channel = new Channel(connection.Output, ++nextChannelNumber);
            channels.Add(channel.ChannelNumber, channel);

            await channel.Open();

            return channel;
        }

        public async Task Close()
        {
            await Send_Connection_Close();

            connection.Input.Complete();
            connection.Output.Complete();

            await connection.DisposeAsync();
        }

        internal async Task Connect()
        {
            var addresses = await Dns.GetHostAddressesAsync(hostName);
            var address = addresses.First();
            var endpoint = new IPEndPoint(address, 5672);

            connection = await SocketConnection.ConnectAsync(endpoint);

            Task.Run(() => ReadLoop()).Ignore();

            var startResult = await Send_Connection_ProtocolHeader();
            var tuneResult = await Send_Connection_StartOk();

            Task.Run(() => SendHeartbeats(tuneResult.Heartbeat)).Ignore();

            await Send_Connection_TuneOk(tuneResult.ChannelMax, tuneResult.FrameMax, tuneResult.Heartbeat);
            var openResult = await Send_Connection_Open();
        }

        async Task ReadLoop()
        {
            while (true)
            {
                var readResult = await connection.Input.ReadAsync();
                var buffer = readResult.Buffer;

                if (readResult.IsCancelled || readResult.IsCompleted || buffer.IsEmpty)
                {
                    break;
                }

                var frameType = buffer.Slice(0, 1).ReadBigEndian<byte>();
                var channelNumber = buffer.Slice(1, 2).ReadBigEndian<ushort>();

                var payloadSize = buffer.Slice(3, 4).ReadBigEndian<uint>();
                var payload = buffer.Slice(7, (int)payloadSize);

                var frameEnd = buffer.Slice((int)payloadSize + 7, 1).ReadBigEndian<byte>();
                //TODO validate frame end

                switch (frameType)
                {
                    case FrameType.Method:
                        ParseMethodFrame(channelNumber, payload);
                        break;
                }

                connection.Input.Advance(buffer.End);
            }
        }

        async Task SendHeartbeats(ushort interval)
        {
            await Task.Delay(200);

            while (true)
            {
                var buffer = connection.Output.Alloc();

                uint length = 0;

                buffer.WriteBigEndian(FrameType.Heartbeat);
                buffer.WriteBigEndian(connectionChannelNumber);
                buffer.WriteBigEndian(length);
                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                await Task.Delay(TimeSpan.FromSeconds(interval));
            }
        }

        void ParseMethodFrame(ushort channelNumber, ReadableBuffer payload)
        {
            var classId = payload.Slice(0, 2).ReadBigEndian<ushort>();
            var methodId = payload.Slice(2, 2).ReadBigEndian<ushort>();
            var arguments = payload.Slice(4);

            if (classId == Command.Connection.ClassId) //TODO validate channel 0
            {
                ParseConnectionMethod(channelNumber, methodId, arguments);
            }
            else
            {
                channels[channelNumber].ParseMethod(classId, methodId, arguments);
            }
        }

        void ParseConnectionMethod(ushort channelNumber, ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Connection.Start:
                    Handle_Connection_Start(channelNumber, arguments);
                    break;

                case Command.Connection.Tune:
                    Handle_Connection_Tune(channelNumber, arguments);
                    break;

                case Command.Connection.OpenOk:
                    Handle_Connection_OpenOk(channelNumber, arguments);
                    break;

                case Command.Connection.CloseOk:
                    Handle_Connection_CloseOk(channelNumber, arguments);
                    break;
            }
        }

        // Connection Handle methods

        struct Connection_StartResult
        {
            public byte VersionMajor;
            public byte VersionMinor;
            public ReadableBuffer ServerProperties;
            public ReadableBuffer Mechanisms;
            public ReadableBuffer Locales;
        }

        void Handle_Connection_Start(ushort channelNumber, ReadableBuffer arguments)
        {
            var result = new Connection_StartResult
            {
                VersionMajor = arguments.Slice(0, 1).ReadBigEndian<byte>(),
                VersionMinor = arguments.Slice(1, 1).ReadBigEndian<byte>()
            };

            var serverPropertiesLength = arguments.Slice(2, 4).ReadBigEndian<uint>();
            result.ServerProperties = arguments.Slice(6, (int)serverPropertiesLength);

            var mechanismsStart = 6 + (int)serverPropertiesLength;
            var mechanismsLength = arguments.Slice(mechanismsStart, 4).ReadBigEndian<uint>();
            result.Mechanisms = arguments.Slice(mechanismsStart + 4, (int)mechanismsLength);

            var localesStart = mechanismsStart + 4 + (int)mechanismsLength;
            var localesLength = arguments.Slice(localesStart, 4).ReadBigEndian<uint>();
            result.Locales = arguments.Slice(localesStart + 4, (int)localesLength);

            connection_ProtocolHeader.SetResult(result);
        }

        struct Connection_TuneResult
        {
            public ushort ChannelMax;
            public uint FrameMax;
            public ushort Heartbeat;
        }

        void Handle_Connection_Tune(ushort channelNumber, ReadableBuffer arguments)
        {
            var result = new Connection_TuneResult
            {
                ChannelMax = arguments.Slice(0, 2).ReadBigEndian<ushort>(),
                FrameMax = arguments.Slice(2, 4).ReadBigEndian<uint>(),
                Heartbeat = arguments.Slice(6, 2).ReadBigEndian<ushort>()
            };

            connection_StartOk.SetResult(result);
        }

        void Handle_Connection_OpenOk(ushort channelNumber, ReadableBuffer arguments)
        {
            connection_OpenOk.SetResult(true);
        }

        void Handle_Connection_CloseOk(ushort channelNumber, ReadableBuffer arguments)
        {
            connection_CloseOk.SetResult(true);
        }

        //Connection Send methods

        TaskCompletionSource<Connection_StartResult> connection_ProtocolHeader;
        Task<Connection_StartResult> Send_Connection_ProtocolHeader()
        {
            connection_ProtocolHeader = new TaskCompletionSource<Connection_StartResult>();

            var buffer = connection.Output.Alloc();
            buffer.Write(protocolHeader);

            buffer.FlushAsync();

            return connection_ProtocolHeader.Task;
        }

        TaskCompletionSource<Connection_TuneResult> connection_StartOk;
        Task<Connection_TuneResult> Send_Connection_StartOk()
        {
            connection_StartOk = new TaskCompletionSource<Connection_TuneResult>();

            var buffer = connection.Output.Alloc();

            var clientProperties = new byte[0];
            var clientPropertiesLength = (uint)clientProperties.Length;

            var mechanism = Encoding.UTF8.GetBytes("PLAIN");
            var mechanismLength = (byte)mechanism.Length;

            var response = Encoding.UTF8.GetBytes($"\0{userName}\0{password}");
            var responseLength = (uint)response.Length;

            var locale = Encoding.UTF8.GetBytes("en_US");
            var localeLength = (byte)locale.Length;

            var payloadSize = 2 + 2 + 4 + clientPropertiesLength + 1 + mechanismLength + 4 + responseLength + 1 + localeLength;

            buffer.WriteBigEndian(FrameType.Method);
            buffer.WriteBigEndian(connectionChannelNumber);
            buffer.WriteBigEndian(payloadSize);
            buffer.WriteBigEndian(Command.Connection.ClassId);
            buffer.WriteBigEndian(Command.Connection.StartOk);
            buffer.WriteBigEndian(clientPropertiesLength);
            buffer.Write(clientProperties);
            buffer.WriteBigEndian(mechanismLength);
            buffer.Write(mechanism);
            buffer.WriteBigEndian(responseLength);
            buffer.Write(response);
            buffer.WriteBigEndian(localeLength);
            buffer.Write(locale);
            buffer.WriteBigEndian(FrameEnd);

            buffer.FlushAsync();

            return connection_StartOk.Task;
        }

        async Task Send_Connection_TuneOk(ushort channelMax, uint frameMax, ushort heartbeat)
        {
            var buffer = connection.Output.Alloc();

            uint payloadSize = 2 + 2 + 2 + 4 + 2;

            buffer.WriteBigEndian(FrameType.Method);
            buffer.WriteBigEndian(connectionChannelNumber);
            buffer.WriteBigEndian(payloadSize);
            buffer.WriteBigEndian(Command.Connection.ClassId);
            buffer.WriteBigEndian(Command.Connection.TuneOk);
            buffer.WriteBigEndian(channelMax);
            buffer.WriteBigEndian(frameMax);
            buffer.WriteBigEndian(heartbeat);
            buffer.WriteBigEndian(FrameEnd);

            await buffer.FlushAsync();

            return;
        }

        TaskCompletionSource<bool> connection_OpenOk;
        Task<bool> Send_Connection_Open()
        {
            connection_OpenOk = new TaskCompletionSource<bool>();

            var buffer = connection.Output.Alloc();

            var virtualHostBytes = Encoding.UTF8.GetBytes(virtualHost);
            var virtualHostLength = (byte)virtualHostBytes.Length;

            uint payloadSize = (uint)2 + 2 + 1 + virtualHostLength + 1 + 1;

            buffer.WriteBigEndian(FrameType.Method);
            buffer.WriteBigEndian(connectionChannelNumber);
            buffer.WriteBigEndian(payloadSize);
            buffer.WriteBigEndian(Command.Connection.ClassId);
            buffer.WriteBigEndian(Command.Connection.Open);
            buffer.WriteBigEndian(virtualHostLength);
            buffer.Write(virtualHostBytes);
            buffer.WriteBigEndian(Reserved);
            buffer.WriteBigEndian(Reserved);
            buffer.WriteBigEndian(FrameEnd);
            buffer.FlushAsync();

            return connection_OpenOk.Task;
        }

        TaskCompletionSource<bool> connection_CloseOk;
        Task Send_Connection_Close()
        {
            connection_CloseOk = new TaskCompletionSource<bool>();

            var buffer = connection.Output.Alloc();

            var replyText = Encoding.UTF8.GetBytes("Connection close forced");
            var replyTextLength = (byte)replyText.Length;

            ushort emptyError = 0;

            uint payloadSize = (uint)2 + 2 + 2 + 1 + replyTextLength + 2 + 2;

            buffer.WriteBigEndian(FrameType.Method);
            buffer.WriteBigEndian(connectionChannelNumber);
            buffer.WriteBigEndian(payloadSize);
            buffer.WriteBigEndian(Command.Connection.ClassId);
            buffer.WriteBigEndian(Command.Connection.Close);
            buffer.WriteBigEndian(ConnectionReplyCode.Success);
            buffer.WriteBigEndian(replyTextLength);
            buffer.Write(replyText);
            buffer.WriteBigEndian(emptyError);
            buffer.WriteBigEndian(emptyError);
            buffer.WriteBigEndian(FrameEnd);

            buffer.FlushAsync();

            return connection_CloseOk.Task;
        }
    }
}

