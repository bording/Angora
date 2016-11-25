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

        SocketConnection connection;

        readonly string hostName;
        readonly string userName;
        readonly string password;

        internal Connection(string hostName, string userName, string password)
        {
            this.hostName = hostName;
            this.userName = userName;
            this.password = password;
        }

        internal async Task Connect()
        {
            var addresses = await Dns.GetHostAddressesAsync(hostName);
            var address = addresses.First();
            var endpoint = new IPEndPoint(address, 5672);

            connection = await SocketConnection.ConnectAsync(endpoint);

            Task.Run(() => ReadLoop()).Ignore();

            var buffer = connection.Output.Alloc();
            buffer.Write(protocolHeader);
            await buffer.FlushAsync();
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
                //validate frame end

                switch (frameType)
                {
                    case FrameType.Method:
                        await ParseMethodFrame(channelNumber, payload);
                        break;
                }

                connection.Input.Advance(buffer.End);
            }
        }

        Task ParseMethodFrame(ushort channelNumber, ReadableBuffer payload)
        {
            var classId = payload.Slice(0, 2).ReadBigEndian<ushort>();
            var methodId = payload.Slice(2, 2).ReadBigEndian<ushort>();
            var arguments = payload.Slice(4);

            var task = Task.CompletedTask;

            switch (classId)
            {
                case Command.Connection.ClassId:
                    task = ParseConnectionMethod(channelNumber, methodId, arguments);
                    break;

                case Command.Channel.ClassId:
                    task = ParseChannelMethod(channelNumber, methodId, arguments);
                    break;
            }

            return task;
        }

        Task ParseConnectionMethod(ushort channelNumber, ushort methodId, ReadableBuffer arguments)
        {
            var task = Task.CompletedTask;

            switch (methodId)
            {
                case Command.Connection.Start:

                    var versionMajor = arguments.Slice(0, 1).ReadBigEndian<byte>();
                    var versionMinor = arguments.Slice(1, 1).ReadBigEndian<byte>();

                    var serverPropertiesLength = arguments.Slice(2, 4).ReadBigEndian<uint>();
                    var serverProperties = arguments.Slice(6, (int)serverPropertiesLength);

                    var mechanismsStart = 6 + (int)serverPropertiesLength;
                    var mechanismsLength = arguments.Slice(mechanismsStart, 4).ReadBigEndian<uint>();
                    var mechaninisms = arguments.Slice(mechanismsStart + 4, (int)mechanismsLength);

                    var localesStart = mechanismsStart + 4 + (int)mechanismsLength;
                    var localesLength = arguments.Slice(localesStart, 4).ReadBigEndian<uint>();
                    var locales = arguments.Slice(localesStart + 4, (int)localesLength);

                    task = Send_Connection_StartOk();
                    break;

                case Command.Connection.Tune:

                    var channelMax = arguments.Slice(0, 2).ReadBigEndian<ushort>();
                    var frameMax = arguments.Slice(2, 4).ReadBigEndian<uint>();
                    var heartbeat = arguments.Slice(6, 2).ReadBigEndian<ushort>();

                    task = Send_Connection_TuneOk(channelMax, frameMax, heartbeat);
                    break;
            }

            return task;
        }

        Task Send_Connection_StartOk()
        {
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

            return buffer.FlushAsync();
        }

        Task Send_Connection_TuneOk(ushort channelMax, uint frameMax, ushort heartbeat)
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

            return buffer.FlushAsync();
        }

        Task ParseChannelMethod(ushort channelNumber, ushort methodId, ReadableBuffer arguments)
        {
            return Task.CompletedTask;
        }
    }
}

