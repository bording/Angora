﻿using System;
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
                        ParseMethodFrame(channelNumber, payload);
                        break;
                }

                connection.Input.Advance(buffer.Start);
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
            var classId = payload.ReadBigEndian<ushort>();
            payload = payload.Slice(sizeof(ushort));

            var methodId = payload.ReadBigEndian<ushort>();
            payload = payload.Slice(sizeof(ushort));

            if (classId == Command.Connection.ClassId) //TODO validate channel 0
            {
                ParseConnectionMethod(methodId, payload);
            }
            else
            {
                channels[channelNumber].ParseMethod(classId, methodId, payload);
            }
        }

        void ParseConnectionMethod(ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Connection.Start:
                    Handle_Connection_Start(arguments);
                    break;

                case Command.Connection.Tune:
                    Handle_Connection_Tune(arguments);
                    break;

                case Command.Connection.OpenOk:
                    Handle_Connection_OpenOk();
                    break;

                case Command.Connection.CloseOk:
                    Handle_Connection_CloseOk();
                    break;
            }
        }

        // Connection Handle methods

        struct Connection_StartResult
        {
            public byte VersionMajor;
            public byte VersionMinor;
            public byte[] ServerProperties;
            public string Mechanisms;
            public string Locales;
        }

        void Handle_Connection_Start(ReadableBuffer arguments)
        {
            Connection_StartResult result;
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

            connection_ProtocolHeader.SetResult(result);
        }

        struct Connection_TuneResult
        {
            public ushort ChannelMax;
            public uint FrameMax;
            public ushort Heartbeat;
        }

        void Handle_Connection_Tune(ReadableBuffer arguments)
        {
            Connection_TuneResult result;

            result.ChannelMax = arguments.ReadBigEndian<ushort>();
            arguments = arguments.Slice(sizeof(ushort));

            result.FrameMax = arguments.ReadBigEndian<uint>();
            arguments = arguments.Slice(sizeof(uint));

            result.Heartbeat = arguments.ReadBigEndian<ushort>();

            connection_StartOk.SetResult(result);
        }

        void Handle_Connection_OpenOk()
        {
            connection_OpenOk.SetResult(true);
        }

        void Handle_Connection_CloseOk()
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

            var payloadSizeBookmark = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

            buffer.WriteBigEndian(Command.Connection.ClassId);
            buffer.WriteBigEndian(Command.Connection.StartOk);
            buffer.WriteTable(new byte[0]); //client-properties
            buffer.WriteShortString("PLAIN"); //mechanism
            buffer.WriteLongString($"\0{userName}\0{password}"); //response
            buffer.WriteShortString("en_US"); //locale

            var payloadSize = (uint)buffer.BytesWritten - FrameHeaderSize;
            payloadSizeBookmark.WriteBigEndian(payloadSize);

            buffer.WriteBigEndian(FrameEnd);

            buffer.FlushAsync();

            return connection_StartOk.Task;
        }

        async Task Send_Connection_TuneOk(ushort channelMax, uint frameMax, ushort heartbeat)
        {
            var buffer = connection.Output.Alloc();

            var payloadSizeBookmark = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

            buffer.WriteBigEndian(Command.Connection.ClassId);
            buffer.WriteBigEndian(Command.Connection.TuneOk);
            buffer.WriteBigEndian(channelMax);
            buffer.WriteBigEndian(frameMax);
            buffer.WriteBigEndian(heartbeat);

            var payloadSize = (uint)buffer.BytesWritten - FrameHeaderSize;
            payloadSizeBookmark.WriteBigEndian(payloadSize);

            buffer.WriteBigEndian(FrameEnd);

            await buffer.FlushAsync();

            return;
        }

        TaskCompletionSource<bool> connection_OpenOk;
        Task<bool> Send_Connection_Open()
        {
            connection_OpenOk = new TaskCompletionSource<bool>();

            var buffer = connection.Output.Alloc();

            var payloadSizeBookmark = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

            buffer.WriteBigEndian(Command.Connection.ClassId);
            buffer.WriteBigEndian(Command.Connection.Open);
            buffer.WriteShortString(virtualHost);
            buffer.WriteBigEndian(Reserved);
            buffer.WriteBigEndian(Reserved);

            var payloadSize = (uint)buffer.BytesWritten - FrameHeaderSize;
            payloadSizeBookmark.WriteBigEndian(payloadSize);

            buffer.WriteBigEndian(FrameEnd);

            buffer.FlushAsync();

            return connection_OpenOk.Task;
        }

        TaskCompletionSource<bool> connection_CloseOk;
        Task Send_Connection_Close(ushort replyCode = ConnectionReplyCode.Success, string replyText = "Goodbye", ushort failingClass = 0, ushort failingMethod = 0)
        {
            connection_CloseOk = new TaskCompletionSource<bool>();

            var buffer = connection.Output.Alloc();

            var payloadSizeBookmark = buffer.WriteFrameHeader(FrameType.Method, connectionChannelNumber);

            buffer.WriteBigEndian(Command.Connection.ClassId);
            buffer.WriteBigEndian(Command.Connection.Close);
            buffer.WriteBigEndian(replyCode);
            buffer.WriteShortString(replyText);
            buffer.WriteBigEndian(failingClass);
            buffer.WriteBigEndian(failingMethod);

            var payloadSize = (uint)buffer.BytesWritten - FrameHeaderSize;
            payloadSizeBookmark.WriteBigEndian(payloadSize);

            buffer.WriteBigEndian(FrameEnd);

            buffer.FlushAsync();

            return connection_CloseOk.Task;
        }
    }
}

