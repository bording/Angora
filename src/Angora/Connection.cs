using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    public class Connection
    {
        readonly ConnectionMethods methods;

        readonly string hostName;
        readonly string userName;
        readonly string password;
        readonly string virtualHost;
        readonly string connectionName;

        readonly Socket socket;

        readonly Dictionary<ushort, Channel> channels;

        readonly TaskCompletionSource<bool> openOk;
        readonly TaskCompletionSource<bool> closeOk;
        readonly TaskCompletionSource<(ushort channelMax, uint frameMax, uint heartbeatInterval)> readyToOpen;

        readonly CancellationTokenSource sendHeartbeats;
        readonly CancellationTokenSource readLoop;

        Task sendHeartbeatsTask;
        Task readLoopTask;

        ushort nextChannelNumber;

        public bool IsOpen { get; private set; }

        public ushort ChannelMax { get; private set; }

        public uint FrameMax { get; private set; }

        public uint HeartbeatInterval { get; private set; }

        internal Connection(string hostName, string userName, string password, string virtualHost, string connectionName)
        {
            this.hostName = hostName;
            this.userName = userName;
            this.password = password;
            this.virtualHost = virtualHost;
            this.connectionName = connectionName;

            socket = new Socket();

            methods = new ConnectionMethods(socket);

            channels = new Dictionary<ushort, Channel>();

            openOk = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            closeOk = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            readyToOpen = new TaskCompletionSource<(ushort channelMax, uint frameMax, uint heartbeatInterval)>(TaskCreationOptions.RunContinuationsAsynchronously);

            sendHeartbeats = new CancellationTokenSource();
            readLoop = new CancellationTokenSource();
        }

        internal async Task Connect()
        {
            var addresses = await Dns.GetHostAddressesAsync(hostName);
            var address = addresses.First();
            var endpoint = new IPEndPoint(address, 5672);

            await socket.Connect(endpoint);

            readLoopTask = Task.Run(() => ReadLoop(readLoop.Token));

            await methods.Send_ProtocolHeader();
            (ChannelMax, FrameMax, HeartbeatInterval) = await readyToOpen.Task;

            await methods.Send_Open(virtualHost);
            IsOpen = await openOk.Task;
        }

        public async Task<Channel> CreateChannel()
        {
            var channel = new Channel(socket, FrameMax - TotalFrameOverhead, ++nextChannelNumber);
            channels.Add(channel.ChannelNumber, channel);

            await channel.Open();

            return channel;
        }

        public Task Close()
        {
            return Close(true);
        }

        async Task Close(bool clientInitiated)
        {
            if (!IsOpen)
            {
                throw new Exception("already closed");
            }

            IsOpen = false;
            sendHeartbeats.Cancel();
            await sendHeartbeatsTask;

            if (clientInitiated)
            {
                await methods.Send_Close();
                await closeOk.Task;
            }
            else
            {
                await methods.Send_CloseOk();
            }

            readLoop.Cancel();
            socket.Input.CancelPendingRead();
            await readLoopTask;
            socket.Close();
        }

        async Task ReadLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                var readResult = await socket.Input.ReadAsync();
                var buffer = readResult.Buffer;

                if (buffer.IsEmpty && readResult.IsCompleted)
                {
                    break;
                }

                ReadCursor consumed = buffer.Start;

                while (!buffer.IsEmpty)
                {
                    if (buffer.Length < FrameHeaderSize)
                    {
                        break;
                    }

                    var frameType = buffer.ReadBigEndian<byte>();
                    buffer = buffer.Slice(sizeof(byte));

                    var channelNumber = buffer.ReadBigEndian<ushort>();
                    buffer = buffer.Slice(sizeof(ushort));

                    var payloadSize = buffer.ReadBigEndian<uint>();
                    buffer = buffer.Slice(sizeof(uint));

                    if (buffer.Length < payloadSize + 1)
                    {
                        break;
                    }

                    buffer = buffer.Slice(buffer.Start);

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
                        case FrameType.ContentHeader:
                        case FrameType.ContentBody:
                            await HandleIncomingContent(channelNumber, frameType, payload);
                            break;
                    }

                    consumed = buffer.Start;
                }

                socket.Input.Advance(consumed, buffer.End);
            }
        }

        async Task SendHeartbeats(ushort interval, CancellationToken token)
        {
            try
            {
                await Task.Delay(200, token);

                while (!token.IsCancellationRequested)
                {
                    await methods.Send_Heartbeat();

                    await Task.Delay(TimeSpan.FromSeconds(interval), token);
                }
            }
            catch (OperationCanceledException) { }
        }

        async Task HandleIncomingMethodFrame(ushort channelNumber, ReadableBuffer payload)
        {
            var method = payload.ReadBigEndian<uint>();
            payload = payload.Slice(sizeof(uint));

            var classId = method >> 16;

            if (classId == ClassId.Connection) //TODO validate channel 0
            {
                await HandleIncomingMethod(method, payload);
            }
            else
            {
                await channels[channelNumber].HandleIncomingMethod(method, payload);
            }
        }

        Task HandleIncomingContent(ushort channelNumber, byte frameType, ReadableBuffer payload)
        {
            return channels[channelNumber].HandleIncomingContent(frameType, payload);
        }

        async Task HandleIncomingMethod(uint method, ReadableBuffer arguments)
        {
            switch (method)
            {
                case Method.Connection.Start:
                    await Handle_Start(arguments);
                    break;

                case Method.Connection.Tune:
                    await Handle_Tune(arguments);
                    break;

                case Method.Connection.Close:
                    await Handle_Close(arguments);
                    break;

                case Method.Connection.OpenOk:
                    openOk.SetResult(true);
                    break;

                case Method.Connection.CloseOk:
                    closeOk.SetResult(true);
                    break;
            }
        }

        async Task Handle_Start(ReadableBuffer arguments)
        {
            var versionMajor = arguments.ReadBigEndian<byte>();
            arguments = arguments.Slice(sizeof(byte));

            var versionMinor = arguments.ReadBigEndian<byte>();
            arguments = arguments.Slice(sizeof(byte));

            var serverProperties = arguments.ReadTable();
            arguments = arguments.Slice(serverProperties.position);

            var mechanism = arguments.ReadLongString();
            arguments = arguments.Slice(mechanism.position);

            var locales = arguments.ReadLongString();

            await methods.Send_StartOk(connectionName, userName, password);
        }

        async Task Handle_Tune(ReadableBuffer arguments)
        {
            var channelMax = arguments.ReadBigEndian<ushort>();
            arguments = arguments.Slice(sizeof(ushort));

            var frameMax = arguments.ReadBigEndian<uint>();
            arguments = arguments.Slice(sizeof(uint));

            var heartbeat = arguments.ReadBigEndian<ushort>();

            sendHeartbeatsTask = Task.Run(() => SendHeartbeats(heartbeat, sendHeartbeats.Token));

            await methods.Send_TuneOk(channelMax, frameMax, heartbeat);

            readyToOpen.SetResult((channelMax, frameMax, heartbeat));
        }

        async Task Handle_Close(ReadableBuffer arguments)
        {
            var replyCode = arguments.ReadBigEndian<ushort>();
            arguments = arguments.Slice(sizeof(ushort));

            var (replyText, cursor) = arguments.ReadShortString();
            arguments = arguments.Slice(cursor);

            var method = arguments.ReadBigEndian<uint>();

            await Close(false);

            foreach (var channel in channels)
            {
                channel.Value.Handle_Connection_Close(replyCode, replyText, method);
            }
        }
    }
}

