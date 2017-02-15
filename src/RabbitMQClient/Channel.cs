using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

using static RabbitMQClient.AmqpConstants;

namespace RabbitMQClient
{
    public class Channel
    {
        public ushort ChannelNumber { get; }

        readonly Socket socket;

        TaskCompletionSource<bool> channel_OpenOk;
        TaskCompletionSource<bool> queue_DeclareOk;

        SemaphoreSlim semaphore;
        ushort expectedMethodId;

        internal Channel(Socket socket, ushort channelNumber)
        {
            this.socket = socket;
            ChannelNumber = channelNumber;

            semaphore = new SemaphoreSlim(1, 1);
        }

        internal void ParseMethod(ushort classId, ushort methodId, ReadableBuffer arguments)
        {
            try
            {
                if (methodId != expectedMethodId)
                {
                    throw new Exception(); // and other appropriate stuff
                }

                switch (classId)
                {
                    case Command.Channel.ClassId:
                        ParseChannelMethod(methodId, arguments);
                        break;

                    case Command.Queue.ClassId:
                        ParseQueueMethod(methodId, arguments);
                        break;
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        internal void ParseChannelMethod(ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Channel.OpenOk:
                    Handle_OpenOk();
                    break;
            }
        }

        internal void ParseQueueMethod(ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Queue.DeclareOk:
                    Handle_DeclareOk(arguments);
                    break;
            }
        }

        internal async Task Open()
        {
            await semaphore.WaitAsync();

            channel_OpenOk = new TaskCompletionSource<bool>();
            expectedMethodId = Command.Channel.OpenOk;

            var buffer = await socket.GetWriteBufferAsync();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Command.Channel.ClassId);
                buffer.WriteBigEndian(Command.Channel.Open);
                buffer.WriteBigEndian(Reserved);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                await channel_OpenOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        internal void Handle_OpenOk()
        {
            channel_OpenOk.SetResult(true);
        }

        public async Task QueueDeclare(string queueName, bool passive, bool durable, bool exclusive, bool autoDelete, bool noWait, Dictionary<string, object> arguments)
        {
            await semaphore.WaitAsync();

            var buffer = await socket.GetWriteBufferAsync();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Command.Queue.ClassId);
                buffer.WriteBigEndian(Command.Queue.Declare);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queueName);
                buffer.WriteBits(passive, durable, exclusive, autoDelete, noWait);
                buffer.WriteTable(arguments);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                if (noWait)
                {
                    await buffer.FlushAsync();
                    semaphore.Release();
                }
                else
                {
                    queue_DeclareOk = new TaskCompletionSource<bool>();
                    expectedMethodId = Command.Queue.DeclareOk;

                    await buffer.FlushAsync();

                    await queue_DeclareOk.Task;
                }
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        internal void Handle_DeclareOk(ReadableBuffer arguments)
        {
            //TODO this has arguments to return
            queue_DeclareOk.SetResult(true);
        }
    }
}
