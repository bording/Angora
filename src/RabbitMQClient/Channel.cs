using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static RabbitMQClient.AmqpConstants;

namespace RabbitMQClient
{
    public class Channel
    {
        public ushort ChannelNumber { get; }

        readonly IPipeWriter writer;

        TaskCompletionSource<bool> channel_OpenOk;
        TaskCompletionSource<bool> queue_DeclareOk;

        SemaphoreSlim semaphore;
        ushort expectedMethodId;

        public Channel(IPipeWriter writer, ushort channelNumber)
        {
            this.writer = writer;
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
                    Handle_OpenOk(arguments);
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

            var buffer = writer.Alloc();

            uint payloadSize = (uint)2 + 2 + 1;

            buffer.WriteBigEndian(FrameType.Method);
            buffer.WriteBigEndian(ChannelNumber);
            buffer.WriteBigEndian(payloadSize);
            buffer.WriteBigEndian(Command.Channel.ClassId);
            buffer.WriteBigEndian(Command.Channel.Open);
            buffer.WriteBigEndian(Reserved);
            buffer.WriteBigEndian(FrameEnd);

            buffer.FlushAsync().Ignore();

            await channel_OpenOk.Task;
        }

        internal void Handle_OpenOk(ReadableBuffer arguments)
        {
            channel_OpenOk.SetResult(true);
        }

        public async Task QueueDeclare(string queueName, bool passive, bool durable, bool exclusive, bool autoDelete, bool noWait, byte[] arguments)
        {
            await semaphore.WaitAsync();

            queue_DeclareOk = new TaskCompletionSource<bool>();
            expectedMethodId = Command.Queue.DeclareOk;

            var buffer = writer.Alloc();

            var queueNameBytes = Encoding.UTF8.GetBytes(queueName);
            var queueNameLength = (byte)queueNameBytes.Length;

            var argumentsLength = (uint)arguments.Length;

            byte bitField = 2; //durable == true only

            uint payloadSize = (uint)2 + 2 + 1 + 1 + 1 + queueNameLength + 1 + 4 + argumentsLength;

            buffer.WriteBigEndian(FrameType.Method);
            buffer.WriteBigEndian(ChannelNumber);
            buffer.WriteBigEndian(payloadSize);
            buffer.WriteBigEndian(Command.Queue.ClassId);
            buffer.WriteBigEndian(Command.Queue.Declare);
            buffer.WriteBigEndian(Reserved);
            buffer.WriteBigEndian(Reserved);
            buffer.WriteBigEndian(queueNameLength);
            buffer.Write(queueNameBytes);
            buffer.WriteBigEndian(bitField);
            buffer.WriteBigEndian(argumentsLength);
            buffer.Write(arguments);
            buffer.WriteBigEndian(FrameEnd);

            buffer.FlushAsync().Ignore();

            await queue_DeclareOk.Task;
        }

        internal void Handle_DeclareOk(ReadableBuffer arguments)
        {
            queue_DeclareOk.SetResult(true);
        }
    }
}
