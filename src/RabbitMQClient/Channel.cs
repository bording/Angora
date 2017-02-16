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

        public Exchange Exchange { get; }

        public Queue Queue { get; }

        public Basic Basic { get; }

        readonly Socket socket;
        readonly SemaphoreSlim semaphore;

        ushort expectedMethodId;

        TaskCompletionSource<bool> openOk;
        TaskCompletionSource<bool> closeOk;

        internal Channel(Socket socket, ushort channelNumber)
        {
            this.socket = socket;
            ChannelNumber = channelNumber;

            semaphore = new SemaphoreSlim(1, 1);

            Exchange = new Exchange(channelNumber, socket, semaphore, SetExpectedMethodId);
            Queue = new Queue(channelNumber, socket, semaphore, SetExpectedMethodId);
            Basic = new Basic(channelNumber, socket, semaphore, SetExpectedMethodId);
        }

        void SetExpectedMethodId(ushort methodId)
        {
            expectedMethodId = methodId;
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

                    case Command.Exchange.ClassId:
                        Exchange.ParseExchangeMethod(methodId, arguments);
                        break;

                    case Command.Queue.ClassId:
                        Queue.ParseQueueMethod(methodId, arguments);
                        break;

                    case Command.Basic.ClassId:
                        Basic.RouteMethod(methodId, arguments);
                        break;
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        void ParseChannelMethod(ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Channel.OpenOk:
                    Handle_OpenOk();
                    break;
                case Command.Channel.CloseOk:
                    Handle_CloseOk();
                    break;
            }
        }

        void Handle_OpenOk()
        {
            openOk.SetResult(true);
        }

        void Handle_CloseOk()
        {
            closeOk.SetResult(true);
        }

        internal async Task Open()
        {
            await semaphore.WaitAsync();

            openOk = new TaskCompletionSource<bool>();
            expectedMethodId = Command.Channel.OpenOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Command.Channel.ClassId);
                buffer.WriteBigEndian(Command.Channel.Open);
                buffer.WriteBigEndian(Reserved);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                await openOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Close(ushort replyCode = ChannelReplyCode.Success, string replyText = "Goodbye", ushort failingClass = 0, ushort failingMethod = 0)
        {
            await semaphore.WaitAsync();

            closeOk = new TaskCompletionSource<bool>();
            expectedMethodId = Command.Channel.CloseOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Command.Channel.ClassId);
                buffer.WriteBigEndian(Command.Channel.Close);
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
