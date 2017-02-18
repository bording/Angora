using System;
using System.Binary;
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

        bool replyIsExpected;
        (ushort classId, ushort methodId) expectedMethod;
        Action<Exception> expectedMethodError;

        TaskCompletionSource<bool> openOk;
        TaskCompletionSource<bool> closeOk;

        internal Channel(Socket socket, ushort channelNumber)
        {
            this.socket = socket;
            ChannelNumber = channelNumber;

            semaphore = new SemaphoreSlim(1, 1);

            Exchange = new Exchange(channelNumber, socket, semaphore, SetExpectedReplyMethod);
            Queue = new Queue(channelNumber, socket, semaphore, SetExpectedReplyMethod);
            Basic = new Basic(channelNumber, socket, semaphore, SetExpectedReplyMethod);
        }

        void SetExpectedReplyMethod((ushort classId, ushort methodId) method, Action<Exception> error)
        {
            expectedMethod = method;
            expectedMethodError = error;

            replyIsExpected = true;
        }

        internal void HandleIncomingMethod((ushort classId, ushort methodId) method, ReadableBuffer arguments)
        {
            try
            {
                if (replyIsExpected && !method.Equals(expectedMethod))
                {
                    expectedMethodError(new Exception($"Expected reply method {expectedMethod}. Received {method}."));

                    // TODO send channel close here with error
                    return;
                }

                switch (method.classId)
                {
                    case Command.Channel.ClassId:
                        HandleIncomingMethod(method.methodId, arguments);
                        break;

                    case Command.Exchange.ClassId:
                        Exchange.HandleIncomingMethod(method.methodId, arguments);
                        break;

                    case Command.Queue.ClassId:
                        Queue.HandleIncomingMethod(method.methodId, arguments);
                        break;

                    case Command.Basic.ClassId:
                        Basic.HandleIncomingMethod(method.methodId, arguments);
                        break;
                }
            }
            finally
            {
                if (replyIsExpected)
                {
                    replyIsExpected = false;
                    semaphore.Release();
                }
            }
        }

        void HandleIncomingMethod(ushort methodId, ReadableBuffer arguments)
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
            SetExpectedReplyMethod((Command.Channel.ClassId, Command.Channel.OpenOk), ex => openOk.SetException(ex));

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
            SetExpectedReplyMethod((Command.Channel.ClassId, Command.Channel.CloseOk), ex => closeOk.SetException(ex));

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
