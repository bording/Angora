using System;
using System.Binary;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    public class Channel
    {
        public ushort ChannelNumber { get; }

        public Exchange Exchange { get; }

        public Queue Queue { get; }

        public Basic Basic { get; }

        readonly Socket socket;
        readonly SemaphoreSlim pendingReply;

        bool replyIsExpected;
        uint expectedMethod;
        Action<Exception> expectedMethodError;

        TaskCompletionSource<bool> openOk;
        TaskCompletionSource<bool> closeOk;

        internal Channel(Socket socket, ushort channelNumber)
        {
            this.socket = socket;
            ChannelNumber = channelNumber;

            pendingReply = new SemaphoreSlim(1, 1);

            Exchange = new Exchange(channelNumber, socket, pendingReply, SetExpectedReplyMethod);
            Queue = new Queue(channelNumber, socket, pendingReply, SetExpectedReplyMethod);
            Basic = new Basic(channelNumber, socket, pendingReply, SetExpectedReplyMethod);
        }

        void SetExpectedReplyMethod(uint method, Action<Exception> error)
        {
            expectedMethod = method;
            expectedMethodError = error;

            replyIsExpected = true;
        }

        public void Handle_Connection_Close(ushort replyCode, string replyText, uint method)
        {
            if (replyIsExpected)
            {
                var classId = method >> 16;
                var methodId = method << 16 >> 16;

                expectedMethodError(new Exception($"Connection Closed: {replyCode} {replyText}. ClassId: {classId} MethodId: {methodId}"));

                replyIsExpected = false;
                pendingReply.Release();
            }
        }

        internal void HandleIncomingMethod(uint method, ReadableBuffer arguments)
        {
            try
            {
                if (replyIsExpected && method != expectedMethod)
                {
                    expectedMethodError(new Exception($"Expected reply method {expectedMethod}. Received {method}."));

                    // TODO send channel close here with error
                    return;
                }

                var classId = method >> 16;

                switch (classId)
                {
                    case ClassId.Channel:
                        HandleIncomingChannelMethod(method, arguments);
                        break;

                    case ClassId.Exchange:
                        Exchange.HandleIncomingMethod(method, arguments);
                        break;

                    case ClassId.Queue:
                        Queue.HandleIncomingMethod(method, arguments);
                        break;

                    case ClassId.Basic:
                        Basic.HandleIncomingMethod(method, arguments);
                        break;
                }
            }
            finally
            {
                if (replyIsExpected)
                {
                    replyIsExpected = false;
                    pendingReply.Release();
                }
            }
        }

        void HandleIncomingChannelMethod(uint method, ReadableBuffer arguments)
        {
            switch (method)
            {
                case Method.Channel.OpenOk:
                    Handle_OpenOk();
                    break;
                case Method.Channel.CloseOk:
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
            await pendingReply.WaitAsync();

            openOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Method.Channel.OpenOk, ex => openOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Method.Channel.Open);
                buffer.WriteBigEndian(Reserved);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            await openOk.Task;
        }

        public async Task Close(ushort replyCode = ChannelReplyCode.Success, string replyText = "Goodbye", ushort failingClass = 0, ushort failingMethod = 0)
        {
            await pendingReply.WaitAsync();

            closeOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Method.Channel.CloseOk, ex => closeOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Method.Channel.Close);
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

            await closeOk.Task;
        }
    }
}
