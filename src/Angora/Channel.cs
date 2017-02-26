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

        public bool IsOpen { get; private set; }

        public Exchange Exchange { get; }

        public Queue Queue { get; }

        public Basic Basic { get; }

        readonly ChannelMethods methods;

        readonly SemaphoreSlim pendingReply;
        bool replyIsExpected;
        uint expectedReplyMethod;
        object replyTaskCompletionSource;
        Action<object, ReadableBuffer, Exception> replyHandler;

        Action<object, ReadableBuffer, Exception> handle_OpenOk;
        Action<object, ReadableBuffer, Exception> handle_CloseOk;

        internal Channel(Socket socket, uint maxContentBodySize, ushort channelNumber)
        {
            ChannelNumber = channelNumber;

            methods = new ChannelMethods(socket, channelNumber);

            pendingReply = new SemaphoreSlim(1, 1);

            Exchange = new Exchange(socket, channelNumber, SetExpectedReplyMethod, ThrowIfClosed);
            Queue = new Queue(socket, channelNumber, SetExpectedReplyMethod, ThrowIfClosed);
            Basic = new Basic(socket, channelNumber, maxContentBodySize, SetExpectedReplyMethod, ThrowIfClosed);

            handle_OpenOk = Handle_OpenOk;
            handle_CloseOk = Handle_CloseOk;
        }

        async Task SetExpectedReplyMethod(uint method, object taskCompletionSource, Action<object, ReadableBuffer, Exception> replyHandler)
        {
            await pendingReply.WaitAsync();

            expectedReplyMethod = method;
            replyTaskCompletionSource = taskCompletionSource;
            this.replyHandler = replyHandler;

            replyIsExpected = true;
        }

        void ThrowIfClosed()
        {
            if (!IsOpen)
            {
                throw new Exception("Channel is closed");
            }
        }

        internal async Task HandleIncomingMethod(uint method, ReadableBuffer arguments)
        {
            switch (method)
            {
                case Method.Channel.Close:
                    await Handle_Close(arguments);
                    break;
                case Method.Basic.Deliver:
                    await Basic.Handle_Deliver(arguments);
                    break;
                default:
                    HandleReplyMethod(method, arguments);
                    break;
            }
        }

        void HandleReplyMethod(uint method, ReadableBuffer arguments)
        {
            if (!replyIsExpected)
            {
                throw new Exception("reply received when not expecting one");
                //TODO send channel exception
            }

            Exception exception = null;

            if (method != expectedReplyMethod)
            {
                exception = new Exception($"Expected reply method {expectedReplyMethod}. Received {method}.");
            }

            replyHandler(replyTaskCompletionSource, arguments, exception);

            replyIsExpected = false;
            pendingReply.Release();
        }

        internal async Task Open()
        {
            var openOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Channel.OpenOk, openOk, handle_OpenOk);

            await methods.Send_Open();

            await openOk.Task;
        }

        void Handle_OpenOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var openOk = (TaskCompletionSource<bool>)tcs;

            if (exception != null)
            {
                openOk.SetException(exception);
            }
            else
            {
                IsOpen = true;
                openOk.SetResult(true);
            }
        }

        public async Task Close(ushort replyCode = ChannelReplyCode.Success, string replyText = "Goodbye", ushort failingClass = 0, ushort failingMethod = 0)
        {
            ThrowIfClosed();

            var closeOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Channel.CloseOk, closeOk, Handle_CloseOk);

            await methods.Send_Close(replyCode, replyText, failingClass, failingMethod);

            await closeOk.Task;
        }

        void Handle_CloseOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var closeOk = (TaskCompletionSource<bool>)tcs;

            if (exception != null)
            {
                closeOk.SetException(exception);
            }
            else
            {
                IsOpen = false;
                closeOk.SetResult(true);
            }
        }

        async Task Handle_Close(ReadableBuffer arguments)
        {
            IsOpen = false;

            await methods.Send_CloseOk();

            if (replyIsExpected)
            {
                var replyCode = arguments.ReadBigEndian<ushort>();
                arguments = arguments.Slice(sizeof(ushort));

                var (replyText, cursor) = arguments.ReadShortString();
                arguments = arguments.Slice(cursor);

                var method = arguments.ReadBigEndian<uint>();

                var classId = method >> 16;
                var methodId = method << 16 >> 16;

                var exception = new Exception($"Channel Closed: {replyCode} {replyText}. ClassId: {classId} MethodId: {methodId}");

                replyHandler(replyTaskCompletionSource, default(ReadableBuffer), exception);

                replyIsExpected = false;
                pendingReply.Release();
            }
        }

        internal void Handle_Connection_Close(ushort replyCode, string replyText, uint method)
        {
            IsOpen = false;

            if (replyIsExpected)
            {
                var classId = method >> 16;
                var methodId = method << 16 >> 16;

                var exception = new Exception($"Connection Closed: {replyCode} {replyText}. ClassId: {classId} MethodId: {methodId}");

                replyHandler(replyTaskCompletionSource, default(ReadableBuffer), exception);

                replyIsExpected = false;
                pendingReply.Release();
            }
        }
    }
}
