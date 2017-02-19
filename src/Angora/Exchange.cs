using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

using static RabbitMQClient.AmqpConstants;

namespace RabbitMQClient
{
    public class Exchange
    {
        readonly ushort channelNumber;
        readonly Socket socket;
        readonly SemaphoreSlim pendingReply;
        readonly Action<uint, Action<Exception>> SetExpectedReplyMethod;

        TaskCompletionSource<bool> declareOk;
        TaskCompletionSource<bool> deleteOk;
        TaskCompletionSource<bool> bindOk;
        TaskCompletionSource<bool> unbindOk;

        internal Exchange(ushort channelNumber, Socket socket, SemaphoreSlim pendingReply, Action<uint, Action<Exception>> setExpectedReplyMethod)
        {
            this.channelNumber = channelNumber;
            this.socket = socket;
            this.pendingReply = pendingReply;
            SetExpectedReplyMethod = setExpectedReplyMethod;
        }

        internal void HandleIncomingMethod(uint method, ReadableBuffer arguments)
        {
            switch (method)
            {
                case Method.Exchange.DeclareOk:
                    Handle_DeclareOk();
                    break;
                case Method.Exchange.DeleteOk:
                    Handle_DeleteOk();
                    break;
                case Method.Exchange.BindOk:
                    Handle_BindOk();
                    break;
                case Method.Exchange.UnbindOk:
                    Handle_UnbindOk();
                    break;
            }
        }

        void Handle_DeclareOk()
        {
            declareOk.SetResult(true);
        }

        void Handle_DeleteOk()
        {
            deleteOk.SetResult(true);
        }

        void Handle_BindOk()
        {
            bindOk.SetResult(true);
        }

        void Handle_UnbindOk()
        {
            unbindOk.SetResult(true);
        }

        public async Task Declare(string exchangeName, string type, bool passive, bool durable, bool autoDelete, bool @internal, Dictionary<string, object> arguments)
        {
            await pendingReply.WaitAsync();

            declareOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Method.Exchange.DeclareOk, ex => declareOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Exchange.Declare);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(exchangeName);
                buffer.WriteShortString(type);
                buffer.WriteBits(passive, durable, autoDelete, @internal);
                buffer.WriteTable(arguments);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            await declareOk.Task;
        }


        public async Task Delete(string exchange, bool onlyIfUnused)
        {
            await pendingReply.WaitAsync();

            deleteOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Method.Exchange.DeleteOk, ex => deleteOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Exchange.Delete);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(exchange);
                buffer.WriteBits(onlyIfUnused);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            await deleteOk.Task;
        }


        public async Task Bind(string source, string destination, string routingKey, Dictionary<string, object> arguments)
        {
            await pendingReply.WaitAsync();

            bindOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Method.Exchange.BindOk, ex => bindOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Exchange.Bind);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(source);
                buffer.WriteShortString(destination);
                buffer.WriteShortString(routingKey);
                buffer.WriteBits();
                buffer.WriteTable(arguments);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            await bindOk.Task;
        }


        public async Task Unbind(string source, string destination, string routingKey, Dictionary<string, object> arguments)
        {
            await pendingReply.WaitAsync();

            unbindOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Method.Exchange.UnbindOk, ex => unbindOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Exchange.Unbind);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(source);
                buffer.WriteShortString(destination);
                buffer.WriteShortString(routingKey);
                buffer.WriteBits();
                buffer.WriteTable(arguments);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            await unbindOk.Task;
        }
    }
}
