using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    public class Exchange
    {
        readonly ushort channelNumber;
        readonly Socket socket;
        readonly Func<uint, object, Action<object, ReadableBuffer, Exception>, Task> SetExpectedReplyMethod;
        readonly Action ThrowIfClosed;

        readonly Action<object, ReadableBuffer, Exception> handle_DeclareOk;
        readonly Action<object, ReadableBuffer, Exception> handle_DeleteOk;
        readonly Action<object, ReadableBuffer, Exception> handle_BindOk;
        readonly Action<object, ReadableBuffer, Exception> handle_UnbindOk;

        internal Exchange(ushort channelNumber, Socket socket, Func<uint, object, Action<object, ReadableBuffer, Exception>, Task> setExpectedReplyMethod, Action throwIfClosed)
        {
            this.channelNumber = channelNumber;
            this.socket = socket;
            SetExpectedReplyMethod = setExpectedReplyMethod;
            ThrowIfClosed = throwIfClosed;

            handle_DeclareOk = Handle_DeclareOk;
            handle_DeleteOk = Handle_DeleteOk;
            handle_BindOk = Handle_BindOk;
            handle_UnbindOk = Handle_UnbindOk;
        }

        void Handle_DeclareOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var declareOk = (TaskCompletionSource<bool>)tcs;

            if (exception != null)
            {
                declareOk.SetException(exception);
            }
            else
            {
                declareOk.SetResult(true);
            }
        }

        void Handle_DeleteOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var deleteOk = (TaskCompletionSource<bool>)tcs;

            if (exception != null)
            {
                deleteOk.SetException(exception);
            }
            else
            {
                deleteOk.SetResult(true);
            }
        }

        void Handle_BindOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var bindOk = (TaskCompletionSource<bool>)tcs;

            if (exception != null)
            {
                bindOk.SetException(exception);
            }
            else
            {
                bindOk.SetResult(true);
            }
        }

        void Handle_UnbindOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var unbindOk = (TaskCompletionSource<bool>)tcs;

            if (exception != null)
            {
                unbindOk.SetException(exception);
            }
            else
            {
                unbindOk.SetResult(true);
            }
        }

        public async Task Declare(string exchangeName, string type, bool passive, bool durable, bool autoDelete, bool @internal, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            var declareOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Exchange.DeclareOk, declareOk, handle_DeclareOk);

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
            ThrowIfClosed();

            var deleteOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Exchange.DeleteOk, deleteOk, handle_DeleteOk);

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
            ThrowIfClosed();

            var bindOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Exchange.BindOk, bindOk, handle_BindOk);

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
            ThrowIfClosed();

            var unbindOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Exchange.UnbindOk, unbindOk, handle_UnbindOk);

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
