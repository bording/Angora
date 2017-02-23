using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    public class Queue
    {
        readonly ushort channelNumber;
        readonly Socket socket;
        readonly Func<uint, object, Action<object, ReadableBuffer, Exception>, Task> SetExpectedReplyMethod;
        readonly Action ThrowIfClosed;

        readonly Action<object, ReadableBuffer, Exception> handle_DeclareOk;
        readonly Action<object, ReadableBuffer, Exception> handle_BindOk;
        readonly Action<object, ReadableBuffer, Exception> handle_UnbindOk;
        readonly Action<object, ReadableBuffer, Exception> handle_PurgeOk;
        readonly Action<object, ReadableBuffer, Exception> handle_DeleteOk;

        internal Queue(ushort channelNumber, Socket socket, Func<uint, object, Action<object, ReadableBuffer, Exception>, Task> setExpectedReplyMethod, Action throwIfClosed)
        {
            this.channelNumber = channelNumber;
            this.socket = socket;
            SetExpectedReplyMethod = setExpectedReplyMethod;
            ThrowIfClosed = throwIfClosed;

            handle_DeclareOk = Handle_DeclareOk;
            handle_BindOk = Handle_BindOk;
            handle_UnbindOk = Handle_UnbindOk;
            handle_PurgeOk = Handle_PurgeOk;
            handle_DeleteOk = Handle_DeleteOk;
        }

        public struct DeclareResult
        {
            public string QueueName;
            public uint MessageCount;
            public uint ConsumerCount;
        }

        void Handle_DeclareOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var declareOk = (TaskCompletionSource<DeclareResult>)tcs;

            if (exception != null)
            {
                declareOk.SetException(exception);
            }
            else
            {
                DeclareResult result;
                ReadCursor cursor;

                (result.QueueName, cursor) = arguments.ReadShortString();
                arguments = arguments.Slice(cursor);

                result.MessageCount = arguments.ReadBigEndian<uint>();
                arguments = arguments.Slice(sizeof(uint));

                result.ConsumerCount = arguments.ReadBigEndian<uint>();

                declareOk.SetResult(result);
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

        void Handle_PurgeOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var purgeOk = (TaskCompletionSource<uint>)tcs;

            if (exception != null)
            {
                purgeOk.SetException(exception);
            }
            else
            {
                var messageCount = arguments.ReadBigEndian<uint>();
                purgeOk.SetResult(messageCount);
            }
        }

        void Handle_DeleteOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var deleteOk = (TaskCompletionSource<uint>)tcs;

            if (exception != null)
            {
                deleteOk.SetException(exception);
            }
            else
            {
                var messageCount = arguments.ReadBigEndian<uint>();
                deleteOk.SetResult(messageCount);
            }
        }

        public async Task<DeclareResult> Declare(string queueName, bool passive, bool durable, bool exclusive, bool autoDelete, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            var declareOk = new TaskCompletionSource<DeclareResult>();
            await SetExpectedReplyMethod(Method.Queue.DeclareOk, declareOk, handle_DeclareOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Queue.Declare);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queueName);
                buffer.WriteBits(passive, durable, exclusive, autoDelete);
                buffer.WriteTable(arguments);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            return await declareOk.Task;
        }

        public async Task Bind(string queue, string exchange, string routingKey, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            var bindOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Queue.BindOk, bindOk, handle_BindOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Queue.Bind);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteShortString(exchange);
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

        public async Task Unbind(string queue, string exchange, string routingKey, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            var unbindOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Queue.UnbindOk, unbindOk, handle_UnbindOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Queue.Unbind);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteShortString(exchange);
                buffer.WriteShortString(routingKey);
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

        public async Task<uint> Purge(string queue)
        {
            ThrowIfClosed();

            var purgeOk = new TaskCompletionSource<uint>();
            await SetExpectedReplyMethod(Method.Queue.PurgeOk, purgeOk, handle_PurgeOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Queue.Purge);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteBits();

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            return await purgeOk.Task;
        }

        public async Task<uint> Delete(string queue, bool onlyIfUnused, bool onlyIfEmpty)
        {
            ThrowIfClosed();

            var deleteOk = new TaskCompletionSource<uint>();
            await SetExpectedReplyMethod(Method.Queue.DeleteOk, deleteOk, handle_DeleteOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Queue.Delete);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteBits(onlyIfEmpty, onlyIfUnused);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            return await deleteOk.Task;
        }
    }
}
