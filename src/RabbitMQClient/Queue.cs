using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

using static RabbitMQClient.AmqpConstants;

namespace RabbitMQClient
{
    public class Queue
    {
        readonly ushort channelNumber;
        readonly Socket socket;
        readonly SemaphoreSlim pendingReply;
        readonly Action<uint, Action<Exception>> SetExpectedReplyMethod;

        TaskCompletionSource<DeclareResult> declareOk;
        TaskCompletionSource<bool> bindOk;
        TaskCompletionSource<bool> unbindOk;
        TaskCompletionSource<uint> purgeOk;
        TaskCompletionSource<uint> deleteOk;

        internal Queue(ushort channelNumber, Socket socket, SemaphoreSlim pendingReply, Action<uint, Action<Exception>> setExpectedReplyMethod)
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
                case Method.Queue.DeclareOk:
                    Handle_DeclareOk(arguments);
                    break;
                case Method.Queue.BindOk:
                    Handle_BindOk();
                    break;
                case Method.Queue.UnbindOk:
                    Handle_UnbindOk();
                    break;
                case Method.Queue.PurgeOk:
                    Handle_PurgeOk(arguments);
                    break;
                case Method.Queue.DeleteOk:
                    Handle_DeleteOk(arguments);
                    break;
            }
        }

        public struct DeclareResult
        {
            public string QueueName;
            public uint MessageCount;
            public uint ConsumerCount;
        }

        void Handle_DeclareOk(ReadableBuffer arguments)
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

        void Handle_BindOk()
        {
            bindOk.SetResult(true);
        }

        void Handle_UnbindOk()
        {
            unbindOk.SetResult(true);
        }

        void Handle_PurgeOk(ReadableBuffer arguments)
        {
            var messageCount = arguments.ReadBigEndian<uint>();

            purgeOk.SetResult(messageCount);
        }

        void Handle_DeleteOk(ReadableBuffer arguments)
        {
            var messageCount = arguments.ReadBigEndian<uint>();

            deleteOk.SetResult(messageCount);
        }

        public async Task<DeclareResult> Declare(string queueName, bool passive, bool durable, bool exclusive, bool autoDelete, Dictionary<string, object> arguments)
        {
            await pendingReply.WaitAsync();

            declareOk = new TaskCompletionSource<DeclareResult>();
            SetExpectedReplyMethod(Method.Queue.DeclareOk, ex => declareOk.SetException(ex));

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
            await pendingReply.WaitAsync();

            bindOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Method.Queue.BindOk, ex => bindOk.SetException(ex));

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
            await pendingReply.WaitAsync();

            unbindOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Method.Queue.UnbindOk, ex => unbindOk.SetException(ex));

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
            await pendingReply.WaitAsync();

            purgeOk = new TaskCompletionSource<uint>();
            SetExpectedReplyMethod(Method.Queue.PurgeOk, ex => purgeOk.SetException(ex));

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
            await pendingReply.WaitAsync();

            deleteOk = new TaskCompletionSource<uint>();
            SetExpectedReplyMethod(Method.Queue.DeleteOk, ex => deleteOk.SetException(ex));

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
