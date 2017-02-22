using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    public class Basic
    {
        readonly ushort channelNumber;
        readonly Socket socket;
        readonly uint maxContentBodySize;
        readonly SemaphoreSlim pendingReply;
        readonly Action<uint, Action<Exception>> SetExpectedReplyMethod;
        readonly Action ThrowIfClosed;

        TaskCompletionSource<bool> qosOk;
        TaskCompletionSource<string> consumeOk;
        TaskCompletionSource<string> cancelOk;
        TaskCompletionSource<bool> recoverOk;

        internal Basic(ushort channelNumber, Socket socket, uint maxContentBodySize, SemaphoreSlim pendingReply, Action<uint, Action<Exception>> setExpectedReplyMethod, Action throwIfClosed)
        {
            this.channelNumber = channelNumber;
            this.socket = socket;
            this.maxContentBodySize = maxContentBodySize;
            this.pendingReply = pendingReply;
            SetExpectedReplyMethod = setExpectedReplyMethod;
            ThrowIfClosed = throwIfClosed;
        }

        internal void HandleIncomingMethod(uint method, ReadableBuffer arguments)
        {
            switch (method)
            {
                case Method.Basic.QosOk:
                    Handle_QosOk();
                    break;
                case Method.Basic.ConsumeOk:
                    Handle_ConsumeOk(arguments);
                    break;
                case Method.Basic.CancelOk:
                    Handle_CancelOk(arguments);
                    break;
                case Method.Basic.RecoverOk:
                    Handle_RecoverOk();
                    break;
            }
        }

        void Handle_QosOk()
        {
            qosOk.SetResult(true);
        }

        void Handle_ConsumeOk(ReadableBuffer arguments)
        {
            var consumerTag = arguments.ReadShortString();

            consumeOk.SetResult(consumerTag.value);
        }

        void Handle_CancelOk(ReadableBuffer arguments)
        {
            var consumerTag = arguments.ReadShortString();

            cancelOk.SetResult(consumerTag.value);
        }

        void Handle_RecoverOk()
        {
            recoverOk.SetResult(true);
        }

        public async Task Qos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            ThrowIfClosed();

            await pendingReply.WaitAsync();

            qosOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Method.Basic.QosOk, ex => qosOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Qos);
                buffer.WriteBigEndian(prefetchSize);
                buffer.WriteBigEndian(prefetchCount);
                buffer.WriteBits(global);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            await qosOk.Task;
        }

        public async Task<string> Consume(string queue, string consumerTag, bool autoAck, bool exclusive, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            await pendingReply.WaitAsync();

            consumeOk = new TaskCompletionSource<string>();
            SetExpectedReplyMethod(Method.Basic.ConsumeOk, ex => consumeOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Consume);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteShortString(consumerTag);
                buffer.WriteBits(false, autoAck, exclusive);
                buffer.WriteTable(arguments);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            return await consumeOk.Task;
        }

        public async Task<string> Cancel(string consumerTag)
        {
            ThrowIfClosed();

            await pendingReply.WaitAsync();

            cancelOk = new TaskCompletionSource<string>();
            SetExpectedReplyMethod(Method.Basic.CancelOk, ex => cancelOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Cancel);
                buffer.WriteShortString(consumerTag);
                buffer.WriteBits();

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            return await cancelOk.Task;
        }

        public async Task Recover()
        {
            ThrowIfClosed();

            await pendingReply.WaitAsync();

            recoverOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Method.Basic.RecoverOk, ex => recoverOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Recover);
                buffer.WriteBits(true);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }

            await recoverOk.Task;
        }

        public async Task Publish(string exchange, string routingKey, bool mandatory, MessageProperties properties, Span<byte> body)
        {
            ThrowIfClosed();

            await pendingReply.WaitAsync();

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Method.Basic.Publish);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(exchange);
                buffer.WriteShortString(routingKey);
                buffer.WriteBits(mandatory);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);
                buffer.WriteBigEndian(FrameEnd);

                WriteContentHeaderFrame(ref buffer, properties, (ulong)body.Length);

                var framesToWrite = body.Length > 0;

                while (framesToWrite)
                {
                    Span<byte> frame;

                    if (body.Length > maxContentBodySize)
                    {
                        frame = body.Slice(0, (int)maxContentBodySize);
                        body = body.Slice((int)maxContentBodySize);
                    }
                    else
                    {
                        frame = body;
                        framesToWrite = false;
                    }

                    WriteContentBodyFrame(ref buffer, frame);
                }

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
                pendingReply.Release(); //TODO this won't get called if GetWriteBuffer throws
            }
        }

        void WriteContentHeaderFrame(ref WritableBuffer buffer, MessageProperties properties, ulong length)
        {
            var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.ContentHeader, channelNumber);

            var bytesWrittenBefore = (uint)buffer.BytesWritten;

            buffer.WriteBigEndian(ClassId.Basic);
            buffer.WriteBigEndian(Reserved);
            buffer.WriteBigEndian(Reserved);
            buffer.WriteBigEndian(length);
            buffer.WriteBasicProperties(properties);

            payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - bytesWrittenBefore);

            buffer.WriteBigEndian(FrameEnd);
        }

        void WriteContentBodyFrame(ref WritableBuffer buffer, Span<byte> body)
        {
            var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.ContentBody, channelNumber);

            var bytesWrittenBefore = (uint)buffer.BytesWritten;

            buffer.Write(body);

            payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - bytesWrittenBefore);

            buffer.WriteBigEndian(FrameEnd);
        }
    }
}
