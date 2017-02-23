using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    public class Basic
    {
        readonly ushort channelNumber;
        readonly Socket socket;
        readonly uint maxContentBodySize;
        readonly Func<uint, object, Action<object, ReadableBuffer, Exception>, Task> SetExpectedReplyMethod;
        readonly Action ThrowIfClosed;

        readonly Action<object, ReadableBuffer, Exception> handle_QosOk;
        readonly Action<object, ReadableBuffer, Exception> handle_ConsumeOk;
        readonly Action<object, ReadableBuffer, Exception> handle_CancelOk;
        readonly Action<object, ReadableBuffer, Exception> handle_RecoverOk;

        internal Basic(ushort channelNumber, Socket socket, uint maxContentBodySize, Func<uint, object, Action<object, ReadableBuffer, Exception>, Task> setExpectedReplyMethod, Action throwIfClosed)
        {
            this.channelNumber = channelNumber;
            this.socket = socket;
            this.maxContentBodySize = maxContentBodySize;
            SetExpectedReplyMethod = setExpectedReplyMethod;
            ThrowIfClosed = throwIfClosed;

            handle_QosOk = Handle_QosOk;
            handle_ConsumeOk = Handle_ConsumeOk;
            handle_CancelOk = Handle_CancelOk;
            handle_RecoverOk = Handle_RecoverOk;
        }

        void Handle_QosOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var qosOk = (TaskCompletionSource<bool>)tcs;

            if (exception != null)
            {
                qosOk.SetException(exception);
            }
            else
            {
                qosOk.SetResult(true);
            }
        }

        void Handle_ConsumeOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var consumeOk = (TaskCompletionSource<string>)tcs;

            if (exception != null)
            {
                consumeOk.SetException(exception);
            }
            else
            {
                var consumerTag = arguments.ReadShortString();
                consumeOk.SetResult(consumerTag.value);
            }
        }

        void Handle_CancelOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var cancelOk = (TaskCompletionSource<string>)tcs;

            if (exception != null)
            {
                cancelOk.SetException(exception);
            }
            else
            {
                var consumerTag = arguments.ReadShortString();
                cancelOk.SetResult(consumerTag.value);
            }
        }

        void Handle_RecoverOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var recoverOk = (TaskCompletionSource<bool>)tcs;

            if (exception != null)
            {
                recoverOk.SetException(exception);
            }
            else
            {
                recoverOk.SetResult(true);
            }
        }

        public async Task Qos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            ThrowIfClosed();

            var qosOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Basic.QosOk, qosOk, handle_QosOk);

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

            var consumeOk = new TaskCompletionSource<string>();
            await SetExpectedReplyMethod(Method.Basic.ConsumeOk, consumeOk, handle_ConsumeOk);

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

            var cancelOk = new TaskCompletionSource<string>();
            await SetExpectedReplyMethod(Method.Basic.CancelOk, cancelOk, handle_CancelOk);

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

            var recoverOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Basic.RecoverOk, recoverOk, handle_RecoverOk);

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
