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
        readonly SemaphoreSlim pendingReply;
        readonly Action<uint, Action<Exception>> SetExpectedReplyMethod;

        TaskCompletionSource<bool> qosOk;
        TaskCompletionSource<string> consumeOk;
        TaskCompletionSource<string> cancelOk;
        TaskCompletionSource<bool> recoverOk;

        internal Basic(ushort channelNumber, Socket socket, SemaphoreSlim pendingReply, Action<uint, Action<Exception>> setExpectedReplyMethod)
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

                //ContentHeader
                payloadSizeHeader = buffer.WriteFrameHeader(FrameType.ContentHeader, channelNumber);

                var bytesWrittenBefore = (uint)buffer.BytesWritten;

                buffer.WriteBigEndian(ClassId.Basic);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Convert.ToUInt64(body.Length));
                buffer.WriteBasicProperties(properties);

                var contentHeaderSize = (uint)buffer.BytesWritten - bytesWrittenBefore;
                payloadSizeHeader.WriteBigEndian(contentHeaderSize);
                buffer.WriteBigEndian(FrameEnd);

                //ContentBody
                //TODO this needs to respect max frame size and split into multiple frames

                payloadSizeHeader = buffer.WriteFrameHeader(FrameType.ContentBody, channelNumber);

                bytesWrittenBefore = (uint)buffer.BytesWritten;

                buffer.Write(body);

                var contentBodySize = (uint)buffer.BytesWritten - bytesWrittenBefore;
                payloadSizeHeader.WriteBigEndian(contentBodySize);
                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();
            }
            finally
            {
                socket.ReleaseWriteBuffer();
                pendingReply.Release(); //TODO this won't get called if GetWriteBuffer throws
            }
        }
    }
}
