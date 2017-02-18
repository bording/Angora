using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

using static RabbitMQClient.AmqpConstants;

namespace RabbitMQClient
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
            }
        }

        private void Handle_QosOk()
        {
            qosOk.SetResult(true);
        }

        private void Handle_ConsumeOk(ReadableBuffer arguments)
        {
            var consumerTag = arguments.ReadShortString();

            consumeOk.SetResult(consumerTag.value);
        }

        private void Handle_CancelOk(ReadableBuffer arguments)
        {
            var consumerTag = arguments.ReadShortString();

            cancelOk.SetResult(consumerTag.value);
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

                await qosOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
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

                return await consumeOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
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

                return await cancelOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

    }
}
