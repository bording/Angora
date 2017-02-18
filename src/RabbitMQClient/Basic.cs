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
        readonly SemaphoreSlim semaphore;
        readonly Action<uint, Action<Exception>> SetExpectedReplyMethod;

        TaskCompletionSource<bool> qosOk;
        TaskCompletionSource<string> consumeOk;
        TaskCompletionSource<string> cancelOk;

        internal Basic(ushort channelNumber, Socket socket, SemaphoreSlim semaphore, Action<uint, Action<Exception>> setExpectedReplyMethod)
        {
            this.channelNumber = channelNumber;
            this.socket = socket;
            this.semaphore = semaphore;
            SetExpectedReplyMethod = setExpectedReplyMethod;
        }

        internal void HandleIncomingMethod(uint method, ReadableBuffer arguments)
        {
            switch (method)
            {
                case Command.Basic.QosOk:
                    Handle_QosOk();
                    break;
                case Command.Basic.ConsumeOk:
                    Handle_ConsumeOk(arguments);
                    break;
                case Command.Basic.CancelOk:
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
            await semaphore.WaitAsync();

            qosOk = new TaskCompletionSource<bool>();
            SetExpectedReplyMethod(Command.Basic.QosOk, ex => qosOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Command.Basic.Qos);
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
            await semaphore.WaitAsync();

            consumeOk = new TaskCompletionSource<string>();
            SetExpectedReplyMethod(Command.Basic.ConsumeOk, ex => consumeOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Command.Basic.Consume);
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
            await semaphore.WaitAsync();

            cancelOk = new TaskCompletionSource<string>();
            SetExpectedReplyMethod(Command.Basic.CancelOk, ex => cancelOk.SetException(ex));

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Command.Basic.Cancel);
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
