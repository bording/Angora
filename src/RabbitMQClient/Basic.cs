using System;
using System.Binary;
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
        readonly Action<ushort> SetExpectedMethodId;

        TaskCompletionSource<bool> qosOk;

        internal Basic(ushort channelNumber, Socket socket, SemaphoreSlim semaphore, Action<ushort> setExpectedMethodId)
        {
            this.channelNumber = channelNumber;
            this.socket = socket;
            this.semaphore = semaphore;
            SetExpectedMethodId = setExpectedMethodId;
        }

        internal void HandleIncomingMethod(ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Basic.QosOk:
                    Handle_QosOk();
                    break;
            }
        }

        private void Handle_QosOk()
        {
            qosOk.SetResult(true);
        }

        public async Task Qos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            await semaphore.WaitAsync();

            qosOk = new TaskCompletionSource<bool>();
            SetExpectedMethodId(Command.Basic.QosOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Command.Basic.ClassId);
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
    }
}
