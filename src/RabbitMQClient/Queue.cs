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
        readonly SemaphoreSlim semaphore;
        readonly Action<ushort> SetExpectedMethodId;

        TaskCompletionSource<DeclareResult> declareOk;
        TaskCompletionSource<bool> bindOk;
        TaskCompletionSource<bool> unbindOk;
        TaskCompletionSource<uint> purgeOk;
        TaskCompletionSource<uint> deleteOk;

        internal Queue(ushort channelNumber, Socket socket, SemaphoreSlim semaphore, Action<ushort> setExpectedMethodId)
        {
            this.channelNumber = channelNumber;
            this.socket = socket;
            this.semaphore = semaphore;
            SetExpectedMethodId = setExpectedMethodId;
        }

        internal void ParseQueueMethod(ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Queue.DeclareOk:
                    Handle_DeclareOk(arguments);
                    break;
                case Command.Queue.BindOk:
                    Handle_BindOk();
                    break;
                case Command.Queue.UnbindOk:
                    Handle_UnbindOk();
                    break;
                case Command.Queue.PurgeOk:
                    Handle_PurgeOk(arguments);
                    break;
                case Command.Queue.DeleteOk:
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
            await semaphore.WaitAsync();

            declareOk = new TaskCompletionSource<DeclareResult>();
            SetExpectedMethodId(Command.Queue.DeclareOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Command.Queue.ClassId);
                buffer.WriteBigEndian(Command.Queue.Declare);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queueName);
                buffer.WriteBits(passive, durable, exclusive, autoDelete);
                buffer.WriteTable(arguments);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                return await declareOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Bind(string queue, string exchange, string routingKey, Dictionary<string, object> arguments)
        {
            await semaphore.WaitAsync();

            bindOk = new TaskCompletionSource<bool>();
            SetExpectedMethodId(Command.Queue.BindOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Command.Queue.ClassId);
                buffer.WriteBigEndian(Command.Queue.Bind);
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

                await bindOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task Unbind(string queue, string exchange, string routingKey, Dictionary<string, object> arguments)
        {
            await semaphore.WaitAsync();

            unbindOk = new TaskCompletionSource<bool>();
            SetExpectedMethodId(Command.Queue.UnbindOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Command.Queue.ClassId);
                buffer.WriteBigEndian(Command.Queue.Unbind);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteShortString(exchange);
                buffer.WriteShortString(routingKey);
                buffer.WriteTable(arguments);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                await unbindOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task<uint> Purge(string queue)
        {
            await semaphore.WaitAsync();

            purgeOk = new TaskCompletionSource<uint>();
            SetExpectedMethodId(Command.Queue.PurgeOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Command.Queue.ClassId);
                buffer.WriteBigEndian(Command.Queue.Purge);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteBits();

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                return await purgeOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        public async Task<uint> Delete(string queue, bool onlyIfUnused, bool onlyIfEmpty)
        {
            await semaphore.WaitAsync();

            deleteOk = new TaskCompletionSource<uint>();
            SetExpectedMethodId(Command.Queue.DeleteOk);

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, channelNumber);

                buffer.WriteBigEndian(Command.Queue.ClassId);
                buffer.WriteBigEndian(Command.Queue.Delete);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteBits(onlyIfEmpty, onlyIfUnused);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                return await deleteOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }
    }
}
