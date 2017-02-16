﻿using System;
using System.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

using static RabbitMQClient.AmqpConstants;

namespace RabbitMQClient
{
    public class Channel
    {
        public ushort ChannelNumber { get; }

        readonly Socket socket;
        readonly SemaphoreSlim semaphore;

        ushort expectedMethodId;

        internal Channel(Socket socket, ushort channelNumber)
        {
            this.socket = socket;
            ChannelNumber = channelNumber;

            semaphore = new SemaphoreSlim(1, 1);
        }

        internal void ParseMethod(ushort classId, ushort methodId, ReadableBuffer arguments)
        {
            try
            {
                if (methodId != expectedMethodId)
                {
                    throw new Exception(); // and other appropriate stuff
                }

                switch (classId)
                {
                    case Command.Channel.ClassId:
                        ParseChannelMethod(methodId, arguments);
                        break;

                    case Command.Exchange.ClassId:
                        ParseExchangeMethod(methodId, arguments);
                        break;

                    case Command.Queue.ClassId:
                        ParseQueueMethod(methodId, arguments);
                        break;
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        void ParseChannelMethod(ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Channel.OpenOk:
                    Handle_Channel_OpenOk();
                    break;
                case Command.Channel.CloseOk:
                    Handle_Channel_CloseOk();
                    break;
            }
        }

        void Handle_Channel_OpenOk()
        {
            channel_OpenOk.SetResult(true);
        }

        void Handle_Channel_CloseOk()
        {
            channel_CloseOk.SetResult(true);
        }

        void ParseExchangeMethod(ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Exchange.DeclareOk:
                    Handle_Exchange_DeclareOk();
                    break;
                case Command.Exchange.DeleteOk:
                    Handle_Exchange_DeleteOk();
                    break;
            }
        }

        void Handle_Exchange_DeclareOk()
        {
            exchange_DeclareOk.SetResult(true);
        }

        void Handle_Exchange_DeleteOk()
        {
            exchange_DeleteOk.SetResult(true);
        }

        void ParseQueueMethod(ushort methodId, ReadableBuffer arguments)
        {
            switch (methodId)
            {
                case Command.Queue.DeclareOk:
                    Handle_Queue_DeclareOk(arguments);
                    break;
                case Command.Queue.BindOk:
                    Handle_Queue_BindOk();
                    break;
                case Command.Queue.UnbindOk:
                    Handle_Queue_UnbindOk();
                    break;
                case Command.Queue.PurgeOk:
                    Handle_Queue_PurgeOk(arguments);
                    break;
                case Command.Queue.DeleteOk:
                    Handle_Queue_DeleteOk(arguments);
                    break;
            }
        }

        public struct Queue_DeclareResult
        {
            public string QueueName;
            public uint MessageCount;
            public uint ConsumerCount;
        }

        void Handle_Queue_DeclareOk(ReadableBuffer arguments)
        {
            Queue_DeclareResult result;
            ReadCursor cursor;

            (result.QueueName, cursor) = arguments.ReadShortString();
            arguments = arguments.Slice(cursor);

            result.MessageCount = arguments.ReadBigEndian<uint>();
            arguments = arguments.Slice(sizeof(uint));

            result.ConsumerCount = arguments.ReadBigEndian<uint>();

            queue_DeclareOk.SetResult(result);
        }

        void Handle_Queue_BindOk()
        {
            queue_BindOk.SetResult(true);
        }

        void Handle_Queue_UnbindOk()
        {
            queue_UnbindOk.SetResult(true);
        }

        void Handle_Queue_PurgeOk(ReadableBuffer arguments)
        {
            var messageCount = arguments.ReadBigEndian<uint>();

            queue_PurgeOk.SetResult(messageCount);
        }

        void Handle_Queue_DeleteOk(ReadableBuffer arguments)
        {
            var messageCount = arguments.ReadBigEndian<uint>();

            queue_DeleteOk.SetResult(messageCount);
        }

        // Channel Send methods

        TaskCompletionSource<bool> channel_OpenOk;
        internal async Task Open()
        {
            await semaphore.WaitAsync();

            channel_OpenOk = new TaskCompletionSource<bool>();
            expectedMethodId = Command.Channel.OpenOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Command.Channel.ClassId);
                buffer.WriteBigEndian(Command.Channel.Open);
                buffer.WriteBigEndian(Reserved);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                await channel_OpenOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        TaskCompletionSource<bool> channel_CloseOk;
        public async Task Close(ushort replyCode = ChannelReplyCode.Success, string replyText = "Goodbye", ushort failingClass = 0, ushort failingMethod = 0)
        {
            await semaphore.WaitAsync();

            channel_CloseOk = new TaskCompletionSource<bool>();
            expectedMethodId = Command.Channel.CloseOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Command.Channel.ClassId);
                buffer.WriteBigEndian(Command.Channel.Close);
                buffer.WriteBigEndian(replyCode);
                buffer.WriteShortString(replyText);
                buffer.WriteBigEndian(failingClass);
                buffer.WriteBigEndian(failingMethod);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                await channel_CloseOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        // Exchange Send methods

        TaskCompletionSource<bool> exchange_DeclareOk;
        public async Task ExchangeDeclare(string exchangeName, string type, bool passive, bool durable, bool autoDelete, bool @internal, Dictionary<string, object> arguments)
        {
            await semaphore.WaitAsync();

            exchange_DeclareOk = new TaskCompletionSource<bool>();
            expectedMethodId = Command.Exchange.DeclareOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Command.Exchange.ClassId);
                buffer.WriteBigEndian(Command.Exchange.Declare);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(exchangeName);
                buffer.WriteShortString(type);
                buffer.WriteBits(passive, durable, autoDelete, @internal);
                buffer.WriteTable(arguments);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                await exchange_DeclareOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        TaskCompletionSource<bool> exchange_DeleteOk;
        public async Task ExchangeDelete(string exchange, bool onlyIfUnused)
        {
            await semaphore.WaitAsync();

            exchange_DeleteOk = new TaskCompletionSource<bool>();
            expectedMethodId = Command.Exchange.DeleteOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Command.Exchange.ClassId);
                buffer.WriteBigEndian(Command.Exchange.Delete);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(exchange);
                buffer.WriteBits(onlyIfUnused);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                await exchange_DeleteOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        // Queue Send methods

        TaskCompletionSource<Queue_DeclareResult> queue_DeclareOk;
        public async Task<Queue_DeclareResult> QueueDeclare(string queueName, bool passive, bool durable, bool exclusive, bool autoDelete, Dictionary<string, object> arguments)
        {
            await semaphore.WaitAsync();

            queue_DeclareOk = new TaskCompletionSource<Queue_DeclareResult>();
            expectedMethodId = Command.Queue.DeclareOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

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

                return await queue_DeclareOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        TaskCompletionSource<bool> queue_BindOk;
        public async Task QueueBind(string queue, string exchange, string routingKey, Dictionary<string, object> arguments)
        {
            await semaphore.WaitAsync();

            queue_BindOk = new TaskCompletionSource<bool>();
            expectedMethodId = Command.Queue.BindOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

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

                await queue_BindOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        TaskCompletionSource<bool> queue_UnbindOk;
        public async Task QueueUnbind(string queue, string exchange, string routingKey, Dictionary<string, object> arguments)
        {
            await semaphore.WaitAsync();

            queue_UnbindOk = new TaskCompletionSource<bool>();
            expectedMethodId = Command.Queue.UnbindOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

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

                await queue_UnbindOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        TaskCompletionSource<uint> queue_PurgeOk;
        public async Task<uint> QueuePurge(string queue)
        {
            await semaphore.WaitAsync();

            queue_PurgeOk = new TaskCompletionSource<uint>();
            expectedMethodId = Command.Queue.PurgeOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Command.Queue.ClassId);
                buffer.WriteBigEndian(Command.Queue.Purge);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteBits();

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                return await queue_PurgeOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }

        TaskCompletionSource<uint> queue_DeleteOk;
        public async Task<uint> QueueDelete(string queue, bool onlyIfUnused, bool onlyIfEmpty)
        {
            await semaphore.WaitAsync();

            queue_DeleteOk = new TaskCompletionSource<uint>();
            expectedMethodId = Command.Queue.DeleteOk;

            var buffer = await socket.GetWriteBuffer();

            try
            {
                var payloadSizeHeader = buffer.WriteFrameHeader(FrameType.Method, ChannelNumber);

                buffer.WriteBigEndian(Command.Queue.ClassId);
                buffer.WriteBigEndian(Command.Queue.Delete);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteBigEndian(Reserved);
                buffer.WriteShortString(queue);
                buffer.WriteBits(onlyIfEmpty, onlyIfUnused);

                payloadSizeHeader.WriteBigEndian((uint)buffer.BytesWritten - FrameHeaderSize);

                buffer.WriteBigEndian(FrameEnd);

                await buffer.FlushAsync();

                return await queue_DeleteOk.Task;
            }
            finally
            {
                socket.ReleaseWriteBuffer();
            }
        }
    }
}
