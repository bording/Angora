﻿using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    public class Queue
    {
        readonly QueueMethods methods;
        readonly Func<uint, object, Action<object, ReadOnlySequence<byte>, Exception>, Task> SetExpectedReplyMethod;
        readonly Action ThrowIfClosed;

        readonly Action<object, ReadOnlySequence<byte>, Exception> handle_DeclareOk;
        readonly Action<object, ReadOnlySequence<byte>, Exception> handle_BindOk;
        readonly Action<object, ReadOnlySequence<byte>, Exception> handle_UnbindOk;
        readonly Action<object, ReadOnlySequence<byte>, Exception> handle_PurgeOk;
        readonly Action<object, ReadOnlySequence<byte>, Exception> handle_DeleteOk;

        internal Queue(Socket socket, ushort channelNumber, Func<uint, object, Action<object, ReadOnlySequence<byte>, Exception>, Task> setExpectedReplyMethod, Action throwIfClosed)
        {
            methods = new QueueMethods(socket, channelNumber);
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

        public async Task<DeclareResult> Declare(string queueName, bool passive, bool durable, bool exclusive, bool autoDelete, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            var declareOk = new TaskCompletionSource<DeclareResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            await SetExpectedReplyMethod(Method.Queue.DeclareOk, declareOk, handle_DeclareOk);

            await methods.Send_Declare(queueName, passive, durable, exclusive, autoDelete, arguments);

            return await declareOk.Task;
        }

        void Handle_DeclareOk(object tcs, ReadOnlySequence<byte> arguments, Exception exception)
        {
            var declareOk = (TaskCompletionSource<DeclareResult>)tcs;

            if (exception != null)
            {
                declareOk.SetException(exception);
            }
            else
            {
                var reader = new CustomBufferReader(arguments);

                DeclareResult result;

                result.QueueName = reader.ReadShortString();
                result.MessageCount = reader.ReadUInt32();
                result.ConsumerCount = reader.ReadUInt32();

                declareOk.SetResult(result);
            }
        }

        public async Task Bind(string queue, string exchange, string routingKey, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            var bindOk = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            await SetExpectedReplyMethod(Method.Queue.BindOk, bindOk, handle_BindOk);

            await methods.Send_Bind(queue, exchange, routingKey, arguments);

            await bindOk.Task;
        }

        void Handle_BindOk(object tcs, ReadOnlySequence<byte> arguments, Exception exception)
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

        public async Task Unbind(string queue, string exchange, string routingKey, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            var unbindOk = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            await SetExpectedReplyMethod(Method.Queue.UnbindOk, unbindOk, handle_UnbindOk);

            await methods.Send_Unbind(queue, exchange, routingKey, arguments);

            await unbindOk.Task;
        }

        void Handle_UnbindOk(object tcs, ReadOnlySequence<byte> arguments, Exception exception)
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

        public async Task<uint> Purge(string queue)
        {
            ThrowIfClosed();

            var purgeOk = new TaskCompletionSource<uint>(TaskCreationOptions.RunContinuationsAsynchronously);
            await SetExpectedReplyMethod(Method.Queue.PurgeOk, purgeOk, handle_PurgeOk);

            await methods.Send_Purge(queue);

            return await purgeOk.Task;
        }

        void Handle_PurgeOk(object tcs, ReadOnlySequence<byte> arguments, Exception exception)
        {
            var purgeOk = (TaskCompletionSource<uint>)tcs;

            if (exception != null)
            {
                purgeOk.SetException(exception);
            }
            else
            {
                var reader = new CustomBufferReader(arguments);
                var messageCount = reader.ReadUInt32();
                purgeOk.SetResult(messageCount);
            }
        }

        public async Task<uint> Delete(string queue, bool onlyIfUnused, bool onlyIfEmpty)
        {
            ThrowIfClosed();

            var deleteOk = new TaskCompletionSource<uint>(TaskCreationOptions.RunContinuationsAsynchronously);
            await SetExpectedReplyMethod(Method.Queue.DeleteOk, deleteOk, handle_DeleteOk);

            await methods.Send_Delete(queue, onlyIfUnused, onlyIfEmpty);

            return await deleteOk.Task;
        }

        void Handle_DeleteOk(object tcs, ReadOnlySequence<byte> arguments, Exception exception)
        {
            var deleteOk = (TaskCompletionSource<uint>)tcs;

            if (exception != null)
            {
                deleteOk.SetException(exception);
            }
            else
            {
                var reader = new CustomBufferReader(arguments);
                var messageCount = reader.ReadUInt32();
                deleteOk.SetResult(messageCount);
            }
        }
    }
}
