﻿using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    public class Basic
    {
        readonly BasicMethods methods;
        readonly Func<uint, object, Action<object, ReadOnlySequence<byte>, Exception>, Task> SetExpectedReplyMethod;
        readonly Action ThrowIfClosed;

        readonly Action<object, ReadOnlySequence<byte>, Exception> handle_QosOk;
        readonly Action<object, ReadOnlySequence<byte>, Exception> handle_ConsumeOk;
        readonly Action<object, ReadOnlySequence<byte>, Exception> handle_CancelOk;
        readonly Action<object, ReadOnlySequence<byte>, Exception> handle_RecoverOk;

        Dictionary<string, Func<DeliverState, Task>> consumers;

        internal Basic(Socket socket, ushort channelNumber, uint maxContentBodySize, Func<uint, object, Action<object, ReadOnlySequence<byte>, Exception>, Task> setExpectedReplyMethod, Action throwIfClosed)
        {
            methods = new BasicMethods(socket, channelNumber, maxContentBodySize);
            SetExpectedReplyMethod = setExpectedReplyMethod;
            ThrowIfClosed = throwIfClosed;

            handle_QosOk = Handle_QosOk;
            handle_ConsumeOk = Handle_ConsumeOk;
            handle_CancelOk = Handle_CancelOk;
            handle_RecoverOk = Handle_RecoverOk;
        }

        public async Task Qos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            ThrowIfClosed();

            var qosOk = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            await SetExpectedReplyMethod(Method.Basic.QosOk, qosOk, handle_QosOk);

            await methods.Send_Qos(prefetchSize, prefetchCount, global);

            await qosOk.Task;
        }

        void Handle_QosOk(object tcs, ReadOnlySequence<byte> arguments, Exception exception)
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

        Func<DeliverState, Task> pendingConsumer;

        public async Task<string> Consume(string queue, string consumerTag, bool autoAck, bool exclusive, Dictionary<string, object> arguments, Func<DeliverState, Task> consumer)
        {
            ThrowIfClosed();

            var consumeOk = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            await SetExpectedReplyMethod(Method.Basic.ConsumeOk, consumeOk, handle_ConsumeOk);

            if (consumers == null)
            {
                consumers = new Dictionary<string, Func<DeliverState, Task>>();
            }

            pendingConsumer = consumer;

            await methods.Send_Consume(queue, consumerTag, autoAck, exclusive, arguments);

            return await consumeOk.Task;
        }

        void Handle_ConsumeOk(object tcs, ReadOnlySequence<byte> arguments, Exception exception)
        {
            var consumeOk = (TaskCompletionSource<string>)tcs;

            if (exception != null)
            {
                consumeOk.SetException(exception);
            }
            else
            {
                var reader = new CustomBufferReader(arguments);

                var consumerTag = reader.ReadShortString();

                consumers.Add(consumerTag, pendingConsumer);

                consumeOk.SetResult(consumerTag);
            }

            pendingConsumer = null;
        }

        public async Task<string> Cancel(string consumerTag)
        {
            ThrowIfClosed();

            var cancelOk = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            await SetExpectedReplyMethod(Method.Basic.CancelOk, cancelOk, handle_CancelOk);

            await methods.Send_Cancel(consumerTag);

            return await cancelOk.Task;
        }

        void Handle_CancelOk(object tcs, ReadOnlySequence<byte> arguments, Exception exception)
        {
            var cancelOk = (TaskCompletionSource<string>)tcs;

            if (exception != null)
            {
                cancelOk.SetException(exception);
            }
            else
            {
                var reader = new CustomBufferReader(arguments);
                var consumerTag = reader.ReadShortString();
                cancelOk.SetResult(consumerTag);
            }
        }

        public async Task Recover()
        {
            ThrowIfClosed();

            var recoverOk = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            await SetExpectedReplyMethod(Method.Basic.RecoverOk, recoverOk, handle_RecoverOk);

            await methods.Send_Recover();

            await recoverOk.Task;
        }

        void Handle_RecoverOk(object tcs, ReadOnlySequence<byte> arguments, Exception exception)
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

        public Task Ack(ulong deliveryTag, bool multiple)
        {
            ThrowIfClosed();

            return methods.Send_Ack(deliveryTag, multiple);
        }

        public Task Publish(string exchange, string routingKey, bool mandatory, MessageProperties properties, Memory<byte> body)
        {
            ThrowIfClosed();

            return methods.Send_Publish(exchange, routingKey, mandatory, properties, body);
        }

        public class DeliverState
        {
            public string ConsumerTag { get; set; }

            public ulong DeliveryTag { get; set; }

            public bool Redelivered { get; set; }

            public string Exchange { get; set; }

            public string RoutingKey { get; set; }

            public MessageProperties Properties { get; set; }

            public byte[] Body { get; set; }
        }


        DeliverState pendingDelivery;

        internal Task Handle_Deliver(ReadOnlySequence<byte> arguments)
        {
            var reader = new CustomBufferReader(arguments);

            pendingDelivery = new DeliverState
            {
                ConsumerTag = reader.ReadShortString(),
                DeliveryTag = reader.ReadUInt64(),
                Redelivered = Convert.ToBoolean(reader.ReadByte()),
                Exchange = reader.ReadShortString(),
                RoutingKey = reader.ReadShortString()
            };

            return Task.CompletedTask;
        }

        internal async Task Handle_ContentHeader(ReadOnlySequence<byte> payload)
        {
            ulong Read()
            {
                var reader = new CustomBufferReader(payload);

                var classId = reader.ReadUInt16();
                var weight = reader.ReadUInt16();
                var bodySize = reader.ReadUInt64();

                pendingDelivery.Properties = reader.ReadBasicProperties();

                return bodySize;
            }

            var size = Read();

            if (size == 0)
            {
                pendingDelivery.Body = new byte[0];

                await consumers[pendingDelivery.ConsumerTag](pendingDelivery);
                pendingDelivery = null;
            }
        }

        internal async Task Handle_ContentBody(ReadOnlySequence<byte> payload)
        {
            pendingDelivery.Body = payload.ToArray();

            await consumers[pendingDelivery.ConsumerTag](pendingDelivery);
            pendingDelivery = null;
        }
    }
}
