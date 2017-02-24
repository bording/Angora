using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading.Tasks;

using static Angora.AmqpConstants;

namespace Angora
{
    public class Exchange
    {
        readonly ExchangeMethods methods;
        readonly Func<uint, object, Action<object, ReadableBuffer, Exception>, Task> SetExpectedReplyMethod;
        readonly Action ThrowIfClosed;

        readonly Action<object, ReadableBuffer, Exception> handle_DeclareOk;
        readonly Action<object, ReadableBuffer, Exception> handle_DeleteOk;
        readonly Action<object, ReadableBuffer, Exception> handle_BindOk;
        readonly Action<object, ReadableBuffer, Exception> handle_UnbindOk;

        internal Exchange(Socket socket, ushort channelNumber, Func<uint, object, Action<object, ReadableBuffer, Exception>, Task> setExpectedReplyMethod, Action throwIfClosed)
        {
            methods = new ExchangeMethods(socket, channelNumber);
            SetExpectedReplyMethod = setExpectedReplyMethod;
            ThrowIfClosed = throwIfClosed;

            handle_DeclareOk = Handle_DeclareOk;
            handle_DeleteOk = Handle_DeleteOk;
            handle_BindOk = Handle_BindOk;
            handle_UnbindOk = Handle_UnbindOk;
        }

        public async Task Declare(string exchangeName, string type, bool passive, bool durable, bool autoDelete, bool @internal, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            var declareOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Exchange.DeclareOk, declareOk, handle_DeclareOk);

            await methods.Send_Declare(exchangeName, type, passive, durable, autoDelete, @internal, arguments);

            await declareOk.Task;
        }

        void Handle_DeclareOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var declareOk = (TaskCompletionSource<bool>)tcs;

            if (exception != null)
            {
                declareOk.SetException(exception);
            }
            else
            {
                declareOk.SetResult(true);
            }
        }

        public async Task Delete(string exchange, bool onlyIfUnused)
        {
            ThrowIfClosed();

            var deleteOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Exchange.DeleteOk, deleteOk, handle_DeleteOk);

            await methods.Send_Delete(exchange, onlyIfUnused);

            await deleteOk.Task;
        }

        void Handle_DeleteOk(object tcs, ReadableBuffer arguments, Exception exception)
        {
            var deleteOk = (TaskCompletionSource<bool>)tcs;

            if (exception != null)
            {
                deleteOk.SetException(exception);
            }
            else
            {
                deleteOk.SetResult(true);
            }
        }

        public async Task Bind(string source, string destination, string routingKey, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            var bindOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Exchange.BindOk, bindOk, handle_BindOk);

            await methods.Send_Bind(source, destination, routingKey, arguments);

            await bindOk.Task;
        }

        void Handle_BindOk(object tcs, ReadableBuffer arguments, Exception exception)
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

        public async Task Unbind(string source, string destination, string routingKey, Dictionary<string, object> arguments)
        {
            ThrowIfClosed();

            var unbindOk = new TaskCompletionSource<bool>();
            await SetExpectedReplyMethod(Method.Exchange.UnbindOk, unbindOk, handle_UnbindOk);

            await methods.Send_Unbind(source, destination, routingKey, arguments);

            await unbindOk.Task;
        }

        void Handle_UnbindOk(object tcs, ReadableBuffer arguments, Exception exception)
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
    }
}
