using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading;

namespace RabbitMQClient
{
    public class Basic
    {
        readonly ushort channelNumber;
        readonly Socket socket;
        readonly SemaphoreSlim semaphore;
        readonly Action<ushort> SetExpectedMethodId;

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
                default:
                    break;
            }
        }
    }
}
