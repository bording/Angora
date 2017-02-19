using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.IO.Pipelines.Networking.Sockets;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQClient
{
    class Socket
    {
        public bool HeartbeatNeeded { get; private set; } = true;

        SocketConnection connection;
        readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

        bool isOpen;

        public IPipeReader Input => connection.Input;

        public async Task Connect(IPEndPoint endpoint)
        {
            connection = await SocketConnection.ConnectAsync(endpoint);
            isOpen = true;
        }

        public async Task<WritableBuffer> GetWriteBuffer(int minimumSize = 0)
        {
            if (!isOpen)
            {
                throw new Exception("socket is closed for writing");
            }

            await semaphore.WaitAsync();

            return connection.Output.Alloc(minimumSize);
        }

        public void ReleaseWriteBuffer(bool wroteHeartbeat = false)
        {
            HeartbeatNeeded = wroteHeartbeat;
            semaphore.Release();
        }

        public void Close()
        {
            isOpen = false;

            connection.Output.Complete();
        }
    }
}
