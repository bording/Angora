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
        SocketConnection connection;
        SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

        public IPipeReader Input => connection.Input;

        public async Task Connect(IPEndPoint endpoint)
        {
            connection = await SocketConnection.ConnectAsync(endpoint);
        }

        public async Task<WritableBuffer> GetWriteBuffer(int minimumSize = 0)
        {
            await semaphore.WaitAsync();

            return connection.Output.Alloc(minimumSize);
        }

        public void ReleaseWriteBuffer()
        {
            semaphore.Release();
        }

        public Task Close() => connection.DisposeAsync();
    }
}
