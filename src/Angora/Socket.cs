using System;
using System.IO.Pipelines;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Angora
{
    class Socket
    {
        public bool HeartbeatNeeded { get; private set; } = true;

        readonly SocketConnection connection = new SocketConnection();
        readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

        bool isOpen;

        public PipeReader Input => connection.Input;

        public async Task Connect(IPEndPoint endpoint)
        {
            await connection.ConnectAsync(endpoint);
            isOpen = true;
        }

        public async Task<PipeWriter> GetWriteBuffer()
        {
            if (!isOpen)
            {
                throw new Exception("socket is closed for writing");
            }

            await semaphore.WaitAsync();

            return connection.Output;
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
