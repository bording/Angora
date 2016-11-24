using System;
using System.Collections.Generic;
using System.IO.Pipelines.Networking.Sockets;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQClient
{
    public class Connection
    {
        static readonly byte[] protocolHeader = Encoding.UTF8.GetBytes("AMQP0091");
        static readonly byte[] protocolHeader2 = { 0x41, 0x4d, 0x51, 0x50, 0x00, 0x00, 0x09, 0x01 };

        // IPipelineConnection connection;

        public static async Task Start()
        {
            var address = IPAddress.Parse("192.168.1.83");
            var endpoint = new IPEndPoint(address, 5672);

            Console.WriteLine("Connecting...");
            var connection = await SocketConnection.ConnectAsync(endpoint);
            Console.WriteLine("Connected.");

            var writableBuffer = connection.Output.Alloc();
            writableBuffer.Write(protocolHeader2);

            Console.WriteLine("Flushing...");
            await writableBuffer.FlushAsync();

            var result = await connection.Input.ReadAsync();

            var buffer = result.Buffer;



            Console.WriteLine("Press any key yo");
            Console.ReadKey();
        }
    }
}
