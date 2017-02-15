using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQClient;

namespace Sample
{
    public class Program
    {
        public static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        public static async Task MainAsync(string[] args)
        {
            var factory = new ConnectionFactory
            {
                HostName = "rabbit"
            };

            var connection = await factory.CreateConnection();

            var channel = await connection.CreateChannel();

            var arguments = new Dictionary<string, object>
            {
                { "x-queue-mode", "lazy" },
                { "x-message-ttl", 3000 }
            };

            await channel.QueueDeclare("test1", false, true, false, false, false, arguments);
            await channel.QueueDeclare("test2", false, true, false, false, true, null);

            Console.WriteLine("Press any key to quit");
            Console.ReadKey();

            await connection.Close();
        }
    }
}
