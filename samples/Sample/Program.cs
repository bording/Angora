using RabbitMQClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

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

            var tasks = new List<Task>
            {
                channel.QueueDeclare("test1", false, true, false, false, false, new byte[0]),
                channel.QueueDeclare("test2", false, true, false, false, false, new byte[0])
            };

            await Task.WhenAll(tasks);

            Console.WriteLine("Press any key to quit");
            Console.ReadKey();

            await connection.Close();
        }
    }
}
