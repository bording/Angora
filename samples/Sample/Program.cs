using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Angora;

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

            var connection = await factory.CreateConnection("test connection");

            var channel = await connection.CreateChannel();

            await channel.Queue.Declare("test", false, true, false, false, null);

            var properties = new MessageProperties()
            {
                ContentType = "message",
                AppId = "123",
                Timestamp = DateTime.UtcNow,
                Headers = new Dictionary<string, object>
                {
                    {"MessageId",  Guid.NewGuid().ToString()}
                }
            };

            await channel.Basic.Publish("", "test", true, properties, System.Text.Encoding.UTF8.GetBytes("Message Payload"));

            Console.WriteLine("Press any key consume messages");
            Console.ReadKey();

            await channel.Basic.Qos(0, 1, false);

            var consumerTag = await channel.Basic.Consume("test", "myConsumer", true, false, null);

            Console.WriteLine("Press any key to quit");
            Console.ReadKey();

            await channel.Basic.Cancel(consumerTag);

            await channel.Close();

            await connection.Close();
        }
    }
}
