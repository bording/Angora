using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Angora;

namespace Producer
{
    class Program
    {
        const int numberOfMessages = 1_000_000;

        static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            var factory = new ConnectionFactory
            {
                HostName = "rabbit"
            };

            var connection = await factory.CreateConnection("Producer");

            var channel = await connection.CreateChannel();

            await channel.Queue.Declare("test", false, true, false, false, null);

            Console.WriteLine("Producer started. Press any key to send messages.");
            Console.ReadKey();

            for (int i = 0; i < numberOfMessages; i++)
            {
                var properties = new MessageProperties()
                {
                    ContentType = "message",
                    AppId = "123",
                    Timestamp = DateTime.UtcNow,
                    Headers = new Dictionary<string, object>
                    {
                        {"MessageId",  i}
                    }
                };

                await channel.Basic.Publish("", "test", true, properties, System.Text.Encoding.UTF8.GetBytes("Message Payload"));
            }

            await channel.Close();

            await connection.Close();
        }
    }
}