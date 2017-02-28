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

            await DemoGeneralUsage(channel);
            await PublishAndConsume(channel);

            await channel.Close();

            await connection.Close();
        }

        static async Task DemoGeneralUsage(Channel channel)
        {
            var arguments = new Dictionary<string, object>
            {
                { "x-queue-mode", "lazy" },
                { "x-message-ttl", 3000 }
            };

            var test1Result = await channel.Queue.Declare("test1", false, true, false, false, arguments);
            var test2Result = await channel.Queue.Declare("test2", false, true, false, false, null);
            var test3Result = await channel.Queue.Declare("test3", false, true, false, false, null);
            var generatedResult = await channel.Queue.Declare("", false, true, true, false, null);

            await channel.Exchange.Declare("test1", "fanout", false, true, false, false, null);
            await channel.Exchange.Declare("test2", "fanout", false, true, false, false, null);
            await channel.Exchange.Declare("test3", "direct", false, true, false, false, null);

            await channel.Exchange.Bind("test1", "test3", "key", arguments);
            await channel.Exchange.Unbind("test1", "test3", "key", arguments);

            await channel.Exchange.Declare("test-internal", "fanout", false, true, false, true, null);

            await channel.Queue.Bind("test1", "test1", "", null);
            await channel.Queue.Bind("test3", "test3", "foo", null);

            await channel.Queue.Bind("test2", "test1", "foo", null);
            await channel.Queue.Unbind("test2", "test1", "foo", null);

            var purgeCount = await channel.Queue.Purge("test2");

            var deleteCount = await channel.Queue.Delete("test2", true, true);

            await channel.Exchange.Delete("test2", false);

            await channel.Basic.Qos(0, 100, false);

            await channel.Basic.Recover();

            Console.WriteLine("Press any key to quit");
            Console.ReadKey();
        }

       static async Task PublishAndConsume(Channel channel)
        {
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

            var consumerTag = await channel.Basic.Consume("test", "myConsumer", true, false, null, message => Task.CompletedTask);

            Console.WriteLine("Press any key to quit");
            Console.ReadKey();

            await channel.Basic.Cancel(consumerTag);
        }
    }
}
