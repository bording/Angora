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

            var consumerTag = await channel.Basic.Consume("test1", null, false, false, null);

            await channel.Basic.Recover();

            var properties = new MessageProperties();
            properties.ContentType = "message";
            properties.AppId = "123";
            properties.Timestamp = DateTime.UtcNow;
            properties.Headers = new Dictionary<string, object>();
            properties.Headers.Add("MessageId", Guid.NewGuid().ToString());
            properties.Headers.Add("OtherHeader", "another value goes here");

            await channel.Basic.Publish("test3", "foo", true, properties, System.Text.Encoding.UTF8.GetBytes("Message Payload"));

            Console.WriteLine("Press any key to quit");
            Console.ReadKey();

            consumerTag = await channel.Basic.Cancel(consumerTag);

            await channel.Close();

            await connection.Close();
        }
    }
}
