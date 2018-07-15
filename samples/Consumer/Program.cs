using System;
using System.Threading.Tasks;
using Angora;

namespace Consumer
{
    class Program
    {
        static async Task Main()
        {
            var factory = new ConnectionFactory
            {
                HostName = "rabbit"
            };

            var connection = await factory.CreateConnection("Consumer");

            var channel = await connection.CreateChannel();

            await channel.Queue.Declare("test", false, true, false, false, null);

            await channel.Basic.Qos(0, 1000, false);

            var consumer = new MesssageConsumer(channel.Basic);
            var consumerTag = await channel.Basic.Consume("test", "Consumer", false, false, null, consumer.HandleIncomingMessage);

            Console.WriteLine("Consumer started. Press any key to quit.");
            Console.ReadKey();

            await channel.Basic.Cancel(consumerTag);

            await channel.Close();

            await connection.Close();
        }
    }

    class MesssageConsumer
    {
        readonly Basic basic;

        public MesssageConsumer(Basic basic)
        {
            this.basic = basic;
        }

        public Task HandleIncomingMessage(Basic.DeliverState messageState)
        {
            return basic.Ack(messageState.DeliveryTag, false);
        }
    }
}