﻿using System;
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

            var test1Result = await channel.QueueDeclare("test1", false, true, false, false, arguments);
            var test2Result = await channel.QueueDeclare("test2", false, true, false, false, null);
            var generatedResult = await channel.QueueDeclare("", false, true, true, false, null);

            await channel.ExchangeDeclare("test1", "fanout", false, true, false, false, null);
            await channel.ExchangeDeclare("test2", "fanout", false, true, false, false, null);
            await channel.ExchangeDeclare("test3", "fanout", false, true, false, false, null);

            await channel.ExchangeBind("test1", "test3", "key", arguments);

            await channel.ExchangeDeclare("test-internal", "fanout", false, true, false, true, null);

            await channel.QueueBind("test1", "test1", "", null);

            await channel.QueueBind("test2", "test1", "foo", null);
            await channel.QueueUnbind("test2", "test1", "foo", null);

            var purgeCount = await channel.QueuePurge("test2");

            var deleteCount = await channel.QueueDelete("test2", true, true);

            await channel.ExchangeDelete("test2", false);

            Console.WriteLine("Press any key to quit");
            Console.ReadKey();

            await channel.Close();

            await connection.Close();
        }
    }
}
