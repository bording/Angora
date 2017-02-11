using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Threading.Tasks;

namespace RabbitMQClient
{
    static class TaskExtensions
    {
        public static void Ignore(this Task task)
        {
        }

        public static void Ignore(this WritableBufferAwaitable awaitable)
        {
        }
    }
}
