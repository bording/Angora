using System.Threading.Tasks;

namespace RabbitMQClient
{
    static class TaskExtensions
    {
        public static void Ignore(this Task task) { }
    }
}
