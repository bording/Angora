using System.Threading.Tasks;

namespace RabbitMQClient
{
    public class ConnectionFactory
    {
        public string HostName { get; set; }

        public string UserName { get; set; } = "guest";

        public string Password { get; set; } = "guest";

        public string VirtualHost { get; set; } = "/";

        public async Task<Connection> CreateConnection(string connectionName = null)
        {
            var connection = new Connection(HostName, UserName, Password, VirtualHost);
            await connection.Connect(connectionName);

            return connection;
        }
    }
}
