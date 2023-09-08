using System.Text;
using RabbitMQ.Client;

const string exchange = "logs";
string providedName = Environment.GetEnvironmentVariable("provided_name") ?? "publisher";

var factory = new ConnectionFactory { HostName = "localhost" };
factory.ClientProvidedName = providedName;

using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Fanout);

int count = 1;

while (count < 1000)
{
    string message = $"Log.Info: Message {count}";
    var body = Encoding.UTF8.GetBytes(message);

    channel.BasicPublish(exchange: exchange,
                         routingKey: string.Empty,
                         basicProperties: null,
                         body: body);

    Console.WriteLine($"{providedName} - {message}");

    Thread.Sleep(1000);

    count++;
}