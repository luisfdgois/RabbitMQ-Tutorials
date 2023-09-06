using System.Text;
using RabbitMQ.Client;

const string queue = "work-queues", routingKey = "work-queues";
string providedName = Environment.GetEnvironmentVariable("provided_name") ?? "producer";

var factory = new ConnectionFactory { HostName = "localhost" };
factory.ClientProvidedName = providedName;

using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.QueueDeclare(queue: queue,
                     durable: true,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);

int count = 1;

var properties = channel.CreateBasicProperties();
properties.Persistent = true;

while (count < 1000)
{
    string message = $"{providedName} - Message {count}";
    var body = Encoding.UTF8.GetBytes(message);

    channel.BasicPublish(exchange: string.Empty,
                         routingKey: routingKey,
                         basicProperties: properties,
                         body: body);

    Console.WriteLine($"Sent: {message}");

    Thread.Sleep(500);

    count++;
}